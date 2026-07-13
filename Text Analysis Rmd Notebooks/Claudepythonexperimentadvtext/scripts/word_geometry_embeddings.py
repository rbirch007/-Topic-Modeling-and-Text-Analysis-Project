"""
Stage 4 — Per-word contextual MEANING vectors for the within-topic word geometry.

For each topic's top probability words, embed EVERY occurrence of the word IN
CONTEXT with MacBERTh (historical-English BERT), restricted to that topic's own
articles (theta >= 75th percentile, matching the notebook), and mean-pool to one
prototype vector per (topic, word). The R notebook (§6, "word meaning-geometry")
reads these and lays each topic's words out in 3-D, so positions reflect genuine
CONTEXTUAL meaning — not the document-embedding average, not raw co-occurrence.

This is the word-level sibling of semantic_change.py: same MacBERTh occurrence
embedding, pooled per (topic, word) instead of per (word, period).

Decoupled like the other stages: reads only the handoff files, writes one parquet.
Checkpointed/resumable: re-running skips topics already in the output, and writes
after each topic so an interrupted run resumes cleanly.

Run (dedicated Python 3.11 venv, NOT system Python):
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\word_geometry_embeddings.py 18
    set ADV_SCENARIO=RSM1935-1950 & ...\\word_geometry_embeddings.py 18
    set ADV_WG_NWORDS=12 & ...                # top prob words per topic (default 12)

Output (EMB_DIR):
    topic_word_vectors_K{K}.parquet   topic, word, n, dim_0..dim_{H-1}
"""
from __future__ import annotations
import os, sys, re
import numpy as np
import pandas as pd
import config as C

_NONLETTER = re.compile(r"[^a-z]")
N_WORDS       = int(os.environ.get("ADV_WG_NWORDS", "12"))    # top prob words per topic
THETA_QUANTILE = 0.75                                          # match the notebook's doc cut
MAX_OCCUR     = int(os.environ.get("ADV_WG_MAXOCCUR", "150"))  # cap occurrences per (topic, word)
MIN_OCCUR     = 3                                              # need >= this many to keep a word


def find_contexts(text: str, target: str, half: int = C.CONTEXT_HALF_WORDS):
    """Yield (context_str, char_start, char_end) for each occurrence of target."""
    words = str(text).split()
    out = []
    for i, w in enumerate(words):
        if _NONLETTER.sub("", w.lower()) == target:
            lo = max(0, i - half); hi = min(len(words), i + half + 1)
            ctx_words = words[lo:hi]
            before = " ".join(ctx_words[: i - lo])
            cs = len(before) + (1 if i - lo > 0 else 0)
            ce = cs + len(words[i])
            out.append((" ".join(ctx_words), cs, ce))
            if len(out) >= MAX_OCCUR:
                break
    return out


def parse_prob_words(s: str, n: int):
    return [w.strip() for w in str(s).split(",") if w.strip()][:n]


def main():
    K = C.resolve_K(sys.argv[1] if len(sys.argv) > 1 else None)

    docs = pd.read_csv(C.docs_csv(K)).sort_values("row_index").reset_index(drop=True)
    theta_df = pd.read_csv(C.theta_csv(K)).sort_values("row_index").reset_index(drop=True)
    theta = theta_df.filter(regex=r"^Topic\d+$").values            # D x K
    topics = pd.read_csv(C.topics_csv(K))
    Kt = theta.shape[1]
    texts = docs["text"].to_numpy()
    out_path = C.EMB_DIR / f"topic_word_vectors_K{K}.parquet"

    # Resume: skip topics already embedded.
    done, prev_frames = set(), []
    if out_path.exists():
        prev = pd.read_parquet(out_path)
        prev_frames = [prev]
        done = set(int(t) for t in prev["topic"].unique())
        print(f"[wordgeom] resuming: {len(done)} topics already done", flush=True)

    import torch
    from transformers import AutoTokenizer, AutoModel
    tok = AutoTokenizer.from_pretrained(C.HIST_MODEL, use_fast=True)
    model = AutoModel.from_pretrained(C.HIST_MODEL); model.eval()
    torch.set_grad_enabled(False)
    print(f"[wordgeom] K={K}  topics={Kt}  model={C.HIST_MODEL}  "
          f"words/topic={N_WORDS}", flush=True)

    def embed_targets(contexts, spans):
        """(n, dim) target-token embeddings (mean over the target's subword pieces)."""
        vecs = []
        for i in range(0, len(contexts), C.HIST_BATCH):
            cb = contexts[i:i + C.HIST_BATCH]; sb = spans[i:i + C.HIST_BATCH]
            enc = tok(cb, return_offsets_mapping=True, truncation=True,
                      max_length=C.HIST_MAXLEN, padding=True, return_tensors="pt")
            offs = enc.pop("offset_mapping")
            hid = model(**enc).last_hidden_state
            for b in range(len(cb)):
                cs, ce = sb[b]
                sel = [(t_s < ce and t_e > cs and not (t_s == 0 and t_e == 0))
                       for (t_s, t_e) in offs[b].tolist()]
                idx = [j for j, keep in enumerate(sel) if keep]
                vecs.append(hid[b, idx].mean(0).numpy().astype(np.float32) if idx else None)
        return vecs

    def l2(m):
        n = np.linalg.norm(m, axis=1, keepdims=True)
        return m / np.clip(n, 1e-9, None)

    frames = list(prev_frames)
    for t in range(1, Kt + 1):
        if t in done:
            continue
        prob = topics.loc[topics["topic_num"] == t, "prob"]
        words = parse_prob_words(prob.iloc[0], N_WORDS) if len(prob) else []
        if not words:
            print(f"[wordgeom] T{t}: no prob words in handoff, skipping", flush=True)
            continue

        thr = np.quantile(theta[:, t - 1], THETA_QUANTILE)
        sel_text = texts[theta[:, t - 1] >= thr]                    # the topic's articles

        wrows = []
        for w in words:
            ctxs, spans = [], []
            for txt in sel_text:
                for (c, cs, ce) in find_contexts(txt, w):
                    ctxs.append(c); spans.append((cs, ce))
                    if len(ctxs) >= MAX_OCCUR:
                        break
                if len(ctxs) >= MAX_OCCUR:
                    break
            if not ctxs:
                continue
            vs = [v for v in embed_targets(ctxs, spans) if v is not None]
            if len(vs) < MIN_OCCUR:
                continue
            proto = l2(np.vstack(vs)).mean(0)                       # L2 then mean (a prototype)
            wrows.append(dict(topic=t, word=w, n=len(vs),
                              **{f"dim_{j}": float(proto[j]) for j in range(len(proto))}))

        if wrows:
            frames.append(pd.DataFrame(wrows))
            pd.concat(frames, ignore_index=True).to_parquet(out_path, index=False)  # checkpoint
            print(f"[wordgeom] T{t}: {len(wrows)}/{len(words)} words embedded "
                  f"(saved {out_path.name})", flush=True)
        else:
            print(f"[wordgeom] T{t}: no usable words (too few occurrences)", flush=True)

    print(f"[wordgeom] DONE -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
