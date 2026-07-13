"""
Stage 3 — Diachronic semantic change with contextual embeddings (the frontier).

For each target word, gather its occurrences per historical period, embed each
occurrence IN CONTEXT with MacBERTh (a BERT trained on historical English), and
quantify how the word's meaning moves across periods using three complementary
measures from the lexical-semantic-change literature (SemEval-2020 Task 1):

  PRT  prototype distance  = 1 - cos(mean_a, mean_b)         (coarse drift)
  APD  avg pairwise dist   = mean 1 - cos over cross pairs    (spread-sensitive)
  JSD  sense-distribution  = Jensen-Shannon over induced word senses (KMeans)

This replaces the old word2vec + Procrustes approach: no rotation alignment is
needed because every occurrence lives in the same pretrained space, and we get
genuine sense-level change, not just neighbor reshuffling.

Run:
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\semantic_change.py
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\semantic_change.py 35
    set ADV_WORDS=suffrage,sister & ...semantic_change.py     # subset for dev

Outputs (DIACHRONIC_DIR):
    diachronic_change_K{K}.csv        word, comparison, prt, apd, jsd, n_a, n_b
    diachronic_prototypes_K{K}.parquet word, period, n, proto vector (for R drift PCA)
    diachronic_periods.json           the periods actually used (R reads this)
"""
from __future__ import annotations
import os, sys, re, json, itertools
import numpy as np
import pandas as pd
import config as C

_NONLETTER = re.compile(r"[^a-z]")


def assign_periods(years: pd.Series):
    bounds = [b for b in C.DEFAULT_PERIOD_BOUNDS]
    lo, hi = int(years.min()), int(years.max())
    used = [(n, s, e) for (n, s, e) in bounds if not (e < lo or s > hi)]
    dropped = [n for (n, s, e) in bounds if (e < lo or s > hi)]
    if dropped:
        print(f"[diachronic] dropping periods outside data range "
              f"{lo}-{hi}: {dropped}", flush=True)

    def which(y):
        if pd.isna(y):
            return None
        for (n, s, e) in used:
            if s <= y <= e:
                return n
        return None
    return [which(y) for y in years], used


def find_contexts(text: str, target: str, half: int = C.CONTEXT_HALF_WORDS):
    """Yield (context_str, char_start, char_end) for each occurrence of target."""
    words = str(text).split()
    out = []
    for i, w in enumerate(words):
        if _NONLETTER.sub("", w.lower()) == target:
            lo = max(0, i - half)
            hi = min(len(words), i + half + 1)
            ctx_words = words[lo:hi]
            before = " ".join(ctx_words[: i - lo])
            cs = len(before) + (1 if i - lo > 0 else 0)
            ce = cs + len(words[i])
            out.append((" ".join(ctx_words), cs, ce))
    return out


def main():
    K = C.resolve_K(sys.argv[1] if len(sys.argv) > 1 else None)
    targets = C.DIACHRONIC_TARGETS
    if os.environ.get("ADV_WORDS"):
        targets = [w.strip() for w in os.environ["ADV_WORDS"].split(",") if w.strip()]

    docs = pd.read_csv(C.docs_csv(K))
    periods, used = assign_periods(docs["year"])
    docs = docs.assign(period=periods)
    period_names = [n for (n, s, e) in used]
    (C.DIACHRONIC_DIR / "diachronic_periods.json").write_text(
        json.dumps({"K": K, "periods": [{"name": n, "start": s, "end": e}
                                        for (n, s, e) in used]}, indent=2))
    print(f"[diachronic] K={K}  periods={period_names}  targets={len(targets)}",
          flush=True)

    import torch
    from transformers import AutoTokenizer, AutoModel
    tok = AutoTokenizer.from_pretrained(C.HIST_MODEL, use_fast=True)
    model = AutoModel.from_pretrained(C.HIST_MODEL)
    model.eval()
    torch.set_grad_enabled(False)

    def embed_targets(contexts, spans):
        """Return (n, dim) target-token embeddings (mean over subword pieces)."""
        vecs = []
        for i in range(0, len(contexts), C.HIST_BATCH):
            cb = contexts[i : i + C.HIST_BATCH]
            sb = spans[i : i + C.HIST_BATCH]
            enc = tok(cb, return_offsets_mapping=True, truncation=True,
                      max_length=C.HIST_MAXLEN, padding=True, return_tensors="pt")
            offs = enc.pop("offset_mapping")
            hid = model(**enc).last_hidden_state          # (B, T, H)
            for b in range(len(cb)):
                cs, ce = sb[b]
                sel = [(t_s < ce and t_e > cs and not (t_s == 0 and t_e == 0))
                       for (t_s, t_e) in offs[b].tolist()]
                idx = [j for j, keep in enumerate(sel) if keep]
                if not idx:                                # truncated past target
                    vecs.append(None); continue
                vecs.append(hid[b, idx].mean(0).numpy().astype(np.float32))
        return vecs

    def l2(m):
        n = np.linalg.norm(m, axis=1, keepdims=True)
        return m / np.clip(n, 1e-9, None)

    change_csv = C.DIACHRONIC_DIR / f"diachronic_change_K{K}.csv"
    proto_pq   = C.DIACHRONIC_DIR / f"diachronic_prototypes_K{K}.parquet"
    change_rows, proto_rows, done = [], [], set()
    if change_csv.exists() and proto_pq.exists():
        change_rows = pd.read_csv(change_csv).to_dict("records")
        proto_rows  = pd.read_parquet(proto_pq).to_dict("records")
        done = {r["word"] for r in change_rows} | {r["word"] for r in proto_rows}
        print(f"[diachronic] resuming: {len(done)} words already done", flush=True)

    for word in targets:
        if word in done:
            continue
        per_emb = {}   # period -> (n, dim)
        for p in period_names:
            sub = docs[docs["period"] == p]
            ctxs, spans = [], []
            for txt in sub["text"]:
                for (c, cs, ce) in find_contexts(txt, word):
                    ctxs.append(c); spans.append((cs, ce))
                    if len(ctxs) >= C.MAX_OCCUR_PER_PERIOD:
                        break
                if len(ctxs) >= C.MAX_OCCUR_PER_PERIOD:
                    break
            if not ctxs:
                continue
            vs = [v for v in embed_targets(ctxs, spans) if v is not None]
            if len(vs) >= 5:
                per_emb[p] = np.vstack(vs)
        present = [p for p in period_names if p in per_emb]
        if len(present) < 2:
            print(f"[diachronic] '{word}': <2 usable periods, skipping", flush=True)
            continue

        # prototypes (for drift PCA in R)
        for p in present:
            proto = l2(per_emb[p]).mean(0)
            proto_rows.append(dict(word=word, period=p, n=len(per_emb[p]),
                                   **{f"dim_{j}": float(proto[j])
                                      for j in range(len(proto))}))

        # induced senses (KMeans over all occurrences, pooled across periods)
        from sklearn.cluster import KMeans
        allv = l2(np.vstack([per_emb[p] for p in present]))
        owner = np.concatenate([[p] * len(per_emb[p]) for p in present])
        k_sense = min(C.N_SENSES, max(2, allv.shape[0] // 30))
        km = KMeans(n_clusters=k_sense, random_state=12345, n_init=5).fit(allv)
        labels = km.labels_

        def sense_dist(p):
            lab = labels[owner == p]
            d = np.bincount(lab, minlength=k_sense).astype(float)
            return d / d.sum()

        def jsd(a, b):
            m = 0.5 * (a + b)
            def kl(x, y):
                mask = x > 0
                return float(np.sum(x[mask] * np.log2(x[mask] / np.clip(y[mask], 1e-12, None))))
            return 0.5 * kl(a, m) + 0.5 * kl(b, m)

        protos = {p: l2(per_emb[p]).mean(0) for p in present}
        for a, b in itertools.combinations(present, 2):
            pa, pb = l2(per_emb[a]), l2(per_emb[b])
            sa = pa[np.random.default_rng(1).choice(len(pa), min(200, len(pa)), replace=False)]
            sb = pb[np.random.default_rng(2).choice(len(pb), min(200, len(pb)), replace=False)]
            apd = float(1 - (sa @ sb.T).mean())
            prt = float(1 - np.dot(protos[a], protos[b]))
            j = jsd(sense_dist(a), sense_dist(b))
            change_rows.append(dict(word=word, comparison=f"{a} vs {b}",
                                    period_a=a, period_b=b,
                                    prt=prt, apd=apd, jsd=j,
                                    n_a=len(per_emb[a]), n_b=len(per_emb[b])))
        print(f"[diachronic] '{word}': {len(present)} periods, "
              f"{sum(len(per_emb[p]) for p in present)} occurrences", flush=True)
        # checkpoint after each word so a reaped run resumes where it left off
        pd.DataFrame(change_rows).to_csv(change_csv, index=False)
        pd.DataFrame(proto_rows).to_parquet(proto_pq, index=False)

    pd.DataFrame(change_rows).to_csv(
        C.DIACHRONIC_DIR / f"diachronic_change_K{K}.csv", index=False)
    pd.DataFrame(proto_rows).to_parquet(
        C.DIACHRONIC_DIR / f"diachronic_prototypes_K{K}.parquet", index=False)
    print(f"[diachronic] DONE  {len(change_rows)} comparisons over "
          f"{len(set(r['word'] for r in change_rows))} words", flush=True)


if __name__ == "__main__":
    main()
