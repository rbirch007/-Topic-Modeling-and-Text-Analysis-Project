"""
Cross-corpus semantic contrast — Woman's Exponent vs. Relief Society Magazine.

Where semantic_change.py asks "did a word's meaning move across TIME," this asks
"do the two PERIODICALS occupy different regions of meaning-space, and do shared
words mean different things in each." Two analyses, one script:

PART A — embedding-space geometry (MiniLM doc embeddings, from embed_documents.py)
    centroid distance   1 - cos(mean_WE, mean_RSM)        how far apart, overall
    overlap (AUC)       cross-validated logistic-regression AUC predicting the
                        publication from a doc's embedding. 0.5 = indistinguishable
                        (total overlap); 1.0 = perfectly separable (no overlap).
    kNN mixing          for each doc, the fraction of its nearest neighbours that
                        belong to the OTHER corpus. ~0.5 = interleaved; ~0 = each
                        corpus sits in its own neighbourhood.

PART B — contrastive semantic change of shared terms (MacBERTh, group by corpus)
    For each shared target word, embed every occurrence IN CONTEXT in each corpus,
    then compare the two publications with the same three measures as the
    diachronic script (SemEval-2020 Task 1), but A/B = the two periodicals:
        PRT  1 - cos(proto_WE, proto_RSM)        coarse framing drift
        APD  mean cross-corpus pairwise distance  spread-sensitive
        JSD  Jensen-Shannon over induced senses   does the SENSE MIX differ

Run (needs the COMBINED-corpus handoff + doc embeddings already built):
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\contrast.py
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\contrast.py 30
    set ADV_WORDS=relief,suffrage & ...contrast.py        # subset for dev

Outputs (CONTRAST_DIR):
    embedding_overlap_K{K}.csv     one row: centroid_dist, auc, n_a, n_b, ...
    knn_mixing_K{K}.csv            row_index, corpus, other_frac   (for R plots)
    term_contrast_K{K}.csv         word, prt, apd, jsd, n_a, n_b   (for R plots)
    term_sense_mix_K{K}.csv        word, corpus, sense, proportion, n, sense_label,
                                   term_dominant  (sense_label = DISTINCTIVE words
                                   that peak in the sense, '^' = a high-freq floater
                                   concentrated here; term_dominant = the term's
                                   overall top words / 'floaters')
    term_floater_lift_K{K}.csv     word, floater, sense, lift, n_in_sense, n_total
                                   (lift = observed/expected concentration; >1 = the
                                   floater leans to that sense, ~1 = background)
    term_prototypes_K{K}.parquet   word, corpus, n, dim_0..767     (for R drift PCA)
    contrast_meta_K{K}.json        the two corpus labels used
"""
from __future__ import annotations
import os, sys, re, json, itertools
import numpy as np
import pandas as pd
import config as C

_NONLETTER = re.compile(r"[^a-z]")

# Function-word stoplist used only to GLOSS senses/terms (§7b sense-mix bars).
_STOP = set("""the of and to a in that is was for it with as his on be at by this
had not are but from or have an they which one you were her all she there would
their we him been has when who will more no if out so said what up its into than
them can only some could these two may then do any my now such like our over me
even most made after also did many before must through back where much your way
well down should because each just those how too very make see being long here
between both under never same another know while last might us great old year off
come since against go came right used take three few during without again place
around however home found thought went say part once high every does got left
number course though less almost hand enough far took head yet set told end why
called find going look asked later knew point next ever least given became week
gave upon shall unto thee thy thou let their your ing con""".split())

# Small irregular-plural map; the rest is a light suffix rule. We normalise so
# morphological variants (woman/women, sister/sisters) collapse to ONE label, and
# mark a collapsed label with "*" (woman*, man*) so the reader sees it aggregates.
_IRREGULAR = {"women": "woman", "men": "man", "children": "child", "wives": "wife",
              "lives": "life", "leaves": "leaf", "feet": "foot", "teeth": "tooth",
              "people": "person", "brethren": "brother"}


def _norm(w: str) -> str:
    """Crude lemmatiser: collapse a token to a display root (no heavy deps)."""
    if w in _IRREGULAR:
        return _IRREGULAR[w]
    if len(w) > 4 and w.endswith("ies"):
        return w[:-3] + "y"
    if w.endswith(("sses", "ches", "shes", "xes", "zes", "ses")):
        return w[:-2]
    if len(w) > 3 and w.endswith("s") and not w.endswith("ss"):
        return w[:-1]
    return w


def _content_keys(ctx: str, target_key: str):
    """Yield (root, surface) content tokens of a context, minus stopwords/target."""
    for w in re.findall(r"[a-z]{3,}", str(ctx).lower()):
        if w in _STOP:
            continue
        k = _norm(w)
        if k == target_key or k in _STOP:
            continue
        yield k, w


def _disp(key: str, forms: set) -> str:
    """Render a root, suffixed with '*' when it aggregates >1 form / a plural-only."""
    return key + ("*" if forms != {key} else "")


def distinctive_terms(sense_ctr, rest_ctr, topn=3):
    """Keys that PEAK in this sense vs the term's other senses (within-word log-odds).

    Frequency is NOT a filter: every word competes, and the informative prior shrinks
    rare-word noise — so a high-frequency word (woman/man) ranks here whenever it is
    genuinely over-represented, and is merely out-competed when it is near-uniform.
    """
    import math
    keys = set(sense_ctr) | set(rest_ctr)
    Ns, Nr, V, a = sum(sense_ctr.values()), sum(rest_ctr.values()), max(len(keys), 1), 0.5
    scored = []
    for k in keys:
        s = sense_ctr.get(k, 0)
        if s < 3:                          # must peak meaningfully in this sense
            continue
        r = rest_ctr.get(k, 0)
        if s + r < 8:                      # and recur across the term's contexts —
            continue                       # drops one-off names / OCR fragments
        lo = (math.log((s + a) / (Ns + a * V - s - a))
              - math.log((r + a) / (Nr + a * V - r - a)))
        scored.append((lo, s, k))
    scored.sort(reverse=True)
    return [k for _, _, k in scored[:topn]]


def dominant_label(all_ctr, forms_map, topn=4):
    """The term's overall most frequent content words (the 'floaters', e.g. woman*)."""
    return ", ".join(_disp(k, forms_map.get(k, {k}))
                     for k, _ in all_ctr.most_common(topn))


def resolve_corpora(docs: pd.DataFrame):
    """Return the two corpus labels (A, B) and validate there are exactly two."""
    if C.CORPUS_COL not in docs.columns:
        sys.exit(f"docs CSV has no '{C.CORPUS_COL}' column — was the handoff written "
                 "by regression_compare_2_corpora.Rmd §27b (which tags each row)?")
    vals = [v for v in pd.unique(docs[C.CORPUS_COL].dropna())]
    if C.CORPUS_ORDER:
        vals = [v for v in C.CORPUS_ORDER if v in vals]
    if len(vals) != 2:
        sys.exit(f"Expected exactly 2 corpora in '{C.CORPUS_COL}', found {vals}.")
    return vals[0], vals[1]


def pretty(label: str) -> str:
    return C.CORPUS_PRETTY.get(label, label)


def l2(m):
    n = np.linalg.norm(m, axis=1, keepdims=True)
    return m / np.clip(n, 1e-9, None)


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


# --- PART A: embedding-space geometry ---------------------------------------
def part_a(docs, A, B, K):
    emb_path = C.EMB_DIR / f"doc_emb_K{K}.parquet"
    if not emb_path.exists():
        print(f"[contrast] PART A skipped — missing {emb_path} "
              f"(run embed_documents.py {K} first).", flush=True)
        return
    emb = pd.read_parquet(emb_path).sort_values("row_index").reset_index(drop=True)
    d = docs[docs["row_index"].isin(emb["row_index"])].sort_values("row_index").reset_index(drop=True)
    dims = [c for c in emb.columns if c.startswith("dim_")]
    X = l2(emb[dims].to_numpy(np.float32, copy=True))
    corpus = d[C.CORPUS_COL].to_numpy()
    ia, ib = corpus == A, corpus == B
    na, nb = int(ia.sum()), int(ib.sum())
    print(f"[contrast] PART A  {pretty(A)}={na}  {pretty(B)}={nb}", flush=True)

    # centroid (mean direction) distance
    ca, cb = l2(X[ia].mean(0, keepdims=True))[0], l2(X[ib].mean(0, keepdims=True))[0]
    centroid_dist = float(1 - np.dot(ca, cb))

    # overlap classifier: cross-validated AUC predicting corpus from embedding
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import cross_val_predict
    from sklearn.metrics import roc_auc_score
    rng = np.random.default_rng(12345)
    cap = C.OVERLAP_MAX_PER_CORPUS
    def sample(mask):
        idx = np.where(mask)[0]
        if cap and len(idx) > cap:
            idx = rng.choice(idx, cap, replace=False)
        return idx
    sel = np.concatenate([sample(ia), sample(ib)])
    Xs, ys = X[sel], (corpus[sel] == B).astype(int)
    try:
        proba = cross_val_predict(
            LogisticRegression(max_iter=2000, C=1.0), Xs, ys,
            cv=5, method="predict_proba")[:, 1]
        auc = float(roc_auc_score(ys, proba))
    except Exception as e:                                  # tiny corpus etc.
        print(f"[contrast] AUC failed ({e}); setting NaN", flush=True)
        auc = float("nan")

    # kNN mixing: fraction of each doc's neighbours in the OTHER corpus
    from sklearn.neighbors import NearestNeighbors
    kk = min(C.KNN_NEIGHBORS + 1, len(X))
    nn = NearestNeighbors(n_neighbors=kk, metric="cosine").fit(X)
    _, nbr = nn.kneighbors(X)
    nbr = nbr[:, 1:]                                        # drop self
    same = (corpus[nbr] == corpus[:, None])
    other_frac = 1.0 - same.mean(axis=1)
    pd.DataFrame({"row_index": d["row_index"].to_numpy(),
                  "corpus": corpus, "other_frac": other_frac}).to_csv(
        C.CONTRAST_DIR / f"knn_mixing_K{K}.csv", index=False)

    pd.DataFrame([dict(
        corpus_a=A, corpus_b=B, n_a=na, n_b=nb,
        centroid_dist=centroid_dist, overlap_auc=auc,
        knn_mixing_a=float(other_frac[ia].mean()),
        knn_mixing_b=float(other_frac[ib].mean()),
        knn_neighbors=C.KNN_NEIGHBORS,
    )]).to_csv(C.CONTRAST_DIR / f"embedding_overlap_K{K}.csv", index=False)
    print(f"[contrast] centroid_dist={centroid_dist:.4f}  overlap_AUC={auc:.3f}  "
          f"kNN-other {pretty(A)}={other_frac[ia].mean():.2f} "
          f"{pretty(B)}={other_frac[ib].mean():.2f}", flush=True)


# --- PART B: contrastive semantic change of shared terms --------------------
def part_b(docs, A, B, K, targets):
    import torch
    from transformers import AutoTokenizer, AutoModel
    tok = AutoTokenizer.from_pretrained(C.HIST_MODEL, use_fast=True)
    model = AutoModel.from_pretrained(C.HIST_MODEL)
    model.eval()
    torch.set_grad_enabled(False)

    def embed_targets(contexts, spans):
        vecs = []
        for i in range(0, len(contexts), C.HIST_BATCH):
            cb = contexts[i : i + C.HIST_BATCH]
            sb = spans[i : i + C.HIST_BATCH]
            enc = tok(cb, return_offsets_mapping=True, truncation=True,
                      max_length=C.HIST_MAXLEN, padding=True, return_tensors="pt")
            offs = enc.pop("offset_mapping")
            hid = model(**enc).last_hidden_state
            for b in range(len(cb)):
                cs, ce = sb[b]
                sel = [(t_s < ce and t_e > cs and not (t_s == 0 and t_e == 0))
                       for (t_s, t_e) in offs[b].tolist()]
                idx = [j for j, keep in enumerate(sel) if keep]
                if not idx:
                    vecs.append(None); continue
                vecs.append(hid[b, idx].mean(0).numpy().astype(np.float32))
        return vecs

    def occ_for(word, corpus_label):
        sub = docs[docs[C.CORPUS_COL] == corpus_label]
        ctxs, spans = [], []
        for txt in sub["text"]:
            for (c, cs, ce) in find_contexts(txt, word):
                ctxs.append(c); spans.append((cs, ce))
                if len(ctxs) >= C.MAX_OCCUR_PER_CORPUS:
                    break
            if len(ctxs) >= C.MAX_OCCUR_PER_CORPUS:
                break
        if not ctxs:
            return None
        vecs = embed_targets(ctxs, spans)
        kept = [(vecs[i], ctxs[i]) for i in range(len(vecs)) if vecs[i] is not None]
        if len(kept) < 5:
            return None
        return np.vstack([k[0] for k in kept]), [k[1] for k in kept]

    def jsd(a, b):
        m = 0.5 * (a + b)
        def kl(x, y):
            mask = x > 0
            return float(np.sum(x[mask] * np.log2(x[mask] / np.clip(y[mask], 1e-12, None))))
        return 0.5 * kl(a, m) + 0.5 * kl(b, m)

    change_rows, proto_rows, sense_rows, floater_rows = [], [], [], []
    for word in targets:
        ra, rb = occ_for(word, A), occ_for(word, B)
        if ra is None or rb is None:
            print(f"[contrast] '{word}': <5 usable occurrences in one corpus, skipping",
                  flush=True)
            continue
        ea, ctx_a = ra
        eb, ctx_b = rb
        pa, pb = l2(ea), l2(eb)
        proto_a, proto_b = pa.mean(0), pb.mean(0)
        for lbl, e, proto in ((A, ea, proto_a), (B, eb, proto_b)):
            proto_rows.append(dict(word=word, corpus=lbl, n=len(e),
                                   **{f"dim_{j}": float(proto[j])
                                      for j in range(len(proto))}))

        # induced senses pooled across BOTH corpora, then compare the mixes
        from sklearn.cluster import KMeans
        allv = np.vstack([pa, pb])
        owner = np.array([A] * len(pa) + [B] * len(pb))
        k_sense = min(C.N_SENSES, max(2, allv.shape[0] // 30))
        labels = KMeans(n_clusters=k_sense, random_state=12345, n_init=5).fit(allv).labels_

        def sense_dist(lbl):
            lab = labels[owner == lbl]
            d = np.bincount(lab, minlength=k_sense).astype(float)
            return d / d.sum()

        # Build normalised content-word counts per sense (pooled across corpora),
        # a surface-form map for the '*' display, and an overall counter. The
        # sense gloss = words that PEAK in that sense (distinctive, not just
        # frequent); term_dominant = the term's overall top words (the floaters).
        from collections import Counter
        all_ctx = ctx_a + ctx_b
        target_key = _norm(word)
        per_sense = [Counter() for _ in range(k_sense)]
        overall = Counter()
        forms_map: dict[str, set] = {}
        for i, ctx in enumerate(all_ctx):
            s = int(labels[i])
            for k, surf in _content_keys(ctx, target_key):
                per_sense[s][k] += 1
                overall[k] += 1
                forms_map.setdefault(k, set()).add(surf)
        term_dominant = dominant_label(overall, forms_map)

        # Lift = observed/expected concentration of a word in a sense. It lets a
        # high-frequency "floater" (woman/man) enter a sense gloss ONLY where it is
        # measurably over-represented, and gives every floater a measured profile.
        grand = sum(overall.values())
        sense_tot = [sum(per_sense[s].values()) for s in range(k_sense)]
        base = [sense_tot[s] / grand if grand else 0.0 for s in range(k_sense)]
        floaters = [k for k, _ in overall.most_common(6)]   # candidate big words
        LIFT_BAR = 1.3

        def lift(k, s):
            if base[s] <= 0 or overall.get(k, 0) == 0:
                return 0.0
            return (per_sense[s].get(k, 0) / overall[k]) / base[s]

        def gloss_for(s):
            keys = distinctive_terms(per_sense[s], overall - per_sense[s])
            have = set(keys)
            parts = [_disp(k, forms_map.get(k, {k})) for k in keys]
            for f in floaters:                              # earned-floater inclusion
                if f in have or per_sense[s].get(f, 0) < 3:
                    continue
                if lift(f, s) >= LIFT_BAR:                   # '^' = big word, concentrated here
                    parts.append(_disp(f, forms_map.get(f, {f})) + "^")
            return ", ".join(parts)

        sense_gloss = {s: gloss_for(s) for s in range(k_sense)}

        # record each floater's per-sense lift (for the §7b distinctiveness readout)
        for f in floaters:
            for s in range(k_sense):
                floater_rows.append(dict(
                    word=word, floater=_disp(f, forms_map.get(f, {f})),
                    sense=int(s), lift=round(lift(f, s), 3),
                    n_in_sense=int(per_sense[s].get(f, 0)),
                    n_total=int(overall.get(f, 0))))

        for lbl in (A, B):
            lab_l = labels[owner == lbl]
            cnt = np.bincount(lab_l, minlength=k_sense).astype(int)
            tot = int(cnt.sum())
            for s in range(k_sense):
                sense_rows.append(dict(
                    word=word, corpus=lbl, sense=int(s),
                    proportion=float(cnt[s] / tot) if tot else 0.0,
                    n=int(cnt[s]), sense_label=sense_gloss[s],
                    term_dominant=term_dominant))

        sa = pa[np.random.default_rng(1).choice(len(pa), min(200, len(pa)), replace=False)]
        sb = pb[np.random.default_rng(2).choice(len(pb), min(200, len(pb)), replace=False)]
        change_rows.append(dict(
            word=word, corpus_a=A, corpus_b=B,
            prt=float(1 - np.dot(proto_a, proto_b)),
            apd=float(1 - (sa @ sb.T).mean()),
            jsd=jsd(sense_dist(A), sense_dist(B)),
            n_a=len(ea), n_b=len(eb)))
        print(f"[contrast] '{word}': {len(ea)} {pretty(A)} / {len(eb)} {pretty(B)} "
              f"occurrences", flush=True)

    pd.DataFrame(change_rows).to_csv(
        C.CONTRAST_DIR / f"term_contrast_K{K}.csv", index=False)
    if sense_rows:
        pd.DataFrame(sense_rows).to_csv(
            C.CONTRAST_DIR / f"term_sense_mix_K{K}.csv", index=False)
    if floater_rows:
        pd.DataFrame(floater_rows).to_csv(
            C.CONTRAST_DIR / f"term_floater_lift_K{K}.csv", index=False)
    if proto_rows:
        pd.DataFrame(proto_rows).to_parquet(
            C.CONTRAST_DIR / f"term_prototypes_K{K}.parquet", index=False)
    print(f"[contrast] PART B DONE — {len(change_rows)} shared terms contrasted",
          flush=True)


def main():
    K = C.resolve_K(sys.argv[1] if len(sys.argv) > 1 else None)
    docs = pd.read_csv(C.docs_csv(K))
    A, B = resolve_corpora(docs)
    (C.CONTRAST_DIR / f"contrast_meta_K{K}.json").write_text(json.dumps(
        {"K": K, "corpus_a": A, "corpus_b": B,
         "corpus_a_pretty": pretty(A), "corpus_b_pretty": pretty(B)}, indent=2))
    print(f"[contrast] K={K}  A={pretty(A)!r}  B={pretty(B)!r}  "
          f"corpus={len(docs)} articles", flush=True)

    targets = C.CONTRAST_TARGETS
    if os.environ.get("ADV_WORDS"):
        targets = [w.strip() for w in os.environ["ADV_WORDS"].split(",") if w.strip()]

    part_a(docs, A, B, K)
    part_b(docs, A, B, K, targets)


if __name__ == "__main__":
    main()
