"""
Hybrid search — combine literal KEYWORD search with MEANING (semantic) search.

Two engines, fused into one ranked list:

  LEXICAL  (BM25)      finds articles that literally contain your query words.
                       Great for exact terms ("suffrage", a name, a place).
  SEMANTIC (MiniLM)    finds articles whose MEANING matches your query, even
                       when they use different words ("enfranchisement",
                       "the ballot", "right to vote").

We rank the whole corpus by each engine, then merge the two rankings with
Reciprocal Rank Fusion (RRF):

    rrf(doc) = 1/(k + rank_lexical) + 1/(k + rank_semantic)      (k = 60)

RRF needs no score normalization (the two engines' scores aren't comparable),
is robust, and is the standard hybrid-search method. A document ranked high by
either engine surfaces; one ranked high by BOTH surfaces at the top.

Run (query is everything after the script name; quote it if it has punctuation):
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\hybrid_search.py women's right to vote
    set ADV_K=35  & ...hybrid_search.py polygamy persecution      # pick K
    set HS_TOPN=25 & ...hybrid_search.py temple work              # more results
    set HS_ALPHA=0.7 & ...                                        # see note below

Needs (already produced by the pipeline, for the chosen K):
    output_advanced_WE/embeddings/doc_emb_K{K}.parquet   (semantic vectors)
    output_linked/advanced_docs_K{K}.csv                 (full text + metadata)

Outputs:
    prints the ranked hits to the terminal, AND writes
    output_advanced_WE/search/hybrid_search_K{K}.csv      (for the .Rmd to show)
"""
from __future__ import annotations
import os, sys, re, math
from collections import Counter
import numpy as np
import pandas as pd
import config as C

RRF_K = int(os.environ.get("HS_RRF_K", "60"))     # RRF damping constant
TOPN  = int(os.environ.get("HS_TOPN", "15"))      # how many hits to show
# Optional weighted blend instead of pure RRF: 1.0 = semantic only,
# 0.0 = keyword only, 0.5 = even. Leave unset to use plain RRF.
ALPHA = os.environ.get("HS_ALPHA")

_TOKEN = re.compile(r"[a-z]+")


def tokenize(t: str):
    return _TOKEN.findall(str(t).lower())


def build_bm25(texts):
    """Precompute BM25 statistics over the corpus (Okapi BM25)."""
    docs = [tokenize(t) for t in texts]
    N = len(docs)
    dl = np.array([len(d) for d in docs], dtype=np.float64)
    avgdl = dl.mean() if N else 0.0
    df = Counter()
    for d in docs:
        for w in set(d):
            df[w] += 1
    idf = {w: math.log(1 + (N - n + 0.5) / (n + 0.5)) for w, n in df.items()}
    tfs = [Counter(d) for d in docs]
    return docs, tfs, dl, avgdl, idf


def bm25_scores(query, tfs, dl, avgdl, idf, k1=1.5, b=0.75):
    q = tokenize(query)
    scores = np.zeros(len(tfs), dtype=np.float64)
    for i, tf in enumerate(tfs):
        s = 0.0
        for w in q:
            f = tf.get(w, 0)
            if f:
                s += idf.get(w, 0.0) * (f * (k1 + 1)) / (f + k1 * (1 - b + b * dl[i] / avgdl))
        scores[i] = s
    return scores


def ranks_from_scores(scores):
    """Return 1-based rank for every doc (rank 1 = highest score)."""
    order = np.argsort(-scores, kind="stable")     # indices, best first
    rank = np.empty(len(scores), dtype=np.int64)
    rank[order] = np.arange(1, len(scores) + 1)
    return rank


def main():
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        sys.exit('Give a query, e.g.:  hybrid_search.py women right to vote')
    K = C.resolve_K(None)

    emb_path = C.EMB_DIR / f"doc_emb_K{K}.parquet"
    if not emb_path.exists():
        sys.exit(f"Missing {emb_path}. Run embed_documents.py first.")
    docs = pd.read_csv(C.docs_csv(K))
    emb = pd.read_parquet(emb_path).sort_values("row_index").reset_index(drop=True)
    docs = docs[docs["row_index"].isin(emb["row_index"])].sort_values("row_index").reset_index(drop=True)
    texts = docs["text"].astype(str).tolist()
    print(f"[hybrid] K={K}  query={query!r}  corpus={len(texts)} articles", flush=True)

    # --- semantic engine -----------------------------------------------------
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(C.DOC_MODEL)
    qv = model.encode([query], normalize_embeddings=True)[0].astype(np.float32)
    E = emb[[c for c in emb.columns if c.startswith("dim_")]].to_numpy(np.float32, copy=True)
    E /= np.clip(np.linalg.norm(E, axis=1, keepdims=True), 1e-9, None)
    sem = E @ qv                                   # cosine similarity, [-1, 1]

    # --- lexical engine ------------------------------------------------------
    _, tfs, dl, avgdl, idf = build_bm25(texts)
    lex = bm25_scores(query, tfs, dl, avgdl, idf)

    # --- fuse ----------------------------------------------------------------
    sem_rank = ranks_from_scores(sem)
    lex_rank = ranks_from_scores(lex)
    if ALPHA is None:
        fused = 1.0 / (RRF_K + sem_rank) + 1.0 / (RRF_K + lex_rank)
        method = f"RRF(k={RRF_K})"
    else:
        a = float(ALPHA)                           # blend normalized scores
        def norm(x):
            r = x.max() - x.min()
            return (x - x.min()) / r if r > 0 else np.zeros_like(x)
        fused = a * norm(sem) + (1 - a) * norm(lex)
        method = f"blend(alpha={a})"

    keyword_present = lex > 0
    out = docs.assign(
        sem_score=sem, lex_score=lex,
        sem_rank=sem_rank, lex_rank=lex_rank,
        keyword_hit=keyword_present, fused_score=fused,
    ).sort_values("fused_score", ascending=False).reset_index(drop=True)

    cols = ["fused_score", "year", "author", "title",
            "keyword_hit", "sem_rank", "lex_rank", "row_index"]
    cols = [c for c in cols if c in out.columns]
    top = out.head(TOPN)

    print(f"[hybrid] fusion = {method}   (keyword_hit = literal word match)\n", flush=True)
    for i, r in top.iterrows():
        tag = "kw+mean" if r["keyword_hit"] else "meaning"
        title = str(r.get("title", ""))[:70]
        yr = r.get("year", "")
        print(f"{i+1:2d}. [{tag:7s}] {yr}  sem#{int(r['sem_rank']):<5d} "
              f"lex#{int(r['lex_rank']):<5d}  {title}", flush=True)

    search_dir = C.ADV_DIR / "search"
    search_dir.mkdir(parents=True, exist_ok=True)
    keep = ["row_index", "year", "author", "title", "article_type",
            "fused_score", "sem_score", "lex_score",
            "sem_rank", "lex_rank", "keyword_hit", "text"]
    keep = [c for c in keep if c in out.columns]
    out_path = search_dir / f"hybrid_search_K{K}.csv"
    out.head(200)[keep].assign(query=query).to_csv(out_path, index=False)
    print(f"\n[hybrid] wrote top-200 to {out_path}", flush=True)


if __name__ == "__main__":
    main()
