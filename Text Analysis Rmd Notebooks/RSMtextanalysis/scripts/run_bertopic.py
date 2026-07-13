"""
Stage 2 — BERTopic neural topic model (a cross-check on STM).

Reuses the document embeddings from stage 1 (no re-embedding), clusters them
with BERTopic, and writes the cluster assignments + topic words. The .Rmd then
crosstabs BERTopic clusters against STM topics (via advanced_theta) to show
where the two methods agree and where they diverge — triangulation, not
replacement.

Run:
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\run_bertopic.py
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\run_bertopic.py 35

Outputs (BERTOPIC_DIR):
    bertopic_docs_K{K}.csv     row_index, bertopic_topic, bertopic_prob
    bertopic_topics_K{K}.csv   topic, count, name, top words
"""
from __future__ import annotations
import sys
import numpy as np
import pandas as pd
import config as C

MIN_TOPIC_SIZE = 30   # raise for fewer/larger clusters; lower for finer ones


def main():
    K = C.resolve_K(sys.argv[1] if len(sys.argv) > 1 else None)
    emb_path = C.EMB_DIR / f"doc_emb_K{K}.parquet"
    if not emb_path.exists():
        sys.exit(f"Missing {emb_path}. Run embed_documents.py first.")

    docs = pd.read_csv(C.docs_csv(K))
    emb = pd.read_parquet(emb_path).sort_values("row_index").reset_index(drop=True)
    # Align docs to the embedded rows (embeddings may be a dev subset)
    docs = docs[docs["row_index"].isin(emb["row_index"])].copy()
    docs = docs.sort_values("row_index").reset_index(drop=True)
    X = emb[[c for c in emb.columns if c.startswith("dim_")]].to_numpy(np.float32)
    texts = docs["text"].astype(str).tolist()
    print(f"[bertopic] K={K}  docs={len(texts)}  emb_dim={X.shape[1]}", flush=True)

    from bertopic import BERTopic
    from sklearn.feature_extraction.text import CountVectorizer
    from umap import UMAP
    from hdbscan import HDBSCAN

    # Deterministic UMAP; CountVectorizer strips English stopwords + rare terms.
    # NOTE: BERTopic runs this vectorizer over the *per-topic* concatenated docs
    # (one row per cluster), so a large absolute min_df can exceed the cluster
    # count and raise "max_df < min_df". min_df=2 is safe and still drops hapaxes.
    vectorizer = CountVectorizer(stop_words="english", min_df=2, ngram_range=(1, 2))
    umap_model = UMAP(n_neighbors=15, n_components=5, min_dist=0.0,
                      metric="cosine", random_state=12345)
    hdbscan_model = HDBSCAN(min_cluster_size=MIN_TOPIC_SIZE,
                            metric="euclidean", prediction_data=True)

    topic_model = BERTopic(
        embedding_model=None,            # we pass precomputed embeddings
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer,
        calculate_probabilities=False,
        verbose=True,
    )
    topics, probs = topic_model.fit_transform(texts, embeddings=X)

    info = topic_model.get_topic_info()
    info.to_csv(C.BERTOPIC_DIR / f"bertopic_topics_K{K}.csv", index=False)

    out = pd.DataFrame({
        "row_index": docs["row_index"].to_numpy(),
        "bertopic_topic": topics,
        "bertopic_prob": (probs if probs is not None else np.nan),
    })
    out.to_csv(C.BERTOPIC_DIR / f"bertopic_docs_K{K}.csv", index=False)

    n_topics = (info["Topic"] >= 0).sum()
    n_outlier = int((out["bertopic_topic"] == -1).sum())
    print(f"[bertopic] DONE  {n_topics} topics  | outliers: {n_outlier}/{len(out)}",
          flush=True)


if __name__ == "__main__":
    main()
