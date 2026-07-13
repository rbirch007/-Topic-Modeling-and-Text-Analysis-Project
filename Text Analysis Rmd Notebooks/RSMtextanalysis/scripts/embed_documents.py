"""
Stage 1 — Document embeddings for the atlas + semantic search.

Reads advanced_docs_K{K}.csv, splits long articles into word-windows, embeds
every window with a sentence-transformer, and length-weighted mean-pools each
document down to a single vector. Resumable: writes a checkpoint every
CHECKPOINT_EVERY docs and skips already-embedded rows on restart.

Run (from a terminal):
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\embed_documents.py
    C:\\Users\\birch\\dh-env\\Scripts\\python.exe scripts\\embed_documents.py 35   # pin K
    set ADV_LIMIT=200 & ...embed_documents.py                                       # dev subset

Outputs (EMB_DIR):
    doc_emb_K{K}.parquet     row_index + dim_0..dim_{d-1}  (one vector per doc)
    doc_emb_K{K}.meta.json   model name, dim, n_docs, params
"""
from __future__ import annotations
import os, sys, json, time
import numpy as np
import pandas as pd
import config as C


def main():
    K = C.resolve_K(sys.argv[1] if len(sys.argv) > 1 else None)
    limit = os.environ.get("ADV_LIMIT")
    out_path = C.EMB_DIR / f"doc_emb_K{K}.parquet"
    ckpt_path = C.EMB_DIR / f"doc_emb_K{K}.ckpt.parquet"

    docs = pd.read_csv(C.docs_csv(K))
    if limit:
        docs = docs.head(int(limit)).copy()
    print(f"[embed] K={K}  docs={len(docs)}  model={C.DOC_MODEL}", flush=True)

    # Resume from checkpoint if present
    done = {}
    if ckpt_path.exists():
        prev = pd.read_parquet(ckpt_path)
        done = {int(r): prev.iloc[i, 1:].to_numpy(np.float32)
                for i, r in enumerate(prev["row_index"].to_numpy())}
        print(f"[embed] resuming: {len(done)} docs already embedded", flush=True)

    todo = docs[~docs["row_index"].isin(done.keys())].reset_index(drop=True)
    print(f"[embed] to do: {len(todo)} docs", flush=True)

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(C.DOC_MODEL)

    results = dict(done)  # row_index -> vector
    t0 = time.time()
    n_since_ckpt = 0

    # Build a flat list of (row_index, chunk_text, weight) then batch-encode.
    def flush_block(block_rows):
        """Embed all chunks for the docs in block_rows, pool, store in results."""
        flat_text, flat_owner, flat_w = [], [], []
        for ri, txt in block_rows:
            chunks = C.chunk_words(txt)
            for ch in chunks:
                flat_text.append(ch)
                flat_owner.append(ri)
                flat_w.append(max(len(ch.split()), 1))
        if not flat_text:
            return
        embs = model.encode(flat_text, batch_size=C.EMBED_BATCH,
                            show_progress_bar=False, normalize_embeddings=False)
        embs = np.asarray(embs, dtype=np.float32)
        owners = np.asarray(flat_owner)
        weights = np.asarray(flat_w, dtype=np.float32)
        for ri in np.unique(owners):
            m = owners == ri
            w = weights[m][:, None]
            pooled = (embs[m] * w).sum(0) / w.sum()
            results[int(ri)] = pooled.astype(np.float32)

    block = []
    for _, row in todo.iterrows():
        block.append((int(row["row_index"]), row["text"]))
        n_since_ckpt += 1
        if len(block) >= 64:
            flush_block(block); block = []
        if n_since_ckpt >= C.CHECKPOINT_EVERY:
            _save(results, ckpt_path)
            rate = len(results) / max(time.time() - t0, 1e-9)
            print(f"[embed] checkpoint: {len(results)}/{len(docs)} "
                  f"({rate:.1f} docs/s)", flush=True)
            n_since_ckpt = 0
    if block:
        flush_block(block)

    _save(results, out_path)
    if ckpt_path.exists():
        ckpt_path.unlink()
    dim = len(next(iter(results.values())))
    meta = dict(model=C.DOC_MODEL, dim=int(dim), n_docs=len(results), K=K,
                chunk_words=C.CHUNK_WORDS, chunk_stride=C.CHUNK_STRIDE)
    (C.EMB_DIR / f"doc_emb_K{K}.meta.json").write_text(json.dumps(meta, indent=2))
    print(f"[embed] DONE  wrote {out_path}  ({len(results)} docs x {dim} dim)",
          flush=True)


def _save(results: dict, path):
    rows = sorted(results.keys())
    mat = np.vstack([results[r] for r in rows])
    df = pd.DataFrame(mat, columns=[f"dim_{i}" for i in range(mat.shape[1])])
    df.insert(0, "row_index", rows)
    df.to_parquet(path, index=False)


if __name__ == "__main__":
    main()
