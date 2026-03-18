# topic-modeling-inputs/

Single source of truth for topic modeling on the Relief Society Magazine (1914-1970).

## What's in here

| Path | Description |
|------|-------------|
| `corpus.jsonl` | 11,542 documents, one JSON per line — primary input for topic modeling |
| `manifest.csv` | Flat index: doc_id, file, path, volume, month, year, etype, title, author, word_count, source |
| `corpus_stats.json` | Summary statistics and breakdowns by type/volume |
| `articles/vol##/Month/*.txt` | Individual cleaned article text files (real copies, not symlinks) |
| `metadata/vol##_entries.json` | Per-volume metadata (57 files) |

## Coverage

All 57 volumes, 1914-1970. 11,542 documents totaling ~14.4M words.

- 7,327 articles
- 2,208 poems
- 880 fiction
- 699 lessons
- 428 editorials

## Where the data came from

This folder merges output from two separate extraction pipelines:

- **regex_and_llm** (vol4-12, vol29-57): Python regex extraction scripts with JSON metadata. 7,648 entries. Scripts live in `preprocessing/relief-society-mag/article-extraction/`.
- **R_RS_article_extracted** (vol1-3, vol13-28): Legacy extraction from an external R workflow. 3,894 entries. Metadata encoded in filenames. **No extraction script exists in this repo** — the `.txt` files in `processed/R_RS_article_extracted/` are the only surviving artifact.

## Reproducibility

This folder is the **most complete artifact in the project**. Re-running the full pipeline from `raw-data/` would NOT reproduce identical results because:

1. **Volumes 1-3, 13-28**: The original R extraction code is not in this repo. If `processed/R_RS_article_extracted/` is lost, these 19 volumes cannot be regenerated.
2. **OCR cleanup** (`raw-data → cleaned-data`): Uses OpenAI gpt-4o via `preprocessing/relief-society-mag/ocr-cleanup/ocr_cleanup.py`. LLM output is non-deterministic, so re-running produces different cleaned text. A local Ollama fallback exists but gives different results.
3. **regex_and_llm extraction** (`cleaned-data → processed`): This step IS deterministic and reproducible for vol4-12 and vol29-57.

## Pipeline stages

Each stage is an independent copy of the data (no symlinks or pointers between stages):

```
raw-data/                   Raw OCR text by volume/month (Vol01-Vol57)
    ↓  [OpenAI gpt-4o cleanup — non-deterministic]
cleaned-data/               OCR-cleaned text by volume/month (Vol01-Vol57)
    ↓  [regex extraction scripts — deterministic, vol4-12/29-57]
    ↓  [unknown R workflow — vol1-3/13-28, NOT reproducible]
processed/                  Extracted articles + JSON metadata
    ↓  [topic_modeling_prep.py — deterministic]
topic-modeling-inputs/      ← YOU ARE HERE
```

## Regenerating this folder

If you need to rebuild from `processed/`:

```
python preprocessing/topic_modeling_prep.py --clean
```

Options: `--min-words 100`, `--exclude-poems`, `--exclude-types poem editorial`.
