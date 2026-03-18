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

---

## Using this data for topic modeling

### The input file

`corpus.jsonl` is the primary input. Each line is a self-contained JSON document with a `text` field (the cleaned article content) and metadata fields (`volume`, `month`, `year`, `etype`, `title`, `author`). Most topic modeling libraries expect either raw text or a document-term matrix — this file gives you the raw text plus enough metadata to slice, filter, and interpret results.

The individual `articles/` text files are there for inspection, spot-checking, and workflows that prefer a file-per-document layout. They contain the same cleaned text as the `text` field in `corpus.jsonl`.

### What this data is and is not

**What's been done (you don't need to redo this):**
- OCR noise removal (page numbers, running headers, separator lines, garbage characters)
- Whitespace normalization
- Entry type normalization (article/poem/fiction/lesson/editorial)
- Minimum word count filtering (default: 50 words)

**What has NOT been done (you need to decide on these):**
- Tokenization
- Lowercasing
- Stopword removal
- Stemming or lemmatization
- N-gram construction
- TF-IDF or other vectorization
- Any document filtering beyond the 50-word minimum

These are intentionally left out because they are modeling decisions that depend on your choice of algorithm and research question.

### Typical next steps

#### 1. Load the corpus

```python
import json

docs = []
with open("topic-modeling-inputs/corpus.jsonl") as f:
    for line in f:
        docs.append(json.loads(line))
```

#### 2. Filter (optional but likely necessary)

You'll probably want to exclude some entry types. Poems and short pieces can muddy topic signals:

```python
# Articles and editorials only
docs = [d for d in docs if d["etype"] in ("article", "editorial", "lesson")]

# Or by time period
docs = [d for d in docs if 1930 <= d["year"] <= 1960]
```

#### 3. Tokenize and vectorize

This is where your modeling choices begin. Common approaches:

**For LDA/NMF (bag-of-words models):**
```python
# Requires: gensim, nltk or spacy
from gensim.utils import simple_preprocess
from gensim.corpora import Dictionary

texts = [simple_preprocess(d["text"], deacc=True) for d in docs]
dictionary = Dictionary(texts)
dictionary.filter_extremes(no_below=10, no_above=0.5)
bow_corpus = [dictionary.doc2bow(t) for t in texts]
```

**For BERTopic (transformer-based):**
```python
# Requires: bertopic, sentence-transformers
from bertopic import BERTopic

texts = [d["text"] for d in docs]
model = BERTopic()
topics, probs = model.fit_transform(texts)
```

**For scikit-learn NMF/LDA:**
```python
# Requires: scikit-learn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF

texts = [d["text"] for d in docs]
vectorizer = TfidfVectorizer(max_df=0.5, min_df=10, stop_words="english")
tfidf = vectorizer.fit_transform(texts)
model = NMF(n_components=20, random_state=42)
W = model.fit_transform(tfidf)
```

### Dependencies

Nothing in this folder requires any packages — it's plain text and JSON. Dependencies arise from your modeling choices:

| Approach | Packages | Notes |
|----------|----------|-------|
| LDA (Gensim) | `gensim`, `nltk` or `spacy` | Bag-of-words. Fast. Good baseline. Needs stopword list and tokenizer. |
| NMF (scikit-learn) | `scikit-learn` | TF-IDF based. Often produces more coherent topics than LDA on smaller corpora. |
| BERTopic | `bertopic`, `sentence-transformers`, `umap-learn`, `hdbscan` | Transformer embeddings. Best for semantic coherence. Heavier compute. GPU recommended for 11K docs. |
| Top2Vec | `top2vec` | Similar to BERTopic, fewer knobs. |
| STM (R) | `stm` (R package) | Structural Topic Model. Good if you want to model how topics vary by year or etype as covariates. `manifest.csv` has the metadata you'd feed as covariates. |

### Assumptions and implied intentions

This corpus was built with the following assumptions:

1. **Unit of analysis is the individual article/entry**, not the monthly issue or volume. Each document is one extracted piece (article, poem, editorial, etc.), not an entire issue.
2. **The corpus spans the full run of the magazine** (1914-1970). Topic models will reflect thematic change over 56 years of publication. If you're studying a narrower period, filter by `year`.
3. **Poems are included by default.** They represent 19% of the corpus and have very different vocabulary from prose. Most topic modeling studies of periodicals exclude them. Consider `--exclude-poems` on rebuild, or filter at load time.
4. **Fiction is included by default.** Same consideration — fictional narratives may cluster into their own topics or introduce noise into thematic analysis of non-fiction content.
5. **The 50-word minimum is low.** Short entries (photo captions, brief notices, lesson headers) can survive at 50 words. A threshold of 100-200 words is more typical for topic modeling. Filter at load time or rebuild with `--min-words 200`.
6. **No deduplication has been performed.** Recurring features (e.g., monthly lesson outlines, visiting teacher messages) may contain near-duplicate text across issues. This could over-represent certain topics.
7. **Author metadata is incomplete.** The R_RS pipeline (vol1-3, 13-28) extracts authors from filenames heuristically — some are wrong or missing. The regex_and_llm pipeline (vol4-12, 29-57) has more reliable author data from JSON metadata but still has gaps. Don't rely on the `author` field for analysis without spot-checking.
