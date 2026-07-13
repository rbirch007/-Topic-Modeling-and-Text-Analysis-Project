# Session Handover - RSMadvancedtextanalysis (hybrid R + Python pipeline)

Reference document for the advanced text-analysis pipeline built for the
Relief Society Magazine (RSM) corpus. Read this if you (or a future assistant)
need to run, rebuild, extend, or port the work. It is written to be usable by
someone who is **comfortable in R but new to Python** — the Python parts are
spelled out step by step.

This is the RSM sibling of the Woman's Exponent (WE) advanced pipeline. The
Python scripts are identical to the WE ones; only `scripts/config.py` (corpus
paths, output dir, diachronic periods/targets) and `scripts/generate_handoff.R`
(BASE) differ. If you know the WE project, you already know this one.

---

## The big idea: why there are two layers

An all-in-one notebook that trains embeddings, clusters, and aligns spaces
**inside the R knit** is exactly what times out: the heavy machine-learning work
and the report are welded together, so every run re-does the slow part.

The pipeline is split in two:

```
  ┌─────────────────────────────┐        ┌──────────────────────────────┐
  │  PYTHON COMPUTE LAYER        │  artifacts  │  R REPORTING LAYER       │
  │  (terminal, runs once,       │  ──────▶ │  RSMadvancedtextanalysis.Rmd │
  │   checkpointed, resumable)   │  .parquet│  (knits fast, just plots)    │
  │                              │  .csv    │                              │
  │  embed_documents.py          │          │  loads artifacts, makes      │
  │  run_bertopic.py             │          │  figures + tables            │
  │  semantic_change.py          │          │                              │
  └─────────────────────────────┘        └──────────────────────────────┘
```

**The R notebook never trains anything.** It reads finished files and plots
them. If an artifact is missing, that section prints a "run script X first" note
and skips — so the notebook always knits, even before the compute layer has run.
This makes the whole thing timeout-proof.

A third hand-off feeds both: the STM linker
(`Topic_to_Article_Linker_RSM.Rmd`) writes `advanced_*_K{K}.*` files that *both*
layers consume.

---

## The six analyses

| # | Analysis | Where it runs | Notes |
|---|---|---|---|
| 1 | **Topic geometry** (JSD dendrogram, MDS quadrants, co-occurrence network) | R (fast) | K-agnostic; reads the saved model, doesn't refit |
| 2 | **Document atlas + semantic search** | Python embeds, R plots | Transformer embeddings (all-MiniLM); meaning-based search |
| 3 | **Hybrid search** (keyword + meaning) | Python (`hybrid_search.py`), R shows table | BM25 keyword + MiniLM semantic, fused with Reciprocal Rank Fusion; free-text queries |
| 4 | **BERTopic cross-check** | Python clusters, R cross-tabs | neural topic model triangulated against STM |
| 5 | **Diachronic semantic change** | Python (MacBERTh), R plots | in-context contextual embeddings (PRT/APD/JSD) |
| 6 | **Stylometry** (Burrows' Delta) | R (`stylo`) | field-standard `stylo` bootstrap consensus tree |

**The frontier piece is #5.** Instead of training one word2vec per period and
rotating spaces with Procrustes (fragile — small periods give noisy spaces, the
rotation is fit on shared vocabulary), we embed every occurrence of a word **in
its sentence context** using MacBERTh (a BERT pretrained on 1500–1950 English),
all in one pretrained space. No rotation needed, and we recover genuine
**sense-level** change (how the *mix of senses* shifts), not just which neighbors
reshuffle. This is the SemEval-2020 Task 1 methodology — current best practice
for lexical semantic-change measurement. MacBERTh's 1500–1950 training window
covers most of RSM's 1914/15–1970 run.

---

## K is not hardcoded

Everything is suffixed by K (e.g., `advanced_docs_K20.csv`, `doc_emb_K20.parquet`).
- The R notebook picks K via `pin_K` in §2 (set `NULL` to auto-pick the newest).
- The Python scripts take K as a CLI argument: `... semantic_change.py 20`.
- Dev artifacts and production artifacts at different K never collide.

**Unlike WE (whose canonical model is K=35), RSM has no fixed canonical K** — use
whatever K you actually fit in the RSM linker. The examples below use `20` purely
as an illustration; substitute your K everywhere. To switch K: (1) drop the new
`topic_linker_session_K{K}.RData` into `output_linked/` and run
`generate_handoff.R {K}`, (2) re-run the three Python scripts with that K, (3) set
`pin_K` and re-knit.

---

## One-time setup

### R packages

```r
install.packages(c(
  "stm", "tidyverse", "scales", "ggrepel", "patchwork",
  "arrow",         # reads the Parquet files the Python layer writes
  "philentropy", "ggdendro", "dendextend", "ggraph", "tidygraph", "igraph",
  "uwot",          # UMAP layout
  "stylo",         # Burrows' Delta + bootstrap consensus tree
  "jsonlite"
))
```

### Python environment (the part that's new for you)

We do **not** use the system Python. We use a small, isolated Python 3.11
installed just for this project, at `C:\Users\birch\dh-env` — shared with the WE
pipeline. Two reasons: the system Python (3.14) is too new — the ML libraries
don't ship installers for it yet; and keeping the environment *outside* OneDrive
stops OneDrive from constantly syncing thousands of library files.

**You do not need to "learn Python" to run this.** You run three commands in a
terminal and read the progress lines they print. That's it. Mental model:

- A *Python environment* is just a folder containing a Python interpreter plus
  installed libraries. `C:\Users\birch\dh-env` is ours.
- To run a script with that environment, you call its python.exe by full path
  and hand it the script. No "activation" needed when you use the full path.
- The scripts print a line every few hundred documents so you can watch progress.
  They save a checkpoint as they go, so if one stops you just run it again and it
  resumes.

The environment is already built (the same one the WE project uses). If it ever
needs rebuilding from scratch:

```powershell
# Install uv (a fast Python installer/manager) once, then:
uv python install 3.11
uv venv "C:\Users\birch\dh-env" --python 3.11
& "C:\Users\birch\dh-env\Scripts\python.exe" -m pip install `
    torch transformers sentence-transformers bertopic `
    umap-learn hdbscan scikit-learn pandas pyarrow numpy
```

---

## How to run the whole thing, start to finish

### Step 1 — Produce the STM hand-off (in R)

The topic linker is `Topic_to_Article_Linker_RSM.Rmd`. Its set-paths chunk is
**environment-aware**: on the Linux server it uses the original `/home/rbirch/...`
locations; on Windows it reads the static corpus + inventory from OneDrive but
writes **all** output — including `topic_linker_session_K{K}.RData` AND the four
`advanced_*_K{K}.*` hand-off files — straight into this project's `output_linked/`.

Whatever produces the hand-off, these four files are what the rest of the pipeline
consumes:

| File | Contents |
|---|---|
| `advanced_input_K{K}.rds` | list: `stm_model`, `meta`, `topic_labels`, `modeled_raw`, `modeled_clean`, `fit_date` |
| `advanced_docs_K{K}.csv` | one row per modeled doc (row_index, doc_id, year, author, title, text, …) |
| `advanced_theta_K{K}.csv` | D×K document-topic matrix, same row order |
| `advanced_topics_K{K}.csv` | topic number, label, FREX/PROB words |

There are two ways to get them, depending on where you fit the RSM model:

**Option A — run the linker on Windows (no copy step).** Open
`Topic_to_Article_Linker_RSM.Rmd`, confirm `chosen_K`, and Run All / knit.
Because of the OS-aware paths, it deposits the `.RData` and all four `advanced_*`
files directly into `output_linked/`. You're done with Step 1 — skip straight to
Step 2. (This is the heavy R job — searchK + fitting takes a while.)

**Option B — run the linker on the server, bring one file over.** Run it on Linux
as usual; it saves `topic_linker_session_K{K}.RData` (and the `advanced_*` files)
into the server output folder. Copy `topic_linker_session_K{K}.RData` into this
project's `output_linked/`, then run the bridge script:

```powershell
& "C:\Program Files\R\R-4.5.3\bin\Rscript.exe" scripts\generate_handoff.R 20
```

`generate_handoff.R` reads the `.RData`, re-aligns each modeled document to its
source text, and writes the same four files. (If you instead copied the four
`advanced_*_K{K}.*` files straight off the server, you don't need this script at
all — go to Step 2.) K comes from the model object itself; pass a number to pick a
specific `.RData`, or omit it to use the newest session file in `output_linked/`.

### Step 2 — Run the Python scripts (in a terminal)

Open PowerShell, then run these one at a time (the trailing `20` pins K — use your K):

```powershell
cd "C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_RSM"
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\embed_documents.py 20
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\run_bertopic.py    20
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\semantic_change.py 20
```

What to expect:
- **embed_documents.py** is the long one (embeds every article; tens of minutes on
  CPU). It prints `checkpoint: N/total` lines and saves a `.ckpt.parquet` as it
  goes. If it stops, run the same command again — it resumes from the checkpoint.
- **run_bertopic.py** reuses those embeddings (no re-embedding) and is quick.
- **semantic_change.py** downloads MacBERTh on first run, then embeds word
  occurrences in context. Minutes, not hours.

Dev shortcuts (optional): `set ADV_LIMIT=200` before embed to test on 200 docs;
`set ADV_WORDS=relief,sister` before semantic_change to test on two words.

**Hybrid search is on-demand** (run it whenever you have a query — it needs only
the embeddings from `embed_documents.py`, so you can run it any time after that):

```powershell
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\hybrid_search.py relief society welfare
```

It fuses BM25 keyword search with MiniLM semantic search (Reciprocal Rank Fusion),
prints the ranked hits, and writes `output_advanced_RSM\search\hybrid_search_K{K}.csv`,
which §9b of the report renders as a searchable table (search box scans full body
text). Knobs: `set HS_TOPN=25` (more rows), `set ADV_K=20` (pick the model),
`set HS_ALPHA=0.7` (favor meaning) / `0.3` (favor keywords) instead of pure RRF.
Each `sem#` / `lex#` column tells you whether a hit came from meaning, keywords,
or both.

### Step 3 — Knit the report (in R)

Open `RSMadvancedtextanalysis.Rmd`, confirm `pin_K` in §2 (set it to your K, or
`NULL` to auto-pick the newest), and knit (or Run All). It loads the hand-off and
the Python artifacts and produces all figures in
`output_advanced_RSM/report_figures/`. Any section whose artifact is missing
prints a skip note instead of erroring.

---

## What each Python script writes

All under `output_advanced_RSM/`, suffixed by K. The R notebook joins everything
back to documents by `row_index` (the same key the linker assigned).

| Script | Output | Schema |
|---|---|---|
| `embed_documents.py` | `embeddings/doc_emb_K{K}.parquet` | `row_index`, `dim_0`…`dim_383` |
| | `embeddings/doc_emb_K{K}.meta.json` | model, dim, n_docs |
| `run_bertopic.py` | `bertopic/bertopic_docs_K{K}.csv` | `row_index`, `bertopic_topic`, `bertopic_prob` |
| | `bertopic/bertopic_topics_K{K}.csv` | `Topic`, `Count`, `Name`, … |
| `semantic_change.py` | `diachronic/diachronic_change_K{K}.csv` | `word`, `comparison`, `period_a/b`, `prt`, `apd`, `jsd`, `n_a`, `n_b` |
| | `diachronic/diachronic_prototypes_K{K}.parquet` | `word`, `period`, `n`, `dim_0`…`dim_767` |
| | `diachronic/diachronic_periods.json` | the periods actually used |

---

## Configuration: `scripts/config.py`

One file holds every Python-side knob. Edit it, not the scripts. This is the main
file that differs from the WE project.

| Setting | Value | Meaning |
|---|---|---|
| `BASE` | `…\Claudepythonexperimentadvtext_RSM` | project home (RSM) |
| `ADV_DIR` | `output_advanced_RSM` | this layer's output dir |
| `DOC_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` | document embedder (384-dim, fast, CPU-friendly) |
| `HIST_MODEL` | `emanjavacas/MacBERTh` | historical-English BERT for diachrony (768-dim) |
| `CHUNK_WORDS` / `CHUNK_STRIDE` | 200 / 200 | long-article windowing for embedding |
| `DEFAULT_PERIOD_BOUNDS` | 1915-29 / 30-39 / 40-49 / 50-59 / 60-70 | RSM historical periods (intersected with observed years; empty ones dropped) |
| `DIACHRONIC_TARGETS` | relief, welfare, war, service, lesson, mother, home, work, sister, nurse, charity, duty, woman, family, faith | words to track |
| `CONTEXT_HALF_WORDS` | 20 | words of context each side of a target occurrence |
| `MAX_OCCUR_PER_PERIOD` | 250 | cap occurrences per (word, period) for tractability |
| `N_SENSES` | 5 | KMeans senses for the JSD sense-induction |

`resolve_K()` in config picks K: CLI arg > env `ADV_K` > newest
`advanced_docs_K*.csv`.

---

## R notebook structure

| § | What it produces | Needs Python artifact? |
|---|---|---|
| 2 | K-agnostic model selection (`pin_K`) | — |
| 3 | loads `advanced_input_K{K}.rds` + docs CSV | — |
| 4–5 | JSD matrix → `01_topic_dendrogram.png` | no |
| 6 | `02_mds_quadrant.png` + axis-anchor printout | no |
| 7 | `03_topic_network.png` | no |
| 8 | `04_document_atlas_*.png` | **doc_emb** |
| 9 | semantic-search example table | **doc_emb** |
| 10 | `05_bertopic_vs_stm.png` + top clusters | **bertopic** |
| 11 | `06_diachronic_change.png`, `07_diachronic_drift.png` | **diachronic** |
| 12 | `08_stylo_consensus_tree.png`, `09_stylometry_mds.png` | no (`stylo`) |
| 13 | `advanced_report_objects_K{K}.rds` | — |

Figures land in `output_advanced_RSM/report_figures/`.

---

## Key design decisions and why

1. **Compute decoupled from the knit.** The slow ML work runs once in
   checkpointed terminal scripts; the R knit only reads artifacts. This is the
   fix for the timeouts and makes everything resumable.
2. **Contextual embeddings for diachrony (not word2vec+Procrustes).** One
   pretrained historical space (MacBERTh), each occurrence embedded in context →
   no rotation, sense-level change. The old per-period word2vec was fragile on
   small periods and rotation-dependent.
3. **BERTopic as triangulation, not replacement.** We reuse the *same*
   embeddings and cross-tab BERTopic against STM. Agreement validates STM topics;
   divergence flags where STM splits or merges a neural cluster.
4. **JSD for topic geometry.** β rows are probability distributions, so
   Jensen-Shannon is the principled, bounded distance.
5. **`stylo` bootstrap consensus tree for authorship.** Aggregates groupings
   across MFW band sizes (100–300), so the result doesn't hinge on one arbitrary
   feature count — far more defensible than a single-cut kNN heuristic.
6. **Author filtering for stylometry.** We drop "Unknown", the periodical name,
   bracketed OCR/column junk, names ending in a comma, organizational/ad bylines
   (publisher, office, company, …), and initials-only signatures, so only real
   personal authors enter the clustering.
7. **Periods chosen historically, not statistically.** Boundaries track RSM
   history so trajectory plots line up with real events:
   - 1915–1929 founding (1914/15), WWI, suffrage won, 1920s
   - 1930–1939 Great Depression, welfare/relief work
   - 1940–1949 WWII, wartime service, rationing
   - 1950–1959 postwar expansion, Cold War domesticity
   - 1960–1970 Correlation era, lead-up to the 1970 discontinuation

---

## Troubleshooting

**A notebook section prints "Run: python scripts\X.py 20".** That artifact hasn't
been produced yet. Run the named script; re-knit. (The section is skipped, not
errored, on purpose.)

**`embed_documents.py` stopped partway.** Just run the same command again — it
reads `doc_emb_K{K}.ckpt.parquet` and resumes. The checkpoint is written every
500 docs.

**`arrow` can't read a Parquet file.** Make sure the Python script finished (a
final `doc_emb_K{K}.parquet` exists, not just the `.ckpt.parquet`). Re-run the
script to completion.

**"Requested K not found" in §2.** You haven't produced that K's hand-off yet.
Copy `topic_linker_session_K{K}.RData` into `output_linked/` and run
`generate_handoff.R {K}`.

**MacBERTh download is slow / warns about HF token.** First run downloads the
model (~400 MB). The "unauthenticated requests to HF Hub" warning is harmless.

**Python says a package is missing.** You're probably using system Python by
accident. Always call the full path `C:\Users\birch\dh-env\Scripts\python.exe`.

**Diachronic results look thin / a word is skipped.** A word needs ≥5 usable
occurrences in ≥2 periods. Rare words or rare-in-a-period words are skipped with
a printed note. Lower the bar or pick more frequent targets in `config.py`.

---

## Relationship to the WE pipeline and the joint comparison

This project is a near-verbatim clone of the Woman's Exponent advanced pipeline
(`Claudepythonexperimentadvtext`). The four Python scripts are byte-identical;
only `config.py` (paths, periods, targets) and `generate_handoff.R` (BASE) carry
RSM specifics. Keep them in sync: a bug fixed in one script should be copied to
the other.

For a *joint* WE+RSM analysis, see the cross-corpus comparison project
(`Claudepythonexperimentadvtext_compare`), which embeds both corpora tagged by
publication and measures centroid distance, embedding-space overlap, and
contrastive semantic change of shared terms (suffrage, relief, welfare, women).
It is fed by `regression_compare_2_corpora.Rmd` (joint STM with publication as a
covariate).

---

## File locations

Project home (self-contained, OUTSIDE OneDrive):
`C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_RSM`

- R report: `RSMadvancedtextanalysis.Rmd`
- This handover: `RSMadvancedtextanalysis_handover.md`
- Hand-off bridge (`.RData` → CSVs): `scripts/generate_handoff.R`
- Python config: `scripts/config.py`
- Python scripts: `scripts/{embed_documents,run_bertopic,semantic_change,hybrid_search}.py`
- STM linker (OS-aware; on Windows writes hand-off straight into `output_linked/`): `Topic_to_Article_Linker_RSM.Rmd`
- Linker session file (Option B input): `output_linked/topic_linker_session_K{K}.RData`
- Hand-off files (bridge → both layers): `output_linked/advanced_*_K{K}.*`
- Python compute artifacts: `output_advanced_RSM/{embeddings,bertopic,diachronic,search}/`
- Report figures: `output_advanced_RSM/report_figures/`
- Python environment: `C:\Users\birch\dh-env` (isolated Python 3.11, outside OneDrive)
```
