# Session Handover - WEadvancedtextanalysis (hybrid R + Python pipeline)

Reference document for the advanced text-analysis pipeline built for the
Woman's Exponent (WE) corpus. Read this if you (or a future assistant) need to
run, rebuild, extend, or port the work. It is written to be usable by someone
who is **comfortable in R but new to Python** — the Python parts are spelled out
step by step.

---

## The big idea: why there are now two layers

The previous version of this notebook trained word2vec, ran Procrustes
alignment, and built UMAPs **inside the R knit**. That is exactly what kept
timing out: the heavy machine-learning work and the report were welded together,
so every run re-did the slow part.

The pipeline is now split in two:

```
  ┌─────────────────────────────┐        ┌──────────────────────────────┐
  │  PYTHON COMPUTE LAYER        │  artifacts  │  R REPORTING LAYER       │
  │  (terminal, runs once,       │  ──────▶ │  WEadvancedtextanalysis.Rmd  │
  │   checkpointed, resumable)   │  .parquet│  (knits fast, just plots)    │
  │                              │  .csv    │                              │
  │  embed_documents.py          │          │  loads artifacts, makes      │
  │  run_bertopic.py             │          │  figures + tables            │
  │  semantic_change.py          │          │                              │
  └─────────────────────────────┘        └──────────────────────────────┘
```

**The R notebook never trains anything anymore.** It reads finished files and
plots them. If an artifact is missing, that section prints a "run script X
first" note and skips — so the notebook always knits, even before the compute
layer has run. This makes the whole thing timeout-proof.

A third hand-off feeds both: the STM linker
(`Topic_to_Article_Linker_WE.Rmd`) writes `advanced_*_K{K}.*` files that *both*
layers consume.

---

## The six analyses (and what's new)

| # | Analysis | Where it runs | What changed from the old version |
|---|---|---|---|
| 1 | **Topic geometry** (JSD dendrogram, MDS quadrants, co-occurrence network) | R (fast) | Now K-agnostic; reads the saved model, doesn't refit |
| 2 | **Document atlas + semantic search** | Python embeds, R plots | Transformer embeddings (all-MiniLM) replace topic-space UMAP; adds meaning-based search |
| 3 | **Hybrid search** (keyword + meaning) | Python (`hybrid_search.py`), R shows table | NEW — BM25 keyword + MiniLM semantic, fused with Reciprocal Rank Fusion; free-text queries |
| 4 | **BERTopic cross-check** | Python clusters, R cross-tabs | NEW — neural topic model triangulated against STM |
| 5 | **Diachronic semantic change** | Python (MacBERTh), R plots | NEW METHOD — in-context contextual embeddings (PRT/APD/JSD) replace word2vec + Procrustes |
| 6 | **Stylometry** (Burrows' Delta) | R (`stylo`) | Uses the field-standard `stylo` bootstrap consensus tree instead of a kNN heuristic |

**The frontier piece is #4.** Old approach: train one word2vec per period, then
rotate the spaces together with Procrustes so vectors are comparable. That is
fragile — small periods give noisy spaces and the rotation is fit on shared
vocabulary. New approach: embed every occurrence of a word **in its sentence
context** using MacBERTh (a BERT pretrained on 1500–1950 English), all in one
pretrained space. No rotation needed, and we recover genuine **sense-level**
change (how the *mix of senses* shifts), not just which neighbors reshuffle.
This is the SemEval-2020 Task 1 methodology — current best practice for lexical
semantic-change measurement.

---

## K is not hardcoded

Everything is suffixed by K (e.g., `advanced_docs_K35.csv`, `doc_emb_K35.parquet`).
- The **linker** sets K via `chosen_K <- 35`, or `chosen_K <- "auto"` to pick the
  balanced coherence/exclusivity K from its own `searchK` run.
- The R notebook picks K via `pin_K <- 35L` in §2 (set `NULL` to auto-pick newest).
- The Python scripts take K as a CLI argument: `... semantic_change.py 35`.
- Different K's never collide (every artifact is suffixed `_K{K}`).

**Canonical model is K=35** (best fit on the Pareto frontier; other K values were
fuzzier). To switch K: (1) drop the new `topic_linker_session_K{K}.RData` into
`output_linked/` and run `generate_handoff.R {K}`, (2) re-run the three Python
scripts with that K, (3) set `pin_K` and re-knit.

---

## Scenarios: time slices with flexible year *and* K

The pipeline can analyze the whole corpus or any time slice, with K chosen per
slice. A **scenario** is an isolated subfolder under both `output_linked/` and
`output_advanced_WE/`, so slices never overwrite each other.

Set the scenario in three places (they must match): the linker's §2
(`scenario <-`), the report's §2 (`scenario <-`), and the Python `ADV_SCENARIO`
environment variable.

| Scenario tag | Corpus |
|---|---|
| `WEfull` | entire run, 1872–1912 (no year filter) |
| `WE1872-1891` | years 1872–1891 |
| `WE1892-1912` | years 1892–1912 |
| `auto` | whatever volumes are physically in `input/`; the linker derives the tag from the observed year span and prints `Scenario tag: WE####-####` |

Two ways to scope a run:
- **Named scenario** — keep the full corpus in `input/` and slice by year range.
- **`auto`** — drop only the volumes you want into `Topicmodel/input/`, set
  `scenario <- "auto"` (and optionally `chosen_K <- "auto"`), Run All. The linker
  self-names the output folder from the observed years; use that printed tag for
  the report's `scenario` and the Python `ADV_SCENARIO`.

Folder layout (each scenario fully isolated):

```
output_linked/<scenario>/        advanced_*_K{K}.*   topic_linker_session_K{K}.RData
output_advanced_WE/<scenario>/   embeddings/ bertopic/ diachronic/ search/ report_figures/
```

K is independent per scenario — the sub-periods are smaller corpora and usually
want a smaller K (set an integer, or `"auto"`). Diachronic period bins are also
per-scenario: `config.py`'s `_PERIOD_BOUNDS_BY_SCENARIO` gives the full run the
5 historical bins and each sub-period finer bins inside its own window.

**Run one scenario end to end** (S = `WEfull` | `WE1872-1891` | `WE1892-1912` | an `auto` tag):
1. **Linker** — set `scenario <- S` and `chosen_K` (integer or `"auto"`), Run All
   → writes `output_linked/S/`.
2. **Python** — `$env:ADV_SCENARIO = "S"`, then run the three scripts (K
   auto-resolves inside the scenario folder).
3. **Report** — set `scenario <- S` (and `pin_K`), knit → figures in
   `output_advanced_WE/S/report_figures/`.

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
installed just for this project, at `C:\Users\birch\dh-env`. Two reasons:
the system Python (3.14) is too new — the ML libraries don't ship installers for
it yet; and keeping the environment *outside* OneDrive stops OneDrive from
constantly syncing thousands of library files.

**You do not need to "learn Python" to run this.** You run three commands in a
terminal and read the progress lines they print. That's it. Here is the mental
model:

- A *Python environment* is just a folder containing a Python interpreter plus
  installed libraries. `C:\Users\birch\dh-env` is ours.
- To run a script with that environment, you call its python.exe by full path
  and hand it the script. There is no "activation" needed when you use the full
  path.
- The scripts print a line every few hundred documents so you can watch progress.
  They save a checkpoint as they go, so if one stops you just run it again and it
  resumes.

The environment is already built. If it ever needs rebuilding from scratch:

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

The topic linker is `Topic_to_Article_Linker_WE.Rmd`. Its set-paths chunk is
**environment-aware** and **scenario-aware**: on the Linux server it uses the
original `/home/rbirch/...` locations; on Windows it now reads the corpus +
inventory from the **local Rachel drive** (no OneDrive) and writes **all** output
— including `topic_linker_session_K{K}.RData` AND the four `advanced_*_K{K}.*`
hand-off files — into this project's `output_linked/<scenario>/`.

Whatever produces the hand-off, these four files are what the rest of the pipeline
consumes:

| File | Contents |
|---|---|
| `advanced_input_K{K}.rds` | list: `stm_model`, `meta`, `topic_labels`, `modeled_raw`, `modeled_clean`, `fit_date` |
| `advanced_docs_K{K}.csv` | one row per modeled doc (row_index, doc_id, year, author, title, text, …) |
| `advanced_theta_K{K}.csv` | D×K document-topic matrix, same row order |
| `advanced_topics_K{K}.csv` | topic number, label, FREX/PROB words |

There are two ways to get them, depending on where you run the K=35 model:

**Option A — run the linker on Windows (no copy step).** Open
`Topic_to_Article_Linker_WE.Rmd`, set `scenario <-` and `chosen_K <-` (an integer
or `"auto"`), and Run All / knit. Because of the OS-aware paths, it deposits the
`.RData` and all four `advanced_*` files directly into `output_linked/<scenario>/`.
You're done with Step 1 — skip straight to Step 2. (The corpus lives on the local
Rachel drive at `Topicmodel/input/`; the run reads it from there. This is the
heavy R job — searchK + fitting takes a while.)

**Option B — run the linker on the server, bring one file over.** Run it on Linux
as usual; it saves `topic_linker_session_K35.RData` (and the `advanced_*` files)
into `/home/rbirch/rbirch/Topicmodel/output/`. Copy `topic_linker_session_K35.RData`
into this project's `output_linked/<scenario>/`, then run the bridge script
(arg 1 = K, arg 2 = scenario tag):

```powershell
& "C:\Program Files\R\R-4.5.3\bin\Rscript.exe" scripts\generate_handoff.R 35 WEfull
```

`generate_handoff.R` reads the `.RData`, re-aligns each modeled document to its
source text, and writes the same four files. (If you instead copied the four
`advanced_*_K35.*` files straight off the server, you don't need this script at
all — go to Step 2.) K comes from the model object itself; pass a number to pick a
specific `.RData`, or omit it to use the newest session file in `output_linked/`.

### Step 2 — Run the Python scripts (in a terminal)

Open PowerShell. First pick the scenario (must match the linker's tag), then run
the three scripts one at a time (the trailing `35` pins K — omit it to auto-resolve):

```powershell
cd "C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext"
$env:ADV_SCENARIO = "WEfull"   # or WE1872-1891 / WE1892-1912 / the auto tag
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\embed_documents.py 35
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\run_bertopic.py    35
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\semantic_change.py 35
```

What to expect:
- **embed_documents.py** is the long one (embeds all ~11k articles; ~20–40 min on
  CPU). It prints `checkpoint: N/total` lines and saves a `.ckpt.parquet` as it
  goes. If it stops, run the same command again — it resumes from the checkpoint.
- **run_bertopic.py** reuses those embeddings (no re-embedding) and is quick.
- **semantic_change.py** downloads MacBERTh on first run, then embeds word
  occurrences in context. Minutes, not hours.

Dev shortcuts (optional): `set ADV_LIMIT=200` before embed to test on 200 docs;
`set ADV_WORDS=suffrage,sister` before semantic_change to test on two words.

**Hybrid search is on-demand** (run it whenever you have a query — it needs only
the embeddings from `embed_documents.py`, so you can run it any time after that):

```powershell
$env:ADV_SCENARIO = "WEfull"   # same scenario you embedded under
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\hybrid_search.py women right to vote
```

It fuses BM25 keyword search with MiniLM semantic search (Reciprocal Rank Fusion),
prints the ranked hits, and writes `output_advanced_WE\search\hybrid_search_K{K}.csv`,
which §9b of the report renders as a searchable table (search box scans full body
text). The CSV lands under the active scenario's `search/` folder. Knobs:
`set HS_TOPN=25` (more rows), `set ADV_K=35` (pick the model),
`set HS_ALPHA=0.7` (favor meaning) / `0.3` (favor keywords) instead of pure RRF.
Each `sem#` / `lex#` column tells you whether a hit came from meaning, keywords,
or both.

### Step 3 — Knit the report (in R)

Open `WEadvancedtextanalysis.Rmd`, confirm `pin_K <- 35L` in §2, and knit (or Run
All). It loads the hand-off and the Python artifacts and produces all figures in
`output_advanced_WE/report_figures/`. Any section whose artifact is missing
prints a skip note instead of erroring.

---

## What each Python script writes

All under `output_advanced_WE/<scenario>/`, suffixed by K. The R notebook joins
everything back to documents by `row_index` (the same key the linker assigned).

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

One file holds every Python-side knob. Edit it, not the scripts.

| Setting | Value | Meaning |
|---|---|---|
| `SCENARIO` (env `ADV_SCENARIO`) | `WEfull` | which corpus-slice subfolder to read/write under `output_linked/` + `output_advanced_WE/` |
| `DOC_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` | document embedder (384-dim, fast, CPU-friendly) |
| `HIST_MODEL` | `emanjavacas/MacBERTh` | historical-English BERT for diachrony (768-dim) |
| `CHUNK_WORDS` / `CHUNK_STRIDE` | 200 / 200 | long-article windowing for embedding |
| `_PERIOD_BOUNDS_BY_SCENARIO` → `DEFAULT_PERIOD_BOUNDS` | per-scenario bins | full run = 5 historical bins (1872-79 / 80-87 / 88-96 / 97-1905 / 06-14); each sub-period = finer bins inside its own window. Intersected with observed years; empty ones dropped |
| `DIACHRONIC_TARGETS` | suffrage, vote, rights, polygamy, sister, mother, home, duty, woman, calling, temple, priesthood, work, relief | words to track |
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

Figures land in `output_advanced_WE/report_figures/`.

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
   feature count — far more defensible than the old single-cut kNN heuristic.
   "Blanche Beechwood" (a known Emmeline B. Wells pseudonym) is a built-in test
   case: it should cluster near Wells's signed work.
6. **Author filtering for stylometry.** We drop "Unknown", the periodical name,
   bracketed OCR/column junk, names ending in a comma, organizational/ad bylines
   (exponent, publisher, office, company, …), and initials-only signatures, so
   only real personal authors enter the clustering.
7. **Periods chosen historically, not statistically.** Boundaries track WE
   history (founding / Edmunds era / Manifesto / post-statehood) so trajectory
   plots line up with real events.

---

## Troubleshooting

**A notebook section prints "Run: python scripts\X.py 35".** That artifact hasn't
been produced yet. Run the named script; re-knit. (The section is skipped, not
errored, on purpose.)

**`embed_documents.py` stopped partway.** Just run the same command again — it
reads `doc_emb_K{K}.ckpt.parquet` and resumes. The checkpoint is written every
500 docs.

**`arrow` can't read a Parquet file.** Make sure the Python script finished (a
final `doc_emb_K{K}.parquet` exists, not just the `.ckpt.parquet`). Re-run the
script to completion.

**"Requested K=35 not found" in §2.** You haven't produced the K=35 hand-off yet.
Copy `topic_linker_session_K35.RData` into `output_linked/` and run
`generate_handoff.R 35`.

**MacBERTh download is slow / warns about HF token.** First run downloads the
model (~400 MB). The "unauthenticated requests to HF Hub" warning is harmless.

**Python says a package is missing.** You're probably using system Python by
accident. Always call the full path `C:\Users\birch\dh-env\Scripts\python.exe`.

**Diachronic results look thin / a word is skipped.** A word needs ≥5 usable
occurrences in ≥2 periods. Rare words or rare-in-a-period words are skipped with
a printed note. Lower the bar or pick more frequent targets in `config.py`.

---

## Extending to RSM (Relief Society Magazine)

The pipeline is publication-agnostic once the hand-off exists. To do RSM:

1. From the RSM linker's `topic_linker_session_K{K}.RData`, run `generate_handoff.R`
   to write `advanced_*_K{K}.csv/.rds` for RSM (point `output_linked` at an RSM
   folder, or suffix filenames with a publication tag).
2. Point `config.py`'s `OUTPUT_LINKED`/`ADV_DIR` at the RSM folders, and update
   `DEFAULT_PERIOD_BOUNDS` to RSM's era (e.g., 1914–1970) and `DIACHRONIC_TARGETS`
   to RSM-relevant terms (priesthood, calling, sister, family, work).
3. Run the three Python scripts and knit a copied `RSMadvancedtextanalysis.Rmd`
   with `pin_K` set to the RSM model.

For a *joint* WE+RSM analysis, build on `regression_compare_2_corpora.Rmd` (joint
STM with publication as a covariate) and add `publication` as a grouping variable
in the diachronic and stylometry plots.

---

## File locations

Project home (self-contained, OUTSIDE OneDrive):
`C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext`

- R report: `WEadvancedtextanalysis.Rmd`
- This handover: `WEadvancedtextanalysis_handover.md`
- Hand-off bridge (`.RData` → CSVs): `scripts/generate_handoff.R`
- Python config: `scripts/config.py`
- Python scripts: `scripts/{embed_documents,run_bertopic,semantic_change,hybrid_search}.py`
- STM linker (OS- and scenario-aware; on Windows writes hand-off into `output_linked/<scenario>/`): `Topic_to_Article_Linker_WE.Rmd`
  - on Windows reads corpus + inventory from the **local Rachel drive**: `Topicmodel/input/` and `exponentorder/output/WE_article_inventory_vols1-41.xlsx`
- Linker session file (Option B input): `output_linked/<scenario>/topic_linker_session_K{K}.RData`
- Hand-off files (bridge → both layers): `output_linked/<scenario>/advanced_*_K{K}.*`
- Python compute artifacts: `output_advanced_WE/<scenario>/{embeddings,bertopic,diachronic,search}/`
- Report figures: `output_advanced_WE/<scenario>/report_figures/`
- Python environment: `C:\Users\birch\dh-env` (isolated Python 3.11, outside OneDrive)
```