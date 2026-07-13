# Session Handover - compareadvancedtextanalysis (cross-corpus contrast)

Reference document for the **cross-corpus** advanced text-analysis pipeline that
contrasts Woman's Exponent (WE) and Relief Society Magazine (RSM). It is the third
sibling of the two single-corpus projects:

| Project | Question it answers |
|---|---|
| `Claudepythonexperimentadvtext` (WE) | what happens inside the Woman's Exponent |
| `Claudepythonexperimentadvtext_RSM` (RSM) | what happens inside the Relief Society Magazine |
| `Claudepythonexperimentadvtext_compare` (this) | **how the two periodicals differ in meaning-space** |

Written for someone comfortable in R but new to Python — the Python parts are
spelled out step by step.

---

## The big idea

The single-corpus pipelines model one periodical at a time. This one starts from
a **joint STM** fit over *both* corpora (`regression_compare_2_corpora.Rmd`) whose
handoff tags every document with its `corpus`. The Python layer then embeds the
combined corpus once and the new `contrast.py` measures, specifically, how the two
publications relate to each other in embedding space.

Same two-layer design as the siblings (heavy ML in checkpointed terminal scripts;
R knit only reads artifacts; missing artifact → section skips, never errors):

```
  ┌─────────────────────────────┐        ┌───────────────────────────────────┐
  │  PYTHON COMPUTE LAYER        │ artifacts │  R REPORTING LAYER              │
  │  embed_documents.py          │ ──────▶ │  compareadvancedtextanalysis.Rmd  │
  │  contrast.py    (NEW)        │ .parquet│  (knits fast, just plots)         │
  │  run_bertopic.py             │ .csv    │                                   │
  └─────────────────────────────┘        └───────────────────────────────────┘
```

Three of the four Python scripts (`embed_documents`, `run_bertopic`,
`semantic_change`) are byte-identical to the single-corpus versions and run on the
**combined** corpus. The one genuinely new script is **`contrast.py`**.

---

## The six analyses

| # | Analysis | Source | What it shows |
|---|---|---|---|
| 1 | **Combined document atlas** (coloured by publication) | `embed_documents.py` | do the corpora interleave or pull apart |
| 2 | **Embedding-space geometry** | `contrast.py` Part A | centroid distance, overlap **AUC**, kNN mixing |
| 3 | **Topic prevalence by publication** | joint `theta` (pure R) | which joint-STM topics each periodical leans on |
| 4 | **Contrastive semantic change of shared terms** | `contrast.py` Part B | does "relief"/"woman"/"suffrage" mean the same in each |
| 5 | **Framing-drift map** (WE→RSM arrows) | `contrast.py` Part B | direction + magnitude of each term's drift |
| 6 | **BERTopic by publication** | `run_bertopic.py` | which neural themes are corpus-specific vs shared |

**The frontier piece is #4–5.** `contrast.py` Part B reuses the diachronic
contextual-embedding machinery (MacBERTh, SemEval-2020 Task 1 measures
PRT/APD/JSD) but the two groups being compared are the two **publications** rather
than two time periods. Each occurrence of a shared word is embedded *in its
sentence context* in one pretrained historical space, so the two periodicals'
usages are directly comparable with no alignment step, and we recover **sense-level**
difference (does the *mix of senses* differ), not just neighbour reshuffling.

---

## `contrast.py` — what it computes and writes

**Part A — embedding-space geometry** (from the MiniLM doc embeddings):
- `centroid_dist` = `1 - cos(mean_WE, mean_RSM)` — how far apart, overall.
- `overlap_auc` — cross-validated logistic-regression AUC predicting publication
  from a document's embedding. **0.5 = indistinguishable (total overlap); 1.0 =
  perfectly separable (no overlap).**
- `knn_mixing` — for each article, the fraction of its nearest neighbours in the
  *other* corpus (~0.5 interleaved, ~0 separated).

**Part B — contrastive semantic change** of shared terms (MacBERTh, grouped by
corpus): for each `CONTRAST_TARGETS` word, PRT / APD / JSD between the two
periodicals' in-context usages.

Outputs (all in `output_advanced_compare/contrast/`, suffixed by K):

| File | Schema |
|---|---|
| `embedding_overlap_K{K}.csv` | one row: `corpus_a/b`, `n_a/b`, `centroid_dist`, `overlap_auc`, `knn_mixing_a/b` |
| `knn_mixing_K{K}.csv` | `row_index`, `corpus`, `other_frac` (per-article, for the histogram) |
| `term_contrast_K{K}.csv` | `word`, `corpus_a/b`, `prt`, `apd`, `jsd`, `n_a`, `n_b` |
| `term_prototypes_K{K}.parquet` | `word`, `corpus`, `n`, `dim_0`…`dim_767` (for the PCA drift map) |
| `contrast_meta_K{K}.json` | the two corpus labels used |

---

## How to run, start to finish

### Step 1 — Produce the joint-STM hand-off (in R)

Knit `regression_compare_2_corpora.Rmd`. Its paths are OS-aware: on Windows it
reads both corpora + inventories from OneDrive but writes **all** output —
including the four `advanced_*_K{K}.*` files AND `comparison_session_K{K}.RData` —
straight into this project's `output_linked/` (§27b does this). The four files:

| File | Contents |
|---|---|
| `advanced_input_K{K}.rds` | list: `stm_model`, `meta`, `topic_labels`, `modeled_raw`, `modeled_clean`, `fit_date` |
| `advanced_docs_K{K}.csv` | one row per modeled doc, **with a `corpus` tag**, plus year/author/title/text |
| `advanced_theta_K{K}.csv` | D×K joint document-topic matrix, same row order |
| `advanced_topics_K{K}.csv` | topic number, label, FREX/PROB words |

If you instead fit the joint model on the Linux server, copy
`comparison_session_K{K}.RData` into `output_linked/` and run the bridge:

```powershell
& "C:\Program Files\R\R-4.5.3\bin\Rscript.exe" scripts\generate_handoff.R 30
```

### Step 2 — Run the Python scripts (in a terminal)

```powershell
cd "C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_compare"
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\embed_documents.py 30
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\contrast.py        30
& "C:\Users\birch\dh-env\Scripts\python.exe" scripts\run_bertopic.py    30
```

(Use whatever K you fit; `30` is only an example.) `embed_documents.py` is the
long, checkpointed one — if it stops, run it again and it resumes. `contrast.py`
reads its embeddings for Part A, then downloads/uses MacBERTh for Part B (minutes).
Dev shortcut: `set ADV_WORDS=relief,suffrage` before `contrast.py` to test on two
words. The same `C:\Users\birch\dh-env` Python 3.11 venv as the sibling projects.

### Step 3 — Knit the report (in R)

Open `compareadvancedtextanalysis.Rmd`, set `pin_K` in §2 (or leave `NULL` to
auto-pick newest), knit. Figures land in `output_advanced_compare/report_figures/`.
Any section whose artifact is missing prints a skip note instead of erroring.

---

## Configuration: `scripts/config.py`

The main file that differs from the single-corpus projects.

| Setting | Value | Meaning |
|---|---|---|
| `BASE` / `ADV_DIR` | `…_compare` / `output_advanced_compare` | project home / output dir |
| `CORPUS_COL` | `corpus` | column in `advanced_docs` holding the publication tag |
| `CORPUS_ORDER` | `None` | fix the A/B order, or `None` to read the two values from data |
| `CORPUS_PRETTY` | WE / RSM display names | for plots + printouts |
| `KNN_NEIGHBORS` | 15 | neighbours per doc for the mixing metric |
| `OVERLAP_MAX_PER_CORPUS` | 4000 | cap docs per corpus for the AUC/kNN (speed) |
| `CONTRAST_TARGETS` | suffrage, relief, welfare, woman, women, sister, … | shared terms to contrast |
| `MAX_OCCUR_PER_CORPUS` | 300 | cap occurrences per (word, corpus) |
| `DOC_MODEL` / `HIST_MODEL` | all-MiniLM-L6-v2 / MacBERTh | doc embedder / historical BERT |
| `DEFAULT_PERIOD_BOUNDS` | 1872-1890 … 1950-1970 | union of WE+RSM eras (only used if you run `semantic_change.py` on the merged corpus) |

`resolve_K()` picks K: CLI arg > env `ADV_K` > newest `advanced_docs_K*.csv`.

---

## R notebook structure

| § | Produces | Needs artifact |
|---|---|---|
| 2–3 | K selection + load joint handoff | — |
| 4 | `01_atlas_by_publication.png` | **doc_emb** |
| 5 | `02_knn_mixing.png` + overlap table | **contrast Part A** |
| 6 | `03_topic_prevalence_by_publication.png` | — (pure R) |
| 7 | `04_term_contrast.png` + table | **contrast Part B** |
| 8 | `05_framing_drift_map.png` | **contrast Part B** |
| 9 | `06_bertopic_by_publication.png` | **bertopic** |
| 10 | `compare_report_objects_K{K}.rds` | — |

---

## Key design decisions

1. **One joint STM, one shared embedding space.** Both corpora are modeled and
   embedded together so every contrast is apples-to-apples — no cross-model
   alignment needed.
2. **Contrast = the diachronic method with publication as the grouping axis.**
   `contrast.py` Part B is `semantic_change.py` with A/B = the two periodicals
   instead of two periods. Same well-grounded PRT/APD/JSD measures.
3. **Overlap reported three ways.** Centroid distance (coarse), AUC (global
   separability), kNN mixing (local interleaving) — they can disagree, and the
   disagreement is informative.
4. **Keep the scripts in sync.** `embed_documents`, `run_bertopic`,
   `semantic_change` are identical across all three projects; fix a bug in one,
   copy it to the others. Only `config.py`, `generate_handoff.R`, and (here)
   `contrast.py` carry project-specific logic.

---

## Troubleshooting

**"advanced_docs has no 'corpus' column."** The handoff wasn't written by the
comparison notebook's §27b (which tags each row). Re-knit that notebook, or use
its `comparison_session_K{K}.RData` with `generate_handoff.R`.

**`contrast.py` says "Expected exactly 2 corpora."** The `corpus` column has more
or fewer than two distinct values. Check the joint model's `meta$corpus`.

**Overlap AUC is `NaN`.** The classifier cross-validation failed (usually a tiny
corpus). Fine to ignore; centroid distance and kNN mixing still report.

**A word is skipped in Part B.** It needs ≥5 usable in-context occurrences in
*both* periodicals. Pick more frequent shared terms in `CONTRAST_TARGETS`.

**Python says a package is missing.** You're probably on system Python. Always use
the full path `C:\Users\birch\dh-env\Scripts\python.exe`.

---

## File locations

Project home (self-contained, OUTSIDE OneDrive):
`C:\Users\birch\Rachel\GMU\Dissertation\textanalysis\Claudepythonexperimentadvtext_compare`

- R report: `compareadvancedtextanalysis.Rmd`
- This handover: `compareadvancedtextanalysis_handover.md`
- Hand-off bridge (`.RData` → CSVs): `scripts/generate_handoff.R`
- Python config: `scripts/config.py`
- Python scripts: `scripts/{embed_documents,run_bertopic,semantic_change,hybrid_search}.py` (shared) + `scripts/contrast.py` (new)
- Joint-STM source notebook (OS-aware; writes handoff into `output_linked/`): `regression_compare_2_corpora.Rmd` (§27b)
- Hand-off files (→ both layers): `output_linked/advanced_*_K{K}.*` and `comparison_session_K{K}.RData`
- Python compute artifacts: `output_advanced_compare/{embeddings,bertopic,contrast,diachronic,search}/`
- Report figures: `output_advanced_compare/report_figures/`
- Python environment: `C:\Users\birch\dh-env` (isolated Python 3.11, outside OneDrive)
```
