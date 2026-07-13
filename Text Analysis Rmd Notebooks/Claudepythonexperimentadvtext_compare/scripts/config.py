"""
Shared configuration for the CROSS-CORPUS advanced-text-analysis compute layer
(Woman's Exponent vs. Relief Society Magazine).

This is the comparison sibling of the two single-corpus projects
(Claudepythonexperimentadvtext = WE, Claudepythonexperimentadvtext_RSM = RSM).
Where those answer "what is happening inside one periodical," this one answers
"how do the two periodicals differ in meaning-space":

    contrast.py   centroid distance + embedding-space overlap (how separable the
                  two corpora are in MiniLM space) and CONTRASTIVE semantic change
                  of shared terms (does "relief"/"woman"/"suffrage" mean the same
                  thing in WE as in RSM?) using MacBERTh, grouping by PUBLICATION
                  instead of by time period.

The other three scripts (embed_documents / run_bertopic / semantic_change) are
byte-identical to the single-corpus versions and run on the COMBINED corpus; the
only difference is THIS file's paths/targets and the publication-aware handoff.

Inputs (written by regression_compare_2_corpora.Rmd §27b, in OUTPUT_LINKED):
    advanced_docs_K{K}.csv    one row per modeled doc, row-aligned to theta, with
                              the ORIGINAL article text AND a `corpus` tag.
    advanced_theta_K{K}.csv   D x K document-topic matrix, same row order.
    advanced_topics_K{K}.csv  topic number, label, top FREX/PROB words.

Outputs (this layer writes into ADV_DIR/<stage>/, suffixed by K):
    embeddings/   doc + chunk transformer embeddings (combined corpus)
    bertopic/     neural topic model over both corpora
    contrast/     cross-corpus centroid/overlap + contrastive semantic change
    diachronic/   (optional) per-period change over the combined timeline
"""
from __future__ import annotations
import os
import sys
import glob
import re
from pathlib import Path

# --- Roots -------------------------------------------------------------------
BASE = Path(
    r"C:\Users\birch\Rachel\GMU\Dissertation\textanalysis"
    r"\Claudepythonexperimentadvtext_compare"
)
# Comparison scenario (must match regression_compare_2_corpora.Rmd + the report).
# Default "compare_full"; set $env:ADV_SCENARIO for era comparisons.
SCENARIO = os.environ.get("ADV_SCENARIO", "compare_full")
OUTPUT_LINKED = BASE / "output_linked" / SCENARIO       # linker artifacts (inputs here)
ADV_DIR = BASE / "output_advanced_compare" / SCENARIO   # this layer's outputs
EMB_DIR = ADV_DIR / "embeddings"
BERTOPIC_DIR = ADV_DIR / "bertopic"
DIACHRONIC_DIR = ADV_DIR / "diachronic"
CONTRAST_DIR = ADV_DIR / "contrast"
for _d in (ADV_DIR, EMB_DIR, BERTOPIC_DIR, DIACHRONIC_DIR, CONTRAST_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# --- Models ------------------------------------------------------------------
DOC_MODEL = "sentence-transformers/all-MiniLM-L6-v2"   # 384-dim
HIST_MODEL = "emanjavacas/MacBERTh"                    # 768-dim, bert-base

# --- Chunking ----------------------------------------------------------------
CHUNK_WORDS = 200
CHUNK_STRIDE = 200
EMBED_BATCH = 64
CHECKPOINT_EVERY = 500

# --- Cross-corpus contrast ---------------------------------------------------
# Column in advanced_docs_K{K}.csv that tags each row with its publication.
CORPUS_COL = "corpus"
# Optional fixed order/labels. If None, contrast.py reads the two distinct values
# from the data (and errors if there aren't exactly two).
CORPUS_ORDER = None        # e.g. ["Womans_Exponent", "Relief_Society_Magazine"]
# Pretty names for plots/printouts, keyed by the raw corpus value.
CORPUS_PRETTY = {
    "Womans_Exponent": "Woman's Exponent",
    "Relief_Society_Magazine": "Relief Society Magazine",
}
# kNN mixing: for each doc, what fraction of its K_NEIGHBORS nearest neighbours
# belong to the OTHER corpus (1.0 = fully interleaved, 0.0 = fully separated).
KNN_NEIGHBORS = 15
# Cap docs per corpus for the overlap classifier / kNN (speed); None = all.
OVERLAP_MAX_PER_CORPUS = 4000

# --- Contrastive semantic change (shared terms across the two corpora) --------
# Terms that exist in BOTH periodicals and whose framing plausibly differs.
# Same machinery as diachronic semantic change, but the two groups are the two
# PUBLICATIONS rather than two time periods.
CONTRAST_TARGETS = [
    "suffrage", "vote", "rights", "relief", "welfare", "charity", "service",
    "woman", "women", "sister", "mother", "home", "work", "duty", "faith",
    "family", "war", "lesson", "temple", "priesthood",
]
CONTEXT_HALF_WORDS = 20      # words of context each side of a target token
MAX_OCCUR_PER_CORPUS = 300   # cap occurrences per (word, corpus) for tractability
HIST_BATCH = 32
HIST_MAXLEN = 96
N_SENSES = 5

# --- Diachronic (optional, over the COMBINED 1872-1970 timeline) -------------
# Union of the WE and RSM era boundaries, so a per-period run on the merged
# corpus still lines up with real events in either periodical.
DEFAULT_PERIOD_BOUNDS = [
    ("1872-1890", 1872, 1890),   # WE founding / Edmunds era
    ("1891-1914", 1891, 1914),   # Manifesto / statehood / WE close
    ("1915-1929", 1915, 1929),   # RSM founding, WWI, suffrage won, 1920s
    ("1930-1939", 1930, 1939),   # Great Depression, welfare/relief work
    ("1940-1949", 1940, 1949),   # WWII, wartime service
    ("1950-1970", 1950, 1970),   # postwar, Correlation, RSM close
]
DIACHRONIC_TARGETS = CONTRAST_TARGETS
MAX_OCCUR_PER_PERIOD = 250

# --- K selection -------------------------------------------------------------
def resolve_K(argv_k: str | None = None) -> int:
    """Pick the model K. Priority: CLI arg > env ADV_K > newest advanced_docs_K*.csv."""
    if argv_k:
        return int(argv_k)
    env_k = os.environ.get("ADV_K")
    if env_k:
        return int(env_k)
    cands = glob.glob(str(OUTPUT_LINKED / "advanced_docs_K*.csv"))
    if not cands:
        sys.exit(
            "No advanced_docs_K*.csv found in output_linked/.\n"
            "Run regression_compare_2_corpora.Rmd §27b first (it writes the "
            "advanced_* hand-off from the joint STM model)."
        )
    newest = max(cands, key=os.path.getmtime)
    k = int(re.search(r"_K(\d+)\.csv$", newest).group(1))
    return k

def docs_csv(K: int) -> Path:   return OUTPUT_LINKED / f"advanced_docs_K{K}.csv"
def theta_csv(K: int) -> Path:  return OUTPUT_LINKED / f"advanced_theta_K{K}.csv"
def topics_csv(K: int) -> Path: return OUTPUT_LINKED / f"advanced_topics_K{K}.csv"


def chunk_words(text: str, size: int = CHUNK_WORDS, stride: int = CHUNK_STRIDE):
    """Yield word-windows over a document. Always yields at least one chunk."""
    words = str(text).split()
    if not words:
        return [""]
    out = []
    i = 0
    n = len(words)
    while i < n:
        out.append(" ".join(words[i : i + size]))
        i += stride
    return out
