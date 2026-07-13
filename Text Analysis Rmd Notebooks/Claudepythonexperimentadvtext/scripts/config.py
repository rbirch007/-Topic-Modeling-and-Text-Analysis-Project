"""
Shared configuration for the advanced-text-analysis Python compute layer.

These scripts run from a terminal using the dedicated Python 3.11 venv at
C:\\Users\\birch\\dh-env — NOT the system Python 3.14. The .Rmd never triggers
these; it only reads the artifacts they write. That keeps the heavy transformer
work out of the notebook knit (and out of the timeout zone).

Inputs  (written by generate_handoff.R from the linker's
         topic_linker_session_K{K}.RData, in OUTPUT_LINKED):
    advanced_docs_K{K}.csv    one row per modeled doc, row-aligned to theta,
                              with the ORIGINAL article text.
    advanced_theta_K{K}.csv   D x K document-topic matrix, same row order.
    advanced_topics_K{K}.csv  topic number, label, top FREX/PROB words.

Outputs (this layer writes into ADV_DIR/<stage>/, suffixed by K):
    embeddings/   doc + chunk transformer embeddings
    bertopic/     neural topic model (STM cross-check)
    diachronic/   period contextual embeddings + semantic-change measures
"""
from __future__ import annotations
import os
import sys
import glob
import re
from pathlib import Path

# --- Roots -------------------------------------------------------------------
# Self-contained project home, OUTSIDE OneDrive (no sync timeouts / file locks).
BASE = Path(
    r"C:\Users\birch\Rachel\GMU\Dissertation\textanalysis"
    r"\Claudepythonexperimentadvtext"
)
# --- Scenario (corpus slice) -------------------------------------------------
# Each scenario is an isolated subfolder under output_linked/ and
# output_advanced_WE/, so same-K runs of different slices never collide.
# Select with:  set ADV_SCENARIO=WE1872-1891   (before calling a script)
# Must match the tag the linker printed ("Scenario tag: ...").
SCENARIO = os.environ.get("ADV_SCENARIO", "WEfull")

OUTPUT_LINKED = BASE / "output_linked" / SCENARIO       # linker artifacts (inputs here)
ADV_DIR = BASE / "output_advanced_WE" / SCENARIO        # this layer's outputs
EMB_DIR = ADV_DIR / "embeddings"
BERTOPIC_DIR = ADV_DIR / "bertopic"
DIACHRONIC_DIR = ADV_DIR / "diachronic"
for _d in (ADV_DIR, EMB_DIR, BERTOPIC_DIR, DIACHRONIC_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# --- Models ------------------------------------------------------------------
# Document atlas / semantic search / BERTopic: fast, CPU-friendly, strong general
DOC_MODEL = "sentence-transformers/all-MiniLM-L6-v2"   # 384-dim
# Diachronic semantic change: BERT trained on HISTORICAL English (1500-1950).
# The scholarly-defensible choice for 19th-c. lexical-semantic-change measurement.
HIST_MODEL = "emanjavacas/MacBERTh"                    # 768-dim, bert-base

# --- Chunking ----------------------------------------------------------------
# Articles run long (median ~400 words, max ~9000). Transformers cap at 512
# tokens, so we split into word-windows, embed each, and length-weight mean-pool
# back to one vector per document.
CHUNK_WORDS = 200          # ~ <512 tokens with headroom
CHUNK_STRIDE = 200         # no overlap (set < CHUNK_WORDS for overlap)
EMBED_BATCH = 64           # sentences per forward pass
CHECKPOINT_EVERY = 500     # docs between checkpoint writes (resumability)

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
            "Run generate_handoff.R first (it reads topic_linker_session_K{K}.RData)."
        )
    # newest by mtime
    newest = max(cands, key=os.path.getmtime)
    k = int(re.search(r"_K(\d+)\.csv$", newest).group(1))
    return k

def docs_csv(K: int) -> Path:   return OUTPUT_LINKED / f"advanced_docs_K{K}.csv"
def theta_csv(K: int) -> Path:  return OUTPUT_LINKED / f"advanced_theta_K{K}.csv"
def topics_csv(K: int) -> Path: return OUTPUT_LINKED / f"advanced_topics_K{K}.csv"


# --- Diachronic semantic change ----------------------------------------------
# Historical period boundaries (inclusive). Intersected with the observed year
# range at runtime; empty periods are dropped and a warning is printed. Chosen
# around WE history (founding / Edmunds era / Manifesto / post-statehood) rather
# than equal-width bins, so trajectory plots line up with real events.
# Per-scenario period bounds. The full run keeps the 5 historical bins; each
# sub-period scenario uses finer bins INSIDE its own window so trajectories have
# resolution. Unknown/auto scenarios fall back to the full set. All are
# intersected with observed years at runtime; empty periods are dropped.
_PERIOD_BOUNDS_BY_SCENARIO = {
    "WEfull": [
        ("1872-1879", 1872, 1879),
        ("1880-1887", 1880, 1887),
        ("1888-1896", 1888, 1896),
        ("1897-1905", 1897, 1905),
        ("1906-1914", 1906, 1914),
    ],
    "WE1872-1891": [
        ("1872-1877", 1872, 1877),
        ("1878-1884", 1878, 1884),
        ("1885-1891", 1885, 1891),
    ],
    "WE1892-1912": [
        ("1892-1898", 1892, 1898),
        ("1899-1905", 1899, 1905),
        ("1906-1912", 1906, 1912),
    ],
}
DEFAULT_PERIOD_BOUNDS = _PERIOD_BOUNDS_BY_SCENARIO.get(
    SCENARIO, _PERIOD_BOUNDS_BY_SCENARIO["WEfull"]
)
# Terms whose meaning plausibly shifts across the WE run. Edit freely.
DIACHRONIC_TARGETS = [
    "suffrage", "vote", "rights", "polygamy", "sister", "mother", "home",
    "duty", "woman", "calling", "temple", "priesthood", "work", "relief",
]
CONTEXT_HALF_WORDS = 20      # words of context on each side of a target token
MAX_OCCUR_PER_PERIOD = 250   # cap occurrences per (word, period) for tractability
HIST_BATCH = 32              # contexts per MacBERTh forward pass
HIST_MAXLEN = 96             # token cap per context window
N_SENSES = 5                 # KMeans senses for the word-sense-induction JSD


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
