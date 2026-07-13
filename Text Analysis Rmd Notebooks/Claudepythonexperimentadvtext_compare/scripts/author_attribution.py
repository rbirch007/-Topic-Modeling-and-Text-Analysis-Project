r"""
Author attribution & authorial-register profiling — Woman's Exponent vs. RSM.

This is the AUTHOR sibling of contrast.py. Like every artifact in this layer it is
a pure function of the document text + `corpus` tag (NOT the joint-STM topic model),
so it is K-INDEPENDENT and copyable across K exactly as the embeddings/contrast are.

It answers three linked questions the report's §6b (top named authors only) can't:

  Tier 1 - WHO is actually named?  Clean the raw OCR/typeset bylines into canonical
           authors: merge initials/OCR variants, resolve documented pseudonyms
           (Blanche Beechwood / Aunt Em / E. B. W. -> Emmeline B. Wells), and split
           advertisement/masthead "bylines" (DRY GOODS, WOMAN'S EXPONENT, Society
           Activities) into an ORG bucket. Driven by scripts/author_crosswalk.csv,
           which you EDIT. This alone yields the full named-author roster.

  Tier 2 - WHO PROBABLY wrote the anonymous articles?  Learn a stylometric
           fingerprint (function-word rates + character n-grams + structural stats,
           deliberately NOT topic words) for each author who has enough confidently
           bylined articles, then score every UNattributed article. Two thresholds
           are emitted side by side so you can compare:
               conservative - assign a probable author only on strong, well-separated
                              evidence (few names, each defensible)
               balanced     - a looser threshold (more coverage, more to vet)
           Below threshold the article ABSTAINS -> Misc. A machine "probable author"
           is never conflated with a real byline (own columns + provenance flag).

  Tier 3 - The Misc pile is not left as one blob: the still-unattributed articles are
           clustered stylometrically into "distinct unknown hands" (recurring
           anonymous fingerprints) you can describe without naming.

Sentiment (AFINN valence + VADER compound, per article) is attached as a DESCRIPTIVE
authorial-register attribute and rolled up per author/tier -- it is NOT used as an
identifier (sentiment tracks genre far more than authorship).

Run (needs only the joint handoff advanced_docs_K{K}.csv; no embeddings required):
    C:\Users\birch\dh-env\Scripts\python.exe scripts\author_attribution.py
    C:\Users\birch\dh-env\Scripts\python.exe scripts\author_attribution.py 20
    $env:ADV_SCENARIO="compare_full"; ...author_attribution.py 20

Tunables via env: ADV_MIN_TRAIN_DOCS (8), ADV_MIN_WORDS (150), ADV_N_HANDS (10),
    ADV_CONS_PROB (0.80), ADV_CONS_MARGIN (0.25), ADV_BAL_PROB (0.55).

Outputs (AUTHOR_DIR = output_advanced_compare/<scenario>/author/, suffixed by K):
    author_doc_K{K}.csv        one row per modeled doc: raw+canonical byline, byline
                               class/provenance, sentiment, both probable-author
                               columns w/ prob, final author under each scheme, hand.
    author_roster_K{K}.csv     one row per (corpus, canonical author) + Misc + Org
                               buckets: byline / probable(cons) / probable(bal) counts,
                               totals, mean sentiment, year span.  THE "all authors" list.
    unknown_hands_K{K}.csv     corpus, hand, n_docs, mean_sentiment, top function words,
                               example titles -- the Tier-3 distinct-hands summary.
    alias_attribution_K{K}.csv per UNRESOLVED alias -- initials (E. T., W. E.) AND single
                               -word pen names (Clio, Avondale): the full-name author it
                               stylometrically resembles, agreement across its articles,
                               mean confidence, cons/bal flags, alias_type. The alias keeps
                               its own identity in the roster; this is a SEPARATE suggestion
                               layer, never merged into the byline. Known pen names can be
                               resolved outright via crosswalk 'pseudonym'/'signature' rows.
    author_attribution_meta_K{K}.json  thresholds, per-corpus candidate list + honest
                               cross-validated accuracy, coverage under each scheme.
"""
from __future__ import annotations
import os, sys, re, json, csv as _csv
from collections import Counter, defaultdict
import numpy as np
import pandas as pd
import config as C

_csv.field_size_limit(1 << 27)

# --- knobs -------------------------------------------------------------------
MIN_TRAIN_DOCS = int(os.environ.get("ADV_MIN_TRAIN_DOCS", 8))   # docs to learn a hand
MIN_WORDS      = int(os.environ.get("ADV_MIN_WORDS", 150))      # "substantial" article
N_HANDS        = int(os.environ.get("ADV_N_HANDS", 10))         # Tier-3 clusters / corpus
CONS_PROB      = float(os.environ.get("ADV_CONS_PROB", 0.80))
CONS_MARGIN    = float(os.environ.get("ADV_CONS_MARGIN", 0.25)) # top prob minus 2nd
BAL_PROB       = float(os.environ.get("ADV_BAL_PROB", 0.55))
# Forced pool-attribution is only trustworthy when the candidate pool is large enough
# that the true author is plausibly IN it. Below this many trainable authors a corpus is
# treated as closed-world-unreliable: its anonymous articles are LEFT UNATTRIBUTED (kept
# open for manual crosswalk work) rather than dumped onto the few candidates. The alias
# resemblance table (§9f) is still produced (it's a labelled suggestion, not an assignment).
# Woman's Exponent (~16 candidates) falls below; RSM (~164) is well above.
ATTR_MIN_CANDIDATES = int(os.environ.get("ADV_ATTR_MIN_CANDIDATES", 40))
# Fold per-doc sentiment into the STYLOMETRIC feature vector? Off by default:
# sentiment is genre-driven, so using it to attribute leaks topic into authorship.
# Turn on to A/B it -- meta records CV accuracy so you can see if it actually helps.
USE_SENT_FEATURES = os.environ.get("ADV_USE_SENTIMENT_FEATURES", "0") == "1"
RANDOM_STATE   = 12345

AD_LABEL   = "Advertisement"
ORG_LABEL  = "Masthead / organization"
MISC_LABEL = "Unattributed"

CROSSWALK = C.BASE / "scripts" / "author_crosswalk.csv"
AUTHOR_DIR = C.ADV_DIR / "author"
AUTHOR_DIR.mkdir(parents=True, exist_ok=True)

# Function words = the stylometric backbone (topic-neutral; Burrows/Koppel tradition).
FUNCTION_WORDS = """a about above after again against all am an and any are as at be because
been before being below between both but by can cannot could did do does doing down during
each few for from further had has have having he her here hers herself him himself his how i
if in into is it its itself me more most my myself no nor not of off on once only or other our
ours ourselves out over own same she should so some such than that the their theirs them
themselves then there these they this those through to too under until up upon very was we were
what when where which while who whom why will with would you your yours yourself unto thee thy
thou ye shall must may might ought hath doth""".split()

# Bylines that are never a personal author even before the crosswalk (mirrors the report).
# Split into COMMERCIAL advertisements vs MASTHEAD/organisational filler so the two land
# in distinct buckets (checked ad-first, since some ad strings also mention an org word).
_AD_RX = re.compile(
    r"(\bco\b|\bco\.|\bbro\b|\bbros\b|&|compan|goods|carriage|wall paper|boots|shoes|"
    r"slipper|remed|physician|waist|tailor|dealer|agent for|manufactur|made by|grocer|"
    r"notion|electric|emporium|store|depot|ticket|emporium|jewel|drug|millinery|"
    r"furnitur|hardware|clothing|dry ?goods|for sale|prices?)", re.I)
_MASTHEAD_RX = re.compile(
    r"(exponent|magazine|publish|office|union|bureau|society|associat|department|"
    r"\bboard\b|building|street|z\. ?c\. ?m\. ?i|pacific|column|contest|conference|"
    r"advertis|committee|presidency|conference|chapter)", re.I)
# A price / money phrase is a strong advertisement signal.
_PRICE_RX = re.compile(
    r"\$\s*\d|\b\d+\s*(?:cents?|cts|dollars?)\b|\bpostpaid\b|"
    r"\bper\s+(?:year|annum|copy|dozen|yard|lb|month|week|line)\b", re.I)
# A leading page/chapter/year/price run bled onto a byline (e.g. "$5. ", "5 ", "1853 ").
_LEAD_NOISE_RX = re.compile(r"^[\[\(]*\s*\$?\s*\d[\d.,\$]*\s*[.\-]?\s+")
_MAX_NAME_WORDS = 5   # a real byline is ~2-5 tokens; more => a headline/sentence fragment
_BLANK = {"", "unknown", "na", "n/a", "none", "anonymous", "selected", "editor",
          "the editor", "correspondent", "a correspondent", "our correspondent"}
# Single-word bylines that are section/heading noise, not a person or a pen name.
_SECTION = {"congratulations", "contents", "editorial", "tuition", "notice", "notices",
            "obituary", "obituaries", "poetry", "poem", "story", "news", "notes",
            "correspondence", "communication", "communications", "index", "greetings",
            "resolutions", "minutes", "report", "reports", "announcement", "announcements",
            "advertisement", "advertisements", "miscellaneous", "selection", "selections"}
_TITLE_RX = re.compile(
    r"^(mrs?|dr|prof|professor|rev|hon|pres|president|sister|elder|bishop|"
    r"secretary-treasurer|secretary|treasurer|supt|superintendent|gen|col|capt)\.?\s+", re.I)


def normalize(raw: str) -> str:
    """Lower-case, fix the OCR '?'->apostrophe, squish, strip wrappers/trailing punct."""
    if raw is None:
        return ""
    s = str(raw).replace("﻿", "").strip()
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = s.lower()
    s = re.sub(r"^[\[\(\{\"'\-—\s]+", "", s)
    s = re.sub(r"[\]\)\}\"'\s]+$", "", s)
    s = re.sub(r"[,.;:\-—\s]+$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def strip_title(norm: str) -> str:
    """Peel a leading honorific ('president louise y. robison' -> 'louise y. robison')."""
    prev = None
    while prev != norm:
        prev = norm
        norm = _TITLE_RX.sub("", norm).strip()
    return re.sub(r",?\s+(jr|sr)\.?$", "", norm).strip()


def is_initials_only(norm: str) -> bool:
    """True for bylines that are only initials (no real >=3-letter surname word)."""
    toks = [t for t in re.split(r"[ .]+", norm) if t]
    if not toks:
        return False
    return all(len(t) <= 2 for t in toks)


def titlecase_name(norm: str) -> str:
    """Display form of a normalised full-name byline."""
    def cap(tok):
        if re.fullmatch(r"[a-z]\.?", tok):        # single initial
            return tok.upper().rstrip(".") + "."
        parts = tok.split("'")
        return "'".join(p[:1].upper() + p[1:] for p in parts)
    return " ".join(cap(t) for t in norm.split())


# --- Tier 1: byline classification ------------------------------------------
def load_crosswalk():
    exact, regexes, signatures, titles = {}, [], [], []
    if not CROSSWALK.exists():
        print(f"[author] no crosswalk at {CROSSWALK} (Tier-1 rules only).", flush=True)
        return exact, regexes, signatures, titles
    def flex(pat):                       # whitespace/hyphen-tolerant multiword regex
        toks = [re.escape(t) for t in pat.lower().split()]
        return r"[\s-]*".join(toks)
    with open(CROSSWALK, encoding="utf-8-sig", newline="") as f:
        for line in f:
            if line.lstrip().startswith("#") or not line.strip():
                continue
            row = next(_csv.reader([line]))
            if len(row) < 4 or row[0] == "match_type":
                continue
            mt, pat, canon, kind = (row[0].strip(), row[1].strip(),
                                    row[2].strip(), row[3].strip().lower())
            if mt == "exact":
                exact[pat] = (canon, kind)
            elif mt == "regex":
                regexes.append((re.compile(pat, re.I), canon, kind))
            elif mt == "title":
                titles.append((re.compile(r"\s+".join(re.escape(t) for t in pat.lower().split()),
                                          re.I), canon))
            elif mt == "signature":
                signatures.append((re.compile(r"\b" + flex(pat) + r"\b", re.I), canon))
    print(f"[author] crosswalk: {len(exact)} exact + {len(regexes)} regex + "
          f"{len(titles)} title + {len(signatures)} signature rules.", flush=True)
    return exact, regexes, signatures, titles


def classify_byline(raw, exact, regexes):
    """Return (canonical_display, byline_class, byline_source).

    class  in {named, advertisement, org, unknown}
        named         -> a personal identity (full name OR unresolved recurring initials)
        advertisement -> commercial ad byline (Dry Goods, & Co, Baby Carriages, ...)
        org           -> masthead / organisational filler (Woman's Exponent, Society, ...)
        unknown       -> truly anonymous ('Unknown'/blank) -> Tier-2 attribution pool
    source in {byline, pseudonym, signature, initials, initials_raw, pseudonym_raw, ad, org, none}
        pseudonym    = a documented pen name mapped to a person via the crosswalk
        signature    = a pen name found in the article TEXT tail (set in main, not here)
        initials     = initials mapped to a person via the crosswalk (inferred)
        initials_raw = an UNRESOLVED recurring initials byline, kept as its own identity
        pseudonym_raw= an UNRESOLVED single-word pen-name byline (Clio), kept as its own
                       identity; both *_raw kinds are surfaced in the alias table (§9f)
    """
    norm = normalize(raw)
    if norm in _BLANK:
        return ("", "unknown", "none")
    has_price = bool(_PRICE_RX.search(norm))
    # strip a leading page/chapter/year/price run bled onto the byline, e.g.
    # "$5. Maggie W. Thompson" -> "maggie w. thompson"; "5 Dorothy S. Romney" -> "..."
    core = _LEAD_NOISE_RX.sub("", norm).strip()
    core = re.sub(r"^\$[\d.,]*\s*", "", core).strip()
    if not core or core in _BLANK:                       # nothing left after the price/number
        return ("", "advertisement", "ad") if has_price else ("", "unknown", "none")

    # 1) explicit crosswalk wins (matched on the CLEANED byline)
    if core in exact:
        canon, kind = exact[core]
        if kind == "org":
            return (canon, "org", "org")
        src = {"person": "byline", "pseudonym": "pseudonym", "initials": "initials"}.get(kind, "byline")
        return (canon, "named", src)
    for rx, canon, kind in regexes:
        if rx.search(core):
            if kind == "org":
                return (canon, "org", "org")
            src = {"person": "byline", "pseudonym": "pseudonym", "initials": "initials"}.get(kind, "byline")
            return (canon, "named", src)
    # 2) a byline carrying a price with NO personal name left -> advertisement
    name_like = 2 <= len(core.split()) <= _MAX_NAME_WORDS and not re.search(r"\d", core)
    if has_price and not name_like:
        return ("", "advertisement", "ad")
    # 3) built-in ad / masthead detectors (advertisement wins over masthead)
    if _AD_RX.search(core):
        return ("", "advertisement", "ad")
    if _MASTHEAD_RX.search(core):
        return ("", "org", "org")
    # 4) strip an honorific, re-check for emptiness
    ns = strip_title(core)
    if not ns or ns in _BLANK:
        return ("", "unknown", "none")
    # 5) a recurring initials-only byline -> KEEP it as its own (unresolved) identity.
    if is_initials_only(ns):
        disp = ns.upper().replace(".", "").strip()
        disp = " ".join(p + "." for p in disp.split())
        return (disp, "named", "initials_raw")
    # 6) headline / sentence fragment (too many words, or interior digits) -> NOT a byline
    if len(ns.split()) > _MAX_NAME_WORDS or re.search(r"\d", ns):
        return ("", "advertisement", "ad") if has_price else ("", "unknown", "none")
    # 7) a single-word byline: section noise -> org; otherwise a likely PEN NAME kept as
    #    its own unresolved identity (Clio, Avondale) -> surfaced in the alias table.
    toks = ns.split()
    if len(toks) == 1 and re.fullmatch(r"[a-z]{4,}", toks[0]):
        if toks[0] in _SECTION:
            return ("", "org", "org")
        return (titlecase_name(ns), "named", "pseudonym_raw")
    # 8) a real (multi-word) named byline
    return (titlecase_name(ns), "named", "byline")


# --- stylometric features ----------------------------------------------------
_WORD_RX = re.compile(r"[A-Za-z]+")

def structural_features(text: str) -> dict:
    t = str(text)
    words = _WORD_RX.findall(t)
    n = max(len(words), 1)
    sents = re.split(r"[.!?]+", t)
    sents = [s for s in sents if s.strip()]
    wl = [len(w) for w in words] or [0]
    return {
        "s_mean_word_len": float(np.mean(wl)),
        "s_ttr": len(set(w.lower() for w in words)) / n,   # type-token ratio
        "s_comma": t.count(",") / n,
        "s_period": t.count(".") / n,
        "s_semicolon": t.count(";") / n,
        "s_colon": t.count(":") / n,
        "s_question": t.count("?") / n,
        "s_exclaim": t.count("!") / n,
        "s_dash": (t.count("-") + t.count("—")) / n,
        "s_quote": (t.count('"') + t.count("'")) / n,
        "s_upper": sum(c.isupper() for c in t) / max(len(t), 1),
        "s_digit": sum(c.isdigit() for c in t) / max(len(t), 1),
        "s_mean_sent_len": n / max(len(sents), 1),
    }


def funcword_features(text: str, fw_index: dict) -> np.ndarray:
    words = _WORD_RX.findall(str(text).lower())
    n = max(len(words), 1)
    vec = np.zeros(len(fw_index), dtype=np.float32)
    cnt = Counter(words)
    for w, i in fw_index.items():
        if w in cnt:
            vec[i] = cnt[w] / n
    return vec


def build_feature_matrix(texts):
    """Dense stylometric matrix: function-word rates + structural + char n-grams (SVD)."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.decomposition import TruncatedSVD
    from sklearn.preprocessing import StandardScaler

    fw_index = {w: i for i, w in enumerate(FUNCTION_WORDS)}
    FW = np.vstack([funcword_features(t, fw_index) for t in texts])
    ST = pd.DataFrame([structural_features(t) for t in texts]).to_numpy(np.float32)

    char = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 4),
                           min_df=5, max_features=20000, lowercase=True)
    Xc = char.fit_transform(texts)
    ndim = min(80, Xc.shape[1] - 1) if Xc.shape[1] > 1 else 1
    svd = TruncatedSVD(n_components=ndim, random_state=RANDOM_STATE)
    CH = svd.fit_transform(Xc).astype(np.float32)

    X = np.hstack([FW, ST, CH])
    X = StandardScaler().fit_transform(X)
    return np.nan_to_num(X, copy=False)


# --- sentiment ---------------------------------------------------------------
def build_sentiment():
    """AFINN valence/word (fast dict lookup over AFINN's own lexicon -- ~20x faster
    than Afinn.score()'s multi-word regex, identical on single-word entries) plus an
    optional VADER compound. Both are DESCRIPTIVE register attributes, not identifiers."""
    lex = None
    try:
        from afinn import Afinn
        lex = getattr(Afinn(language="en"), "_dict", None)
    except Exception as e:
        print(f"[author] afinn unavailable ({e})", flush=True)
    vader = None
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        vader = SentimentIntensityAnalyzer()
    except Exception as e:
        print(f"[author] vader unavailable ({e})", flush=True)

    tok_rx = re.compile(r"[a-z']+")

    def score(text, n_words):
        t = str(text)
        if lex is not None:
            toks = tok_rx.findall(t.lower())
            a = sum(lex.get(w, 0) for w in toks) / max(len(toks), 1)   # valence / word
        else:
            a = float("nan")
        v = float(vader.polarity_scores(t[:3000])["compound"]) if vader else float("nan")
        return a, v
    return score, (lex is not None), (vader is not None)


# --- main --------------------------------------------------------------------
def main():
    K = C.resolve_K(sys.argv[1] if len(sys.argv) > 1 else None)
    docs = pd.read_csv(C.docs_csv(K), dtype=str, keep_default_na=False)
    docs["row_index"] = docs["row_index"].astype(int)
    if "n_words" in docs:
        docs["n_words"] = pd.to_numeric(docs["n_words"], errors="coerce").fillna(0).astype(int)
    else:
        docs["n_words"] = docs["text"].str.split().str.len().fillna(0).astype(int)
    docs["year"] = pd.to_numeric(docs.get("year", ""), errors="coerce")
    corpora = sorted(docs[C.CORPUS_COL].dropna().unique())
    print(f"[author] K={K}  {len(docs)} docs  corpora={corpora}", flush=True)

    # ---- Tier 1: classify every byline ----
    exact, regexes, signatures, titles = load_crosswalk()
    cls = docs["author"].map(lambda a: classify_byline(a, exact, regexes))
    docs["canonical_author"] = [c[0] for c in cls]
    docs["byline_class"]     = [c[1] for c in cls]
    docs["byline_source"]    = [c[2] for c in cls]

    # ---- title / pen-name recovery for still-anonymous articles ----
    #   (a) TITLE rules: a serialized column with a stable title (e.g. "Prince of Ur")
    #       -> its known author. Precise, no body-text false positives.
    #   (b) SIGNATURE: the pen name (whitespace-flexible) appended to the TITLE -> author.
    #       TITLE-ONLY on purpose: scanning body text matched the common adjective
    #       "homespun" and mis-tagged unrelated articles, so it is deliberately excluded.
    if titles or signatures:
        has_title = "title" in docs.columns
        n_title = n_sig = 0
        for gi in docs.index[docs["byline_class"] == "unknown"]:
            ttl = str(docs.at[gi, "title"]) if has_title else ""
            hit = None
            for rx, canon in titles:
                if rx.search(ttl):
                    hit = ("title", canon); break
            if hit is None:
                for rx, canon in signatures:
                    if rx.search(ttl):
                        hit = ("signature", canon); break
            if hit:
                docs.at[gi, "canonical_author"] = hit[1]
                docs.at[gi, "byline_class"] = "named"
                docs.at[gi, "byline_source"] = hit[0]
                if hit[0] == "title": n_title += 1
                else: n_sig += 1
        print(f"[author] recovery: {n_title} by title rule + {n_sig} by pen-name signature "
              f"-> resolved from anonymous to named.", flush=True)

    # ---- sentiment (descriptive) ----
    score, has_afinn, has_vader = build_sentiment()
    print(f"[author] scoring sentiment for {len(docs)} docs (afinn={has_afinn} vader={has_vader})...", flush=True)
    sa, sv = [], []
    for i, (t, n) in enumerate(zip(docs["text"], docs["n_words"])):
        a, v = score(t, n)
        sa.append(a); sv.append(v)
        if (i + 1) % 10000 == 0:
            print(f"[author]   sentiment {i + 1}/{len(docs)}", flush=True)
    docs["sentiment_afinn"] = sa
    docs["sentiment_vader"] = sv
    print(f"[author] sentiment done.", flush=True)

    # attribution output columns (filled per corpus below)
    for c in ("attr_cons_author", "attr_bal_author", "alias_probable_author", "alias_type"):
        docs[c] = ""
    for c in ("attr_cons_prob", "attr_bal_prob", "alias_probable_prob"):
        docs[c] = np.nan
    docs["unknown_hand"] = ""
    docs.loc[docs["byline_source"] == "initials_raw", "alias_type"] = "initials"
    docs.loc[docs["byline_source"] == "pseudonym_raw", "alias_type"] = "pen name"
    ri_to_idx = {ri: idx for idx, ri in zip(docs.index, docs["row_index"])}

    meta = {"K": K, "min_train_docs": MIN_TRAIN_DOCS, "min_words": MIN_WORDS,
            "n_hands": N_HANDS, "cons_prob": CONS_PROB, "cons_margin": CONS_MARGIN,
            "bal_prob": BAL_PROB, "use_sentiment_features": USE_SENT_FEATURES,
            "sentiment": {"afinn": has_afinn, "vader": has_vader},
            "corpora": {}}

    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import cross_val_predict
    from sklearn.cluster import KMeans

    # confident training labels = full-name bylines + documented pseudonyms
    # (NOT 'initials' -- those are inferred, so we don't train the model on them).
    TRAIN_SRC = {"byline", "pseudonym"}

    for corp in corpora:
        cm = docs[C.CORPUS_COL] == corp
        sub = docs[cm & (docs["n_words"] >= MIN_WORDS)].copy()
        pretty = C.CORPUS_PRETTY.get(corp, corp)
        if sub.empty:
            continue

        # features for every substantial doc in this corpus (shared by Tier 2 & 3)
        print(f"[author] {pretty}: featurizing {len(sub)} substantial docs...", flush=True)
        X = build_feature_matrix(sub["text"].tolist())
        if USE_SENT_FEATURES:
            from sklearn.preprocessing import StandardScaler
            extra = StandardScaler().fit_transform(
                sub[["sentiment_afinn", "sentiment_vader"]].to_numpy(np.float32))
            X = np.hstack([X, np.nan_to_num(extra)])
        pos = {ri: i for i, ri in enumerate(sub["row_index"].tolist())}

        train_mask = (sub["byline_class"] == "named") & (sub["byline_source"].isin(TRAIN_SRC))
        counts = sub.loc[train_mask, "canonical_author"].value_counts()
        candidates = counts[counts >= MIN_TRAIN_DOCS].index.tolist()
        cov = {"n_substantial": int(len(sub)),
               "n_named_byline": int((sub["byline_class"] == "named").sum()),
               "n_advertisement": int((sub["byline_class"] == "advertisement").sum()),
               "n_org": int((sub["byline_class"] == "org").sum()),
               "n_unknown_pool": int((sub["byline_class"] == "unknown").sum()),
               "n_candidates": len(candidates), "candidates": candidates}

        pool_mask = sub["byline_class"] == "unknown"      # anonymous -> attributable
        # unresolved ALIASES (initials + single-word pen names) -> SEPARATE scoring
        alias_mask = sub["byline_source"].isin(["initials_raw", "pseudonym_raw"])
        if len(candidates) >= 2 and (pool_mask.any() or alias_mask.any()):
            tr = sub[train_mask & sub["canonical_author"].isin(candidates)]
            Xtr = X[[pos[ri] for ri in tr["row_index"]]]
            ytr = tr["canonical_author"].to_numpy()
            clf = LogisticRegression(max_iter=3000, C=10.0, class_weight="balanced")
            # honest self-accuracy via stratified CV on the labelled set
            try:
                cvpred = cross_val_predict(clf, Xtr, ytr, cv=5)
                cv_acc = float((cvpred == ytr).mean())
            except Exception as e:
                cv_acc = float("nan"); print(f"[author]  CV failed: {e}", flush=True)
            clf.fit(Xtr, ytr)
            classes = clf.classes_

            def score_top(mask):
                sm = sub[mask]
                Xm = X[[pos[ri] for ri in sm["row_index"]]]
                pm = clf.predict_proba(Xm)
                od = np.argsort(-pm, axis=1)
                t1 = pm[np.arange(len(pm)), od[:, 0]]
                t2 = pm[np.arange(len(pm)), od[:, 1]] if pm.shape[1] > 1 else np.zeros(len(pm))
                return sm["row_index"].to_numpy(), classes[od[:, 0]], t1, t2

            # (a) anonymous pool -> probable-author columns, ONLY if the candidate pool is
            #     large enough to be trustworthy. Otherwise leave anonymous UNATTRIBUTED.
            attribute_pool = len(candidates) >= ATTR_MIN_CANDIDATES
            cov["attribution_suppressed"] = not attribute_pool
            if pool_mask.any() and attribute_pool:
                ris, best, top, second = score_top(pool_mask)
                cons_ok = (top >= CONS_PROB) & ((top - second) >= CONS_MARGIN)
                bal_ok = top >= BAL_PROB
                for ri, b, p, co, ba in zip(ris, best, top, cons_ok, bal_ok):
                    gi = ri_to_idx[ri]
                    if ba:
                        docs.at[gi, "attr_bal_author"] = b
                        docs.at[gi, "attr_bal_prob"] = round(float(p), 4)
                    if co:
                        docs.at[gi, "attr_cons_author"] = b
                        docs.at[gi, "attr_cons_prob"] = round(float(p), 4)
                cov["n_probable_cons"] = int(cons_ok.sum())
                cov["n_probable_bal"] = int(bal_ok.sum())
            else:
                cov["n_probable_cons"] = cov["n_probable_bal"] = 0
                if not attribute_pool:
                    print(f"[author]  {pretty}: pool attribution SUPPRESSED "
                          f"({len(candidates)} candidates < {ATTR_MIN_CANDIDATES}); "
                          f"{int(pool_mask.sum())} anonymous -> Unattributed (edit later).", flush=True)

            # (b) unresolved-alias docs (initials + pen names) -> SEPARATE columns
            #     (identity is NOT changed; this only suggests which full-name author the
            #     style resembles, for manual review)
            if alias_mask.any():
                ris, best, top, _ = score_top(alias_mask)
                for ri, b, p in zip(ris, best, top):
                    gi = ri_to_idx[ri]
                    docs.at[gi, "alias_probable_author"] = b
                    docs.at[gi, "alias_probable_prob"] = round(float(p), 4)
                cov["n_alias_docs_scored"] = int(alias_mask.sum())

            cov["cv_accuracy"] = cv_acc
            print(f"[author]  {pretty}: {len(candidates)} candidate authors, CV acc={cv_acc:.2f}; "
                  f"probable cons={cov.get('n_probable_cons')} bal={cov.get('n_probable_bal')} "
                  f"of {int(pool_mask.sum())} anonymous; aliases scored={int(alias_mask.sum())}", flush=True)
        else:
            cov["cv_accuracy"] = None
            cov["n_probable_cons"] = cov["n_probable_bal"] = 0
            print(f"[author]  {pretty}: too few candidates ({len(candidates)}) — attribution skipped.", flush=True)

        # ---- Tier 3: distinct unknown hands over the STILL-Misc (under conservative) pool ----
        still = sub[pool_mask].copy()
        cons_named = {ri for ri in still["row_index"]
                      if docs.at[ri_to_idx[ri], "attr_cons_author"]}
        still = still[~still["row_index"].isin(cons_named)]
        if len(still) >= N_HANDS * 3:
            Xs = X[[pos[ri] for ri in still["row_index"]]]
            km = KMeans(n_clusters=N_HANDS, random_state=RANDOM_STATE, n_init=10).fit(Xs)
            tag = corp.split("_")[0][:3].upper()
            labels = [f"{tag}-hand-{c:02d}" for c in km.labels_]
            for ri, lab in zip(still["row_index"], labels):
                docs.at[ri_to_idx[ri], "unknown_hand"] = lab
            cov["n_hands_docs"] = int(len(still))
        else:
            cov["n_hands_docs"] = 0

        meta["corpora"][corp] = cov

    # ---- final author under each scheme ----
    def final_author(row, scheme):
        if row["byline_class"] == "named":
            return row["canonical_author"]
        if row["byline_class"] == "advertisement":
            return AD_LABEL
        if row["byline_class"] == "org":
            return ORG_LABEL
        col = "attr_cons_author" if scheme == "cons" else "attr_bal_author"
        if row[col]:
            return row[col]
        return MISC_LABEL
    docs["final_author_cons"] = docs.apply(lambda r: final_author(r, "cons"), axis=1)
    docs["final_author_bal"]  = docs.apply(lambda r: final_author(r, "bal"), axis=1)

    # ---- write per-doc ----
    doc_cols = ["row_index", "doc_id", C.CORPUS_COL, "year", "n_words", "title",
                "author", "canonical_author", "byline_class", "byline_source",
                "sentiment_afinn", "sentiment_vader",
                "attr_cons_author", "attr_cons_prob", "attr_bal_author", "attr_bal_prob",
                "alias_type", "alias_probable_author", "alias_probable_prob",
                "final_author_cons", "final_author_bal", "unknown_hand"]
    doc_cols = [c for c in doc_cols if c in docs.columns]
    docs[doc_cols].to_csv(AUTHOR_DIR / f"author_doc_K{K}.csv", index=False)

    # ---- alias-attribution summary: which full-name author each UNRESOLVED alias
    #      (initials OR pen name) stylometrically resembles (identity itself unchanged) ----
    init_rows = []
    idf = docs[docs["byline_source"].isin(["initials_raw", "pseudonym_raw"]) &
               (docs["alias_probable_author"] != "")]
    for (corp, ident), g in idf.groupby([C.CORPUS_COL, "canonical_author"]):
        vc = g["alias_probable_author"].value_counts()
        best = vc.index[0]
        agree = float(vc.iloc[0] / len(g))                      # share of its docs pointing there
        best_prob = float(g.loc[g["alias_probable_author"] == best, "alias_probable_prob"].mean())
        runner = vc.index[1] if len(vc) > 1 else ""
        init_rows.append(dict(
            corpus=corp, alias=ident, alias_type=g["alias_type"].iloc[0], n_docs=int(len(g)),
            probable_author=best, agreement=round(agree, 3),
            mean_confidence=round(best_prob, 4),
            meets_conservative=bool(best_prob >= CONS_PROB and agree >= 0.5),
            meets_balanced=bool(best_prob >= BAL_PROB),
            also_possible=runner,
            mean_sentiment_afinn=round(float(g["sentiment_afinn"].mean()), 4),
            year_min=float(g["year"].min()), year_max=float(g["year"].max())))
    if init_rows:
        pd.DataFrame(init_rows).sort_values(
            ["corpus", "n_docs"], ascending=[True, False]).to_csv(
            AUTHOR_DIR / f"alias_attribution_K{K}.csv", index=False)
        n_pen = sum(1 for r in init_rows if r["alias_type"] == "pen name")
        print(f"[author] alias-attribution: {len(init_rows)} unresolved aliases scored "
              f"({len(init_rows) - n_pen} initials, {n_pen} pen names).", flush=True)

    # ---- roster: one row per (corpus, canonical author) + Misc/Org buckets ----
    roster_rows = []
    for corp in corpora:
        cm = docs[C.CORPUS_COL] == corp
        d = docs[cm]
        # named authors (Tier 1)
        named = d[d["byline_class"] == "named"]
        for auth, g in named.groupby("canonical_author"):
            n_bal = int((docs[cm & (docs["attr_bal_author"] == auth)]).shape[0])
            n_cons = int((docs[cm & (docs["attr_cons_author"] == auth)]).shape[0])
            n_full = int((g["byline_source"].isin(["byline", "pseudonym", "signature", "title"])).sum())
            n_init = int((g["byline_source"].isin(["initials", "initials_raw"])).sum())
            # named if it ever appears as a full byline/pseudonym/signature; else an
            # unresolved alias identity (pen name if single-word, else initials)
            if n_full > 0:
                row_kind = "named"
            elif (g["byline_source"] == "pseudonym_raw").any():
                row_kind = "pen_name_unresolved"
            else:
                row_kind = "initials_unresolved"
            roster_rows.append(dict(
                corpus=corp, canonical_author=auth, kind=row_kind,
                n_byline=n_full,
                n_byline_initials=n_init,
                n_probable_cons=n_cons, n_probable_bal=n_bal,
                n_total_cons=len(g) + n_cons, n_total_bal=len(g) + n_bal,
                mean_sentiment_afinn=round(float(g["sentiment_afinn"].mean()), 4),
                mean_sentiment_vader=round(float(g["sentiment_vader"].mean()), 4),
                year_min=float(g["year"].min()), year_max=float(g["year"].max())))
        # Advertisement + Masthead/org buckets
        for bucket, lbl in (("advertisement", AD_LABEL), ("org", ORG_LABEL)):
            g = d[d["byline_class"] == bucket]
            if len(g):
                roster_rows.append(dict(
                    corpus=corp, canonical_author=lbl, kind=bucket,
                    n_byline=len(g), n_byline_initials=0,
                    n_probable_cons=0, n_probable_bal=0, n_total_cons=len(g), n_total_bal=len(g),
                    mean_sentiment_afinn=round(float(g["sentiment_afinn"].mean()), 4),
                    mean_sentiment_vader=round(float(g["sentiment_vader"].mean()), 4),
                    year_min=float(g["year"].min()), year_max=float(g["year"].max())))
        for scheme in ("cons", "bal"):
            misc = d[d[f"final_author_{scheme}"] == MISC_LABEL]
            roster_rows.append(dict(
                corpus=corp, canonical_author=f"{MISC_LABEL} ({scheme})", kind="misc",
                n_byline=0, n_byline_initials=0,
                n_probable_cons=0, n_probable_bal=0,
                n_total_cons=len(misc) if scheme == "cons" else 0,
                n_total_bal=len(misc) if scheme == "bal" else 0,
                mean_sentiment_afinn=round(float(misc["sentiment_afinn"].mean()), 4) if len(misc) else float("nan"),
                mean_sentiment_vader=round(float(misc["sentiment_vader"].mean()), 4) if len(misc) else float("nan"),
                year_min=float(misc["year"].min()) if len(misc) else float("nan"),
                year_max=float(misc["year"].max()) if len(misc) else float("nan")))
    roster = pd.DataFrame(roster_rows).sort_values(
        ["corpus", "n_total_cons"], ascending=[True, False])
    roster.to_csv(AUTHOR_DIR / f"author_roster_K{K}.csv", index=False)

    # ---- unknown-hands summary ----
    fw_index = {w: i for i, w in enumerate(FUNCTION_WORDS)}
    hand_rows = []
    hd = docs[docs["unknown_hand"] != ""]
    for (corp, hand), g in hd.groupby([C.CORPUS_COL, "unknown_hand"]):
        # distinctive function words: mean rate in hand vs rest of that corpus's pool
        rest = docs[(docs[C.CORPUS_COL] == corp) & (docs["unknown_hand"] != "") &
                    (docs["unknown_hand"] != hand)]
        fw_g = np.vstack([funcword_features(t, fw_index) for t in g["text"]]).mean(0)
        fw_r = (np.vstack([funcword_features(t, fw_index) for t in rest["text"]]).mean(0)
                if len(rest) else np.zeros(len(fw_index)))
        lift = (fw_g + 1e-6) / (fw_r + 1e-6)
        topw = [FUNCTION_WORDS[i] for i in np.argsort(-lift)[:8]]
        titles = [t for t in g.sort_values("n_words", ascending=False)["title"].head(3)]
        hand_rows.append(dict(
            corpus=corp, hand=hand, n_docs=len(g),
            mean_sentiment_afinn=round(float(g["sentiment_afinn"].mean()), 4),
            mean_sentiment_vader=round(float(g["sentiment_vader"].mean()), 4),
            year_min=float(g["year"].min()), year_max=float(g["year"].max()),
            top_function_words=", ".join(topw),
            example_titles=" | ".join(str(t)[:60] for t in titles)))
    if hand_rows:
        pd.DataFrame(hand_rows).sort_values(["corpus", "n_docs"], ascending=[True, False]).to_csv(
            AUTHOR_DIR / f"unknown_hands_K{K}.csv", index=False)

    # ---- coverage summary + meta ----
    for corp in corpora:
        cm = docs[C.CORPUS_COL] == corp
        n = int(cm.sum())
        for scheme in ("cons", "bal"):
            fa = docs.loc[cm, f"final_author_{scheme}"]
            named = int((~fa.isin([MISC_LABEL, AD_LABEL, ORG_LABEL])).sum())
            meta["corpora"][corp][f"coverage_{scheme}"] = {
                "n_docs": n,
                "attributed_or_named": named,
                "pct": round(100 * named / n, 1),
                "misc": int((fa == MISC_LABEL).sum()),
                "advertisement": int((fa == AD_LABEL).sum()),
                "org": int((fa == ORG_LABEL).sum())}
    (AUTHOR_DIR / f"author_attribution_meta_K{K}.json").write_text(
        json.dumps(meta, indent=2, default=str))

    print(f"[author] DONE. roster={len(roster)} rows -> {AUTHOR_DIR}", flush=True)
    for corp in corpora:
        cc = meta["corpora"].get(corp, {})
        print(f"[author]  {C.CORPUS_PRETTY.get(corp, corp)}: "
              f"named-byline={cc.get('n_named_byline')} ad={cc.get('n_advertisement')} "
              f"org={cc.get('n_org')} "
              f"anon-pool={cc.get('n_unknown_pool')} candidates={cc.get('n_candidates')} "
              f"cov_cons={cc.get('coverage_cons', {}).get('pct')}% "
              f"cov_bal={cc.get('coverage_bal', {}).get('pct')}%", flush=True)


if __name__ == "__main__":
    main()
