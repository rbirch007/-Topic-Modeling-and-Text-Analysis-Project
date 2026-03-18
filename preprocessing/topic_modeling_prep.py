#!/usr/bin/env python3
"""
Preprocessing pipeline for topic modeling on Relief Society Magazine extracts.

Reads extracted articles from BOTH pipelines:
  - processed/regex_and_llm/vol*/       (vol4-12, vol29-57 — JSON metadata)
  - processed/R_RS_article_extracted/    (vol1-3, vol13-28 — filename metadata)

Produces a fully self-contained topic-modeling-inputs/ directory
suitable for LDA/NMF/BERTopic analysis.

The output directory is an independent stage in the data pipeline:
  raw-data/ → cleaned-data/ → processed/ → topic-modeling-inputs/

Each stage contains real copies of the data (no symlinks or pointers).

Output (all under topic-modeling-inputs/):
  - articles/vol##/Month/##_Title.txt    (cleaned article text files)
  - metadata/vol##_entries.json          (per-volume metadata)
  - corpus.jsonl                         (one JSON doc per line — primary model input)
  - manifest.csv                         (flat index of all included documents)
  - corpus_stats.json                    (summary statistics)

Each corpus.jsonl document contains:
  {
    "doc_id": "vol52_January_03",
    "volume": "vol52",
    "month": "January",
    "title": "...",
    "author": "...",
    "etype": "article|poem|fiction|lesson|editorial",
    "year": 1965,
    "word_count": 523,
    "text": "cleaned article text..."
  }

Usage:
    python topic_modeling_prep.py
    python topic_modeling_prep.py --min-words 100
    python topic_modeling_prep.py --exclude-poems
    python topic_modeling_prep.py --exclude-types poem editorial
"""

import argparse
import csv
import json
import re
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGEX_LLM_DIR = PROJECT_ROOT / "processed" / "regex_and_llm"
R_RS_DIR = PROJECT_ROOT / "processed" / "R_RS_article_extracted" / "extracted_articles"
OUTPUT_DIR = PROJECT_ROOT / "topic-modeling-inputs"

# Volume -> year mapping (Vol 1 = 1914, Vol 2 = 1915, ...)
VOL_YEARS = {}
for v in range(1, 58):
    VOL_YEARS[v] = 1913 + v

# Files/patterns to exclude (not article content)
EXCLUDE_FILENAMES = {"TOC.txt", "MISC.txt", "ADS.txt"}

# R_RS filename exclusion patterns
_R_RS_EXCLUDE_PATTERNS = [
    r'_00_CONTENTS',
    r'_00_BOARD',
    r'_00_FRONT_MATTER',
    r'_00_Misc',
    r'_Misc_front_matter',
    r'_Misc_back_matter',
    r'_99_ADVERTISERS',
    r"_ADVERTISERS'_DIRECTORY",
]
_R_RS_EXCLUDE_RE = re.compile('|'.join(_R_RS_EXCLUDE_PATTERNS))

# R_RS etype mapping from filename type tokens
_R_RS_ETYPE_MAP = {
    'article': 'article',
    'poetry': 'poem',
    'poem': 'poem',
    'editorial': 'editorial',
    'frontispiece': 'article',
    'fiction': 'fiction',
    'story': 'fiction',
    'serial': 'fiction',
    'lesson': 'lesson',
    'misc': 'article',
}

# ---------------------------------------------------------------------------
# Regex patterns for noise removal
# ---------------------------------------------------------------------------
_PAGE_DATE_RE = re.compile(
    r'\b\d{1,3}\s+'
    r'(?:January|February|March|April|May|June|'
    r'July|August|September|October|November|December)\s+'
    r'19\d{2}\b',
    re.IGNORECASE
)

_STANDALONE_PAGE_NUM_RE = re.compile(r'^\s*\d{1,3}\s*$', re.MULTILINE)

_SEPARATOR_RE = re.compile(r'^[\-_=]{3,}\s*$', re.MULTILINE)

_OCR_GARBAGE_RE = re.compile(
    # Known OCR gibberish patterns: "Kon Koo Kos"
    r'[KkXx][oa][onrs]\s+[KkXx][oa][onrs]\s+[KkXx][oa][onrs]'
    r'|'
    # Alphanumeric hash-like strings (not real words): "X8K2M QZLT9"
    r'(?=\S*\d)\S{5,}\s+(?=\S*\d)\S{5,}'
)

_AT_SYMBOL_RE = re.compile(r'\s*@\s*')

_RUNNING_HEADER_RE = re.compile(
    r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|'
    r'JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+19\d{2}',
    re.IGNORECASE
)

_MAGAZINE_HEADER_RE = re.compile(
    r'(?:RELIEF\s+)?SOCIETY\s+MAGAZINE'
    r'(?:[\s\u2014\u2013\-]*'
    r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|'
    r'JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)'
    r'[\s,]*(?:19\d{2})?)?',
    re.IGNORECASE
)


def clean_text(text: str) -> str:
    """Remove noise from extracted article text for topic modeling."""
    cleaned = text

    cleaned = _PAGE_DATE_RE.sub(' ', cleaned)
    cleaned = _STANDALONE_PAGE_NUM_RE.sub('', cleaned)
    cleaned = _SEPARATOR_RE.sub('', cleaned)
    cleaned = _AT_SYMBOL_RE.sub(' ', cleaned)
    cleaned = _RUNNING_HEADER_RE.sub(' ', cleaned)
    cleaned = _MAGAZINE_HEADER_RE.sub(' ', cleaned)

    lines = cleaned.split('\n')
    filtered_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            filtered_lines.append(line)
            continue
        alpha_count = sum(1 for c in stripped if c.isalpha())
        total_count = len(stripped)
        if total_count > 5 and alpha_count / total_count < 0.4:
            continue
        if _OCR_GARBAGE_RE.search(stripped):
            continue
        filtered_lines.append(line)
    cleaned = '\n'.join(filtered_lines)

    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

    return cleaned.strip()


def word_count(text: str) -> int:
    return len(text.split())


def normalize_etype(etype: str) -> str:
    """Normalize entry type to one of: article, poem, fiction, lesson, editorial."""
    if ':' in etype or len(etype) > 30:
        etype_lower = etype.lower()
        if 'poem' in etype_lower or 'poetry' in etype_lower:
            return 'poem'
        elif 'fiction' in etype_lower or 'story' in etype_lower or 'serial' in etype_lower:
            return 'fiction'
        elif 'lesson' in etype_lower or 'work_meeting' in etype_lower or 'visiting_teacher' in etype_lower:
            return 'lesson'
        elif 'editorial' in etype_lower:
            return 'editorial'
        else:
            return 'article'

    etype_map = {
        'poetry': 'poem',
        'serial_fiction': 'fiction',
        'special_features': 'article',
        'general_features': 'article',
        'features_for_the_home': 'article',
        'front_matter': 'article',
        'misc': 'article',
        'report': 'article',
        'story': 'fiction',
        'special_short_stories': 'fiction',
        'lesson_department': 'lesson',
        'lessons_and_previews': 'lesson',
        'letter': 'article',
        'visiting_teachers_message': 'lesson',
        'visiting_teacher_message': 'lesson',
    }
    return etype_map.get(etype, etype)


# ---------------------------------------------------------------------------
# regex_and_llm pipeline ingestion
# ---------------------------------------------------------------------------

def load_volume_entries(vol_dir: Path) -> list[dict]:
    """Load entries from a volume's entries JSON file."""
    json_files = list(vol_dir.glob("*_entries.json"))
    if not json_files:
        return []

    with open(json_files[0]) as f:
        data = json.load(f)

    entries = []
    vol_name = data.get("volume", vol_dir.name)

    for month_name, month_data in data.get("months", {}).items():
        for entry in month_data.get("entries", []):
            entry["volume"] = vol_name
            entry["month"] = month_name
            entries.append(entry)

    return entries


def get_content(entry: dict, match_data: dict) -> str:
    """Extract content from entry, trying match data first, then file on disk."""
    content = match_data.get("content", "") or entry.get("content", "")
    if not content:
        entry_path = match_data.get("path", "") or entry.get("path", "")
        filename = match_data.get("file", "") or entry.get("file", "")
        if entry_path and filename:
            file_path = PROJECT_ROOT / entry_path / filename
            if file_path.exists() and file_path.is_file():
                content = file_path.read_text(encoding="utf-8", errors="replace")
    return content


def ingest_regex_llm(exclude_types: set, min_words: int) -> list[dict]:
    """Ingest all volumes from the regex_and_llm pipeline.

    Returns list of normalized entry dicts ready for output.
    """
    results = []

    vol_dirs = sorted(REGEX_LLM_DIR.glob("[Vv]ol*"))
    for vol_dir in vol_dirs:
        if not vol_dir.is_dir():
            continue
        if "old_schema" in vol_dir.name:
            continue

        vol_name = vol_dir.name.lower()
        vol_num = int(re.search(r'\d+', vol_name).group())
        year = VOL_YEARS.get(vol_num, 0)

        entries = load_volume_entries(vol_dir)
        if not entries:
            continue

        for entry in entries:
            match_data = entry.get("match", {})
            if not match_data:
                match_data = entry.get("strict_match", {}) or entry.get("loose_match", {})

            filename = match_data.get("file", "") or entry.get("file", "")

            if filename in EXCLUDE_FILENAMES:
                continue

            etype = normalize_etype(entry.get("etype", "article"))
            if etype in exclude_types:
                continue

            content = get_content(entry, match_data)
            if not content:
                continue

            cleaned = clean_text(content)
            wc = word_count(cleaned)
            if wc < min_words:
                continue

            month = entry.get("month", "unknown")
            index = entry.get("index", 0)

            if filename:
                out_filename = filename
            else:
                safe_title = re.sub(r'[^\w\s-]', '', entry.get("title", "untitled"))
                safe_title = re.sub(r'\s+', '_', safe_title.strip())[:80]
                out_filename = f"{index:02d}_{safe_title}.txt"

            results.append({
                "vol_name": vol_name,
                "vol_num": vol_num,
                "year": year,
                "month": month,
                "index": index,
                "title": entry.get("title", ""),
                "author": entry.get("author"),
                "etype": etype,
                "out_filename": out_filename,
                "cleaned_text": cleaned,
                "word_count": wc,
                "source": "regex_and_llm",
            })

    return results


# ---------------------------------------------------------------------------
# R_RS pipeline ingestion — metadata parsed from filenames
# ---------------------------------------------------------------------------

# Month abbreviation map for Vol18's dir names (Vol18-Jan1931 etc.)
_MONTH_ABBREV = {
    'jan': 'January', 'feb': 'February', 'mar': 'March',
    'apr': 'April', 'april': 'April', 'may': 'May', 'jun': 'June',
    'june': 'June', 'jul': 'July', 'july': 'July', 'aug': 'August',
    'august': 'August', 'sep': 'September', 'september': 'September',
    'oct': 'October', 'october': 'October', 'nov': 'November',
    'november': 'November', 'dec': 'December', 'december': 'December',
    'january': 'January', 'february': 'February', 'march': 'March',
}

# Patterns for parsing R_RS filenames
# Vol 1-3, 13-16:  {Month}_Vol{N}_{NN}_{Type}_{Title}_{Author}.txt
# Vol 17, 19-28:   {Month}_Vol{NN}_No{N}_{NN}_{Type}_{Title}_{Author}.txt
# Vol 18:          {Month}_Vol18_Article{NN}_{Title}_{Author}.txt

_R_RS_PAT_STANDARD = re.compile(
    r'^(?P<month>[A-Za-z]+)_Vol\d+_(?P<index>\d+)_(?P<type>[A-Za-z]+)_(?P<rest>.+)\.txt$'
)
_R_RS_PAT_WITH_ISSUE = re.compile(
    r'^(?P<month>[A-Za-z]+)_Vol\d+_No\d+_(?P<index>\d+)_(?P<type>[A-Za-z]+)_(?P<rest>.+)\.txt$'
)
_R_RS_PAT_VOL18 = re.compile(
    r'^(?P<month>[A-Za-z]+)_Vol18_(?P<type>Article|Board|Contents|Misc)(?P<index>\d*)_?(?P<rest>.*)\.txt$'
)
# Vol1 has no type field:  {Month}_Vol1_{NN}_{TITLE}.txt
_R_RS_PAT_VOL1_NOTTYPE = re.compile(
    r'^(?P<month>[A-Za-z]+)_Vol\d+_(?P<index>\d+)_(?P<rest>.+)\.txt$'
)


def parse_r_rs_filename(filename: str, vol_num: int) -> dict | None:
    """Parse metadata from an R_RS filename. Returns dict or None if excluded."""
    # Check exclusion patterns
    if _R_RS_EXCLUDE_RE.search(filename):
        return None

    # Also exclude bare Misc files (e.g., "November_Vol18_Misc.txt")
    if filename.endswith('_Misc.txt') or filename.endswith('_Board.txt') or filename.endswith('_Contents.txt'):
        return None

    parsed = {"filename": filename}

    if vol_num == 18:
        m = _R_RS_PAT_VOL18.match(filename)
        if m:
            parsed["month"] = m.group("month")
            parsed["etype_raw"] = m.group("type").lower()
            idx = m.group("index")
            parsed["index"] = int(idx) if idx else 0
            rest = m.group("rest")
            parsed["title"], parsed["author"] = _split_title_author(rest)
            return parsed
        # Fall through to other patterns

    # Try with-issue pattern first (Vol17, 19-28)
    m = _R_RS_PAT_WITH_ISSUE.match(filename)
    if m:
        parsed["month"] = m.group("month")
        parsed["index"] = int(m.group("index"))
        parsed["etype_raw"] = m.group("type").lower()
        parsed["title"], parsed["author"] = _split_title_author(m.group("rest"))
        return parsed

    # Standard pattern (Vol 1-3, 13-16)
    m = _R_RS_PAT_STANDARD.match(filename)
    if m:
        type_str = m.group("type")
        # Check if this looks like a real type or just part of title (Vol1)
        if type_str.lower() in _R_RS_ETYPE_MAP or type_str.lower() in ('article', 'poetry', 'poem', 'editorial', 'frontispiece', 'fiction', 'story', 'misc'):
            parsed["month"] = m.group("month")
            parsed["index"] = int(m.group("index"))
            parsed["etype_raw"] = type_str.lower()
            parsed["title"], parsed["author"] = _split_title_author(m.group("rest"))
            return parsed
        else:
            # No type field — rest of filename is title (e.g. Vol1)
            parsed["month"] = m.group("month")
            parsed["index"] = int(m.group("index"))
            parsed["etype_raw"] = "article"
            full_rest = type_str + "_" + m.group("rest")
            parsed["title"], parsed["author"] = _split_title_author(full_rest)
            return parsed

    # Last resort: Vol1-style with no type field
    m = _R_RS_PAT_VOL1_NOTTYPE.match(filename)
    if m:
        parsed["month"] = m.group("month")
        parsed["index"] = int(m.group("index"))
        parsed["etype_raw"] = "article"
        parsed["title"], parsed["author"] = _split_title_author(m.group("rest"))
        return parsed

    return None


def _split_title_author(rest: str) -> tuple[str, str | None]:
    """Split the rest of a filename into title and author.

    The last underscore-separated segment(s) are often the author name.
    Heuristic: if the last segment looks like a name (2-4 capitalized words),
    treat it as author.
    """
    # Replace underscores with spaces for the title
    title = rest.replace('_', ' ').strip()
    title = re.sub(r'\s+', ' ', title)

    # Try to detect author at the end
    # Common pattern: "Title_Words_Author_Name" where author is last 1-3 segments
    parts = rest.split('_')

    # Don't try to extract author if too few parts
    if len(parts) <= 2:
        return title, None

    # Check if last 1-3 parts look like an author name
    for n_author_parts in (3, 2, 1):
        if len(parts) <= n_author_parts:
            continue
        candidate = ' '.join(parts[-n_author_parts:])
        title_parts = parts[:-n_author_parts]

        # Author heuristic: each word is capitalized, reasonable length,
        # contains only letters/periods/hyphens
        words = candidate.split()
        if all(
            w[0].isupper() and
            len(w) >= 2 and
            re.match(r'^[A-Za-z.\-\']+$', w)
            for w in words
        ) and 1 <= len(words) <= 4:
            # Make sure we're not just grabbing the end of a title
            # Exclude common title words that look like names
            title_words = {'The', 'Her', 'His', 'Our', 'New', 'Old', 'One', 'Two',
                           'All', 'For', 'And', 'But', 'Not', 'May', 'Can'}
            if len(words) == 1 and words[0] in title_words:
                continue
            candidate_title = ' '.join(title_parts).replace('_', ' ').strip()
            if candidate_title:
                return candidate_title, candidate

    return title, None


def _resolve_month(dirname: str, filename_month: str, vol_num: int) -> str:
    """Resolve month name, handling Vol18's special dir names."""
    # For Vol18, dir names like "Vol18-Jan1931"
    if vol_num == 18:
        m = re.match(r'Vol18[-_](\w+?)(\d{4})?$', dirname)
        if m:
            abbrev = m.group(1).lower()
            return _MONTH_ABBREV.get(abbrev, filename_month.capitalize())

    # Normal case: month from filename or directory name
    month = filename_month.capitalize()
    return _MONTH_ABBREV.get(month.lower(), month)


def ingest_r_rs(exclude_types: set, min_words: int) -> list[dict]:
    """Ingest all volumes from the R_RS_article_extracted pipeline.

    Returns list of normalized entry dicts ready for output.
    """
    results = []

    if not R_RS_DIR.exists():
        return results

    for vol_dir in sorted(R_RS_DIR.iterdir()):
        if not vol_dir.is_dir():
            continue

        dirname = vol_dir.name
        # Parse volume number and year from dirname (e.g., "Vol13-1926", "Vol19_1932")
        m = re.match(r'Vol(\d+)[-_](\d{4})', dirname)
        if not m:
            continue
        vol_num = int(m.group(1))
        year = int(m.group(2))
        vol_name = f"vol{vol_num}"

        # Collect all .txt files across month subdirectories
        txt_files = []
        for item in sorted(vol_dir.iterdir()):
            if item.is_file() and item.suffix == '.txt':
                txt_files.append((item, dirname))
            elif item.is_dir():
                for f in sorted(item.iterdir()):
                    if f.is_file() and f.suffix == '.txt':
                        txt_files.append((f, item.name))

        for txt_path, parent_dirname in txt_files:
            parsed = parse_r_rs_filename(txt_path.name, vol_num)
            if parsed is None:
                continue

            # Map etype
            etype_raw = parsed.get("etype_raw", "article")
            etype = _R_RS_ETYPE_MAP.get(etype_raw, "article")
            etype = normalize_etype(etype)

            if etype in exclude_types:
                continue

            # Read content
            content = txt_path.read_text(encoding="utf-8", errors="replace")
            if not content.strip():
                continue

            cleaned = clean_text(content)
            wc = word_count(cleaned)
            if wc < min_words:
                continue

            month = _resolve_month(parent_dirname, parsed.get("month", "unknown"), vol_num)
            index = parsed.get("index", 0)
            title = parsed.get("title", "")
            author = parsed.get("author")

            # Generate a clean output filename
            safe_title = re.sub(r'[^\w\s-]', '', title)
            safe_title = re.sub(r'\s+', '_', safe_title.strip())[:80]
            out_filename = f"{index:02d}_{safe_title}.txt" if safe_title else f"{index:02d}_untitled.txt"

            results.append({
                "vol_name": vol_name,
                "vol_num": vol_num,
                "year": year,
                "month": month,
                "index": index,
                "title": title,
                "author": author,
                "etype": etype,
                "out_filename": out_filename,
                "cleaned_text": cleaned,
                "word_count": wc,
                "source": "R_RS",
            })

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build self-contained topic-modeling-inputs/ from all extracted articles"
    )
    parser.add_argument("--min-words", type=int, default=50,
                        help="Minimum word count to include (default: 50)")
    parser.add_argument("--exclude-poems", action="store_true",
                        help="Exclude poem entries")
    parser.add_argument("--exclude-types", nargs="*", default=[],
                        help="Entry types to exclude (e.g., poem editorial)")
    parser.add_argument("--clean", action="store_true",
                        help="Remove existing topic-modeling-inputs/ before rebuilding")
    args = parser.parse_args()

    exclude_types = set(args.exclude_types)
    if args.exclude_poems:
        exclude_types.add("poem")

    # Prepare output directory
    if args.clean and OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    articles_dir = OUTPUT_DIR / "articles"
    metadata_dir = OUTPUT_DIR / "metadata"
    articles_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    # Ingest from both pipelines
    print("Ingesting from regex_and_llm pipeline...")
    regex_llm_entries = ingest_regex_llm(exclude_types, args.min_words)
    print(f"  → {len(regex_llm_entries)} entries from regex_and_llm")

    print("Ingesting from R_RS_article_extracted pipeline...")
    r_rs_entries = ingest_r_rs(exclude_types, args.min_words)
    print(f"  → {len(r_rs_entries)} entries from R_RS")

    # Merge — check for volume overlap (there shouldn't be any)
    regex_llm_vols = {e["vol_num"] for e in regex_llm_entries}
    r_rs_vols = {e["vol_num"] for e in r_rs_entries}
    overlap = regex_llm_vols & r_rs_vols
    if overlap:
        print(f"  WARNING: overlapping volumes {sorted(overlap)} — regex_and_llm takes precedence")
        r_rs_entries = [e for e in r_rs_entries if e["vol_num"] not in overlap]

    all_entries = regex_llm_entries + r_rs_entries
    # Sort by volume, then month order, then index
    month_order = {m: i for i, m in enumerate([
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ])}
    all_entries.sort(key=lambda e: (e["vol_num"], month_order.get(e["month"], 99), e["index"]))

    # Write output
    all_docs = []
    manifest_rows = []
    vol_metadata_map = {}  # vol_name -> metadata dict
    stats = {
        "volumes_processed": 0,
        "total_entries_included": 0,
        "by_type": {},
        "by_volume": {},
        "by_source": {"regex_and_llm": 0, "R_RS": 0},
        "min_words": args.min_words,
        "exclude_types": sorted(exclude_types),
    }

    seen_vols = set()

    for entry in all_entries:
        vol_name = entry["vol_name"]
        month = entry["month"]
        index = entry["index"]
        doc_id = f"{vol_name}_{month}_{index:02d}"

        # Write individual cleaned article file
        article_month_dir = articles_dir / vol_name / month
        article_month_dir.mkdir(parents=True, exist_ok=True)

        out_filename = entry["out_filename"]
        article_path = article_month_dir / out_filename

        # Handle filename collisions
        if article_path.exists():
            base = article_path.stem
            suffix = article_path.suffix
            counter = 2
            while article_path.exists():
                article_path = article_month_dir / f"{base}_{counter}{suffix}"
                out_filename = article_path.name
                counter += 1

        article_path.write_text(entry["cleaned_text"], encoding="utf-8")

        # Build corpus document
        doc = {
            "doc_id": doc_id,
            "volume": vol_name,
            "month": month,
            "title": entry["title"],
            "author": entry["author"],
            "etype": entry["etype"],
            "year": entry["year"],
            "word_count": entry["word_count"],
            "text": entry["cleaned_text"],
        }
        all_docs.append(doc)

        # Build manifest row
        manifest_rows.append({
            "doc_id": doc_id,
            "file": out_filename,
            "path": str(article_path.relative_to(OUTPUT_DIR)),
            "volume": vol_name,
            "month": month,
            "year": entry["year"],
            "etype": entry["etype"],
            "title": entry["title"],
            "author": entry["author"] or "",
            "word_count": entry["word_count"],
            "source": entry["source"],
        })

        # Per-volume metadata
        if vol_name not in vol_metadata_map:
            vol_metadata_map[vol_name] = {
                "volume": vol_name,
                "year": entry["year"],
                "months": {},
            }
        vm = vol_metadata_map[vol_name]
        if month not in vm["months"]:
            vm["months"][month] = {"entries": []}
        vm["months"][month]["entries"].append({
            "index": index,
            "title": entry["title"],
            "author": entry["author"],
            "etype": entry["etype"],
            "file": out_filename,
            "path": str(article_path.relative_to(OUTPUT_DIR)),
            "word_count": entry["word_count"],
        })

        # Stats
        seen_vols.add(vol_name)
        stats["by_type"][entry["etype"]] = stats["by_type"].get(entry["etype"], 0) + 1
        stats["by_volume"][vol_name] = stats["by_volume"].get(vol_name, 0) + 1
        stats["by_source"][entry["source"]] += 1

    stats["volumes_processed"] = len(seen_vols)
    stats["total_entries_included"] = len(all_docs)

    # Write per-volume metadata files
    for vol_name, vm in vol_metadata_map.items():
        meta_path = metadata_dir / f"{vol_name}_entries.json"
        with open(meta_path, "w") as f:
            json.dump(vm, f, indent=2, ensure_ascii=False)

    # Write corpus.jsonl
    corpus_path = OUTPUT_DIR / "corpus.jsonl"
    with open(corpus_path, "w") as f:
        for doc in all_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    # Write manifest.csv
    manifest_path = OUTPUT_DIR / "manifest.csv"
    if manifest_rows:
        fieldnames = [
            "doc_id", "file", "path", "volume", "month", "year",
            "etype", "title", "author", "word_count", "source",
        ]
        with open(manifest_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(manifest_rows)

    # Write stats
    stats_path = OUTPUT_DIR / "corpus_stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    # Summary
    print(f"\n{'='*60}")
    print(f"TOPIC MODELING INPUTS BUILT")
    print(f"{'='*60}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Volumes processed: {stats['volumes_processed']}")
    print(f"Total entries included: {stats['total_entries_included']}")
    print(f"  From regex_and_llm: {stats['by_source']['regex_and_llm']}")
    print(f"  From R_RS: {stats['by_source']['R_RS']}")
    print(f"\nBy type:")
    for etype, count in sorted(stats["by_type"].items()):
        print(f"  {etype}: {count}")
    print(f"\nBy volume:")
    for vol, count in sorted(stats["by_volume"].items(),
                              key=lambda x: int(re.search(r'\d+', x[0]).group())):
        yr = VOL_YEARS.get(int(re.search(r'\d+', vol).group()), '?')
        print(f"  {vol} ({yr}): {count} docs")

    # Coverage check
    all_vol_nums = sorted({int(re.search(r'\d+', v).group()) for v in seen_vols})
    full_range = set(range(1, 58))
    missing = sorted(full_range - set(all_vol_nums))
    if missing:
        print(f"\nMissing volumes: {['vol' + str(v) for v in missing]}")
    else:
        print(f"\nFull coverage: vol1-vol57 (1914-1970)")

    # Word count distribution
    wcs = [d["word_count"] for d in all_docs]
    if wcs:
        print(f"\nWord count stats:")
        print(f"  Total docs: {len(wcs)}")
        print(f"  Total words: {sum(wcs):,}")
        print(f"  Min: {min(wcs)}, Max: {max(wcs)}")
        print(f"  Median: {sorted(wcs)[len(wcs)//2]}")
        print(f"  Mean: {sum(wcs)//len(wcs)}")

    print(f"\nOutput files:")
    print(f"  {corpus_path.relative_to(PROJECT_ROOT)}")
    print(f"  {manifest_path.relative_to(PROJECT_ROOT)}")
    print(f"  {stats_path.relative_to(PROJECT_ROOT)}")
    print(f"  {articles_dir.relative_to(PROJECT_ROOT)}/  ({len(manifest_rows)} article files)")
    print(f"  {metadata_dir.relative_to(PROJECT_ROOT)}/  ({stats['volumes_processed']} volume metadata files)")


if __name__ == "__main__":
    main()
