#!/usr/bin/env python3
"""
Preprocessing pipeline for topic modeling on Relief Society Magazine extracts.

Reads all extracted articles from processed/regex_and_llm/vol*/
and produces a clean corpus suitable for LDA/NMF/BERTopic analysis.

Output:
  - processed/topic_modeling/corpus.jsonl  (one JSON doc per line)
  - processed/topic_modeling/corpus_stats.json  (summary statistics)

Each output document contains:
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
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = PROJECT_ROOT / "processed" / "regex_and_llm"
OUTPUT_DIR = PROJECT_ROOT / "processed" / "topic_modeling"

# Volume -> year mapping
VOL_YEARS = {}
for v in range(30, 58):
    # Vol 30 = 1943, Vol 31 = 1944, ...
    VOL_YEARS[v] = 1913 + v

# Files to exclude (not article content)
EXCLUDE_FILENAMES = {"TOC.txt", "MISC.txt", "ADS.txt"}

# Regex patterns for noise removal
_PAGE_DATE_RE = re.compile(
    r'\b\d{1,3}\s+'
    r'(?:January|February|March|April|May|June|'
    r'July|August|September|October|November|December)\s+'
    r'19\d{2}\b',
    re.IGNORECASE
)

_STANDALONE_PAGE_NUM_RE = re.compile(r'^\s*\d{1,3}\s*$', re.MULTILINE)

_SEPARATOR_RE = re.compile(r'^[\-_=]{3,}\s*$', re.MULTILINE)

# Lines that are predominantly non-alphabetic (OCR garbage)
_GARBAGE_LINE_RE = re.compile(r'^[^a-zA-Z]*$', re.MULTILINE)

# Known OCR garbage patterns
_OCR_GARBAGE_RE = re.compile(
    r'[A-Z]{2,}\s+[A-Z]{2,}\s+[A-Z]{2,}\s+[A-Z]{2,}'  # "SS SQ XS MD OD"
    r'|'
    r'[KkXx][oa][onrs]\s+[KkXx][oa][onrs]\s+[KkXx][oa][onrs]'  # "Kon Koo Kos"
    r'|'
    r'[A-Z0-9]{5,}\s+[A-Z0-9]{5,}'  # Long all-caps/digit strings
)

_AT_SYMBOL_RE = re.compile(r'\s*@\s*')

# Running header patterns (month names in ALL CAPS within text)
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

    # Remove page number + date strings (e.g., "243 January 1969")
    cleaned = _PAGE_DATE_RE.sub(' ', cleaned)

    # Remove standalone page numbers on their own lines
    cleaned = _STANDALONE_PAGE_NUM_RE.sub('', cleaned)

    # Remove separator lines
    cleaned = _SEPARATOR_RE.sub('', cleaned)

    # Remove @ symbols (OCR artifacts)
    cleaned = _AT_SYMBOL_RE.sub(' ', cleaned)

    # Remove running headers
    cleaned = _RUNNING_HEADER_RE.sub(' ', cleaned)
    cleaned = _MAGAZINE_HEADER_RE.sub(' ', cleaned)

    # Remove lines that are OCR garbage (high ratio of non-alpha chars)
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
            continue  # Skip garbage lines
        if _OCR_GARBAGE_RE.search(stripped):
            continue
        filtered_lines.append(line)
    cleaned = '\n'.join(filtered_lines)

    # Collapse multiple whitespace
    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

    return cleaned.strip()


def word_count(text: str) -> int:
    """Count words in text."""
    return len(text.split())


def load_volume_entries(vol_dir: Path) -> list[dict]:
    """Load entries from a volume's entries JSON file."""
    # Find the entries JSON file
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


def main():
    parser = argparse.ArgumentParser(description="Prepare corpus for topic modeling")
    parser.add_argument("--min-words", type=int, default=50,
                        help="Minimum word count to include (default: 50)")
    parser.add_argument("--exclude-poems", action="store_true",
                        help="Exclude poem entries")
    parser.add_argument("--exclude-types", nargs="*", default=[],
                        help="Entry types to exclude (e.g., poem editorial)")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_docs = []
    stats = {
        "volumes_processed": 0,
        "total_entries": 0,
        "entries_included": 0,
        "entries_excluded_type": 0,
        "entries_excluded_filename": 0,
        "entries_excluded_short": 0,
        "entries_excluded_no_content": 0,
        "by_type": {},
        "by_volume": {},
        "min_words": args.min_words,
    }

    exclude_types = set(args.exclude_types)
    if args.exclude_poems:
        exclude_types.add("poem")

    # Process all volumes
    vol_dirs = sorted(INPUT_DIR.glob("[Vv]ol*"))
    for vol_dir in vol_dirs:
        if not vol_dir.is_dir():
            continue

        vol_name = vol_dir.name.lower()
        vol_num = int(re.search(r'\d+', vol_name).group())
        year = VOL_YEARS.get(vol_num, 0)

        entries = load_volume_entries(vol_dir)
        if not entries:
            continue

        stats["volumes_processed"] += 1
        vol_included = 0

        for entry in entries:
            stats["total_entries"] += 1

            # Content and file info may be nested under different keys
            # depending on the extraction script generation:
            #   Vol36+: single "match" key with {file, path, content, ...}
            #   Vol30-35: "strict_match" and "loose_match" keys
            match_data = entry.get("match", {})
            if not match_data:
                # Old schema (vol30-35): prefer strict_match, fall back to loose_match
                match_data = entry.get("strict_match", {}) or entry.get("loose_match", {})

            filename = match_data.get("file", "") or entry.get("file", "")
            entry_path = match_data.get("path", "") or entry.get("path", "")

            # Skip excluded file types
            if filename in EXCLUDE_FILENAMES:
                stats["entries_excluded_filename"] += 1
                continue

            # Normalize entry type
            etype = entry.get("etype", "article")
            # Old schema (vol30-35) has messy etypes with page numbers/author info
            if ':' in etype or len(etype) > 30:
                etype_lower = etype.lower()
                if 'poem' in etype_lower or 'poetry' in etype_lower:
                    etype = 'poem'
                elif 'fiction' in etype_lower or 'story' in etype_lower or 'serial' in etype_lower:
                    etype = 'fiction'
                elif 'lesson' in etype_lower or 'work_meeting' in etype_lower or 'visiting_teacher' in etype_lower:
                    etype = 'lesson'
                elif 'editorial' in etype_lower:
                    etype = 'editorial'
                else:
                    etype = 'article'
            # Normalize known variants
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
            etype = etype_map.get(etype, etype)

            if etype in exclude_types:
                stats["entries_excluded_type"] += 1
                continue

            # Get content from match data or file
            content = match_data.get("content", "") or entry.get("content", "")
            if not content:
                # Try reading from file
                if entry_path and filename:
                    file_path = PROJECT_ROOT / entry_path / filename
                    if file_path.exists() and file_path.is_file():
                        content = file_path.read_text(encoding="utf-8", errors="replace")
                if not content:
                    stats["entries_excluded_no_content"] += 1
                    continue

            # Clean text
            cleaned = clean_text(content)

            # Check word count
            wc = word_count(cleaned)
            if wc < args.min_words:
                stats["entries_excluded_short"] += 1
                continue

            # Build document
            doc_id = f"{vol_name}_{entry.get('month', 'unknown')}_{entry.get('index', 0):02d}"
            doc = {
                "doc_id": doc_id,
                "volume": vol_name,
                "month": entry.get("month", ""),
                "title": entry.get("title", ""),
                "author": entry.get("author"),
                "etype": etype,
                "year": year,
                "word_count": wc,
                "text": cleaned,
            }
            all_docs.append(doc)
            vol_included += 1

            # Update type stats
            stats["by_type"][etype] = stats["by_type"].get(etype, 0) + 1

        stats["by_volume"][vol_name] = vol_included

    stats["entries_included"] = len(all_docs)

    # Write corpus
    corpus_path = OUTPUT_DIR / "corpus.jsonl"
    with open(corpus_path, "w") as f:
        for doc in all_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    # Write stats
    stats_path = OUTPUT_DIR / "corpus_stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    # Summary
    print(f"\n{'='*60}")
    print(f"TOPIC MODELING CORPUS PREPARED")
    print(f"{'='*60}")
    print(f"Volumes processed: {stats['volumes_processed']}")
    print(f"Total entries scanned: {stats['total_entries']}")
    print(f"Entries included: {stats['entries_included']}")
    print(f"  Excluded (type filter): {stats['entries_excluded_type']}")
    print(f"  Excluded (TOC/MISC/ADS): {stats['entries_excluded_filename']}")
    print(f"  Excluded (< {args.min_words} words): {stats['entries_excluded_short']}")
    print(f"  Excluded (no content): {stats['entries_excluded_no_content']}")
    print(f"\nBy type:")
    for etype, count in sorted(stats["by_type"].items()):
        print(f"  {etype}: {count}")
    print(f"\nCorpus written to: {corpus_path}")
    print(f"Stats written to: {stats_path}")

    # Word count distribution
    wcs = [d["word_count"] for d in all_docs]
    if wcs:
        print(f"\nWord count stats:")
        print(f"  Total docs: {len(wcs)}")
        print(f"  Total words: {sum(wcs):,}")
        print(f"  Min: {min(wcs)}, Max: {max(wcs)}")
        print(f"  Median: {sorted(wcs)[len(wcs)//2]}")
        print(f"  Mean: {sum(wcs)//len(wcs)}")


if __name__ == "__main__":
    main()
