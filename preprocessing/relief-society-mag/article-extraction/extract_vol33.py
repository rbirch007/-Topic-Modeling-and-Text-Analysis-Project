#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 33 (1946).

KEY STRATEGY:
- Uses loose regex patterns from TOC (optional descriptors, flexible spacing)
- NEVER relies on newlines for boundary detection
- Uses PAGE NUMBERS as primary content boundaries
- Uses SECTION HEADERS as secondary boundaries
- Capitaliz ation patterns as tertiary markers

Usage:
    python extract_vol33.py
    python extract_vol33.py --dry-run
    python extract_vol33.py --verbose
"""

import argparse
import json
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEAN_DIR = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"
OUTPUT_DIR = PROJECT_ROOT / "processed"

# Import TOC data
import vol33_toc

TOC = vol33_toc.TOC

# ---------------------------------------------------------------------------
# Filename mapping for Vol33
# ---------------------------------------------------------------------------
ISSUE_FILES = {}
for no, month, year in [
    ("No1", "January", "1946"), ("No2", "February", "1946"),
    ("No3", "March", "1946"), ("No4", "April", "1946"),
    ("No5", "May", "1946"), ("No6", "June", "1946"),
    ("No7", "July", "1946"), ("No8", "August", "1946"),
    ("No9", "September", "1946"), ("No10", "October", "1946"),
    ("No11", "November", "1946"), ("No12", "December", "1946"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"vol33_{key}.txt"
    ISSUE_FILES[("Vol33", key)] = (fname, month)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def sanitize_filename(s: str, max_len: int = 80) -> str:
    """Turn a title string into a safe filename component."""
    s = s.strip()
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    s = re.sub(r'[\s\-,;.!\'()]+', '_', s)
    s = re.sub(r'_+', '_', s)
    s = s.strip('_')
    if len(s) > max_len:
        s = s[:max_len].rstrip('_')
    return s


def split_toc_and_body(text: str) -> tuple:
    """Split at 'PUBLISHED MONTHLY BY' marker."""
    marker = "PUBLISHED MONTHLY BY THE GENERAL BOARD"
    idx = text.find(marker)
    if idx == -1:
        return "", text
    return text[:idx], text[idx:]


def find_all_page_numbers(text: str) -> list:
    """
    Find all page numbers in text (3+ consecutive digits).
    Returns list of (position, page_number) tuples.
    """
    matches = []
    for m in re.finditer(r'\b(\d{3,4})\b', text):
        matches.append((m.start(), int(m.group(1))))
    return matches


def find_section_headers(text: str) -> list:
    """
    Find major section headers (not requiring newlines).
    Returns list of (position, header_name) tuples.
    """
    headers = [
        "SPECIAL FEATURES", "FICTION", "GENERAL FEATURES",
        "POETRY", "LESSON DEPARTMENT", "THEOLOGY",
        "VISITING TEACHERS", "LITERATURE", "SOCIAL SCIENCE"
    ]
    matches = []
    for header in headers:
        for m in re.finditer(re.escape(header), text, re.IGNORECASE):
            matches.append((m.start(), header))
    return sorted(matches)


def find_title_in_body(title_pattern: str, author_pattern: str, body_text: str) -> tuple:
    """
    Find title match using loose regex.
    Returns (start_position, end_position, matched_text) or (None, None, None).
    """
    # Try matching title with author first
    if author_pattern:
        combined = f"({title_pattern})\\s+({author_pattern})"
        match = re.search(combined, body_text, re.IGNORECASE)
        if match:
            return (match.start(), match.end(), match.group(0))

    # Fall back to title only
    match = re.search(title_pattern, body_text, re.IGNORECASE)
    if match:
        return (match.start(), match.end(), match.group(0))

    return None, None, None


def find_content_boundary(body_text: str, start_pos: int) -> int:
    """
    Find where article ends using:
    1. Next page number (primary)
    2. Next section header (secondary)
    3. Next title-like pattern (tertiary)
    No reliance on newlines.
    """
    remaining = body_text[start_pos:]

    # Find next page number
    page_matches = re.finditer(r'\b(\d{3,4})\b', remaining)
    page_positions = [m.start() for m in page_matches]

    # Find next section header
    headers = [
        "SPECIAL FEATURES", "FICTION", "GENERAL FEATURES",
        "POETRY", "LESSON DEPARTMENT"
    ]
    header_positions = []
    for header in headers:
        for m in re.finditer(re.escape(header), remaining, re.IGNORECASE):
            header_positions.append(m.start())

    # Combine and find nearest boundary (prefer page numbers, then headers)
    boundaries = sorted(page_positions + header_positions)

    if boundaries:
        # Take first boundary that's at least 50 chars away (to avoid matching boundary in header)
        for boundary in boundaries:
            if boundary > 50:
                return start_pos + boundary

    # No boundary found, use remaining text
    return len(body_text)


def clean_extracted_text(text: str) -> str:
    """Remove page numbers and boilerplate."""
    # Remove standalone page numbers
    text = re.sub(r'\b\d{3,4}\b(?=\s|$)', '', text)
    # Remove common headers/boilerplate
    text = re.sub(r'(?i)relief\s+society\s+magazine', '', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
    return text.strip()


def extract_issue(text: str, entries: list, vol: str, month: str,
                  source_filename: str, output_dir: Path,
                  dry_run: bool = False, verbose: bool = False) -> dict:
    """Extract a single month's issue using loose regex + page boundaries."""

    toc_section, body_section = split_toc_and_body(text)

    # Prepare output directory
    issue_dir = output_dir / vol / month
    rel_dir = f"processed/{vol}/{month}"

    if not dry_run:
        issue_dir.mkdir(parents=True, exist_ok=True)

    # Extract each entry
    json_entries = []

    for idx, entry in enumerate(entries, 1):
        title_pattern = entry.get("title_pattern")
        author_pattern = entry.get("author_pattern")
        title_display = entry.get("title", "Unknown")
        etype = entry.get("etype", "article")

        if not title_pattern:
            # Skip entries with no pattern
            continue

        # Find title in body
        start_pos, end_pos, matched = find_title_in_body(title_pattern, author_pattern, body_section)

        if start_pos is None:
            # Entry not found
            json_entries.append({
                "index": idx,
                "title": title_display,
                "author": entry.get("author"),
                "etype": etype,
                "strict_loose_identical": False,
                "strict_match": {"file": None, "path": rel_dir, "content": ""},
                "loose_match": {"file": None, "path": rel_dir, "content": ""},
            })
            continue

        # Find content boundary (page number or section header)
        end_boundary = find_content_boundary(body_section, end_pos)

        # Extract content
        raw_content = body_section[start_pos:end_boundary]
        cleaned = clean_extracted_text(raw_content)

        if len(cleaned) < 20:
            # Too short, likely didn't find actual article
            json_entries.append({
                "index": idx,
                "title": title_display,
                "author": entry.get("author"),
                "etype": etype,
                "strict_loose_identical": False,
                "strict_match": {"file": None, "path": rel_dir, "content": ""},
                "loose_match": {"file": None, "path": rel_dir, "content": ""},
            })
            continue

        # Write file
        title_safe = sanitize_filename(title_display)
        filename = f"{idx:02d}_{title_safe}.txt"

        if not dry_run:
            (issue_dir / filename).write_text(cleaned, encoding="utf-8")

        # Create JSON entry
        json_entries.append({
            "index": idx,
            "title": title_display,
            "author": entry.get("author"),
            "etype": etype,
            "strict_loose_identical": True,
            "strict_match": {
                "file": filename,
                "path": rel_dir,
                "position": start_pos,
                "length": len(raw_content),
                "content": cleaned,
            },
            "loose_match": {
                "file": filename,
                "path": rel_dir,
                "content": cleaned,
            },
        })

    return {
        "source_file": source_filename,
        "source_path": "cleaned-data/relief-society/txtvolumesbymonth/vol33",
        "entries": json_entries,
        "toc": {"file": "TOC.txt", "path": rel_dir, "content": ""},
        "ads": {"file": "ADS.txt", "path": rel_dir, "content": ""},
        "misc": {"file": "MISC.txt", "path": rel_dir, "content": toc_section[:1000]},
    }


def main():
    parser = argparse.ArgumentParser(description="Extract Vol33 Relief Society Magazine")
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--verbose', action='store_true', help='Detailed output')
    args = parser.parse_args()

    vol = "Vol33"
    all_month_data = {}

    for (v, issue_key), (filename, month) in ISSUE_FILES.items():
        toc_key = (v, issue_key)

        if toc_key not in TOC:
            if args.verbose:
                print(f"WARNING: No TOC for {v} {issue_key}")
            continue

        toc_entries = TOC[toc_key]
        source_path = CLEAN_DIR / vol.lower() / filename

        if not source_path.exists():
            print(f"ERROR: Source not found: {source_path}")
            continue

        with open(source_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()

        if args.verbose:
            print(f"Extracting {vol} {month}...")

        month_data = extract_issue(
            text, toc_entries, vol.lower(), month, filename,
            OUTPUT_DIR / "regex_and_llm",
            dry_run=args.dry_run,
            verbose=args.verbose
        )

        all_month_data[month] = month_data

    # Write global JSON
    if not args.dry_run and all_month_data:
        output_file = OUTPUT_DIR / "regex_and_llm" / vol.lower() / f"{vol.lower()}_entries.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        global_data = {"volume": vol, "months": all_month_data}

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(global_data, f, indent=2)

        print(f"✓ Written: {output_file}")

    print(f"✓ Extraction complete for {vol}")


if __name__ == "__main__":
    main()
