#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volumes 33-40.

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched using two strategies (strict line-start and loose
anywhere-match) and both results are written as separate text files plus
a per-volume JSON containing full content.  See processed/README.md for
schema documentation.

Usage:
    python extract_vol33-40.py
    python extract_vol33-40.py --dry-run
    python extract_vol33-40.py --volume 33
"""

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # up from article-extraction → relief-society-mag → preprocessing → root
CLEAN_DIR = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"
OUTPUT_DIR = PROJECT_ROOT / "processed"

# ---------------------------------------------------------------------------
# Helper to sanitize filenames
# ---------------------------------------------------------------------------
def sanitize_filename(s: str, max_len: int = 80) -> str:
    """Turn a title/author string into a safe filename component."""
    s = s.strip()
    # Replace characters not safe for filenames
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    # Replace spaces and runs of special chars with underscores
    s = re.sub(r'[\s\-,;.!\'()]+', '_', s)
    # Collapse multiple underscores
    s = re.sub(r'_+', '_', s)
    # Strip trailing underscores
    s = s.strip('_')
    if len(s) > max_len:
        print(f"WARNING: filename {s} exceeds 80 chars and is being clipped.")
        s = s[:max_len].rstrip('_')
    return s


# ---------------------------------------------------------------------------
# TOC DATA
#
# Each issue is keyed by (volume, issue_identifier) where issue_identifier
# matches the filename pattern.  Entries are in the order they appear in the
# magazine.  The regex will search for the title (and optionally author) as
# boundary markers.
#
# Fields per entry:
#   title  - the title as it appears in the text (used for regex matching)
#   author - author name or None
#   etype  - entry type classification
#
# NOTE: This script contains TOC data for Vol33-35 (manually extracted).
#       Vol36-40 TOC data should be added when available.
# ---------------------------------------------------------------------------

import vol33_35_toc

TOC = vol33_35_toc.TOC

# ---------------------------------------------------------------------------
# Filename mapping: issue key -> (source filename, month name for output)
# ---------------------------------------------------------------------------
ISSUE_FILES = {}

# Volume 33
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

# Volume 34
for no, month, year in [
    ("No1", "January", "1947"), ("No2", "February", "1947"),
    ("No3", "March", "1947"), ("No4", "April", "1947"),
    ("No5", "May", "1947"), ("No6", "June", "1947"),
    ("No7", "July", "1947"), ("No8", "August", "1947"),
    ("No9", "September", "1947"), ("No10", "October", "1947"),
    ("No11", "November", "1947"), ("No12", "December", "1947"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"vol34_{key}.txt"
    ISSUE_FILES[("Vol34", key)] = (fname, month)

# Volume 35
for no, month, year in [
    ("No1", "January", "1948"), ("No2", "February", "1948"),
    ("No3", "March", "1948"), ("No4", "April", "1948"),
    ("No5", "May", "1948"), ("No6", "June", "1948"),
    ("No7", "July", "1948"), ("No8", "August", "1948"),
    ("No9", "September", "1948"), ("No10", "October", "1948"),
    ("No11", "November", "1948"), ("No12", "December", "1948"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"vol35_{key}.txt"
    ISSUE_FILES[("Vol35", key)] = (fname, month)

# ---------------------------------------------------------------------------
# Extraction engine
# ---------------------------------------------------------------------------

def build_regex_for_title(title: str, require_line_start: bool = True) -> re.Pattern:
    """
    Build a regex pattern that finds the title in the text.
    Allows for minor OCR variation and flexible whitespace.
    When require_line_start is True, the title must appear at the
    beginning of a line (after a newline or at position 0) to avoid
    matching common phrases buried mid-sentence in article body text.
    """
    # Escape regex special chars in the title
    escaped = re.escape(title)
    # Allow flexible whitespace (OCR may have inserted extra spaces)
    escaped = re.sub(r'\\ ', r'\\s+', escaped)
    if require_line_start:
        escaped = r'(?:^|\n)\s*' + escaped
    return re.compile(escaped, re.IGNORECASE)


def strip_running_noise(text: str) -> tuple[str, list[str]]:
    """
    Remove running headers and mailing statements from article body text.
    Returns (cleaned_text, list_of_stripped_fragments).
    """
    noise = []

    # Running headers: "RELIEF SOCIETY MAGAZINE" optionally followed by
    # month/year with optional punctuation (OCR often merges them)
    header_pat = re.compile(
        r'\d*\s*RELIEF SOCIETY MAGAZINE\s*[\W]*\s*'
        r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|'
        r'SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)?\s*\d{0,4}',
        re.IGNORECASE,
    )
    for m in header_pat.finditer(text):
        noise.append(m.group().strip())
    text = header_pat.sub('', text)

    # "LESSON DEPARTMENT" running section headers (appear mid-page)
    lesson_dept_pat = re.compile(r'\n\s*LESSON DEPARTMENT\s*\n')
    for m in lesson_dept_pat.finditer(text):
        noise.append(m.group().strip())
    text = lesson_dept_pat.sub('\n', text)

    # Mailing statement block
    mailing_pat = re.compile(
        r'Entered as second-class matter.*?authorized\s+June\s+29,\s+1918\.',
        re.DOTALL | re.IGNORECASE,
    )
    for m in mailing_pat.finditer(text):
        noise.append(m.group().strip())
    text = mailing_pat.sub('', text)

    # "Stamps should accompany manuscripts for their return."
    stamps_pat = re.compile(r'Stamps should accompany manuscripts for their return\.?')
    for m in stamps_pat.finditer(text):
        noise.append(m.group().strip())
    text = stamps_pat.sub('', text)

    # Collapse runs of blank lines left behind
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text, noise


def find_ads_section(body: str, body_offset: int) -> tuple[str, str, int]:
    """
    Look for advertising content at the tail of the body text.
    Returns (body_without_ads, ads_text, ads_start_in_full_text).
    If no ads found, ads_text is empty and body is unchanged.
    """
    # Search in the last 30% of the body for ad markers
    search_start = int(len(body) * 0.7)
    search_region = body[search_start:]

    ad_markers = [
        r"When Buying Mention Relief Society Magazine",
        r"DESERET NEWS PRESS",
        r"DESERET BOOK COMPANY",
        r"DAYNES\S?\s*MUSIC\s*CO",
        r"L\.\s*D\.\s*S\.\s*BUSINESS COLLEGE",
        r"MORMON HANDICRAFT",
        r"Brigham Young University",
    ]

    earliest_pos = None
    for marker in ad_markers:
        m = re.search(marker, search_region, re.IGNORECASE)
        if m:
            pos = search_start + m.start()
            if earliest_pos is None or pos < earliest_pos:
                earliest_pos = pos

    if earliest_pos is None:
        return body, "", body_offset + len(body)

    # Walk backwards from earliest_pos to find a paragraph break
    # that likely starts the ads section
    newline_pos = body.rfind('\n\n', 0, earliest_pos)
    if newline_pos != -1 and earliest_pos - newline_pos < 500:
        earliest_pos = newline_pos

    ads_text = body[earliest_pos:].strip()
    body_trimmed = body[:earliest_pos]

    return body_trimmed, ads_text, body_offset + earliest_pos


def split_front_matter_fallback(text: str) -> tuple[str, str]:
    """
    Fallback method to split front matter when MAGAZINE CIRCULATION marker is missing.
    Uses Contents section and page markers to identify the boundary.

    Returns (front_matter, body).  If fallback fails, returns ("", text).
    """
    # Strategy 1: Find where CONTENTS section ends and first major section begins
    # This is a good boundary since contents list articles, and first section starts them
    contents_match = re.search(
        r'CONTENTS\s*\n(.*?)(?=GENERAL\s+FEATURES|SPECIAL\s+FEATURES|FICTION|GENERAL\s+BOARD|PUBLISHED|$)',
        text, re.DOTALL | re.IGNORECASE,
    )

    if contents_match:
        # Found contents; now look for the first major section header after it
        search_start = contents_match.end()
        section_match = re.search(
            r'\n\s*(GENERAL\s+FEATURES|SPECIAL\s+FEATURES|FICTION|GENERAL\s+ARTICLES|LESSON\s+DEPARTMENT)',
            text[search_start:],
            re.IGNORECASE,
        )
        if section_match:
            split_pos = search_start + section_match.start()
            return text[:split_pos], text[split_pos:]

    # Strategy 2: Look for page markers (Page 4, Page 5, etc.)
    # Articles typically start on page 4 or later, front matter is pages 1-3
    page_matches = list(re.finditer(r'\nPage\s+(\d+)', text, re.IGNORECASE))
    if page_matches:
        # Find first page >= 4
        for match in page_matches:
            page_num = int(match.group(1))
            if page_num >= 4:
                return text[:match.start()], text[match.start():]
        # If all pages < 4, use the last page marker
        if page_matches:
            last_match = page_matches[-1]
            return text[:last_match.start()], text[last_match.start():]

    # Strategy 3: Look for multiple consecutive section headers or major gaps
    # Front matter tends to be more dense; body has clearer structure
    # Find the first occurrence of a title pattern that looks like an article
    # (capitalized phrase on its own line, not part of a list)

    # Fallback: if all else fails, return empty front matter
    print("  WARNING: Could not determine front matter boundary with fallback strategies")
    return "", text


def split_front_matter(text: str) -> tuple[str, str]:
    """
    Split the issue text into front matter (TOC, board listing, ads, subscription
    info) and body content.  The marker is 'MAGAZINE CIRCULATION' which appears
    in every issue of Vol 33-40 at the end of the front matter section.

    Returns (front_matter, body).  If the marker is not found, attempts fallback
    strategies using Contents section and page markers.
    """
    marker = re.search(r'MAGAZINE CIRCULATION[^\n]*', text)
    if marker:
        split_pos = marker.end()
        return text[:split_pos], text[split_pos:]
    else:
        print("  WARNING: MAGAZINE CIRCULATION marker not found, using fallback method")
        return split_front_matter_fallback(text)


def _match_entries_with_strategy(body: str, entries: list[dict],
                                 body_offset: int,
                                 require_line_start: bool) -> list[tuple[int, dict]]:
    """
    Match TOC entries in body text using one strategy.
    Returns list of (position_in_full_text, entry_dict) for found entries.
    """
    found = []
    for entry in entries:
        pattern = build_regex_for_title(entry["title"],
                                        require_line_start=require_line_start)
        match = pattern.search(body)
        if match:
            pos = match.start()
            if require_line_start:
                # Adjust past the newline/whitespace prefix to point at the title
                matched_text = match.group()
                first_word = entry["title"].split()[0]
                title_in_match = re.search(re.escape(first_word),
                                           matched_text, re.IGNORECASE)
                if title_in_match:
                    pos += title_in_match.start()
            found.append((pos + body_offset, entry))
    return found


def _boundaries_from_found(found: list[tuple[int, dict]],
                           body_end: int) -> list[tuple[int, int, dict]]:
    """Convert sorted (pos, entry) list into (start, end, entry) triples."""
    found_sorted = sorted(found, key=lambda x: x[0])
    boundaries = []
    for i, (start, entry) in enumerate(found_sorted):
        end = found_sorted[i + 1][0] if i + 1 < len(found_sorted) else body_end
        boundaries.append((start, end, entry))
    return boundaries


def extract_toc_from_front_matter(front_matter: str) -> tuple[str, str]:
    """
    Extract the CONTENTS/TOC section from front matter.
    Returns (toc_text, remaining_front_matter).
    """
    # Look for CONTENTS header through the next major section boundary
    toc_match = re.search(
        r'(CONTENTS.*?)(?=GENERAL\s+BOARD|PUBLISHED\s+MONTHLY|'
        r'MAGAZINE\s+CIRCULATION|$)',
        front_matter, re.DOTALL | re.IGNORECASE,
    )
    if toc_match:
        toc_text = toc_match.group(1).strip()
        remaining = (front_matter[:toc_match.start()] +
                     front_matter[toc_match.end():]).strip()
        return toc_text, remaining
    return "", front_matter


def extract_issue(text: str, entries: list[dict], vol: str, month: str,
                  source_filename: str, output_dir: Path,
                  dry_run: bool = False) -> dict:
    """
    Extract a single issue's text into individual entry files.
    Returns a dict with stats, manifest_rows, and a month_json object.
    """
    # Split off front matter so title matches happen in body only
    front_matter, body = split_front_matter(text)
    body_offset = len(front_matter)

    # Separate ads from the tail of the body
    body, ads_text, ads_start = find_ads_section(body, body_offset)
    body_end = body_offset + len(body)

    # Run both strategies independently
    strict_found = _match_entries_with_strategy(body, entries, body_offset,
                                                require_line_start=True)
    loose_found = _match_entries_with_strategy(body, entries, body_offset,
                                               require_line_start=False)

    strict_bounds = _boundaries_from_found(strict_found, body_end)
    loose_bounds = _boundaries_from_found(loose_found, body_end)

    # Build lookup dicts: title -> (start, end) for each strategy
    strict_by_title = {e["title"]: (s, nd) for s, nd, e in strict_bounds}
    loose_by_title = {e["title"]: (s, nd) for s, nd, e in loose_bounds}

    stats = {"matched": 0, "misc_bytes": 0,
             "total_bytes": len(text.encode("utf-8")),
             "manifest_rows": []}

    issue_dir = output_dir / vol / month
    rel_dir = f"processed/{vol}/{month}"
    if not dry_run:
        issue_dir.mkdir(parents=True, exist_ok=True)

    # Collect all noise stripped from articles for MISC
    all_noise = []
    # Track covered intervals (union of strict and loose)
    covered_intervals = []
    # JSON entries for this month
    json_entries = []

    # Use strict ordering for index numbering (fall back to loose if strict empty)
    ordering = strict_bounds if strict_bounds else loose_bounds
    title_order = [e["title"] for _, _, e in ordering]
    # Add any loose-only titles not in strict
    for _, _, e in loose_bounds:
        if e["title"] not in title_order:
            title_order.append(e["title"])

    # Build entry lookup by title for metadata
    entry_by_title = {e["title"]: e for e in entries}

    for idx, title in enumerate(title_order, 1):
        entry = entry_by_title.get(title)
        if not entry:
            continue

        title_safe = sanitize_filename(entry["title"])

        # Process strict match
        strict_result = None
        if title in strict_by_title:
            s_start, s_end = strict_by_title[title]
            raw_text = text[s_start:s_end].strip()
            raw_len = len(raw_text)
            cleaned, noise_frags = strip_running_noise(raw_text)
            cleaned = cleaned.strip()
            all_noise.extend(noise_frags)
            covered_intervals.append((s_start, s_end))

            s_filename = f"{idx:02d}_strict_{title_safe}.txt"
            if not dry_run and cleaned:
                (issue_dir / s_filename).write_text(cleaned, encoding="utf-8")

            strict_result = {
                "file": s_filename,
                "path": rel_dir,
                "position": s_start,
                "length": raw_len,
                "content": cleaned,
            }

        # Process loose match
        loose_result = None
        if title in loose_by_title:
            l_start, l_end = loose_by_title[title]
            raw_text = text[l_start:l_end].strip()
            raw_len = len(raw_text)
            cleaned, noise_frags = strip_running_noise(raw_text)
            cleaned = cleaned.strip()
            # Only add noise from loose if strict didn't already cover it
            if title not in strict_by_title:
                all_noise.extend(noise_frags)
            covered_intervals.append((l_start, l_end))

            l_filename = f"{idx:02d}_loose_{title_safe}.txt"
            if not dry_run and cleaned:
                (issue_dir / l_filename).write_text(cleaned, encoding="utf-8")

            loose_result = {
                "file": l_filename,
                "path": rel_dir,
                "position": l_start,
                "length": raw_len,
                "content": cleaned,
            }

        if strict_result or loose_result:
            stats["matched"] += 1

            # Determine if strict and loose are identical
            identical = False
            if strict_result and loose_result:
                identical = strict_result["content"] == loose_result["content"]

            json_entry = {
                "index": idx,
                "title": entry["title"],
                "author": entry["author"],
                "etype": entry["etype"],
                "strict_loose_identical": identical,
                "strict_match": strict_result,
                "loose_match": loose_result,
            }
            json_entries.append(json_entry)

            # Manifest rows — one per strategy that matched
            if strict_result:
                stats["manifest_rows"].append({
                    "file": strict_result["file"],
                    "path": rel_dir,
                    "volume": vol,
                    "month": month,
                    "etype": entry["etype"],
                    "title": entry["title"],
                    "author": entry["author"],
                    "strategy": "strict",
                })
            if loose_result:
                stats["manifest_rows"].append({
                    "file": loose_result["file"],
                    "path": rel_dir,
                    "volume": vol,
                    "month": month,
                    "etype": entry["etype"],
                    "title": entry["title"],
                    "author": entry["author"],
                    "strategy": "loose",
                })

            if strict_result or loose_result:
                matched_label = entry["etype"]
                s_chars = len(strict_result["content"]) if strict_result else 0
                l_chars = len(loose_result["content"]) if loose_result else 0
                ident_flag = " [identical]" if identical else ""
                if dry_run:
                    print(f"  [{matched_label:12s}] #{idx:02d} "
                          f"strict={s_chars} loose={l_chars}{ident_flag} "
                          f"{entry['title'][:50]}")
        else:
            print(f"  WARNING: No match for '{entry['title']}' in body text")

    # Extract TOC from front matter
    toc_text, remaining_fm = extract_toc_from_front_matter(front_matter)

    toc_json = None
    if toc_text:
        toc_filename = "TOC.txt"
        if not dry_run:
            (issue_dir / toc_filename).write_text(toc_text, encoding="utf-8")
        elif dry_run:
            print(f"  [{'toc':12s}] {toc_filename} ({len(toc_text)} chars)")
        toc_json = {
            "file": toc_filename,
            "path": rel_dir,
            "content": toc_text,
        }
        stats["manifest_rows"].append({
            "file": toc_filename, "path": rel_dir,
            "volume": vol, "month": month,
            "etype": "toc", "title": "TOC",
            "author": "", "strategy": "",
        })

    # Write ads file
    ads_json = None
    if ads_text:
        ads_filename = "ADS.txt"
        if not dry_run:
            (issue_dir / ads_filename).write_text(ads_text, encoding="utf-8")
        elif dry_run:
            print(f"  [{'ads':12s}] {ads_filename} ({len(ads_text)} chars)")
        ads_json = {
            "file": ads_filename,
            "path": rel_dir,
            "content": ads_text,
        }
        stats["manifest_rows"].append({
            "file": ads_filename, "path": rel_dir,
            "volume": vol, "month": month,
            "etype": "ads", "title": "ADS",
            "author": "", "strategy": "",
        })

    # Collect uncovered text into MISC
    misc_parts = []

    # Remaining front matter (after TOC extraction) goes into MISC
    if remaining_fm.strip():
        misc_parts.append(remaining_fm.strip())

    # Find gaps in body not covered by any entry (using union of intervals)
    all_intervals = sorted(set(covered_intervals))
    cursor = body_offset
    for iv_start, iv_end in all_intervals:
        if cursor < iv_start:
            gap_text = text[cursor:iv_start].strip()
            if gap_text:
                misc_parts.append(gap_text)
        cursor = max(cursor, iv_end)

    if cursor < body_end:
        gap_text = text[cursor:body_end].strip()
        if gap_text:
            misc_parts.append(gap_text)

    # Stripped noise goes into MISC
    if all_noise:
        misc_parts.append("--- STRIPPED NOISE ---")
        # Deduplicate noise fragments
        seen = set()
        for frag in all_noise:
            if frag not in seen:
                seen.add(frag)
                misc_parts.append(frag)

    misc_json = None
    if misc_parts:
        misc_text = "\n\n---\n\n".join(misc_parts)
        stats["misc_bytes"] = len(misc_text.encode("utf-8"))
        misc_filename = "MISC.txt"

        if not dry_run:
            (issue_dir / misc_filename).write_text(misc_text, encoding="utf-8")
        elif dry_run:
            print(f"  [{'misc':12s}] {misc_filename} ({len(misc_text)} chars)")

        misc_json = {
            "file": misc_filename,
            "path": rel_dir,
            "content": misc_text,
        }
        stats["manifest_rows"].append({
            "file": misc_filename, "path": rel_dir,
            "volume": vol, "month": month,
            "etype": "misc", "title": "MISC",
            "author": "", "strategy": "",
        })

    # Build month JSON object
    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/{vol}"
    stats["month_json"] = {
        "source_file": source_filename,
        "source_path": source_rel_path,
        "entries": json_entries,
        "toc": toc_json,
        "ads": ads_json,
        "misc": misc_json,
    }

    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract Relief Society Magazine Vol 33-40 into individual entries"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without creating files")
    parser.add_argument("--volume", type=int, choices=[33, 34, 35, 36, 37, 38, 39, 40],
                        help="Process only one volume")
    args = parser.parse_args()

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    # Collect JSON data per volume: { "Vol30": {"volume": ..., "months": {...}} }
    volume_json = {}


    for (vol, issue_key), entries in TOC.items():
        vol_num = int(vol.replace("Vol", ""))
        if args.volume and vol_num != args.volume:
            continue

        if (vol, issue_key) not in ISSUE_FILES:
            print(f"WARNING: No file mapping for ({vol}, {issue_key}), skipping")
            continue

        filename, month = ISSUE_FILES[(vol, issue_key)]
        source_path = CLEAN_DIR / vol / filename

        if not source_path.exists():
            # Try case variations
            for candidate in CLEAN_DIR.iterdir():
                if candidate.name.lower() == vol.lower():
                    source_path = candidate / filename
                    break

        if not source_path.exists():
            print(f"WARNING: Source file not found: {source_path}")
            continue

        print(f"\n{'='*60}")
        print(f"Processing: {vol} / {month} ({filename})")
        print(f"{'='*60}")

        text = source_path.read_text(encoding="utf-8", errors="replace")

        stats = extract_issue(text, entries, vol, month, filename,
                              OUTPUT_DIR, dry_run=args.dry_run)

        issues_processed += 1
        total_matched += stats["matched"]
        total_misc += stats["misc_bytes"]
        total_bytes += stats["total_bytes"]
        all_manifest_rows.extend(stats["manifest_rows"])

        # Accumulate into volume JSON
        if vol not in volume_json:
            volume_json[vol] = {"volume": vol, "months": {}}
        volume_json[vol]["months"][month] = stats["month_json"]


        coverage = ((stats["total_bytes"] - stats["misc_bytes"]) / stats["total_bytes"] * 100
                     if stats["total_bytes"] > 0 else 0)
        print(f"  Entries matched: {stats['matched']}")
        print(f"  Coverage: {coverage:.1f}%")
        print(f"  Misc bytes: {stats['misc_bytes']}")

    # Write per-volume JSON files and flagged_for_review.json
    if not args.dry_run:
        for vol, data in volume_json.items():
            vol_dir = OUTPUT_DIR / vol
            vol_dir.mkdir(parents=True, exist_ok=True)

            json_path = vol_dir / f"{vol}_entries.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            print(f"\nJSON written: {json_path}")

            # Build flagged_for_review.json — entries whose content does
            # not start with their own title, indicating a likely false
            # split where the title was matched mid-sentence in a
            # preceding article's body text.
            flagged = []
            for month_name, month_data in data["months"].items():
                for entry_json in month_data["entries"]:
                    title = entry_json["title"]
                    title_pat = re.compile(
                        re.sub(r'\s+', r'\\s+', re.escape(title)),
                        re.IGNORECASE,
                    )
                    for strategy in ("strict_match", "loose_match"):
                        match_data = entry_json.get(strategy)
                        if match_data is None:
                            continue
                        content = match_data["content"]
                        # Check if the title appears near the start
                        # (first 200 chars to allow for minor leading whitespace)
                        head = content[:200] if content else ""
                        if not title_pat.search(head):
                            flagged.append({
                                "title": title,
                                "author": entry_json["author"],
                                "etype": entry_json["etype"],
                                "index": entry_json["index"],
                                "month": month_name,
                                "strategy": strategy.replace("_match", ""),
                                "file": match_data["file"],
                                "path": match_data["path"],
                                "position": match_data["position"],
                                "length": match_data["length"],
                                "content": content,
                                "strict_loose_identical": entry_json["strict_loose_identical"],
                                "title_not_at_start": True,
                            })

            if flagged:
                flagged_path = vol_dir / "flagged_for_review.json"
                with open(flagged_path, "w", encoding="utf-8") as f:
                    json.dump(flagged, f, indent=2, ensure_ascii=False, default=str)
                print(f"Flagged for review: {flagged_path} "
                      f"({len(flagged)} entries with title not at start)")

    # Write manifest CSV
    if all_manifest_rows and not args.dry_run:
        manifest_path = OUTPUT_DIR / "manifest.csv"
        fieldnames = ["file", "path", "volume", "month", "etype",
                      "title", "author", "strategy"]
        with open(manifest_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_manifest_rows)
        print(f"Manifest written: {manifest_path} ({len(all_manifest_rows)} entries)")

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Issues processed: {issues_processed}")
    print(f"Total entries matched: {total_matched}")
    overall_coverage = ((total_bytes - total_misc) / total_bytes * 100
                         if total_bytes > 0 else 0)
    print(f"Overall coverage: {overall_coverage:.1f}%")
    print(f"Total misc bytes: {total_misc}")


if __name__ == "__main__":
    main()
