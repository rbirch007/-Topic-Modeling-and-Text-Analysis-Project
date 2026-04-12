#!/usr/bin/env python3
"""
Extract poems from Misc files across Vols 30-40 using CONTENTS/TOC files as ground truth.

Phase 1: Parse CONTENTS files for POETRY sections
Phase 2: Check what's already extracted
Phase 3: Extract missing poems from Misc files

Usage:
    python extract_misc_poems.py --dry-run    # Preview only
    python extract_misc_poems.py              # Extract for real
"""

import os
import re
import sys
import glob
import argparse
from pathlib import Path
from collections import defaultdict

BASE_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\OutputExtractedarticles"
REPORT_PATH = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\misc_poem_extraction_report.txt"

VOL_YEARS = {
    30: 1943, 31: 1944, 32: 1945, 33: 1946, 34: 1947,
    35: 1948, 36: 1949, 37: 1950, 38: 1951, 39: 1952, 40: 1953,
}

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# Sections that end the POETRY block in TOC
NON_POETRY_SECTIONS = [
    "SPECIAL FEATURES", "FICTION", "GENERAL FEATURES", "FEATURES FOR THE HOME",
    "LESSONS", "PUBLISHED MONTHLY", "Editorial and Business",
    "LESSON DEPARTMENT", "LESSON AIDS", "HOME FEATURES",
]


def normalize_title(title):
    """Normalize a title for comparison: lowercase, strip punctuation, collapse whitespace."""
    t = title.lower().strip()
    t = re.sub(r'[^a-z0-9\s]', '', t)
    t = re.sub(r'\s+', ' ', t)
    return t


def normalize_author(author):
    """Normalize an author name for comparison."""
    a = author.lower().strip()
    a = re.sub(r'[^a-z\s]', '', a)
    a = re.sub(r'\s+', ' ', a)
    return a


def parse_poetry_line(line):
    """
    Parse a CONTENTS poetry line into (title, author, page_number).
    """
    line = line.strip()
    if not line:
        return None

    # Handle "by" separator (later volumes use this format)
    by_match = re.match(r'^(.+?)\s+(?:—\s*Frontispiece\s+)?by\s+(.+?)(?:\s+(\d{1,4}))?$', line, re.IGNORECASE)
    if by_match:
        title = by_match.group(1).strip()
        author = by_match.group(2).strip()
        page = by_match.group(3) if by_match.group(3) else None
        # Clean qualifiers from title
        title = re.sub(r'\s*—?\s*Frontispiece\b', '', title, flags=re.IGNORECASE).strip()
        return (title, author, page)

    # Remove page number at end
    page = None
    m = re.search(r'\s+(\d{1,4})\s*$', line)
    if m:
        page = m.group(1)
        line = line[:m.start()].strip()
    elif re.search(r'Back of Front(?:ispiece|is-piece| page)\s*$', line, re.IGNORECASE):
        page = 'Frontispiece'
        line = re.sub(r'\s*Back of Front(?:ispiece|is-piece| page)\s*$', '', line, flags=re.IGNORECASE).strip()

    # Remove qualifiers
    qualifiers = [
        r'\s*—?\s*Frontispiece\b',
        r'\s+First Prize[,]?\s*(?:Poem\s+Contest)?(?:\s*Story)?',
        r'\s+Second Prize[,]?\s*(?:Poem\s+Contest)?(?:\s*Story)?',
        r'\s+Third Prize[,]?\s*(?:Poem\s+Contest)?(?:\s*Story)?',
        r'\s+Honorable Mention[,]?\s*(?:Poem\s+Contest)?',
        r'\s+Prize[,]?\s*(?:Poem\s+Contest)?',
    ]
    cleaned = line
    for q in qualifiers:
        cleaned = re.sub(q, ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # Split into title and author
    words = cleaned.split()
    if len(words) < 2:
        return (cleaned, "", page)

    best_title = cleaned
    best_author = ""
    for n_author_words in range(5, 1, -1):
        if n_author_words >= len(words):
            continue
        candidate_author = ' '.join(words[-n_author_words:])
        candidate_title = ' '.join(words[:-n_author_words])

        author_words = candidate_author.split()
        looks_like_name = all(
            (w[0].isupper() or len(w) <= 2) and not w.isupper()
            for w in author_words
            if w not in ('de', 'van', 'von', 'la', 'le', 'the', 'of', 'and')
        )

        if looks_like_name and candidate_title:
            best_title = candidate_title
            best_author = candidate_author
            break

    return (best_title.strip(), best_author.strip(), page)


def parse_contents_poetry(contents_path):
    """
    Parse the POETRY section from a CONTENTS file.
    Returns list of (title, author, page) tuples.
    """
    try:
        with open(contents_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    except Exception as e:
        return []

    poems = []
    in_poetry = False

    for line in lines:
        stripped = line.strip()

        if stripped == "POETRY":
            in_poetry = True
            continue

        if in_poetry:
            if any(stripped.startswith(s) for s in NON_POETRY_SECTIONS):
                break
            if stripped == "":
                continue

            result = parse_poetry_line(stripped)
            if result:
                poems.append(result)

    return poems


def sanitize_for_filename(text, max_words=4):
    """Convert text to filename-safe format, keeping first N words."""
    words = re.sub(r'[^a-zA-Z0-9\s]', '', text).split()
    words = words[:max_words]
    return '_'.join(words)


def find_existing_poem_file(month_dir, month, vol_num, title, author):
    """
    Check if a poem file already exists for this title/author.
    Returns the filename if found, None otherwise.
    """
    norm_title = normalize_title(title)
    norm_author = normalize_author(author)

    try:
        files = os.listdir(month_dir)
    except Exception:
        return None

    for fname in files:
        if not fname.endswith('.txt'):
            continue
        if 'Misc' in fname or 'CONTENTS' in fname or 'BOARD' in fname:
            continue

        m = re.match(r'.+?_Vol\d+_\d+_(Poem|Article|Frontispiece|Notes)_(.+)\.txt', fname, re.IGNORECASE)
        if m:
            file_title_author = m.group(2).replace('_', ' ').lower()

            title_words = norm_title.split()
            significant_words = [w for w in title_words if len(w) > 2]
            if significant_words:
                matches = sum(1 for w in significant_words if w in file_title_author)
                if matches >= min(2, len(significant_words)):
                    return fname

            if norm_author:
                author_parts = norm_author.split()
                if author_parts and author_parts[-1] in file_title_author:
                    if any(w in file_title_author for w in title_words if len(w) > 2):
                        return fname

    return None


def get_next_sequence(month_dir, month, vol_num):
    """Get the next available sequence number for a month directory."""
    max_seq = 0
    try:
        files = os.listdir(month_dir)
    except Exception:
        return 30

    for fname in files:
        m = re.match(rf'{month}_Vol{vol_num}_(\d+)_', fname)
        if m:
            seq = int(m.group(1))
            if seq > max_seq:
                max_seq = seq

    return max_seq + 1


def get_misc_files(month_dir, month, vol_num):
    """Get all Misc files for a given month, sorted. Does NOT include CONTENTS."""
    files = []
    base_misc = os.path.join(month_dir, f"{month}_Vol{vol_num}_00_Misc.txt")
    if os.path.exists(base_misc):
        files.append(base_misc)

    pattern = os.path.join(month_dir, f"{month}_Vol{vol_num}_Misc_cont_*.txt")
    cont_files = sorted(glob.glob(pattern))
    files.extend(cont_files)

    return files


def is_toc_line(line):
    """
    Detect if a line is a TOC entry (title + author + page number).
    TOC entries end with a page number and are typically short references.
    """
    stripped = line.strip()
    if not stripped:
        return False

    # TOC lines end with a page number
    if re.search(r'\s+\d{1,4}\s*$', stripped):
        # And they're relatively short (just title + author + page)
        # and don't contain verse-like content
        if len(stripped) < 120:
            return True

    # "Back of Frontispiece" style
    if re.search(r'Back of Front', stripped, re.IGNORECASE):
        if len(stripped) < 120:
            return True

    return False


def is_in_toc_block(lines, line_idx):
    """
    Check if the given line index is within a TOC/CONTENTS block.
    A TOC block is identified by having POETRY/FICTION/SPECIAL FEATURES headers
    nearby and multiple TOC-style lines in sequence.
    """
    # Look backwards up to 20 lines for section headers
    for i in range(max(0, line_idx - 20), line_idx):
        stripped = lines[i].strip()
        if stripped in ("POETRY", "FICTION", "SPECIAL FEATURES", "GENERAL FEATURES",
                        "LESSONS", "FEATURES FOR THE HOME", "HOME FEATURES",
                        "LESSON DEPARTMENT", "Contents"):
            # Check if surrounding lines are also TOC-style
            toc_count = 0
            for j in range(max(0, line_idx - 3), min(len(lines), line_idx + 3)):
                if is_toc_line(lines[j]):
                    toc_count += 1
            if toc_count >= 2:
                return True

    return False


def extract_poem_from_text(text, title, author):
    """
    Try to extract a poem from text given its title and author.

    Handles:
    - Multi-line poems (normal formatting with line breaks)
    - Single-line collapsed poems (OCR artifact where entire poem is on one line)

    Key: SKIP TOC entries. Only extract actual poem text.

    Returns the extracted poem text, or None if not found.
    """
    lines = text.split('\n')
    norm_title = normalize_title(title)
    title_words = [w for w in norm_title.split() if len(w) > 2]

    if not title_words:
        return None

    candidates = []

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue

        norm_line = normalize_title(stripped)

        # Check if title appears in this line
        title_match = False

        # Exact title match at start of line
        if norm_line.startswith(norm_title):
            title_match = True
        # Partial match (at least 2 significant words from start)
        elif len(title_words) >= 2:
            title_start = ' '.join(title_words[:3])
            if norm_line.startswith(title_start) or title_start in norm_line[:len(title_start)+20]:
                title_match = True

        if not title_match:
            continue

        # Skip if this is a TOC line (title + author + page number, no verse)
        if is_toc_line(stripped):
            # But check: is this a collapsed poem? (very long line with verse after the page number area)
            # A collapsed poem line would be very long (>150 chars)
            if len(stripped) < 150:
                continue
            # Long line - might be a collapsed poem even though it starts like a TOC entry

        # Skip if in a TOC block context
        if is_in_toc_block(lines, i):
            if len(stripped) < 150:
                continue

        # Skip lines that are just ALL-CAPS headers (like "RELIEF SOCIETY MAGAZINE")
        # unless they're very long (collapsed poem)
        if stripped.isupper() and len(stripped) < 150:
            continue

        # Check: is this a collapsed poem (entire poem on one long line)?
        if len(stripped) > 150 and author:
            poem_text = extract_collapsed_poem(stripped, title, author)
            if poem_text:
                candidates.append(('collapsed', poem_text, i))
                continue

        # Check: is this a multi-line poem?
        poem_lines = extract_poem_block(lines, i, title, author)
        if poem_lines:
            candidates.append(('block', '\n'.join(poem_lines), i))

    # Return the best candidate
    # Prefer block poems over collapsed, and longer extractions over shorter
    if not candidates:
        return None

    # Sort: prefer 'block' type, then by length
    candidates.sort(key=lambda c: (0 if c[0] == 'block' else 1, -len(c[1])))
    return candidates[0][1]


def extract_poem_block(lines, start_idx, title, author):
    """
    Extract a multi-line poem block starting from the title line.
    Returns list of lines, or None if this doesn't look like a real poem.
    """
    poem_lines = []

    # Include the title line
    first_line = lines[start_idx].strip()
    poem_lines.append(first_line)

    # Look at subsequent lines for author and poem body
    found_author = False
    consecutive_empty = 0

    for j in range(start_idx + 1, min(start_idx + 60, len(lines))):
        line = lines[j].strip()

        if not line:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break  # Two consecutive blank lines = end
            if len(poem_lines) > 2:
                # Check if next non-empty line looks like poem
                next_content = ""
                for k in range(j + 1, min(j + 3, len(lines))):
                    if lines[k].strip():
                        next_content = lines[k].strip()
                        break
                if next_content and is_poem_line(next_content) and len(next_content) < 80:
                    poem_lines.append("")  # Stanza break
                    continue
                else:
                    break
            continue

        consecutive_empty = 0

        # End signals
        if is_end_signal(line):
            break

        # Author line detection
        if not found_author and author and len(poem_lines) <= 2:
            author_last = author.split()[-1].lower() if author else ""
            if author_last and author_last in line.lower():
                # This could be the author credit line
                if len(line) < 60:
                    poem_lines.append(line)
                    found_author = True
                    continue

        # Is this a poem line?
        if len(line) > 100:
            # Very long line - not typical for poetry
            break

        # Check for TOC-like patterns (title + page number)
        if is_toc_line(line):
            break

        if is_poem_line(line):
            poem_lines.append(line)
        else:
            break

    # Validate: need at least title + 2 lines of verse
    # Count non-empty, non-title lines
    verse_lines = [l for l in poem_lines[1:] if l.strip() and not is_just_author(l, author)]
    if len(verse_lines) < 2:
        return None

    # Validate content looks like verse, not organizational/header content
    all_text = ' '.join(l for l in poem_lines[1:] if l.strip())
    # Reject if it looks like magazine header/board listing
    if re.search(r'(President|First Counselor|Second Counselor|Secretary-Treasurer|General Board)', all_text):
        if all_text.count('President') + all_text.count('Counselor') >= 2:
            return None

    return poem_lines


def is_just_author(line, author):
    """Check if a line is just the author name."""
    if not author:
        return False
    norm_line = normalize_author(line)
    norm_auth = normalize_author(author)
    return norm_line == norm_auth or norm_auth in norm_line and len(line.strip()) < 40


def extract_collapsed_poem(line, title, author):
    """
    Extract a poem from a single collapsed OCR line.
    Pattern: TITLE Author Name [verse text...] [ending markers]
    """
    if not author:
        return None

    author_last = author.split()[-1]

    # Find author name in the line (after the title)
    title_end = 0
    norm_title_upper = title.upper()
    # Find where title ends in the line
    for t_word in title.split():
        pos = line.upper().find(t_word.upper(), title_end)
        if pos >= 0:
            title_end = pos + len(t_word)

    # Find author after title
    search_start = max(0, title_end)
    m = re.search(re.escape(author_last), line[search_start:], re.IGNORECASE)
    if not m:
        return None

    # The poem text starts after the full author name
    author_end = search_start + m.end()
    # Try to capture the rest of the author name (might have more words after last name)
    # Just skip a few more characters to get past any trailing author words
    remaining = line[author_end:].strip()

    if not remaining or len(remaining) < 20:
        return None

    # Trim ending noise: look for markers that signal end of poem content
    end_markers = [
        r'\s*The Cover:',
        r'\s*VOL\.\s*\d+',
        r'\s*Page\s+\d+',
        r'\s*INSURANCE',
        r'\s*RELIEF SOCIETY',
        r'\s*Photograph by',
        r'\s*\|\s*[\'"]?\s*[a-zA-Z]{1,3}\s+[A-Z]{2}',  # OCR noise like "| 'iN HT al"
        r'\s*[A-Z][a-z]+ [A-Z][a-z]+ [A-Z][a-z]+\s+(?:President|Elder|Bishop|Member)',  # Article starts
    ]

    poem_text = remaining
    for marker in end_markers:
        m2 = re.search(marker, poem_text, re.IGNORECASE)
        if m2:
            candidate = poem_text[:m2.start()].strip()
            if len(candidate) >= 20:
                poem_text = candidate

    # Also try to cut at a point where poem-like content stops
    # Poems have short phrases; look for where we get long prose
    # Heuristic: if we see a sentence > 40 chars without comma/period, it might be prose
    # For now, use the marker-based approach above

    if not poem_text or len(poem_text) < 20:
        return None

    # Format with title and author
    result = f"{title}\n{author}\n\n{poem_text}"
    return result


def is_poem_line(line):
    """Check if a line looks like it belongs to a poem."""
    line = line.strip()
    if not line:
        return True

    if len(line) > 100:
        return False

    if line.isupper() and len(line) > 5:
        return False

    if re.match(r'^Page\s+\d+', line, re.IGNORECASE):
        return False

    if re.match(r'^(INSURANCE|ADVERTISEMENT|ORDER|SEND|WRITE|PUBLISHED)', line, re.IGNORECASE):
        return False

    if '=== CONSOLIDATED FROM:' in line:
        return False

    if re.match(r'^\[Vol\d+', line):
        return False

    # TOC entry (ends with page number after short text)
    if re.match(r'^.{10,70}\s+\d{1,3}\s*$', line):
        return False

    return True


def is_end_signal(line):
    """Check if a line signals the end of a poem."""
    line = line.strip()

    if re.match(r'^Page\s+\d+', line, re.IGNORECASE):
        return True

    if line.isupper() and len(line) > 10 and ' ' in line:
        return True

    if '=== CONSOLIDATED FROM:' in line:
        return True

    if re.match(r'^\[Vol\d+', line):
        return True

    if len(line) > 120:
        return True

    if re.match(r'^(PUBLISHED|INSURANCE|ADVERTISEMENT|Editorial and Business)', line, re.IGNORECASE):
        return True

    return False


def main():
    parser = argparse.ArgumentParser(description="Extract poems from Misc files using CONTENTS as ground truth")
    parser.add_argument('--dry-run', action='store_true', help="Preview only, don't create files")
    args = parser.parse_args()

    report_lines = []
    stats = {
        'total_poems_in_toc': 0,
        'already_extracted': 0,
        'found_in_misc': 0,
        'not_found': 0,
        'created': 0,
        'errors': 0,
    }

    report_lines.append("=" * 80)
    report_lines.append("MISC POEM EXTRACTION REPORT")
    report_lines.append(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    report_lines.append("=" * 80)
    report_lines.append("")

    for vol_num in range(30, 41):
        year = VOL_YEARS[vol_num]
        vol_dir = os.path.join(BASE_DIR, f"Vol{vol_num}_{year}")

        if not os.path.isdir(vol_dir):
            report_lines.append(f"Vol{vol_num}_{year}: Directory not found, skipping")
            continue

        report_lines.append(f"\n{'='*60}")
        report_lines.append(f"Vol{vol_num} ({year})")
        report_lines.append(f"{'='*60}")

        for month in MONTHS:
            month_dir = os.path.join(vol_dir, month)
            if not os.path.isdir(month_dir):
                continue

            contents_path = os.path.join(month_dir, f"{month}_Vol{vol_num}_00_CONTENTS.txt")
            if not os.path.exists(contents_path):
                report_lines.append(f"  {month}: No CONTENTS file")
                continue

            # Phase 1: Parse CONTENTS
            poems = parse_contents_poetry(contents_path)
            if not poems:
                continue

            report_lines.append(f"\n  {month} - {len(poems)} poems in TOC:")
            stats['total_poems_in_toc'] += len(poems)

            # Phase 2: Check existing
            missing_poems = []
            for title, author, page in poems:
                existing = find_existing_poem_file(month_dir, month, vol_num, title, author)
                if existing:
                    report_lines.append(f"    [EXISTS] {title} / {author} -> {existing}")
                    stats['already_extracted'] += 1
                else:
                    report_lines.append(f"    [MISSING] {title} / {author}")
                    missing_poems.append((title, author, page))

            if not missing_poems:
                continue

            # Phase 3: Extract from Misc files
            misc_files = get_misc_files(month_dir, month, vol_num)
            if not misc_files:
                for title, author, page in missing_poems:
                    report_lines.append(f"    [NO MISC] {title} / {author} - No Misc files found")
                    stats['not_found'] += 1
                continue

            all_misc = []
            for fpath in misc_files:
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                        text = f.read()
                    all_misc.append((fpath, text))
                except Exception:
                    pass

            next_seq = get_next_sequence(month_dir, month, vol_num)

            for title, author, page in missing_poems:
                poem_text = None
                source_file = None

                for fpath, text in all_misc:
                    poem_text = extract_poem_from_text(text, title, author)
                    if poem_text:
                        source_file = os.path.basename(fpath)
                        break

                if poem_text:
                    stats['found_in_misc'] += 1

                    # Build filename
                    title_part = sanitize_for_filename(title)
                    author_part = sanitize_for_filename(author, max_words=2)
                    if author_part:
                        fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}_{author_part}.txt"
                    else:
                        fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}.txt"

                    fpath_out = os.path.join(month_dir, fname)

                    # Build file content with metadata header
                    file_content = f"[Vol{vol_num} | {month} {year} | Sequence: {next_seq:02d} | Category: Poem | Title: {title}]\n\n"
                    file_content += poem_text.strip() + "\n"

                    preview = poem_text.strip()[:120].replace('\n', ' | ')

                    if args.dry_run:
                        report_lines.append(f"    [WOULD CREATE] {fname}")
                        report_lines.append(f"      Source: {source_file}")
                        report_lines.append(f"      Preview: {preview}...")
                    else:
                        try:
                            with open(fpath_out, 'w', encoding='utf-8') as f:
                                f.write(file_content)
                            report_lines.append(f"    [CREATED] {fname}")
                            report_lines.append(f"      Source: {source_file}")
                            stats['created'] += 1
                        except Exception as e:
                            report_lines.append(f"    [ERROR] Could not create {fname}: {e}")
                            stats['errors'] += 1

                    next_seq += 1
                else:
                    report_lines.append(f"    [NOT FOUND IN MISC] {title} / {author}")
                    stats['not_found'] += 1

    # Summary
    report_lines.append("\n" + "=" * 80)
    report_lines.append("SUMMARY")
    report_lines.append("=" * 80)
    report_lines.append(f"Total poems in TOC:      {stats['total_poems_in_toc']}")
    report_lines.append(f"Already extracted:       {stats['already_extracted']}")
    report_lines.append(f"Found in Misc:           {stats['found_in_misc']}")
    report_lines.append(f"Not found:               {stats['not_found']}")
    if not args.dry_run:
        report_lines.append(f"Files created:           {stats['created']}")
        report_lines.append(f"Errors:                  {stats['errors']}")
    report_lines.append("")

    report_text = '\n'.join(report_lines)
    print(report_text)

    # Write report
    try:
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"\nReport written to: {REPORT_PATH}")
    except Exception as e:
        print(f"\nCould not write report: {e}")


if __name__ == '__main__':
    main()
