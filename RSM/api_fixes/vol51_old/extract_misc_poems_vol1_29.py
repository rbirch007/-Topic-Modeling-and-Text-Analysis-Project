#!/usr/bin/env python3
"""
Extract poems from Misc files across Vols 1-29 using CONTENTS files as ground truth.

Handles two different CONTENTS formats:
- Vols 1-10: Poems marked "(Poem)" or "(A Poem)" inline
- Vols 11-29: No poem markers; need heuristic detection from Misc file content

Also handles two different file naming conventions:
- Vol 1: V1_1914_Mon_00_CONTENTS.txt / V1_1914_Mon_00_Misc.txt
- Vols 2-29: Month_VolN_00_CONTENTS.txt / Month_VolN_00_Misc.txt

Usage:
    python extract_misc_poems_vol1_29.py --dry-run    # Preview only
    python extract_misc_poems_vol1_29.py              # Extract for real
"""

import os
import re
import sys
import glob
import argparse
from pathlib import Path
from collections import defaultdict

BASE_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\OutputExtractedarticles"
REPORT_PATH = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\misc_poem_extraction_report_vol1_29.txt"

VOL_YEARS = {
    1: 1914, 2: 1915, 3: 1916, 4: 1917, 5: 1918,
    6: 1919, 7: 1920, 8: 1921, 9: 1922, 10: 1923,
    11: 1924, 12: 1925, 13: 1926, 14: 1927, 15: 1928,
    16: 1929, 17: 1930, 18: 1931, 19: 1932, 20: 1933,
    21: 1934, 22: 1935, 23: 1936, 24: 1937, 25: 1938,
    26: 1939, 27: 1940, 28: 1941, 29: 1942,
}

MONTHS_FULL = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# 3-letter abbreviations used by Vol 1
MONTH_ABBREVS = {
    "January": "Jan", "February": "Feb", "March": "Mar", "April": "Apr",
    "May": "May", "June": "Jun", "July": "Jul", "August": "Aug",
    "September": "Sep", "October": "Oct", "November": "Nov", "December": "Dec"
}

# Known poet names that appear frequently in Vols 11-29
KNOWN_POETS = [
    "Christie Lund Coles", "Christie Lund", "Eva Willes Wangsgaard",
    "Anna Prince Redd", "Grace Sayre", "Dorothy J. Roberts",
    "Alice Morrey Bailey", "Vesta Pierce Crawford", "Bertha A. Kleinman",
    "Gladys Quayle", "Mabel Jones Gabbott", "Grace Ingles Frost",
    "Ida R. Alldredge", "Lula Greene Richards", "Emmeline B. Wells",
    "Alfred Lambourne", "Lucy May Green", "Maud Baggarley",
    "Annie D. Palmer", "Annie D. S. Palmer", "Elsie Talmage Brandley",
    "Elsie C. Carroll", "Florence Jepperson Madsen", "Lois M. Musser",
    "Carrie Tanner", "Claire Stewart Boyer", "Myrtle Wentworth",
    "Rosannah Cannon", "Helen M. Livingston", "Ella J. Coulam",
    "Ivy Houtz Woolley", "Julina B. Green", "Valerie Peck",
    "La Rene King Bleecker", "Beatrice F. Stevens", "Ruth May Fox",
    "Kate Thomas", "Josephine Spencer", "Augusta Joyce Crocheron",
    "Emily Hill Woodmansee", "Sarah E. Carmichael", "Orson F. Whitney",
    "Minnie J. Snow", "Hannah T. King", "Eliza R. Snow",
    "Vilate Raile", "Iris W. Schoon", "Grace Zenor Pratt",
    "Bertha A. Erickson", "Mabel S. Harmer", "Charlotte Owen Sackett",
    "Isabelle Ruby Owen", "Evelyn Hodges", "Lottie Hammer Singley",
    "Sadie Adamson", "Roberta Flake Clayton",
]

# Sections that end the POETRY block in TOC (for vols that have them)
NON_POETRY_SECTIONS = [
    "SPECIAL FEATURES", "FICTION", "GENERAL FEATURES", "FEATURES FOR THE HOME",
    "LESSONS", "PUBLISHED MONTHLY", "Editorial and Business",
    "LESSON DEPARTMENT", "LESSON AIDS", "HOME FEATURES",
    "Guide Lessons", "Lesson Department", "Notes from the Field",
    "Notes to the Field", "Current Topics", "Home Science",
    "Instructions", "Editorial",
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


def parse_poem_marker_line(line):
    """
    Parse a CONTENTS line that has (Poem) or (A Poem) marker.
    Returns (title, author, page) or None.
    """
    line = line.strip()
    if not line:
        return None

    # Remove the (Poem) / (A Poem) marker and capture what's around it
    # Pattern: Title (A Poem) Author Page
    # or: Title (Poem) Author Page
    m = re.match(r'^(.+?)\s*\((?:A\s+)?Poem\)\s*(.*?)(?:\s+(\d{1,4}))?\s*$', line, re.IGNORECASE)
    if not m:
        return None

    title = m.group(1).strip()
    rest = m.group(2).strip() if m.group(2) else ""
    page = m.group(3)

    # rest is the author name (might be empty)
    author = rest

    return (title, author, page)


def parse_contents_line_general(line):
    """
    Parse a general CONTENTS line into (title, author, page).
    Used for Vols 11-29 where there's no poem marker.
    """
    line = line.strip()
    if not line:
        return None

    # Skip obvious non-content lines
    if line.startswith("PUBLISHED MONTHLY"):
        return None
    if line.startswith("Editorial and Business"):
        return None
    if re.match(r'^\$\d', line):
        return None
    if line.startswith("Organ of"):
        return None
    if re.match(r'^Vol\.?\s*\d+', line):
        return None
    if re.match(r'^Room\s+\d+', line):
        return None

    # Remove page number at end
    page = None
    m = re.search(r'\s+(\d{1,4})\s*$', line)
    if m:
        page = m.group(1)
        line = line[:m.start()].strip()
    elif re.search(r'Frontispiece\s*$', line, re.IGNORECASE):
        page = 'Frontispiece'
        line = re.sub(r'\s*Frontispiece\s*$', '', line, flags=re.IGNORECASE).strip()

    if not line:
        return None

    # Try to split into title and author
    # For multi-line format (Vol 25+), author may be on next line - handled by caller
    # For single-line format, author is at the end
    words = line.split()
    if len(words) < 2:
        return (line, "", page)

    best_title = line
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
            if w not in ('de', 'van', 'von', 'la', 'le', 'the', 'of', 'and', 'Dr.')
        )

        if looks_like_name and candidate_title:
            best_title = candidate_title
            best_author = candidate_author
            break

    return (best_title.strip(), best_author.strip(), page)


def parse_contents_poems_v1_10(contents_path):
    """
    Parse poems from CONTENTS for Vols 1-10 where poems are marked with (Poem).
    Returns list of (title, author, page) tuples.
    """
    try:
        with open(contents_path, 'r', encoding='utf-8', errors='replace') as f:
            text = f.read()
    except Exception:
        return []

    poems = []
    # Find all lines with (Poem) or (A Poem) marker
    for line in text.split('\n'):
        if re.search(r'\((?:A\s+)?Poem\)', line, re.IGNORECASE):
            result = parse_poem_marker_line(line)
            if result:
                poems.append(result)

    # Also check for "Poems by" entries
    for line in text.split('\n'):
        if re.search(r'Poems?\s+by\s+', line, re.IGNORECASE):
            m = re.match(r'^(Poems?\s+by\s+.+?)(?:\s+(\d{1,4}))?\s*$', line.strip())
            if m:
                title_author = m.group(1).strip()
                page = m.group(2)
                poems.append((title_author, "", page))

    return poems


def parse_contents_all_entries(contents_path):
    """
    Parse ALL entries from a CONTENTS file for Vols 11-29.
    Returns list of (title, author, page) tuples.
    """
    try:
        with open(contents_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    except Exception:
        return []

    entries = []
    # Detect if this is a multi-line format (author on separate line)
    # by checking if many lines are just names without page numbers
    name_only_lines = 0
    content_lines = 0
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        content_lines += 1
        words = stripped.split()
        if 1 <= len(words) <= 4 and all(w[0].isupper() for w in words if len(w) > 1):
            if not re.search(r'\d', stripped):
                name_only_lines += 1

    is_multiline = (content_lines > 0 and name_only_lines / max(content_lines, 1) > 0.2)

    if is_multiline:
        # Multi-line format: title on one line, author on next
        i = 0
        while i < len(lines):
            stripped = lines[i].strip()
            if not stripped:
                i += 1
                continue

            # Skip headers
            if stripped in ("CONTENTS", "CONTENTS:", "Page") or re.match(r'^CONTENTS\s+\d{4}', stripped):
                i += 1
                continue
            if stripped.startswith("PUBLISHED MONTHLY") or stripped.startswith("Editorial and Business"):
                break
            if stripped.startswith("Organ of"):
                i += 1
                continue
            if re.match(r'^\$', stripped) or re.match(r'^Room\s+\d+', stripped):
                i += 1
                continue

            # Remove page number
            page = None
            line_text = stripped
            m = re.search(r'\s+(\d{1,4})\s*$', line_text)
            if m:
                page = m.group(1)
                line_text = line_text[:m.start()].strip()
            elif re.search(r'Frontispiece\s*$', line_text, re.IGNORECASE):
                page = 'Frontispiece'
                line_text = re.sub(r'\s*Frontispiece\s*$', '', line_text, flags=re.IGNORECASE).strip()

            title = line_text

            # Check if next line is an author name
            author = ""
            if i + 1 < len(lines):
                next_stripped = lines[i + 1].strip()
                if next_stripped:
                    next_words = next_stripped.split()
                    # Author line: 1-5 capitalized words, no page number typically
                    if 1 <= len(next_words) <= 5:
                        all_cap = all(w[0].isupper() for w in next_words if len(w) > 1)
                        no_digits = not re.search(r'\d', next_stripped)
                        # Not a title-like line (those tend to be longer or have specific patterns)
                        if all_cap and no_digits and len(next_stripped) < 40:
                            author = next_stripped
                            i += 1

            if title:
                entries.append((title, author, page))
            i += 1
    else:
        # Single-line format: Title Author Page
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped in ("CONTENTS", "CONTENTS:", "Page") or re.match(r'^CONTENTS\s+\w+,?\s+\d{4}', stripped):
                continue
            if stripped.startswith("[Vol"):
                continue
            if stripped.startswith("PUBLISHED MONTHLY") or stripped.startswith("Editorial and Business"):
                break

            result = parse_contents_line_general(stripped)
            if result:
                entries.append(result)

    return entries


def is_known_poet(author):
    """Check if the author is a known poet."""
    if not author:
        return False
    norm = normalize_author(author)
    for poet in KNOWN_POETS:
        if normalize_author(poet) == norm:
            return True
        # Partial match: last name match
        poet_last = poet.split()[-1].lower()
        author_last = author.split()[-1].lower() if author.split() else ""
        if poet_last == author_last and len(poet_last) > 3:
            # Check first name initial too
            poet_first = poet.split()[0][0].lower() if poet.split() else ""
            author_first = author.split()[0][0].lower() if author.split() else ""
            if poet_first == author_first:
                return True
    return False


def looks_like_poem_content(text):
    """
    Check if a block of text looks like a poem based on structure.
    Poems have: short lines, verse-like structure, typically 4-40 lines.
    """
    lines = [l for l in text.strip().split('\n') if l.strip()]
    if len(lines) < 3:
        return False
    if len(lines) > 60:
        return False

    # Check average line length (poems have shorter lines than prose)
    avg_len = sum(len(l.strip()) for l in lines) / len(lines)
    if avg_len > 70:
        return False

    # Count short lines (< 60 chars) - poems should have mostly short lines
    short_lines = sum(1 for l in lines if len(l.strip()) < 60)
    if short_lines / len(lines) < 0.6:
        return False

    # Check for verse-like patterns: lines starting with capital letters
    cap_starts = sum(1 for l in lines if l.strip() and l.strip()[0].isupper())
    if cap_starts / len(lines) < 0.5:
        return False

    # Check it's not a list of names/addresses or organizational content
    org_words = ['President', 'Counselor', 'Secretary', 'Treasurer', 'Board', 'Committee',
                 'Department', 'Subscription', 'Price', 'Address', 'Telephone']
    org_count = sum(1 for l in lines for w in org_words if w in l)
    if org_count > 2:
        return False

    # Reject advertisement / boilerplate content
    all_text = ' '.join(l.strip() for l in lines)
    ad_patterns = [
        r'When Buying Mention', r'Relief Society Magazine', r'INSURANCE',
        r'GARMENTS', r'Salt Lake City', r'Bishop.s Bldg', r'Single Copy',
        r'\$\d+\.\d+', r'Subscription', r'second-class matter',
        r'Coal', r'Washer', r'Ironer', r'LAUNDRY', r'BEAUTY',
        r'Postage prepaid', r'Mail.*Order', r'OFFER', r'DISCOUNT',
        r'Entered as', r'Post Office', r'Foreign.*Year',
        r'Latter.day Saints', r'PUBLISHED MONTHLY', r'Editorial and Business',
    ]
    ad_count = sum(1 for pat in ad_patterns if re.search(pat, all_text, re.IGNORECASE))
    if ad_count >= 2:
        return False

    return True


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
        if fname.startswith('_backup'):
            continue
        if 'Misc' in fname or 'CONTENTS' in fname or 'BOARD' in fname:
            continue

        # Check various naming patterns
        fname_lower = fname.lower().replace('_', ' ').replace('.txt', '')

        # Check for Poem/Poetry category marker
        is_poem_file = bool(re.search(r'(poem|poetry)', fname_lower, re.IGNORECASE))

        # Check title match
        title_words = [w for w in norm_title.split() if len(w) > 2]
        if title_words:
            significant_words = title_words[:4]
            matches = sum(1 for w in significant_words if w in fname_lower)
            if matches >= min(2, len(significant_words)):
                return fname

        # Author + partial title match
        if norm_author:
            author_parts = norm_author.split()
            if author_parts and author_parts[-1] in fname_lower:
                if any(w in fname_lower for w in title_words if len(w) > 2):
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
        # Handle both naming conventions
        m = re.match(rf'(?:{month}_Vol{vol_num}|V{vol_num}_\d{{4}}_\w+)_(\d+)', fname)
        if m:
            seq = int(m.group(1))
            if seq > max_seq:
                max_seq = seq

    return max_seq + 1


def get_misc_files(month_dir, month, vol_num, year):
    """Get all Misc files for a given month, sorted. Does NOT include CONTENTS."""
    files = []

    # Standard naming: Month_VolN_00_Misc.txt
    base_misc = os.path.join(month_dir, f"{month}_Vol{vol_num}_00_Misc.txt")
    if os.path.exists(base_misc):
        files.append(base_misc)

    # Vol 1 naming: V1_YEAR_Mon_00_Misc.txt
    abbrev = MONTH_ABBREVS.get(month, month[:3])
    vol1_misc = os.path.join(month_dir, f"V{vol_num}_{year}_{abbrev}_00_Misc.txt")
    if os.path.exists(vol1_misc) and vol1_misc not in files:
        files.append(vol1_misc)

    # Alternative: Month_VolN_Misc.txt (no 00_)
    alt_misc = os.path.join(month_dir, f"{month}_Vol{vol_num}_Misc.txt")
    if os.path.exists(alt_misc) and alt_misc not in files:
        files.append(alt_misc)

    # Continuation files
    patterns = [
        os.path.join(month_dir, f"{month}_Vol{vol_num}_Misc_cont_*.txt"),
        os.path.join(month_dir, f"{month}_Vol{vol_num}_Misc_back_matter.txt"),
        os.path.join(month_dir, f"{month}_Vol{vol_num}_Misc_front_matter.txt"),
        os.path.join(month_dir, f"V{vol_num}_{year}_{abbrev}_Misc_cont_*.txt"),
    ]
    for pattern in patterns:
        cont_files = sorted(glob.glob(pattern))
        for cf in cont_files:
            if cf not in files:
                files.append(cf)

    return files


def get_contents_path(month_dir, month, vol_num, year):
    """Find the CONTENTS file, handling both naming conventions."""
    # Standard naming
    p = os.path.join(month_dir, f"{month}_Vol{vol_num}_00_CONTENTS.txt")
    if os.path.exists(p):
        return p

    # Vol 1 naming
    abbrev = MONTH_ABBREVS.get(month, month[:3])
    p = os.path.join(month_dir, f"V{vol_num}_{year}_{abbrev}_00_CONTENTS.txt")
    if os.path.exists(p):
        return p

    return None


def is_toc_line(line):
    """Detect if a line is a TOC entry."""
    stripped = line.strip()
    if not stripped:
        return False
    if re.search(r'\s+\d{1,4}\s*$', stripped):
        if len(stripped) < 120:
            return True
    if re.search(r'Back of Front', stripped, re.IGNORECASE):
        if len(stripped) < 120:
            return True
    return False


def is_in_toc_block(lines, line_idx):
    """Check if the given line index is within a TOC/CONTENTS block."""
    for i in range(max(0, line_idx - 20), line_idx):
        stripped = lines[i].strip()
        if stripped in ("POETRY", "FICTION", "SPECIAL FEATURES", "GENERAL FEATURES",
                        "LESSONS", "FEATURES FOR THE HOME", "HOME FEATURES",
                        "LESSON DEPARTMENT", "Contents", "CONTENTS", "CONTENTS:"):
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

        if norm_line.startswith(norm_title):
            title_match = True
        elif len(title_words) >= 2:
            title_start = ' '.join(title_words[:3])
            if norm_line.startswith(title_start) or title_start in norm_line[:len(title_start)+20]:
                title_match = True
        elif len(title_words) == 1 and len(title_words[0]) > 4:
            # Single significant word title - need exact start match
            if norm_line.startswith(title_words[0]):
                title_match = True

        if not title_match:
            continue

        # Skip TOC lines
        if is_toc_line(stripped) and len(stripped) < 150:
            continue
        if is_in_toc_block(lines, i) and len(stripped) < 150:
            continue
        if stripped.isupper() and len(stripped) < 150:
            continue

        # Check collapsed poem
        if len(stripped) > 150 and author:
            poem_text = extract_collapsed_poem(stripped, title, author)
            if poem_text:
                candidates.append(('collapsed', poem_text, i))
                continue

        # Multi-line poem
        poem_lines = extract_poem_block(lines, i, title, author)
        if poem_lines:
            candidates.append(('block', '\n'.join(poem_lines), i))

    if not candidates:
        return None

    candidates.sort(key=lambda c: (0 if c[0] == 'block' else 1, -len(c[1])))
    return candidates[0][1]


def extract_poem_block(lines, start_idx, title, author):
    """Extract a multi-line poem block starting from the title line."""
    poem_lines = []
    first_line = lines[start_idx].strip()
    poem_lines.append(first_line)

    found_author = False
    consecutive_empty = 0

    for j in range(start_idx + 1, min(start_idx + 60, len(lines))):
        line = lines[j].strip()

        if not line:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            if len(poem_lines) > 2:
                next_content = ""
                for k in range(j + 1, min(j + 3, len(lines))):
                    if lines[k].strip():
                        next_content = lines[k].strip()
                        break
                if next_content and is_poem_line(next_content) and len(next_content) < 80:
                    poem_lines.append("")
                    continue
                else:
                    break
            continue

        consecutive_empty = 0

        if is_end_signal(line):
            break

        # Author line detection
        if not found_author and author and len(poem_lines) <= 2:
            author_last = author.split()[-1].lower() if author else ""
            if author_last and author_last in line.lower():
                if len(line) < 60:
                    poem_lines.append(line)
                    found_author = True
                    continue

        if len(line) > 100:
            break

        if is_toc_line(line):
            break

        if is_poem_line(line):
            poem_lines.append(line)
        else:
            break

    verse_lines = [l for l in poem_lines[1:] if l.strip() and not is_just_author(l, author)]
    if len(verse_lines) < 2:
        return None

    all_text = ' '.join(l for l in poem_lines[1:] if l.strip())
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
    return norm_line == norm_auth or (norm_auth in norm_line and len(line.strip()) < 40)


def extract_collapsed_poem(line, title, author):
    """Extract a poem from a single collapsed OCR line."""
    if not author:
        return None

    author_last = author.split()[-1]
    title_end = 0
    for t_word in title.split():
        pos = line.upper().find(t_word.upper(), title_end)
        if pos >= 0:
            title_end = pos + len(t_word)

    search_start = max(0, title_end)
    m = re.search(re.escape(author_last), line[search_start:], re.IGNORECASE)
    if not m:
        return None

    author_end = search_start + m.end()
    remaining = line[author_end:].strip()

    if not remaining or len(remaining) < 20:
        return None

    end_markers = [
        r'\s*The Cover:',
        r'\s*VOL\.\s*\d+',
        r'\s*Page\s+\d+',
        r'\s*INSURANCE',
        r'\s*RELIEF SOCIETY',
        r'\s*Photograph by',
        r'\s*\|\s*[\'"]?\s*[a-zA-Z]{1,3}\s+[A-Z]{2}',
    ]

    poem_text = remaining
    for marker in end_markers:
        m2 = re.search(marker, poem_text, re.IGNORECASE)
        if m2:
            candidate = poem_text[:m2.start()].strip()
            if len(candidate) >= 20:
                poem_text = candidate

    if not poem_text or len(poem_text) < 20:
        return None

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
    if re.match(r'^=== (FRONT|BACK) MATTER', line):
        return True
    return False


def scan_misc_for_poems(misc_text, known_poets_lower):
    """
    Scan Misc text for poem-like blocks, especially for Vols 11-29
    where CONTENTS doesn't mark poems.
    Only matches blocks where a known poet name is found.
    Returns list of (title, author, poem_text, start_line) tuples.
    """
    lines = misc_text.split('\n')
    found_poems = []

    # Skip patterns for titles that are clearly not poems
    skip_title_patterns = [
        r'^When Buying', r'^Relief Society', r'^PUBLISHED', r'^Editorial',
        r'^Subscription', r'^Complete Suits', r'^Entered as', r'^Salt Lake',
        r'^Foreign', r'^Canada', r'^Organ of', r'^Room \d+', r'^\$',
        r'^No\.\s+\d+', r'^L\.\s*D\.\s*S', r'^GARMENTS', r'^INSURANCE',
        r'^Ask ', r'^Genuine ', r'^Our ', r'^If we are', r'^Utah Coal',
        r'^The BIG HOME', r'^BENEFICIAL', r'^HEBER', r'^Lesson Preview',
        r'^Guide Lessons', r'^Notes from', r'^Notes to', r'^Lesson Department',
        r'^Conference', r'^Paris Letter', r'^Jesus Christ',
        r'^=== ', r'^\[Vol',
    ]

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped:
            i += 1
            continue

        # Skip obviously non-poem content
        if stripped.startswith('==='):
            i += 1
            continue
        if stripped.isupper() and len(stripped) > 10:
            i += 1
            continue
        if re.match(r'^\[Vol\d+', stripped):
            i += 1
            continue

        # Skip known non-poem title patterns
        should_skip = False
        for pat in skip_title_patterns:
            if re.match(pat, stripped, re.IGNORECASE):
                should_skip = True
                break
        if should_skip:
            i += 1
            continue

        # Check if this line could be a poem title followed by verse
        # A poem title is typically a short line (< 50 chars) followed by
        # author or verse lines
        if 3 <= len(stripped) <= 50:
            # Look ahead for poem-like content
            block_lines = [stripped]
            author_found = ""
            j = i + 1

            # Check for author line right after title
            if j < len(lines) and lines[j].strip():
                next_line = lines[j].strip()
                # Is it a known poet? (require exact match)
                for poet in KNOWN_POETS:
                    if normalize_author(poet) == normalize_author(next_line):
                        author_found = next_line
                        block_lines.append(next_line)
                        j += 1
                        break

            # Only proceed if we found a known poet
            if not author_found:
                i += 1
                continue

            # Now collect verse lines
            verse_lines = []
            consecutive_empty = 0
            while j < len(lines) and j < i + 50:
                vline = lines[j].strip()
                if not vline:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                    verse_lines.append("")
                    j += 1
                    continue
                consecutive_empty = 0
                if is_end_signal(vline):
                    break
                if is_toc_line(vline):
                    break
                if len(vline) > 80:
                    break
                verse_lines.append(vline)
                j += 1

            # Remove trailing empty lines
            while verse_lines and not verse_lines[-1]:
                verse_lines.pop()

            # Check if this looks like a poem
            if len(verse_lines) >= 3 and len(verse_lines) <= 40:
                avg_len = sum(len(l) for l in verse_lines if l) / max(1, len([l for l in verse_lines if l]))
                if avg_len < 55:
                    poem_text = '\n'.join(block_lines + [''] + verse_lines)
                    # Final validation
                    if looks_like_poem_content(poem_text):
                        found_poems.append((stripped, author_found, poem_text, i))
                        i = j
                        continue

        i += 1

    return found_poems


def main():
    parser = argparse.ArgumentParser(description="Extract poems from Misc files Vols 1-29")
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
        'poems_detected_heuristic': 0,
    }
    vol_stats = {}

    report_lines.append("=" * 80)
    report_lines.append("MISC POEM EXTRACTION REPORT - Vols 1-29")
    report_lines.append(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    report_lines.append("=" * 80)
    report_lines.append("")

    for vol_num in range(1, 30):
        year = VOL_YEARS[vol_num]
        vol_dir = os.path.join(BASE_DIR, f"Vol{vol_num}_{year}")

        if not os.path.isdir(vol_dir):
            report_lines.append(f"Vol{vol_num}_{year}: Directory not found, skipping")
            continue

        report_lines.append(f"\n{'='*60}")
        report_lines.append(f"Vol{vol_num} ({year})")
        report_lines.append(f"{'='*60}")

        vol_found = 0
        vol_extracted = 0

        for month in MONTHS_FULL:
            month_dir = os.path.join(vol_dir, month)
            if not os.path.isdir(month_dir):
                continue

            contents_path = get_contents_path(month_dir, month, vol_num, year)
            if not contents_path:
                report_lines.append(f"  {month}: No CONTENTS file")
                continue

            # Phase 1: Parse CONTENTS for poems
            if vol_num <= 10:
                # Vols 1-10: explicit (Poem) markers
                poems = parse_contents_poems_v1_10(contents_path)
                if not poems:
                    continue

                report_lines.append(f"\n  {month} - {len(poems)} poems marked in TOC:")
                stats['total_poems_in_toc'] += len(poems)
                vol_found += len(poems)

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
                misc_files = get_misc_files(month_dir, month, vol_num, year)
                if not misc_files:
                    for title, author, page in missing_poems:
                        report_lines.append(f"    [NO MISC] {title} / {author}")
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
                    # Clean title: remove "(A Poem)" from the title for searching
                    clean_title = re.sub(r'\s*\((?:A\s+)?Poem\)\s*', ' ', title, flags=re.IGNORECASE).strip()

                    poem_text = None
                    source_file = None

                    for fpath, text in all_misc:
                        poem_text = extract_poem_from_text(text, clean_title, author)
                        if poem_text:
                            source_file = os.path.basename(fpath)
                            break

                    if poem_text:
                        stats['found_in_misc'] += 1
                        vol_extracted += 1

                        title_part = sanitize_for_filename(clean_title)
                        author_part = sanitize_for_filename(author, max_words=2)

                        # Determine filename prefix based on vol naming convention
                        if vol_num == 1:
                            abbrev = MONTH_ABBREVS.get(month, month[:3])
                            if author_part:
                                fname = f"V{vol_num}_{year}_{abbrev}_{next_seq:02d}_Poem_{title_part}_{author_part}.txt"
                            else:
                                fname = f"V{vol_num}_{year}_{abbrev}_{next_seq:02d}_Poem_{title_part}.txt"
                        else:
                            if author_part:
                                fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}_{author_part}.txt"
                            else:
                                fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}.txt"

                        fpath_out = os.path.join(month_dir, fname)

                        file_content = f"[Vol{vol_num} | {month} {year} | Sequence: {next_seq:02d} | Category: Poem | Title: {clean_title}]\n\n"
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
                        report_lines.append(f"    [NOT FOUND IN MISC] {clean_title} / {author}")
                        stats['not_found'] += 1

            else:
                # Vols 11-29: No poem markers - parse all entries and check Misc
                all_entries = parse_contents_all_entries(contents_path)

                # Get Misc files
                misc_files = get_misc_files(month_dir, month, vol_num, year)
                if not misc_files:
                    continue

                all_misc = []
                for fpath in misc_files:
                    try:
                        with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                            text = f.read()
                        all_misc.append((fpath, text))
                    except Exception:
                        pass

                if not all_misc:
                    continue

                known_poets_lower = set(normalize_author(p) for p in KNOWN_POETS)
                poems_found_this_month = []

                # Strategy 1: Check each CONTENTS entry against Misc files
                # Look for entries by known poets or with poem-like titles
                for title, author, page in all_entries:
                    # Skip non-poem entries
                    if not title:
                        continue
                    # Skip obvious non-poem entries
                    skip_patterns = [
                        r'^Guide Lessons', r'^Lesson Department', r'^Notes from',
                        r'^Notes to', r'^Editorial', r'^Happenings',
                        r'^Conference', r'^PUBLISHED', r'^Organ of',
                        r'^Portrait', r'^Frontispiece', r'^Lesson Preview',
                        r'^Address$', r'^Music Department', r'^Book Review',
                        r'^Celebrates', r'^Centenary', r'^Conventions',
                        r'^Relief Society', r'^Singing Mothers',
                        r'^Foreign', r'^Canada and Foreign', r'^Subscription',
                        r'^Jesus Christ', r'^Latter.day Saints',
                        r'^Paris Letter', r'^Utah State',
                        r'^Good Health', r'^Social Work',
                        r'^World Happenings', r'^International',
                        r'^The Children.s Bureau', r'^Community Plan',
                        r'^Preparing the Meal', r'^Your Home',
                        r'^True Beaver', r'^Ingenious',
                        r'^Martha McChesney', r'^Roses With',
                        r'^Excerpts from', r'^Mary Connelly',
                    ]
                    should_skip = False
                    for pat in skip_patterns:
                        if re.match(pat, title, re.IGNORECASE):
                            should_skip = True
                            break
                    if should_skip:
                        continue

                    # Check if this could be a poem:
                    # Only check entries by known poets
                    is_candidate = False

                    if is_known_poet(author):
                        is_candidate = True

                    if not is_candidate:
                        continue

                    # Check if already extracted
                    existing = find_existing_poem_file(month_dir, month, vol_num, title, author)
                    if existing:
                        # Check if it's actually a poem file
                        if 'Poem' in existing or 'Poetry' in existing:
                            poems_found_this_month.append((title, author, 'exists', existing))
                            continue
                        # It exists but not as poem - might be mis-categorized, skip
                        continue

                    # Search in Misc files
                    poem_text = None
                    source_file = None
                    for fpath, text in all_misc:
                        poem_text = extract_poem_from_text(text, title, author)
                        if poem_text:
                            # Verify it looks like a poem
                            if looks_like_poem_content(poem_text):
                                source_file = os.path.basename(fpath)
                                break
                            else:
                                poem_text = None

                    if poem_text:
                        poems_found_this_month.append((title, author, 'extract', poem_text))
                        stats['found_in_misc'] += 1
                        stats['poems_detected_heuristic'] += 1
                        vol_found += 1
                        vol_extracted += 1

                        next_seq = get_next_sequence(month_dir, month, vol_num)
                        title_part = sanitize_for_filename(title)
                        author_part = sanitize_for_filename(author, max_words=2)
                        if author_part:
                            fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}_{author_part}.txt"
                        else:
                            fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}.txt"

                        fpath_out = os.path.join(month_dir, fname)

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
                                report_lines.append(f"    [ERROR] {fname}: {e}")
                                stats['errors'] += 1

                # Strategy 2: Scan Misc files directly for verse blocks by known poets
                for fpath, text in all_misc:
                    scan_results = scan_misc_for_poems(text, known_poets_lower)
                    for scan_title, scan_author, scan_text, scan_line in scan_results:
                        # Check if we already found this one
                        already_found = False
                        for t, a, status, _ in poems_found_this_month:
                            if normalize_title(t) == normalize_title(scan_title):
                                already_found = True
                                break
                        if already_found:
                            continue

                        # Check if already extracted as a standalone file
                        existing = find_existing_poem_file(month_dir, month, vol_num, scan_title, scan_author)
                        if existing:
                            continue

                        poems_found_this_month.append((scan_title, scan_author, 'heuristic', scan_text))
                        stats['found_in_misc'] += 1
                        stats['poems_detected_heuristic'] += 1
                        vol_found += 1
                        vol_extracted += 1

                        next_seq = get_next_sequence(month_dir, month, vol_num)
                        title_part = sanitize_for_filename(scan_title)
                        author_part = sanitize_for_filename(scan_author, max_words=2)
                        if author_part:
                            fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}_{author_part}.txt"
                        else:
                            fname = f"{month}_Vol{vol_num}_{next_seq:02d}_Poem_{title_part}.txt"

                        fpath_out = os.path.join(month_dir, fname)

                        file_content = f"[Vol{vol_num} | {month} {year} | Sequence: {next_seq:02d} | Category: Poem | Title: {scan_title}]\n\n"
                        file_content += scan_text.strip() + "\n"

                        preview = scan_text.strip()[:120].replace('\n', ' | ')

                        if args.dry_run:
                            report_lines.append(f"    [WOULD CREATE - HEURISTIC] {fname}")
                            report_lines.append(f"      Source: {os.path.basename(fpath)}")
                            report_lines.append(f"      Preview: {preview}...")
                        else:
                            try:
                                with open(fpath_out, 'w', encoding='utf-8') as f:
                                    f.write(file_content)
                                report_lines.append(f"    [CREATED - HEURISTIC] {fname}")
                                report_lines.append(f"      Source: {os.path.basename(fpath)}")
                                stats['created'] += 1
                            except Exception as e:
                                report_lines.append(f"    [ERROR] {fname}: {e}")
                                stats['errors'] += 1

                if poems_found_this_month:
                    report_lines.append(f"\n  {month} - {len(poems_found_this_month)} poems found:")
                    for t, a, status, detail in poems_found_this_month:
                        if status == 'exists':
                            report_lines.append(f"    [EXISTS] {t} / {a} -> {detail}")
                            stats['already_extracted'] += 1

        vol_stats[vol_num] = {'found': vol_found, 'extracted': vol_extracted}

    # Summary
    report_lines.append("\n" + "=" * 80)
    report_lines.append("SUMMARY")
    report_lines.append("=" * 80)
    report_lines.append(f"Total poems in TOC (Vols 1-10):     {stats['total_poems_in_toc']}")
    report_lines.append(f"Poems detected heuristically (11-29): {stats['poems_detected_heuristic']}")
    report_lines.append(f"Already extracted:                   {stats['already_extracted']}")
    report_lines.append(f"Found in Misc:                       {stats['found_in_misc']}")
    report_lines.append(f"Not found:                           {stats['not_found']}")
    if not args.dry_run:
        report_lines.append(f"Files created:                       {stats['created']}")
        report_lines.append(f"Errors:                              {stats['errors']}")

    report_lines.append("\n" + "-" * 60)
    report_lines.append("PER-VOLUME BREAKDOWN:")
    report_lines.append("-" * 60)
    for vol_num in range(1, 30):
        year = VOL_YEARS[vol_num]
        vs = vol_stats.get(vol_num, {'found': 0, 'extracted': 0})
        report_lines.append(f"  Vol{vol_num} ({year}): {vs['found']} found, {vs['extracted']} extracted")

    report_lines.append("")

    report_text = '\n'.join(report_lines)
    # Print with error handling for Windows console encoding
    try:
        print(report_text)
    except UnicodeEncodeError:
        print(report_text.encode('ascii', errors='replace').decode('ascii'))

    try:
        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"\nReport written to: {REPORT_PATH}")
    except Exception as e:
        print(f"\nCould not write report: {e}")


if __name__ == '__main__':
    main()
