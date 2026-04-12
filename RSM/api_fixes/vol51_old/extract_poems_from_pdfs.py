#!/usr/bin/env python3
"""
Extract 306 missing poems from PDF files for Volumes 30-40.
Parses the misc_poem_extraction_report.txt to find NOT FOUND IN MISC poems,
looks up page numbers from CONTENTS files, extracts text from PDFs.
"""

import os
import re
import json
import sys
import fitz  # PyMuPDF
from pathlib import Path
from collections import defaultdict

# Base paths
BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles"
OUTPUT_BASE = os.path.join(BASE, "OutputExtractedarticles")
PDF_BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"
REPORT_FILE = os.path.join(BASE, "misc_poem_extraction_report.txt")
PAGE_MAP_FILE = os.path.join(BASE, "vol30_40_pdf_page_map.json")
OUTPUT_REPORT = os.path.join(BASE, "pdf_poem_extraction_report_vol30_40.txt")

# Volume years
VOL_YEARS = {
    30: 1943, 31: 1944, 32: 1945, 33: 1946, 34: 1947,
    35: 1948, 36: 1949, 37: 1950, 38: 1951, 39: 1952, 40: 1953
}

MONTHS_ORDER = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"]


def load_page_map():
    with open(PAGE_MAP_FILE, 'r') as f:
        return json.load(f)


def parse_missing_poems_from_report():
    """Parse the misc_poem_extraction_report.txt to extract NOT FOUND IN MISC entries."""
    missing = []
    current_vol = None
    current_year = None
    current_month = None

    with open(REPORT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')

            # Match volume header: Vol30 (1943)
            m = re.match(r'^Vol(\d+)\s*\((\d{4})\)', line)
            if m:
                current_vol = int(m.group(1))
                current_year = int(m.group(2))
                continue

            # Match month header: "  January - 7 poems in TOC:"
            m = re.match(r'^\s+(\w+)\s*-\s*\d+\s*poems?\s*in\s*TOC', line)
            if m:
                current_month = m.group(1)
                continue

            # Match NOT FOUND IN MISC lines
            m = re.match(r'^\s+\[NOT FOUND IN MISC\]\s+(.*)', line)
            if m and current_vol is not None and current_month is not None:
                raw = m.group(1).strip()
                missing.append({
                    'vol': current_vol,
                    'year': current_year,
                    'month': current_month,
                    'raw': raw
                })

    return missing


def parse_title_author(raw):
    """
    Parse the raw string from the report like:
      'Mabel / Jones Gabbott'
      'No Priorities Dott J. Sartori / '
      'The Dream / Is Ours Christie Lund Coles'
      'Elsie C. Carroll / '

    The '/' in the raw string indicates line-breaks in the TOC, not a separator.
    We need to reconstruct the full title and author from these entries.
    """
    # Remove the / which represents line breaks in the TOC
    clean = raw.replace(' / ', ' ').replace('/ ', '').replace(' /', '').strip()
    return clean


def parse_contents_poetry_section(contents_path):
    """Parse the POETRY section of a CONTENTS file.
    Returns list of dicts with title, author, page_num."""
    poems = []
    if not os.path.exists(contents_path):
        return poems

    with open(contents_path, 'r', encoding='utf-8', errors='replace') as f:
        text = f.read()

    # Find POETRY section
    poetry_match = re.search(r'\bPOETRY\b', text, re.IGNORECASE)
    if not poetry_match:
        return poems

    # Get text from POETRY to end or next section
    poetry_text = text[poetry_match.end():]
    # Cut at next major section or "PUBLISHED MONTHLY"
    end_match = re.search(r'\b(PUBLISHED MONTHLY|LESSON DEPARTMENT|FEATURES FOR THE HOME|$)', poetry_text)
    if end_match:
        poetry_text = poetry_text[:end_match.start()]

    # Parse each line in poetry section
    lines = poetry_text.strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Try to extract page number from end of line
        # Patterns: "Title Author 123" or "Title Author Back of Frontispiece"
        page_match = re.search(r'\b(\d+)\s*$', line)
        page_num = None
        if page_match:
            page_num = int(page_match.group(1))
            line_no_page = line[:page_match.start()].strip()
        else:
            # Check for "Back of Frontispiece" or "Frontispiece"
            fp_match = re.search(r'(Back of )?Frontispiece\s*$', line, re.IGNORECASE)
            if fp_match:
                page_num = 1  # Frontispiece is typically page 1
                line_no_page = line[:fp_match.start()].strip()
            else:
                line_no_page = line

        if line_no_page:
            poems.append({
                'line': line_no_page,
                'full_line': line,
                'page_num': page_num
            })

    return poems


def find_poem_page_in_contents(contents_poems, title_author_raw):
    """Try to match a missing poem to its CONTENTS entry to get page number."""
    # Clean up the raw title/author by removing /
    clean = title_author_raw.replace(' / ', ' ').replace('/ ', '').replace(' /', '').strip()
    clean_lower = clean.lower()

    # Extract key words for matching
    words = [w for w in clean_lower.split() if len(w) > 2]

    best_match = None
    best_score = 0

    for cp in contents_poems:
        line_lower = cp['line'].lower()

        # Try exact substring match first
        if clean_lower in line_lower or line_lower in clean_lower:
            return cp['page_num']

        # Count matching words
        score = sum(1 for w in words if w in line_lower)
        if score > best_score:
            best_score = score
            best_match = cp

    # Require at least half the words to match
    if best_match and best_score >= max(2, len(words) // 2):
        return best_match['page_num']

    return None


def mag_page_to_pdf_page(vol, month, mag_page, page_map):
    """Convert magazine page number to PDF page number (0-indexed).
    Magazine page 1 corresponds to the 'start' PDF page in the map."""
    vol_str = str(vol)
    if vol_str not in page_map:
        return None
    if month not in page_map[vol_str]:
        return None

    start = page_map[vol_str][month]['start']
    # Magazine page 1 = PDF page 'start'
    # But often frontispiece is page 0 in the issue, and page 1 is actually start+0
    # The start value is the 0-indexed PDF page for the start of that month's issue
    # Magazine page numbers typically start from the content after the cover
    # Let's map: mag_page N -> pdf_page = start + (N - 1)
    # But we need to verify this makes sense
    pdf_page = start + mag_page - 1
    return pdf_page


def extract_text_from_pdf_page(pdf_path, page_num):
    """Extract text from a specific PDF page (0-indexed)."""
    try:
        doc = fitz.open(pdf_path)
        if page_num < 0 or page_num >= len(doc):
            doc.close()
            return None
        page = doc[page_num]
        text = page.get_text()
        doc.close()
        return text
    except Exception as e:
        return None


def search_pdf_for_poem(pdf_path, title_words, start_page, end_page):
    """Search a range of PDF pages for text matching the poem title.
    Returns (page_num, page_text) or (None, None)."""
    try:
        doc = fitz.open(pdf_path)
        max_page = min(end_page, len(doc) - 1)

        for pg in range(max(0, start_page), max_page + 1):
            text = doc[pg].get_text()
            text_lower = text.lower()

            # Check if title words appear on this page
            matches = sum(1 for w in title_words if w.lower() in text_lower)
            if matches >= len(title_words):
                doc.close()
                return pg, text

        # Second pass: more lenient matching (at least 2/3 of words)
        threshold = max(1, int(len(title_words) * 0.6))
        for pg in range(max(0, start_page), max_page + 1):
            text = doc[pg].get_text()
            text_lower = text.lower()
            matches = sum(1 for w in title_words if w.lower() in text_lower)
            if matches >= threshold:
                doc.close()
                return pg, text

        doc.close()
    except Exception as e:
        pass

    return None, None


def extract_poem_text(page_text, title_words, author_parts):
    """Extract the poem portion from a full page of text.
    Returns the poem text (title + author + verses)."""
    lines = page_text.split('\n')
    lines = [l.strip() for l in lines]

    # Find the title line
    title_str = ' '.join(title_words)
    title_lower = title_str.lower()

    best_start = -1
    best_score = 0

    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        if not line_lower:
            continue

        # Check for title match
        matches = sum(1 for w in title_words if w.lower() in line_lower)

        # Also check if title spans across consecutive lines
        combined = line_lower
        if i + 1 < len(lines):
            combined = line_lower + ' ' + lines[i + 1].lower().strip()

        combined_matches = sum(1 for w in title_words if w.lower() in combined)

        score = max(matches, combined_matches)
        if score > best_score and score >= max(1, len(title_words) - 1):
            best_score = score
            best_start = i

    if best_start < 0:
        # Couldn't find title, return full text stripped
        return page_text.strip()

    # Now extract from title line to the end of the poem
    # Poems typically end when we hit:
    # - A long prose paragraph
    # - A new article title (all caps)
    # - Page number indicator
    # - "VOL." or volume indicator

    poem_lines = []
    started = False
    blank_count = 0
    found_author = False

    for i in range(best_start, len(lines)):
        line = lines[i].strip()

        if not started:
            started = True

        if not line:
            blank_count += 1
            if blank_count >= 3:
                break
            poem_lines.append('')
            continue
        else:
            blank_count = 0

        # Check if we've hit a new section (all-caps line that's not our title)
        if (i > best_start + 2 and line.isupper() and len(line) > 15
                and not any(w.lower() in line.lower() for w in title_words)):
            break

        # Check if we've hit a clearly prose section (very long line)
        if i > best_start + 3 and len(line) > 120 and not found_author:
            break

        # Check for volume/page indicators
        if re.match(r'^VOL\.\s*\d+', line, re.IGNORECASE):
            break
        if re.match(r'^Page\s+\d+', line):
            break
        if re.match(r'^\d{3,}$', line):  # Just a page number
            break

        # Check for author match
        if author_parts:
            auth_matches = sum(1 for a in author_parts if a.lower() in line.lower())
            if auth_matches >= len(author_parts) * 0.5:
                found_author = True

        poem_lines.append(line)

        # If poem seems to have ended (found author after several lines of verse)
        if found_author and i > best_start + 2:
            # Check if next non-blank line looks like prose or new content
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                next_line = lines[j].strip()
                # If next content looks like prose (long line) or new title
                if len(next_line) > 100 or next_line.isupper():
                    break

    # Clean up trailing blanks
    while poem_lines and not poem_lines[-1].strip():
        poem_lines.pop()

    result = '\n'.join(poem_lines)
    return result if result.strip() else page_text.strip()


def get_next_sequence(month_dir, month_name, vol):
    """Get the next available sequence number for a month directory."""
    if not os.path.exists(month_dir):
        return 30  # Start at a high number to avoid conflicts

    existing = os.listdir(month_dir)
    max_seq = 0
    pattern = re.compile(rf'^{month_name}_Vol{vol}_(\d+)_')
    for f in existing:
        m = pattern.match(f)
        if m:
            seq = int(m.group(1))
            if seq > max_seq:
                max_seq = seq

    return max_seq + 1


def sanitize_filename(s):
    """Clean a string for use in filenames."""
    # Remove quotes, special chars
    s = re.sub(r'["\'\(\)\[\]{}!?,;:.]', '', s)
    s = s.strip()
    # Replace spaces with underscores
    s = re.sub(r'\s+', '_', s)
    # Remove leading/trailing underscores
    s = s.strip('_')
    return s


def parse_title_and_author_from_raw(raw):
    """Parse the raw 'NOT FOUND IN MISC' entry into title and author.

    These entries come from the CONTENTS file poetry section.
    Format is generally: Title Author / or Title / continuation Author
    The / indicates line breaks in the original TOC.

    We need to look at the CONTENTS file to understand the actual structure.
    """
    # Remove / markers
    clean = raw.replace(' / ', ' ').replace('/ ', ' ').replace(' /', ' ').strip()
    clean = re.sub(r'\s+', ' ', clean)
    return clean


def guess_title_author(clean_entry, contents_poems):
    """Try to split a clean entry into title and author by matching against CONTENTS."""
    for cp in contents_poems:
        cp_clean = cp['line'].strip()
        # The CONTENTS line is: Title Author PageNum
        # or Title Author (with no page)
        # Try to find the entry text in the contents line
        entry_lower = clean_entry.lower()
        line_lower = cp_clean.lower()

        if not any(w in line_lower for w in entry_lower.split() if len(w) > 2):
            continue

        # Found a match - the CONTENTS line has the full info
        # Try to split at known author patterns
        return cp_clean, cp.get('page_num')

    return clean_entry, None


def split_title_author_heuristic(text):
    """Heuristically split 'Title Author' text into (title, author).
    Assumes author is at the end and is a proper name (capitalized words)."""
    words = text.split()
    if len(words) <= 1:
        return text, "Unknown"

    # Try to find where the author starts
    # Authors are typically 2-4 capitalized words at the end
    # Look for patterns of 2-4 consecutive capitalized words at end

    # Also check for known patterns
    # "Title Word Word AuthorFirst AuthorLast"

    # Simple heuristic: last 2-4 words that are all capitalized
    best_split = len(words)
    for i in range(max(1, len(words) - 5), len(words)):
        remaining = words[i:]
        # Check if these look like author names (capitalized, not common title words)
        common_title_words = {'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for',
                            'and', 'but', 'or', 'is', 'are', 'was', 'my', 'this',
                            'that', 'its', 'with', 'from', 'by', 'be', 'we', 'i',
                            'must', 'shall', 'will', 'can', 'let', 'thy', 'no',
                            'not', 'do', 'all', 'so', 'up'}
        if all(w[0].isupper() and w.lower() not in common_title_words for w in remaining):
            if len(remaining) >= 2 and len(remaining) <= 5:
                best_split = i
                break

    if best_split < len(words):
        title = ' '.join(words[:best_split])
        author = ' '.join(words[best_split:])
    else:
        # Fallback: assume last 2 words are author if both capitalized
        if len(words) >= 3 and words[-1][0].isupper() and words[-2][0].isupper():
            title = ' '.join(words[:-2])
            author = ' '.join(words[-2:])
        else:
            title = text
            author = "Unknown"

    return title, author


def main():
    print("=" * 70)
    print("PDF POEM EXTRACTION - Volumes 30-40")
    print("=" * 70)

    # Load page map
    page_map = load_page_map()

    # Parse missing poems from report
    missing_poems = parse_missing_poems_from_report()
    print(f"\nFound {len(missing_poems)} missing poems in report")

    # Group by volume and month
    by_vol_month = defaultdict(list)
    for p in missing_poems:
        key = (p['vol'], p['month'])
        by_vol_month[key].append(p)

    # Track results
    results = []
    vol_counts = defaultdict(lambda: {'found': 0, 'not_found': 0, 'total': 0})

    # Open PDFs once per volume
    pdf_cache = {}

    for vol_num in range(30, 41):
        year = VOL_YEARS[vol_num]
        pdf_path = os.path.join(PDF_BASE, f"reliefsocietymagvol{vol_num}.pdf")

        if not os.path.exists(pdf_path):
            print(f"\n  WARNING: PDF not found: {pdf_path}")
            continue

        # Open PDF
        try:
            pdf_doc = fitz.open(pdf_path)
            pdf_cache[vol_num] = pdf_doc
        except Exception as e:
            print(f"\n  ERROR opening PDF vol{vol_num}: {e}")
            continue

        vol_dir = os.path.join(OUTPUT_BASE, f"Vol{vol_num}_{year}")

        for month in MONTHS_ORDER:
            key = (vol_num, month)
            if key not in by_vol_month:
                continue

            poems_to_find = by_vol_month[key]
            month_dir = os.path.join(vol_dir, month)

            # Load CONTENTS for this month
            contents_path = os.path.join(month_dir, f"{month}_Vol{vol_num}_00_CONTENTS.txt")
            contents_poems = parse_contents_poetry_section(contents_path)

            # Get month page range from page_map
            vol_str = str(vol_num)
            month_start = 0
            month_end = len(pdf_doc) - 1
            if vol_str in page_map and month in page_map[vol_str]:
                month_start = page_map[vol_str][month]['start']
                month_end_val = page_map[vol_str][month]['end']
                if month_end_val > month_start:
                    month_end = month_end_val
                else:
                    # Sometimes end < start in map, use start + 80
                    month_end = min(month_start + 80, len(pdf_doc) - 1)

            # Get next sequence number
            next_seq = get_next_sequence(month_dir, month, vol_num)

            print(f"\n  Vol{vol_num} {month} {year}: {len(poems_to_find)} poems to extract")

            for poem_info in poems_to_find:
                raw = poem_info['raw']
                vol_counts[vol_num]['total'] += 1

                # Parse title/author from raw
                clean = parse_title_and_author_from_raw(raw)

                # Try to find page number from CONTENTS
                page_num = find_poem_page_in_contents(contents_poems, raw)

                # Split into title and author
                title, author = split_title_author_heuristic(clean)

                # Get title words for searching
                title_words = [w for w in title.split() if len(w) > 1]
                author_parts = [w for w in author.split() if len(w) > 1]

                # Strategy 1: Use page number from CONTENTS
                found_page = None
                found_text = None

                if page_num is not None:
                    pdf_page = mag_page_to_pdf_page(vol_num, month, page_num, page_map)
                    if pdf_page is not None:
                        # Search a few pages around the target
                        for offset in [0, -1, 1, -2, 2]:
                            pg = pdf_page + offset
                            if 0 <= pg < len(pdf_doc):
                                text = pdf_doc[pg].get_text()
                                text_lower = text.lower()
                                matches = sum(1 for w in title_words if w.lower() in text_lower)
                                if matches >= max(1, len(title_words) - 1):
                                    found_page = pg
                                    found_text = text
                                    break

                # Strategy 2: Search the month's page range
                if found_page is None:
                    found_page, found_text = search_pdf_for_poem(
                        pdf_path, title_words, month_start, month_end
                    )

                # Strategy 3: Broader search if still not found
                if found_page is None:
                    # Try wider range
                    wide_start = max(0, month_start - 5)
                    wide_end = min(len(pdf_doc) - 1, month_end + 10)
                    found_page, found_text = search_pdf_for_poem(
                        pdf_path, title_words, wide_start, wide_end
                    )

                if found_page is not None and found_text is not None:
                    # Extract just the poem portion
                    poem_text = extract_poem_text(found_text, title_words, author_parts)

                    # Build filename
                    title_clean = sanitize_filename(title)[:40]
                    author_clean = sanitize_filename(author.split()[-1] if author != "Unknown" else "Unknown")[:20]

                    seq_str = f"{next_seq:02d}"
                    filename = f"{month}_Vol{vol_num}_{seq_str}_Poem_{title_clean}_{author_clean}.txt"

                    # Build header
                    header = f"[Vol{vol_num} | {month} {year} | Sequence: {seq_str} | Category: Poem | Title: {title}]"

                    # Write file
                    filepath = os.path.join(month_dir, filename)
                    os.makedirs(month_dir, exist_ok=True)

                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(header + '\n\n')
                        f.write(poem_text + '\n')

                    vol_counts[vol_num]['found'] += 1
                    next_seq += 1

                    results.append({
                        'status': 'EXTRACTED',
                        'vol': vol_num,
                        'month': month,
                        'year': year,
                        'title': title,
                        'author': author,
                        'page_num': page_num,
                        'pdf_page': found_page,
                        'filename': filename
                    })

                    print(f"    [EXTRACTED] {title} by {author} -> {filename}")
                else:
                    vol_counts[vol_num]['not_found'] += 1
                    results.append({
                        'status': 'NOT_FOUND',
                        'vol': vol_num,
                        'month': month,
                        'year': year,
                        'title': title,
                        'author': author,
                        'page_num': page_num,
                        'raw': raw
                    })
                    print(f"    [NOT FOUND] {title} by {author} (raw: {raw})")

        # Close PDF
        pdf_doc.close()

    # Write summary report
    print("\n" + "=" * 70)
    print("WRITING REPORT")
    print("=" * 70)

    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + '\n')
        f.write("PDF POEM EXTRACTION REPORT - Volumes 30-40\n")
        f.write("=" * 70 + '\n\n')

        total_extracted = sum(v['found'] for v in vol_counts.values())
        total_not_found = sum(v['not_found'] for v in vol_counts.values())
        total = sum(v['total'] for v in vol_counts.values())

        f.write(f"Total poems attempted: {total}\n")
        f.write(f"Total extracted: {total_extracted}\n")
        f.write(f"Total not found in PDF: {total_not_found}\n\n")

        f.write("-" * 70 + '\n')
        f.write("SUMMARY BY VOLUME\n")
        f.write("-" * 70 + '\n')
        for vol_num in range(30, 41):
            vc = vol_counts[vol_num]
            if vc['total'] > 0:
                f.write(f"  Vol{vol_num} ({VOL_YEARS[vol_num]}): {vc['found']}/{vc['total']} extracted, {vc['not_found']} not found\n")

        f.write("\n" + "-" * 70 + '\n')
        f.write("DETAILED RESULTS\n")
        f.write("-" * 70 + '\n\n')

        current_vol = None
        for r in results:
            if r['vol'] != current_vol:
                current_vol = r['vol']
                f.write(f"\n{'=' * 50}\n")
                f.write(f"Vol{current_vol} ({r['year']})\n")
                f.write(f"{'=' * 50}\n")

            if r['status'] == 'EXTRACTED':
                f.write(f"  [{r['month']}] EXTRACTED: {r['title']} by {r['author']}\n")
                f.write(f"    -> {r['filename']}\n")
                f.write(f"    Magazine page: {r.get('page_num', 'unknown')}, PDF page: {r['pdf_page']}\n")
            else:
                f.write(f"  [{r['month']}] NOT FOUND: {r['title']} by {r['author']}\n")
                f.write(f"    Raw: {r.get('raw', '')}\n")
                f.write(f"    Magazine page from TOC: {r.get('page_num', 'unknown')}\n")

        f.write("\n\n" + "=" * 70 + '\n')
        f.write("NOT FOUND LIST (for manual review)\n")
        f.write("=" * 70 + '\n')
        for r in results:
            if r['status'] == 'NOT_FOUND':
                f.write(f"  Vol{r['vol']} {r['month']}: {r['title']} by {r['author']}\n")

    print(f"\nReport written to: {OUTPUT_REPORT}")
    print(f"\nFINAL SUMMARY:")
    print(f"  Total extracted: {total_extracted}/{total}")
    print(f"  Not found: {total_not_found}/{total}")

    for vol_num in range(30, 41):
        vc = vol_counts[vol_num]
        if vc['total'] > 0:
            print(f"  Vol{vol_num}: {vc['found']}/{vc['total']} extracted")


if __name__ == '__main__':
    main()
