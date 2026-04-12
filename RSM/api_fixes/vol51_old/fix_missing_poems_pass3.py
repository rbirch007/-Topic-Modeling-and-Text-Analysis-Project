#!/usr/bin/env python3
"""
Third pass: aggressive search for remaining missing poems.
Uses partial word matching, OCR-aware fuzzy matching, and full PDF scan.
"""

import os
import re
import json
import fitz
from collections import defaultdict

BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles"
OUTPUT_BASE = os.path.join(BASE, "OutputExtractedarticles")
PDF_BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"
PAGE_MAP_FILE = os.path.join(BASE, "vol30_40_pdf_page_map.json")
OUTPUT_REPORT = os.path.join(BASE, "pdf_poem_extraction_report_vol30_40.txt")

VOL_YEARS = {
    30: 1943, 31: 1944, 32: 1945, 33: 1946, 34: 1947,
    35: 1948, 36: 1949, 37: 1950, 38: 1951, 39: 1952, 40: 1953
}

# Remaining NOT FOUND poems from pass 2
STILL_MISSING = [
    (31, "July", "I-30", "Dott J. Sartori"),
    (31, "November", "In November", "Beatrice Knowlton Ekman"),
    (32, "May", "Parallel", "Alice Morrey Bailey"),
    (32, "May", "Fulfillment", "Mabel Jones Gabbott"),
    (32, "July", "Missing", "Mable Jones Gabbott"),
    (32, "October", "Tawdry Jade", "Courtney E. Cottam"),
    (32, "October", "Orphaned", "Dott J. Sartori"),
    (33, "March", "Renewal", "Alice Morrey Bailey"),
    (33, "August", "Self-Confidence", "Katherine Fernelius Larsen"),
    (33, "October", "Yesterdays", "Beatrice K. Ekman"),
    (33, "December", "Christmas-Eve Magic", "Mary J. Morris"),
    (33, "December", "Dorothy J. Roberts poem", "Dorothy J. Roberts"),
    (34, "July", "Sego Lily", "Eva Willes Wangsgaard"),
    (35, "June", "Stay-at-Home", "Lael Hill"),
    (35, "September", "Antics", "Mary Pack Triplett"),
    (35, "October", "Epilogue", "Beatrice K. Ekman"),
    (36, "July", "Patriot Day", "Josephine J. Harvey"),
    (36, "August", "Remembrance", "Ora Lee Parthesius"),
    (36, "December", "Snowflakes", "Elise Maness"),
    (37, "April", "Departure", "C. Cameron Johns"),
    (37, "June", "Floral Offering", "Eva Willes Wangsgaard"),
    (37, "August", "Promontory", "Maude O. Cook"),
    (37, "August", "Chalk Cliff", "Ruth Harwood"),
    (37, "August", "Incarnation", "Marian Schroder Crothers"),
    (37, "August", "Complacent", "Beulah Huish Sadleir"),
    (37, "August", "Lovelier", "Margaret B. Shomaker"),
    (37, "December", "Communications", "Edwin S. Flynn"),
    (38, "July", "Compensation", "Maude O. Cook"),
    (38, "July", "Gypsy Soul", "Grace B. Wilson"),
    (39, "March", "Inefficacy", "Hazel M. Thomson"),
    (39, "June", "Concerto", "Ing Smith"),
    (39, "August", "Dispossessed", "Blanche Sutherland"),
    (39, "November", "Valleyward", "Dorothy J. Roberts"),
    (39, "November", "Unexpressed", "Hilda V. Cameron"),
    (39, "December", "Emergence", "Margery S. Stewart"),
    (40, "May", "About Flowers", "Elaine Swain"),
    (40, "November", "Woman-Power", "Emily Carey Alleman"),
]


def load_page_map():
    with open(PAGE_MAP_FILE, 'r') as f:
        return json.load(f)


def sanitize_filename(s):
    s = re.sub(r'["\'\(\)\[\]{}!?,;:.]', '', s)
    s = s.strip()
    s = re.sub(r'\s+', '_', s)
    s = s.strip('_')
    return s


def get_next_sequence(month_dir, month_name, vol):
    if not os.path.exists(month_dir):
        return 50
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


def extract_poem_from_page(page_text, title, author):
    """Extract poem text from page."""
    lines = page_text.split('\n')
    lines_stripped = [l.strip() for l in lines]

    title_lower = title.lower().strip('"').strip("'").strip(',')

    # Find title line - try multiple strategies
    best_start = -1

    # Strategy 1: exact substring
    for i, line in enumerate(lines_stripped):
        if title_lower in line.lower():
            best_start = i
            break

    # Strategy 2: check author surname to anchor, then look backwards
    if best_start < 0:
        author_last = author.split()[-1].lower()
        for i, line in enumerate(lines_stripped):
            if author_last in line.lower() and len(line) < 80:
                # Look backwards for short lines (poem text)
                start = max(0, i - 30)
                for j in range(i, start, -1):
                    if not lines_stripped[j]:
                        # Found blank before author area, poem might start after previous blank
                        best_start = j + 1
                        break
                if best_start < 0:
                    best_start = max(0, i - 10)
                break

    if best_start < 0:
        return None

    # Extract lines
    poem_lines = []
    blank_count = 0
    for i in range(best_start, min(best_start + 50, len(lines_stripped))):
        line = lines_stripped[i]

        if not line:
            blank_count += 1
            if blank_count >= 3:
                break
            poem_lines.append('')
            continue
        else:
            blank_count = 0

        if re.match(r'^VOL\.\s*\d+', line, re.IGNORECASE):
            break
        if re.match(r'^\d{3,}$', line):
            break
        if i > best_start + 3 and len(line) > 120:
            break
        if i > best_start + 2 and line.isupper() and len(line) > 20:
            break

        poem_lines.append(line)

    while poem_lines and not poem_lines[-1].strip():
        poem_lines.pop()

    return '\n'.join(poem_lines) if poem_lines else None


def main():
    page_map = load_page_map()

    print("=" * 70)
    print("THIRD PASS: Aggressive search for remaining poems")
    print("=" * 70)

    extracted = 0
    not_found = 0
    results = []

    for vol, month, title, author in STILL_MISSING:
        year = VOL_YEARS[vol]
        pdf_path = os.path.join(PDF_BASE, f"reliefsocietymagvol{vol}.pdf")
        month_dir = os.path.join(OUTPUT_BASE, f"Vol{vol}_{year}", month)

        doc = fitz.open(pdf_path)

        vol_str = str(vol)
        month_start = 0
        month_end = len(doc) - 1
        if vol_str in page_map and month in page_map[vol_str]:
            month_start = page_map[vol_str][month]['start']
            month_end_val = page_map[vol_str][month]['end']
            if month_end_val > month_start:
                month_end = min(month_end_val, len(doc) - 1)
            else:
                month_end = min(month_start + 80, len(doc) - 1)

        # Clean title for search
        clean_title = title.strip('"').strip("'").strip(',').lower()
        # Also try variants
        title_variants = [
            clean_title,
            clean_title.replace('-', ''),
            clean_title.replace('-', ' '),
            clean_title.replace(' ', ''),
        ]

        # Author surname
        author_last = author.split()[-1].lower()

        found_page = None
        found_text = None

        # Search each page
        for pg in range(month_start, min(month_end + 1, len(doc))):
            text = doc[pg].get_text()
            text_lower = text.lower()

            # Check for any title variant
            for tv in title_variants:
                if tv in text_lower and author_last in text_lower:
                    found_page = pg
                    found_text = text
                    break

            if found_page is not None:
                break

            # For the "Dorothy J. Roberts poem" entry - search by author only
            # since this might be a poem where only the author was listed in TOC
            if "poem" in title.lower() and author_last in text_lower:
                # Check if this page has short lines (poem-like)
                page_lines = text.split('\n')
                short_lines = sum(1 for l in page_lines if 5 < len(l.strip()) < 60)
                if short_lines > 3:
                    found_page = pg
                    found_text = text
                    break

        # If still not found, try just author in the month range
        if found_page is None:
            for pg in range(month_start, min(month_end + 1, len(doc))):
                text = doc[pg].get_text()
                text_lower = text.lower()

                # Check for author name presence and at least first word of title
                title_first = clean_title.split()[0] if clean_title.split() else ''
                if title_first and len(title_first) > 2:
                    if title_first in text_lower and author_last in text_lower:
                        found_page = pg
                        found_text = text
                        break

        if found_page is not None and found_text is not None:
            poem_text = extract_poem_from_page(found_text, title, author)
            if poem_text is None:
                # Use raw text limited to a section
                lines = found_text.split('\n')
                # Find author line
                for i, l in enumerate(lines):
                    if author_last.lower() in l.lower():
                        start = max(0, i - 15)
                        end = min(len(lines), i + 5)
                        poem_text = '\n'.join(lines[start:end])
                        break
                if poem_text is None:
                    poem_text = found_text.strip()[:1500]

            next_seq = get_next_sequence(month_dir, month, vol)
            seq_str = f"{next_seq:02d}"

            # Handle the special "Dorothy J. Roberts poem" case
            display_title = title.replace(" poem", "") if "poem" in title else title
            title_clean = sanitize_filename(display_title)[:40]
            author_clean = sanitize_filename(author.split()[-1])[:20]

            filename = f"{month}_Vol{vol}_{seq_str}_Poem_{title_clean}_{author_clean}.txt"
            header = f"[Vol{vol} | {month} {year} | Sequence: {seq_str} | Category: Poem | Title: {display_title}]"

            filepath = os.path.join(month_dir, filename)
            os.makedirs(month_dir, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(header + '\n\n')
                f.write(poem_text + '\n')

            extracted += 1
            results.append(('EXTRACTED', vol, month, title, author, filename, found_page))
            print(f"  [EXTRACTED] Vol{vol} {month}: {title} by {author} (pg {found_page}) -> {filename}")
        else:
            not_found += 1
            results.append(('NOT_FOUND', vol, month, title, author, '', None))
            print(f"  [NOT FOUND] Vol{vol} {month}: {title} by {author}")

        doc.close()

    print(f"\n{'=' * 70}")
    print(f"THIRD PASS RESULTS:")
    print(f"  Extracted: {extracted}")
    print(f"  Not found: {not_found}")

    # Append to report
    with open(OUTPUT_REPORT, 'a', encoding='utf-8') as f:
        f.write("\n\n" + "=" * 70 + '\n')
        f.write("THIRD PASS RESULTS\n")
        f.write("=" * 70 + '\n')
        f.write(f"Extracted: {extracted}\n")
        f.write(f"Not found: {not_found}\n\n")

        for status, vol, month, title, author, filename, pg in results:
            if status == 'EXTRACTED':
                f.write(f"  [{month}] EXTRACTED: {title} by {author} (PDF pg {pg}) -> {filename}\n")
            else:
                f.write(f"  [{month}] NOT FOUND: {title} by {author}\n")


if __name__ == '__main__':
    main()
