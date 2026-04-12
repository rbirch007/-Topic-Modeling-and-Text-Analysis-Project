#!/usr/bin/env python3
"""
Second pass: fix the 54 poems NOT FOUND in the first extraction pass.
Issues to fix:
1. Some single-word titles need broader search
2. Some entries are ads/cover descriptions, not poems - skip them
3. Some quoted titles with commas need better parsing
4. Multi-poem concatenated entries need splitting
"""

import os
import re
import json
import fitz
from pathlib import Path
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

# NOT FOUND items from first pass - manually parsed with correct title/author
# Format: (vol, month, title, author, raw)
# Skip items that are clearly NOT poems (ads, cover descriptions)
NOT_FOUND = [
    # Vol31
    (31, "July", "I-30", "Dott J. Sartori", "I-30 Dott J. Sartori /"),
    (31, "November", "In November", "Beatrice Knowlton Ekman", "In / November Beatrice Knowlton Ekman"),
    # Vol32
    (32, "May", "Parallel", "Alice Morrey Bailey", "Parallel / Alice Morrey Bailey"),
    (32, "May", "Fulfillment", "Mabel Jones Gabbott", "Fulfillment / Mabel Jones Gabbott"),
    (32, "July", "Missing", "Mable Jones Gabbott", "Missing / Mable Jones Gabbott"),
    (32, "October", "Tawdry Jade", "Courtney E. Cottam", "Tawdry Jade Courtney E. Cottam /"),
    (32, "October", "Orphaned", "Dott J. Sartori", "Orphaned Dott J. Sartori /"),
    # Vol33
    (33, "March", "Renewal", "Alice Morrey Bailey", "Renewal / Alice Morrey Bailey"),
    (33, "August", "Self-Confidence", "Katherine Fernelius Larsen", "Self-Confidence / Katherine Fernelius Larsen"),
    (33, "October", "Yesterdays", "Beatrice K. Ekman", "Yesterdays Beatrice K. Ekman /"),
    (33, "December", "Christmas-Eve Magic", "Mary J. Morris", "Christmas-Eve Magic Mary J. Morris /"),
    (33, "December", "Dorothy J. Roberts poem", "Dorothy J. Roberts", "Dorothy J. Roberts /"),  # Author listed as standalone
    # Vol34
    (34, "July", "Sego Lily", "Eva Willes Wangsgaard", "Sego / Lily Eva Willes Wangsgaard"),
    # Vol35
    (35, "June", "Stay-at-Home", "Lael Hill", "Stay-at-Home / Lael Hill"),
    (35, "September", "Antics", "Mary Pack Triplett", "Antics / Mary Pack Triplett"),
    (35, "October", "Epilogue", "Beatrice K. Ekman", "Epilogue Beatrice K. Ekman /"),
    # Skip: 40th ANNIVERSARY, ZIONS COOPERATIVE, ZCMI - these are ads
    # Skip: MADONNA GRANDUCA - this is a cover/frontispiece description
    (35, "November", "ZCMI Gateway", "SKIP", "SKIP"),  # ad, skip
    # Vol36
    (36, "July", "Embryo", "C. Cameron Johns", '"Embryo," / C. Cameron Johns,'),
    (36, "July", "Patriot Day", "Josephine J. Harvey", '"Patriot Day," / Josephine J. Harvey,'),
    (36, "August", "Remembrance", "Ora Lee Parthesius", "Remembrance / Ora Lee Parthesius"),
    (36, "December", "Blizzard", "Evelyn Fjeldsted", '"Blizzard," / Evelyn Fjeldsted,'),
    (36, "December", "Holy Night", "Christie Lund Coles", '"Holy Night," / Christie Lund Coles,'),
    (36, "December", "Snowflakes", "Elise Maness", '"Snowflakes," / Elise Maness,'),
    (36, "December", "Remnant", "Marvin Jones", '"Remnant," / Marvin Jones,'),
    # Vol37
    (37, "April", "Departure", "C. Cameron Johns", "Departure, / C. Cameron Johns,"),
    (37, "April", "Announcement", "Hilda V. Cameron", "Announcement, / Hilda V. Cameron,"),
    (37, "June", "Floral Offering", "Eva Willes Wangsgaard", "Floral Offering / Eva Willes Wangsgaard"),
    (37, "August", "Promontory", "Maude O. Cook", "Promontory / Maude O. Cook"),
    (37, "August", "Chalk Cliff", "Ruth Harwood", "Chalk Cliff / Ruth Harwood"),
    (37, "August", "Incarnation", "Marian Schroder Crothers", "Incarnation / Marian Schroder Crothers"),
    (37, "August", "Complacent", "Beulah Huish Sadleir", "Complacent / Beulah Huish Sadleir"),
    (37, "August", "Lovelier", "Margaret B. Shomaker", "Lovelier / Margaret B. Shomaker"),
    (37, "December", "Communications", "Edwin S. Flynn", '"Communications," / Edwin S. Flynn,'),
    # Vol38
    (38, "January", "Progress", "Agnes Just Reid", '"Progress," / Agnes Just Reid,'),
    (38, "April", "Living Water", "Alberta H. Christensen", '"Living Water" / Alberta H. Christensen'),
    (38, "July", "Compensation", "Maude O. Cook", '"Compensation," / Maude O. Cook,'),
    (38, "July", "Gypsy Soul", "Grace B. Wilson", '"Gypsy Soul," / Grace B. Wilson,'),
    (38, "July", "First Steps", "Bernice T. Clayton", '"First Steps," / Bernice T. Clayton,'),
    (38, "July", "Desert Scene", "Bertha Cragun", '"Desert Scene," / Bertha Cragun,'),
    (38, "July", "Prayer", "Virginia L. Morris", '"Prayer," / Virginia L. Morris,'),
    (38, "November", "November Wind", "Zera Wilde Earl", '"November Wind," / Zera Wilde Earl,'),
    # Vol39
    (39, "March", "Sound", "Gene Romolo", "Sound, / Gene Romolo,"),
    (39, "March", "Inefficacy", "Hazel M. Thomson", "Inefficacy, / Hazel M. Thomson,"),
    (39, "June", "Concerto", "Ing Smith", "Concerto / Ing Smith"),
    (39, "August", "Dispossessed", "Blanche Sutherland", "Dispossessed / Blanche Sutherland"),
    (39, "November", "Valleyward", "Dorothy J. Roberts", "Valleyward / Dorothy J. Roberts"),
    (39, "November", "Unexpressed", "Hilda V. Cameron", "Unexpressed / Hilda V. Cameron"),
    (39, "December", "Emergence", "Margery S. Stewart", "Emergence / Margery S. Stewart"),
    # Vol40
    # Skip: THE COVER description - not a poem
    (40, "May", "About Flowers", "Elaine Swain", "About Flowers / Elaine Swain"),
    (40, "November", "Woman-Power", "Emily Carey Alleman", "Woman-Power / Emily Carey Alleman"),
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
        return 40
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
    """Extract poem text from page, searching for title."""
    lines = page_text.split('\n')
    lines = [l.strip() for l in lines]

    title_words = [w for w in title.split() if len(w) > 1]
    title_lower = title.lower()

    # Find the title line
    best_start = -1
    best_score = 0

    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        if not line_lower:
            continue

        # Exact match
        if title_lower in line_lower:
            best_start = i
            best_score = 100
            break

        # Word match
        matches = sum(1 for w in title_words if w.lower() in line_lower)
        if matches > best_score:
            best_score = matches
            best_start = i

    if best_start < 0:
        return None

    # Extract poem lines
    poem_lines = []
    blank_count = 0

    for i in range(best_start, min(best_start + 60, len(lines))):
        line = lines[i].strip()

        if not line:
            blank_count += 1
            if blank_count >= 3:
                break
            poem_lines.append('')
            continue
        else:
            blank_count = 0

        # Stop conditions
        if i > best_start + 2 and line.isupper() and len(line) > 15:
            if not any(w.lower() in line.lower() for w in title_words):
                break
        if re.match(r'^VOL\.\s*\d+', line, re.IGNORECASE):
            break
        if re.match(r'^\d{3,}$', line):
            break
        if i > best_start + 3 and len(line) > 120:
            break

        poem_lines.append(line)

        # Check for author to know we're near end
        if author and i > best_start + 1:
            author_last = author.split()[-1].lower()
            if author_last in line.lower() and len(author_last) > 2:
                # Check if next non-blank looks like new content
                j = i + 1
                while j < len(lines) and not lines[j].strip():
                    j += 1
                if j < len(lines):
                    next_l = lines[j].strip()
                    if len(next_l) > 80 or (next_l.isupper() and len(next_l) > 10):
                        break

    while poem_lines and not poem_lines[-1].strip():
        poem_lines.pop()

    return '\n'.join(poem_lines) if poem_lines else None

def main():
    page_map = load_page_map()

    print("=" * 70)
    print("SECOND PASS: Extracting remaining missing poems")
    print("=" * 70)

    extracted = 0
    not_found = 0
    skipped = 0
    results = []

    for entry in NOT_FOUND:
        vol, month, title, author, raw = entry

        if author == "SKIP":
            skipped += 1
            continue

        year = VOL_YEARS[vol]
        pdf_path = os.path.join(PDF_BASE, f"reliefsocietymagvol{vol}.pdf")
        month_dir = os.path.join(OUTPUT_BASE, f"Vol{vol}_{year}", month)

        if not os.path.exists(pdf_path):
            print(f"  PDF not found: {pdf_path}")
            not_found += 1
            continue

        # Get page range for this month
        vol_str = str(vol)
        month_start = 0
        month_end = 999

        doc = fitz.open(pdf_path)

        if vol_str in page_map and month in page_map[vol_str]:
            month_start = page_map[vol_str][month]['start']
            month_end_val = page_map[vol_str][month]['end']
            if month_end_val > month_start:
                month_end = min(month_end_val, len(doc) - 1)
            else:
                month_end = min(month_start + 80, len(doc) - 1)

        # Search for the poem
        title_words = [w for w in title.split() if len(w) > 1]
        # For short titles, also include author words in search
        search_words = title_words.copy()

        found_page = None
        found_text = None

        # Search page by page
        for pg in range(month_start, min(month_end + 1, len(doc))):
            text = doc[pg].get_text()
            text_lower = text.lower()

            # Check for title
            title_matches = sum(1 for w in title_words if w.lower() in text_lower)

            # For single-word titles, also require author name
            if len(title_words) <= 1:
                author_words = [w for w in author.split() if len(w) > 2]
                author_matches = sum(1 for w in author_words if w.lower() in text_lower)
                if title_matches >= len(title_words) and author_matches >= 1:
                    found_page = pg
                    found_text = text
                    break
            else:
                if title_matches >= max(1, len(title_words)):
                    found_page = pg
                    found_text = text
                    break

        # Second pass: more lenient
        if found_page is None:
            for pg in range(month_start, min(month_end + 1, len(doc))):
                text = doc[pg].get_text()
                text_lower = text.lower()

                # Try hyphenated variants
                title_nohyphen = title.replace('-', ' ').replace('-', '')
                if title_nohyphen.lower() in text_lower:
                    found_page = pg
                    found_text = text
                    break

                # Try without quotes
                title_noquote = title.replace('"', '').replace("'", '')
                if title_noquote.lower() in text_lower:
                    found_page = pg
                    found_text = text
                    break

        if found_page is not None and found_text is not None:
            # Extract poem
            poem_text = extract_poem_from_page(found_text, title, author)
            if poem_text is None:
                poem_text = found_text.strip()[:2000]  # fallback

            # Get sequence
            next_seq = get_next_sequence(month_dir, month, vol)
            seq_str = f"{next_seq:02d}"

            title_clean = sanitize_filename(title)[:40]
            author_last = author.split()[-1] if author else "Unknown"
            author_clean = sanitize_filename(author_last)[:20]

            filename = f"{month}_Vol{vol}_{seq_str}_Poem_{title_clean}_{author_clean}.txt"
            header = f"[Vol{vol} | {month} {year} | Sequence: {seq_str} | Category: Poem | Title: {title}]"

            filepath = os.path.join(month_dir, filename)
            os.makedirs(month_dir, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(header + '\n\n')
                f.write(poem_text + '\n')

            extracted += 1
            results.append(('EXTRACTED', vol, month, title, author, filename))
            print(f"  [EXTRACTED] Vol{vol} {month}: {title} by {author} -> {filename}")
        else:
            not_found += 1
            results.append(('NOT_FOUND', vol, month, title, author, ''))
            print(f"  [NOT FOUND] Vol{vol} {month}: {title} by {author}")

        doc.close()

    print(f"\n{'=' * 70}")
    print(f"SECOND PASS RESULTS:")
    print(f"  Extracted: {extracted}")
    print(f"  Not found: {not_found}")
    print(f"  Skipped (ads/non-poems): {skipped}")
    print(f"{'=' * 70}")

    # Append to report
    with open(OUTPUT_REPORT, 'a', encoding='utf-8') as f:
        f.write("\n\n" + "=" * 70 + '\n')
        f.write("SECOND PASS RESULTS\n")
        f.write("=" * 70 + '\n')
        f.write(f"Extracted: {extracted}\n")
        f.write(f"Not found: {not_found}\n")
        f.write(f"Skipped (ads/non-poems): {skipped}\n\n")

        for status, vol, month, title, author, filename in results:
            if status == 'EXTRACTED':
                f.write(f"  [{month}] EXTRACTED: {title} by {author} -> {filename}\n")
            else:
                f.write(f"  [{month}] NOT FOUND: {title} by {author}\n")

if __name__ == '__main__':
    main()
