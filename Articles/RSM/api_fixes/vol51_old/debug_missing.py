#!/usr/bin/env python3
"""Debug remaining NOT FOUND poems - check OCR quality on target pages."""

import os
import json
import fitz

PDF_BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"
PAGE_MAP_FILE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\vol30_40_pdf_page_map.json"

with open(PAGE_MAP_FILE, 'r') as f:
    page_map = json.load(f)

# Check a few specific cases
cases = [
    (32, "May", "Parallel", "Bailey", 320, 393),
    (32, "October", "Tawdry", "Cottam", 720, 799),
    (33, "March", "Renewal", "Bailey", 160, 239),
    (31, "November", "November", "Ekman", 800, 879),
    (37, "August", "Promontory", "Cook", 560, 639),
    (39, "November", "Valleyward", "Roberts", 800, 879),
]

for vol, month, title_word, author_word, start, end in cases:
    pdf_path = os.path.join(PDF_BASE, f"reliefsocietymagvol{vol}.pdf")
    doc = fitz.open(pdf_path)
    print(f"\n{'='*60}")
    print(f"Vol{vol} {month}: Looking for '{title_word}' by '{author_word}'")
    print(f"  Scanning pages {start}-{min(end, len(doc)-1)}")

    found = False
    for pg in range(start, min(end + 1, len(doc))):
        text = doc[pg].get_text()
        tl = text.lower()
        tw = title_word.lower()
        aw = author_word.lower()

        if tw in tl:
            print(f"  FOUND '{title_word}' on page {pg}!")
            # Print context around it
            idx = tl.index(tw)
            snippet = text[max(0,idx-100):idx+200]
            print(f"  Context: ...{snippet}...")
            found = True
            break

    if not found:
        # Check if author is anywhere
        for pg in range(start, min(end + 1, len(doc))):
            text = doc[pg].get_text()
            if author_word.lower() in text.lower():
                print(f"  Author '{author_word}' found on page {pg}, but not title")
                lines = text.split('\n')
                for i, l in enumerate(lines):
                    if author_word.lower() in l.lower():
                        context_start = max(0, i-3)
                        context_end = min(len(lines), i+2)
                        for j in range(context_start, context_end):
                            print(f"    line {j}: {lines[j][:100]}")
                        break
                break
        else:
            print(f"  Neither title nor author found in range!")
            # Check if the PDF even has text on these pages
            sample_pg = start + 10
            if sample_pg < len(doc):
                sample = doc[sample_pg].get_text()
                print(f"  Sample text from page {sample_pg}: {sample[:200]}")

    doc.close()
