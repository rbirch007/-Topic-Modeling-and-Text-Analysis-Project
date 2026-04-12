"""
Phase 6: The CONTENTS at PDF page 533 may NOT be August.
The Vol37 PDF has 912 pages = 12 months x ~76 pages.
Let me find all CONTENTS pages to identify which month is which,
and find the actual August CONTENTS that lists Promontory and Chalk Cliff.
Also check if there are other occurrences of Maude O. Cook in Vol37.
"""
import fitz, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
PDF_DIR = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine'

doc = fitz.open(f'{PDF_DIR}/reliefsocietymagvol37.pdf')

# Find all CONTENTS pages
print("=== Finding all CONTENTS/Vol references in Vol37 PDF ===")
for pp in range(len(doc)):
    text = doc[pp].get_text()
    # Look for "VOL. 37" or "VOL  37" or similar
    if 'vol.' in text.lower() and ('37' in text) and ('no.' in text.lower() or 'contents' in text.lower()):
        lines = text.split('\n')
        for line in lines[:5]:
            if 'vol' in line.lower() or 'no.' in line.lower():
                print(f"  PDF {pp}: {line.strip()}")
                break

# Search for Promontory across ALL pages
print("\n=== Searching ALL pages for 'promontory' ===")
for pp in range(len(doc)):
    text = doc[pp].get_text()
    if 'promontory' in text.lower():
        print(f"  Found 'promontory' on PDF page {pp}")
        # Print surrounding context
        lines = text.split('\n')
        for i, line in enumerate(lines):
            if 'promontory' in line.lower():
                for j in range(max(0,i-2), min(len(lines),i+5)):
                    print(f"    {lines[j]}")
                print()

# Search for Chalk Cliff
print("\n=== Searching ALL pages for 'chalk' ===")
for pp in range(len(doc)):
    text = doc[pp].get_text()
    if 'chalk' in text.lower():
        print(f"  Found 'chalk' on PDF page {pp}")
        lines = text.split('\n')
        for i, line in enumerate(lines):
            if 'chalk' in line.lower():
                for j in range(max(0,i-2), min(len(lines),i+5)):
                    print(f"    {lines[j]}")
                print()

doc.close()

# Also do same for Vol40 - search for "Woman-Power" specifically
print("\n=== Vol40: Searching for 'woman-power' or 'womanpower' ===")
doc = fitz.open(f'{PDF_DIR}/reliefsocietymagvol40.pdf')
for pp in range(len(doc)):
    text = doc[pp].get_text()
    if 'woman-power' in text.lower() or 'womanpower' in text.lower():
        print(f"  Found on PDF page {pp}")
        lines = text.split('\n')
        for i, line in enumerate(lines):
            if 'woman' in line.lower() and 'power' in line.lower():
                for j in range(max(0,i-2), min(len(lines),i+5)):
                    print(f"    {lines[j]}")
                print()
doc.close()
