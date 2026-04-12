"""
Phase 5: Search more specifically for Promontory and Chalk Cliff in Vol37.
The OCR may have mangled these titles. Also verify the Vol40 poem situation.
"""
import fitz, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
PDF_DIR = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine'

# VOL 37: The poem "My Thorn" by Maude O. Cook was found on PDF page 548.
# But CONTENTS says "Promontory" by Cook on page 519.
# These might be different poems. Let me look at the raw text on page 548 again more carefully,
# and also search broadly for any OCR variant of "Promontory"
doc = fitz.open(f'{PDF_DIR}/reliefsocietymagvol37.pdf')

print("=== Searching ALL pages of Vol37 for Cook/Promontory/Chalk ===")
for pp in range(530, 580):
    text = doc[pp].get_text().lower()
    if 'cook' in text or 'harwood' in text or 'chalk' in text or 'promontory' in text:
        full_text = doc[pp].get_text()
        print(f"\nPDF page {pp}:")
        # Print just the relevant portions
        lines = full_text.split('\n')
        for i, line in enumerate(lines):
            if any(kw in line.lower() for kw in ['cook', 'harwood', 'chalk', 'promontory', 'thorn', 'cliff']):
                start = max(0, i-3)
                end = min(len(lines), i+10)
                print(f"  Lines {start}-{end}:")
                for j in range(start, end):
                    print(f"    {lines[j]}")
                print()

# Also try: render the pages as images and use OCR? No, let's first check
# if the text is just badly OCR'd. Let me dump raw text of pages 548, 549, 550
print("\n=== Raw text dumps ===")
for pp in [548, 549, 550]:
    print(f"\n--- Full dump of PDF page {pp} ---")
    text = doc[pp].get_text()
    print(text)
    print("--- END ---")

doc.close()

# VOL 40: Check if "Woman-Power" is a separate piece from "Walking Hand in Hand"
print("\n\n=== VOL 40: Checking for Woman-Power vs Walking Hand in Hand ===")
doc = fitz.open(f'{PDF_DIR}/reliefsocietymagvol40.pdf')
for pp in range(756, 830):
    text = doc[pp].get_text().lower()
    if 'woman' in text and 'power' in text:
        full_text = doc[pp].get_text()
        print(f"\nPDF page {pp} contains 'woman' and 'power':")
        lines = full_text.split('\n')
        for i, line in enumerate(lines):
            if 'woman' in line.lower() and 'power' in line.lower():
                start = max(0, i-2)
                end = min(len(lines), i+5)
                for j in range(start, end):
                    print(f"  {lines[j]}")
doc.close()
