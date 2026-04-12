"""
Phase 2: Extract specific pages from PDFs for the 4 poems.
Now that we know:
- Vol31 "Trek" (listed as I-30 in CONTENTS) by Sartori is on magazine page 359
  - The CONTENTS page was on PDF page 377, and lists this on mag page 359
  - Offset from CONTENTS: mag 353 = first content page. With offset ~18, try PDF page 359+18=377
  - Actually the offset found was -18, meaning pdf_page = mag_page + 18 roughly
  - Let's search around PDF pages 359+18 = 377 area
- Vol37 "Promontory" by Cook on mag page 519
- Vol37 "Chalk Cliff" by Harwood on mag page 521
- Vol40 "Walking Hand in Hand" by Alleman on mag page 782
  - CONTENTS was on PDF page 756, listing mag page 713+ items
"""

import fitz
import re

PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"

# Vol31: Search for "Trek" or "Sartori" on pages near where page 359 would be
print("=" * 60)
print("VOL 31 - Trek by Dott J. Sartori (mag page 359)")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol31.pdf")
# From the CONTENTS on PDF page 377, we can see mag page 354 = "So Let It Ring"
# So the offset is roughly mag_page + 18 = pdf_page, meaning page 359 -> PDF 377
# But 377 was the CONTENTS page itself. Let's search more carefully.
for pp in range(370, 410):
    page = doc[pp]
    text = page.get_text()
    if "trek" in text.lower() or "sartori" in text.lower() or "ponder" in text.lower():
        print(f"  Found on PDF page {pp}")
        print(text[:2000])
        print("---")
doc.close()

# Vol37: Search for Promontory, Chalk Cliff
print("\n" + "=" * 60)
print("VOL 37 - Promontory by Cook (mag 519) and Chalk Cliff by Harwood (mag 521)")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol37.pdf")
# Let's find the page offset by searching for known content
# CONTENTS listed "Navajo Weaver" frontispiece on page 505
# Let's search for "Promontory" and "Chalk Cliff" and "Cook" and "Harwood"
for pp in range(500, 600):
    page = doc[pp]
    text = page.get_text()
    if any(t in text.lower() for t in ["promontory", "chalk cliff", "chalk  cliff"]):
        print(f"  Found on PDF page {pp}")
        print(text[:2000])
        print("---")
# Broader search if not found
print("  Searching author names broadly...")
for pp in range(0, len(doc)):
    page = doc[pp]
    text = page.get_text()
    if "promontory" in text.lower():
        print(f"  'Promontory' on PDF page {pp}")
        print(text[:1500])
        print("---")
        break
for pp in range(0, len(doc)):
    page = doc[pp]
    text = page.get_text()
    if "chalk" in text.lower() and "cliff" in text.lower():
        print(f"  'Chalk Cliff' on PDF page {pp}")
        print(text[:1500])
        print("---")
        break
# If still not found, search for Ruth Harwood
for pp in range(0, len(doc)):
    page = doc[pp]
    text = page.get_text()
    if "harwood" in text.lower():
        print(f"  'Harwood' on PDF page {pp}")
        print(text[:500])
        print("---")
doc.close()

# Vol40: Search for Woman-Power and Walking Hand in Hand
print("\n" + "=" * 60)
print("VOL 40 - Woman-Power by Alleman (mag 782)")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol40.pdf")
# CONTENTS was on PDF page 756, showing mag page 713
# So offset is roughly 756 - 713 = 43, meaning page 782 -> PDF page 782+43 = 825?
# But that seems high. Let's check: CONTENTS listed first item at mag page 713
# If CONTENTS is at PDF page 756, and it's typically the first or second page of the issue...
# Actually let's just search
for pp in range(780, 850):
    if pp < len(doc):
        page = doc[pp]
        text = page.get_text()
        if any(t in text.lower() for t in ["woman-power", "woman power", "alleman", "walking hand"]):
            print(f"  Found on PDF page {pp}")
            print(text[:2000])
            print("---")

# Broader search
print("  Broader search for 'woman-power' or 'alleman'...")
for pp in range(700, len(doc)):
    page = doc[pp]
    text = page.get_text()
    if "woman-power" in text.lower() or "woman power" in text.lower():
        print(f"  'Woman-Power' on PDF page {pp}")
        print(text[:2000])
        print("---")
        break
for pp in range(700, len(doc)):
    page = doc[pp]
    text = page.get_text()
    if "walking hand in hand" in text.lower():
        print(f"  'Walking Hand in Hand' on PDF page {pp}")
        print(text[:2000])
        print("---")
        break
doc.close()
