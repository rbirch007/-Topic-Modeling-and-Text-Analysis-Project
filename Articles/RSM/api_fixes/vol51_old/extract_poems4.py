"""
Phase 4: Extract remaining poems. Handle encoding issues.
Known: Vol31 Trek on PDF page 384 - CLEAN TEXT AVAILABLE
Vol37 offset = 28: Promontory -> PDF 547, Chalk Cliff -> PDF 549
Vol40 offset: CONTENTS on PDF 756 for mag 713, so offset = 43: Woman-Power -> PDF 825
"""

import fitz
import sys
import io

# Force UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"

# VOL 37 - Promontory and Chalk Cliff
print("=" * 60)
print("VOL 37 - Promontory (PDF ~547) and Chalk Cliff (PDF ~549)")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol37.pdf")

# The offset from "Navajo Weaver" on page 533 for mag 505 gives offset=28
# But Navajo Weaver was the August frontispiece, so let me verify more carefully
# by looking at nearby content. Lamanites was also on 533 but should be mag 507.
# So the CONTENTS+frontispiece are on the same PDF page. Let's check page 534+
for pp in range(533, 560):
    text = doc[pp].get_text()
    # Look for page numbers in text to calibrate
    if "page" in text.lower() and any(str(n) in text for n in range(505, 530)):
        pass  # too noisy
    # Print first 200 chars to see what's on each page
    clean = text[:200].replace('\n', ' | ')
    print(f"  PDF {pp}: {clean}")

print("\n--- Full text of promising pages ---")
for pp in [546, 547, 548, 549, 550, 551]:
    text = doc[pp].get_text()
    print(f"\n=== PDF page {pp} ===")
    print(text[:4000])

doc.close()

# VOL 40 - Woman-Power
print("\n" + "=" * 60)
print("VOL 40 - Woman-Power")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol40.pdf")

# Verify offset: search for "Sixty Years Ago" (mag page 742) or "From Near and Far" (mag 714)
for pp in range(755, 770):
    text = doc[pp].get_text()
    if "sixty years ago" in text.lower():
        print(f"  'Sixty Years Ago' on PDF {pp} (mag 742), offset = {pp - 742}")
        break
    if "from near and far" in text.lower():
        print(f"  'From Near and Far' on PDF {pp} (mag 714), offset = {pp - 714}")
        break

# Try offset 43: page 782 + 43 = 825
# But also try offset from CONTENTS page 756
# Actually, the CONTENTS itself is typically on the page right after the frontispiece
# Frontispiece = mag 713, CONTENTS = mag 714 or thereabouts
# If frontispiece is PDF 756, then offset is 756 - 713 = 43
# So mag 782 -> PDF 825

for pp in range(820, 835):
    text = doc[pp].get_text()
    clean = text[:200].replace('\n', ' | ')
    print(f"  PDF {pp}: {clean}")

print("\n--- Full text of pages around 825 ---")
for pp in range(823, 830):
    if pp < len(doc):
        text = doc[pp].get_text()
        print(f"\n=== PDF page {pp} ===")
        print(text[:4000])

doc.close()
