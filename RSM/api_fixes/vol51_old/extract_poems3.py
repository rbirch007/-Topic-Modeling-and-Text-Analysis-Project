"""
Phase 3: Get full text from the specific PDF pages we need.
Vol31 Trek: found on PDF page 384. Get full text + next page.
Vol37: Need to find offset. Search for known text like "Navajo Weaver" (pg 505).
Vol40: Need to find offset. Search for known text like "Autumn Lens" (pg 713).
"""

import fitz

PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"

# VOL 31 - Get full text of Trek poem from PDF page 384 and 385
print("=" * 60)
print("VOL 31 - TREK - PDF page 384-385")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol31.pdf")
for pp in [384, 385]:
    print(f"\n--- PDF page {pp} ---")
    print(doc[pp].get_text())
doc.close()

# VOL 37 - Find page offset
print("\n" + "=" * 60)
print("VOL 37 - Finding offset")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol37.pdf")

# Search for "Navajo Weaver" which should be on mag page 505 (frontispiece for August)
for pp in range(500, 600):
    text = doc[pp].get_text()
    if "navajo" in text.lower():
        print(f"  'Navajo Weaver' found on PDF page {pp} (mag page 505)")
        print(f"  Offset = {pp} - 505 = {pp - 505}")
        offset37 = pp - 505
        break

# Also search for "Lamanites" which is on mag page 507
for pp in range(500, 600):
    text = doc[pp].get_text()
    if "lamanites" in text.lower() and "ivins" in text.lower():
        print(f"  'Lamanites/Ivins' found on PDF page {pp} (mag page 507)")
        break

# With the offset, calculate where pages 519 and 521 should be
print(f"\n  Promontory (mag 519) should be at PDF page {519 + offset37}")
print(f"  Chalk Cliff (mag 521) should be at PDF page {521 + offset37}")

# Print those pages
for target_mag, name in [(519, "Promontory"), (521, "Chalk Cliff")]:
    target_pdf = target_mag + offset37
    print(f"\n--- {name} - PDF page {target_pdf} (mag {target_mag}) ---")
    for pp in range(target_pdf - 1, target_pdf + 2):
        if 0 <= pp < len(doc):
            print(f"\n  [PDF page {pp}]")
            print(doc[pp].get_text()[:3000])
doc.close()

# VOL 40 - Find page offset
print("\n" + "=" * 60)
print("VOL 40 - Finding offset")
doc = fitz.open(f"{PDF_DIR}/reliefsocietymagvol40.pdf")

# CONTENTS was on PDF page 756, showing items starting at mag page 713-714
# So offset = 756 - 713 = 43 approximately
# But let's verify by searching for a known article
# "Autumn Lens" frontispiece should be mag page 713
for pp in range(750, 770):
    text = doc[pp].get_text()
    if "autumn" in text.lower() and "lens" in text.lower():
        print(f"  'Autumn Lens' found on PDF page {pp} (mag page 713)")
        print(f"  Offset = {pp} - 713 = {pp - 713}")
        offset40 = pp - 713
        break
else:
    # Try the CONTENTS approach
    offset40 = 756 - 713  # = 43
    print(f"  Using CONTENTS-based offset: {offset40}")

# Woman-Power should be on mag page 782
target_pdf = 782 + offset40
print(f"\n  Woman-Power (mag 782) should be at PDF page {target_pdf}")

# Also check: the CONTENTS listed "Walking Hand in Hand" by Alleman at mag page 782
# and "Woman-Power" at mag page 782 too. These might be the same poem or on the same page.
# Let's look at pages around that area
for pp in range(target_pdf - 3, target_pdf + 3):
    if 0 <= pp < len(doc):
        print(f"\n--- PDF page {pp} ---")
        text = doc[pp].get_text()
        print(text[:3000])
doc.close()
