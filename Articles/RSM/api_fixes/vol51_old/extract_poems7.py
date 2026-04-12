"""
Phase 7: The poem titles Promontory, Chalk Cliff, and Woman-Power do NOT appear
in OCR text at all. The OCR is completely garbled for those pages.

Strategy:
1. Find the August CONTENTS page in Vol37 PDF (between pages ~530-610)
2. Identify which pages should have the poems
3. Render those PDF pages as images so we can read them visually
4. Or try extracting text with different methods

Let me first find the August CONTENTS page and the actual page numbers.
Then render the target pages as PNG images.
"""
import fitz, sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
PDF_DIR = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine'
OUTPUT = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\api_fixes\vol51_old'

# Vol37: July starts at ~460, Sept at ~612. August should be in between.
# Let me find the August CONTENTS page
doc = fitz.open(f'{PDF_DIR}/reliefsocietymagvol37.pdf')

# Look for AUGUST between pages 500-560
print("=== Vol37: Looking for August issue ===")
for pp in range(500, 560):
    text = doc[pp].get_text()
    if 'august' in text.lower() or 'AUGUST' in text:
        print(f"  PDF {pp}: contains 'august'")
        # Show first few lines
        for line in text.split('\n')[:5]:
            print(f"    {line}")
        break

# The CONTENTS I found earlier at PDF 533 was for August
# It listed My Thorn on 519 and Radiant Gift on 521
# But the USER's extracted CONTENTS file says Promontory on 519 and Chalk Cliff on 521
# The USER's CONTENTS may have been extracted differently (possibly manually corrected)
# OR the OCR text on the CONTENTS page may have been wrong and user has the right titles

# Either way, the poems ARE on magazine pages 519 and 521
# With offset 28 (from Navajo Weaver at mag 505 = PDF 533), that's PDF pages 547 and 549
# But we found OTHER content there (fiction story and article)

# Wait - let me reconsider the offset. The Navajo Weaver frontispiece is at mag page 505,
# and I found "navajo" on PDF page 533. But PDF 533 is the CONTENTS page, not the frontispiece!
# The frontispiece would be on the NEXT page. Let me check:
print("\n=== Checking frontispiece location ===")
for pp in [533, 534, 535]:
    text = doc[pp].get_text()
    print(f"\nPDF {pp} first 300 chars:")
    print(text[:300])

# PDF 534 says "VOL. 27, NO. 8 AUGUST 1950" and has "Navajo Weaver" poem text
# PDF 535 says "Photograph by Hal Rumel NAVAJO WEAVER" - this is the frontispiece image
# PDF 536 starts "The Lamanites" which is on mag page 507

# So: frontispiece (mag 505) = PDF 535, or actually the poem text before it = PDF 534
# Mag page 505 = PDF 534 (the text page with poem)
# Then mag 507 = PDF 536 (The Lamanites)
# Offset = 536 - 507 = 29
# Mag 519 = PDF 548, Mag 521 = PDF 550

# But PDF 548 is the end of "The Jumpher Family Camps" with "My Thorn" at the bottom
# And PDF 550 is "Organizing the Relief Society Magazine Campaign" with "Radiant Gift" at bottom

# These small poems are TUCKED at the bottom of pages. They are there!
# The titles from OCR ("My Thorn" and "Radiant Gift") may be OCR errors!
# "fliy Q/horn" -> "My Thorn" but could actually be "Promontory" garbled
# "LKadiant ^ift" -> "Radiant Gift" but could be "Chalk Cliff" garbled?

# Wait - "fliy Q/horn" could be garbled "My Thorn" which is a real poem title
# Let me render these pages as images to see the actual printed text

# Render pages as PNG
pages_to_render = {
    'vol37_p548_mag519': (f'{PDF_DIR}/reliefsocietymagvol37.pdf', 548),
    'vol37_p550_mag521': (f'{PDF_DIR}/reliefsocietymagvol37.pdf', 550),
    'vol40_p823_mag782': (f'{PDF_DIR}/reliefsocietymagvol40.pdf', 823),
}

for name, (pdf_path, page_num) in pages_to_render.items():
    d = fitz.open(pdf_path)
    page = d[page_num]
    # Render at 2x resolution for readability
    mat = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=mat)
    img_path = os.path.join(OUTPUT, f'{name}.png')
    pix.save(img_path)
    print(f"Saved: {img_path}")
    d.close()

# Also render the Vol37 CONTENTS page to verify poem titles
d = fitz.open(f'{PDF_DIR}/reliefsocietymagvol37.pdf')
page = d[533]
mat = fitz.Matrix(2, 2)
pix = page.get_pixmap(matrix=mat)
img_path = os.path.join(OUTPUT, 'vol37_contents_p533.png')
pix.save(img_path)
print(f"Saved: {img_path}")
d.close()

doc.close()
print("\nDone rendering pages. Check the images to read the actual poem titles.")
