import fitz, os
PDF_DIR = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine'
OUTPUT = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\api_fixes\vol51_old'

# Zoom into the POETRY section of Vol40 CONTENTS (PDF page 756)
doc = fitz.open(f'{PDF_DIR}/reliefsocietymagvol40.pdf')
page = doc[756]
# Render at 4x for detail
mat = fitz.Matrix(4, 4)
pix = page.get_pixmap(matrix=mat)
# Crop to just the poetry section (bottom half of page)
# Page is roughly 612x792 points, so at 4x that's 2448x3168 pixels
# Poetry section is in the bottom third
clip = fitz.Rect(0, 500, 612, 720)  # points
pix2 = page.get_pixmap(matrix=mat, clip=clip)
pix2.save(os.path.join(OUTPUT, 'vol40_contents_poetry_zoom.png'))
print("Saved zoomed poetry section")
doc.close()
