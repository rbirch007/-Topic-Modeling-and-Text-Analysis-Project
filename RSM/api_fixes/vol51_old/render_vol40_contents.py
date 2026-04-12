import fitz, os
PDF_DIR = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine'
OUTPUT = r'C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\api_fixes\vol51_old'

doc = fitz.open(f'{PDF_DIR}/reliefsocietymagvol40.pdf')
page = doc[756]
mat = fitz.Matrix(2, 2)
pix = page.get_pixmap(matrix=mat)
pix.save(os.path.join(OUTPUT, 'vol40_contents_p756.png'))
print("Saved vol40_contents_p756.png")
doc.close()
