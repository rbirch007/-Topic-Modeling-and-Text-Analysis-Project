import fitz
import sys
sys.stdout.reconfigure(encoding='utf-8')

PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"

# Article 2: Find the actual article pages (magazine page 790+)
pdf_path = f"{PDF_DIR}\\reliefsocietymagvol40.pdf"
doc = fitz.open(pdf_path)

# The table of contents says "Woman's Influence" starts on magazine page 790
# Search for the article title near McKay
for i in range(len(doc)):
    text = doc[i].get_text()
    if "I now turn the key" in text or "Prophet Joseph Smith" in text:
        print(f"Found 'I now turn the key' / 'Prophet Joseph Smith' on PDF page {i+1}")
    if "Annual General Relief Society Conference" in text and "McKay" in text:
        print(f"Found conference reference on PDF page {i+1}")

# Let's try searching around PDF page 836 area (where TOC is) - the article is at magazine pg 790
# The TOC page was PDF page 833, and TOC lists article at page 790
# Magazine page 787 = PDF page 833, so offset = 833 - 787 = 46... wait that's wrong
# Actually the page footer says "Page 787" on PDF page 833
# So PDF page = magazine page + (833-787) = magazine page + 46
# Magazine page 790 => PDF page 790 + 46 = 836
# Let's check PDF page 836

for p in range(835, 843):
    if p < len(doc):
        text = doc[p].get_text()
        print(f"\nPDF page {p+1}, first 200 chars:")
        print(text[:200])

doc.close()
