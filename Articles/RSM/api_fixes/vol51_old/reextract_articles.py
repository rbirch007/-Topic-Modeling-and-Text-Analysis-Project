import fitz  # PyMuPDF
import re
import os
import sys

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"
OUTPUT_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\OutputExtractedarticles"

# ============================================================
# ARTICLE 1: "Replica of Yesteryear" by Cecil G. Pugmire
# Vol 39, January 1952
# Pages 40-41 then continued on 70-71(+)
# ============================================================

def extract_replica_of_yesteryear():
    pdf_path = os.path.join(PDF_DIR, "reliefsocietymagvol39.pdf")
    doc = fitz.open(pdf_path)

    # We know from the scan: pages 40, 41 (main), then continued on 70, 71, possibly 72
    # Let's get the full text from all candidate pages
    candidate_pages = [39, 40, 69, 70, 71, 72]  # 0-indexed

    all_raw = {}
    for p in candidate_pages:
        if p < len(doc):
            text = doc[p].get_text()
            all_raw[p] = text
            print(f"Page {p+1}: {len(text)} chars")

    # Let's print the full text of the continuation pages to find the ending
    for p in [70, 71, 72]:
        if p in all_raw:
            print(f"\n=== FULL TEXT PAGE {p+1} ===")
            print(all_raw[p])
            print(f"=== END PAGE {p+1} ===")

    doc.close()
    return all_raw

print("=" * 60)
print("ARTICLE 1: Finding ending pages")
print("=" * 60)
art1_raw = extract_replica_of_yesteryear()

print("\n\n")
print("=" * 60)
print("ARTICLE 2: Finding all pages")
print("=" * 60)

def find_womans_influence():
    pdf_path = os.path.join(PDF_DIR, "reliefsocietymagvol40.pdf")
    doc = fitz.open(pdf_path)

    # Search for the article start - page 836 in magazine
    found_start = None
    for i in range(len(doc)):
        text = doc[i].get_text()
        if "McKay" in text and ("Woman" in text and "Influence" in text):
            print(f"Found McKay + Woman's Influence on PDF page {i+1}")
            if found_start is None:
                found_start = i

    if found_start is None:
        # Try broader search
        for i in range(len(doc)):
            text = doc[i].get_text()
            if "McKay" in text and "Relief Society" in text:
                print(f"Found McKay + Relief Society on PDF page {i+1}")
                if found_start is None:
                    found_start = i

    # Extract 8 pages starting from found page to be safe
    if found_start:
        for p in range(found_start, min(len(doc), found_start + 8)):
            text = doc[p].get_text()
            print(f"\n=== FULL TEXT PAGE {p+1} ===")
            print(text)
            print(f"=== END PAGE {p+1} ===")

    doc.close()
    return found_start

start_page = find_womans_influence()
