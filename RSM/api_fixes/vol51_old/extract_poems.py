"""
Extract 4 poems from RSM PDFs using PyMuPDF.
Poems have garbled OCR in existing extraction; we extract directly from PDF text.
"""

import fitz  # PyMuPDF
import re
import os

PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"
OUTPUT_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\OutputExtractedarticles"

poems = [
    {
        "vol": 31, "year": 1944, "month": "July",
        "title": "I-30", "author": "Dott J. Sartori",
        "pdf": "reliefsocietymagvol31.pdf",
        "mag_page": 359,
        "category": "Poem",
    },
    {
        "vol": 37, "year": 1950, "month": "August",
        "title": "Promontory", "author": "Maude O. Cook",
        "pdf": "reliefsocietymagvol37.pdf",
        "mag_page": 519,
        "category": "Poem",
    },
    {
        "vol": 37, "year": 1950, "month": "August",
        "title": "Chalk Cliff", "author": "Ruth Harwood",
        "pdf": "reliefsocietymagvol37.pdf",
        "mag_page": 521,
        "category": "Poem",
    },
    {
        "vol": 40, "year": 1953, "month": "November",
        "title": "Woman-Power", "author": "Emily Carey Alleman",
        "pdf": "reliefsocietymagvol40.pdf",
        "mag_page": 782,
        "category": "Poem",
    },
]


def find_page_offset(doc, known_mag_page, search_text):
    """Try different offsets to find the right PDF page for a magazine page number."""
    # Search a range of possible offsets
    for offset in range(-5, 20):
        pdf_page = known_mag_page - offset
        if 0 <= pdf_page < len(doc):
            page = doc[pdf_page]
            text = page.get_text()
            if search_text.lower() in text.lower():
                return offset, pdf_page
    # Brute force: search all pages near the expected area
    for pdf_page in range(max(0, known_mag_page - 50), min(len(doc), known_mag_page + 50)):
        page = doc[pdf_page]
        text = page.get_text()
        if search_text.lower() in text.lower():
            return known_mag_page - pdf_page, pdf_page
    return None, None


def extract_and_report():
    results = []

    for poem in poems:
        pdf_path = os.path.join(PDF_DIR, poem["pdf"])
        print(f"\n{'='*60}")
        print(f"Processing: {poem['title']} by {poem['author']}")
        print(f"  PDF: {poem['pdf']}, Magazine page: {poem['mag_page']}")

        doc = fitz.open(pdf_path)
        print(f"  PDF has {len(doc)} pages")

        # Search for the poem title
        search_terms = [poem["title"]]
        if poem["title"] == "I-30":
            search_terms = ["I-30", "I—30", "I - 30", "TREK", "Sartori"]
        elif poem["title"] == "Woman-Power":
            search_terms = ["Woman-Power", "Woman Power", "Walking Hand in Hand", "Alleman"]

        found_page = None
        found_text = None

        for term in search_terms:
            offset, pdf_page = find_page_offset(doc, poem["mag_page"], term)
            if pdf_page is not None:
                page = doc[pdf_page]
                found_text = page.get_text()
                found_page = pdf_page
                print(f"  Found '{term}' on PDF page {pdf_page} (offset={offset})")
                break

        if found_page is None:
            # Broader search
            print(f"  Title not found near expected page. Searching broadly...")
            for pdf_page in range(len(doc)):
                page = doc[pdf_page]
                text = page.get_text()
                for term in search_terms:
                    if term.lower() in text.lower():
                        found_text = text
                        found_page = pdf_page
                        print(f"  Found '{term}' on PDF page {pdf_page}")
                        break
                if found_page is not None:
                    break

        if found_page is not None:
            # Also get adjacent pages in case poem spans pages
            adjacent_text = ""
            if found_page + 1 < len(doc):
                adjacent_text = doc[found_page + 1].get_text()

            print(f"\n  --- RAW TEXT from PDF page {found_page} ---")
            print(found_text[:3000])
            print(f"  --- END RAW TEXT ---")
            if adjacent_text:
                print(f"\n  --- NEXT PAGE (PDF page {found_page+1}) ---")
                print(adjacent_text[:2000])
                print(f"  --- END NEXT PAGE ---")

            results.append({
                **poem,
                "pdf_page": found_page,
                "raw_text": found_text,
                "next_page_text": adjacent_text,
            })
        else:
            print(f"  *** POEM NOT FOUND IN PDF ***")
            results.append({**poem, "pdf_page": None, "raw_text": None, "next_page_text": None})

        doc.close()

    return results


if __name__ == "__main__":
    results = extract_and_report()
