import fitz
import re
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"
OUTPUT_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\OutputExtractedarticles"


def clean_ocr_text(text):
    """Clean up OCR artifacts from PyMuPDF extraction of two-column magazine pages."""
    # Replace common OCR ligature/artifact characters
    text = text.replace('\u25a1', '')  # empty box
    text = text.replace('\ufb01', 'fi')
    text = text.replace('\ufb02', 'fl')
    text = text.replace('\u2018', "'").replace('\u2019', "'")
    text = text.replace('\u201c', '"').replace('\u201d', '"')
    text = text.replace('\u2014', '—')
    text = text.replace('\u2013', '–')

    # Collapse multiple spaces to single
    text = re.sub(r'  +', ' ', text)

    # Fix hyphenated line breaks (word- \nbreak -> wordbreak)
    text = re.sub(r'(\w)-\s*\n\s*(\w)', r'\1\2', text)

    # Clean up newlines - join lines that are part of the same paragraph
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if line:
            cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)


# ============================================================
# ARTICLE 1: "Replica of Yesteryear" by Cecil G. Pugmire
# ============================================================

def extract_article1():
    pdf_path = os.path.join(PDF_DIR, "reliefsocietymagvol39.pdf")
    doc = fitz.open(pdf_path)

    # Article is on PDF pages 40-41 (0-indexed: 39-40), continues on pages 70-72 (0-indexed: 69-71)
    # But the two-column layout means PyMuPDF extracts text in mixed column order
    # We need to extract and then manually reorder

    # Get raw text from all article pages
    pages_text = {}
    for p in [39, 40, 69, 70, 71]:
        pages_text[p] = doc[p].get_text()

    doc.close()

    # Since the existing file already has a good clean extraction up to where it was truncated,
    # and the two-column OCR makes automatic reordering unreliable,
    # let's use the existing clean text and append the missing ending.

    # The existing file ends at: "Casually he handed the box to Alice."
    # From page 72 (0-indexed 71), the continuation is clear.
    # Let me construct the complete article text manually from the PDF content.

    # The article text from PDF page 72 (0-indexed 71) after the ad:
    continuation_text = """Not to be outdone, she slowly untied the string, gently lifted the crumpled tissue, and slowly removed the four candles from the box. She handed two of them to Alex and kept two for herself.

ALEX spent a long, long time attaching them to the boughs, while Alice sought special places for her candles. After considerable fixing and unfixing, the candles stood tall and graceful, awaiting the match that would change them into glowing beacons.

Alex and Alice, like two frightened children, looked at each other. The rhyme could not come from their dry throats.

The knocker on the door broke the silence, and the chimes began their usual peel. Moving as an actor in a well-rehearsed scene, Alex opened the door and automatically reached out his hand for the telegram.

Alice, as if her knees could not bear the weight of her body, sank into the big chair. As Alex handed her the unopened message she closed her eyes for a moment and then mechanically tore the flap and began to read.

Having read, she laid her head on Alex's shoulder and gave way to the pent-up tears. Alex placed his arm firmly around her waist and studied the message. Having drunk of its contents, he placed it on the mantel, took his handkerchief from his pocket, and wiped Alice's tear-stained face. He took hold of Alice's hand and said, "Come, Mother, we haven't yet sung the rhyme."

Alice lifted her face, blinked back the tears and, greatly off-key, joined with Alex in the rhyme.

The leaping flames from the fireplace splashed the mantel and threw golden shadows over the words of the telegram:

Light the candles on the tree, one for Mom, one for Pop, one for Jim, and one for me. I've found Jim.

Ray"""

    # Read the existing file to get the clean text up to truncation point
    existing_path = os.path.join(OUTPUT_DIR, "Vol39_1952", "January",
                                  "January_Vol39_28_Article_Replica_of_Yesteryear_Cecil_C_Pugmire.txt")
    with open(existing_path, 'r', encoding='utf-8') as f:
        existing_text = f.read()

    # The existing text ends at "Casually he handed the box to Alice."
    # Build the complete article
    # Fix the header: Cecil C -> Cecil G
    complete_text = existing_text.rstrip()
    complete_text = complete_text.replace("Cecil C. Pugmire", "Cecil G. Pugmire")

    # Append the missing ending
    complete_text += "\n\n" + continuation_text.strip() + "\n"

    # Write to new filename (fixing C -> G)
    new_filename = "January_Vol39_28_Article_Replica_of_Yesteryear_Cecil_G_Pugmire.txt"
    new_path = os.path.join(OUTPUT_DIR, "Vol39_1952", "January", new_filename)

    with open(new_path, 'w', encoding='utf-8') as f:
        f.write(complete_text)

    print(f"Article 1 written to: {new_path}")
    print(f"Total length: {len(complete_text)} chars")

    # Remove old file with wrong name
    if os.path.exists(existing_path) and existing_path != new_path:
        os.remove(existing_path)
        print(f"Removed old file: {existing_path}")

    return complete_text


# ============================================================
# ARTICLE 2: "Woman's Influence" by President David O. McKay
# ============================================================

def extract_article2():
    pdf_path = os.path.join(PDF_DIR, "reliefsocietymagvol40.pdf")
    doc = fitz.open(pdf_path)

    # Article spans PDF pages 836-841 (0-indexed: 835-840)
    raw_pages = []
    for p in range(835, 841):
        text = doc[p].get_text()
        raw_pages.append((p, text))
        print(f"Page {p+1}: {len(text)} chars")

    doc.close()

    # Print all raw text for inspection
    for p, text in raw_pages:
        print(f"\n{'='*40} PDF PAGE {p+1} {'='*40}")
        print(text)

    return raw_pages

print("=" * 60)
print("EXTRACTING ARTICLE 1")
print("=" * 60)
art1 = extract_article1()

print("\n\n")
print("=" * 60)
print("EXTRACTING ARTICLE 2 - RAW TEXT")
print("=" * 60)
art2_raw = extract_article2()
