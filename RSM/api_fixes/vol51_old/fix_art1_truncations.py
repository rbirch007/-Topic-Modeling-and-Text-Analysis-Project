"""
Fix 8 truncated Art 1 files in Vols 1-29.
- 2 files fixed from 00_Misc files (Vol12 Jan, Vol13 Nov)
- 6 files fixed via PDF extraction (Vol5, Vol7x2, Vol8, Vol10x2)
"""

import os
import shutil
import re
import fitz  # PyMuPDF
from datetime import datetime

BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\OutputExtractedarticles"
PDF_DIR = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine"

BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_backups_art1_truncation_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
os.makedirs(BACKUP_DIR, exist_ok=True)

results = []


def backup_file(filepath):
    fname = os.path.basename(filepath)
    vol_month = os.path.basename(os.path.dirname(filepath))
    vol = os.path.basename(os.path.dirname(os.path.dirname(filepath)))
    backup_name = f"{vol}__{vol_month}__{fname}"
    dest = os.path.join(BACKUP_DIR, backup_name)
    shutil.copy2(filepath, dest)
    return dest


def prepend_to_art1(filepath, missing_text):
    """Prepend missing text to Art 1 file, after the header line."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    lines = content.split('\n')
    header = lines[0] if lines[0].startswith('[') else ""

    if header:
        rest_start = len(header) + 1
        remaining = content[rest_start:].lstrip('\n')
        new_content = header + "\n\n" + missing_text.rstrip('\n') + "\n\n" + remaining
    else:
        new_content = missing_text.rstrip('\n') + "\n\n" + content

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)


def clean_ocr_text(text):
    """Clean OCR text: collapse multiple spaces, remove page headers."""
    # Collapse multiple spaces to single space (per line)
    lines = text.split('\n')
    cleaned = []
    for i, line in enumerate(lines):
        line = re.sub(r'  +', ' ', line).strip()
        # Skip page number headers like "492 RELIEF SOCIETY MAGAZINE."
        if re.match(r'^\d+\s+RELIEF\s+SOCIETY\s+MAGAZINE\.?$', line, re.IGNORECASE):
            continue
        # Skip reverse format: "RELIEF SOCIETY MAGAZINE 492"
        if re.match(r'^RELIEF\s+SOCIETY\s+MAGAZINE\.?\s+\d+$', line, re.IGNORECASE):
            continue
        # Skip standalone page numbers
        if re.match(r'^\d+\s*$', line):
            continue
        # Skip mid-article page headers like "MADAME CURIE 437" or "THE EVOLUTION OF THE POST 625"
        if re.match(r'^[A-Z][A-Z\s\.]+\d+\s*$', line):
            continue
        # Skip magazine masthead lines
        if re.match(r'^(The\s+)?Relief\s+Societ', line, re.IGNORECASE):
            continue
        if re.match(r'^Vol\.\s+[IVXLC]+', line):
            continue
        if line.strip() == 'THE' and i + 1 < len(lines) and re.match(r'Relief\s+Societ', lines[i + 1].strip(), re.IGNORECASE):
            continue
        cleaned.append(line)
    return '\n'.join(cleaned).strip()


# ============================================================
# FIX 1: Vol12 January - from Misc file
# ============================================================
print("=" * 60)
print("FIX 1: Vol12_1925/January Art 1 - from Misc file")
print("=" * 60)

misc_path = os.path.join(BASE, "Vol12_1925", "January", "January_Vol12_00_Misc.txt")
art1_path = os.path.join(BASE, "Vol12_1925", "January", "January_Vol12_01_Article_The_Year_Has_Gone_Eliza_R_Snow.txt")

with open(misc_path, 'r', encoding='utf-8') as f:
    misc_content = f.read()

poem_start = misc_content.find("THE YEAR HAS GONE")
snow_end = misc_content.find("Eliza R. Snow", poem_start) if poem_start >= 0 else -1

if poem_start < 0 or snow_end < 0:
    print("ERROR: Could not find poem text in Misc file")
    results.append(("Vol12 Jan", "FAILED"))
else:
    poem_text = misc_content[poem_start:snow_end + len("Eliza R. Snow")].strip()
    print(f"Found poem text ({len(poem_text)} chars)")
    backup_file(art1_path)
    prepend_to_art1(art1_path, poem_text)
    with open(art1_path, 'r', encoding='utf-8') as f:
        new_lines = f.read().split('\n')
    first_content = next((l for l in new_lines[1:] if l.strip()), "?")
    print(f"Now starts: {first_content[:80]}")
    results.append(("Vol12 Jan", f"OK - prepended {len(poem_text)} chars (poem from Misc)"))

# ============================================================
# FIX 2: Vol13 November - from Misc file
# ============================================================
print("\n" + "=" * 60)
print("FIX 2: Vol13_1926/November Art 1 - from Misc file")
print("=" * 60)

misc_path = os.path.join(BASE, "Vol13_1926", "November", "November_Vol13_00_Misc.txt")
art1_path = os.path.join(BASE, "Vol13_1926", "November",
                         "November_Vol13_01_Frontispiece_-_Typical_View_of_Green_River_Frontispiece_Susa_T._Jennings.txt")

with open(misc_path, 'r', encoding='utf-8') as f:
    misc_content = f.read()

fall_idx = misc_content.rfind("Fall\nBy Susan")
if fall_idx < 0:
    fall_idx = misc_content.rfind("Fall\nBy Susa")

if fall_idx < 0:
    print("ERROR: Could not find Fall poem in Misc file")
    results.append(("Vol13 Nov", "FAILED"))
else:
    poem_text = misc_content[fall_idx:].strip()
    print(f"Found poem text ({len(poem_text)} chars)")
    backup_file(art1_path)
    prepend_to_art1(art1_path, poem_text)
    with open(art1_path, 'r', encoding='utf-8') as f:
        new_lines = f.read().split('\n')
    first_content = next((l for l in new_lines[1:] if l.strip()), "?")
    print(f"Now starts: {first_content[:80]}")
    results.append(("Vol13 Nov", f"OK - prepended {len(poem_text)} chars (poem from Misc)"))

# ============================================================
# FIXES 3-8: PDF extraction
# ============================================================

# For each case, I've identified the exact PDF pages:
#
# Fix 3: Vol5 Sep - poem "Mothers of Israel" on PDF p549 (0-idx 548)
#         Art 1 starts "mothers in midwifery" from p558 (0-idx 557)
#         Missing: pages 548-556 (poem + Women Pioneers articles)
#
# Fix 4: Vol7 Sep - poem "The Needs of Yesterday" on PDF p545 (0-idx 544)
#         Art 1 starts "the needs and requirements" from Guide Lessons
#         Missing: just the poem on page 544
#
# Fix 5: Vol7 Nov - "The Evolution of the Post" starts PDF p683 (0-idx 682)
#         Art 1 starts "evolution of THE EVOLUTION" from p685 (0-idx 684)
#         Missing: pages 682-683 (first 2 pages of article)
#
# Fix 6: Vol8 Aug - Curie frontispiece p481, article starts p482 (0-idx 481)
#         Art 1 starts "you as foremost among scientists" from p487 (0-idx 486)
#         Missing: pages 480-485 (frontispiece + 5 pages of article)
#         Note: Art 1 already has "Mme. Curie in Center of Group" on line 3,
#               then "you as foremost" on next line. Missing is pages 481-486.
#
# Fix 7: Vol10 May - "A Mother's Love" starts PDF p263 (0-idx 262)
#         Art 1 starts "the mother's love was there" from same page
#         Missing: first ~80% of the page
#
# Fix 8: Vol10 Dec - frontispiece caption on PDF p714 (0-idx 713)
#         Art 1 has "Mrs. Percy Goddard and her little \nson, in the role..."
#         This matches the full PDF content - file appears COMPLETE
#         The truncation detection was a false positive (line wrap at "son,")

pdf_fixes = [
    {
        "label": "Vol5 Sep",
        "vol": 5,
        "art1_path": os.path.join(BASE, "Vol5_1918", "September",
                                   "September_Vol5_01_Mothers_in_Israel_Mothers_in_Israel_Coral_J._Black.txt"),
        "trunc_text": "mothers in midwifery",
        "start_page": 548,  # 0-indexed, poem page
        "end_page": 557,    # 0-indexed, page with truncated text
    },
    {
        "label": "Vol7 Sep",
        "vol": 7,
        "art1_path": os.path.join(BASE, "Vol7_1920", "September",
                                   "September_Vol7_01_Article_The_Needs_of_Yesterday_Bertha_A._Kleinman.txt"),
        "trunc_text": "the needs and requirements",
        "start_page": 544,  # 0-indexed, poem page
        "end_page": 544,    # poem is one page, Art 1 content is from later section
        "single_page_poem": True,  # Just extract the whole page as the poem
    },
    {
        "label": "Vol7 Nov",
        "vol": 7,
        "art1_path": os.path.join(BASE, "Vol7_1920", "November",
                                   "November_Vol7_01_Article_Page_The_Evolution_of_the_Post.txt"),
        "trunc_text": "mailing  systems  so  rapidly",  # OCR version
        "trunc_text_alt": "mailing systems so rapidly",  # clean version
        "start_page": 682,  # 0-indexed
        "end_page": 684,    # 0-indexed, page with truncated text
    },
    {
        "label": "Vol8 Aug",
        "vol": 8,
        "art1_path": os.path.join(BASE, "Vol8_1921", "August",
                                   "August_Vol8_01_Frontispiece_Mme._Curie_in_her_Laboratory_in_Paris_Frontispiece.txt"),
        "trunc_text": "you as foremost among scientists",
        "start_page": 480,  # 0-indexed, frontispiece page
        "end_page": 486,    # 0-indexed, page with truncated text
    },
    {
        "label": "Vol10 May",
        "vol": 10,
        "art1_path": os.path.join(BASE, "Vol10_1923", "May",
                                   "May_Vol10_01_Poem_The_Mother_Frontispiece.txt"),
        "trunc_text": "the mother's love was there",
        "trunc_text_alt": "the mother\u2019s love was there",
        "start_page": 262,  # 0-indexed
        "end_page": 262,    # same page
    },
    {
        "label": "Vol10 Dec",
        "vol": 10,
        "art1_path": os.path.join(BASE, "Vol10_1923", "December",
                                   "December_Vol10_01_Frontispiece_Mother_and_Son_in_Role_of_Madonna_and_Child_Frontispiece.txt"),
        "trunc_text": "son, in the role",
        "start_page": 713,  # 0-indexed
        "end_page": 713,    # same page
        "likely_complete": True,
    },
]


for i, fix in enumerate(pdf_fixes, 3):
    print(f"\n{'=' * 60}")
    print(f"FIX {i}: {fix['label']} - PDF extraction")
    print("=" * 60)

    pdf_path = os.path.join(PDF_DIR, f"reliefsocietymagvol{fix['vol']:02d}.pdf")
    art1_path = fix['art1_path']

    if not os.path.exists(pdf_path) or not os.path.exists(art1_path):
        print(f"ERROR: File not found")
        results.append((fix['label'], "FAILED - file not found"))
        continue

    doc = fitz.open(pdf_path)

    # Special case: Vol7 Sep - poem is self-contained on one page
    # The Art 1 content (Guide Lessons) is from a completely different section
    if fix.get('single_page_poem'):
        page_text = doc[fix['start_page']].get_text()
        missing_text = clean_ocr_text(page_text)
        print(f"Extracted poem ({len(missing_text)} chars) from PDF page {fix['start_page']+1}")
        print(f"Starts: {missing_text[:100]}...")
        backup_file(art1_path)
        prepend_to_art1(art1_path, missing_text)
        with open(art1_path, 'r', encoding='utf-8') as f:
            new_lines = f.read().split('\n')
        first_content = next((l for l in new_lines[1:] if l.strip()), "?")
        print(f"Now starts: {first_content[:80]}")
        results.append((fix['label'], f"OK - prepended {len(missing_text)} chars from PDF"))
        doc.close()
        continue

    # Check if the file appears complete already
    if fix.get('likely_complete'):
        with open(art1_path, 'r', encoding='utf-8') as f:
            art1_content = f.read()
        # Check if the content before the truncated text is already in the file
        lines = art1_content.split('\n')
        content_lines = [l for l in lines if l.strip() and not l.startswith('[')]
        if content_lines and not content_lines[0][0].islower():
            # First content line starts uppercase - might be complete
            # Compare with PDF
            page_text = doc[fix['start_page']].get_text()
            page_clean = ' '.join(page_text.split())
            file_clean = ' '.join(' '.join(content_lines).split())
            # Check if the file content is essentially the same as the PDF
            if page_clean[:30].lower().replace(' ', '') in file_clean[:50].lower().replace(' ', ''):
                print(f"File appears COMPLETE - content matches PDF page {fix['start_page']+1}")
                print(f"File starts: {content_lines[0][:80]}")
                print(f"PDF starts: {page_clean[:80]}")
                results.append((fix['label'], "SKIPPED - file already complete (false positive in truncation list)"))
                doc.close()
                continue

    # General case: extract pages from start to truncation page,
    # find truncation point, extract text before it
    all_text = ""
    for p in range(fix['start_page'], fix['end_page'] + 1):
        all_text += doc[p].get_text()

    # Find the truncation point
    trunc_text = fix['trunc_text']
    trunc_pos = all_text.lower().find(trunc_text.lower())

    if trunc_pos < 0 and 'trunc_text_alt' in fix:
        trunc_text = fix['trunc_text_alt']
        trunc_pos = all_text.lower().find(trunc_text.lower())

    if trunc_pos < 0:
        # Try with collapsed spaces
        collapsed = re.sub(r'\s+', ' ', all_text.lower())
        collapsed_search = re.sub(r'\s+', ' ', fix['trunc_text'].lower())
        trunc_pos_collapsed = collapsed.find(collapsed_search)
        if trunc_pos_collapsed >= 0:
            # Map back to original position approximately
            # Count chars up to collapsed position
            orig_pos = 0
            collapsed_pos = 0
            for ch in all_text:
                if collapsed_pos >= trunc_pos_collapsed:
                    break
                if ch.isspace():
                    if collapsed_pos > 0 and collapsed[collapsed_pos - 1] != ' ':
                        collapsed_pos += 1
                else:
                    collapsed_pos += 1
                orig_pos += 1
            trunc_pos = orig_pos
            print(f"Found truncation point via collapsed search at ~pos {trunc_pos}")

    if trunc_pos <= 0:
        print(f"ERROR: Could not find truncation text '{fix['trunc_text']}' in PDF pages {fix['start_page']+1}-{fix['end_page']+1}")
        # For debugging
        print(f"Text from these pages ({len(all_text)} chars):")
        print(f"First 200: {all_text[:200]}")
        results.append((fix['label'], "FAILED - truncation point not found"))
        doc.close()
        continue

    missing_raw = all_text[:trunc_pos].strip()
    missing_clean = clean_ocr_text(missing_raw)

    if len(missing_clean) < 10:
        print(f"WARNING: Very short missing text ({len(missing_clean)} chars) - file may be nearly complete")
        print(f"Missing text: '{missing_clean}'")

    print(f"Extracted {len(missing_clean)} chars from PDF pages {fix['start_page']+1}-{fix['end_page']+1}")
    print(f"Starts: {missing_clean[:100]}...")
    print(f"Ends: ...{missing_clean[-100:]}")

    backup_file(art1_path)
    prepend_to_art1(art1_path, missing_clean)

    with open(art1_path, 'r', encoding='utf-8') as f:
        new_lines = f.read().split('\n')
    first_content = next((l for l in new_lines[1:] if l.strip()), "?")
    print(f"Now starts: {first_content[:80]}")

    doc.close()
    results.append((fix['label'], f"OK - prepended {len(missing_clean)} chars from PDF"))

# ============================================================
# SUMMARY
# ============================================================
print("\n\n" + "=" * 60)
print("RESULTS SUMMARY")
print("=" * 60)
for label, status in results:
    print(f"  {label}: {status}")
print(f"\nBackups saved to: {BACKUP_DIR}")
