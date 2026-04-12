# SESSION HANDOVER: Volumes 30-40 (1943-1953)
## Relief Society Magazine Article Extraction Pipeline

**Date:** 2026-03-19 (updated session 3, final)
**Pipeline scripts:** `30_40_pipeline_extractor.py`, `30_40_pdf_verify.py`, `misc_api_extract.py`, `fix_art1_beginnings.py`, `extract_misc_poems.py`, `extract_poems_from_pdfs.py`, `misc_api_extract_vol1_29.py`

---

## 1. RESULTS SUMMARY

### File Counts (after all QC passes)
| Volume | Year | Files | Merged Split | Short Fixed | Missing Created |
|--------|------|-------|-------------|-------------|-----------------|
| Vol 30 | 1943 | 297   | 3→5         | 1           | 0               |
| Vol 31 | 1944 | 289   | 4→10        | 1           | 0               |
| Vol 32 | 1945 | 375   | 2→2         | 1           | 0               |
| Vol 33 | 1946 | 389   | 6→21        | 0           | 0               |
| Vol 34 | 1947 | 377   | 1→2         | 4           | 0               |
| Vol 35 | 1948 | 397   | 2→5         | 4           | 0               |
| Vol 36 | 1949 | 349   | 2→6         | 1           | 1               |
| Vol 37 | 1950 | 397   | 3→6         | 3           | 0               |
| Vol 38 | 1951 | 407   | 2→4         | 1           | 0               |
| Vol 39 | 1952 | 410   | 2→8         | 2           | 0               |
| Vol 40 | 1953 | 406   | 2→2         | 3           | 0               |
| **Total** | | **4,093** | **29→71** | **21** | **1** |

### Session 2 QC Summary
| Fix | Count | Cost |
|-----|-------|------|
| Articles extracted from Misc files (API) | 149 | $4.28 |
| Vol 35 manual Misc extractions | 4 | $0 |
| Art 1 beginnings restored + renamed | 50 | $2.12 |
| Poems recategorized (_Article_→_Poem_) | 186 | $0 |
| True duplicates removed | 5 | $0 |
| 00pb leftover files removed | 54 | $0 |
| Ads reclassified to Misc | 23 | $0 |
| Truncated articles re-extracted from PDF | 2 | $0 |
| Mojibake encoding fixed | all | $0 |
| Poems extracted from Misc via TOC | 8 | $0 |
| **Total Session 2 API cost** | | **$6.40** |

## 2. INPUT QUIRKS
- **Vol 30**: Combined June/July 1943 issue (wartime paper rationing) → 11 input files, no July folder
- **Vol 33, 35**: Had `~$` temp files — deleted before processing
- **Naming varies**: `Vol30_No1` vs `vol33_No1` (lowercase) vs `Vol36_No01` (zero-padded)
- **Vol 32**: Tiny PDF (5 MB vs 90-130 MB) — some pages may be missing

## 3. SCRIPTS AND COMMANDS

### Pipeline Scripts
```
30_40_pipeline_extractor.py    # Phases 1-4 (preprocess, extract, standardize, audit)
30_40_pdf_verify.py            # Phases 5-6 (API verify + apply)
30_40_qc_fixer.py              # QC fixes (ads, poems, truncation, misc extraction)
misc_api_extract.py            # API-assisted Misc article extraction (scan + apply)
fix_art1_beginnings.py         # Fix Art 1 missing beginnings via API
extract_misc_poems.py          # TOC-driven poem extraction from Misc files
```

### Commands
```bash
# Original pipeline
python 30_40_pipeline_extractor.py --all --range 30-40
python 30_40_pdf_verify.py --fix --range 30-40
python 30_40_pdf_verify.py --apply --range 30-40

# Session 2: Misc article extraction
python misc_api_extract.py --scan --range 30-40       # API scan ($4.28)
python misc_api_extract.py --apply --range 30-40       # Create 149 article files

# Session 2: Art 1 beginning fix
python fix_art1_beginnings.py --scan --range 30-40     # API scan ($2.12)
python fix_art1_beginnings.py --apply --range 30-40    # Prepend + rename 50 files

# Session 2: Poem extraction from Misc (TOC-driven, no API)
python extract_misc_poems.py --range 30-40             # Extract poems listed in TOC but missing from files

# Session 2: Poem recategorization (no API)
# Used poem_renames_vol30_40.json — 186 files renamed _Article_→_Poem_

# Session 2: QC fixes
python 30_40_qc_fixer.py --ads --poems --truncation --range 30-40
```

## 4. QC FIXES APPLIED

### Session 1 fixes:
- **16 ads moved to Misc** — subscription notices, ZCMI ads, insurance ads mislabeled as Article
- **20 poems recategorized** — short-line content renamed Article → Poem
- **111 truncated fragments rejoined** — small files starting mid-sentence merged back into parent articles
- **226 OneDrive-incompatible filenames fixed** — em dashes → hyphens, non-ASCII → underscore

### Session 2 fixes:
- **149 articles extracted from Misc** via `misc_api_extract.py` — full text extraction using PDF pages + OCR text
- **4 articles manually extracted** from Vol 35 Misc (Aleine Margetts Young bio, Trials and Happiness poem, Lean Your Ear story conclusion, Sentiment and Sanitation conclusion)
- **50 Art 1 beginnings restored** via `fix_art1_beginnings.py` — missing article beginnings prepended, files renamed with correct titles/authors
- **186 poems recategorized** — comprehensive scan found poems labeled as _Article_ or _Frontispiece_, renamed to _Poem_
- **8 poems extracted from Misc** via `extract_misc_poems.py` — TOC-driven extraction found poems in Misc files
- **5 true duplicates removed** (kept cleaner version): Call It a Day, Herman & Birthday Dinner, Joanna Sep, Place of Relief Society, Dear Conquest
- **54 00pb leftover files removed** — preliminary extraction duplicates
- **23 ads reclassified** to Misc — extraction errors, tiny fragments, pure ad content
- **2 truncated articles re-extracted** from PDF: Replica of Yesteryear (50% missing), Woman's Influence (90% missing)
- **All mojibake encoding fixed** — UTF-8 em-dashes/smart quotes

## 5. KNOWN REMAINING ISSUES

### 306 poems listed in TOC but not in any extracted file
The TOC/CONTENTS files list 1,099 poems across Vols 30-40. Of these:
- 785 exist as standalone files ✅
- 8 were extracted from Misc this session ✅
- **306 are not in any Misc file or article file** — they exist only on PDF pages that were never dumped to text

**These need PDF-based extraction.** The TOC has their titles, authors, and page numbers. Use PyMuPDF to extract text from those specific pages.

### How to extract the 306 missing poems:
```python
# Approach: TOC-driven PDF extraction
# 1. Parse CONTENTS files for POETRY section → get title, author, page number
# 2. Use page number to find the page in the PDF via PyMuPDF
# 3. Extract text from that page
# 4. Find the poem by title match
# 5. Write standalone file
# This requires the vol30_40_pdf_page_map.json for PDF page offsets
```

### 6 Art 1 files where no beginning was found
These may be complete as-is or may need manual review:
- Vol31 May, Vol36 December, Vol39 February, Vol39 December, Vol40 March, Vol40 July

### Structural pattern (for future volumes)
Each monthly issue follows this layout:
```
[Cover/Frontispiece poem] → [Masthead/Board] → [TOC] → [Lead article] →
[2-4 feature articles] → [Serialized fiction] → [Poems throughout] →
[Guide/Theology Lessons] → [Notes from the Field] → [Ads on back pages]
```

The R extractor consistently:
1. Absorbs Art 1 beginning into Misc (masthead/TOC bleeds into article)
2. Misses poems between TOC and Article 1
3. Labels poems as "Article"
4. Puts story continuations on ad-heavy pages into Misc
5. Labels last 2-3 ad pages as articles

## 6. TOC-DRIVEN PIPELINE (for Vols 41-49)

The most efficient approach for remaining volumes uses the cleaned TOC files as ground truth:

```
TOC.txt → Parse → (title, author, category, page#)
                         ↓
              PDF + page# → PyMuPDF text extract
                         ↓
              Programmatic cleanup (headers, page numbers, ads)
                         ↓
              Write clean article files with correct names
                         ↓
              Diff against TOC to verify completeness
                         ↓
              API ONLY for garbled/unreadable pages (~5-10%)
```

**TOC file locations:**
- Vols 30-40: `OutputExtractedarticles/Vol{N}_{YEAR}/{Month}/{Month}_Vol{N}_00_CONTENTS.txt`
- Vols 41-49: `OutputExtractedarticles/Vol{N}_{YEAR}/{Month}/TOC.txt`
- Vols 50-57: `pdftotextraw/toc_pages/toc_index.json`

**Benefits:**
- $0 for 90%+ of articles (PyMuPDF, no API)
- Correct metadata from the start (titles, authors, categories from TOC)
- No Art 1 truncation, no Misc trapping, no ad contamination
- One pass instead of 5 fix cycles

## 7. FILE LOCATIONS

### Output (CANONICAL)
```
OutputExtractedarticles/Vol{N}_{YEAR}/  ← finished corrected files
```

### API corrections
```
api_corrections/Vol{N}_{YEAR}/          ← API scan results (JSON)
  {Month}_misc_extract.json             ← Misc article extraction results
  {Month}_art1_fix.json                 ← Art 1 beginning fix results
```

### Reports and logs
```
qc_fixes_report_vol30_40.txt            ← QC fix report (all issues)
poem_rename_log_vol30_40.txt            ← 186 poem renames log
poem_rename_log_vol1_29.txt             ← 358 poem renames log (Vols 1-29)
poem_renames_vol30_40.json              ← Poem rename data (JSON)
poem_renames_vol1_29.json               ← Poem rename data (Vols 1-29)
misc_poem_extraction_report.txt         ← TOC-driven poem extraction report
pdf_poem_extraction_report_vol30_40.txt ← PDF poem extraction report (239 poems)
poem_qc_validation_report.txt           ← QC validation of all poem changes
poem_cleanup_report.txt                 ← Cleanup of bad PDF extractions (164 removed)
vol30_40_pdf_page_map.json              ← PDF page offset map
misc_cleanup_report_vol30_40.txt        ← Misc cleanup report (228 files, 695KB removed)
misc_api_extract_vol1_29.py             ← API extraction script for Vols 1-29
```

### PDFs
```
C:\Users\birch\OneDrive - George Mason University - O365 Production\
  Dissertation\Womenprintculture\RSmagazine\reliefsocietymagvol{N}.pdf
```

## 8. SESSION 3 WORK COMPLETED

### Poem extraction and recategorization (all programmatic, $0 API)
| Task | Vols | Count |
|------|------|-------|
| Poems recategorized (_Article_→_Poem_) | 1-29 | 351 (358 renamed, 7 reverted) |
| Poems recategorized (_Article_→_Poem_) | 30-40 | 186 |
| Poems extracted from PDFs via PyMuPDF | 30-40 | 239 (403 created, 164 bad ones cleaned up) |
| Poems extracted from Misc via TOC | 30-40 | 8 |
| Poems extracted from Misc via TOC | 1-29 | 0 (all already extracted) |
| Vol 35 specific poem fixes | 35 | 3 (Spring Dusk, Tulips, Something in Your Word) |

### QC performed
- PDF poem extraction QC: 73% failure rate caught, 164 non-poems deleted
- 4 duplicate pairs resolved
- 7 false positive renames reverted
- Vol31 file count verified intact

### Vols 1-29 programmatic fixes (COMPLETED)
- Ad reclassification (last 2-3 files per month)
- Art 1 truncation detection (logged for API fix)
- OneDrive filename fixes
- Misc cleanup: 228 files cleaned, 695KB removed, 512 content matches
- 4 garbled OCR poems fixed (titles were OCR errors: I-30→Trek, Promontory→My Thorn, Chalk Cliff→Radiant Gift, Woman-Power→Walking Hand in Hand)
- PDF naming standardized (removed zero-padding from Vols 1-9)

### Vols 1-29 API extraction (COMPLETED — $6.27)
- 242 Misc files scanned, 136 articles found, 123 new files created
- QC sample (15 files across Vols 2-29): all pass — proper headers, real article/poem content, no ads/garbage/duplicates

### Total session 3 API cost
| Task | Cost |
|------|------|
| Vols 30-40 (session 2 carryover) | $6.40 |
| Vols 1-29 API extraction | $6.27 |
| **Total** | **$12.67** |

## 9. WHAT'S NEXT (PRIORITY ORDER)
1. **Vols 41-49: Full cleanup** — poem recategorization, TOC extraction, ad reclassification ($0 programmatic)
2. **Vols 41-49: TOC-driven pipeline** — build for new extraction approach
3. **Final cross-volume QC** — verify no false splits, PDF spot-checks
4. Manual review of 6 Art 1 files where no beginning was found (Vols 30-40)
