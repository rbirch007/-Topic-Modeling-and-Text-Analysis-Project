# SESSION HANDOVER: Volumes 21-29 (1934-1942)
## Relief Society Magazine Article Extraction Pipeline

**Date:** 2026-03-17
**Pipeline scripts:** `21_29_pipeline_extractor.py`, `21_29_pdf_verify.py`

---

## 1. PIPELINE OVERVIEW

### Architecture
```
Phase 1: PREPROCESS  →  Insert line breaks into OCR mega-lines (up to 150K chars/line)
Phase 2: EXTRACT     →  Run universal_run_extractor_v2.Rmd via Rscript
Phase 3: STANDARDIZE →  Rename files (Conv B→A), add bracket headers, consolidate ads
Phase 4: AUDIT       →  Cross-reference CONTENTS pages, flag merged/short/missing
Phase 5: PDF VERIFY  →  Targeted API calls against PDF images for flagged items
Phase 6: APPLY       →  Write corrected/split/new files from API results
```

### Key Decisions Made
- **v2 extractor won** over vol19extractor (372 vs 294 files on Vol23 test)
- **Preprocessor** inserted 41,034 line breaks across 108 input files
- **Targeted API approach** used (only flagged items, not full-text scan)
- **Double-nesting fix**: R extractor creates `Vol{N}_{YEAR}/Month/` internally, so output_dir points to parent

---

## 2. FILE LOCATIONS

### Scripts
```
C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\
├── 21_29_pipeline_extractor.py    # Phases 1-4 (preprocess, extract, standardize, audit)
├── 21_29_pdf_verify.py            # Phases 5-6 (API verification + apply)
├── universal_run_extractor_v2.Rmd # R extractor (READ ONLY - template)
├── vol19extractor.Rmd             # R extractor (DO NOT MODIFY - user needs it as-is)
└── universal_extractor.Rmd        # Has normalize_text() function (READ ONLY)
```

### Input Files
```
input/Vol{N}split/                    # Original OCR files (mega-lines)
input/Vol{N}split_preprocessed/       # After Phase 1 (line breaks inserted)
```

### Output Files (FINISHED)
```
OutputExtractedarticles/Vol{N}_{YEAR}/  # ← CANONICAL LOCATION (v2 + corrections)
  {Month}/
    {Month}_Vol{N}_{##}_{Category}_{Title}.txt
```

### Intermediate/Reference
```
OutputExtractedarticles_v2/Vol{N}_{YEAR}/   # v2 output (same as canonical, source of copy)
OutputExtractedarticles_v19/Vol23_1936/     # v19 test output (Vol23 only, for comparison)
api_corrections/Vol{N}_{YEAR}/
  pdf_verify_results.json                   # Full API results with corrected text
vol21_29_pdf_page_map.json                  # PDF page ranges per volume/month
pdf_audit/manual_review_list_vols1-20_52-57.txt  # Manual review items (Vols 21-29 appended)
```

### PDF Sources
```
C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine\
  reliefsocietymagvol{N}.pdf    # One PDF per volume
```

---

## 3. RESULTS SUMMARY

### File Counts by Volume
| Volume | Year | Files | Merged Split | Short Fixed | Missing Created | Missing Not Found |
|--------|------|-------|-------------|-------------|-----------------|-------------------|
| Vol 21 | 1934 | 403   | 3→7         | 3           | 0               | 4                 |
| Vol 22 | 1935 | 393   | 3→8         | 1           | 4               | 2                 |
| Vol 23 | 1936 | 391   | 2→6         | 0           | 1               | 9                 |
| Vol 24 | 1937 | 437   | 6→21        | 4           | 2               | 0                 |
| Vol 25 | 1938 | 404   | 2→9         | 5           | 8               | 11                |
| Vol 26 | 1939 | 404   | 1→3         | 2           | 3               | 4                 |
| Vol 27 | 1940 | 384   | 5→23        | 2           | 7               | 9                 |
| Vol 28 | 1941 | 384   | 1→2         | 1           | 4               | 9                 |
| Vol 29 | 1942 | 347   | 0           | 0           | 5               | 11                |
| **Total** | | **3,547** | **23→79** | **18** | **34** | **59** |

### Pipeline Statistics
- **Preprocessor**: 41,034 line breaks inserted, avg line length reduced from 500-2200 to 220-400 chars
- **Extraction**: v2 extractor produced 3,100+ raw files across 9 volumes
- **Standardization**: Convention A filenames, bracket headers, ad consolidation
- **API Verification**: 155 items processed (31 merged, 30 short, 93 missing)
- **Corrections Applied**: 79 new split articles, 18 short fixes, 34 missing found

---

## 4. REMAINING MANUAL REVIEW ITEMS

See `pdf_audit/manual_review_list_vols1-20_52-57.txt` (Vols 21-29 section appended).

### Summary of Items Needing Manual Review
- **8 unsplit merged files** — API returned single article but file may contain multiple
  - Vol 21: June (Joseph Barker)
  - Vol 23: February (Farth and Irvine)
  - Vol 24: June (Trifles Light As)
  - Vol 28: June (Mid-Summer)
  - Vol 29: March, September, October, November (serial chapters)
- **59 missing not found** — breakdown:
  - Garbled OCR entries (e.g., "ILS Blanche Robbing", "PUAppPeMIMGS", "(GSTS 9")
  - Frontispiece image-only pages (no text to extract)
  - Articles on PDF pages the API couldn't locate (page range limitations)
  - "Happenings" column by Annie Wells Cannon (appears in multiple volumes, OCR garbled)

---

## 5. TECHNICAL NOTES

### OCR Mega-Line Problem
Vols 21-29 input files had lines up to 150K characters where entire magazine issues collapsed into 1-2 lines. The preprocessor uses 6-step pattern matching:
1. Running headers (`NNN RELIEF SOCIETY MAGAZINE`)
2. ALL CAPS titles (3+ words)
3. "By Author" patterns
4. Section markers (CONTENTS, EDITORIAL, etc.)
5. Page number patterns
6. Sentence boundary breaks for lines >3000 chars

### R Extractor Integration
- Generated standalone `.R` scripts from `.Rmd` templates with patched paths
- ASCII encoding for R script files (non-ASCII → `?` replacement)
- Double-nesting fix: R creates `Vol{N}_{YEAR}/Month/` internally
- Rscript path: `C:\Program Files\R\R-4.5.2\bin\Rscript.exe`

### API Configuration
- Model: `claude-sonnet-4-20250514` (temperature=0)
- API key: `.api_key` file in project directory
- PDF images: PyMuPDF at DPI 150 (drops to 120 if >4MB)
- Cost: ~$5-7 for all 155 items (targeted approach)

### Filename Convention A
```
{Month}_Vol{N}_{##}_{Category}_{Title}.txt
```
- `##` = sequence number (2-digit, zero-padded)
- Category: Article, Poem, Editorial, Guide_Lessons, Frontispiece, Board, Misc, Contents
- Bracket header: `[Vol{N} | {Month} {Year} | Sequence: ## | Category: Cat | Title: Title]`

### OneDrive Workaround
Write to temp directory first, then copy back to OneDrive path (avoids EEXIST errors).

---

## 6. WHAT'S NEXT (IF CONTINUING)

1. **Manual review** of 8 unsplit merged files and 59 missing-not-found items
2. **Free OCR scan** (Phase 1 of Approach 2) to check all 3,547 files for low OCR match scores — this would identify additional garbled text files without API cost
3. **Cross-volume consistency check** — verify Board/Contents/Misc files exist for every issue
4. **Quality spot-checks** against archive.org PDFs for random articles
5. **Integration** — merge Vol 21-29 output with Vols 1-20 for unified corpus

---

## 7. COMMANDS REFERENCE

```bash
# Full pipeline (all phases)
python 21_29_pipeline_extractor.py --all --extractor v2

# Individual phases
python 21_29_pipeline_extractor.py --preprocess
python 21_29_pipeline_extractor.py --extract --extractor v2
python 21_29_pipeline_extractor.py --standardize
python 21_29_pipeline_extractor.py --audit

# PDF verification
python 21_29_pdf_verify.py --dry-run --range 21-29    # Preview what will be processed
python 21_29_pdf_verify.py --fix --range 21-29        # Run API verification
python 21_29_pdf_verify.py --apply --range 21-29      # Apply corrections to files

# Single volume
python 21_29_pdf_verify.py --fix --range 23-23
```
