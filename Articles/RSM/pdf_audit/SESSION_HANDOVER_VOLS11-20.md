# Session Handover: Vols 11-20 Pipeline Fixes
## Relief Society Magazine Article Extraction
**Date:** 2026-03-17 (updated 2026-03-17 with Misc markers, cross-ref improvements, 2-column findings)
**Scope:** Vols 11-20 (1924-1933) — all programmatic fixes complete

---

## 1. Pipeline Overview (Streamlined Order)

The pipeline principle: **(1) Programmatic fixes first, (2) API/PDF verification second, (3) Manual review last.**

### Step-by-step for a new volume range:

```
Step 1: STANDARDIZE
  python vol11_20_pipeline.py --range {START}-{END} --apply
  → Normalizes filenames, adds bracket headers, cross-refs CONTENTS

Step 2: QC AUDIT
  python qc_audit_early.py --range {START}-{END}
  → Detects MERGED, SHORT, AD_AS_ARTICLE, MISSING via CONTENTS cross-ref
  → Outputs: pdf_audit/Vol{N}_{YEAR}/{Month}_audit.json

Step 3: SPLIT MERGED (API — one call per merged file)
  python split_merged.py --range {START}-{END}
  → Sends merged files + PDF images to Claude Sonnet
  → Returns split boundaries AND complete_text for each article
  → Outputs: api_corrections/Vol{N}_{YEAR}/{Month}_split.json

Step 4: APPLY SPLITS (no API)
  python apply_splits.py --range {START}-{END}
  → Creates individual article files from split instructions
  → Uses complete_text fallback when boundary matching fails
  → Applies validation (reject stubs <200 chars, ad-contaminated)
  → Applies decontamination (strips trailing/leading ad blocks)
  → Outputs: api_corrections/Vol{N}_{YEAR}/{Month}_split_applied.json

Step 5: RETRY FAILED SPLITS (no API — aggressive matching)
  python apply_splits.py --retry-failed --range {START}-{END}
  → Retries SKIP/FAILED entries with aggressive OCR-tolerant matching
  → 4 strategies: case-insensitive, OCR-error-tolerant, 2-word windows, sliding 5-word
  → Outputs: api_corrections/Vol{N}_{YEAR}/{Month}_split_retry.json

Step 6: RETRY EXTRACT (API — one call per still-failed article)
  python split_merged.py --retry-extract --range {START}-{END}
  → Sends PDF page images to Claude for articles still not found
  → Uses CONTENTS page hints to find correct PDF pages
  → Skips articles already found in prior runs (safe to re-run)
  → Outputs: api_corrections/Vol{N}_{YEAR}/{Month}_retry_extract.json

Step 7: APPLY RETRY EXTRACT (no API)
  python apply_splits.py --apply-retry-extract --range {START}-{END}
  → Creates article files from complete_text in retry_extract results
  → Applies same validation and decontamination as Step 4

Step 8: FIX SHORT FILES (API — one call per short file)
  python split_merged.py --fix-short --range {START}-{END}
  → Compares short files against PDF pages
  → Outputs: api_corrections/Vol{N}_{YEAR}/{Month}_short_fixes.json

Step 9: APPLY SHORT FIXES (no API)
  python apply_splits.py --apply-short --range {START}-{END}

Step 10: FIND MISSING (API — searches for CONTENTS entries not yet extracted)
  python split_merged.py --find-missing --range {START}-{END}
  → Outputs: api_corrections/Vol{N}_{YEAR}/{Month}_missing_found.json

Step 11: APPLY MISSING (no API)
  python apply_splits.py --apply-missing --range {START}-{END}

Step 12: EXTRACT MISC (no API)
  python extract_misc.py --range {START}-{END}
  → Extracts CONSOLIDATED articles from *_00_Misc.txt files

Step 13: REGENERATE MANUAL REVIEW LIST (no API)
  python C:\Users\birch\AppData\Local\Temp\gen_review.py
  → Updates pdf_audit/manual_review_list_vols1-20_52-57.txt
```

### Key principle: Steps 1-5 and 7, 9, 11-13 are FREE (no API). Steps 3, 6, 8, 10 use the API.

---

## 2. Key Files and Locations

### Scripts
| Script | Location | Purpose |
|--------|----------|---------|
| `vol11_20_pipeline.py` | Articleextractionrfiles/ | Standardize filenames + bracket headers |
| `qc_audit_early.py` | Articleextractionrfiles/ | QC audit (MERGED/SHORT/MISSING detection) |
| `split_merged.py` | Articleextractionrfiles/ | API-based splitting + PDF extraction |
| `apply_splits.py` | Articleextractionrfiles/ | Apply all corrections (splits, retries, short, missing) |
| `extract_misc.py` | Articleextractionrfiles/ | Extract consolidated Misc articles |
| `gen_review.py` | C:\Users\birch\AppData\Local\Temp\ | Regenerate manual review list |
| `universal_run_extractor.Rmd` | Articleextractionrfiles/ | R-based article extraction (Vol 57, adaptable) |
| `vol19extractor.Rmd` | Articleextractionrfiles/ | R-based extraction for Vols 18-29 (2-column) |
| `add_misc_markers.py` | C:\Users\birch\AppData\Local\Temp\ | Add structural markers to Misc files |

### Data Directories
| Directory | Contents |
|-----------|----------|
| `OutputExtractedarticles/Vol{N}_{YEAR}/` | Article text files organized by month |
| `api_corrections/Vol{N}_{YEAR}/` | All JSON correction files (splits, retries, etc.) |
| `pdf_audit/Vol{N}_{YEAR}/` | Audit JSON files per month |
| `pdf_audit/manual_review_list_vols1-20_52-57.txt` | Combined manual review list |

### PDFs
Location: `C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine\`
Naming: `reliefsocietymagvol{N}.pdf` (e.g., `reliefsocietymagvol11.pdf`)

---

## 3. What Was Fixed in This Session (Vols 11-20)

### Programmatic Fixes (no API cost)
- **Aggressive boundary matching** added to `apply_splits.py` — recovered **94 articles** that failed normal matching
  - 4 strategies: case-insensitive, OCR-error-tolerant substitutions (l/I/1, O/0, rn/m), 2-word windows, sliding 5-word windows
- **Validation** added: rejects stubs (<200 chars body), ad-contaminated (>30% ad-pattern lines), TOC listings
- **Decontamination** added: strips trailing/leading ad/publication blocks from article edges
- **27 bad files deleted** (18 stubs + 9 ad-contaminated)

### API Fixes ($5.26 total)
- **Retry-extract mode** added to `split_merged.py` — extracts single articles directly from PDF pages
- **CONTENTS page hints** — uses audit JSON `contents_xref` to find correct PDF pages instead of guessing
- **47 articles found** via PDF extraction across Vols 11-20
- **56 article files created** from extracted text

### Pipeline Future-Proofing
- **Default split prompt** now returns `complete_text` alongside boundaries — if boundary matching fails, the full text is available as fallback
- **`complete_text` fallback** in `apply_splits.py` — bypasses boundary matching entirely when complete_text > 100 chars
- **Skip logic** in retry-extract — safe to re-run; already-found articles are preserved and skipped

---

## 4. Final Results (Vols 11-20)

| Volume | Article Files | Failed Splits | Contents Not Found | Short | Unsplit |
|--------|:---:|:---:|:---:|:---:|:---:|
| 11 (1924) | 206 | 8 | 24 | 1 | - |
| 12 (1925) | 202 | 19 | 32 | 1 | - |
| 13 (1926) | 193 | 8 | 4 | 1 | - |
| 14 (1927) | 189 | 4 | 20 | 4 | - |
| 15 (1928) | 230 | 17 | 26 | 7 | - |
| 16 (1929) | 232 | 15 | 36 | - | - |
| 17 (1930) | 228 | 12 | 43 | - | 1 |
| 18 (1931) | 283 | 12 | 76 | 2 | - |
| 19 (1932) | 271 | 14 | 59 | - | 1 |
| 20 (1933) | 308 | 7 | 49 | - | - |
| **Total** | **2,342** | **116** | **369** | **16** | **2** |

- **116 failed splits**: OCR text too mangled or article not present in source file
- **369 contents not found**: In TOC but not located in the PDF scan pages (note: many were false "missing" due to weak cross-ref matching — see Section 9)
- **16 short files**: Under threshold, likely fragments or ads
- **2 unsplit merged**: Still contain multiple articles

---

## 5. Adapting for Next Volume Range (e.g., Vols 21-30)

### Before starting:
1. **Add volumes to VOL_YEARS dict** in `split_merged.py` (line ~25):
   ```python
   21: 1934, 22: 1935, 23: 1936, ...
   ```
2. **Add PDF filenames to PDF_FILES dict** in `split_merged.py` (line ~40):
   ```python
   21: "reliefsocietymagvol21.pdf", ...
   ```
3. **Add volumes to VOL_YEARS dict** in `extract_misc.py` and `qc_audit_early.py`
4. **Verify PDF files exist** in the RSmagazine directory
5. **Check if `vol11_20_pipeline.py` naming conventions apply** — Vols 21+ may use different file naming patterns. If so, adapt the pipeline script or create a new one.
6. **Verify R extractor output structure** — The original extraction is done by R scripts (`universal_run_extractor.Rmd` / `vol19extractor.Rmd`). Input split files are at `input/Vol{N}split/`. Check that the OCR text files have been cleaned for 2-column layout (Vol 18+ magazines switched to 2 columns).

### Then run the pipeline steps 1-13 in order.

### Key flags for all scripts:
- `--range 21-30` — Process entire volume range
- `--vol 21` — Single volume
- `--month January` — Single month
- `--dry-run` — Preview without writing (use first!)
- `--delay 3` — Seconds between API calls (default 3)

---

## 6. API Cost Reference

- **Model:** `claude-sonnet-4-20250514`
- **Cost formula:** `(input_tokens * 3 / 1_000_000) + (output_tokens * 15 / 1_000_000)`
- **Typical costs per call:**
  - Split merged (text + 5 PDF images): ~$0.03-0.07
  - Retry-extract (5 PDF images): ~$0.03 (not found) to ~$0.06 (found with text)
  - Fix-short: ~$0.03
  - Find-missing: ~$0.03-0.05

---

## 7. Known Issues and Workarounds

### OneDrive EEXIST error
Writing files directly to OneDrive paths sometimes fails with EEXIST. The `safe_write_json()` function in `split_merged.py` and `safe_write_file()` in `apply_splits.py` work around this by writing to `C:\Users\birch\AppData\Local\Temp\` first, then copying back.

### PDF page-finding limitations
The `find_pdf_pages_for_title()` function searches for article titles in PDF text. For volumes where the OCR quality is poor or articles have very generic titles, it may land on wrong pages. The CONTENTS page hints system mitigates this but requires audit JSON to be generated first (Step 2).

### Parse errors from API
Sometimes the API returns text before the JSON (e.g., "Looking through the PDF images..."). The `parse_api_response()` function handles markdown code blocks and raw JSON, but mixed text+JSON responses register as parse_error. These articles are skipped during apply.

### Misc file structural markers (Vols 16-20)
Vols 11-15 had structural markers (`=== FRONT MATTER ===`, `=== GENERAL BOARD ===`, `=== BACK MATTER ===`) in their `_00_Misc.txt` files from the `vol11_20_pipeline.py` standardization. Vols 16-20 were missing these markers. **Fixed 2026-03-17**: Added `=== FRONT MATTER ===` markers to all 60 Misc files in Vols 16-20. Backups saved with `_backup_premarker` suffix.

Note: No `=== CONSOLIDATED ===` markers exist in any volume — ads were never standalone files that got consolidated. They were placed directly into Misc by the R extractor.

### 2-Column layout (Vol 18+)
The magazine switched to a 2-column layout starting Vol 18 (1931). The OCR input files (`input/Vol{N}split/`) have already been cleaned to account for this — columns are properly merged into sequential text with clean TOC sections. The R extractors (`universal_run_extractor.Rmd`, `vol19extractor.Rmd`) handle this correctly.

The `find_pdf_pages_for_title()` function reads PDF text via PyMuPDF's `get_text()` which reads left column then right column. Titles spanning the full page width survive intact, but body text from 2-column pages is interleaved. This doesn't affect title searching but means boundary matching against PDF page text may be unreliable for 2-column volumes.

### Python version
Uses **Python 3.13** at `C:\Users\birch\AppData\Local\Programs\Python\Python313\python.exe`

### R extractors for original article extraction
The original article extraction is done by R scripts, not the Python pipeline:
- `universal_run_extractor.Rmd` — Used for Vol 57 and can be adapted for others
- `vol19extractor.Rmd` — Used for Vols 18-29 (handles 2-column layout)
- Input: `input/Vol{N}split/Vol{N}_No{M}_{Month}_{Year}.txt` (one file per issue)
- These scripts parse CONTENTS, classify articles (Article, Poetry, Editorial, etc.), extract boundaries from the OCR text, and output individual article files with category labels in filenames.

---

## 8. CONTENTS Cross-Reference Matching (Improved 2026-03-17)

The QC audit (Step 2) compares CONTENTS entries to extracted article files to find missing articles. The original matching in `qc_audit_early.py` used simple word overlap, which caused many false "missing" entries.

### Problem
For Vols 11-20, the original matching reported **501 missing CONTENTS entries**. Investigation showed many were false positives:
- **Title/filename mismatch**: CONTENTS title `"The Spirit of Easter Dr. George H. Brimhall"` didn't match filename `*Spirit_of_Easter*Brimhall*` due to strict word-overlap scoring
- **Garbled titles**: `"Lesson DepartmentTheology and, Testimony"` (missing spaces from OCR) scored 0.0
- **Noise entries**: Masthead lines, photo captions, and truncated entries parsed as CONTENTS entries

### Improvements made to `qc_audit_early.py`
1. **`_strip_author()`**: Strips trailing author names from CONTENTS titles before matching
2. **First-word anchor matching**: Matches if first 2-3 significant words align (score 0.6-0.75)
3. **Normalized substring containment**: Catches garbled titles where words run together
4. **`is_contents_noise()`**: Filters masthead lines, photo captions, fragments, and boilerplate
5. **Expanded stopwords**: Added "poem", "illustrated", "photo", "portrait"

### Results
| Volume | Old Missing | New Missing | Improvement |
|--------|:---:|:---:|:---:|
| 11 | 28 | 15 | +13 matches |
| 12 | 36 | 22 | +14 matches |
| 13 | 16 | 5 | +10 matches |
| 14 | 22 | 11 | +9 matches |
| 15 | 32 | 15 | +17 matches |
| 16 | 47 | 19 | +25 matches |
| 17 | 50 | 23 | +27 matches |
| 18 | 110 | 55 | +48 matches |
| 19 | 94 | 35 | +59 matches |
| 20 | 66 | 28 | +38 matches |
| **Total** | **501** | **228** | **+260 matches** |

This means **260 fewer articles** will be sent to the `--find-missing` API step in future runs, saving ~$8-13 in API costs per volume range.

### Impact on pipeline
The improved matching applies at Step 2 (QC audit). For future volume ranges, this means:
- **Fewer false "missing" entries** → fewer unnecessary API calls in Steps 6, 8, 10
- **More accurate manual review list** → human reviewers focus on genuinely missing articles
- The remaining 228 missing entries in Vols 11-20 represent articles the R extractor genuinely did not extract

---

## 9. Category Labels in Filenames

All article files across Vols 11-20 include category labels in their filenames (e.g., `_Article_`, `_Poetry_`, `_Editorial_`, `_Frontispiece_`, `_Lesson_`, `_Guide_`, `_Notes_`). **368 out of 369 files** have explicit category labels (99.7% coverage). Only 1 file in Vol 13 is missing a category.

These labels come from the R extractor's `classify_article()` function and are preserved through the Python pipeline. They are suitable for CSV export and text analysis categorization.

---

## 10. JSON File Formats

(Renumbered from Section 8)

### Split JSON (`{Month}_split.json`)
```json
{
  "source_file": "filename.txt",
  "num_articles_found": 5,
  "splits": [
    {
      "order": "a",
      "title": "Article Title",
      "category": "Article",
      "author": "Author Name",
      "start_boundary": "First 50 chars of article...",
      "end_boundary": "Last 50 chars of article...",
      "complete_text": "Full article text (when PDF attached)",
      "confidence": "high"
    }
  ],
  "running_headers_to_strip": ["THE RELIEF SOCIETY MAGAZINE"],
  "validation_issues": []
}
```

### Retry Extract JSON (`{Month}_retry_extract.json`)
```json
{
  "volume": 11, "month": "September", "year": 1924,
  "mode": "retry_extract",
  "total_failed": 5,
  "total_cost": 0.0631,
  "stats": {"found": 1, "not_found": 1},
  "articles": [
    {
      "order": "f",
      "title": "Etiquette",
      "status": "found",
      "complete_text": "Full article text...",
      "category": "Article",
      "author": "Author Name",
      "confidence": "high",
      "api_usage": {"input_tokens": 8385, "output_tokens": 560, "cost": 0.0336}
    }
  ]
}
```
