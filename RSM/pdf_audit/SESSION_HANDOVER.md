# RSM Article Extraction Pipeline - Session Handover

## Project Overview
Extracting articles from the Relief Society Magazine (Vols 1-57, 1914-1970) into individual text files for dissertation research on women's print culture. Each magazine issue's articles, poems, fiction, lessons, and features are split into one-per-file with standardized naming.

## Current State (2026-03-21)

### What's Done
- **20,492 article files** extracted across 57 volumes (1914-1970)
- **Inventory spreadsheet**: `RSM_article_inventory_vols1-57.xlsx` (20,492 rows with volume/year/month/title/author/type/path)
- **QC report**: `pdf_audit/manual_review_list_vols1-57.txt` (5,257 remaining issues)
- **418 stubs fixed** via PyMuPDF PDF text extraction (programmatic, zero API cost)
- **~319 articles rescued** from oversized Misc files via Claude API (Vols 41-50)
- **Vol 49 March files 07-10** fixed: 4 scrambled files replaced with 7 properly bounded articles
- **All backups** consolidated in `backups_consolidated/` folder (523 files)

### Remaining Issues (from QC report)
| Issue Type | Count | Priority | Description |
|---|---|---|---|
| WRONG_CONTENT | 1,848 | HIGH | File body doesn't match title (R extractor boundary errors) |
| MISSING_ARTICLE | 1,074 | MEDIUM | CONTENTS lists article but no matching file exists |
| DUPLICATE | 767 | MEDIUM | >50% text overlap between files in same month |
| STUB_POEM | 717 | LOW | Short poems/frontispieces <500B (legitimate, not broken) |
| LARGE_MISC | 518 | MEDIUM | Misc files >4KB may contain trapped articles |
| CATEGORY_MISMATCH | 110 | LOW | Category label doesn't match content |
| STUB | 48 | MEDIUM | Non-poem files <500B that couldn't be fixed from PDF |
| EMPTY | 32 | HIGH | Files with no body content |

### Volume-Specific Notes
- **Vols 1-35**: Older naming convention (`August_Vol10_09_Article_Title.txt`), many have category issues
- **Vols 36-40**: Mixed naming (transition period), some have both old and new-style files
- **Vols 41-50**: New naming (`V49_03_10_Edit_Title_Author.txt`), Misc rescue completed
- **Vols 51-57**: Cleanest extraction, fewest issues

## File Structure
```
Articleextractionrfiles/
  OutputExtractedarticles/           # All extracted articles
    Vol10_1923/                      # One folder per volume
      January/                       # One folder per month
        V10_01_00_BOARD.txt          # Board listing
        V10_01_00_CONTENTS.txt       # Table of contents
        V10_01_00_Misc.txt           # Ads, masthead, boilerplate
        V10_01_01_Art_Title_Author.txt  # Individual articles
        ...
  backups_consolidated/              # All .bak and .stub_bak files
  pdf_audit/
    manual_review_list_vols1-57.txt  # QC report for manual review
  RSM_article_inventory_vols1-57.xlsx # Complete file inventory
```

## Key Scripts

### QC & Reporting
| Script | Purpose | Usage |
|---|---|---|
| `comprehensive_qc.py` | Scans all volumes for issues | `python comprehensive_qc.py --report pdf_audit/manual_review_list_vols1-57.txt` |
| `fix_stubs_pymupdf.py` | Fixes stub files from PDF text | `python fix_stubs_pymupdf.py --fix [--vol 41-50] [--dry-run]` |

### Misc Rescue (Vols 41-50)
| Script | Purpose | Usage |
|---|---|---|
| `misc_rescue_api.py` | API-based Misc segmentation | `python misc_rescue_api.py --scan` / `--rescue --vol 46 --month May` |
| `misc_rescue_all.py` | Programmatic Misc rescue (less effective) | `python misc_rescue_all.py --report` |

### Vol 50 QC
| Script | Purpose | Usage |
|---|---|---|
| `qc_vol50.py` | Complete QC pipeline for Vol 50 | `python qc_vol50.py --all --dry-run` |

### Existing Pipeline (R/Rmd)
| Script | Purpose |
|---|---|
| `vol51_extractor_fresh.Rmd` | Primary R extractor (Vols 36-50 re-extraction) |
| `vol19extractor.Rmd` | Extractor for older volumes (don't modify input paths!) |

## Naming Convention
```
V{vol}_{mm}_{seq}_{Category}_{TitleSlug}_{AuthorSlug}.txt
```
- **Categories**: Art, Poem, Fict, Lssn, SpFt, GenFt, Home, Frnt, Edit
- **Sequence**: 00 = BOARD/CONTENTS/Misc, 01+ = articles in magazine order
- **Suffixes**: a, b, c for sub-articles (e.g., 10a, 10b, 10c)

## Core Rules (User Requirements)
1. **Retain ALL text** - nothing discarded, ads go to Misc
2. **One article/poem per file** - no article content in Misc
3. **Every issue has**: BOARD, CONTENTS, Misc files
4. **Never write "I cannot transcribe this"** - use Problematic file instead
5. **File order = magazine order** - sequence numbers reflect page order
6. **Minimize API costs** - use programmatic fixes first (PyMuPDF, R extractors)
7. **Use R/Rmd extractors** over Python when possible (established pipeline)
8. **Don't modify vol19extractor.Rmd input paths** - user has them pointed correctly

## PDF Resources
- **Local PDFs**: `../Chris_Rogers/RSMagazinepdffiles/reliefsocietymagvol{nn}.pdf` (Vols 01-57)
  - Note: Vol 57 is named `reliefsocietymagvolvol57.pdf` (extra "vol")
- **Page map** (Vols 41-50): `vol41_50_pdf_page_map.json` - PDF page ranges per month
- **Archive.org**: https://archive.org/details/reliefsocietymagazine (same source as local copies)

## Streamlined Pipeline for Future Work

### For fixing WRONG_CONTENT issues (1,848 remaining):
```
1. Identify files: grep "WRONG_CONTENT" pdf_audit/manual_review_list_vols1-57.txt
2. For each file:
   a. Read CONTENTS to find expected page number
   b. Use PyMuPDF to extract text from correct PDF pages
   c. Replace file content, preserving header
   d. Backup original to backups_consolidated/
```

### For fixing MISSING_ARTICLE issues (1,074 remaining):
```
1. Cross-reference CONTENTS with existing files
2. Check if article is trapped in Misc (run misc_rescue_api.py --scan)
3. If in Misc: use misc_rescue_api.py --rescue
4. If not in Misc: use PyMuPDF to extract from PDF pages listed in CONTENTS
5. Create new file with proper naming convention
```

### For fixing DUPLICATE issues (767 remaining):
```
1. Compare duplicate pairs
2. Keep the more complete version
3. Move shorter/fragment version to backups_consolidated/
```

### For fixing LARGE_MISC (518 remaining):
```
1. Run misc_rescue_api.py --scan to identify large Misc files
2. For files >4KB: run --rescue to segment via API
3. Verify rescued articles have full text (not just CONTENTS stubs)
```

## API Configuration
- **Model**: Claude Sonnet (claude-sonnet-4-20250514)
- **API key**: Set in environment variable `ANTHROPIC_API_KEY`
- **Typical costs**: ~$0.03-0.15 per Misc rescue call, ~$10 total for all Vols 41-50

## Environment
- **Python**: 3.13 (use `python` not `python3` on Windows)
- **Key packages**: fitz (PyMuPDF), anthropic, openpyxl
- **R**: Used for vol51_extractor_fresh.Rmd and vol19extractor.Rmd
- **OS**: Windows (paths use backslashes, UTF-8 wrapper needed for stdout)
- **UTF-8 fix**: Scripts need `sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')`
