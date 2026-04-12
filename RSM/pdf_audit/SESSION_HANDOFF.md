# Relief Society Magazine — Article Extraction Pipeline Handoff

## Project Overview

Dissertation project extracting individual articles from the Relief Society Magazine (1914-1970, 57 volumes) for text analysis. Each volume is a bound PDF. Articles are extracted into individual `.txt` files organized by `Volume/Month/` directories.

**Corpus**: ~2,300+ article files across Vols 1-10 (Phase 2), plus Vols 11-57 (Phase 1, largely complete).

## Directory Structure

```
BASE = C:\Users\birch\OneDrive - George Mason University - O365 Production\
       Dissertation\textanalysis\Articleextractionrfiles\

OutputExtractedarticles/     # Extracted article .txt files
  Vol1_1914/January/         #   Vol 1 uses: V1_1914_Mon_##_Cat_Title.txt
  Vol2_1915/January/         #   Vols 2-10 use: Month_Vol#_##_Cat_Title.txt
  ...
  Vol57_1970/

input/                       # OCR text files (one per monthly issue)
InputClaude/                 # Alternative OCR location (some volumes)
  Vol1split/                 #   Vol 1's OCR files are here

api_corrections/             # API-generated correction JSONs
  Vol{N}_{YEAR}/
    {Month}_split.json       #   Merged file split instructions
    {Month}_short_fixes.json #   SHORT file fix results
    {Month}_missing_found.json # Missing article extraction results
    {Month}_corrections.json #   Text verification corrections

pdf_audit/                   # QC audit results (from qc_audit_early.py)
  Vol{N}_{YEAR}/
    {Month}_audit.json       #   Per-month audit with flags, CONTENTS xref

Womenprintculture/RSmagazine/  # Source PDFs (separate OneDrive path)
  reliefsocietymagvol01.pdf    #   ...through vol57
```

Full PDF path: `C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\Womenprintculture\RSmagazine\`

## Key Technical Details

- **Python**: `C:\Users\birch\AppData\Local\Programs\Python\Python313\python.exe`
- **API model**: `claude-sonnet-4-20250514` (temperature=0)
- **API key**: Set in `ANTHROPIC_API_KEY` environment variable
- **PDF library**: PyMuPDF (`fitz`) -- `page.get_pixmap(dpi=150)` for image extraction
- **OneDrive EEXIST bug**: Direct file writes to OneDrive paths often fail with EEXIST. Workaround: write to `C:\Users\birch\AppData\Local\Temp\` then `cp` back to OneDrive.
- **Cost formula**: `(input_tokens * 3 / 1_000_000) + (output_tokens * 15 / 1_000_000)`

## Active Scripts (Phase 2 -- Vols 1-10)

### Core Pipeline Scripts

| Script | Lines | Purpose |
|--------|-------|---------|
| `qc_audit_early.py` | 875 | **QC scanner** -- Audits all files, flags SHORT/MERGED/EMPTY, cross-refs against CONTENTS page, produces audit JSONs |
| `split_merged.py` | 1764 | **Multi-mode fixer** -- Three modes: (1) split merged files, (2) `--fix-short` fixes truncated files via PDF, (3) `--find-missing` extracts missing articles from PDF |
| `extract_merged.py` | ~800 | **Complete-text splitter** -- Extracts full article text from MERGED files (bypasses boundary-matching). `--apply` to write files |
| `apply_splits.py` | 662 | **Apply splits** -- Takes `_split.json` files, extracts text between boundaries, creates new article files |
| `apply_corrections.py` | 474 | **Apply corrections** -- Takes `_corrections.json` files, applies trim/prepend/append text fixes |
| `add_headers.py` | ~250 | **Metadata headers** -- Adds `[Vol# | Month Year | Seq: ## | Cat | Title]` header to all article files |
| `verify_and_fix.py` | 667 | **Text verifier** -- Compares extracted text against OCR source, identifies truncations/errors |

### How They Connect

```
qc_audit_early.py  -->  pdf_audit/Vol{N}_{YEAR}/{Month}_audit.json
                            |
split_merged.py (default mode)  -->  api_corrections/.../Month_split.json
split_merged.py --fix-short     -->  api_corrections/.../Month_short_fixes.json
split_merged.py --find-missing  -->  api_corrections/.../Month_missing_found.json
extract_merged.py               -->  api_corrections/.../Month_merged_extract.json
                            |
apply_splits.py         -->  Creates new files from boundary splits
extract_merged.py --apply --> Creates new files from complete_text extracts
apply_corrections.py    -->  Applies text corrections to existing files
                            |
add_headers.py --range 2-10  -->  Adds [Vol | Month | Seq | Cat | Title] headers
                            |
qc_audit_early.py  -->  Re-audit to verify improvements
```

### CLI Examples

```bash
# Python executable (use full path)
PY="/c/Users/birch/AppData/Local/Programs/Python/Python313/python.exe"

# QC Audit
$PY qc_audit_early.py --vol 3 --year 1916        # Single volume
$PY qc_audit_early.py --range 1-10                # All early volumes

# Split merged files (default mode)
$PY split_merged.py --vol 2 --year 1915 --month January --with-pdf
$PY split_merged.py --range 1-10 --with-pdf --delay 3

# Fix SHORT/truncated files
$PY split_merged.py --fix-short --vol 4 --year 1917 --delay 3
$PY split_merged.py --fix-short --range 4-10 --delay 3

# Find missing articles from PDF
$PY split_merged.py --find-missing --vol 5 --year 1918 --month June
$PY split_merged.py --find-missing --range 2-10 --delay 3

# Apply split instructions
$PY apply_splits.py --vol 2 --year 1915 --dry-run  # Preview
$PY apply_splits.py --range 1-10                     # Apply all

# Apply text corrections
$PY apply_corrections.py --vol 2 --year 1915

# Extract merged files (complete-text approach -- better than boundary matching)
$PY extract_merged.py --range 3-10 --with-pdf --delay 3  # API calls
$PY extract_merged.py --range 3-10 --apply --dry-run      # Preview apply
$PY extract_merged.py --range 3-10 --apply                 # Apply

# Add metadata headers
$PY add_headers.py --range 2-10 --dry-run                  # Preview
$PY add_headers.py --range 2-10                             # Apply
```

## File Naming Conventions

**Vol 1** (bracket header format):
```
V1_1914_Apr_03_Art_Genealogy.txt
V1_1914_Apr_03a_Art_Part_A.txt     # Split files use letter suffix
```
Header: `[Article -- Genealogy -- By Author Name -- April 1914]`

**Vols 2-10** (metadata header format):
```
January_Vol2_10_Editorial_Editorial.txt
January_Vol2_10a_Editorial_Relief_Society_Pledge.txt  # Split
```
Header (first line): `[Vol2 | January 1915 | Sequence: 10 | Category: Editorial | Title: Editorial]`

## Current State (as of March 15, 2026)

### Overall Metrics (Vols 2-10)
- **2,113 total files** across 9 volumes
- **96.3% OK** (2,034 files)
- **79 flagged** (50 SHORT + 22 MERGED false-positives + 7 other)
- **1,788 article files** have metadata headers
- **325 special files** (BOARD, CONTENTS, Misc) skipped for headers

### Vol 1 (cleaned up with vol1_cleanup.py)
- **142 total files** (106 articles, 36 special)
- **141 OK (99.3%)**
- **1 MERGED flag** (November Home-Ethics -- multi-lesson content, minor)
- **101/101 INDEX entries matched (100%)**
- **65+ ad files consolidated into Misc** (text preserved, not deleted)
- All articles have bracket headers: `[Vol1 | Month 1914 | Sequence: ## | Category: Type | Title: Title]`
- Categories: Art, HomeEthics, HomeGarden, Gen, Lit, Edit, Instr, CurrEvents, Notice
- Script: `vol1_cleanup.py` (--analyze, --apply, --month MonthName)

### Per-Volume Breakdown (Vols 2-10)
| Vol | Total | OK | % | SHORT | MERGED* |
|-----|-------|----|---|-------|---------|
| 2 | 210 | 210 | 100.0% | 0 | 0 |
| 3 | 247 | 239 | 96.8% | 2 | 3 |
| 4 | 248 | 237 | 95.6% | 5 | 5 |
| 5 | 253 | 241 | 95.3% | 5 | 5 |
| 6 | 256 | 247 | 96.5% | 8 | 1 |
| 7 | 201 | 196 | 97.5% | 4 | 1 |
| 8 | 218 | 207 | 95.0% | 9 | 2 |
| 9 | 242 | 230 | 95.0% | 10 | 1 |
| 10 | 238 | 227 | 95.4% | 7 | 4 |

*MERGED flags are mostly false positives -- API verified these are single articles that reference other lessons or have ALL-CAPS section headers.

### Completed Work
1. Four rounds of merged file splitting (reduced from 177 to 0 actual merged)
2. `extract_merged.py` complete-text approach resolved all 26 remaining MERGED files ($2.18 API cost)
3. Text verification and corrections applied
4. Vol 1 ad fragment deletion (30 files)
5. `--fix-short` API calls completed for all Vols 3-10 (120 SHORT files processed)
6. `--find-missing` API calls completed for all Vols 2-10 (62+ articles searched, 17 found)
7. Short fixes applied: 32+ files replaced with full text, 42+ junk files deleted
8. Missing articles applied: 16 new articles created from PDF
9. Content filtering crash fix (BadRequestError now caught gracefully)
10. Metadata headers added to all 1,788 article files in Vols 2-10
11. Vol 1 full cleanup via `vol1_cleanup.py`: parsed INDEX TO LESSON STATEMENTS, classified 65+ ad files and consolidated into Misc, matched 101/101 INDEX entries, extracted missing articles from OCR, added bracket headers to all articles, proper category prefixes (HomeEthics, Gen, Lit, Edit, etc.)

### Remaining Issues
1. **50 SHORT files** in Vols 2-10 -- mostly brief stubs, captions, or items not recoverable from PDF
2. **22 MERGED false positives** -- single articles flagged due to multi-lesson references or ALL-CAPS headers. No action needed.
3. **1 MERGED flag** in Vol 1 November (Home-Ethics contains two lessons) -- minor, could be manually split if needed

### API Cost So Far
- fix-short: ~$2.50 (120 files processed)
- find-missing: ~$1.50 (62 articles searched)
- extract-merged (complete-text): ~$2.18 (26 files)
- Splits + verification (earlier rounds): ~$20-25
- Total Phase 2: ~$30-35

## Correction JSON Formats

### _short_fixes.json
```json
{
  "volume": 3, "month": "April", "year": 1916,
  "mode": "fix_short",
  "total_short": 4,
  "stats": {"truncated": 3, "complete": 0, "delete": 1, "not_found": 0},
  "files": [
    {
      "source_file": "April_Vol3_07_Guide_Lessons_Work_and_Business.txt",
      "status": "truncated",
      "current_chars": 64,
      "complete_text": "LESSON II.\nWork and Business\n...",
      "title": "LESSON II. Work and Business",
      "category": "Guide_Lessons",
      "confidence": "high",
      "pdf_pages": [53, 54, 55, 56]
    }
  ]
}
```
Statuses: `truncated` (replace text), `complete` (no action), `delete` (remove file), `not_found` (skip), `content_filtered` (API blocked)

### _missing_found.json
```json
{
  "volume": 5, "month": "April", "year": 1918,
  "mode": "find_missing",
  "total_missing": 1,
  "stats": {"found": 1, "not_found": 0, "already_extracted": 0},
  "articles": [
    {
      "title": "Charity (A Poem) Grace Ingles Frost",
      "printed_page": 185,
      "status": "found",
      "complete_text": "Charity.\n\nBy Grace Ingles Frost.\n...",
      "category": "Poem",
      "author": "Grace Ingles Frost",
      "confidence": "high",
      "pdf_pages": [205, 206, 207, 208]
    }
  ]
}
```
Statuses: `found` (create new file), `not_found` (skip), `already_extracted` (skip), `content_filtered` (API blocked)

### _split.json
```json
{
  "source_file": "January_Vol2_10_Editorial_Editorial.txt",
  "num_articles_found": 3,
  "splits": [
    {
      "order": "a",
      "title": "Relief Society Pledge",
      "category": "Editorial",
      "author": null,
      "start_boundary": "EDITORIAL . RELIEF SOCIETY MAGAZINE Editor...",
      "end_boundary": "we would like to develop...",
      "confidence": "high"
    }
  ],
  "running_headers_to_strip": ["RELIEF SOCIETY MAGAZINE"]
}
```

## Known Issues and Gotchas

1. **Content filtering**: Some PDF pages trigger Anthropic's content filter (400 BadRequestError). The script catches these gracefully and records status `"content_filtered"`.

2. **Vol 9 year typo**: Vol 9's OCR files may have `1822` instead of `1922` in paths. The scripts handle this.

3. **Vol 1 "Poemfrom" stubs**: ~13 files in Vol 1 with garbled names like `V1_1914_Jan_05_Poemfrom...` are ad extraction artifacts. Most should be deleted.

4. **Vol 6 running headers**: Vol 6 articles have running page headers that look like article boundaries. The split prompt has specific guidance to ignore these.

5. **Guide Lessons "Work and Business"**: ~70 SHORT files across all volumes are "Work and Business" lesson stubs containing only the header. Most are actually truncated and recoverable from PDF.

6. **CONTENTS matching**: The audit cross-references extracted files against the magazine's CONTENTS page. Some titles don't match due to OCR errors, abbreviations, or editorial variations. "not_found" in find-missing often means the article is a frontispiece/image, music page, or already extracted under a different name.

7. **OneDrive sync latency**: After writing files, OneDrive may take a moment to sync. The `safe_write_json()` function in `split_merged.py` handles the EEXIST error by writing to temp first.

## Prompt Template for New Session

When starting a new session, use something like:

---

I'm continuing work on the Relief Society Magazine article extraction pipeline (Phase 2, Vols 1-10). Please read the session handoff document:

```
Read: C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\SESSION_HANDOFF.md
```

Key environment:
- Python: `C:\Users\birch\AppData\Local\Programs\Python\Python313\python.exe`
- Working dir: `C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles\`
- OneDrive EEXIST workaround: Write to `C:\Users\birch\AppData\Local\Temp\` then `cp` back
- Active scripts: `qc_audit_early.py`, `split_merged.py`, `extract_merged.py`, `apply_splits.py`, `apply_corrections.py`, `add_headers.py`, `verify_and_fix.py`

[Then describe your specific task, e.g.:]
- "Clean up the remaining 50 SHORT files in Vols 2-10"
- "Delete the 14 Vol 1 Poemfrom ad artifacts"
- "Run the full pipeline on Vols 11-20"
- "Add metadata headers to Vol 1"

---

## Legacy Scripts (Phase 1 -- Vols 11-57)

These were used for the initial extraction of later volumes and are mostly complete. Not actively used for Phase 2:

`extract_articles.py`, `extract_articles_v2.py`, `extract_by_page.py`, `extract_volume.py`, `extract_with_toc.py`, `parse_toc.py`, `parse_clean_toc.py`, `qc_extraction.py`, `qc_and_fix_volume.py`, `pdf_audit.py`, `pdf_verify.py`, `misc_cleanup.py`, `misc_extract.py`, and various `vol50`/`vol51`-specific scripts.
