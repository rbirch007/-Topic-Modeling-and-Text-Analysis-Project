# SESSION HANDOVER: Volumes 41-50 Extraction
## Relief Society Magazine Article Extraction Pipeline

**Date:** 2026-03-19
**Previous:** Vols 36-40 QC complete (44 articles remain for manual review)
**Next:** Vols 41-50 fresh extraction

---

## STEP-BY-STEP PIPELINE FOR VOLS 41-50

### Step 1: Preprocess Input Files
The R extractor expects files named `Vol{N}_No{MM}_{Month}_{Year}.txt` in `input/Vol{N}split_preprocessed/`.

Raw split files exist at `input/Vol{N}split/` but may need renaming. Check naming:
```
ls input/Vol41split/
```
If files aren't in the expected format, create preprocessed copies. Example target:
```
input/Vol41split_preprocessed/Vol41_No01_January_1954.txt
```

Volume years: 41=1954, 42=1955, 43=1956, 44=1957, 45=1958, 46=1959, 47=1960, 48=1961, 49=1962, 50=1963

### Step 2: Run R Extractor (one volume at a time)
Use `universal_run_extractor.Rmd` which calls `vol51_extractor_fresh.Rmd`.

Configure the Rmd with:
- `vol_num` — volume number
- `year` — volume year
- Input path → `input/Vol{N}split_preprocessed/`
- Output path → `OutputExtractedarticles/Vol{N}_{year}/`

**DO NOT use Python extractors** — R/Rmd works much better for this magazine.

### Step 3: Programmatic QC (FREE — no API)
Run these IN ORDER before touching the API:

1. **Delete old-format files** — If both `January_Vol41_*` and `V41_01_*` exist in same month, delete the old format (`{Month}_Vol{N}_*`)
2. **Ad cleanup** — `python ad_cleanup.py --fix` moves ad files to Misc
3. **Misc rescue** — `python misc_rescue.py --fix` restores article content incorrectly placed in Misc
4. **Check for duplicate article content in Misc files** — especially large Misc files (>5KB). If article text appears in both Misc AND a separate article file, remove it from Misc.

### Step 4: API Truncation Verify ($0.01-0.02 per article)
This catches articles with missing endings. Run in this order:

**Round 1: Text-match page finder + API verify**
```
python endcut_pdf_verify_v2.py --fix --vol {N}
```
- Uses PyMuPDF to find correct PDF page via text matching (FREE)
- Then sends page image to Claude API for verification
- Fixes ~50% of truncated articles
- Cost: ~$0.02/article × ~60 articles/vol ≈ $1.20/vol

**Round 2: CONTENTS-based lookup for remaining**
```
python endcut_contents_lookup.py --fix
```
- Uses CONTENTS page numbers to find the exact PDF page
- PDF page ≈ magazine page + 1 (offset varies ±2)
- Catches articles where OCR was too garbled for text matching

### Step 5: Manual Review
Remaining articles go to `pdf_audit/manual_review_list_vols1-20_52-57.txt`.
Use `python gen_review.py` to generate the checklist with CONTENTS page numbers.

---

## CRITICAL LESSONS LEARNED (from Vols 36-40)

### What worked
- R extractor (`vol51_extractor_fresh.Rmd`) is the best extraction tool
- PyMuPDF text search finds the right PDF page ~80% of the time for API verify
- CONTENTS page numbers are reliable for locating articles in PDF
- Running ad_cleanup and misc_rescue BEFORE API verify saves money

### What wasted time/money
- **Python extractors** — don't use them, R is better
- **Estimated page numbers** (article_num × 2 + month_offset) — terrible accuracy, waste of API calls
- **Broad PDF text search without month filtering** — too slow, still misses garbled OCR
- **Multiple API retry rounds on garbled OCR** — these articles can't be fixed by API, put them on manual review list immediately
- **Not cleaning up old-format files before QC** — causes duplicate detection issues

### Red flags to watch for
- **Misc files > 5KB** — likely contain article content that needs extraction
- **Files ending mid-word** — truncated, need API verify
- **Files with "(Continued on page XXX)"** — check that continuation text is included
- **Two naming formats in same month** — old format needs deletion
- **Article files that are 100% ad text** — move to Misc immediately

---

## DIRECTORY STRUCTURE

```
Articleextractionrfiles/
├── input/
│   ├── Vol41split/                    # Raw monthly breakout
│   ├── Vol41split_preprocessed/       # Standardized names (create if needed)
│   └── ...through Vol50split/
├── output/                            # Intermediate (pre-parsed)
├── OutputExtractedarticles/           # FINAL canonical output
│   └── Vol{N}_{year}/{Month}/V{N}_{MM}_{NN}_{Type}_{Title}_{Author}.txt
├── pdftotextraw/output2/vol{N}.txt    # Raw OCR per volume
├── pdf_audit/                         # QC reports and manual review lists
└── .api_key                           # Anthropic API key
```

**Output file naming:** `V{vol}_{month:02d}_{seq:02d}_{Type}_{TitleWords}_{Author}.txt`
- Types: Art, Fict, Poem
- Special: `_00_BOARD.txt`, `_00_CONTENTS.txt`, `_00_Misc.txt`

**PDF location:** `Womenprintculture/RSmagazine/reliefsocietymagvol{N}.pdf`
**Archive.org:** https://archive.org/details/reliefsocietymagazine

---

## GOALS

1. **Retain all text** — no content loss
2. **One whole article or poem per file** — no merged content
3. **Ads → Misc** — NO article/poem material in Misc
4. **Board, Misc, Contents** file per issue
5. **Articles in magazine order** within each issue
6. **Minimize API costs** — programmatic fixes first, API only for verification

---

## WHAT WAS DONE (Vols 36-40 QC Reference)

| Step | Result | Cost |
|------|--------|------|
| R extractor re-extraction | 1,143 files | $0 |
| Programmatic boundary fixes | 76 articles extended | $0 |
| PDF API verify (text-match) | 118 fixed, 88 complete | $3.80 |
| CONTENTS-based API lookup | 2 fixed, 1 complete | $0.88 |
| API page search (fallback) | 3 fixed, 3 complete | $1.07 |
| Misc rescue | 11 chunks restored | $0 |
| Ad cleanup | 4 ads moved to Misc | $0 |
| Manual fixes | 3 content issues | $0 |
| **Total** | **123 fixed, 92 complete** | **$5.75** |
| Remaining for manual review | 44 articles | — |

Manual review list: `pdf_audit/manual_review_list_vols1-20_52-57.txt` (Vols 36-40 section)

---

## API CONFIGURATION

- Key: `Articleextractionrfiles/.api_key`
- Model: `claude-sonnet-4-20250514`
- Dependencies: `anthropic`, `PyMuPDF` (both installed)
- PDF page offset: **PDF page ≈ magazine page + 1**
