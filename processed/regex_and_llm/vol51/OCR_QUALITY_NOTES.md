# Vol 51 OCR Quality Notes

## Current State

The cleaned source files for Volume 51 (1964) were processed through the LLM-based OCR cleanup pipeline (90.5% average quality score). Extraction achieves **229 of ~425 entries matched** (~54%) with 27 flagged for review.

## Remaining OCR Issues Limiting Extraction

### 1. Running Headers Embedded in Text

Page headers like `THE TURN OF THE WHEEL` and issue identifiers like `AUGUST 1964` are OCR'd directly into the article body text rather than appearing on separate lines. This pollutes extracted articles and occasionally causes false boundary matches.

### 2. Column-Merge Artifacts

Two-column magazine layouts cause OCR to interleave text from adjacent columns. This destroys article titles, author bylines, and lesson headers — the exact features the extraction script relies on for boundary detection. Examples from Vol 51:

- `"Wherefore, Be Not Christine H. Robinson For First Meeting, April 1964 Weary in Well-Doing"` — author name and date spliced into the middle of a lesson title
- `Visiting Teacher CMhersissatignee —H. R"oLbeianvseo n Judgment Alone With Me` — complete garble of author + title
- `Bailey Joyce B. Don and Rita drove in silence` — reversed author name merged into story opening

### 3. Destroyed Headings

Decorative or large-font headings are rendered as garbage: `wsdtye e CHAPTER 1` instead of the article title and chapter heading. When the extraction script can't find a title, the article either goes unmatched (absorbed into MISC) or gets incorrect boundaries.

### 4. Drop-Cap and Decorative Initial Errors

Large initial letters at article openings are misread: `\NYONE` for `ANYONE`, `AAMoNG` for `Among`, `AX` for `A`. These are worked around in the extraction script with OCR-tolerant regex patterns but add fragility.

## Impact on Extraction

These issues account for the majority of the ~196 unmatched entries. The extraction script uses increasingly complex regex patterns and heuristic scoring to work around OCR damage, but column-merge artifacts in particular cannot be fixed by pattern matching alone.

## Recommended Next Steps

A targeted post-processing pass on the cleaned source files could address issues 1 and 4 (running headers, drop-caps). Issues 2 and 3 (column merge, destroyed headings) would require either re-OCR from original scans with a layout-aware OCR engine, or manual correction of the affected pages.
