# Boundary Bleed Post-Processing (2026-03-17)

## Problem

Extracted article files in `regex_and_llm/` and `R_RS_article_extracted/` suffered from **boundary bleed**: content from adjacent magazine sections (other articles, lesson departments, ads, biographical blurbs) was appended to the end of article files. The upstream extraction pipeline (`extract_vol*.py`) already has 8 trim strategies, but ~14% of files still contained bleed.

## What was done

Created and ran `preprocessing/postprocess_trim_bleed.py` -- a post-processing script that scans all extracted `.txt` files and trims trailing content that doesn't belong to the article.

### Results

| Metric | Value |
|--------|-------|
| Files scanned | 12,886 |
| Files trimmed | ~1,812 |
| Characters removed | ~11.1M |

### Detection strategies (in `find_bleed_point()`, line 188)

The script uses 7 strategies, each scanning for a different bleed signal. All strategies share guards against false positives: prose-context rejection, self-title matching via filename, and narrative-continuity checks.

**Strategy 1 -- Section headers** (~1,344 files). OCR-tolerant regexes for recurring magazine sections:

```python
# preprocessing/postprocess_trim_bleed.py lines 20-37
_SECTION_HEADERS = [
    re.compile(r'Notes?\s+(?:TO|FROM|...)...', re.IGNORECASE),  # Notes From/To the Field
    re.compile(r'(?:F|St)rom\s+Near\s+(?:and|...)...'),          # From Near and Far
    re.compile(r'Sixty\s+Years\s+Ago', ...),
    re.compile(r"(?:W|V)oman.s\s+(?:Sp|S)here", ...),           # Woman's Sphere
    ...
]
_LESSON_HEADERS = [  # Visiting Teacher Messages, Homemaking, etc.
    ...
]
```

**Strategy 2 -- ALL-CAPS headings** (multi-word caps lines that aren't the article's own title or a photo caption). Rejects self-references by comparing against words extracted from the filename:

```python
# lines 131-165
def _extract_article_title(filename):
    """Derive title words from filename like '05_Symphony_of_the_Snow.txt'."""
    ...
def _matches_article_title(candidate_text, title_words):
    """True if >50% of candidate words overlap with the article's own title."""
    ...
```

Also rejects photo captions via `_text_continues_after()` (line 168) which checks whether the narrative continues in lowercase after the heading.

**Strategy 3 -- Title + Author blocks** (~75 files). Two patterns: explicit "Title By Author" and "Title\nAuthor" on separate lines. Heavy filtering rejects non-person "author" names (wards, months, common nouns):

```python
# lines 299-314
if re.search(r'\b(?:Ward|Stake|Branch|Mission|Store|Books?|Church|...)\b',
             author_candidate):
    continue
```

**Strategy 4 -- Biographical intros** (e.g. "MRS. HILDA ANDERSSON ERICKSON, one hundred and three years old..."). Checks that the bio subject isn't the article's own subject.

**Strategy 5 -- "List of..." sections** (2 files).

**Strategy 6 -- Ad/commercial markers** (~345 files). Known advertiser names (BENEFICIAL LIFE, L.D.S. Business College, ZCMI, etc.). Trims at the last sentence boundary before the ad.

**Strategy 7 -- Name+bio blocks** (~20 files). ALL-CAPS person names followed by biographical verbs ("is a well-known...", "was the first..."). Includes a mid-line variant for 3+ word names with strong biographical openings. Skips names overlapping with article title or containing non-person words.

### False-positive guards (shared across strategies)

```python
# Prose context: reject if preceded by mid-sentence words
def _is_prose_context(text, pos):       # line 125

# Self-reference: reject if match overlaps with filename-derived title
def _matches_article_title(...)         # line 153

# Narrative continuity: reject if text continues lowercase after match
def _text_continues_after(text, pos):   # line 168
```

### Usage

```bash
# Preview without modifying files
python3 preprocessing/postprocess_trim_bleed.py --dry-run

# Run for real (idempotent -- safe to re-run)
python3 preprocessing/postprocess_trim_bleed.py

# Verbose mode shows skipped detections
python3 preprocessing/postprocess_trim_bleed.py --dry-run -v
```

## Files

- Script: `preprocessing/postprocess_trim_bleed.py`
- Upstream extraction (for context): `preprocessing/relief-society-mag/article-extraction/extract_vol*.py`
