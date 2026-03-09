# Relief Society Magazine Volume Extraction — Autonomous Prompt

## Purpose
This document is a self-contained prompt for extracting articles from any Relief Society Magazine volume. A fresh Claude Code context should be able to read this file and execute the full extraction for a target volume without human intervention.

## Usage
Tell Claude Code:
```
Read VOLUME_EXTRACTION_PROMPT.md and follow its instructions to extract Volume NN.
```

**This task is designed to run completely autonomously without any user prompts or confirmations. All necessary permissions are pre-granted in the "Allowed Operations" section below.**

---

## Allowed Operations (No Confirmation Needed)
**IMPORTANT: This extraction should run COMPLETELY AUTONOMOUSLY with NO user prompts.**

All of the following operations are pre-approved and should never prompt for confirmation:
- **Read tool**: Read any file under the project directory (including all cleaned-data/, preprocessing/, processed/ paths)
- **Grep tool**: Search file contents anywhere in the project with any pattern, output mode, or parameters
- **Glob tool**: Find files by pattern anywhere in the project
- **Write tool**: Write new files under `preprocessing/relief-society-mag/article-extraction/` and `processed/regex_and_llm/`
- **Edit tool**: Edit files under `preprocessing/relief-society-mag/article-extraction/`
- **Bash tool**:
  - Run `python3` commands (scripts, one-liners, diagnostics)
  - Run `/bin/ls`, `/bin/rm -rf` on output directories under `processed/regex_and_llm/`
  - Run `git` commands if explicitly requested
- **Task tool**: Launch subagents (general-purpose, Explore) for TOC extraction and OCR pattern research
- Delete and recreate output directories under `processed/regex_and_llm/volNN/` during iterative runs

**When launching Task subagents**: The subagents inherit these permissions and should also run without prompts. They have full access to Read, Grep, and Glob tools across all project files.

## Tool Usage Rules
- **NEVER** use Bash for file reading or searching. Use the dedicated tools instead:
  - Read files: use the **Read** tool (not `cat`, `head`, `tail`, `awk`, `sed`)
  - Search file contents: use the **Grep** tool (not `grep`, `rg`, `awk`)
  - Find files: use the **Glob** tool (not `find`, `ls`)
  - Check file sizes or line counts: use **Read** tool or `python3 -c` one-liners
- The **only** Bash commands you should run are:
  - `python3` (to run extraction scripts or quick one-liner diagnostics)
  - `/bin/ls` and `/bin/rm -rf` (for output directory management)
  - `git` commands (if explicitly requested)

---

## Volume-Year Mapping
Each volume corresponds to a calendar year. The formula is: **year = volume_number + 1913**

| Vol | Year | Vol | Year | Vol | Year |
|-----|------|-----|------|-----|------|
| 30  | 1943 | 40  | 1953 | 50  | 1963 |
| 31  | 1944 | 41  | 1954 | 51  | 1964 |
| 32  | 1945 | 42  | 1955 | 52  | 1965 |
| 33  | 1946 | 43  | 1956 | 53  | 1966 |
| 34  | 1947 | 44  | 1957 | 54  | 1967 |
| 35  | 1948 | 45  | 1958 | 55  | 1968 |
| 36  | 1949 | 46  | 1959 | 56  | 1969 |
| 37  | 1950 | 47  | 1960 | 57  | 1970 |
| 38  | 1951 | 48  | 1961 |     |      |
| 39  | 1952 | 49  | 1962 |     |      |

Already processed: vol30-vol38. Remaining: vol39-vol57.

## File Locations

### Source files
```
cleaned-data/relief-society/txtvolumesbymonth/VolNN/VolNN_No01_January_YYYY.txt
cleaned-data/relief-society/txtvolumesbymonth/VolNN/VolNN_No02_February_YYYY.txt
... (12 files, one per month)
cleaned-data/relief-society/txtvolumesbymonth/VolNN/VolNN_No12_December_YYYY.txt
```

### Template script (ALWAYS USE THIS AS THE BASE)
```
preprocessing/relief-society-mag/article-extraction/extract_vol38.py
```

### Output script
```
preprocessing/relief-society-mag/article-extraction/extract_volNN.py
```

### Output data
```
processed/regex_and_llm/volNN/{Month}/          # individual .txt files per article
processed/regex_and_llm/volNN/volNN_entries.json # full volume JSON
processed/regex_and_llm/volNN/flagged_for_review.json
```

---

## STEP-BY-STEP PROCEDURE

### STEP 1: Read the template script
Read `extract_vol38.py` in full. Understand every section:
- `VOLNN_TOC` dict: keyed by `("VolNN", "NoMM_Month_YYYY")`, each value is a list of entry dicts
- `ISSUE_FILES`: maps issue keys to `(filename, month_name)` tuples
- `_OCR_WORD_START_ALTS`, `_OCR_SINGLE_CHAR_ALTS`: OCR character substitution tables
- `_KNOWN_HEADER_PATTERNS`: handcrafted regex for recurring mangled section headers
- Serial fiction chapter patterns (volume-specific)
- `build_regex_for_title()`, `_build_regex_chars()`: the regex builder
- `split_front_matter()`, `strip_running_noise()`, `find_ads_section()`
- `extract_issue()`: the main per-issue pipeline
- `main()`: CLI entry point with JSON/manifest/flagged output

**Newline warning:** The template script may contain newline-dependent regex anchors (`^`, `$`, or patterns that assume `\n` between sections). Note these, but do NOT replicate them in new extraction scripts. The OCR source text has unreliable newline placement — newlines may or may not appear between sections, so any regex that depends on their presence will silently fail on some issues. See "Never Rely on Newlines" under CRITICAL THINGS TO WATCH FOR.

### STEP 2: Extract TOC data from all 12 source files
Launch a Task subagent (general-purpose) to read the first ~150 lines of each of the 12 source files and extract every TOC entry. The TOC appears near the beginning of each file, grouped by sections.

**Prompt for the subagent:**
```
Read the first 150 lines of each of these 12 files to extract the Table of Contents:
[list all 12 file paths]

For each month, extract every article with: title (exact, strip page numbers), author, section heading.
Include serial fiction chapters. Note any OCR artifacts. Be thorough — don't skip entries.
```

**etype mapping from TOC sections:**
- SPECIAL FEATURES → `"article"`
- FICTION / SHORT STORIES / SERIAL → `"fiction"`
- GENERAL FEATURES → `"article"` (except Editorial → `"editorial"`)
- LESSON DEPARTMENT (any variant) → `"lesson"`
- FEATURES FOR THE HOME → `"article"`
- POETRY → `"poem"`

**TOC entry rules:**
- Strip quote marks from poetry titles that appear in quotes
- Strip page numbers from titles
- Omit compound/multi-item "Notes to the Field" entries (those listing multiple items with semicolons) — too complex to match
- Include the frontispiece poem (usually the first poem listed, marked "Frontispiece")
- Include serial fiction chapters with their chapter numbers
- For titles with smart quotes in the TOC, use straight quotes or omit them in the dict

### STEP 3: Research OCR patterns in body text
Launch a Task subagent (general-purpose) to grep the source files for how recurring section headers appear in the OCR'd body text.

**Prompt for the subagent:**
```
Search the body text (after "PUBLISHED MONTHLY BY THE GENERAL BOARD") of at least
6 of the 12 source files for these recurring patterns. Report the EXACT text found:

1. "Theology" / "Sheology" — what series title follows? What lesson numbering format?
2. "Visiting Teacher Messages" — all OCR variants of this header
3. "EDITORIAL" — what noise precedes/follows? What format: VOL. NN MONTH YEAR?
4. "Literature" — how is the series title formatted?
5. "Social Science" — OCR variants
6. "Work Meeting" — OCR variants
7. "Notes From the Field" / "From The Field" — does "Notes" appear or not?
8. "From Near and Far" — does this header exist in body text?
9. Serial fiction chapter headers — exact format (CHAPTER N, etc.)
10. "Sixty Years Ago" — any corruption?
11. "Music:" — OCR variants of "Music"
12. Any other section headers unique to this volume

Report the exact OCR'd text so regex patterns can be built accurately.

IMPORTANT: Do not assume newlines separate sections. The OCR text may run
sections together without line breaks. Report whether headers appear on their
own line or are run together with surrounding text.
```

### STEP 4: Create the extraction script

**CRITICAL — Never rely on newlines:** The OCR source files have unreliable newline placement. When writing regex patterns for the new script:
- Do NOT use `^` or `$` anchors to find section boundaries or titles
- Do NOT assume `\n` separates sections, headers, or titles from body text
- Use `\s+` or `\s*` to bridge whitespace (which covers both spaces and newlines) rather than requiring newlines
- TOC title entries must never contain literal `\n`. If a title accidentally reads `"Moby\nDick"`, the regex built from it must not require that newline — use `\s+` or `\s*` between words instead
- Patterns like `r"Moby\s+Dick"` are correct; patterns like `r"Moby.*?Dick"` are not (`.` does not match `\n` by default, and the match is too greedy)
- If the template script (`extract_vol38.py`) uses `^`, `$`, or newline-dependent patterns, replace them with newline-agnostic equivalents in the new script

Copy `extract_vol38.py` as the base. Change ONLY these things:

1. **Docstring**: Update volume number and year
2. **`VOLNN_TOC` dict**: Replace with new TOC data from Step 2. Key format: `("VolNN", "NoMM_Month_YYYY")`
3. **`ISSUE_FILES` loop**: Change `year = YYYY` and `Vol38` → `VolNN`
4. **`_KNOWN_HEADER_PATTERNS`**: Update based on OCR research from Step 3:
   - Update the Theology series title pattern (changes every 1-2 years)
   - Update the Visiting Teacher Messages series title pattern
   - Update the Literature series title if it changed
   - Update the Social Science series and part titles if changed
   - Update the Work Meeting series title if changed
   - Update the Music series title if changed
   - Update the EDITORIAL noise pattern (VOL. NN changes)
   - **Remove old serial fiction patterns** and add new ones for this volume's serials
   - Keep the OCR character alternations — they are consistent across volumes
5. **Serial fiction chapter patterns**: Each volume has different serials. Create `_SERIAL_NAME_CHAPTER_PAT` patterns for each serial in this volume. The pattern format is:
   ```python
   _SERIAL_CHAPTER_PAT = (
       r"Title\s+Words\s+Here"
       r".{0,80}?"  # skip author name
       r"(?:(?:CHAPTER|Chapter)\s+)?"  # CHAPTER may be absent in some months
   )
   ```
   Then add entries like:
   ```python
   "Serial Title, Chapter 1": _SERIAL_CHAPTER_PAT + r"(?:1|I)\b",
   ```
6. **`main()` function**: Update description string and `VOL37_TOC` → `VOLNN_TOC` reference
7. **`source_rel_path`**: Update in `extract_issue()` to `VolNN`

### STEP 5: First run and verify
```bash
python3 preprocessing/relief-society-mag/article-extraction/extract_volNN.py 2>&1
```

**Check:**
- All 12 months produce output
- No Python errors/exceptions
- Match rates per month (target >80% per issue, >90% overall)
- Coverage percentages (target >90% per issue)

### STEP 6: Diagnose unmatched entries
Collect the unmatched entries from the output. For the most impactful ones (appearing in multiple months, or important content types like lessons/editorials), launch a Task subagent to search the body text for what's actually there.

**Common root causes and fixes:**

| Problem | Symptom | Fix |
|---------|---------|-----|
| TOC title differs from body title | Editorial/article not found | Change the title in the TOC dict to match body text |
| Section header absent from body | "From Near and Far", "Notes From the Field" | Accept as unfixable — header was dropped by OCR |
| OCR prefix on word | `fRepentance`, `(Desert` | The `f?` prefix and `\(?` in `_build_regex_chars` handle most cases |
| "CHAPTER N" absent | Serial fiction not found | Make CHAPTER optional in pattern: `(?:(?:CHAPTER\|Chapter)\s+)?` |
| New OCR artifact | Word garbled in new way | Add alternation to `_KNOWN_HEADER_PATTERNS` or `_OCR_WORD_START_ALTS` |
| Extra words in body | "God the Eternal Father and" vs "God and" | Shorten the TOC title to match what's reliably in body, or add a custom known-header pattern |
| Poem title garbled | `Fill` for `Hill`, etc. | Accept or add a specific known-header entry for that title |

### STEP 7: Apply fixes and re-run
After diagnosing:
1. Fix TOC entries (wrong titles, missing entries)
2. Update regex patterns for new OCR artifacts
3. Clean output directory: `/bin/rm -rf processed/regex_and_llm/volNN/`
4. Re-run the script
5. Repeat until match rates stabilize (diminishing returns after 2-3 iterations)

### STEP 8: Final verification
```bash
# Verify output structure
/bin/ls processed/regex_and_llm/volNN/

# Verify JSON schema matches vol38
python3 -c "
import json
with open('processed/regex_and_llm/vol38/vol38_entries.json') as f:
    v38 = json.load(f)
with open('processed/regex_and_llm/volNN/volNN_entries.json') as f:
    vNN = json.load(f)
v38_e = v38['months']['January']['entries'][0]
vNN_e = vNN['months']['January']['entries'][0]
print('Top keys match:', sorted(v38.keys()) == sorted(vNN.keys()))
print('Entry keys match:', sorted(v38_e.keys()) == sorted(vNN_e.keys()))
print('Match keys match:', sorted(v38_e['match'].keys()) == sorted(vNN_e['match'].keys()))
print('Months:', list(vNN['months'].keys()))
for m, md in vNN['months'].items():
    print(f'  {m}: {len(md[\"entries\"])} entries')
"

# Verify flagged file exists
/bin/ls -la processed/regex_and_llm/volNN/flagged_for_review.json
```

### STEP 9: Report results
Print a summary table showing:
- Per-month match counts and coverage percentages
- Total matched entries and overall coverage
- Number of flagged entries
- Any entries that remain unmatched and why

---

## CRITICAL THINGS TO WATCH FOR

### Never Rely on Newlines
The OCR source text files have **unreliable newline placement**. Newlines may or may not appear between sections, between a header and its body, or between title words. Therefore:
- **Never** use `^` or `$` regex anchors to locate section boundaries or titles
- **Never** assume `\n` exists between a section header and the text that follows
- **Never** place literal `\n` in TOC title entries in the Python dict
- **Always** use `\s+` or `\s*` to bridge whitespace between words in regex patterns — this matches both spaces and newlines without requiring either
- A pattern like `r"Moby\s+Dick"` is correct. A pattern like `r"Moby.*?Dick"` is wrong (`.` does not match `\n` by default, making the match fragile in a different way)

This applies to all regex construction: `_KNOWN_HEADER_PATTERNS`, `build_regex_for_title()` output, serial fiction chapter patterns, and any ad-hoc patterns added during debugging.

### OCR Patterns That Change Between Volumes
The OCR artifacts are caused by decorative/large-font initial letters in the printed magazine. These are **mostly consistent** across volumes 36-38 but may shift:
- `Th` → `Sh` or `Ch` (very consistent)
- `T` → `S`, `J`, or `(` (very consistent)
- `F` → `St` (very consistent)
- `M` in "Music"/"Meeting"/"Messages" → wildly variable (`IT`, `Nl`, `Vl`, etc.)
- Stray `f` prefix before capitalized words
- Stray `(` before words

**For each new volume, you MUST verify these patterns still apply.** The OCR quality may improve in later volumes (1960s+) or get worse.

### Lesson Department Changes
The lesson series titles change every 1-2 years:
- **Theology**: Series title changes (e.g., "The Life and Ministry of the Savior" → "Characters and Teachings of the Book of Mormon"). The body has `Sheology—SERIES TITLE Lesson NN—SUBTITLE`. The known-header pattern must skip the series title to reach the subtitle from the TOC.
- **Visiting Teacher Messages**: Same pattern — series title changes.
- **Literature**: Usually "The Literature of England" but may change.
- **Social Science**: Series title and part numbers change.
- **Work Meeting**: Series title changes (e.g., "The Art of Homemaking" → "Sewing: The Art of Mending").
- **Music**: Usually "Fundamentals of Musicianship" but may change.

The July issue typically contains **lesson previews** for the coming year, which means the series titles change mid-volume (Jan-Jun use the old series, Jul-Dec use the new one). The known-header patterns must be loose enough to match BOTH series titles, or you need separate patterns.

### Editorial Title Mismatch
The editorial title in the TOC sometimes differs from the actual body text. The body text is authoritative. If an editorial doesn't match:
1. Search the body for `EDITORIAL`
2. Read the actual title after the `VOL. NN MONTH YYYY` noise
3. Update the TOC dict to use the body title
4. Beware of `Sh` for `Th` in the body title (e.g., "She Old and the New" = "The Old and the New")

### "From Near and Far" Section
This section header is **frequently absent from body text** (the content exists but has no heading). It matches in only ~2 of 12 months typically. Accept this as an inherent limitation.

### "Notes From the Field" / "Notes to the Field"
In vol38, the body header was often just `From The Field` (missing "Notes" prefix). The pattern has been updated to make "Notes" optional. Verify this still works for the target volume. The subtitle from the TOC (e.g., "Relief Society Socials, Bazaars, and Other Activities") is usually absent from the body header too — the known-header pattern should only match the header, not the subtitle.

### Serial Fiction
Each volume typically has 1-2 serial novels running across multiple issues. Identify them from the TOC (they have "Chapter N" designations). Key points:
- Chapter numbering may be Arabic (1, 2, 3) or Roman (I, II, III) in the body
- Some chapters lack "CHAPTER N" entirely — just the title + author
- The last chapter may say "(Conclusion)" instead of a chapter number
- Create separate `_SERIAL_CHAPTER_PAT` for each serial

### Front Matter Splitting
The script splits on `"PUBLISHED MONTHLY BY THE GENERAL BOARD"`. This marker is present in all volumes 36-38. If it's absent in a new volume, the script will raise an exception. Check for OCR variants like `"ISHED MONTHLY"` or `"MONTHLY BY THE GENERAL BOARD"` (already handled).

### Manifest CSV
The script writes `processed/regex_and_llm/manifest.csv`. This gets **overwritten** each run with ONLY the current volume's data. This is expected behavior — the manifest is per-run, not cumulative.

---

## THINGS NOT TO DO
- Do NOT add features, refactor, or "improve" the script architecture
- Do NOT change the output JSON schema
- Do NOT change file naming conventions
- Do NOT attempt to fix poems/articles whose titles are completely absent from body text — flag them and move on
- Do NOT spend more than 3 iterations fixing regex patterns — diminishing returns
- Do NOT modify existing extraction scripts for other volumes
- Do NOT push to git or commit — just create the script and output files

---

## EXAMPLE: What the final output should look like

After a successful run for volume NN:
```
processed/regex_and_llm/volNN/
├── January/
│   ├── 01_Title_Here.txt
│   ├── 02_Another_Title.txt
│   ├── ...
│   ├── ADS.txt
│   ├── MISC.txt
│   └── TOC.txt
├── February/
│   └── ...
├── ... (all 12 months)
├── volNN_entries.json
└── flagged_for_review.json
```

Console output should show:
```
SUMMARY
============================================================
Issues processed: 12
Total entries matched: 250-350 (varies by volume)
Overall coverage: 95-99%
Total misc bytes: <10% of total
```

Target metrics:
- All 12 months produce output directories
- >80% match rate per month
- >90% overall coverage
- JSON schema identical to vol38
