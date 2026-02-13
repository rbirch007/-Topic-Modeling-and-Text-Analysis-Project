Processed files are ready to be used as inputs to Topic Modeling.

## JSON Schema (VolNN_entries.json)

One JSON file per volume lives in its volume folder. Each file contains
all months for that volume and is the authoritative record of every extracted
entry. Text files on disk are redundant with the JSON and exist solely for
human review; they will eventually be deleted.

### Top-level structure

```json
{
  "volume": "Vol32",
  "months": {
    "March": { ... },
    "April": { ... }
  }
}
```

### Month object

```json
{
  "source_file": "Vol32_No3_March_1945.txt",
  "source_path": "cleaned-data/relief-society/txtvolumesbymonth/Vol32",
  "entries": [ ... ],
  "toc":  { "file": "TOC.txt",  "path": "processed/Vol32/March", "content": "..." },
  "ads":  { "file": "ADS.txt",  "path": "processed/Vol32/March", "content": "..." },
  "misc": { "file": "MISC.txt", "path": "processed/Vol32/March", "content": "..." }
}
```

- `source_file` / `source_path`: The OCR-cleaned input file that was parsed.
- `toc`: Table of contents extracted from front matter. Null if not found.
- `ads`: Advertising content from the tail of the issue. Null if not found.
- `misc`: Remaining unmatched text (front matter minus TOC, inter-entry gaps,
  stripped noise fragments). Null if empty.

### Entry object

Each entry represents one article, poem, editorial, lesson, etc.

```json
{
  "index": 1,
  "title": "The Relief Society in Church Welfare",
  "author": "Marion G. Romney",
  "etype": "article",
  "strict_loose_identical": false,
  "strict_match": {
    "file": "01_strict_The_Relief_Society_in_Church_Welfare.txt",
    "path": "processed/Vol32/March",
    "position": 1234,
    "length": 4444,
    "content": "The Relief Society In Church Welfare..."
  },
  "loose_match": {
    "file": "01_loose_The_Relief_Society_in_Church_Welfare.txt",
    "path": "processed/Vol32/March",
    "position": 1200,
    "length": 2256,
    "content": "The Relief Society In Church Welfare..."
  }
}
```

### Field definitions

| Field | Description |
|-------|-------------|
| `index` | 1-based ordinal position within the issue, determined by where the title match appears in the source text (sorted by character offset). Used in filenames. |
| `title` | Entry title as it appears in the table of contents. |
| `author` | Author name. Null if none listed. Present in JSON only, not in filenames. |
| `etype` | Entry type classification: article, poem, editorial, fiction, lesson, report, letter, obituary, front_matter, misc, ads. |
| `strict_loose_identical` | Boolean. True when both matching strategies produced identical text content. |
| `strict_match` | Result from line-start matching (title must appear at the beginning of a line). Null if no match found. |
| `loose_match` | Result from anywhere-matching (title can appear mid-text). Null if no match found. |
| `position` | Character offset (0-based) in the source file where the match begins. |
| `length` | Character count of the raw extracted slice before noise removal. |
| `content` | Noise-stripped text. Running headers, mailing statements, and section labels are removed. |
| `file` | Filename on disk. Author is not included in filenames. |
| `path` | Path from project root to the containing folder. |

### Matching Strategies: Strict vs Loose

The extraction script finds article boundaries by searching for TOC titles
in the body text of each issue. This is done with two independent strategies,
and both results are preserved for every entry.

**Why two strategies exist:**

Magazine entries are extracted by using their titles as boundary markers. The
script escapes each TOC title into a regex and searches for it in the issue
text. The position of each title match becomes a fence post; the text between
consecutive fence posts is attributed to the earlier entry. This works well
when titles are distinctive phrases, but breaks down when a title is a common
word or short phrase that also appears in the running prose of another article.

**The split-content problem:**

When an article titled "Gifts" (a poem) appears in the TOC, and the word
"gifts" also appears mid-sentence in the body of a preceding article ("The
Story of a Gifted Lady"), the regex matches the mid-sentence occurrence first.
This splits the preceding article at an arbitrary point mid-sentence, and the
"Gifts" entry captures only the tail end of someone else's article rather than
the actual poem. The same problem occurs with titles like "Released", "Work
Meeting", "The Relief Society President", and any other title that happens to
be a phrase used in ordinary English prose.

This is a **known, recurring issue** that will likely appear in every volume
of the magazine, not just Volumes 30-32. The frequency depends on how many
TOC titles use common words or phrases. Short poem titles are the worst
offenders.

**Strict matching** (line-start):

The title must appear at the beginning of a line in the source text. The regex
requires the match to be preceded by a newline (or be at position 0). This
reflects the physical magazine layout: real article headings are typeset on
their own line, not buried mid-paragraph. Because OCR preserves line breaks
from the original page layout, a genuine title will almost always start a new
line in the cleaned text.

Strict matching **reduces false splits** dramatically, because a word like
"gifts" appearing mid-sentence will not be at the start of a line. However,
strict matching can **miss real entries** if OCR merged the title into the
preceding line, or if the title genuinely appears inline (e.g., a short poem
title on the same line as the author name).

**Loose matching** (anywhere):

The title can appear anywhere in the text, including mid-sentence. This is the
original, simpler strategy. It finds more entries but is vulnerable to the
split-content problem described above.

**How to use both during review:**

- When `strict_loose_identical` is `true`, both strategies found the same
  boundary. The entry is almost certainly correct.
- When `strict_loose_identical` is `false`, the two strategies disagree.
  Compare the two output files to determine which one captured the right
  content. Common patterns:
  - Strict is null but loose found something: the title matched mid-sentence
    (likely a false split). Check whether loose captured a real entry or just
    a fragment of the preceding article.
  - Both found matches at different positions: strict is usually correct
    (matched the real heading), while loose matched an earlier occurrence in
    running prose.
- When both are null, the title was not found in the body text at all (likely
  an OCR discrepancy between the TOC and the body).

**For future volumes:**

Expect this problem to recur. When extending the script to new volumes, watch
for WARNING messages about entries that only matched loosely or not at all.
Titles that are single common words (e.g., "Faith", "Hope", "Spring") or
short phrases (e.g., "A Prayer", "In Memoriam") are the most likely to cause
false splits. The strict/loose dual output exists specifically to make these
problems visible and reviewable.

### Noise removal

The following patterns are stripped from entry content before storage:

- Running headers: `RELIEF SOCIETY MAGAZINE` with optional month/year
- Section labels: `LESSON DEPARTMENT`
- Mailing statement: `Entered as second-class matter...authorized June 29, 1918.`
- `Stamps should accompany manuscripts for their return.`

Stripped fragments are preserved in the MISC file for that issue.

### Filename convention

Text files use the pattern: `{index}_{strategy}_{title}.txt`

Examples:
- `01_strict_The_Relief_Society_in_Church_Welfare.txt`
- `01_loose_The_Relief_Society_in_Church_Welfare.txt`
- `TOC.txt`
- `ADS.txt`
- `MISC.txt`

Volume and month are encoded in the directory path, not repeated in filenames.

### Flagged for review (flagged_for_review.json)

One file per volume, located alongside the entries JSON at
`processed/VolNN/flagged_for_review.json`. This file lists entries that are
likely false splits caused by the split-content problem described above.

**Detection method — title not at start:**

For each extracted entry, the script checks whether the entry's own title
appears in the first 200 characters of its content. If it does not, the entry
is flagged. The reasoning: when an entry is correctly extracted, its content
begins with its title (the heading). When a title matches mid-sentence inside
a preceding article, the extracted "content" starts with a sentence fragment
from the wrong article — the title appears somewhere in the middle, not at the
start.

Example from Vol32/March: "The Relief Society President" (index 04) matched
the phrase "the Relief Society president" mid-sentence inside the body of
"The Relief Society in Church Welfare" (index 03). The content of entry 04
starts with lowercase "the Relief Society president have been described as the
father and the mother of the ward" — a sentence fragment, not an article
heading. Because the title is not at the start of the content, it gets flagged.

**Flagged entry fields:**

| Field | Description |
|-------|-------------|
| `title` | The TOC title. |
| `author` | Author name, if any. |
| `etype` | Entry type. |
| `index` | 1-based position within the issue. |
| `month` | Month name. |
| `strategy` | `strict` or `loose` — which match triggered the flag. |
| `file` | Filename on disk. |
| `path` | Path to containing folder. |
| `position` | Character offset in source file. |
| `length` | Character count of raw extracted slice. |
| `content` | The noise-stripped content (for inspection). |
| `strict_loose_identical` | Whether both strategies agreed for this entry. |
| `title_not_at_start` | Always `true` in this file. |

**How to use during review:**

Entries in this file are strong candidates for false splits. For each flagged
entry, check the preceding entry (index - 1) in the same month — the two
likely belong together. The flagged entry's content is probably a fragment of
the preceding article, while the real entry with that title may appear later
in the text (captured correctly by strict matching) or may have been missed
entirely.

### Directory layout

```
processed/
  manifest.csv
  Vol32/
    Vol32_entries.json
    flagged_for_review.json
    March/
      01_strict_The_Relief_Society_in_Church_Welfare.txt
      01_loose_The_Relief_Society_in_Church_Welfare.txt
      02_strict_Easter_Time.txt
      02_loose_Easter_Time.txt
      ...
      TOC.txt
      ADS.txt
      MISC.txt
```
