# Relief Society Magazine: Extracted Output Formats by Volume

## Format overview

There are three distinct format eras. The existing topic modeling corpus (`corpus.jsonl`) only ingests from the `regex_and_llm` pipeline (Vols 30-57). To process all volumes with one codebase, the R_RS output needs to be mapped to the same JSONL schema.

## corpus.jsonl target schema (what topic modeling expects)

```json
{
  "doc_id":     "vol30_January_01",
  "volume":     "vol30",
  "month":      "January",
  "title":      "Messages for the New Year",
  "author":     null,
  "etype":      "editorial",
  "year":       1943,
  "word_count": 2181,
  "text":       "...full article text..."
}
```

Filtering: minimum 50 words; excluded types: toc, ads, misc.

---

## Per-volume format table

| Volumes | Pipeline | Dir naming | Month dirs | File naming | Metadata source | etype taxonomy | Year source |
|---------|----------|-----------|------------|-------------|-----------------|----------------|-------------|
| **1-3** (1914-1916) | R_RS | `Vol1-1914`, `Vol2-1915`, `Vol3-1916` | Title case (`January`, `February`) | `Month_VolN_NN_Type_Title_Author.txt` | filename only | Article, Poetry, Editorial, Frontispiece, Misc, Contents | dir name |
| **13-16** (1926-1929) | R_RS | `Vol13-1926` ... `Vol16-1929` | Title case | `Month_VolNN_NN_Type_Title_Author.txt` | filename only | same as above | dir name |
| **17** (1930) | R_RS | `Vol17_1930` (underscore) | Title case | `Month_VolNN_NoN_NN_Type_Title_Author.txt` (adds issue No) | filename only | same | dir name |
| **18** (1931) | R_RS | `Vol18-1931` | **`Vol18-Jan1931`** (different!) | `Month_Vol18_ArticleNN_Title_Author.txt` (different pattern) | filename only | same | dir name |
| **19-28** (1932-1941) | R_RS | `Vol19_1932` ... `Vol28_1941` (underscore) | Title case | `Month_VolNN_NoN_NN_Type_Title_Author.txt` | filename only | same + BOARD, CONTENTS | dir name |
| **30, 32, 34, 36** | regex_and_llm | `Vol30`, `Vol32`, `Vol34`, `Vol36` (capital V) | Title case (+ `JuneJuly`) | `NN_Title.txt` | `manifest.csv` | poem, article, fiction, editorial, lesson, toc, ads, misc | manifest |
| **31, 33, 35, 37-57** | regex_and_llm | `vol31`, `vol33` ... `vol57` (lowercase v) | Title case (+ `JuneJuly`) | `NN_Title.txt` | `manifest.csv` | same as above | manifest |

### Notes

- **Vol 4-12 and Vol 29 are missing** from both pipelines.
- **Vol 18** has a unique sub-structure: month dirs are `Vol18-Jan1931` instead of just `January`, and files use `ArticleNN` instead of `NN_Type`.
- The `regex_and_llm` pipeline has a single `manifest.csv` at its root with columns: `file, path, volume, month, etype, title, author, strategy`.
- The R_RS pipeline has **no manifest** — all metadata is encoded in filenames.

---

## Harmonization gaps (what blocks single-codebase ingestion)

| Field | regex_and_llm (30-57) | R_RS (1-28) | Gap |
|-------|----------------------|-------------|-----|
| `doc_id` | derivable from manifest (`vol30_January_01`) | must parse from filename | need parser |
| `volume` | in manifest, lowercase (`vol33`) | in dirname, mixed format (`Vol19_1932`) | normalize to `volNN` |
| `month` | in manifest, Title case | in dirname or filename, Title case (except Vol18) | normalize Vol18 |
| `title` | in manifest | embedded in filename (underscores = spaces) | parse from filename |
| `author` | in manifest (may be empty) | embedded in filename suffix (may be absent) | parse from filename |
| `etype` | in manifest (`poem`, `article`, `fiction`, `editorial`, `lesson`) | in filename (`Article`, `Poetry`, `Editorial`, etc.) | map: `Article`→`article`, `Poetry`→`poem`, etc. |
| `year` | derivable from volume number | in dirname (`Vol19_1932` → 1932) | parse from dirname |
| `text` | read .txt file | read .txt file | same |

### etype mapping needed

| R_RS filename type | → corpus.jsonl etype |
|--------------------|---------------------|
| `Article` | `article` |
| `Poetry` / `Poem` | `poem` |
| `Editorial` | `editorial` |
| `Frontispiece` | exclude or `article` |
| `Notes_from_Field` | `article` |
| `CONTENTS` | exclude |
| `BOARD` | exclude |
| `FRONT_MATTER` | exclude |
| `Misc` | exclude |

### Filename parsing patterns (R_RS)

```
Vols 1-3, 13-16:   {Month}_Vol{N}_{NN}_{Type}_{Title}_{Author}.txt
Vol 17, 19-28:      {Month}_Vol{NN}_No{N}_{NN}_{Type}_{Title}_{Author}.txt
Vol 18:             {Month}_Vol18_Article{NN}_{Title}_{Author}.txt
```

### Special files to exclude (both pipelines)

| regex_and_llm | R_RS |
|---------------|------|
| `TOC.txt` | `*_00_CONTENTS.txt` |
| `ADS.txt` | (none) |
| `MISC.txt` | `*_Misc.txt`, `*_Misc_front_matter.txt`, `*_Misc_back_matter.txt` |
| (none) | `*_00_BOARD.txt` |
| (none) | `*_00_FRONT_MATTER.txt` |
