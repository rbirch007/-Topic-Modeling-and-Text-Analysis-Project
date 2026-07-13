# Clean Articles

Cleaned, per-article plain-text files extracted from two periodical corpora used in this
topic-modeling and text-analysis project. Each `.txt` file is a single article (or discrete
item such as a poem, editorial, contents page, or masthead).

## Structure

```
Clean Articles/
├── RSM/   Relief Society Magazine — Vol 1 (1914) … Vol 18 (1931)
└── WE/    Woman's Exponent — all volumes (Vol 1 … Vol 41)
            + WE_article_inventory_vols1-41.xlsx
```

### RSM/ — Relief Society Magazine
- Volumes `Vol1_1914` through `Vol18_1931` (18 volumes).
- Folder names encode volume number and year, e.g. `Vol10_1923`.
- Within each volume, articles are organized into month subfolders (January, February, …).
- File names follow the pattern `<Month>_Vol<N>_<seq>_<Type>_<Title>_<Author>.txt`
  (e.g. `April_Vol18_02a_Article_Women_in_Organization_Work.txt`).

### WE/ — Woman's Exponent
- Per-article text files grouped by volume folder `WEVol1` … `WEVol41`.
- `WE_article_inventory_vols1-41.xlsx` — an inventory spreadsheet cataloging the WE articles
  across volumes 1–41.

## Notes on scope

- All files are UTF-8 plain text (`.txt`), one article per file.
- **Excluded:** internal pipeline backup folders (`_backups_split`, `_backups_standardize`,
  `_backups_shortfix`, `_backups_cleanup`, `_backups_deleted`, `_backups_merged_extract`)
  that were present in the RSM source. These held intermediate/older working copies and are
  not part of the clean corpus.
- **Excluded:** the large combined `sorted_articles.txt` dump from the WE source; the
  per-article files and the inventory spreadsheet are provided instead.
