# ============================================================
# API Post-Processing: Patch R extraction gaps with API fixes
# Stage 3 of the pipeline: R extraction → QC → API patches
# ============================================================
#
# This script reads the QC report from the R extractor, identifies
# problematic articles (undersized, empty, high ad density), and
# replaces them with clean text from API-transcribed page files.
#
# Usage: Rscript api_postprocess.R
# Requires: vol51_clean.R to have been run first (output in output/Vol51_1964/)
# ============================================================

library(stringr)
library(readr)
library(jsonlite)

# ============================================================
# CONFIGURATION
# ============================================================
VOLUME   <- 51L
YEAR     <- 1964L
BASE_DIR <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
OUTPUT_DIR  <- file.path(BASE_DIR, "output", paste0("Vol", VOLUME, "_", YEAR))
API_DIR     <- file.path(BASE_DIR, "api_fixes", paste0("vol", VOLUME))
API_RESULTS <- file.path(API_DIR, "_api_results.json")

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

# Thresholds for identifying problem files
UNDERSIZED_THRESHOLD <- 500L    # chars — files smaller than this are candidates
HIGH_AD_THRESHOLD    <- 5.0     # ad score density per 1K chars
MIN_API_IMPROVEMENT  <- 1.5     # API text must be at least 1.5x the R text to replace

cat(sprintf("API Post-Processing — Vol%d (%d)\n", VOLUME, YEAR))
cat(sprintf("Output dir: %s\n", OUTPUT_DIR))
cat(sprintf("API dir: %s\n\n", API_DIR))

# ============================================================
# STEP 1: Build API page index
# Maps (month, magazine_page) -> API file path
# ============================================================

cat("--- Step 1: Building API page index ---\n")

api_index <- list()  # key = "Month_PageNum", value = file path

# Scan all API fix files (non-ocr, non-json)
api_files <- list.files(API_DIR, pattern = "^vol\\d+_[A-Za-z]+_p\\d+\\.txt$",
                        full.names = TRUE)
cat(sprintf("  Found %d API fix files\n", length(api_files)))

# Parse filename to extract month and page: vol51_January_p002.txt
for (af in api_files) {
  bn <- basename(af)
  m <- str_match(bn, "^vol\\d+_(\\w+)_p(\\d+)\\.txt$")
  if (is.na(m[1, 1])) next
  month_name <- m[1, 2]
  mag_page <- as.integer(m[1, 3])
  key <- paste0(month_name, "_", mag_page)
  api_index[[key]] <- af
}

# Also parse each API file header to find all pages it covers
# Format: "--- PAGE N (PDF page M) ---"
api_page_coverage <- list()  # key = "Month_PageNum", value = file path (for every page covered)

for (af in api_files) {
  bn <- basename(af)
  m <- str_match(bn, "^vol\\d+_(\\w+)_p(\\d+)\\.txt$")
  if (is.na(m[1, 1])) next
  month_name <- m[1, 2]

  content <- tryCatch(read_file(af), error = function(e) "")
  if (nchar(content) < 50) next

  # Find all page markers
  page_markers <- str_match_all(content, "---\\s*PAGE\\s+(\\d+)")[[1]]
  if (nrow(page_markers) > 0) {
    for (pi in seq_len(nrow(page_markers))) {
      covered_page <- as.integer(page_markers[pi, 2])
      cov_key <- paste0(month_name, "_", covered_page)
      api_page_coverage[[cov_key]] <- af
    }
  }
}

cat(sprintf("  API page coverage: %d unique pages mapped\n\n", length(api_page_coverage)))

# ============================================================
# STEP 2: Read CONTENTS files to get page assignments
# Maps each article file to its CONTENTS page number
# ============================================================

cat("--- Step 2: Reading CONTENTS for page mapping ---\n")

article_pages <- list()  # key = filename, value = page number

month_dirs <- sort(list.dirs(OUTPUT_DIR, recursive = FALSE))
for (mdir in month_dirs) {
  month_name <- basename(mdir)
  contents_file <- list.files(mdir, pattern = "_CONTENTS\\.txt$", full.names = TRUE)
  if (length(contents_file) == 0) next

  contents <- tryCatch(read_file(contents_file[1]), error = function(e) "")
  if (nchar(contents) < 10) next

  # Get all article files in this month
  txt_files <- sort(list.files(mdir, pattern = "\\.txt$"))
  art_files <- txt_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", txt_files)]

  for (af in art_files) {
    # Extract page number from filename: V51_01_13_SpFt_The_New_Foundation.txt
    # The page number is in the CONTENTS, not the filename
    # Instead, match the article title words against CONTENTS entries
    # Simpler: extract the sequence number and match by order
    article_pages[[paste0(month_name, "/", af)]] <- NA_integer_
  }
}

# ============================================================
# STEP 3: Identify problem files and patch with API content
# ============================================================

cat("\n--- Step 3: Patching problem files with API content ---\n")

patch_count <- 0L
skip_count <- 0L
total_checked <- 0L

for (mdir in month_dirs) {
  month_name <- basename(mdir)

  txt_files <- sort(list.files(mdir, pattern = "\\.txt$", full.names = TRUE))
  art_files <- txt_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", txt_files)]

  for (af in art_files) {
    content <- tryCatch(read_file(af), error = function(e) "")
    fsize <- nchar(content)
    total_checked <- total_checked + 1L

    # Determine if this file needs patching
    needs_patch <- FALSE
    reason <- ""

    # Check 1: Undersized
    if (fsize < UNDERSIZED_THRESHOLD) {
      needs_patch <- TRUE
      reason <- sprintf("undersized (%d chars)", fsize)
    }

    # Check 2: High ad density
    if (!needs_patch && fsize > 100) {
      ad_score <- 0L
      ad_check <- str_sub(content, 1L, min(1000L, fsize))
      ad_score <- ad_score + str_count(ad_check, "\\$\\d+\\.\\d{2}") * 3L
      ad_score <- ad_score + str_count(ad_check, "\\d{3}[-.]?\\d{4}") * 3L
      ad_score <- ad_score + str_count(ad_check, regex("(?:Write|Send)\\s+(?:for|to)", ignore_case=TRUE)) * 2L
      ad_score <- ad_score + str_count(ad_check, "\\bFREE\\b") * 2L
      ad_density <- ad_score / (nchar(ad_check) / 1000)
      if (ad_density > HIGH_AD_THRESHOLD) {
        needs_patch <- TRUE
        reason <- sprintf("high ad density (%.1f/1K)", ad_density)
      }
    }

    if (!needs_patch) next

    # Extract the article's page from bracket header
    # Format: [Vol51 | January 1964 | Page 25 | Category | Title | Author]
    page_match <- str_match(content, "\\| Page\\s+(\\d+)\\s*\\|")
    art_page <- if (!is.na(page_match[1, 2])) as.integer(page_match[1, 2]) else NA_integer_

    # Detect if this is a poem (from filename or bracket header)
    is_poem <- grepl("_Poem_", basename(af)) ||
               str_detect(content, regex("\\| Poet(ry|ic)", ignore_case = TRUE))

    # If no bracket header, try to find page from CONTENTS
    if (is.na(art_page)) {
      # Read CONTENTS and match by title
      contents_file <- list.files(mdir, pattern = "_CONTENTS\\.txt$", full.names = TRUE)
      if (length(contents_file) > 0) {
        cont_text <- tryCatch(read_file(contents_file[1]), error = function(e) "")
        # Extract title from filename: V51_01_13_SpFt_The_New_Foundation.txt
        fn_parts <- str_match(basename(af), "V\\d+_\\d+_\\d+_[^_]+_(.+)\\.txt$")
        if (!is.na(fn_parts[1, 2])) {
          title_words <- str_replace_all(fn_parts[1, 2], "_", " ")
          # Search for title words in CONTENTS to find page
          sig_words <- str_extract_all(title_words, "[A-Za-z]{4,}")[[1]]
          if (length(sig_words) >= 1) {
            cont_lines <- str_split(cont_text, "\n")[[1]]
            for (cl in cont_lines) {
              hits <- sum(sapply(sig_words[1:min(3, length(sig_words))],
                function(w) str_detect(cl, fixed(w, ignore_case = TRUE))))
              if (hits >= 1) {
                # Try page-at-start format: "25 Title Author"
                pg_m <- str_match(cl, "^(\\d{1,3})\\s+")
                if (!is.na(pg_m[1, 2])) {
                  art_page <- as.integer(pg_m[1, 2])
                  break
                }
                # Try page-at-end format (poetry): "Title, by Author, 49"
                pg_end <- str_match(cl, ",\\s*(\\d{1,3})\\.?\\s*$")
                if (!is.na(pg_end[1, 2])) {
                  art_page <- as.integer(pg_end[1, 2])
                  break
                }
              }
            }
          }
        }
      }
    }

    if (is.na(art_page)) {
      cat(sprintf("  SKIP: %s/%s — %s, but no page number found\n",
                  month_name, basename(af), reason))
      skip_count <- skip_count + 1L
      next
    }

    # Find API files covering this page and nearby pages
    # Collect all candidate API files (may be multiple due to off-by-one page numbering)
    candidate_api_files <- character(0)
    for (offset in c(0, -1, 1, -2, 2)) {
      cand_key <- paste0(month_name, "_", art_page + offset)
      cand_file <- api_page_coverage[[cand_key]]
      if (!is.null(cand_file) && !(cand_file %in% candidate_api_files)) {
        candidate_api_files <- c(candidate_api_files, cand_file)
      }
    }

    if (length(candidate_api_files) == 0) {
      cat(sprintf("  NO API: %s/%s — %s (pg %d), no API coverage\n",
                  month_name, basename(af), reason, art_page))
      skip_count <- skip_count + 1L
      next
    }

    # Build title search pattern from article title
    art_title_for_search <- ""
    title_hdr_pre <- str_match(content, "\\|\\s*([^|]+?)\\s*\\|\\s*([^|\\]]+?)\\s*\\]")
    if (!is.na(title_hdr_pre[1, 2])) {
      art_title_for_search <- str_trim(title_hdr_pre[1, 2])
    }
    if (nchar(art_title_for_search) < 3) {
      fn_p <- str_match(basename(af), "V\\d+_\\d+_\\d+_[^_]+_(.+)\\.txt$")
      if (!is.na(fn_p[1, 2])) {
        art_title_for_search <- str_replace_all(fn_p[1, 2], "_", " ")
      }
    }
    title_search_words <- str_extract_all(art_title_for_search, "[A-Za-z]{4,}")[[1]]

    # Search each candidate API file for the best title match
    best_api_file <- NULL
    best_title_pos <- NA_integer_
    for (caf in candidate_api_files) {
      caf_content <- tryCatch(read_file(caf), error = function(e) "")
      if (nchar(caf_content) < 50) next
      if (length(title_search_words) >= 2) {
        sw <- title_search_words[1:min(3, length(title_search_words))]
        tpat <- paste(sw, collapse = "\\s+.*?")
        tloc <- str_locate(caf_content, regex(tpat, ignore_case = TRUE))
        if (!is.na(tloc[1, 1])) {
          if (is.null(best_api_file) || is.na(best_title_pos) || tloc[1, 1] < best_title_pos) {
            best_api_file <- caf
            best_title_pos <- tloc[1, 1]
          }
        }
      }
      # If no title match yet, keep first file as fallback
      if (is.null(best_api_file)) {
        best_api_file <- caf
      }
    }

    api_file <- best_api_file

    # Extract text for this article's page(s) from API file
    api_content <- tryCatch(read_file(api_file), error = function(e) "")
    if (nchar(api_content) < 50) next

    # Split API file by page markers
    page_sections <- str_split(api_content, "---\\s*PAGE\\s+\\d+\\s*\\(PDF page \\d+\\)\\s*---")[[1]]
    page_nums <- as.integer(str_match_all(api_content, "---\\s*PAGE\\s+(\\d+)")[[1]][, 2])

    if (length(page_nums) == 0) next

    # Find the section for our article's page
    # Allow ±1 page offset for the page match (off-by-one is common)
    max_pages <- if (is_poem) 1L else 2L
    target_sections <- character(0)
    for (si in seq_along(page_nums)) {
      if (page_nums[si] >= (art_page - 1L) && page_nums[si] < (art_page + max_pages)) {
        if (si + 1 <= length(page_sections)) {
          target_sections <- c(target_sections, str_trim(page_sections[si + 1]))
        }
      }
    }

    # If still no match, use the entire API file content (title matching will narrow it)
    if (length(target_sections) == 0 && !is.na(best_title_pos)) {
      # Title was found in this file — just use from the title position onward
      start_at_raw <- max(1L, best_title_pos - 50L)
      target_sections <- c(str_trim(str_sub(api_content, start_at_raw)))
    }

    if (length(target_sections) == 0) {
      cat(sprintf("  NO MATCH: %s/%s — %s (pg %d), API file has pages %s\n",
                  month_name, basename(af), reason, art_page,
                  paste(page_nums, collapse = ",")))
      skip_count <- skip_count + 1L
      next
    }

    api_text_raw <- paste(target_sections, collapse = "\n\n")

    # --- Title/author matching: extract only the relevant article's portion ---
    # Extract title words from the bracket header or filename
    art_title <- ""
    title_hdr <- str_match(content, "\\|\\s*([^|]+?)\\s*\\|\\s*([^|\\]]+?)\\s*\\]")
    if (!is.na(title_hdr[1, 2])) {
      art_title <- str_trim(title_hdr[1, 2])
    }
    if (nchar(art_title) < 3) {
      fn_parts <- str_match(basename(af), "V\\d+_\\d+_\\d+_[^_]+_(.+)\\.txt$")
      if (!is.na(fn_parts[1, 2])) {
        art_title <- str_replace_all(fn_parts[1, 2], "_", " ")
      }
    }

    # Try to find article title in API text to extract just the right section
    api_text <- api_text_raw
    if (nchar(art_title) >= 4) {
      title_sig_words <- str_extract_all(art_title, "[A-Za-z]{4,}")[[1]]
      if (length(title_sig_words) >= 2) {
        # Build a flexible regex from the first 3 significant words
        search_words <- title_sig_words[1:min(3, length(title_sig_words))]
        flex_pat <- paste(search_words, collapse = "\\s+.*?")
        title_loc <- str_locate(api_text_raw, regex(flex_pat, ignore_case = TRUE))
        if (!is.na(title_loc[1, 1])) {
          # Found the title — start from a bit before it (capture any heading)
          start_at <- max(1L, title_loc[1, 1] - 50L)
          api_text <- str_trim(str_sub(api_text_raw, start_at))
          cat(sprintf("    → Title match: found '%s' at position %d in API text\n",
                      art_title, title_loc[1, 1]))
        }
      }
    }

    # Poem sanity check: poems are typically short (< 2000 chars)
    # If API text is very long for a poem, something went wrong
    if (is_poem && nchar(api_text) > 2000) {
      cat(sprintf("  SKIP: %s/%s — poem but API text too large (%d chars), likely wrong content\n",
                  month_name, basename(af), nchar(api_text)))
      skip_count <- skip_count + 1L
      next
    }

    # Check if API text is meaningfully better
    if (nchar(api_text) < nchar(content) * MIN_API_IMPROVEMENT) {
      cat(sprintf("  SKIP: %s/%s — API text (%d chars) not enough improvement over R text (%d chars)\n",
                  month_name, basename(af), nchar(api_text), fsize))
      skip_count <- skip_count + 1L
      next
    }

    # Patch: replace R-extracted content with API content
    # Preserve the bracket header(s) if present
    header <- ""
    hdr_lines <- str_extract_all(content, "\\[[^\\]]+\\]")[[1]]
    if (length(hdr_lines) > 0) {
      header <- paste0(paste(hdr_lines, collapse = "\n"), "\n\n")
    }

    patched <- paste0(header, "[API-PATCHED: original ", fsize, " chars → ",
                      nchar(api_text), " chars]\n\n", api_text)
    write_file(patched, af)
    patch_count <- patch_count + 1L
    cat(sprintf("  PATCHED: %s/%s — %s (pg %d): %d → %d chars\n",
                month_name, basename(af), reason, art_page,
                fsize, nchar(api_text)))
  }
}

cat(sprintf("\n--- Summary ---\n"))
cat(sprintf("Total articles checked: %d\n", total_checked))
cat(sprintf("Files patched with API content: %d\n", patch_count))
cat(sprintf("Files skipped (no API / no improvement): %d\n", skip_count))
cat(sprintf("API post-processing complete.\n"))
