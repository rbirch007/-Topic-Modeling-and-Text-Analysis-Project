# apply_api_merge.R - Page-based API merge (all volumes)
# Works with correctly-labeled API files (v2 pipeline output)
# Usage: Rscript apply_api_merge.R <volume> <year>
#   e.g.: Rscript apply_api_merge.R 53 1966
library(stringr)
library(readr)

# Parse command line args (or use defaults for interactive use)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 2) {
  VOLUME <- as.integer(args[1])
  YEAR <- as.integer(args[2])
} else {
  # Default - change these for interactive use
  VOLUME <- 52L
  YEAR <- 1965L
  cat("No args provided, using defaults: Vol", VOLUME, "(", YEAR, ")\n")
  cat("Usage: Rscript apply_api_merge.R <volume> <year>\n\n")
}

BASE_DIR <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
OUTPUT_DIR <- file.path(BASE_DIR, "output", paste0("Vol", VOLUME, "_", YEAR))
API_DIR <- file.path(BASE_DIR, "api_fixes", paste0("vol", VOLUME))

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

cat(sprintf("=== Page-Based API Merge - Vol%d (%d) ===\n", VOLUME, YEAR))

# STEP 1: Build (month, page)->text map from API files
# Key includes month to handle volumes with per-issue pagination (Vols 53-57)
# where multiple months can have the same page number
cat("--- Step 1: Building page text map ---\n")
api_page_text <- list()
api_files <- list.files(API_DIR, pattern = "^vol\\d+_[A-Za-z]+_p\\d+\\.txt$", full.names = TRUE)
cat(sprintf("  Found %d API files\n", length(api_files)))

for (af in api_files) {
  content <- tryCatch(read_file(af), error = function(e) "")
  if (nchar(content) < 50) next
  if (grepl("QC:\\s*FAIL", content)) next

  # Extract month from filename: vol52_February_p160.txt -> February
  fname <- basename(af)
  file_month <- str_match(fname, "^vol\\d+_([A-Za-z]+)_p\\d+\\.txt$")[1, 2]
  if (is.na(file_month)) next

  page_nums <- as.integer(str_match_all(content, "---\\s*PAGE\\s+(\\d+)")[[1]][, 2])
  page_sections <- str_split(content, "---\\s*PAGE\\s+\\d+\\s*\\(PDF page \\d+\\)\\s*---")[[1]]

  for (pi in seq_along(page_nums)) {
    if (pi + 1 <= length(page_sections)) {
      page_text <- str_trim(page_sections[pi + 1])
      if (nchar(page_text) > 10) {
        # Use month_page as key to avoid collisions across months
        key <- paste0(file_month, "_", page_nums[pi])
        if (is.null(api_page_text[[key]]) || nchar(page_text) > nchar(api_page_text[[key]])) {
          api_page_text[[key]] <- page_text
        }
      }
    }
  }
}

all_keys <- names(api_page_text)
if (length(all_keys) == 0) {
  cat("  No API pages found. Nothing to merge.\n")
  quit(save = "no")
}
cat(sprintf("  Page text map: %d unique (month, page) entries\n\n", length(api_page_text)))

# STEP 2: Process each month
total_replaced <- 0L
total_kept <- 0L

for (mi in seq_along(MONTHS)) {
  month_name <- MONTHS[mi]
  month_dir <- file.path(OUTPUT_DIR, month_name)
  if (!dir.exists(month_dir)) next
  cat(sprintf("\n=== %s ===\n", month_name))

  # Get the page range for this month's API files
  month_prefix <- paste0(month_name, "_")
  month_keys <- all_keys[str_starts(all_keys, month_prefix)]
  month_api_pages <- sort(as.integer(str_replace(month_keys, paste0("^", month_name, "_"), "")))

  txt_files <- sort(list.files(month_dir, pattern = "\\.txt$"))
  art_files <- txt_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", txt_files)]
  if (length(art_files) == 0) next

  n <- length(art_files)
  art_page <- rep(NA_integer_, n)
  art_content <- rep("", n)
  art_body_len <- rep(0L, n)
  art_is_poem <- rep(FALSE, n)

  for (i in seq_len(n)) {
    filepath <- file.path(month_dir, art_files[i])
    art_content[i] <- tryCatch(read_file(filepath), error = function(e) "")
    pg_m <- str_match(art_content[i], "\\|\\s*Page\\s+(\\d+)\\s*\\|")
    if (!is.na(pg_m[1, 2])) art_page[i] <- as.integer(pg_m[1, 2])
    art_is_poem[i] <- grepl("_Poem_|_Frnt_", art_files[i])
    body <- str_replace_all(art_content[i], "\\[[^\\]]+\\]\\s*", "")
    art_body_len[i] <- nchar(str_trim(body))
  }

  # Determine end page for each article from the next article's start page
  for (i in seq_len(n)) {
    if (is.na(art_page[i])) {
      total_kept <- total_kept + 1L
      cat(sprintf("  KEPT (no page): %s\n", art_files[i]))
      next
    }

    # Find end page: next article's page - 1, or max API page for this month
    end_page <- if (length(month_api_pages) > 0) max(month_api_pages) else art_page[i]
    if (i < n) {
      for (j in (i+1):n) {
        if (!is.na(art_page[j])) {
          end_page <- art_page[j] - 1L
          break
        }
      }
    }

    if (art_is_poem[i]) {
      end_page <- min(end_page, art_page[i] + 1L)
    }

    # Guard: end_page must be >= start page
    if (end_page < art_page[i]) {
      total_kept <- total_kept + 1L
      cat(sprintf("  KEPT (reversed range): %s (page %d, next starts %d)\n",
                  art_files[i], art_page[i], end_page + 1L))
      next
    }

    # Look up API text using month-scoped keys
    page_texts <- character(0)
    pages_found <- integer(0)
    for (pg in art_page[i]:end_page) {
      key <- paste0(month_name, "_", pg)
      if (!is.null(api_page_text[[key]])) {
        page_texts <- c(page_texts, api_page_text[[key]])
        pages_found <- c(pages_found, pg)
      }
    }

    if (length(page_texts) == 0) {
      total_kept <- total_kept + 1L
      cat(sprintf("  KEPT (no API): %s (pages %d-%d)\n", art_files[i], art_page[i], end_page))
      next
    }

    merged_text <- paste(page_texts, collapse = "\n\n")

    if (nchar(merged_text) < 20) {
      total_kept <- total_kept + 1L
      cat(sprintf("  KEPT (too short): %s (%d chars)\n", art_files[i], nchar(merged_text)))
      next
    }

    header_lines <- str_extract_all(art_content[i], "\\[[^\\]]+\\]")[[1]]
    header <- ""
    if (length(header_lines) > 0) {
      header <- paste0(paste(header_lines, collapse = "\n"), "\n\n")
    }
    patched <- paste0(header, merged_text, "\n")

    filepath <- file.path(month_dir, art_files[i])
    write_file(patched, filepath)
    total_replaced <- total_replaced + 1L

    ratio <- nchar(merged_text) / max(1L, art_body_len[i])
    flag <- ""
    if (ratio < 0.1 && art_body_len[i] > 100) flag <- " [WARN: shrunk >90%]"
    if (ratio > 5 && art_body_len[i] > 100) flag <- " [WARN: grew >5x]"

    cat(sprintf("  MERGED: %s (pages %d-%d, %d API pages, %d->%d chars)%s\n",
                art_files[i], art_page[i], end_page,
                length(pages_found), art_body_len[i], nchar(merged_text), flag))
  }
}

cat("\n============================================================\n")
cat("SUMMARY\n")
cat("============================================================\n")
cat(sprintf("Articles replaced with API text: %d\n", total_replaced))
cat(sprintf("Articles kept (R extraction):    %d\n", total_kept))
cat("\nDone.\n")
