# ============================================================
# FIX TRUNCATED — Extend the 123 genuinely truncated articles
# Uses the input text to find the continuation and append it.
# Stops at the next known article boundary.
# ============================================================

library(stringr)
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
INPUT_BASE <- file.path(BASE, "input")
OUTPUT_BASE <- file.path(BASE, "OutputExtractedarticles")

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

read_file_safe <- function(path) {
  tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) tryCatch(
      read_file(path, locale=locale(encoding="windows-1252")),
      error=function(e2) ""))
}

# Read classification results
class_df <- read.csv(file.path(BASE, "endcut_classification.csv"), stringsAsFactors = FALSE)
truncated <- class_df[class_df$classification == "TRUNCATED", ]

cat(sprintf("Fixing %d truncated articles...\n\n", nrow(truncated)))

input_cache <- list()
fixed <- 0
skipped <- 0

for (i in seq_len(nrow(truncated))) {
  row <- truncated[i, ]
  vol <- row$volume
  month <- row$month
  fname <- row$filename
  year <- vol + 1913

  # Read article
  art_path <- file.path(OUTPUT_BASE, sprintf("Vol%d_%d", vol, year), month, fname)
  if (!file.exists(art_path)) { skipped <- skipped + 1; next }
  art_text <- read_file_safe(art_path)
  art_text_trimmed <- str_trim(art_text)

  # Get input text
  cache_key <- sprintf("V%d_%s", vol, month)
  if (is.null(input_cache[[cache_key]])) {
    input_preproc <- file.path(INPUT_BASE, sprintf("Vol%dsplit_preprocessed", vol))
    input_raw <- file.path(INPUT_BASE, sprintf("Vol%dsplit", vol))
    input_dir <- if (dir.exists(input_preproc)) input_preproc else input_raw
    input_files <- list.files(input_dir, pattern = sprintf("(?i)%s.*\\.txt$", month), full.names = TRUE)
    if (length(input_files) == 0) {
      month_num <- sprintf("%02d", which(MONTHS == month))
      input_files <- list.files(input_dir, pattern = sprintf("(?i)No%s|%s", month_num, month), full.names = TRUE)
    }
    if (length(input_files) > 0) input_cache[[cache_key]] <- read_file_safe(input_files[1])
    else input_cache[[cache_key]] <- ""
  }
  input_text <- input_cache[[cache_key]]
  if (nchar(input_text) < 100) { skipped <- skipped + 1; next }

  # Find article ending in input
  last_30 <- str_sub(art_text_trimmed, max(1, nchar(art_text_trimmed) - 29), nchar(art_text_trimmed))
  esc <- str_replace_all(last_30, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
  esc <- str_replace_all(esc, "\\s+", "\\\\s+")

  m <- tryCatch(str_locate(input_text, regex(esc, ignore_case = FALSE)),
                error = function(e) matrix(NA, 1, 2))
  if (is.na(m[1,1])) {
    m <- tryCatch(str_locate(input_text, regex(esc, ignore_case = TRUE)),
                  error = function(e) matrix(NA, 1, 2))
  }
  if (is.na(m[1,1])) { skipped <- skipped + 1; next }

  # Read continuation from input (up to 10KB max)
  after_pos <- m[1,2] + 1
  max_read <- min(after_pos + 10000, nchar(input_text))
  continuation <- str_sub(input_text, after_pos, max_read)

  # Find where to stop: look for the start of the NEXT extracted article
  # Get next article file in the same month
  month_dir <- file.path(OUTPUT_BASE, sprintf("Vol%d_%d", vol, year), month)
  all_arts <- sort(list.files(month_dir, pattern = sprintf("^V%d_", vol)))
  all_arts <- all_arts[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", all_arts)]
  my_idx <- which(all_arts == fname)

  stop_pos <- nchar(continuation)  # default: take everything

  if (length(my_idx) > 0 && my_idx < length(all_arts)) {
    # Try to find next article's start text in the continuation
    for (next_idx in (my_idx + 1):min(my_idx + 3, length(all_arts))) {
      next_art <- read_file_safe(file.path(month_dir, all_arts[next_idx]))
      next_art <- str_trim(next_art)
      # Remove header
      if (str_detect(next_art, "^\\s*\\[.+\\]")) {
        next_art <- str_trim(str_replace(next_art, "^\\s*\\[.+?\\]\\s*", ""))
      }
      if (nchar(next_art) < 20) next

      # Take first 60 chars of next article as stop marker
      next_start <- str_sub(next_art, 1, min(60, nchar(next_art)))
      next_esc <- str_replace_all(next_start, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      next_esc <- str_replace_all(next_esc, "\\s+", "\\\\s+")

      next_m <- tryCatch(str_locate(continuation, regex(next_esc, ignore_case = FALSE)),
                         error = function(e) matrix(NA, 1, 2))
      if (!is.na(next_m[1,1])) {
        stop_pos <- next_m[1,1] - 1
        break
      }
    }
  }

  # Also check for ad content in the continuation
  ad_pats <- c("\\$\\d+\\.\\d{2}", "Salt Lake City.{0,20}Utah",
               "(?i)\\border now\\b", "(?i)\\bDeseret Book\\b", "(?i)\\bZCMI\\b")
  for (ap in ad_pats) {
    ad_m <- tryCatch(str_locate(continuation, ap), error = function(e) matrix(NA, 1, 2))
    if (!is.na(ad_m[1,1]) && ad_m[1,1] < stop_pos) {
      # Walk back to sentence end before ad
      pre_ad <- str_sub(continuation, max(1, ad_m[1,1] - 200), ad_m[1,1])
      sent_ends <- str_locate_all(pre_ad, "[.!?][\"')\\]]?\\s")[[1]]
      if (nrow(sent_ends) > 0) {
        stop_pos <- min(stop_pos, max(1, ad_m[1,1] - 200) + sent_ends[nrow(sent_ends), 2])
      } else {
        stop_pos <- min(stop_pos, ad_m[1,1] - 1)
      }
      break
    }
  }

  # Extract continuation text
  added_text <- str_trim(str_sub(continuation, 1, stop_pos))

  if (nchar(added_text) < 5) { skipped <- skipped + 1; next }

  # Append to article
  new_text <- paste0(art_text_trimmed, " ", added_text)
  write_file(new_text, art_path)

  cat(sprintf("  FIXED: [V%d %s] %s (+%d chars)\n", vol, month, fname, nchar(added_text)))
  fixed <- fixed + 1
}

cat(sprintf("\n=== COMPLETE: %d fixed, %d skipped ===\n", fixed, skipped))
