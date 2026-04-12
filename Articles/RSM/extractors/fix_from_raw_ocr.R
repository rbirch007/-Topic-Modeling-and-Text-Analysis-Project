# ============================================================
# FIX FROM RAW OCR — Try to fix remaining END_CUT articles
# using the raw OCR text (vol36.txt etc.) which is a different
# OCR run that may have text the preprocessed input missed.
# ============================================================

library(stringr)
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
RAW_OCR_DIR <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/pdftotextraw/output2"
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

# Read current classification
class_df <- read.csv(file.path(BASE, "endcut_classification.csv"), stringsAsFactors = FALSE)
# Focus on articles that still need fixing
needs_fix <- class_df[class_df$classification %in% c("TRUNCATED", "NEEDS_PDF_QC", "UNCLEAR", "UNCLEAR_NOT_FOUND"), ]

cat(sprintf("Attempting raw OCR fix for %d articles...\n\n", nrow(needs_fix)))

raw_cache <- list()
fixed <- 0

for (i in seq_len(nrow(needs_fix))) {
  row <- needs_fix[i, ]
  vol <- row$volume
  month <- row$month
  fname <- row$filename
  year <- vol + 1913

  # Read article
  art_path <- file.path(OUTPUT_BASE, sprintf("Vol%d_%d", vol, year), month, fname)
  if (!file.exists(art_path)) next
  art_text <- str_trim(read_file_safe(art_path))
  if (nchar(art_text) < 50) next

  # Check if it still ends mid-word
  last_char <- str_sub(art_text, nchar(art_text), nchar(art_text))
  if (!str_detect(last_char, "[a-zA-Z]")) next

  # Load raw OCR for this volume
  cache_key <- sprintf("vol%d", vol)
  if (is.null(raw_cache[[cache_key]])) {
    raw_path <- file.path(RAW_OCR_DIR, sprintf("vol%d.txt", vol))
    if (file.exists(raw_path)) {
      raw_cache[[cache_key]] <- read_file_safe(raw_path)
    } else {
      raw_cache[[cache_key]] <- ""
    }
  }
  raw_text <- raw_cache[[cache_key]]
  if (nchar(raw_text) < 100) next

  # Find article's last 25 chars in raw OCR
  last_25 <- str_sub(art_text, max(1, nchar(art_text) - 24), nchar(art_text))
  last_25 <- str_trim(last_25)
  if (nchar(last_25) < 10) next

  esc <- str_replace_all(last_25, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
  esc <- str_replace_all(esc, "\\s+", "\\\\s+")

  m <- tryCatch(str_locate(raw_text, regex(esc, ignore_case = FALSE)),
                error = function(e) matrix(NA, 1, 2))
  if (is.na(m[1,1])) {
    m <- tryCatch(str_locate(raw_text, regex(esc, ignore_case = TRUE)),
                  error = function(e) matrix(NA, 1, 2))
  }
  if (is.na(m[1,1])) next

  # Found! Read continuation
  after_pos <- m[1,2] + 1
  continuation <- str_sub(raw_text, after_pos, min(after_pos + 5000, nchar(raw_text)))

  # Find stop: next article file's content
  month_dir <- file.path(OUTPUT_BASE, sprintf("Vol%d_%d", vol, year), month)
  all_arts <- sort(list.files(month_dir, pattern = sprintf("^V%d_", vol)))
  all_arts <- all_arts[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", all_arts)]
  my_idx <- which(all_arts == fname)

  stop_pos <- nchar(continuation)
  if (length(my_idx) > 0 && my_idx < length(all_arts)) {
    for (ni in (my_idx+1):min(my_idx+3, length(all_arts))) {
      next_text <- str_trim(read_file_safe(file.path(month_dir, all_arts[ni])))
      if (str_detect(next_text, "^\\s*\\[.+\\]")) {
        next_text <- str_trim(str_replace(next_text, "^\\s*\\[.+?\\]\\s*", ""))
      }
      if (nchar(next_text) < 20) next
      next_start <- str_sub(next_text, 1, min(50, nchar(next_text)))
      next_esc <- str_replace_all(next_start, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      next_esc <- str_replace_all(next_esc, "\\s+", "\\\\s+")
      next_m <- tryCatch(str_locate(continuation, regex(next_esc, ignore_case=FALSE)),
                         error = function(e) matrix(NA, 1, 2))
      if (!is.na(next_m[1,1])) { stop_pos <- next_m[1,1] - 1; break }
    }
  }

  # Also stop at ad content
  ad_pats <- c("\\$\\d+\\.\\d{2}", "Salt Lake City", "(?i)\\bZCMI\\b", "(?i)\\border now\\b")
  for (ap in ad_pats) {
    ad_m <- tryCatch(str_locate(continuation, ap), error = function(e) matrix(NA, 1, 2))
    if (!is.na(ad_m[1,1]) && ad_m[1,1] < stop_pos) {
      pre <- str_sub(continuation, max(1, ad_m[1,1]-150), ad_m[1,1])
      se <- str_locate_all(pre, "[.!?]\\s")[[1]]
      if (nrow(se) > 0) stop_pos <- min(stop_pos, max(1, ad_m[1,1]-150) + se[nrow(se),2])
      else stop_pos <- min(stop_pos, ad_m[1,1]-1)
      break
    }
  }

  added <- str_trim(str_sub(continuation, 1, stop_pos))
  if (nchar(added) < 5) next

  # Don't add more than 5KB
  if (nchar(added) > 5000) added <- str_sub(added, 1, 5000)

  new_text <- paste0(art_text, " ", added)
  write_file(new_text, art_path)
  cat(sprintf("  FIXED: [V%d %s] %s (+%d chars from raw OCR)\n", vol, month, fname, nchar(added)))
  fixed <- fixed + 1
}

cat(sprintf("\n=== COMPLETE: %d fixed from raw OCR ===\n", fixed))
