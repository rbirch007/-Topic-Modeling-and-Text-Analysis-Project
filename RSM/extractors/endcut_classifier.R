# ============================================================
# END_CUT CLASSIFIER — Triage truncated articles
# Checks each END_CUT article against the input text to determine:
#   COMPLETE: Article text matches up to the next article's start
#   TRUNCATED: More article content exists in the input
#   UNCLEAR: Can't determine programmatically (needs PDF QC)
# ============================================================

library(stringr)
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
INPUT_BASE <- file.path(BASE, "input")
OUTPUT_BASE <- file.path(BASE, "OutputExtractedarticles")

# Read the diagnostic results
qc <- read.csv(file.path(BASE, "boundary_qc_results.csv"), stringsAsFactors = FALSE)
endcuts <- qc[grepl("END_CUT", qc$issue_type), ]

cat(sprintf("Classifying %d END_CUT articles...\n\n", nrow(endcuts)))

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

read_file_safe <- function(path) {
  tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) tryCatch(
      read_file(path, locale=locale(encoding="windows-1252")),
      error=function(e2) ""
    ))
}

# Check if text ends mid-word
ends_mid_word <- function(text) {
  text <- str_trim(text)
  n <- nchar(text)
  if (n < 10) return(FALSE)
  last_char <- str_sub(text, n, n)
  # If last char is a letter and previous chars don't have terminal punct
  if (str_detect(last_char, "[a-zA-Z]")) {
    last_20 <- str_sub(text, max(1, n-20), n)
    if (!str_detect(last_20, "[.!?\"')\\]]\\s*$")) return(TRUE)
  }
  FALSE
}

# For each END_CUT article, check the input text
results <- data.frame(
  volume = integer(),
  month = character(),
  filename = character(),
  classification = character(),
  article_end_chars = character(),
  input_continuation = character(),
  stringsAsFactors = FALSE
)

# Cache input files
input_cache <- list()

for (i in seq_len(nrow(endcuts))) {
  row <- endcuts[i, ]
  vol <- row$volume
  month <- row$month
  fname <- row$filename

  # Determine year from volume
  year <- vol + 1913  # Vol36=1949, Vol37=1950, etc.

  # Read article file
  art_path <- file.path(OUTPUT_BASE, sprintf("Vol%d_%d", vol, year), month, fname)
  if (!file.exists(art_path)) next
  art_text <- read_file_safe(art_path)
  art_text <- str_trim(art_text)

  # Get last 50 chars of article (for matching in input)
  n_art <- nchar(art_text)
  last_50 <- str_sub(art_text, max(1, n_art - 49), n_art)
  last_50 <- str_trim(last_50)
  if (nchar(last_50) < 15) {
    results <- rbind(results, data.frame(
      volume=vol, month=month, filename=fname,
      classification="UNCLEAR_SHORT", article_end_chars=last_50,
      input_continuation="", stringsAsFactors=FALSE))
    next
  }

  # Find input file
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
    if (length(input_files) > 0) {
      input_cache[[cache_key]] <- read_file_safe(input_files[1])
    } else {
      input_cache[[cache_key]] <- ""
    }
  }
  input_text <- input_cache[[cache_key]]
  if (nchar(input_text) < 100) {
    results <- rbind(results, data.frame(
      volume=vol, month=month, filename=fname,
      classification="UNCLEAR_NO_INPUT", article_end_chars=last_50,
      input_continuation="", stringsAsFactors=FALSE))
    next
  }

  # Find the article's ending text in the input
  search_phrase <- str_sub(last_50, max(1, nchar(last_50) - 30), nchar(last_50))
  search_phrase <- str_trim(search_phrase)
  esc <- str_replace_all(search_phrase, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
  esc <- str_replace_all(esc, "\\s+", "\\\\s+")

  m <- tryCatch(
    str_locate(input_text, regex(esc, ignore_case = FALSE)),
    error = function(e) matrix(NA, 1, 2)
  )

  if (is.na(m[1,1])) {
    # Try case-insensitive
    m <- tryCatch(
      str_locate(input_text, regex(esc, ignore_case = TRUE)),
      error = function(e) matrix(NA, 1, 2)
    )
  }

  if (is.na(m[1,1])) {
    results <- rbind(results, data.frame(
      volume=vol, month=month, filename=fname,
      classification="UNCLEAR_NOT_FOUND", article_end_chars=last_50,
      input_continuation="", stringsAsFactors=FALSE))
    next
  }

  # Found! Check what comes AFTER in the input
  after_pos <- m[1,2] + 1
  continuation <- str_sub(input_text, after_pos, min(after_pos + 300, nchar(input_text)))
  continuation <- str_trim(continuation)
  cont_display <- str_sub(continuation, 1, min(150, nchar(continuation)))
  cont_display <- str_replace_all(cont_display, "\n", " ")

  # Classify based on what comes next
  classification <- "UNCLEAR"

  # Check for ad indicators in continuation
  ad_score <- 0
  ad_pats <- c("\\$\\d+\\.\\d{2}", "Salt Lake City", "\\bFREE\\b",
               "(?i)\\border now\\b", "(?i)\\bDeseret Book\\b",
               "(?i)\\bZCMI\\b", "(?i)\\bpostpaid\\b",
               "(?i)When Buying", "(?i)Mention.{0,10}Magazine")
  for (ap in ad_pats) {
    if (tryCatch(str_detect(continuation, ap), error=function(e) FALSE)) {
      ad_score <- ad_score + 1
    }
  }

  if (ad_score >= 2) {
    classification <- "COMPLETE_AD_FOLLOWS"
  } else {
    # Check if continuation starts with a new article title (uppercase words + author)
    if (str_detect(str_sub(continuation, 1, 80), "^[A-Z][a-z]+\\s+[A-Z].*\\b[A-Z][a-z]+\\s+[A-Z][a-z]+\\b")) {
      classification <- "COMPLETE_NEXT_ART"
    }
    # Check if continuation has page markers indicating new content
    else if (str_detect(continuation, "^\\s*\\d{1,3}\\s+[A-Z]")) {
      classification <- "LIKELY_COMPLETE"
    }
    # Check if continuation looks like more of the same article
    else if (nchar(continuation) > 50 && !str_detect(str_sub(continuation, 1, 100), "^[A-Z]{3,}")) {
      # Continuation has lowercase text = likely more article content
      first_100 <- str_sub(continuation, 1, 100)
      if (str_detect(first_100, "^[a-z]|^\\s+[a-z]|^[,;:]")) {
        classification <- "TRUNCATED"
      } else if (str_detect(first_100, "^[A-Z][a-z]")) {
        # Could be new sentence of same article or new article
        classification <- "NEEDS_PDF_QC"
      }
    }
  }

  results <- rbind(results, data.frame(
    volume=vol, month=month, filename=fname,
    classification=classification,
    article_end_chars=str_replace_all(last_50, "\n", " "),
    input_continuation=cont_display,
    stringsAsFactors=FALSE))
}

# Summary
cat("\n=== CLASSIFICATION SUMMARY ===\n")
class_counts <- table(results$classification)
for (cl in sort(names(class_counts))) {
  cat(sprintf("  %-25s: %d\n", cl, class_counts[cl]))
}
cat(sprintf("\n  TOTAL: %d articles classified\n", nrow(results)))

# Save results
out_file <- file.path(BASE, "endcut_classification.csv")
write.csv(results, out_file, row.names = FALSE)
cat(sprintf("\nResults saved to: %s\n", out_file))

# Show genuinely truncated articles
truncated <- results[results$classification == "TRUNCATED", ]
if (nrow(truncated) > 0) {
  cat(sprintf("\n=== %d GENUINELY TRUNCATED (need fixing) ===\n", nrow(truncated)))
  for (j in seq_len(min(20, nrow(truncated)))) {
    r <- truncated[j, ]
    cat(sprintf("  [V%d %s] %s\n", r$volume, r$month, r$filename))
    cat(sprintf("    Ends: ...%s\n", str_sub(r$article_end_chars, max(1, nchar(r$article_end_chars)-60))))
    cat(sprintf("    Continues: %s...\n\n", str_sub(r$input_continuation, 1, 80)))
  }
}

# Show those needing PDF QC
needs_pdf <- results[results$classification == "NEEDS_PDF_QC", ]
cat(sprintf("\n=== %d NEED PDF QC ===\n", nrow(needs_pdf)))
