# ============================================================
# BOUNDARY QC DIAGNOSTIC - Vols 36-40
# Identifies truncated articles (cut-off beginnings/endings)
# by comparing extracted articles against input source text
# ============================================================

library(stringr)
library(readr)

# ---- Configuration ----
BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
INPUT_DIR  <- file.path(BASE, "input")
OUTPUT_DIR <- file.path(BASE, "OutputExtractedarticles")

VOLUMES <- list(
  list(vol=36, year=1949),
  list(vol=37, year=1950),
  list(vol=38, year=1951),
  list(vol=39, year=1952),
  list(vol=40, year=1953)
)

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

read_file_safe <- function(path) {
  txt <- tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) read_file(path, locale=locale(encoding="windows-1252"))
  )
  txt
}

# ---- Diagnostic functions ----

# Check if text ends mid-word or mid-sentence
check_ending <- function(text) {
  text <- str_trim(text)
  if (nchar(text) == 0) return("EMPTY")

  # Get last 100 chars
  tail_text <- str_sub(text, max(1, nchar(text) - 100), nchar(text))

  # Check if ends mid-word (no terminal punctuation, no complete word)
  last_char <- str_sub(text, nchar(text), nchar(text))
  last_5 <- str_sub(text, max(1, nchar(text) - 5), nchar(text))

  # Ends mid-word if last char is a letter and there's no space/punctuation after meaningful text
  if (str_detect(last_char, "[a-zA-Z]")) {
    # Check if the ending looks like a truncation
    # Look for incomplete sentences
    if (!str_detect(last_5, "[.!?\"')\\]]$")) {
      return("TRUNCATED_END")
    }
  }

  return("OK")
}

# Check if article begins with its expected title
check_beginning <- function(text, filename) {
  # Skip BOARD, CONTENTS, Misc files
  if (str_detect(filename, "_(BOARD|CONTENTS|Misc)\\.txt$")) return("SKIP")

  lines <- str_split(text, "\n")[[1]]
  lines <- lines[nchar(str_trim(lines)) > 0]
  if (length(lines) == 0) return("EMPTY")

  first_line <- lines[1]

  # Check if starts with [Title by Author] header
  if (str_detect(first_line, "^\\[.+\\]")) return("OK")

  return("NO_HEADER")
}

# Check if article content matches expected title from filename
check_content_vs_filename <- function(text, filename) {
  if (str_detect(filename, "_(BOARD|CONTENTS|Misc)\\.txt$")) return("SKIP")

  # Extract expected title words from filename
  # Filename format: V39_03_01_Art_Individual_Simonse.txt
  parts <- str_match(filename, "V\\d+_\\d+_\\d+_[A-Za-z]+_(.+)\\.txt")
  if (is.na(parts[1,1])) return("UNKNOWN")

  title_part <- parts[1,2]
  title_words <- str_split(title_part, "_")[[1]]

  # Check if any title words appear in first 500 chars
  first_500 <- str_sub(text, 1, 500)
  matches <- sum(sapply(title_words, function(w) {
    str_detect(first_500, regex(w, ignore_case=TRUE))
  }))

  if (matches >= max(1, length(title_words) * 0.5)) return("OK")
  return("TITLE_MISMATCH")
}

# ---- Run diagnostic ----
cat("=" , rep("=", 69), "\n", sep="")
cat("  BOUNDARY QC DIAGNOSTIC — Vols 36-40\n")
cat("=", rep("=", 69), "\n\n", sep="")

all_issues <- data.frame(
  volume = integer(),
  month = character(),
  filename = character(),
  issue_type = character(),
  file_size = integer(),
  tail_text = character(),
  stringsAsFactors = FALSE
)

for (vol_info in VOLUMES) {
  vol <- vol_info$vol
  year <- vol_info$year
  vol_dir <- file.path(OUTPUT_DIR, sprintf("Vol%d_%d", vol, year))

  if (!dir.exists(vol_dir)) {
    cat(sprintf("  Vol %d: directory not found, skipping\n", vol))
    next
  }

  cat(sprintf("\n--- Vol %d (%d) ---\n", vol, year))

  for (month in MONTHS) {
    month_dir <- file.path(vol_dir, month)
    if (!dir.exists(month_dir)) next

    files <- list.files(month_dir, pattern = "^V\\d+.*\\.txt$", full.names = TRUE)
    if (length(files) == 0) next

    truncated_count <- 0
    mismatch_count <- 0

    for (f in files) {
      fname <- basename(f)

      # Skip special files
      if (str_detect(fname, "_(BOARD|CONTENTS|Misc)\\.txt$")) next

      text <- read_file_safe(f)
      fsize <- file.size(f)

      # Check ending
      end_status <- check_ending(text)

      # Check beginning
      begin_status <- check_beginning(text, fname)

      # Check content vs filename
      content_status <- check_content_vs_filename(text, fname)

      if (end_status == "TRUNCATED_END" || begin_status == "NO_HEADER" ||
          content_status == "TITLE_MISMATCH") {

        # Get last 60 chars for display
        tail_text <- str_sub(str_trim(text), max(1, nchar(str_trim(text)) - 60), nchar(str_trim(text)))
        tail_text <- str_replace_all(tail_text, "\n", " ")

        issues <- c()
        if (end_status == "TRUNCATED_END") issues <- c(issues, "END_CUT")
        if (begin_status == "NO_HEADER") issues <- c(issues, "NO_HEADER")
        if (content_status == "TITLE_MISMATCH") issues <- c(issues, "WRONG_CONTENT")

        issue_str <- paste(issues, collapse="+")

        all_issues <- rbind(all_issues, data.frame(
          volume = vol,
          month = month,
          filename = fname,
          issue_type = issue_str,
          file_size = fsize,
          tail_text = tail_text,
          stringsAsFactors = FALSE
        ))

        if ("END_CUT" %in% issues) truncated_count <- truncated_count + 1
        if ("WRONG_CONTENT" %in% issues || "NO_HEADER" %in% issues) mismatch_count <- mismatch_count + 1
      }
    }

    total_files <- length(files) - sum(str_detect(basename(files), "_(BOARD|CONTENTS|Misc)\\.txt$"))
    if (truncated_count > 0 || mismatch_count > 0) {
      cat(sprintf("  %s: %d/%d articles | %d truncated ends, %d wrong content\n",
                  month, total_files, length(files), truncated_count, mismatch_count))
    }
  }
}

cat("\n\n")
cat("=", rep("=", 69), "\n", sep="")
cat(sprintf("  TOTAL ISSUES FOUND: %d\n", nrow(all_issues)))
cat("=", rep("=", 69), "\n\n", sep="")

if (nrow(all_issues) > 0) {
  cat("DETAILED ISSUE LIST:\n\n")
  for (i in seq_len(nrow(all_issues))) {
    row <- all_issues[i, ]
    cat(sprintf("  [V%d %s] %s\n", row$volume, row$month, row$filename))
    cat(sprintf("    Issue: %s | Size: %d bytes\n", row$issue_type, row$file_size))
    if (str_detect(row$issue_type, "END_CUT")) {
      cat(sprintf("    Tail: ...%s\n", row$tail_text))
    }
    cat("\n")
  }
}

# Save results
results_file <- file.path(BASE, "boundary_qc_results.csv")
write_csv(all_issues, results_file)
cat(sprintf("Results saved to: %s\n", results_file))
