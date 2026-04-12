# fix_article_boundaries.R
# Post-processing: fix article boundary bleeding
library(stringr)
library(readr)

VOLUME <- 56L
YEAR <- 1969L
BASE_DIR <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
OUTPUT_DIR <- file.path(BASE_DIR, "output", paste0("Vol", VOLUME, "_", YEAR))
DRY_RUN <- FALSE

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

cat(sprintf("=== Article Boundary Fix - Vol%d (%d) ===\n", VOLUME, YEAR))
if (DRY_RUN) cat("*** DRY RUN ***\n")

# Helper: extract body text (after bracket headers)
get_body <- function(text) {
  lines <- str_split(text, "\n")[[1]]
  body_start <- 1L
  for (i in seq_along(lines)) {
    if (grepl("^\\[", lines[i], fixed = FALSE)) {
      body_start <- i + 1L
    } else {
      break
    }
  }
  if (body_start > length(lines)) return("")
  paste(lines[body_start:length(lines)], collapse = "\n")
}

# Helper: extract bracket header
get_header <- function(text) {
  lines <- str_split(text, "\n")[[1]]
  header_lines <- character(0)
  for (i in seq_along(lines)) {
    if (grepl("^\\[", lines[i], fixed = FALSE)) {
      header_lines <- c(header_lines, lines[i])
    } else {
      break
    }
  }
  if (length(header_lines) == 0) return("")
  paste0(paste(header_lines, collapse = "\n"), "\n")
}

# Helper: n-gram overlap
ngram_overlap <- function(text_a, text_b, n = 5L) {
  words_a <- str_split(tolower(text_a), "\\s+")[[1]]
  words_b <- str_split(tolower(text_b), "\\s+")[[1]]
  if (length(words_a) < n || length(words_b) < n) return(0.0)
  ngrams_a <- character(length(words_a) - n + 1L)
  for (i in seq_len(length(words_a) - n + 1L)) {
    ngrams_a[i] <- paste(words_a[i:(i+n-1L)], collapse = " ")
  }
  ngrams_b <- character(length(words_b) - n + 1L)
  for (i in seq_len(length(words_b) - n + 1L)) {
    ngrams_b[i] <- paste(words_b[i:(i+n-1L)], collapse = " ")
  }
  shared <- length(intersect(ngrams_a, ngrams_b))
  min_total <- min(length(ngrams_a), length(ngrams_b))
  if (min_total == 0L) return(0.0)
  shared / min_total
}

# Helper: find where text_b content starts in text_a
find_overlap_start <- function(body_a, body_b) {
  words_b <- str_split(str_trim(body_b), "\\s+")[[1]]
  if (length(words_b) < 5) return(NA_integer_)
  for (nw in c(12, 10, 8, 6)) {
    if (nw > length(words_b)) next
    search_words <- words_b[1:nw]
    # Use fixed string search with first N words
    search_str <- paste(search_words, collapse = " ")
    # Normalize both to single spaces for matching
    norm_a <- str_replace_all(body_a, "\\s+", " ")
    pos <- regexpr(search_str, norm_a, fixed = TRUE)
    if (pos[1] > 0) return(pos[1])
  }
  return(NA_integer_)
}

total_fixed <- 0L
total_checked <- 0L

for (month_name in MONTHS) {
  month_dir <- file.path(OUTPUT_DIR, month_name)
  if (!dir.exists(month_dir)) next

  txt_files <- sort(list.files(month_dir, pattern = "\\.txt$"))
  art_files <- txt_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", txt_files)]
  if (length(art_files) < 2) next

  n <- length(art_files)
  cat(sprintf("\n=== %s (%d articles) ===\n", month_name, n))

  # Read all article content
  art_content <- character(n)
  art_body <- character(n)
  art_header <- character(n)
  for (i in seq_len(n)) {
    filepath <- file.path(month_dir, art_files[i])
    art_content[i] <- tryCatch(read_file(filepath), error = function(e) "")
    art_body[i] <- get_body(art_content[i])
    art_header[i] <- get_header(art_content[i])
  }

  # Check adjacent pairs for overlap
  for (i in seq_len(n - 1L)) {
    body_a <- str_trim(art_body[i])
    body_b <- str_trim(art_body[i + 1L])
    if (nchar(body_a) < 50 || nchar(body_b) < 50) next
    total_checked <- total_checked + 1L

    # Quick overlap check
    overlap <- ngram_overlap(body_a, body_b)
    if (overlap < 0.25) next

    cat(sprintf("  Overlap %.0f%%: %s <-> %s\n",
                overlap * 100, art_files[i], art_files[i + 1L]))

    # Find where text_b beginning appears in text_a
    overlap_pos <- find_overlap_start(body_a, body_b)

    if (!is.na(overlap_pos) && overlap_pos > 50) {
      trimmed_body <- str_trim(substr(body_a, 1, overlap_pos - 1L))
      old_len <- nchar(body_a)
      new_len <- nchar(trimmed_body)

      if (new_len >= 20 && new_len < old_len) {
        cat(sprintf("    TRIM %s: %d -> %d chars\n",
                    art_files[i], old_len, new_len))
        if (!DRY_RUN) {
          patched <- paste0(art_header[i], trimmed_body, "\n")
          write_file(patched, file.path(month_dir, art_files[i]))
          art_body[i] <- trimmed_body
        }
        total_fixed <- total_fixed + 1L
      }
    } else {
      cat(sprintf("    No clean boundary found\n"))
    }
  }
}

cat("\n============================================================\n")
cat("SUMMARY\n")
cat("============================================================\n")
cat(sprintf("Adjacent pairs checked: %d\n", total_checked))
cat(sprintf("Articles trimmed:       %d\n", total_fixed))
if (DRY_RUN) cat("(DRY RUN - no files modified)\n")
cat("\nDone.\n")