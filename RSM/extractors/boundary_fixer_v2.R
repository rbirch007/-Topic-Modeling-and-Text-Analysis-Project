# ============================================================
# BOUNDARY FIXER v2 — Targeted fixes only
# 1. END_CUT: Extends truncated articles by finding the continuation
#    in the input text. Only adds the MISSING text, doesn't re-extract.
# 2. NO_HEADER: Adds [Title by Author] headers from CONTENTS.
# NO API calls. Creates backups before changes.
# ============================================================

library(stringr)
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
INPUT_DIR  <- file.path(BASE, "input")
OUTPUT_DIR <- file.path(BASE, "OutputExtractedarticles")

TARGET_VOLS <- c(36, 37, 38, 39, 40)

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
  tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) read_file(path, locale=locale(encoding="windows-1252"))
  )
}

# ---- Check if text ends mid-word/sentence (truncated) ----
is_truncated <- function(text) {
  text <- str_trim(text)
  if (nchar(text) < 50) return(FALSE)
  last_char <- str_sub(text, nchar(text), nchar(text))
  last_5 <- str_sub(text, max(1, nchar(text) - 5), nchar(text))
  # Ends with a letter but no terminal punctuation = truncated
  if (str_detect(last_char, "[a-zA-Z]") && !str_detect(last_5, "[.!?\"')\\]]$")) {
    return(TRUE)
  }
  FALSE
}

# ---- Find continuation of truncated text in input ----
# Takes the last N chars of the article, finds them in input,
# then reads ahead until hitting a reasonable stop point
find_continuation <- function(article_text, input_text, next_art_files) {
  article_text <- str_trim(article_text)
  n <- nchar(article_text)

  # Take last 40-60 chars as search phrase
  for (phrase_len in c(60, 50, 40, 30)) {
    search_phrase <- str_sub(article_text, max(1, n - phrase_len + 1), n)
    search_phrase <- str_trim(search_phrase)
    if (nchar(search_phrase) < 20) next

    # Escape for regex
    esc <- str_replace_all(search_phrase, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
    esc <- str_replace_all(esc, "\\s+", "\\\\s+")

    # Find in input text
    m <- tryCatch(
      str_locate(input_text, regex(esc, ignore_case = FALSE)),
      error = function(e) matrix(NA, nrow=1, ncol=2)
    )

    if (!is.na(m[1,1])) {
      match_end <- m[1,2]

      # Now read ahead from match_end to find the continuation
      remaining <- str_sub(input_text, match_end + 1, nchar(input_text))

      # Find stop points: next article's first line, or reasonable break
      stop_pos <- nchar(remaining)  # default: read to end

      # Try to find the next article's content in the remaining text
      for (next_file in next_art_files) {
        if (!file.exists(next_file)) next
        next_text <- read_file_safe(next_file)
        next_text <- str_trim(next_text)
        # Remove header line if present
        if (str_detect(next_text, "^\\s*\\[.+\\]")) {
          next_text <- str_trim(str_replace(next_text, "^\\s*\\[.+?\\]\\s*", ""))
        }
        if (nchar(next_text) < 30) next

        # Take first 50-80 chars of next article as stop marker
        for (stop_len in c(80, 60, 50, 40)) {
          stop_phrase <- str_sub(next_text, 1, min(stop_len, nchar(next_text)))
          stop_phrase <- str_trim(stop_phrase)
          if (nchar(stop_phrase) < 20) next

          stop_esc <- str_replace_all(stop_phrase, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
          stop_esc <- str_replace_all(stop_esc, "\\s+", "\\\\s+")

          stop_m <- tryCatch(
            str_locate(remaining, regex(stop_esc, ignore_case = FALSE)),
            error = function(e) matrix(NA, nrow=1, ncol=2)
          )

          if (!is.na(stop_m[1,1])) {
            stop_pos <- stop_m[1,1] - 1
            break
          }
        }
        if (stop_pos < nchar(remaining)) break
      }

      # Safety: don't add more than 15KB (a very long article)
      if (stop_pos > 15000) {
        # Look for a page break or article-ending pattern closer
        closer_text <- str_sub(remaining, 1, min(stop_pos, 15000))

        # Try to find a sentence end near a reasonable length
        # Look for page markers like "Page NNN NNN"
        page_breaks <- str_locate_all(closer_text, "Page\\s+\\d{1,3}\\s+\\d{1,3}")[[1]]
        if (nrow(page_breaks) > 0) {
          # Use the last page break before 15000 as stop
          stop_pos <- page_breaks[nrow(page_breaks), 2]
        } else {
          stop_pos <- min(stop_pos, 15000)
        }
      }

      continuation <- str_sub(remaining, 1, stop_pos)
      continuation <- str_trim(continuation)

      if (nchar(continuation) > 10) {
        return(continuation)
      }
    }
  }

  return(NULL)
}

# ---- Parse title and author from CONTENTS ----
parse_contents_entries <- function(input_text) {
  lines <- str_split(input_text, "\n")[[1]]

  # Find Contents section
  contents_start <- which(str_detect(lines, "(?i)^\\s*Contents\\s*$"))
  if (length(contents_start) == 0) return(data.frame())
  contents_start <- contents_start[1]

  # Find end of Contents
  contents_end <- length(lines)
  for (i in (contents_start+1):min(length(lines), contents_start + 80)) {
    if (str_detect(lines[i], "(?i)^\\s*PUBLISHED\\s+MONTHLY")) {
      contents_end <- i - 1
      break
    }
  }

  current_cat <- "Article"
  entries <- data.frame(title=character(), author=character(), page=integer(),
                        category=character(), stringsAsFactors=FALSE)

  for (line in lines[contents_start:contents_end]) {
    line <- str_trim(line)
    if (nchar(line) == 0) next

    # Category headers
    if (str_detect(line, "(?i)^(SPECIAL FEATURES|FICTION|GENERAL FEATURES|FEATURES FOR THE HOME|POETRY|LESSONS?)\\s*$")) {
      if (str_detect(line, "(?i)FICTION")) current_cat <- "Fiction"
      else if (str_detect(line, "(?i)POETRY")) current_cat <- "Poetry"
      else current_cat <- "Article"
      next
    }
    if (str_detect(line, "(?i)^Contents\\s*$")) next

    # Title by Author PAGE
    m <- str_match(line, "^(.+?)\\s+by\\s+([A-Z][A-Za-z .]+?)\\s+(\\d{1,3})\\s*$")
    if (!is.na(m[1,1])) {
      entries <- rbind(entries, data.frame(title=str_trim(m[1,2]), author=str_trim(m[1,3]),
                                           page=as.integer(m[1,4]), category=current_cat,
                                           stringsAsFactors=FALSE))
      next
    }
    # Title PAGE (no author)
    m2 <- str_match(line, "^(.+?)\\s+(\\d{1,3})\\s*$")
    if (!is.na(m2[1,1]) && nchar(str_trim(m2[1,2])) > 3) {
      entries <- rbind(entries, data.frame(title=str_trim(m2[1,2]), author="",
                                           page=as.integer(m2[1,3]), category=current_cat,
                                           stringsAsFactors=FALSE))
      next
    }
    # Poetry: Title, by Author, PAGE
    m3 <- str_match(line, "^(.+?),\\s+by\\s+([A-Z][A-Za-z .]+?),?\\s+(\\d{1,3})\\s*$")
    if (!is.na(m3[1,1])) {
      entries <- rbind(entries, data.frame(title=str_trim(m3[1,2]), author=str_trim(m3[1,3]),
                                           page=as.integer(m3[1,4]), category="Poetry",
                                           stringsAsFactors=FALSE))
    }
  }

  if (nrow(entries) > 0) entries <- entries[order(entries$page), ]
  entries
}

# ---- Match article file to CONTENTS entry ----
match_file_to_contents <- function(filename, article_text, contents) {
  if (nrow(contents) == 0) return(NULL)

  # Extract words from filename: V39_03_01_Art_Individual_Simonse.txt
  parts <- str_match(filename, "V\\d+_\\d+_(\\d+)_[A-Za-z]+_(.+)\\.txt")
  if (is.na(parts[1,1])) return(NULL)

  art_num <- as.integer(parts[1,2])
  name_part <- parts[1,3]
  name_words <- str_split(name_part, "_")[[1]]

  # Try to match against contents entries
  best_match <- NULL
  best_score <- 0

  for (i in seq_len(nrow(contents))) {
    title <- contents$title[i]
    author <- contents$author[i]
    score <- 0

    # Check if filename words appear in title or author
    for (w in name_words) {
      if (nchar(w) < 3) next
      if (str_detect(title, regex(w, ignore_case = TRUE))) score <- score + 2
      if (nchar(author) > 0 && str_detect(author, regex(w, ignore_case = TRUE))) score <- score + 3
    }

    # Check if title words appear in first 500 chars of article
    first_500 <- str_sub(article_text, 1, min(500, nchar(article_text)))
    title_words <- str_split(title, "\\s+")[[1]]
    title_words <- title_words[nchar(title_words) > 3]
    for (tw in title_words[1:min(3, length(title_words))]) {
      tw_esc <- str_replace_all(tw, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      hit <- tryCatch(str_detect(first_500, regex(tw_esc, ignore_case = TRUE)),
                       error = function(e) FALSE)
      if (isTRUE(hit)) {
        score <- score + 1
      }
    }

    if (score > best_score) {
      best_score <- score
      best_match <- contents[i, ]
    }
  }

  if (best_score >= 3) return(best_match)
  NULL
}

# ============================================================
# MAIN PROCESSING
# ============================================================

cat("=", rep("=", 69), "\n", sep="")
cat("  BOUNDARY FIXER v2 — Targeted fixes\n")
cat("  Fix 1: Extend truncated articles (END_CUT)\n")
cat("  Fix 2: Add missing [Title by Author] headers\n")
cat("=", rep("=", 69), "\n\n", sep="")

total_extended <- 0
total_headers <- 0

for (vol_info in VOLUMES) {
  vol <- vol_info$vol
  year <- vol_info$year
  if (!is.null(TARGET_VOLS) && !(vol %in% TARGET_VOLS)) next

  vol_dir <- file.path(OUTPUT_DIR, sprintf("Vol%d_%d", vol, year))
  if (!dir.exists(vol_dir)) next

  input_preproc <- file.path(INPUT_DIR, sprintf("Vol%dsplit_preprocessed", vol))
  input_raw <- file.path(INPUT_DIR, sprintf("Vol%dsplit", vol))
  use_input_dir <- if (dir.exists(input_preproc)) input_preproc else input_raw

  cat(sprintf("\n=== Vol %d (%d) ===\n", vol, year))

  for (month in MONTHS) {
    month_dir <- file.path(vol_dir, month)
    if (!dir.exists(month_dir)) next

    # Find input file
    input_files <- list.files(use_input_dir, pattern = sprintf("(?i)%s.*\\.txt$", month),
                              full.names = TRUE)
    if (length(input_files) == 0) {
      month_num <- sprintf("%02d", which(MONTHS == month))
      input_files <- list.files(use_input_dir, pattern = sprintf("(?i)No%s|_%s_|%s", month_num, month, month),
                                full.names = TRUE)
    }
    if (length(input_files) == 0) next

    input_text <- read_file_safe(input_files[1])
    contents <- parse_contents_entries(input_text)

    # Get all article files sorted by number
    art_files <- list.files(month_dir, pattern = "^V\\d+.*\\.txt$", full.names = TRUE)
    art_files <- art_files[!str_detect(basename(art_files), "_(BOARD|CONTENTS|Misc)\\.txt$")]
    art_files <- sort(art_files)

    month_extended <- 0
    month_headers <- 0

    for (fi in seq_along(art_files)) {
      f <- art_files[fi]
      fname <- basename(f)
      text <- read_file_safe(f)

      # ---- FIX 1: Extend truncated articles ----
      if (is_truncated(text)) {
        # Get next article files as stop markers
        next_files <- if (fi < length(art_files)) art_files[(fi+1):min(fi+3, length(art_files))] else character(0)

        continuation <- find_continuation(text, input_text, next_files)

        if (!is.null(continuation) && nchar(continuation) > 20) {
          # Backup
          backup_dir <- file.path(month_dir, "_v2_backups")
          if (!dir.exists(backup_dir)) dir.create(backup_dir)
          file.copy(f, file.path(backup_dir, fname), overwrite = FALSE)

          # Append continuation
          extended_text <- paste0(str_trim(text), " ", continuation)
          write_file(extended_text, f)

          cat(sprintf("    EXTENDED: %s (+%d chars)\n", fname, nchar(continuation)))
          month_extended <- month_extended + 1
          total_extended <- total_extended + 1

          # Re-read for header check
          text <- extended_text
        }
      }

      # ---- FIX 2: Add missing headers ----
      has_header <- str_detect(text, "^\\s*\\[.+\\]")
      if (!has_header) {
        match <- match_file_to_contents(fname, text, contents)
        if (!is.null(match)) {
          if (nchar(match$author) > 0) {
            header <- sprintf("[%s by %s]", match$title, match$author)
          } else {
            header <- sprintf("[%s]", match$title)
          }

          # Backup
          backup_dir <- file.path(month_dir, "_v2_backups")
          if (!dir.exists(backup_dir)) dir.create(backup_dir)
          if (!file.exists(file.path(backup_dir, fname))) {
            file.copy(f, file.path(backup_dir, fname), overwrite = FALSE)
          }

          corrected <- paste0(header, "\n\n", text)
          write_file(corrected, f)

          cat(sprintf("    HEADER: %s -> %s\n", fname, header))
          month_headers <- month_headers + 1
          total_headers <- total_headers + 1
        }
      }
    }

    if (month_extended > 0 || month_headers > 0) {
      cat(sprintf("  %s: %d extended, %d headers added\n", month, month_extended, month_headers))
    }
  }
}

cat("\n")
cat("=", rep("=", 69), "\n", sep="")
cat(sprintf("  COMPLETE: %d articles extended, %d headers added\n", total_extended, total_headers))
cat("  Backups in _v2_backups/ directories\n")
cat("=", rep("=", 69), "\n", sep="")
