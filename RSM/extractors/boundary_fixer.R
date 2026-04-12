# ============================================================
# BOUNDARY FIXER — Re-extract articles with correct boundaries
# Uses text search (not page estimation) to find article titles
# in the raw input, then extracts the full text between titles.
# NO API calls needed.
# ============================================================

library(stringr)
library(readr)

# ---- Configuration ----
BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
INPUT_DIR  <- file.path(BASE, "input")
OUTPUT_DIR <- file.path(BASE, "OutputExtractedarticles")

# Which volumes/months to process (set to NULL for all)
TARGET_VOLS <- c(36, 37, 38, 39)

VOLUMES <- list(
  list(vol=36, year=1949),
  list(vol=37, year=1950),
  list(vol=38, year=1951),
  list(vol=39, year=1952),
  list(vol=40, year=1953)
)

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

# ---- Utility functions ----

read_file_safe <- function(path) {
  txt <- tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) read_file(path, locale=locale(encoding="windows-1252"))
  )
  txt
}

# Get significant words from a title for fuzzy matching
get_sig_words <- function(title) {
  stops <- c("the","a","an","of","in","on","at","to","for","and","or","but","by",
             "its","is","are","was","were","be","been","our","her","his","my","we",
             "it","as","with","from","that","this","not","no","out","new","all")
  words <- str_split(title, "[\\s\\-\u2014\u2013]+")[[1]]
  words <- str_replace_all(words, "[^A-Za-z0-9]", "")
  words <- words[nchar(words) > 2]
  words <- words[!tolower(words) %in% stops]
  words
}

# Parse CONTENTS section from input file
# Returns data.frame(title, author, page, category, order)
parse_contents <- function(input_text) {
  lines <- str_split(input_text, "\n")[[1]]

  # Find Contents section
  contents_start <- which(str_detect(lines, "(?i)^\\s*Contents\\s*$"))
  if (length(contents_start) == 0) {
    contents_start <- which(str_detect(lines, "(?i)Contents"))
    if (length(contents_start) == 0) return(NULL)
  }
  contents_start <- contents_start[1]

  # Find end of Contents (look for PUBLISHED or the poem/body text start)
  contents_end <- length(lines)
  for (i in (contents_start+1):min(length(lines), contents_start + 80)) {
    if (str_detect(lines[i], "(?i)^\\s*PUBLISHED\\s+MONTHLY")) {
      contents_end <- i - 1
      break
    }
  }

  contents_lines <- lines[contents_start:contents_end]
  contents_text <- paste(contents_lines, collapse = "\n")

  # Category detection
  current_cat <- "Article"
  entries <- data.frame(title=character(), author=character(), page=integer(),
                        category=character(), stringsAsFactors=FALSE)

  for (line in contents_lines) {
    line <- str_trim(line)
    if (nchar(line) == 0) next

    # Category headers
    if (str_detect(line, "(?i)^\\s*(SPECIAL FEATURES|FICTION|GENERAL FEATURES|FEATURES FOR THE HOME|POETRY|LESSONS?|DEPARTMENTS?)\\s*$") ||
        str_detect(line, "(?i)^(SPECIAL FEATURES|FICTION|GENERAL FEATURES|FEATURES FOR THE HOME|POETRY)")) {
      if (str_detect(line, "(?i)FICTION")) current_cat <- "Fiction"
      else if (str_detect(line, "(?i)POETRY")) current_cat <- "Poetry"
      else if (str_detect(line, "(?i)LESSON")) current_cat <- "Lesson"
      else if (str_detect(line, "(?i)FEATURES FOR THE HOME")) current_cat <- "Home"
      else if (str_detect(line, "(?i)GENERAL FEATURES")) current_cat <- "General"
      else if (str_detect(line, "(?i)SPECIAL FEATURES")) current_cat <- "Special"

      # Check if there are entries on the same line as the category header
      # e.g., "FICTION \"The Least of These\" by Author 153"
      remaining <- str_replace(line, "^\\s*(SPECIAL FEATURES|FICTION|GENERAL FEATURES|FEATURES FOR THE HOME|POETRY|LESSONS?)\\s*", "")
      if (nchar(str_trim(remaining)) < 5) next
      line <- remaining
    }

    # Skip the "Contents" header itself
    if (str_detect(line, "(?i)^\\s*Contents\\s*$")) next

    # Try to extract: Title by Author PAGE or Title PAGE
    # Pattern: title text followed by author name and page number
    m <- str_match(line, "^(.+?)\\s+by\\s+([A-Z][A-Za-z .]+?)\\s+(\\d{1,3})\\s*$")
    if (!is.na(m[1,1])) {
      entries <- rbind(entries, data.frame(
        title=str_trim(m[1,2]), author=str_trim(m[1,3]),
        page=as.integer(m[1,4]), category=current_cat, stringsAsFactors=FALSE))
      next
    }

    # Pattern: title text followed by page number (no author)
    m2 <- str_match(line, "^(.+?)\\s+(\\d{1,3})\\s*$")
    if (!is.na(m2[1,1]) && nchar(str_trim(m2[1,2])) > 3) {
      entries <- rbind(entries, data.frame(
        title=str_trim(m2[1,2]), author="",
        page=as.integer(m2[1,3]), category=current_cat, stringsAsFactors=FALSE))
      next
    }

    # Poetry entries often: Title, by Author, PAGE
    m3 <- str_match(line, "^(.+?),\\s+by\\s+([A-Z][A-Za-z .]+?),?\\s+(\\d{1,3})\\s*$")
    if (!is.na(m3[1,1])) {
      entries <- rbind(entries, data.frame(
        title=str_trim(m3[1,2]), author=str_trim(m3[1,3]),
        page=as.integer(m3[1,4]), category="Poetry", stringsAsFactors=FALSE))
      next
    }
  }

  # Sort by page number
  if (nrow(entries) > 0) {
    entries <- entries[order(entries$page), ]
    entries$order <- seq_len(nrow(entries))
  }

  entries
}

# Search for a title in body text, returning character position
# Uses fuzzy matching with significant words
find_title_in_body <- function(body_text, title, author, search_start = 1) {
  n <- nchar(body_text)
  if (search_start >= n) return(NA_integer_)

  search_text <- str_sub(body_text, search_start, n)

  # Strategy 1: Exact title match (case insensitive)
  # First escape ALL regex special chars, then selectively relax quotes/dashes
  clean_title <- str_replace_all(title, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
  clean_title <- str_replace_all(clean_title, "[\"'\u2018\u2019\u201c\u201d]", ".?")
  clean_title <- str_replace_all(clean_title, "[\\-\u2014\u2013]", ".{1,3}")
  clean_title <- str_replace_all(clean_title, "\\s+", "\\\\s+")
  m <- tryCatch(
    str_locate(search_text, regex(clean_title, ignore_case = TRUE)),
    error = function(e) matrix(NA, nrow=1, ncol=2)
  )
  if (!is.na(m[1,1])) return(search_start + m[1,1] - 1L)

  # Strategy 2: First 3+ significant words in sequence
  sig_words <- get_sig_words(title)
  if (length(sig_words) >= 2) {
    # Escape each word for regex safety
    esc_words <- str_replace_all(sig_words, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
    # Build a flexible pattern: word1 ... word2 ... word3 within 50 chars
    for (n_words in min(length(esc_words), 4):2) {
      words_to_use <- esc_words[1:n_words]
      pat <- paste(words_to_use, collapse = ".{0,30}")
      m <- tryCatch(
        str_locate(search_text, regex(pat, ignore_case = TRUE)),
        error = function(e) matrix(NA, nrow=1, ncol=2)
      )
      if (!is.na(m[1,1])) return(search_start + m[1,1] - 1L)
    }
  }

  # Strategy 3: Author name near title words
  if (nchar(author) > 3 && length(sig_words) >= 1) {
    author_last <- tail(str_split(author, "\\s+")[[1]], 1)
    if (nchar(author_last) > 2) {
      # Find author name, then check if title words are nearby
      author_locs <- str_locate_all(search_text, regex(author_last, ignore_case = TRUE))[[1]]
      if (nrow(author_locs) > 0) {
        for (ai in seq_len(nrow(author_locs))) {
          a_pos <- author_locs[ai, 1]
          # Check 500 chars before author name for title words
          check_start <- max(1, a_pos - 500)
          check_text <- str_sub(search_text, check_start, a_pos + 50)
          word_hits <- sum(sapply(sig_words[1:min(3, length(sig_words))], function(w) {
            str_detect(check_text, regex(w, ignore_case = TRUE))
          }))
          if (word_hits >= min(2, length(sig_words))) {
            # Found it — return position of first title word
            first_word_loc <- str_locate(str_sub(search_text, check_start, a_pos + 50),
                                          regex(sig_words[1], ignore_case = TRUE))
            if (!is.na(first_word_loc[1,1])) {
              return(search_start + check_start + first_word_loc[1,1] - 2L)
            }
          }
        }
      }
    }
  }

  NA_integer_
}

# Find the body text start (after CONTENTS/PUBLISHED section)
find_body_start <- function(input_text) {
  lines <- str_split(input_text, "\n")[[1]]

  # Look for "PUBLISHED MONTHLY" or "Entered as second-class"
  for (i in seq_along(lines)) {
    if (str_detect(lines[i], "(?i)Entered as second.class|Acceptance for mailing")) {
      # Body starts after the next blank line
      for (j in (i+1):min(length(lines), i+5)) {
        if (nchar(str_trim(lines[j])) == 0) {
          # Find position of this line in full text
          pos <- sum(nchar(lines[1:j])) + j  # approximate
          return(pos)
        }
      }
      # Just use character position
      m <- str_locate(input_text, lines[i])
      if (!is.na(m[1,1])) return(m[1,2] + 1L)
    }
  }

  # Fallback: look for the first page header pattern
  m <- str_locate(input_text, "(?i)RELIEF SOCIETY MAGAZINE\\s+VOL")
  if (!is.na(m[1,1]) && m[1,1] > 100) return(m[1,1])

  # Last resort: skip first 15% of file (header/contents area)
  as.integer(nchar(input_text) * 0.10)
}

# Remove running headers from text
clean_running_headers <- function(text, month, year) {
  month_upper <- toupper(month)
  # Remove "Page NNN" markers
  text <- str_replace_all(text, "\\bPage\\s+\\d{1,3}\\b", "")
  # Remove "NNN MONTH YEAR" odd-page headers
  pat1 <- sprintf("\\b\\d{1,3}\\s+%s\\s+%d\\b", month_upper, year)
  text <- str_replace_all(text, regex(pat1, ignore_case=TRUE), "")
  # Remove "RELIEF SOCIETY MAGAZINE—MONTH YEAR" even-page headers
  pat2 <- sprintf("RELIEF SOCIETY MAGAZINE[\\-\u2014\u2013]+%s\\s+%d", month_upper, year)
  text <- str_replace_all(text, regex(pat2, ignore_case=TRUE), "")
  # Remove "RELIEF SOCIETY MAGAZINE NNN"
  text <- str_replace_all(text, "(?i)RELIEF SOCIETY MAGAZINE\\s+\\d{1,3}\\b", "")
  # Clean up multiple blank lines
  text <- str_replace_all(text, "\n{3,}", "\n\n")
  str_trim(text)
}

# ---- Main processing ----

cat("=", rep("=", 69), "\n", sep="")
cat("  BOUNDARY FIXER — Vols 36-40\n")
cat("  Mode: Text-search re-extraction (no API)\n")
cat("=", rep("=", 69), "\n\n", sep="")

total_fixed <- 0
total_checked <- 0

for (vol_info in VOLUMES) {
  vol <- vol_info$vol
  year <- vol_info$year

  if (!is.null(TARGET_VOLS) && !(vol %in% TARGET_VOLS)) next

  vol_dir <- file.path(OUTPUT_DIR, sprintf("Vol%d_%d", vol, year))
  if (!dir.exists(vol_dir)) {
    cat(sprintf("  Vol %d: output directory not found, skipping\n", vol))
    next
  }

  # Find input directory
  input_preproc <- file.path(INPUT_DIR, sprintf("Vol%dsplit_preprocessed", vol))
  input_raw <- file.path(INPUT_DIR, sprintf("Vol%dsplit", vol))
  use_input_dir <- if (dir.exists(input_preproc)) input_preproc else input_raw

  cat(sprintf("\n=== Vol %d (%d) ===\n", vol, year))
  cat(sprintf("  Input: %s\n", basename(use_input_dir)))

  for (month in MONTHS) {
    month_dir <- file.path(vol_dir, month)
    if (!dir.exists(month_dir)) next

    # Find input file for this month
    input_files <- list.files(use_input_dir, pattern = sprintf("(?i)%s.*\\.txt$", month),
                              full.names = TRUE)
    if (length(input_files) == 0) {
      # Try alternate naming: Vol39_No03_March_1952.txt
      month_num <- sprintf("%02d", which(MONTHS == month))
      input_files <- list.files(use_input_dir, pattern = sprintf("(?i)No%s|_%s_|%s", month_num, month, month),
                                full.names = TRUE)
    }
    if (length(input_files) == 0) next

    input_file <- input_files[1]
    input_text <- read_file_safe(input_file)

    # Parse CONTENTS
    contents <- parse_contents(input_text)
    if (is.null(contents) || nrow(contents) == 0) {
      cat(sprintf("  %s: could not parse CONTENTS, skipping\n", month))
      next
    }

    # Find body text start
    body_start <- find_body_start(input_text)
    body_text <- str_sub(input_text, body_start, nchar(input_text))

    # Build position map: find each CONTENTS title in body text
    contents$body_pos <- NA_integer_
    search_from <- 1L
    for (i in seq_len(nrow(contents))) {
      pos <- find_title_in_body(body_text, contents$title[i], contents$author[i], search_from)
      if (!is.na(pos)) {
        contents$body_pos[i] <- pos
        search_from <- pos + 10L  # ensure monotonic ordering
      }
    }

    found_count <- sum(!is.na(contents$body_pos))
    cat(sprintf("  %s: %d/%d CONTENTS titles located in body text\n",
                month, found_count, nrow(contents)))

    # Now check each article file
    art_files <- list.files(month_dir, pattern = "^V\\d+.*\\.txt$", full.names = TRUE)
    art_files <- art_files[!str_detect(basename(art_files), "_(BOARD|CONTENTS|Misc)\\.txt$")]

    month_fixed <- 0

    for (f in art_files) {
      fname <- basename(f)
      total_checked <- total_checked + 1

      current_text <- read_file_safe(f)

      # Extract article number from filename: V39_03_01_...
      art_num <- as.integer(str_match(fname, "V\\d+_\\d+_(\\d+)_")[1,2])
      if (is.na(art_num)) next

      # Check if this file has the [Title by Author] header
      has_header <- str_detect(current_text, "^\\s*\\[.+\\]")
      header_line <- ""
      body_content <- current_text
      if (has_header) {
        header_line <- str_match(current_text, "^\\s*(\\[.+?\\])")[1,2]
        body_content <- str_trim(str_sub(current_text, nchar(header_line) + 1))
      }

      # Try to match this article to a CONTENTS entry
      # Use the article's content to find its position in the input
      # Take a distinctive phrase from the first 200 chars of body content
      first_200 <- str_sub(body_content, 1, min(200, nchar(body_content)))
      # Find a 30-char phrase that's likely unique
      phrase_start <- max(1, nchar(first_200) %/% 3)
      search_phrase <- str_sub(first_200, phrase_start, min(phrase_start + 40, nchar(first_200)))
      search_phrase <- str_trim(search_phrase)

      # Escape for regex
      search_phrase_esc <- str_replace_all(search_phrase, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      # Allow some flexibility for OCR differences
      search_phrase_esc <- str_replace_all(search_phrase_esc, "\\s+", "\\\\s+")

      # Find this phrase in the body text
      art_pos_in_body <- tryCatch({
        m <- str_locate(body_text, regex(search_phrase_esc, ignore_case = TRUE))
        if (!is.na(m[1,1])) m[1,1] else NA_integer_
      }, error = function(e) NA_integer_)

      if (is.na(art_pos_in_body)) next

      # Find the NEXT article's start position in the body text
      # Look at all CONTENTS entries with body_pos > current article's position
      later_entries <- contents[!is.na(contents$body_pos) & contents$body_pos > art_pos_in_body, ]

      if (nrow(later_entries) > 0) {
        next_art_pos <- min(later_entries$body_pos)

        # Extract the full article text from input
        full_article <- str_sub(body_text, art_pos_in_body, next_art_pos - 1L)
        full_article <- str_trim(full_article)

        # Compare lengths
        current_len <- nchar(body_content)
        full_len <- nchar(full_article)

        if (full_len > current_len + 50) {
          # Article was truncated — we have more text!
          # Clean running headers from the full text
          full_article_clean <- clean_running_headers(full_article, month, year)

          # Build the corrected file
          if (nchar(header_line) > 0) {
            corrected <- paste0(header_line, "\n\n", full_article_clean)
          } else {
            # Try to build a header from CONTENTS
            # Find which CONTENTS entry this matches
            matching_entry <- NULL
            for (ci in seq_len(nrow(contents))) {
              if (!is.na(contents$body_pos[ci]) &&
                  abs(contents$body_pos[ci] - art_pos_in_body) < 200) {
                matching_entry <- contents[ci, ]
                break
              }
            }
            if (!is.null(matching_entry)) {
              if (nchar(matching_entry$author) > 0) {
                header_line <- sprintf("[%s by %s]", matching_entry$title, matching_entry$author)
              } else {
                header_line <- sprintf("[%s]", matching_entry$title)
              }
            }
            corrected <- paste0(header_line, "\n\n", full_article_clean)
          }

          # Backup original
          backup_dir <- file.path(month_dir, "_boundary_fix_backups")
          if (!dir.exists(backup_dir)) dir.create(backup_dir)
          file.copy(f, file.path(backup_dir, fname), overwrite = FALSE)

          # Write corrected file
          write_file(corrected, f)

          added_chars <- full_len - current_len
          cat(sprintf("    FIXED: %s (+%d chars, was %d, now %d)\n",
                      fname, added_chars, current_len, full_len))
          month_fixed <- month_fixed + 1
          total_fixed <- total_fixed + 1

        } else if (!has_header && nrow(later_entries) > 0) {
          # Has correct content but missing header
          matching_entry <- NULL
          for (ci in seq_len(nrow(contents))) {
            if (!is.na(contents$body_pos[ci]) &&
                abs(contents$body_pos[ci] - art_pos_in_body) < 200) {
              matching_entry <- contents[ci, ]
              break
            }
          }
          if (!is.null(matching_entry)) {
            if (nchar(matching_entry$author) > 0) {
              new_header <- sprintf("[%s by %s]", matching_entry$title, matching_entry$author)
            } else {
              new_header <- sprintf("[%s]", matching_entry$title)
            }
            corrected <- paste0(new_header, "\n\n", body_content)

            # Backup and write
            backup_dir <- file.path(month_dir, "_boundary_fix_backups")
            if (!dir.exists(backup_dir)) dir.create(backup_dir)
            file.copy(f, file.path(backup_dir, fname), overwrite = FALSE)

            write_file(corrected, f)
            cat(sprintf("    HEADER ADDED: %s -> %s\n", fname, new_header))
            month_fixed <- month_fixed + 1
            total_fixed <- total_fixed + 1
          }
        }
      }
    }

    if (month_fixed > 0) {
      cat(sprintf("  %s: %d files fixed\n", month, month_fixed))
    }
  }
}

cat("\n")
cat("=", rep("=", 69), "\n", sep="")
cat(sprintf("  COMPLETE: %d files fixed out of %d checked\n", total_fixed, total_checked))
cat("  Backups saved in _boundary_fix_backups/ directories\n")
cat("=", rep("=", 69), "\n", sep="")
