# ============================================================
# PER-MONTH RE-EXTRACTOR
# Re-extracts articles from input text using title search
# instead of page estimation. For months with boundary issues.
# NO API calls.
# ============================================================

library(stringr)
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"

# ---- CONFIGURATION: Set volume/month to process ----
VOLUME  <- 39L
YEAR    <- 1952L
TARGET_MONTHS <- c("April")  # Set to NULL for all months

INPUT_DIR  <- file.path(BASE, "input", sprintf("Vol%dsplit_preprocessed", VOLUME))
OUTPUT_DIR <- file.path(BASE, "OutputExtractedarticles", sprintf("Vol%d_%d", VOLUME, YEAR))

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

# ---- Utilities ----

read_file_safe <- function(path) {
  tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) read_file(path, locale=locale(encoding="windows-1252"))
  )
}

# Build a regex pattern from a title, tolerant of OCR errors
title_to_regex <- function(title) {
  # Escape regex specials
  pat <- str_replace_all(title, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
  # Allow flexible whitespace
  pat <- str_replace_all(pat, "\\s+", "\\\\s+")
  # Allow flexible quotes/dashes
  pat <- str_replace_all(pat, "[\"'\u2018\u2019\u201c\u201d]", ".?")
  pat <- str_replace_all(pat, "[-\u2014\u2013]", ".{1,3}")
  pat
}

# Search for title in text, return start position
find_title <- function(text, title, search_from = 1) {
  if (nchar(title) < 3) return(NA_integer_)
  search_text <- str_sub(text, search_from, nchar(text))

  # Strategy 1: Full title (with OCR tolerance)
  pat <- title_to_regex(title)
  m <- tryCatch(
    str_locate(search_text, regex(pat, ignore_case = TRUE)),
    error = function(e) matrix(NA, 1, 2)
  )
  if (!is.na(m[1,1])) return(search_from + m[1,1] - 1L)

  # Strategy 2: Key words from title (with flexible gaps)
  words <- str_split(title, "[\\s\u2014\u2013\\-]+")[[1]]
  words <- str_replace_all(words, "[^A-Za-z0-9]", "")
  words <- words[nchar(words) > 3]
  stops <- c("the","and","for","from","with","that","this","have","been",
             "were","will","upon","into","unto")
  words <- words[!tolower(words) %in% stops]

  if (length(words) >= 2) {
    for (n in min(length(words), 4):2) {
      use_words <- words[1:n]
      esc_words <- str_replace_all(use_words, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      pat2 <- paste(esc_words, collapse = ".{0,60}")
      m2 <- tryCatch(
        str_locate(search_text, regex(pat2, ignore_case = TRUE)),
        error = function(e) matrix(NA, 1, 2)
      )
      if (!is.na(m2[1,1])) return(search_from + m2[1,1] - 1L)
    }
  }

  # Strategy 3: Single longest significant word (for short titles)
  if (length(words) >= 1) {
    longest_word <- words[which.max(nchar(words))]
    if (nchar(longest_word) >= 6) {
      esc_w <- str_replace_all(longest_word, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      # Look for the word at start of a line or after whitespace, followed by author-like text
      pat3 <- sprintf("(?:^|\\n)\\s*%s", esc_w)
      m3 <- tryCatch(
        str_locate(search_text, regex(pat3, ignore_case = TRUE)),
        error = function(e) matrix(NA, 1, 2)
      )
      if (!is.na(m3[1,1])) return(search_from + m3[1,1] - 1L)
    }
  }

  NA_integer_
}

# Clean running headers from extracted text
clean_headers <- function(text, month, year) {
  mu <- toupper(month)
  # Remove "Page NNN" or "Page NNN NNN"
  text <- str_replace_all(text, "\\bPage\\s+\\d{1,3}\\b", "")
  # Remove "NNN MONTH YEAR"
  pat1 <- sprintf("\\b\\d{1,3}\\s+%s\\s+%d\\b", mu, year)
  text <- str_replace_all(text, regex(pat1, ignore_case=TRUE), "")
  # Remove "RELIEF SOCIETY MAGAZINE—MONTH YEAR"
  pat2 <- sprintf("RELIEF SOCIETY MAGAZINE[\\-\u2014\u2013]+%s\\s+%d", mu, year)
  text <- str_replace_all(text, regex(pat2, ignore_case=TRUE), "")
  text <- str_replace_all(text, "\n{3,}", "\n\n")
  str_trim(text)
}

# Short title for filename (max 18 chars)
short_title <- function(title) {
  clean <- str_replace_all(title, "[<>:\"/\\\\|?*,;()\\[\\]\u2014\u2013'\u2018\u2019\u201c\u201d`]", "")
  clean <- str_trim(clean)
  words <- str_split(clean, "\\s+")[[1]]
  words <- words[nchar(words) > 0]
  if (length(words) == 0) return("untitled")
  # Take first 2-3 words
  snippet <- paste(head(words, 3), collapse = "_")
  if (nchar(snippet) > 18) snippet <- str_sub(snippet, 1, 18)
  str_replace(snippet, "_+$", "")
}

# Classify article type from title
classify_type <- function(title, category) {
  t <- tolower(title)
  if (str_detect(t, "poem|poetry|frontispiece|verse")) return("Poem")
  if (category == "Poetry") return("Poem")
  if (category == "Fiction") return("Fict")
  if (str_detect(t, "editorial")) return("Art")
  if (str_detect(t, "lesson|theology|visiting teacher|work meeting|literature|social science|music")) return("Art")
  "Art"
}

# ---- Parse CONTENTS ----
parse_contents <- function(input_text) {
  lines <- str_split(input_text, "\n")[[1]]

  # Find Contents line - handle "Contents" jammed with other text
  ci <- which(str_detect(lines, "(?i)^\\s*Contents\\s*$"))
  if (length(ci) == 0) {
    ci <- which(str_detect(lines, "(?i)^Contents"))
    if (length(ci) == 0) {
      # Try: "No. NContents" (jammed together)
      ci <- which(str_detect(lines, "(?i)Contents\\s*SPECIAL|Contents\\s*FICTION"))
      if (length(ci) == 0) return(NULL)
    }
  }
  ci <- ci[1]

  # Pre-process: split jammed category headers in the contents lines
  # e.g., "246APRIL SHORT STORIESSmitty by..." -> "246\nAPRIL SHORT STORIES\nSmitty by..."
  # e.g., "274SERIALSDear Conquest" -> "274\nSERIALS\nDear Conquest"
  # e.g., "268POETRYConcerto" -> "268\nPOETRY\nConcerto"
  for (li in ci:min(length(lines), ci+80)) {
    lines[li] <- str_replace_all(lines[li],
      "(\\d{3})(SPECIAL FEATURES|FICTION|GENERAL FEATURES|FEATURES FOR THE HOME|POETRY|LESSONS?|APRIL SHORT STORIES|SERIALS|DEPARTMENTS?)([A-Z])",
      "\\1\n\\2\n\\3")
    # Also split "ContentsSPECIAL" -> "Contents\nSPECIAL"
    lines[li] <- str_replace_all(lines[li], "Contents(SPECIAL|FICTION|GENERAL|FEATURES|POETRY)", "Contents\n\\1")
  }
  # Re-split lines since we added newlines
  lines <- str_split(paste(lines, collapse="\n"), "\n")[[1]]
  # Re-find Contents line after re-splitting
  ci <- which(str_detect(lines, "(?i)^\\s*Contents\\s*$|(?i)Contents\\s*$"))
  if (length(ci) == 0) ci <- which(str_detect(lines, "(?i)Contents"))[1]
  else ci <- ci[1]

  # Find end of contents (PUBLISHED MONTHLY line)
  end_i <- length(lines)
  for (i in (ci+1):min(length(lines), ci+80)) {
    if (str_detect(lines[i], "(?i)^\\s*PUBLISHED\\s+MONTHLY")) {
      end_i <- i - 1
      break
    }
  }

  current_cat <- "Article"
  entries <- list()

  for (line in lines[ci:end_i]) {
    line <- str_trim(line)
    if (nchar(line) == 0 || str_detect(line, "(?i)^\\s*Contents\\s*$")) next

    # Category header check (now also handles APRIL SHORT STORIES, SERIALS)
    if (str_detect(line, "(?i)^(SPECIAL FEATURES|FICTION|GENERAL FEATURES|FEATURES FOR THE HOME|POETRY|LESSONS?|APRIL SHORT STORIES|SERIALS|DEPARTMENTS?)\\s*$")) {
      if (str_detect(line, "(?i)FICTION|SHORT STOR")) current_cat <- "Fiction"
      else if (str_detect(line, "(?i)POETRY")) current_cat <- "Poetry"
      else if (str_detect(line, "(?i)FEATURES FOR")) current_cat <- "Home"
      else if (str_detect(line, "(?i)GENERAL")) current_cat <- "General"
      else if (str_detect(line, "(?i)SPECIAL")) current_cat <- "Special"
      else if (str_detect(line, "(?i)SERIAL")) current_cat <- "Fiction"
      else current_cat <- "Article"
      next
    }

    # Multi-entry lines: split on page numbers
    # Pattern: "Title by Author PAGE Title by Author PAGE"
    # Split: find "NNN " patterns that mark entry boundaries
    # Each entry is: title text [by author] page_number
    raw_entries <- str_split(line, "(?<=\\d{1,3})\\s+(?=[A-Z\"\u201c])")[[1]]

    for (re in raw_entries) {
      re <- str_trim(re)
      if (nchar(re) < 4) next

      # Try: "Title by Author PAGE"
      m <- str_match(re, "^(.+?)\\s+by\\s+([A-Z][A-Za-z .]+?)\\s+(\\d{1,3})$")
      if (!is.na(m[1,1])) {
        entries <- c(entries, list(list(
          title = str_trim(m[1,2]), author = str_trim(m[1,3]),
          page = as.integer(m[1,4]), category = current_cat
        )))
        next
      }

      # Try: "Title PAGE" (no author)
      m2 <- str_match(re, "^(.+?)\\s+(\\d{1,3})$")
      if (!is.na(m2[1,1]) && nchar(str_trim(m2[1,2])) > 3) {
        entries <- c(entries, list(list(
          title = str_trim(m2[1,2]), author = "",
          page = as.integer(m2[1,3]), category = current_cat
        )))
        next
      }

      # Poetry: "Title, by Author, PAGE"
      m3 <- str_match(re, "^(.+?),\\s+by\\s+([A-Z][A-Za-z .]+?),?\\s+(\\d{1,3})$")
      if (!is.na(m3[1,1])) {
        entries <- c(entries, list(list(
          title = str_trim(m3[1,2]), author = str_trim(m3[1,3]),
          page = as.integer(m3[1,4]), category = "Poetry"
        )))
      }
    }
  }

  # Sort by page
  if (length(entries) > 0) {
    pages <- sapply(entries, function(e) e$page)
    entries <- entries[order(pages)]
  }

  entries
}

# ============================================================
# MAIN PROCESSING
# ============================================================

cat("=", rep("=", 69), "\n", sep="")
cat(sprintf("  PER-MONTH RE-EXTRACTOR — Vol %d (%d)\n", VOLUME, YEAR))
cat("=", rep("=", 69), "\n\n", sep="")

for (month in MONTHS) {
  if (!is.null(TARGET_MONTHS) && !(month %in% TARGET_MONTHS)) next

  month_dir <- file.path(OUTPUT_DIR, month)
  if (!dir.exists(month_dir)) {
    cat(sprintf("  %s: output directory not found, skipping\n", month))
    next
  }

  # Find input file
  input_files <- list.files(INPUT_DIR, pattern = sprintf("(?i)%s.*\\.txt$", month), full.names = TRUE)
  if (length(input_files) == 0) {
    month_num <- sprintf("%02d", which(MONTHS == month))
    input_files <- list.files(INPUT_DIR, pattern = sprintf("(?i)No%s|%s", month_num, month), full.names = TRUE)
  }
  if (length(input_files) == 0) {
    cat(sprintf("  %s: no input file found\n", month))
    next
  }

  input_text <- read_file_safe(input_files[1])
  cat(sprintf("\n--- %s ---\n", month))
  cat(sprintf("  Input: %s (%d chars)\n", basename(input_files[1]), nchar(input_text)))

  # Parse CONTENTS
  entries <- parse_contents(input_text)
  if (is.null(entries) || length(entries) == 0) {
    cat("  Could not parse CONTENTS\n")
    next
  }
  cat(sprintf("  CONTENTS entries: %d\n", length(entries)))

  # Find body text start (after PUBLISHED/Entered section)
  body_start <- 1L
  m_pub <- str_locate(input_text, "(?i)Acceptance for mailing")
  if (!is.na(m_pub[1,1])) {
    # Find next blank line after this
    post <- str_sub(input_text, m_pub[1,2], min(m_pub[1,2] + 200, nchar(input_text)))
    nl <- str_locate(post, "\n\\s*\n")
    if (!is.na(nl[1,1])) {
      body_start <- m_pub[1,2] + nl[1,2]
    } else {
      body_start <- m_pub[1,2] + 1L
    }
  }

  body_text <- str_sub(input_text, body_start, nchar(input_text))
  cat(sprintf("  Body starts at char %d (%d body chars)\n", body_start, nchar(body_text)))

  # Find each CONTENTS title in body text
  # Search the FULL body for each title (don't enforce monotonic order)
  positions <- list()
  found <- 0L
  for (i in seq_along(entries)) {
    e <- entries[[i]]
    pos <- find_title(body_text, e$title, 1L)  # always search from start
    if (!is.na(pos)) {
      # Verify this isn't a CONTENTS reference (skip first 200 chars of body)
      if (pos > 200 || found == 0) {
        positions <- c(positions, list(list(idx=i, pos=pos, entry=e)))
        found <- found + 1L
      }
    }
  }

  # Sort positions by their actual body position (text order, not page order)
  if (length(positions) > 1) {
    pos_order <- order(sapply(positions, function(p) p$pos))
    positions <- positions[pos_order]
  }

  # Remove duplicate positions (same position matched by multiple titles)
  if (length(positions) > 1) {
    keep <- rep(TRUE, length(positions))
    for (k in 2:length(positions)) {
      if (abs(positions[[k]]$pos - positions[[k-1]]$pos) < 20) {
        keep[k] <- FALSE
      }
    }
    positions <- positions[keep]
    found <- sum(keep)
  }

  cat(sprintf("  Titles found in body: %d/%d\n", found, length(entries)))

  if (found < 3) {
    cat("  Too few titles found, skipping\n")
    next
  }

  # Print what we found
  for (p in positions) {
    e <- p$entry
    snippet <- str_sub(body_text, p$pos, min(p$pos + 50, nchar(body_text)))
    snippet <- str_replace_all(snippet, "\n", " ")
    cat(sprintf("    [p%d pos=%d] %s => %.50s...\n",
                e$page, p$pos, e$title, snippet))
  }

  # Backup existing files
  backup_dir <- file.path(month_dir, "_reextract_backups")
  if (!dir.exists(backup_dir)) dir.create(backup_dir)

  existing_files <- list.files(month_dir, pattern = "^V\\d+.*\\.txt$", full.names = TRUE)
  existing_files <- existing_files[!str_detect(basename(existing_files), "_(BOARD|CONTENTS|Misc)\\.txt$")]
  for (ef in existing_files) {
    dest <- file.path(backup_dir, basename(ef))
    if (!file.exists(dest)) file.copy(ef, dest)
  }
  cat(sprintf("  Backed up %d existing article files\n", length(existing_files)))

  # Now extract articles based on found positions
  # For each found title, extract from its position to the next found title's position
  month_num <- sprintf("%02d", which(MONTHS == month))
  new_files <- 0L

  for (pi in seq_along(positions)) {
    p <- positions[[pi]]
    e <- p$entry

    # Start position
    start_pos <- p$pos

    # End position: next found title's position (or end of body)
    if (pi < length(positions)) {
      end_pos <- positions[[pi + 1]]$pos - 1L
    } else {
      end_pos <- nchar(body_text)
    }

    # Extract text
    article_text <- str_sub(body_text, start_pos, end_pos)
    article_text <- str_trim(article_text)

    # Clean running headers
    article_text <- clean_headers(article_text, month, YEAR)

    # Skip very short extracts (likely poems we'll handle separately)
    if (nchar(article_text) < 30) next

    # Build header
    if (nchar(e$author) > 0) {
      header <- sprintf("[%s by %s]", e$title, e$author)
    } else {
      header <- sprintf("[%s]", e$title)
    }

    # Build filename
    art_type <- classify_type(e$title, e$category)
    title_short <- short_title(e$title)
    # Get author last name
    if (nchar(e$author) > 0) {
      author_parts <- str_split(e$author, "\\s+")[[1]]
      author_last <- tail(author_parts, 1)
      if (nchar(author_last) > 8) author_last <- str_sub(author_last, 1, 8)
    } else {
      author_last <- ""
    }
    fname_base <- if (nchar(author_last) > 0) {
      sprintf("V%d_%s_%02d_%s_%s_%s", VOLUME, month_num, pi, art_type, title_short, author_last)
    } else {
      sprintf("V%d_%s_%02d_%s_%s", VOLUME, month_num, pi, art_type, title_short)
    }
    fname <- paste0(fname_base, ".txt")

    # Write file
    file_content <- paste0(header, "\n\n", article_text)
    write_file(file_content, file.path(month_dir, fname))
    new_files <- new_files + 1L
    cat(sprintf("  WROTE: %s (%d chars)\n", fname, nchar(article_text)))
  }

  # Remove old article files that don't match new naming
  # (Keep BOARD, CONTENTS, Misc, and backup dirs)
  old_files <- list.files(month_dir, pattern = "^V\\d+.*\\.txt$", full.names = TRUE)
  old_files <- old_files[!str_detect(basename(old_files), "_(BOARD|CONTENTS|Misc)\\.txt$")]
  new_fnames <- list.files(month_dir, pattern = sprintf("^V%d_%s_\\d+_", VOLUME, month_num))
  new_fnames_full <- file.path(month_dir, new_fnames)

  # Find old files that aren't in the new set
  to_remove <- setdiff(old_files, new_fnames_full)
  # Only remove files that were backed up
  backed_up <- list.files(backup_dir, pattern = "\\.txt$")
  for (tf in to_remove) {
    if (basename(tf) %in% backed_up) {
      file.remove(tf)
      cat(sprintf("  REMOVED old: %s\n", basename(tf)))
    }
  }

  cat(sprintf("\n  %s complete: %d new article files written\n", month, new_files))
}

cat("\n")
cat("=", rep("=", 69), "\n", sep="")
cat("  RE-EXTRACTION COMPLETE\n")
cat("  Backups in _reextract_backups/ directories\n")
cat("=", rep("=", 69), "\n", sep="")
