# ============================================================
# BATCH RE-EXTRACTOR — Process all problem volumes/months
# Runs month_reextractor logic for each configured vol/month
# Then cleans up old files.
# ============================================================

library(stringr)
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"

# ---- VOLUMES TO PROCESS ----
JOBS <- list(
  list(vol=36L, year=1949L, months=NULL),  # NULL = all months
  list(vol=37L, year=1950L, months=NULL),
  list(vol=38L, year=1951L, months=NULL),
  list(vol=39L, year=1952L, months=c("May","June","July","August","September","October","November","December"))
)

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")

# ---- Source the re-extractor functions ----
# (Copy key functions from month_reextractor.R)

read_file_safe <- function(path) {
  tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) read_file(path, locale=locale(encoding="windows-1252"))
  )
}

title_to_regex <- function(title) {
  pat <- str_replace_all(title, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
  pat <- str_replace_all(pat, "\\s+", "\\\\s+")
  pat <- str_replace_all(pat, "[\"'\u2018\u2019\u201c\u201d]", ".?")
  pat <- str_replace_all(pat, "[-\u2014\u2013]", ".{1,3}")
  pat
}

find_title <- function(text, title, search_from = 1) {
  if (nchar(title) < 3) return(NA_integer_)
  search_text <- str_sub(text, search_from, nchar(text))
  pat <- title_to_regex(title)
  m <- tryCatch(str_locate(search_text, regex(pat, ignore_case = TRUE)),
                error = function(e) matrix(NA, 1, 2))
  if (!is.na(m[1,1])) return(search_from + m[1,1] - 1L)
  words <- str_split(title, "[\\s\u2014\u2013\\-]+")[[1]]
  words <- str_replace_all(words, "[^A-Za-z0-9]", "")
  words <- words[nchar(words) > 3]
  stops <- c("the","and","for","from","with","that","this","have","been",
             "were","will","upon","into","unto")
  words <- words[!tolower(words) %in% stops]
  if (length(words) >= 2) {
    for (n in min(length(words), 4):2) {
      esc_words <- str_replace_all(words[1:n], "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      pat2 <- paste(esc_words, collapse = ".{0,60}")
      m2 <- tryCatch(str_locate(search_text, regex(pat2, ignore_case = TRUE)),
                     error = function(e) matrix(NA, 1, 2))
      if (!is.na(m2[1,1])) return(search_from + m2[1,1] - 1L)
    }
  }
  if (length(words) >= 1) {
    longest_word <- words[which.max(nchar(words))]
    if (nchar(longest_word) >= 6) {
      esc_w <- str_replace_all(longest_word, "([\\[\\](){}.*+?^$|\\\\])", "\\\\\\1")
      pat3 <- sprintf("(?:^|\\n)\\s*%s", esc_w)
      m3 <- tryCatch(str_locate(search_text, regex(pat3, ignore_case = TRUE)),
                     error = function(e) matrix(NA, 1, 2))
      if (!is.na(m3[1,1])) return(search_from + m3[1,1] - 1L)
    }
  }
  NA_integer_
}

clean_headers <- function(text, month, year) {
  mu <- toupper(month)
  text <- str_replace_all(text, "\\bPage\\s+\\d{1,3}\\b", "")
  pat1 <- sprintf("\\b\\d{1,3}\\s+%s\\s+%d\\b", mu, year)
  text <- str_replace_all(text, regex(pat1, ignore_case=TRUE), "")
  pat2 <- sprintf("RELIEF SOCIETY MAGAZINE[\\-\u2014\u2013]+%s\\s+%d", mu, year)
  text <- str_replace_all(text, regex(pat2, ignore_case=TRUE), "")
  text <- str_replace_all(text, "\n{3,}", "\n\n")
  str_trim(text)
}

short_title <- function(title) {
  clean <- str_replace_all(title, "[<>:\"/\\\\|?*,;()\\[\\]\u2014\u2013'\u2018\u2019\u201c\u201d`]", "")
  clean <- str_trim(clean)
  words <- str_split(clean, "\\s+")[[1]]
  words <- words[nchar(words) > 0]
  if (length(words) == 0) return("untitled")
  snippet <- paste(head(words, 3), collapse = "_")
  if (nchar(snippet) > 18) snippet <- str_sub(snippet, 1, 18)
  str_replace(snippet, "_+$", "")
}

classify_type <- function(title, category) {
  t <- tolower(title)
  if (str_detect(t, "poem|poetry|frontispiece|verse")) return("Poem")
  if (category == "Poetry") return("Poem")
  if (category == "Fiction") return("Fict")
  "Art"
}

parse_contents <- function(input_text) {
  lines <- str_split(input_text, "\n")[[1]]
  ci <- which(str_detect(lines, "(?i)^\\s*Contents\\s*$"))
  if (length(ci) == 0) {
    ci <- which(str_detect(lines, "(?i)^Contents"))
    if (length(ci) == 0) {
      ci <- which(str_detect(lines, "(?i)Contents\\s*SPECIAL|Contents\\s*FICTION"))
      if (length(ci) == 0) return(NULL)
    }
  }
  ci <- ci[1]
  for (li in ci:min(length(lines), ci+80)) {
    lines[li] <- str_replace_all(lines[li],
      "(\\d{3})(SPECIAL FEATURES|FICTION|GENERAL FEATURES|FEATURES FOR THE HOME|POETRY|LESSONS?|APRIL SHORT STORIES|SERIALS|DEPARTMENTS?)([A-Z])",
      "\\1\n\\2\n\\3")
    lines[li] <- str_replace_all(lines[li], "Contents(SPECIAL|FICTION|GENERAL|FEATURES|POETRY)", "Contents\n\\1")
  }
  lines <- str_split(paste(lines, collapse="\n"), "\n")[[1]]
  ci <- which(str_detect(lines, "(?i)^\\s*Contents\\s*$|(?i)Contents\\s*$"))
  if (length(ci) == 0) ci <- which(str_detect(lines, "(?i)Contents"))[1]
  else ci <- ci[1]
  end_i <- length(lines)
  for (i in (ci+1):min(length(lines), ci+80)) {
    if (str_detect(lines[i], "(?i)^\\s*PUBLISHED\\s+MONTHLY")) { end_i <- i-1; break }
  }
  current_cat <- "Article"
  entries <- list()
  for (line in lines[ci:end_i]) {
    line <- str_trim(line)
    if (nchar(line) == 0 || str_detect(line, "(?i)^\\s*Contents\\s*$")) next
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
    raw_entries <- str_split(line, "(?<=\\d{1,3})\\s+(?=[A-Z\"\u201c])")[[1]]
    for (re in raw_entries) {
      re <- str_trim(re)
      if (nchar(re) < 4) next
      m <- str_match(re, "^(.+?)\\s+by\\s+([A-Z][A-Za-z .]+?)\\s+(\\d{1,3})$")
      if (!is.na(m[1,1])) {
        entries <- c(entries, list(list(title=str_trim(m[1,2]), author=str_trim(m[1,3]),
                                        page=as.integer(m[1,4]), category=current_cat)))
        next
      }
      m2 <- str_match(re, "^(.+?)\\s+(\\d{1,3})$")
      if (!is.na(m2[1,1]) && nchar(str_trim(m2[1,2])) > 3) {
        entries <- c(entries, list(list(title=str_trim(m2[1,2]), author="",
                                        page=as.integer(m2[1,3]), category=current_cat)))
        next
      }
      m3 <- str_match(re, "^(.+?),\\s+by\\s+([A-Z][A-Za-z .]+?),?\\s+(\\d{1,3})$")
      if (!is.na(m3[1,1])) {
        entries <- c(entries, list(list(title=str_trim(m3[1,2]), author=str_trim(m3[1,3]),
                                        page=as.integer(m3[1,4]), category="Poetry")))
      }
    }
  }
  if (length(entries) > 0) {
    pages <- sapply(entries, function(e) e$page)
    entries <- entries[order(pages)]
  }
  entries
}

# ============================================================
# MAIN BATCH LOOP
# ============================================================
cat("=", rep("=", 69), "\n", sep="")
cat("  BATCH RE-EXTRACTOR — Multiple Volumes\n")
cat("=", rep("=", 69), "\n\n", sep="")

grand_total <- 0

for (job in JOBS) {
  VOLUME <- job$vol
  YEAR <- job$year
  target_months <- job$months

  INPUT_DIR <- file.path(BASE, "input", sprintf("Vol%dsplit_preprocessed", VOLUME))
  if (!dir.exists(INPUT_DIR)) {
    INPUT_DIR <- file.path(BASE, "input", sprintf("Vol%dsplit", VOLUME))
  }
  OUTPUT_DIR <- file.path(BASE, "OutputExtractedarticles", sprintf("Vol%d_%d", VOLUME, YEAR))

  if (!dir.exists(OUTPUT_DIR)) {
    cat(sprintf("\n=== Vol %d: output dir not found, skipping ===\n", VOLUME))
    next
  }

  cat(sprintf("\n=== Vol %d (%d) ===\n", VOLUME, YEAR))

  for (month in MONTHS) {
    if (!is.null(target_months) && !(month %in% target_months)) next

    month_dir <- file.path(OUTPUT_DIR, month)
    if (!dir.exists(month_dir)) next

    # Find input file
    input_files <- list.files(INPUT_DIR, pattern = sprintf("(?i)%s.*\\.txt$", month), full.names = TRUE)
    if (length(input_files) == 0) {
      month_num <- sprintf("%02d", which(MONTHS == month))
      input_files <- list.files(INPUT_DIR, pattern = sprintf("(?i)No%s|%s", month_num, month), full.names = TRUE)
    }
    if (length(input_files) == 0) next

    input_text <- read_file_safe(input_files[1])
    entries <- parse_contents(input_text)
    if (is.null(entries) || length(entries) == 0) {
      cat(sprintf("  %s: no CONTENTS parsed, skip\n", month))
      next
    }

    # Find body start
    body_start <- 1L
    m_pub <- str_locate(input_text, "(?i)Acceptance for mailing")
    if (!is.na(m_pub[1,1])) {
      post <- str_sub(input_text, m_pub[1,2], min(m_pub[1,2]+200, nchar(input_text)))
      nl <- str_locate(post, "\n\\s*\n")
      if (!is.na(nl[1,1])) body_start <- m_pub[1,2] + nl[1,2]
      else body_start <- m_pub[1,2] + 1L
    }
    body_text <- str_sub(input_text, body_start, nchar(input_text))

    # Find titles in body
    positions <- list()
    found <- 0L
    for (i in seq_along(entries)) {
      e <- entries[[i]]
      pos <- find_title(body_text, e$title, 1L)
      if (!is.na(pos) && (pos > 200 || found == 0)) {
        positions <- c(positions, list(list(idx=i, pos=pos, entry=e)))
        found <- found + 1L
      }
    }
    if (length(positions) > 1) {
      pos_order <- order(sapply(positions, function(p) p$pos))
      positions <- positions[pos_order]
    }
    if (length(positions) > 1) {
      keep <- rep(TRUE, length(positions))
      for (k in 2:length(positions)) {
        if (abs(positions[[k]]$pos - positions[[k-1]]$pos) < 20) keep[k] <- FALSE
      }
      positions <- positions[keep]
      found <- sum(keep)
    }

    if (found < 3) {
      cat(sprintf("  %s: only %d/%d titles found, skip\n", month, found, length(entries)))
      next
    }

    # Backup existing files
    backup_dir <- file.path(month_dir, "_reextract_backups")
    if (!dir.exists(backup_dir)) dir.create(backup_dir)
    existing <- list.files(month_dir, pattern = sprintf("^V%d_", VOLUME), full.names = TRUE)
    existing <- existing[!str_detect(basename(existing), "_(BOARD|CONTENTS|Misc)\\.txt$")]
    for (ef in existing) {
      dest <- file.path(backup_dir, basename(ef))
      if (!file.exists(dest)) file.copy(ef, dest)
    }

    # Extract articles
    month_num <- sprintf("%02d", which(MONTHS == month))
    new_files <- 0L
    for (pi in seq_along(positions)) {
      p <- positions[[pi]]
      e <- p$entry
      start_pos <- p$pos
      end_pos <- if (pi < length(positions)) positions[[pi+1]]$pos - 1L else nchar(body_text)
      article_text <- str_trim(str_sub(body_text, start_pos, end_pos))
      article_text <- clean_headers(article_text, month, YEAR)
      if (nchar(article_text) < 30) next
      header <- if (nchar(e$author) > 0) sprintf("[%s by %s]", e$title, e$author) else sprintf("[%s]", e$title)
      art_type <- classify_type(e$title, e$category)
      title_short <- short_title(e$title)
      author_last <- if (nchar(e$author) > 0) {
        al <- tail(str_split(e$author, "\\s+")[[1]], 1)
        if (nchar(al) > 8) str_sub(al, 1, 8) else al
      } else ""
      fname <- if (nchar(author_last) > 0) {
        sprintf("V%d_%s_%02d_%s_%s_%s.txt", VOLUME, month_num, pi, art_type, title_short, author_last)
      } else {
        sprintf("V%d_%s_%02d_%s_%s.txt", VOLUME, month_num, pi, art_type, title_short)
      }
      write_file(paste0(header, "\n\n", article_text), file.path(month_dir, fname))
      new_files <- new_files + 1L
    }

    # Remove old files
    backed_up <- list.files(backup_dir, pattern = "\\.txt$")
    all_files <- list.files(month_dir, pattern = sprintf("^V%d_", VOLUME), full.names = TRUE)
    all_files <- all_files[!str_detect(basename(all_files), "_(BOARD|CONTENTS|Misc)\\.txt$")]
    new_pattern <- sprintf("^V%d_%s_\\d+_", VOLUME, month_num)
    for (af in all_files) {
      if (!str_detect(basename(af), new_pattern) && basename(af) %in% backed_up) {
        file.remove(af)
      }
    }

    cat(sprintf("  %s: %d/%d titles -> %d files written\n", month, found, length(entries), new_files))
    grand_total <- grand_total + new_files
  }
}

cat("\n")
cat("=", rep("=", 69), "\n", sep="")
cat(sprintf("  BATCH COMPLETE: %d total files written\n", grand_total))
cat("=", rep("=", 69), "\n", sep="")
