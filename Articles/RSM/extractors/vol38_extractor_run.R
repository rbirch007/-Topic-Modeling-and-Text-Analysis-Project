## ----setup, message=FALSE, warning=FALSE--------------------------------------
for (pkg in c("stringr","readr","knitr","kableExtra")) {
  if (!requireNamespace(pkg, quietly=TRUE)) install.packages(pkg)
}
library(stringr); library(readr); library(knitr); library(kableExtra)

# ============================================================
# CONFIGURATION — Vol 39 (1952) Re-extraction
# Proof of concept: April only (set MONTHS_FILTER)
# To run all months: set MONTHS_FILTER <- NULL
# ============================================================
VOLUME  <- 38L
YEAR    <- 1951L
INPUT_DIR  <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/input/Vol38split"
OUTPUT_DIR <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/output"

# Filter to specific months (NULL = all months)
MONTHS_FILTER <- c("March")

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")
MONTH_UPPER <- toupper(MONTHS)

if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive=TRUE)

# Toggle line splitting of mega-lines in input files
SPLIT_INPUT_LINES <- TRUE


## ----preprocess-line-splitter-------------------------------------------------
# ============================================================
# PREPROCESSING: Split mega-lines in input files
# ============================================================
# OCR output often produces one continuous line for the entire body text
# (140K-185K chars). This step splits those mega-lines at sentence
# boundaries for readability and easier debugging.
#
# Split points:
#   - Period + space + capital letter  (sentence endings)
#   - Question mark + space + capital  (questions)
#   - Exclamation + space + capital    (exclamations)
#   - Bare numbers (\d{2,3}) preceding a capital letter
#     (embedded page numbers / running headers)
# ============================================================

split_mega_line <- function(text, threshold = 10000L) {
  # Only split lines longer than threshold
  if (nchar(text) <= threshold) return(text)

  # 1. Split on sentence endings: ". A" -> ".\nA"
  text <- str_replace_all(text, "(\\.\\s)([A-Z])", "\\1\n\\2")

  # 2. Split on question marks: "? A" -> "?\nA"
  text <- str_replace_all(text, "(\\?\\s)([A-Z])", "\\1\n\\2")

  # 3. Split on exclamation marks: "! A" -> "!\nA"
  text <- str_replace_all(text, "(!\\s)([A-Z])", "\\1\n\\2")

  # 4. Number-based splitting — added at END of sentence splitting
  #    Helps separate CONTENTS entries like "Bailey 10 Verdure" into
  #    separate lines. Applied after sentence splits so most body text
  #    is already broken into small lines. Body text is joined anyway
  #    in the extractor, so these extra breaks don't affect body parsing.
  text <- str_replace_all(text, "(\\s)(\\d{2,3}\\s+[A-Z])", "\\1\n\\2")

  text
}

preprocess_input_files <- function(input_dir, do_split = TRUE) {
  if (!do_split) {
    cat("Line splitting disabled (SPLIT_INPUT_LINES = FALSE)\n")
    return(invisible(NULL))
  }

  files <- list.files(input_dir, pattern = "\\.txt$", full.names = TRUE)
  cat(sprintf("Preprocessing %d input files for line splitting...\n", length(files)))

  for (f in files) {
    txt <- tryCatch(
      read_file(f, locale=locale(encoding="UTF-8")),
      error=function(e) read_file(f, locale=locale(encoding="windows-1252"))
    )
    lines <- str_split(txt, "\n")[[1]]

    # Find mega-lines (>10K chars) — handle encoding errors
    line_lens <- tryCatch(nchar(lines), error = function(e) nchar(lines, allowNA = TRUE))
    line_lens[is.na(line_lens)] <- 0L
    mega_idx <- which(line_lens > 10000)
    if (length(mega_idx) == 0) {
      cat(sprintf("  %s: no mega-lines (already split)\n", basename(f)))
      next
    }

    cat(sprintf("  %s: splitting %d mega-line(s)", basename(f), length(mega_idx)))
    for (idx in mega_idx) {
      original_len <- nchar(lines[idx])
      lines[idx] <- split_mega_line(lines[idx])
      new_line_count <- str_count(lines[idx], "\n") + 1L
      cat(sprintf(" [L%d: %s chars -> %d lines]", idx, format(original_len, big.mark=","), new_line_count))
    }
    cat("\n")

    # Write back with split lines
    write_file(paste(lines, collapse="\n"), f)
  }

  cat("Preprocessing complete.\n\n")
}

# Run the preprocessor
preprocess_input_files(INPUT_DIR, SPLIT_INPUT_LINES)


## ----helpers------------------------------------------------------------------
# ============================================================
# UTILITY FUNCTIONS
# ============================================================

read_file_safe <- function(path) {
  txt <- tryCatch(
    read_file(path, locale=locale(encoding="UTF-8")),
    error=function(e) read_file(path, locale=locale(encoding="windows-1252"))
  )
  ok <- tryCatch({ nchar(txt); TRUE }, error=function(e) FALSE)
  if (!ok) {
    txt <- read_file(path, locale=locale(encoding="latin1"))
    cat("  NOTE: Re-read as latin1 (non-UTF-8 bytes detected)\n")
  }
  txt
}

short_title_filename <- function(title) {
  clean <- str_replace_all(title,
    "[<>:\"/\\\\|?*,;()\\[\\]\r\n\u2014\u2013'\u2018\u2019\u201c\u201d`]", "")
  clean <- str_trim(clean)
  if (nchar(clean) == 0) return("untitled")
  words <- str_split(clean, "\\s+")[[1]]
  words <- words[nchar(words) > 0]
  if (length(words) == 0) return("untitled")
  author_last <- ""
  title_words <- words
  if (length(words) > 3) {
    for (n_auth in c(4L, 3L, 2L)) {
      if (n_auth >= length(words)) next
      cand <- tail(words, n_auth)
      is_name <- sapply(cand, function(w)
        str_detect(w, "^[A-Z]") ||
        w %in% c("de","van","von","del","la","le","da","di"))
      if (all(is_name)) {
        author_last <- tail(cand, 1)
        title_words <- head(words, length(words) - n_auth)
        break
      }
    }
  }
  snippet_parts <- character(0)
  n_chars <- 0L
  for (w in title_words) {
    if (n_chars + nchar(w) + 1L > 8L && length(snippet_parts) >= 1) break
    snippet_parts <- c(snippet_parts, w)
    n_chars <- n_chars + nchar(w) + 1L
  }
  snippet <- paste(snippet_parts, collapse = "_")
  result <- if (nchar(author_last) > 0 && nchar(snippet) > 0) {
    paste0(snippet, "_", author_last)
  } else if (nchar(snippet) > 0) {
    snippet
  } else {
    paste(head(words, min(3, length(words))), collapse = "_")
  }
  result <- str_replace_all(result, "\\s+", "_")
  result <- str_replace_all(result, "_+", "_")
  result <- str_replace(result, "^_+|_+$", "")
  if (nchar(result) > 18) result <- str_sub(result, 1, 18)
  result <- str_replace(result, "_+$", "")
  result
}

# Category abbreviation for shorter filenames (OneDrive path limits)
abbrev_category <- function(cat_label) {
  if (is.na(cat_label) || cat_label == "") return("Art")
  switch(cat_label,
    "General Feature" = "GenFt",
    "Special Feature" = "SpFt",
    "Poetry" = "Poem",
    "Fiction" = "Fict",
    "Lesson" = "Lssn",
    "Frontispiece" = "Frnt",
    "Article" = "Art",
    cat_label)
}

get_significant_words <- function(title) {
  stops <- c("the","a","an","of","in","on","at","to","for","and","or","but","by",
             "its","is","are","was","were","be","been","our","her","his","my","we",
             "it","as","with","from","that","this","not","no","out","new","all",
             "how","what","who","when","where","why","can","may","will","shall",
             "has","had","have","does","did","do","about","into","upon")
  words <- str_split(title, "[\\s\u2014\u2013]+|\\s*-+\\s*|'-")[[1]]
  words <- words[nchar(words) > 2]
  words <- words[!tolower(words) %in% stops]
  words <- str_replace(words, "[?.!,;:'\"]+$", "")
  words[nchar(words) > 0]
}

classify_vol50_article <- function(title, category) {
  t <- tolower(title)
  if (category != "") return(category)
  if (str_detect(t, "^editorial"))          return("Editorial")
  if (str_detect(t, "notes (from|to) the field")) return("Notes_from_Field")
  if (str_detect(t, "frontispiece"))        return("Frontispiece")
  if (str_detect(t, "^theology"))           return("Lesson")
  if (str_detect(t, "visiting teacher"))    return("Lesson")
  if (str_detect(t, "work meeting"))        return("Lesson")
  if (str_detect(t, "literature"))          return("Lesson")
  if (str_detect(t, "social science"))      return("Lesson")
  "Article"
}

get_metadata_vol50 <- function(filename) {
  list(
    volume = str_extract(filename, "(?i)(?<=Vol)\\d+"),
    number = str_extract(filename, "(?i)(?<=No)\\d+"),
    month  = str_extract(filename, paste0(
      "(?i)(January|February|March|April|May|June|July|",
      "August|September|October|November|December)")),
    year   = str_extract(filename, "\\d{4}")
  )
}


## ----page-index---------------------------------------------------------------
# ============================================================
# PAGE INDEX — find running header positions in body text
# Vol50 format: odd pages have "PAGE_NUM MONTH YEAR"
# (e.g., "5 JANUARY 1963", "85 FEBRUARY 1963")
# ============================================================

build_vol50_page_index <- function(body_text, body_offset, month_upper, year) {
  pages <- data.frame(page=integer(), position=integer(), stringsAsFactors=FALSE)

  # Odd-page headers: "NUM MONTH YEAR"
  pat <- paste0("\\b(\\d{1,3})\\s+", month_upper, "\\s+", year, "\\b")
  m <- str_locate_all(body_text, regex(pat, ignore_case=TRUE))[[1]]
  if (nrow(m) > 0) {
    for (i in seq_len(nrow(m))) {
      chunk <- str_sub(body_text, m[i,1], m[i,2])
      pg <- suppressWarnings(as.integer(str_extract(chunk, "^\\d+")))
      if (!is.na(pg) && pg >= 1 && pg <= 999)
        pages <- rbind(pages, data.frame(page=pg,
          position=m[i,1] + body_offset - 1L, stringsAsFactors=FALSE))
    }
  }

  # Even-page headers: "RELIEF SOCIETY MAGAZINE NUM" or "THE RELIEF SOCIETY MAGAZINE NUM"
  pat_even <- "(?:THE\\s+)?RELIEF\\s+SOCIETY\\s+MAGAZINE\\s+(\\d{1,3})\\b"
  m_even <- str_locate_all(body_text, regex(pat_even, ignore_case=TRUE))[[1]]
  if (nrow(m_even) > 0) {
    for (i in seq_len(nrow(m_even))) {
      chunk <- str_sub(body_text, m_even[i,1], m_even[i,2])
      pg <- suppressWarnings(as.integer(str_extract(chunk, "\\d{1,3}$")))
      if (!is.na(pg) && pg >= 1 && pg <= 999)
        pages <- rbind(pages, data.frame(page=pg,
          position=m_even[i,1] + body_offset - 1L, stringsAsFactors=FALSE))
    }
  }

  # Filter outliers: page numbers should increase monotonically
  if (nrow(pages) > 2) {
    pages <- pages[order(pages$position), ]
    keep <- rep(TRUE, nrow(pages))
    for (i in 2:(nrow(pages)-1)) {
      if (pages$page[i] < pages$page[i-1] - 5 ||
          pages$page[i] > pages$page[i+1] + 5) keep[i] <- FALSE
    }
    pages <- pages[keep, ]
  }
  pages[!duplicated(pages$page), ]
}

page_to_pos <- function(pi, pg, lo, hi) {
  if (nrow(pi)==0) return(lo)
  ex <- pi[pi$page==pg, ]
  if (nrow(ex)>0) return(ex$position[1])
  bef <- pi[pi$page < pg, ]
  aft <- pi[pi$page > pg, ]
  CPP <- 2400L
  if (nrow(bef)>0 && nrow(aft)>0) {
    b <- bef[nrow(bef),]; a <- aft[1,]
    frac <- (pg - b$page) / (a$page - b$page)
    return(as.integer(b$position + frac*(a$position - b$position)))
  }
  if (nrow(bef)>0) return(as.integer(bef$position[nrow(bef)] + (pg - bef$page[nrow(bef)]) * CPP))
  if (nrow(aft)>0) return(max(lo, as.integer(aft$position[1] - (aft$page[1] - pg) * CPP)))
  lo
}


## ----snap-to-para-------------------------------------------------------------
# ============================================================
# SNAP TO PARAGRAPH START
# Adapted for Vol50: after body preprocessing, text has \n\n
# at running header positions. Also handles sentence-end snap
# for run-together OCR text.
# ============================================================

snap_to_para_start <- function(text, pos, min_pos, max_back=600L) {
  if (pos <= min_pos) return(pos)
  lo  <- max(min_pos, pos - max_back)
  pre <- str_sub(text, lo, pos - 1L)

  find_nearby_sent_end <- function() {
    lo_t <- max(min_pos, pos - 150L)
    pre_t <- str_sub(text, lo_t, pos - 1L)
    se <- str_locate_all(pre_t, "[.!?][)'\",]?\\s+")[[1]]
    if (nrow(se) > 0L) {
      snap_s <- lo_t + se[nrow(se), 2]
      if (snap_s <= pos) return(snap_s)
    }
    NA_integer_
  }

  # First choice: double-newline (paragraph break from header insertion)
  breaks <- str_locate_all(pre, "(\r?\n){2,}")[[1]]
  if (nrow(breaks) > 0L) {
    snap <- lo + breaks[nrow(breaks), 2]
    if (snap < pos) {
      if ((pos - snap) <= 200L) return(snap)
      sent_snap <- find_nearby_sent_end()
      if (!is.na(sent_snap)) return(sent_snap)
      return(snap)
    }
  }

  # Second choice: single newline
  single_nl <- str_locate_all(pre, "\n")[[1]]
  if (nrow(single_nl) > 0L) {
    snap <- lo + single_nl[nrow(single_nl), 2]
    if (snap < pos) {
      if ((pos - snap) <= 200L) return(snap)
      sent_snap <- find_nearby_sent_end()
      if (!is.na(sent_snap)) return(sent_snap)
      return(snap)
    }
  }

  # Third choice: Vol50 running header end
  lo_h  <- max(min_pos, pos - max_back - 100L)
  pre_h <- str_sub(text, lo_h, pos - 1L)
  rh_pat <- paste0("\\d{1,3}\\s+(?:", paste(MONTH_UPPER, collapse="|"), ")\\s+", YEAR)
  hdrs <- str_locate_all(pre_h, regex(rh_pat, ignore_case=TRUE))[[1]]
  if (nrow(hdrs) > 0L) {
    snap_h <- lo_h + hdrs[nrow(hdrs), 2]
    if (snap_h <= pos && (pos - snap_h) <= 300L) return(snap_h)
  }

  # Fourth choice: sentence end within 150 chars
  sent_snap <- find_nearby_sent_end()
  if (!is.na(sent_snap)) return(sent_snap)

  pos
}


## ----detect-ad-start----------------------------------------------------------
# ============================================================
# BIRTHDAY-SPECIFIC AD TRIMMER
# Birthday name lists contain addresses that look like ads to
# the general detector. This function uses specific known
# advertiser names and price markers to trim ad content.
# ============================================================
trim_bday_ads <- function(bday_text) {
  # Known ad markers that never appear in birthday name lists
  ad_pats <- c(
    "\\bDeseret Book\\b",
    "\\bBeneficial Life\\b",
    "\\bOut of the Best Books\\b",
    "\\bSecond Class Postage\\b",
    "\\bZim's\\b",
    "\\$\\d+\\.\\d{2}",
    "\\bORDER\\s+NOW\\b",
    "\\bOrder now\\b",
    "\\bSurface mail\\b",
    "\\bHere is the newest volume\\b",
    "\\bNew!\\s+Volume\\b"
  )
  trimmed_ads <- ""
  earliest <- nchar(bday_text) + 1L
  for (ap in ad_pats) {
    m <- str_locate(bday_text, regex(ap, ignore_case = FALSE))
    if (!is.na(m[1,1]) && m[1,1] > 200L && m[1,1] < earliest) {
      earliest <- m[1,1]
    }
  }
  if (earliest <= nchar(bday_text)) {
    trimmed_ads <- str_trim(str_sub(bday_text, earliest))
    # Walk back to last complete name entry (ends with state/province)
    pre <- str_sub(bday_text, 1L, earliest - 1L)
    # Find last line break or Mrs. entry
    last_nl <- max(c(0L, str_locate_all(pre, "\n")[[1]][,1]))
    if (last_nl > 200L) {
      bday_text <- str_trim(str_sub(bday_text, 1L, last_nl - 1L))
    } else {
      bday_text <- str_trim(str_sub(bday_text, 1L, earliest - 1L))
    }
  }
  list(text = bday_text, ads = trimmed_ads)
}

# ============================================================
# AD DETECTOR — Find where advertisement content begins
# Uses a sliding window with weighted ad-indicator patterns.
# Returns character position where ad content starts, or NA.
# ============================================================

detect_ad_start <- function(text, min_article_len = 100L) {
  # Ad indicator patterns with weights
  patterns <- list(
    list(pat = "\\$\\d+\\.\\d{2}",                    w = 3L),  # prices ($12.95)
    list(pat = "\\d{3}[-.]?\\d{4}",                    w = 3L),  # phone numbers
    list(pat = "Utah\\s+\\d{5}|[A-Z][a-z]+,\\s+[A-Z]{2}\\s+\\d{5}", w = 2L),  # ZIP+state
    list(pat = "(?i)(?:Write|Send)\\s+(?:for|to)",     w = 2L),  # call-to-action
    list(pat = "\\bFREE\\b",                           w = 2L),  # FREE (caps)
    list(pat = "\\bDISCOUNT|CATALOG|INTRODUCTORY\\b",  w = 2L),  # promo terms
    list(pat = "(?i)\\bpostpaid|postage\\b",           w = 2L),  # mailing terms
    list(pat = "\\bTOUR\\b|\\bHAWAII\\b|\\bHEMISFAIR\\b", w = 2L),  # travel ads
    list(pat = "\\bBeneficial Life\\b|\\bDeseret Book\\b|\\bZim's\\b", w = 3L)  # known advertisers
  )

  n <- nchar(text)
  if (n < 300L) return(NA_integer_)

  # Sliding window: 500 chars, step 250
  first_ad_pos <- NA_integer_
  for (start in seq(1L, max(1L, n - 499L), by = 250L)) {
    chunk <- str_sub(text, start, min(start + 499L, n))
    score <- 0L
    for (p in patterns) {
      hits <- str_count(chunk, p$pat)
      score <- score + hits * p$w
    }
    if (score >= 7L) {
      first_ad_pos <- start
      break
    }
  }

  if (is.na(first_ad_pos)) return(NA_integer_)
  if (first_ad_pos < min_article_len) return(NA_integer_)  # too early = false positive

  # Walk backwards to find last sentence end before ad block
  pre_start <- max(1L, first_ad_pos - 300L)
  pre_ad <- str_sub(text, pre_start, first_ad_pos)
  sent_end <- str_locate_all(pre_ad, "[.!?][)'\"]?\\s")[[1]]
  if (nrow(sent_end) > 0) {
    return(pre_start + sent_end[nrow(sent_end), 2] - 1L)
  }

  first_ad_pos
}


## ----strip-mid-article-ads----------------------------------------------------
# ============================================================
# MID-ARTICLE AD STRIPPER — Remove interleaved advertisements
# Magazine pages often have ads interspersed between article text.
# This function finds ad blocks WITHIN an article (not leading/
# trailing) and separates them, returning clean article text and
# the extracted ad text for the Misc file.
# ============================================================

strip_mid_article_ads <- function(text, min_before = 200L) {
  n <- nchar(text)
  if (n < 500L) return(list(clean = text, ads = ""))

  # Same ad patterns as detect_ad_start
  patterns <- list(
    list(pat = "\\$\\d+\\.\\d{2}",                    w = 3L),
    list(pat = "\\d{3}[-.]?\\d{4}",                    w = 3L),
    list(pat = "Utah\\s+\\d{5}|[A-Z][a-z]+,\\s+[A-Z]{2}\\s+\\d{5}", w = 2L),
    list(pat = "(?i)(?:Write|Send)\\s+(?:for|to)",     w = 2L),
    list(pat = "\\bFREE\\b",                           w = 2L),
    list(pat = "\\bDISCOUNT|CATALOG|INTRODUCTORY\\b",  w = 2L),
    list(pat = "(?i)\\bpostpaid|postage\\b",           w = 2L),
    list(pat = "\\bTOUR\\b|\\bHAWAII\\b|\\bHEMISFAIR\\b", w = 2L),
    list(pat = "\\bBeneficial Life\\b|\\bDeseret Book\\b|\\bZim's\\b", w = 3L)
  )

  # Sliding window: find all ad windows
  ad_starts <- integer(0)
  for (ws in seq(1L, max(1L, n - 499L), by = 250L)) {
    we <- min(ws + 499L, n)
    chunk <- str_sub(text, ws, we)
    score <- 0L
    for (p in patterns) {
      hits <- str_count(chunk, p$pat)
      score <- score + hits * p$w
    }
    if (score >= 7L) ad_starts <- c(ad_starts, ws)
  }

  if (length(ad_starts) == 0L) return(list(clean = text, ads = ""))

  # Only strip MID-article ads: after min_before and before last 100 chars
  ad_starts <- ad_starts[ad_starts >= min_before & ad_starts < (n - 100L)]
  if (length(ad_starts) == 0L) return(list(clean = text, ads = ""))

  # Merge consecutive ad windows into contiguous ranges
  ranges <- list()
  rs <- ad_starts[1]; re <- min(ad_starts[1] + 499L, n)
  if (length(ad_starts) > 1) {
    for (w in ad_starts[-1]) {
      if (w <= re + 250L) {
        re <- min(w + 499L, n)
      } else {
        ranges <- c(ranges, list(c(rs, re)))
        rs <- w; re <- min(w + 499L, n)
      }
    }
  }
  ranges <- c(ranges, list(c(rs, re)))

  # Filter: only keep ranges that don't extend to the end of text
  mid_ranges <- list()
  for (r in ranges) {
    if (r[2] < n - 50L) mid_ranges <- c(mid_ranges, list(r))
  }
  if (length(mid_ranges) == 0L) return(list(clean = text, ads = ""))

  # Snap ad range boundaries to sentence/paragraph breaks
  for (ri in seq_along(mid_ranges)) {
    r <- mid_ranges[[ri]]
    # Snap start: walk back to last sentence end
    pre <- str_sub(text, max(1L, r[1] - 300L), r[1])
    sent_ends <- str_locate_all(pre, "[.!?][)'\"]?\\s")[[1]]
    if (nrow(sent_ends) > 0) {
      r[1] <- max(1L, r[1] - 300L) + sent_ends[nrow(sent_ends), 2]
    }
    # Snap end: walk forward to next sentence start (capital after space)
    post <- str_sub(text, r[2], min(r[2] + 300L, n))
    sent_start <- str_locate(post, "\\s[A-Z][a-z]")
    if (!is.na(sent_start[1,1]) && sent_start[1,1] <= 300L) {
      r[2] <- r[2] + sent_start[1,1] - 1L
    }
    mid_ranges[[ri]] <- r
  }

  # Build clean text (article parts) and ad text (removed parts)
  clean_parts <- character(0)
  ad_parts <- character(0)
  pos <- 1L
  for (r in mid_ranges) {
    if (r[1] > pos) clean_parts <- c(clean_parts, str_sub(text, pos, r[1] - 1L))
    ad_parts <- c(ad_parts, str_sub(text, r[1], r[2]))
    pos <- r[2] + 1L
  }
  if (pos <= n) clean_parts <- c(clean_parts, str_sub(text, pos, n))

  list(
    clean = paste(clean_parts, collapse = "\n"),
    ads   = paste(ad_parts, collapse = "\n---\n")
  )
}


## ----find-all-contents-in-body------------------------------------------------
# ============================================================
# CONTENTS TITLE PRE-SCAN — Build position map of ALL titles
# Searches body text for every CONTENTS title, including ones
# the main title search missed. Returns data.frame(idx, position)
# sorted by position.
# ============================================================

find_all_contents_in_body <- function(body_text, arts, body_start, body_end,
                                       known_positions, known_methods) {
  results <- data.frame(idx = integer(0), position = integer(0))

  for (i in seq_len(nrow(arts))) {
    if (!is.na(known_positions[i]) && known_methods[i] == "title") {
      # Already found by title search — use that position
      results <- rbind(results, data.frame(idx = i, position = known_positions[i]))
    } else {
      # Try to find this title in the full body (conservative: no S10 OCR substitution)
      pos <- find_in_window(body_text, arts$title[i], body_start, body_end,
                            allow_s10 = FALSE)
      if (!is.na(pos)) {
        results <- rbind(results, data.frame(idx = i, position = pos))
      }
    }
  }

  results[order(results$position), ]
}


## ----find-article-end---------------------------------------------------------
# ============================================================
# ARTICLE END DETECTOR — Content-aware end boundary
# Combines 4 signals to find where an article actually ends:
#   1. Page-range cap (based on next CONTENTS page)
#   2. "continued on page X" marker
#   3. Next CONTENTS title found in body text
#   4. Advertisement detection
# Always returns <= naive_end (only shortens, never extends).
# ============================================================

find_article_end <- function(body_text, start_pos, current_idx, arts,
                              page_index, body_start, body_end,
                              title_pos_map, naive_end, tier = "T1") {
  article_page <- arts$page[current_idx]
  is_poem <- isTRUE(arts$category[current_idx] == "Poetry")
  category <- arts$category[current_idx]

  # --- Signal 1: Page-range cap ---
  higher_pages <- sort(unique(arts$page[arts$page > article_page]))
  if (length(higher_pages) > 0) {
    next_page <- higher_pages[1]
  } else {
    next_page <- article_page + 4L
  }

  # Category-specific buffers: chars past next CONTENTS page start.
  # A typical magazine page is ~2500-3000 chars.
  # T1 articles have confirmed title positions so we use generous buffers
  # to tolerate inaccurate page-to-position mapping (sparse page markers).
  # T2 articles use tighter buffers since positions are estimated.
  if (tier == "T1") {
    buffer <- if (is_poem) 4000L
              else if (category %in% c("Fiction")) 15000L
              else if (category %in% c("Lesson", "Home")) 8000L
              else 6000L
  } else {
    buffer <- if (is_poem) 1500L
              else if (category %in% c("Fiction")) 5000L
              else if (category %in% c("Lesson", "Home")) 3000L
              else 2000L
  }

  page_cap_pos <- page_to_pos(page_index, next_page, body_start, body_end)
  page_cap <- min(body_end, page_cap_pos + buffer)

  # Poems get a hard cap (more generous for T1)
  if (is_poem) {
    poem_cap <- if (tier == "T1") 8000L else 5000L
    page_cap <- min(page_cap, start_pos + poem_cap)
  }

  # --- Signal 2: "continued on page X" marker ---
  search_end <- min(page_cap, naive_end)
  search_range <- str_sub(body_text, start_pos, search_end)
  cont_loc <- str_locate(search_range, regex("\\(continued on page \\d+\\)", ignore_case = TRUE))
  cont_end <- if (!is.na(cont_loc[1,2])) start_pos + cont_loc[1,2] - 1L else NA_integer_

  # --- Signal 3: Next CONTENTS title found in body ---
  # Only consider titles from articles on HIGHER CONTENTS pages.
  # This prevents false truncation from title matches of earlier-page articles
  # that happen to match at nearby positions in the OCR text.
  min_gap <- if (is_poem) 30L else 200L
  next_title_pos <- NA_integer_
  if (nrow(title_pos_map) > 0) {
    # Filter: position must be after current, different article, AND higher page
    higher_page_idx <- which(arts$page > article_page)
    candidates <- title_pos_map[title_pos_map$position > start_pos + min_gap &
                                 title_pos_map$idx != current_idx &
                                 title_pos_map$idx %in% higher_page_idx, ]
    if (nrow(candidates) > 0) {
      next_title_pos <- candidates$position[1]  # already sorted by position
    }
  }

  # --- Signal 4: Advertisement detection ---
  chunk <- str_sub(body_text, start_pos, search_end)
  ad_pos_rel <- detect_ad_start(chunk, min_article_len = 100L)
  ad_pos <- if (!is.na(ad_pos_rel)) start_pos + ad_pos_rel - 1L else NA_integer_

  # --- AD HOP-OVER ---
  # Ads often appear mid-article (interleaved between magazine pages).
  # When the first ad would truncate the article too short relative to
  # its expected page span, check if article text continues after the ad.
  # If it does, hop over the ad and use the NEXT ad block as the end.
  if (!is.na(ad_pos) && !is_poem) {
    expected_pages <- max(1L, next_page - article_page)
    min_expected_chars <- expected_pages * 1200L
    ad_article_len <- ad_pos - start_pos
    if (ad_article_len < min_expected_chars * 0.5 &&
        (page_cap - start_pos) > min_expected_chars) {
      # Find end of the ad block: scan forward past ad windows
      ad_block_end <- ad_pos
      while (ad_block_end < search_end) {
        check_chunk <- str_sub(body_text, ad_block_end,
                               min(ad_block_end + 499L, search_end))
        check_score <- 0L
        for (p in list(
          list(pat = "\\$\\d+\\.\\d{2}", w = 3L),
          list(pat = "\\d{3}[-.]?\\d{4}", w = 3L),
          list(pat = "Utah\\s+\\d{5}|[A-Z][a-z]+,\\s+[A-Z]{2}\\s+\\d{5}", w = 2L),
          list(pat = "(?i)(?:Write|Send)\\s+(?:for|to)", w = 2L),
          list(pat = "\\bFREE\\b", w = 2L),
          list(pat = "\\bTOUR\\b|\\bHAWAII\\b", w = 2L),
          list(pat = "\\bBeneficial Life\\b|\\bDeseret Book\\b", w = 3L)
        )) {
          hits <- str_count(check_chunk, p$pat)
          check_score <- check_score + hits * p$w
        }
        if (check_score < 4L) break
        ad_block_end <- ad_block_end + 250L
      }
      # Check if text after ad block contains article title words
      if (ad_block_end < search_end - 200L) {
        post_ad <- str_sub(body_text, ad_block_end,
                           min(ad_block_end + 800L, search_end))
        sig_w <- get_significant_words(arts$title[current_idx])
        sig_w <- sig_w[nchar(sig_w) >= 4]
        if (length(sig_w) >= 1) {
          post_hits <- sum(sapply(sig_w[seq_len(min(3L, length(sig_w)))],
            function(w) str_detect(post_ad, fixed(w, ignore_case = TRUE))))
          if (post_hits >= 1L) {
            # Article continues past the ad — find the NEXT ad block
            remaining <- str_sub(body_text, ad_block_end, search_end)
            next_ad_rel <- detect_ad_start(remaining, min_article_len = 200L)
            if (!is.na(next_ad_rel)) {
              ad_pos <- ad_block_end + next_ad_rel - 1L
            } else {
              ad_pos <- NA_integer_  # no more ads; use page_cap
            }
          }
        }
      }
    }
  }

  # --- Combine signals: take the earliest valid one ---
  # Poems can be very short; use lower minimum for them
  min_len <- if (is_poem) 15L else 50L
  all_candidates <- c(cont_end, next_title_pos, ad_pos, page_cap)
  all_candidates <- all_candidates[!is.na(all_candidates)]
  all_candidates <- all_candidates[all_candidates > start_pos + min_len]

  if (length(all_candidates) == 0) return(naive_end)

  best_end <- min(all_candidates)

  # Never extend past naive_end
  best_end <- min(best_end, naive_end)

  # Snap to paragraph boundary
  best_end <- snap_to_para_start(body_text, best_end, start_pos)

  # Safety: never return less than start_pos + min_len
  if (best_end <= start_pos + min_len) return(naive_end)

  best_end
}


## ----get-article-page-range---------------------------------------------------
# ============================================================
# PAGE RANGE HELPER — Independent article search window
# Given an article's page number, computes the character-position
# window [start, end] in body_text where this article should be
# found, using the page index. Each article gets its own window.
# ============================================================

get_article_page_range <- function(arts, idx, page_index, body_start, body_end, buffer = 1500L) {
  pg <- arts$page[idx]
  # Find the next higher page among all articles
  all_pages <- sort(unique(arts$page[arts$page > pg]))
  next_pg <- if (length(all_pages) > 0) all_pages[1] else pg + 4L
  if (next_pg == pg) next_pg <- pg + 1L

  win_start <- max(body_start, page_to_pos(page_index, pg, body_start, body_end) - buffer)
  win_end   <- min(body_end,   page_to_pos(page_index, next_pg, body_start, body_end) + buffer)

  list(start = win_start, end = win_end)
}


## ----find-in-window-----------------------------------------------------------
# ============================================================
# TITLE FINDER — Multi-strategy fuzzy match
# Searches for a title string within a character window.
# Returns absolute character position or NA.
# ============================================================

find_in_window <- function(text, title, w_start, w_end, allow_s10 = TRUE) {
  if (w_start >= w_end || nchar(str_trim(title)) == 0) return(NA_integer_)
  chunk <- str_sub(text, w_start, w_end)

  title <- str_replace_all(title, "\u2014|\u2013|---?", "-")
  title <- str_replace_all(title, "(?<=[\\w'\")])\\?(?=\\w)", "-")
  title <- str_trim(str_replace(title,
    regex("\\s+Frontispiece\\s*$", ignore_case=TRUE), ""))

  try_pat <- function(pat) {
    m <- tryCatch(str_locate(chunk, regex(pat, ignore_case=TRUE)),
                  error=function(e) matrix(NA_integer_,1,2))
    if (!is.na(m[1,1])) return(w_start + m[1,1] - 1L)
    NA_integer_
  }

  try_pat_para <- function(pat) {
    pos <- try_pat(pat)
    if (is.na(pos)) return(NA_integer_)
    if (pos <= w_start) return(pos)
    preceding <- str_sub(text, max(1L, pos - 2L), pos - 1L)
    if (str_detect(preceding, "\n")) return(pos)
    NA_integer_
  }

  esc <- function(s) str_replace_all(s, "([.?*+^$\\[\\]\\\\(){}|])", "\\\\\\1")

  # 1. Exact match
  pos <- try_pat(esc(title)); if (!is.na(pos)) return(pos)

  # 2. Whitespace flexible
  pos <- try_pat(str_replace_all(esc(title), "\\s+", "\\\\s+"))
  if (!is.na(pos)) return(pos)

  # 3. Dash flexible
  df  <- str_replace_all(esc(title), "[-\u2014\u2013]", "[-\u2014\u2013]+")
  df  <- str_replace_all(df, "\\s+", "\\\\s+")
  pos <- try_pat(df); if (!is.na(pos)) return(pos)

  # 4. Drop intra-word hyphens
  nh <- str_replace_all(title, "(?<=[a-zA-Z])-(?=[a-zA-Z])", "")
  if (nh != title) {
    pos <- try_pat(str_replace_all(esc(nh), "\\s+", "\\\\s+"))
    if (!is.na(pos)) return(pos)
  }

  # 5. EDITORIAL special case
  if (str_detect(title, regex("^Editorial", ignore_case=TRUE))) {
    subtitle <- str_trim(str_replace(title, regex("^Editorial[-: ]*", ignore_case=TRUE), ""))
    sub_words <- str_split(subtitle, "\\s+")[[1]]
    sub_words <- sub_words[nchar(sub_words) > 2]
    if (length(sub_words) >= 1) {
      ed_pat <- paste0("EDITORIAL[\\s\\S]{0,200}\\b",
                       esc(sub_words[1]), "\\b")
      pos <- try_pat(ed_pat)
      if (!is.na(pos)) return(pos)
    }
    pos <- try_pat("EDITORIAL\\b")
    if (!is.na(pos)) return(pos)
  }

  # 6. Title before dash
  if (str_detect(title, "-")) {
    before_dash <- str_trim(str_split(title, "-")[[1]][1])
    before_dash <- str_replace(before_dash, "['\"]+$", "")
    if (nchar(before_dash) >= 4 && before_dash != title) {
      pos <- try_pat(str_replace_all(esc(before_dash), "\\s+", "\\\\s+"))
      if (!is.na(pos)) return(pos)
    }
  }

  # 7. Strip leading honorifics
  cleaned <- str_trim(str_replace(title,
    regex("^(?:Pres\\.|Dr\\.|Mrs?\\.|Prof\\.)\\s+"), ""))
  if (cleaned != title && nchar(cleaned) >= 4) {
    pos <- try_pat(str_replace_all(esc(cleaned), "\\s+", "\\\\s+"))
    if (!is.na(pos)) return(pos)
  }

  # 7b. Serialized fiction "Chapter N" search
  # For titles like "Heart Room for Home Chapter 4 Alice Morrey Bailey",
  # the series name often doesn't appear in OCR (decorative font / graphic).
  # But "Chapter N" and the author name are usually present.
  if (str_detect(title, regex("Chapter\\s+\\d+", ignore_case = TRUE))) {
    chap_m <- str_match(title, regex("(Chapter\\s+\\d+)", ignore_case = TRUE))
    if (!is.na(chap_m[1,1])) {
      chap_pat <- str_replace_all(esc(chap_m[1,1]), "\\s+", "\\\\s+")
      pos <- try_pat(chap_pat)
      if (!is.na(pos)) {
        # Validate: author last name should be within 300 chars
        author_m_chap <- str_match(title,
          "([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){1,3})$")
        if (!is.na(author_m_chap[1,1])) {
          last_name_chap <- str_extract(str_trim(author_m_chap[1,1]), "[A-Za-z]+$")
          if (!is.na(last_name_chap) && nchar(last_name_chap) >= 3) {
            near_chap <- str_sub(text, max(1L, pos - 300L),
                                 min(nchar(text), pos + 300L))
            if (str_detect(near_chap,
                           fixed(last_name_chap, ignore_case = TRUE))) {
              return(pos)
            }
          }
        }
      }
    }
  }

  # 8. Significant-word proximity
  sigs <- get_significant_words(title)
  if (length(sigs) >= 2) {
    for (n in c(min(length(sigs),4), min(length(sigs),3), 2)) {
      if (n > length(sigs)) next
      pat <- paste(
        sapply(sigs[seq_len(n)], function(w) paste0("\\b", esc(w), "\\b")),
        collapse=".{0,120}")   # wider gap for mega-line text
      pos <- try_pat(pat)
      if (!is.na(pos)) {
        # When only 2 words matched but title has 4+ significant words,
        # the match is too loose — reject it. A 4+ word title matching
        # on only 2 words creates false positives (e.g. "Return"+"September"
        # matching a running header + common word near the wrong text).
        if (n == 2 && length(sigs) >= 4) {
          next  # too few words matched for a distinctive title — skip
        }
        pos_ls <- try_pat(paste0("(?<=\\n)[ \\t]*", pat))
        if (!is.na(pos_ls) && pos_ls > pos && (pos_ls - pos) <= 3000L)
          return(pos_ls)
        return(pos)
      }
    }
  }

  # 8b. Plural/singular normalization
  if (length(sigs) >= 1) {
    sigs_norm <- sapply(sigs, function(w)
      if (str_detect(w, "s$") && nchar(w) > 4) str_sub(w, 1, nchar(w)-1L) else w)
    if (!identical(sigs, sigs_norm)) {
      for (n in c(min(length(sigs_norm),4), min(length(sigs_norm),3), 2)) {
        if (n > length(sigs_norm)) next
        pat <- paste(
          sapply(sigs_norm[seq_len(n)], function(w) paste0("\\b", esc(w), "\\b")),
          collapse=".{0,120}")
        pos <- try_pat(pat)
        if (!is.na(pos)) {
          if (n == 2 && length(sigs_norm) >= 4) {
            next  # too few words matched for a distinctive title
          }
          pos_ls <- try_pat(paste0("(?<=\\n)[ \\t]*", pat))
          if (!is.na(pos_ls) && pos_ls > pos && (pos_ls - pos) <= 3000L)
            return(pos_ls)
          return(pos)
        }
      }
    }
  }

  # 8c. First significant word ALL-CAPS at line start
  #     Last resort before author search — catches short poem titles
  #     like "FAITH", "CHOICE", "BETTER" where the word is common
  #     but the ALL-CAPS form at a line start is distinctive.
  #     Allow page numbers (digits) between newline and title.
  if (length(sigs) >= 1 && nchar(sigs[1]) >= 4) {
    upper_first <- toupper(sigs[1])
    upper_pat <- paste0("(?<=\\n)[\\s\\d]*", esc(upper_first), "\\b")
    m <- tryCatch(str_locate(chunk, regex(upper_pat, ignore_case = FALSE)),
                  error = function(e) matrix(NA_integer_, 1, 2))
    if (!is.na(m[1,1])) {
      # Snap to the actual title word position (skip past page number prefix)
      title_offset <- regexpr(upper_first, str_sub(chunk, m[1,1], m[1,2]),
                              fixed = TRUE)
      if (title_offset[1] > 0) {
        return(w_start + m[1,1] + title_offset[1] - 2L)
      }
      return(w_start + m[1,1] - 1L)
    }
  }

  # 9. Author-name search
  author_m_check <- str_match(title,
    "^(.{3,40}?)\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){1,3})$")
  has_embedded_author <- !is.na(author_m_check[1,1])
  if (!allow_s10 && !has_embedded_author) return(NA_integer_)

  author_m <- str_match(title,
    "^(.{3,40}?)\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){1,3})$")
  if (!is.na(author_m[1,1])) {
    tp_check <- get_significant_words(str_trim(author_m[1,2]))
    if (!any(nchar(tp_check) >= 5)) {
      author_m2 <- str_match(title,
        "^(.{3,40})\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){1,3})$")
      if (!is.na(author_m2[1,1])) author_m <- author_m2
    }
  }
  if (!is.na(author_m[1,1])) {
    title_part <- str_trim(author_m[1,2])
    author_str <- str_trim(author_m[1,3])
    last_name  <- str_extract(author_str, "[A-Za-z]+$")
    is_valid_author <- !is.na(last_name) && nchar(last_name) >= 4
    tp_sigs      <- get_significant_words(title_part)
    tp_long_sigs <- tp_sigs[nchar(tp_sigs) >= 5]
    author_search_safe <- length(tp_long_sigs) >= 1

    if (is_valid_author) {
      if (author_search_safe) {
        pos <- try_pat(paste0("[Bb]y\\s+[A-Z][a-zA-Z.]+(?:\\s+[A-Z.]+){0,2}\\s+",
                              esc(last_name), "\\b"))
        if (!is.na(pos)) {
          author_names <- str_extract_all(author_str, "[A-Za-z]{3,}")[[1]]
          author_names <- head(author_names[author_names != last_name], 2)
          if (length(author_names) >= 1) {
            near <- str_sub(text, max(1L, pos - 5L), min(nchar(text), pos + 120L))
            name_found <- any(sapply(author_names, function(nm)
              str_detect(near, regex(paste0("\\b", esc(nm), "\\b"), ignore_case=TRUE))))
            if (name_found) return(pos)
          } else {
            return(pos)
          }
        }
      }
      if (author_search_safe) {
        pos <- try_pat(str_replace_all(esc(author_str), "\\s+", "\\\\s+"))
        if (!is.na(pos)) {
          # Reject if preceded by poem attribution markers (em-dash, tilde, ~)
          pre_chars <- str_sub(text, max(1L, pos - 5L), pos - 1L)
          if (!str_detect(pre_chars, "[\u2014\u2013~-]")) return(pos)
        }
      }
      tp_long5 <- tp_sigs[nchar(tp_sigs) >= 5]
      tp_long8 <- tp_sigs[nchar(tp_sigs) >= 8]
      title_search_safe <- length(tp_long5) >= 2 || length(tp_long8) >= 1
      if (title_search_safe && title_part != title) {
        pos <- try_pat(str_replace_all(esc(title_part), "\\s+", "\\\\s+"))
        if (!is.na(pos)) return(pos)
      }
      if (author_search_safe && length(tp_sigs) >= 1) {
        pos <- try_pat(paste0("\\b", esc(tp_sigs[1]), "\\b.{0,400}\\b",
                              esc(last_name), "\\b"))
        if (!is.na(pos)) return(pos)
      }
      tp_fn <- try_pat
      for (sw in tp_sigs[nchar(tp_sigs) >= 4]) {
        pos <- tp_fn(paste0("\\b", esc(sw), "\\b.{0,800}\\b", esc(last_name), "\\b"))
        if (!is.na(pos)) return(pos)
        pos <- tp_fn(paste0("\\b", esc(last_name), "\\b.{0,200}\\b", esc(sw), "\\b"))
        if (!is.na(pos)) return(pos)
      }
    }
  }

  # 10. OCR character-class substitution
  ocr_sigs <- get_significant_words(title)
  ocr_sigs <- ocr_sigs[nchar(ocr_sigs) >= 4]
  if (length(ocr_sigs) >= 2) {
    ocr_pats <- sapply(ocr_sigs[seq_len(min(3, length(ocr_sigs)))], function(w) {
      w <- str_replace_all(w, "l", "[lI1]")
      w <- str_replace_all(w, "I", "[lI1]")
      w <- str_replace_all(w, "O", "[O0]")
      paste0("\\b", w, "\\b")
    })
    pat <- paste(ocr_pats, collapse=".{0,120}")
    pos <- try_pat(pat)
    if (!is.na(pos)) return(pos)
  }

  # 11. Known recurring titles
  known_titles <- c(
    "From Near and Far", "FROM NEAR AND FAR",
    "Notes from the Field", "NOTES FROM THE FIELD",
    "Notes to the Field", "NOTES TO THE FIELD",
    "Birthday Congratulations", "BIRTHDAY CONGRATULATIONS",
    "Woman's Sphere", "WOMAN'S SPHERE",
    "Woman\u2019s Sphere", "WOMAN\u2019S SPHERE",
    "Visiting Teacher Message", "VISITING TEACHER MESSAGE",
    "Visiting Teacher", "VISITING TEACHER"
  )
  t_lower <- tolower(title)
  for (kt in known_titles) {
    kt_lower <- tolower(kt)
    if (str_detect(t_lower, fixed(kt_lower))) {
      pos <- try_pat(paste0("\\b", esc(kt), "\\b"))
      if (!is.na(pos)) return(pos)
    }
    # Substring fallback: require 6-char overlap (not 4) to avoid
    # false positives like "ng t" matching both "Living The" and
    # "Visiting Teacher".
    if (nchar(kt_lower) >= 6) {
      for (start in seq_len(max(1, nchar(kt_lower) - 5))) {
        substr_kt <- str_sub(kt_lower, start, start + 5)
        if (str_detect(t_lower, fixed(substr_kt))) {
          pos <- try_pat(paste0("\\b", esc(kt), "\\b"))
          if (!is.na(pos)) return(pos)
          break
        }
      }
    }
  }

  # 11b. Category-based heading search for garbled titles
  if (str_detect(tolower(title), "visiting\\s*teacher|visiting.*message")) {
    pos <- try_pat("VISITING\\s+TEACHER")
    if (!is.na(pos)) return(pos)
    pos <- try_pat("Visiting\\s+Teacher")
    if (!is.na(pos)) return(pos)
  }
  if (str_detect(tolower(title), "^editorial")) {
    pos <- try_pat("EDITORIAL[:\\s]")
    if (!is.na(pos)) return(pos)
  }

  # 12. First 2 significant words only
  if (length(sigs) >= 2) {
    first2 <- sigs[seq_len(min(2, length(sigs)))]
    if (all(nchar(first2) >= 5)) {
      pat <- paste(sapply(first2, function(w) paste0("\\b", esc(w), "\\b")),
                   collapse=".{0,40}")
      pos <- try_pat(pat)
      if (!is.na(pos)) return(pos)
    }
  }

  NA_integer_
}


## ----contents-parser----------------------------------------------------------
# ============================================================
# VOL50 CONTENTS PARSER
# Handles three formats:
#   1. Multi-line: each entry on its own line (January)
#   2. Single-line PAGE-first: "PAGE TITLE AUTHOR" (most months)
#   3. Single-line TITLE-first with OCR noise (February)
# ============================================================

# Category headers used in Vol50 CONTENTS
VOL50_CATEGORIES <- c(
  "SPECIAL FEATURES?:?",
  "FICTION(?:\\s*-\\s*SPECIAL[^\\n]{0,40})?",
  "GENERAL FEATURES?",
  "FEATURES? FOR THE HOME",
  "THE HOME\\s*[-:]\\s*(?:INSIDE AND OUT|Inside and Out)",
  "POETRY",
  "LESSONS? FOR \\w+",
  "LESSON DEPARTMENT[^\\n]{0,60}",
  "FEATURE FOR THE HOME"
)
cat_pattern <- paste0("(?:", paste(VOL50_CATEGORIES, collapse="|"), ")")

# Clean OCR noise from CONTENTS text (dot leaders become garbage)
clean_ocr_noise <- function(text) {
  # Remove runs of 4+ identical lowercase letters (OCR of dot leaders)
  text <- str_replace_all(text, "([a-z])\\1{3,}", " ")
  # Remove runs of dots/dashes mixed with spaces
  text <- str_replace_all(text, "[\\.\\-]{3,}", " ")
  # Remove runs of mixed OCR garbage: lowercase + dots + dashes
  text <- str_replace_all(text, "(?:[a-z\\.\\-\\s,;:]{5,}){2,}", " ")
  # Collapse whitespace
  text <- str_replace_all(text, "\\s+", " ")
  str_trim(text)
}

# Detect whether CONTENTS uses PAGE-first or TITLE-first format
detect_contents_format <- function(contents_text) {
  # Remove category headers
  stripped <- str_replace_all(contents_text,
    regex(cat_pattern, ignore_case=TRUE), " ")
  stripped <- str_trim(stripped)
  # Check if text starts with a 2-3 digit number (PAGE-first)
  if (str_detect(stripped, "^\\d{2,3}\\s+[A-Z]")) return("page_first")
  # Check if text has TITLE then page number at end of first entry
  if (str_detect(stripped, "^[A-Z].+?\\d{2,3}\\b")) return("title_first")
  "unknown"
}

# Parse multi-line CONTENTS (one entry per line)
parse_contents_multiline <- function(lines) {
  arts <- data.frame(title=character(), page=integer(), category=character(),
                     stringsAsFactors=FALSE)
  current_cat <- "Article"
  poetry_lines <- character(0)
  in_poetry <- FALSE

  # First pass: detect format (page-first vs page-last)
  page_first_count <- 0L
  page_last_count  <- 0L
  for (line in lines) {
    line <- str_trim(line)
    if (nchar(line) < 5) next
    if (str_detect(line, regex(paste0("^", cat_pattern), ignore_case=TRUE))) next
    if (str_detect(line, regex("^Contents?$", ignore_case=TRUE))) next
    if (str_detect(line, "^\\d{2,3}\\s+[A-Z]")) page_first_count <- page_first_count + 1L
    if (str_detect(line, "\\b\\d{2,3}\\s*$")) page_last_count <- page_last_count + 1L
  }
  is_page_first <- page_first_count > page_last_count

  for (line in lines) {
    line <- str_trim(line)
    if (nchar(line) < 3) next

    # Skip the "Contents" header itself
    if (str_detect(line, regex("^Contents?$", ignore_case=TRUE))) next

    # Skip month-year headers (e.g., "JANUARY 1963")
    if (str_detect(line, regex("^(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)['\u2019]?\\s*(?:19\\d{2})?",
                               ignore_case=TRUE)) && nchar(line) < 25) next

    # Skip masthead/editor lines
    if (str_detect(line, regex("^RELIEF SOCIETY MAGAZINE|^Marianne C\\. Sharp|^Vesta P\\. Crawford.*Editor|^Belle S\\. Spa",
                               ignore_case=TRUE))) next

    # Skip VOL/NO headers (e.g., "VOL. 50 Monthly Publication...")
    if (str_detect(line, regex("^VOL\\.?\\s*\\d|^Monthly Publication",
                               ignore_case=TRUE))) next

    # Skip publishing info and non-CONTENTS lines
    if (str_detect(line, regex(paste0(
      "^Published m(?:onthly|ainthly)|^Entered as second|^Subscription|^Editorial and Bus",
      "|^Jesus Christ of Latter|^the Act of March|^section 1103|^unless return postage",
      "|^numbers can be supplied|^address at once|^The Magazine is not responsible",
      "|^Color Transparency|^Photograph by|^Art Layout|^Cover Lithograph",
      "|^20c a copy|^manuscripts\\.?$|^The Church of Jesus|^Church of Jesus Christ"
    ), ignore_case=TRUE))) next

    # Check for OCR-garbled category headers (e.g., "nCTION" for "FICTION")
    if (str_detect(line, regex("^n?CTION\\s*$|^HCTION\\s*$|^FlCTION\\s*$", ignore_case=FALSE))) {
      current_cat <- "Fiction"
      in_poetry <- FALSE
      next
    }

    # Check for category header (exact match or prefix)
    if (str_detect(line, regex(paste0("^", cat_pattern, "\\s*$"), ignore_case=TRUE))) {
      cat_match <- str_extract(line, regex(cat_pattern, ignore_case=TRUE))
      current_cat <- categorize_header(cat_match)
      if (current_cat == "Poetry") { in_poetry <- TRUE } else { in_poetry <- FALSE }
      next
    }
    if (str_detect(line, regex(paste0("^", cat_pattern), ignore_case=TRUE))) {
      cat_match <- str_extract(line, regex(cat_pattern, ignore_case=TRUE))
      current_cat <- categorize_header(cat_match)
      if (current_cat == "Poetry") { in_poetry <- TRUE } else { in_poetry <- FALSE }
      line <- str_trim(str_replace(line, regex(cat_pattern, ignore_case=TRUE), ""))
      if (nchar(line) < 3) next
    }

    # Skip garbled lines (OCR interleaving produces many single-char tokens)
    alpha_ratio <- str_count(line, "[A-Za-z]") / max(nchar(line), 1)
    if (alpha_ratio < 0.3 && nchar(line) > 20) next
    tokens <- str_split(line, "\\s+")[[1]]
    tokens <- tokens[nchar(tokens) > 0]
    if (length(tokens) > 5) {
      single_char_ratio <- sum(nchar(tokens) == 1) / length(tokens)
      if (single_char_ratio > 0.30) {
        cat(sprintf("    Skipping garbled line: '%s'\n", str_sub(line, 1, 60)))
        next
      }
    }

    # Poetry section: collect lines for semicolon parsing later
    if (in_poetry) {
      # Skip publishing info that leaked into poetry section
      if (str_detect(line, regex("^Published|^Entered as|^Subscription|GENERAL BOARD|^Editorial and Bus",
                                  ignore_case=TRUE))) next
      # Skip cover/art credit lines
      if (str_detect(line, regex("^Color Transparency|^Photograph by|^Art Layout|^Cover Lithograph",
                                  ignore_case=TRUE))) next
      poetry_lines <- c(poetry_lines, line)
      next
    }

    # Regular entries: try both page-first and page-last formats
    pg <- NA_integer_
    title_text <- ""

    if (is_page_first) {
      # Format: "484 Title Text Author Name"
      pg_m <- str_match(line, "^(\\d{1,3})\\s+(.+)")
      if (!is.na(pg_m[1,1])) {
        pg <- as.integer(pg_m[1,2])
        title_text <- str_trim(pg_m[1,3])
      }
    } else {
      # Format: "Title Text Author Name 484"
      pg_m <- str_match(line, "^(.+?)\\s+(\\d{1,3})\\s*$")
      if (!is.na(pg_m[1,1])) {
        title_text <- str_trim(pg_m[1,2])
        pg <- as.integer(pg_m[1,3])
      }
    }

    # If primary format failed, try the other format
    if (is.na(pg)) {
      if (!is_page_first) {
        pg_m <- str_match(line, "^(\\d{1,3})\\s+(.+)")
        if (!is.na(pg_m[1,1])) {
          pg <- as.integer(pg_m[1,2])
          title_text <- str_trim(pg_m[1,3])
        }
      } else {
        pg_m <- str_match(line, "^(.+?)\\s+(\\d{1,3})\\s*$")
        if (!is.na(pg_m[1,1])) {
          title_text <- str_trim(pg_m[1,2])
          pg <- as.integer(pg_m[1,3])
        }
      }
    }

    # Handle entries without page numbers: continuation of previous entry
    if (is.na(pg) && nrow(arts) > 0 && nchar(line) >= 3) {
      # Append to previous entry's title (it's a continuation line)
      arts$title[nrow(arts)] <- paste(arts$title[nrow(arts)], line)
      next
    }

    if (!is.na(pg) && nchar(title_text) >= 3 && pg >= 1 && pg <= 999) {
      arts <- rbind(arts, data.frame(
        title=title_text, page=pg, category=current_cat,
        stringsAsFactors=FALSE))
    }
  }

  # Parse poetry section: semicolon-separated list + frontispiece
  if (length(poetry_lines) > 0) {
    poetry_text <- paste(poetry_lines, collapse=" ")

    # Frontispiece entry (first line, typically "PAGE Title - Frontispiece Author")
    # Author is 2-3 capitalized words (First M. Last), use {1,2} to avoid capturing next title
    if (is_page_first) {
      fp_m <- str_match(poetry_text,
        "^(\\d{1,3})\\s+(.+?)\\s*[-\u2014\u2013]\\s*Frontispiece\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){1,2})")
      if (!is.na(fp_m[1,1])) {
        arts <- rbind(arts, data.frame(
          title=paste(str_trim(fp_m[1,3]), str_trim(fp_m[1,4]), "- Frontispiece"),
          page=as.integer(fp_m[1,2]), category="Frontispiece",
          stringsAsFactors=FALSE))
      }
    } else {
      fp_m <- str_match(poetry_text,
        "^(.+?)\\s*[-\u2014\u2013]\\s*Frontispiece\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){1,2})\\s+(\\d{1,3})(?=\\s|$|[;,])")
      if (!is.na(fp_m[1,1])) {
        arts <- rbind(arts, data.frame(
          title=paste(str_trim(fp_m[1,2]), str_trim(fp_m[1,3]), "- Frontispiece"),
          page=as.integer(fp_m[1,4]), category="Frontispiece",
          stringsAsFactors=FALSE))
      }
    }

    # Strip frontispiece entry from poetry text before parsing individual poems
    # This prevents the poem regex from matching the entire frontispiece line
    poems_text <- poetry_text
    if (!is.na(fp_m[1,1])) {
      # Remove the frontispiece match from the beginning of poetry text
      poems_text <- str_trim(str_sub(poetry_text, nchar(fp_m[1,1]) + 1L))
    }

    # Individual poems: "Title, by Author, PAGE" or "Title, by Author, PAGE;"
    poem_m <- str_match_all(poems_text,
      "([^;,]+?),?\\s+by\\s+([^,;]+?),?\\s+(\\d{2,3})")[[1]]
    if (nrow(poem_m) > 0) {
      for (j in seq_len(nrow(poem_m))) {
        pg <- as.integer(poem_m[j, 4])
        ptitle <- str_trim(poem_m[j, 2])
        pauthor <- str_trim(poem_m[j, 3])
        if (nchar(ptitle) >= 3 && pg >= 1 && pg <= 999) {
          arts <- rbind(arts, data.frame(
            title=paste(ptitle, pauthor), page=pg, category="Poetry",
            stringsAsFactors=FALSE))
        }
      }
    }
  }

  # Deduplicate entries with the same page number (OCR duplicate lines)
  if (nrow(arts) > 0) {
    arts <- arts[!duplicated(arts$page), ]
  }

  arts
}

# Parse PAGE-first format CONTENTS
parse_contents_page_first <- function(text, min_pg, max_pg) {
  arts <- data.frame(title=character(), page=integer(), category=character(),
                     stringsAsFactors=FALSE)

  # Split into category sections
  cat_positions <- str_locate_all(text, regex(cat_pattern, ignore_case=TRUE))[[1]]
  if (nrow(cat_positions) == 0) {
    # No category headers found — treat entire text as one section
    sections <- list(list(cat="Article", text=text))
  } else {
    sections <- list()
    for (i in seq_len(nrow(cat_positions))) {
      cat_text <- str_sub(text, cat_positions[i,1], cat_positions[i,2])
      cat_name <- categorize_header(cat_text)
      sec_start <- cat_positions[i,2] + 1L
      sec_end <- if (i < nrow(cat_positions)) cat_positions[i+1,1] - 1L else nchar(text)
      sec_text <- str_trim(str_sub(text, sec_start, sec_end))
      sections[[length(sections)+1]] <- list(cat=cat_name, text=sec_text)
    }
  }

  for (sec in sections) {
    sec_text <- sec$text
    if (nchar(sec_text) < 5) next

    # For POETRY sections: extract frontispiece entry, skip semicolon poem list
    if (sec$cat == "Poetry") {
      # Find frontispiece entry — match author as capitalized words
      fp_m <- str_match(sec_text,
        paste0("^\\s*(\\d{1,3})\\s+(.+?)\\s*[-\u2014\u2013]?\\s*Frontispiece\\s+",
               "([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){1,2})"))
      if (is.na(fp_m[1,1])) {
        # Fallback: simple first-entry extraction
        fp_m <- str_match(sec_text,
          "^\\s*(\\d{1,3})\\s+(.+?)(?=\\s+\\d{1,3}\\s|$)")
      }
      if (!is.na(fp_m[1,1])) {
        pg <- as.integer(fp_m[1,2])
        fp_title <- if (ncol(fp_m) >= 4 && !is.na(fp_m[1,4]))
          paste(str_trim(fp_m[1,3]), str_trim(fp_m[1,4]), "- Frontispiece")
        else str_trim(fp_m[1,3])
        if (pg >= min_pg && pg <= max_pg) {
          arts <- rbind(arts, data.frame(
            title=fp_title, page=pg, category="Frontispiece",
            stringsAsFactors=FALSE))
        }
      }
      # Parse individual poems — no anchor required, matches anywhere
      poem_list <- str_match_all(sec_text,
        "([^;,]+?),?\\s+by\\s+([^,;]+?),?\\s+(\\d{1,3})")[[1]]
      if (nrow(poem_list) > 0) {
        for (j in seq_len(nrow(poem_list))) {
          pg <- as.integer(poem_list[j, 4])
          ptitle <- str_trim(poem_list[j, 2])
          pauthor <- str_trim(poem_list[j, 3])
          if (pg >= min_pg && pg <= max_pg && nchar(ptitle) >= 3) {
            arts <- rbind(arts, data.frame(
              title=paste(ptitle, pauthor), page=pg, category="Poetry",
              stringsAsFactors=FALSE))
          }
        }
      }
      next
    }

    # For non-poetry sections: find entries by page numbers
    # Strategy: locate all page numbers in range, extract text between them
    all_nums <- str_locate_all(sec_text, "\\b\\d{1,3}\\b")[[1]]
    if (nrow(all_nums) == 0) next

    num_vals <- as.integer(str_sub(sec_text, all_nums[,1], all_nums[,2]))
    valid <- which(num_vals >= min_pg & num_vals <= max_pg)
    if (length(valid) == 0) next

    for (vi in seq_along(valid)) {
      idx <- valid[vi]
      pg <- num_vals[idx]
      # Title+author text: from after page number to before next page number
      title_start <- all_nums[idx, 2] + 1L
      if (vi < length(valid)) {
        # Find the end: look for start of next entry
        next_idx <- valid[vi + 1]
        title_end <- all_nums[next_idx, 1] - 1L
      } else {
        title_end <- nchar(sec_text)
      }
      entry_text <- str_trim(str_sub(sec_text, title_start, title_end))

      # Clean OCR noise from entry
      entry_text <- clean_ocr_noise(entry_text)

      # Skip if too short or looks like garbage
      if (nchar(entry_text) < 3) next
      alpha_ratio <- str_count(entry_text, "[A-Za-z]") / max(nchar(entry_text), 1)
      if (alpha_ratio < 0.5 && nchar(entry_text) > 10) next

      arts <- rbind(arts, data.frame(
        title=entry_text, page=pg, category=sec$cat,
        stringsAsFactors=FALSE))
    }
  }

  arts
}

# Map raw category text to clean category name
categorize_header <- function(cat_text) {
  ct <- toupper(str_trim(cat_text))
  if (str_detect(ct, "SPECIAL FEATURE"))  return("Special_Features")
  if (str_detect(ct, "FICTION"))           return("Fiction")
  if (str_detect(ct, "GENERAL FEATURE"))   return("General_Features")
  if (str_detect(ct, "FEATURE.+HOME|HOME.+INSIDE")) return("Home")
  if (str_detect(ct, "POETRY"))            return("Poetry")
  if (str_detect(ct, "LESSON"))            return("Lesson")
  "Article"
}

# Main CONTENTS parser: auto-detects format and dispatches
# Parse CONTENTS without page numbers (e.g., July where OCR missed all page nums).
# Uses known recurring titles + category-based splitting + author-name detection.
parse_contents_no_pages <- function(contents_text) {
  arts <- data.frame(title=character(), page=integer(), category=character(),
                     stringsAsFactors=FALSE)

  # --- Step 1: Split by category headers ---
  cat_positions <- str_locate_all(contents_text,
    regex(cat_pattern, ignore_case=TRUE))[[1]]
  if (nrow(cat_positions) == 0) return(arts)

  sections <- list()
  for (i in seq_len(nrow(cat_positions))) {
    cat_text <- str_sub(contents_text, cat_positions[i,1], cat_positions[i,2])
    cat_name <- categorize_header(cat_text)
    sec_start <- cat_positions[i,2] + 1L
    sec_end <- if (i < nrow(cat_positions)) cat_positions[i+1,1] - 1L
               else nchar(contents_text)
    sec_text <- str_trim(str_sub(contents_text, sec_start, sec_end))
    sections[[length(sections)+1]] <- list(cat=cat_name, text=sec_text)
  }

  # --- Step 2: Split entries within each section ---
  for (sec in sections) {
    if (nchar(sec$text) < 5) next
    sec_text <- sec$text

    # POETRY: extract frontispiece + individual poems
    if (sec$cat == "Poetry") {
      # Frontispiece: "Title — Frontispiece Author" at start
      fp_m <- str_match(sec_text,
        regex("^(.+?)\\s*[\u2014-]\\s*Frontispiece\\s+(.+?)(?=\\s+[A-Z][a-z]+ [A-Z]|\\s+\\d{3}|$)",
              ignore_case=TRUE))
      if (!is.na(fp_m[1,1])) {
        fp_title <- paste(str_trim(fp_m[1,2]), "- Frontispiece")
        arts <- rbind(arts, data.frame(
          title=fp_title, page=0L, category="Frontispiece", stringsAsFactors=FALSE))
      }
      # Individual poems: split by page numbers if present
      poem_m <- str_match_all(sec_text,
        "(?:^|;)\\s*([^;]+?),?\\s+(?:by\\s+)?([A-Z][a-z]+(?:\\s+[A-Z][a-z.]+){1,3}),?\\s+(\\d{3})")[[1]]
      if (nrow(poem_m) > 0) {
        for (j in seq_len(nrow(poem_m))) {
          pg <- as.integer(poem_m[j, 4])
          ptitle <- str_trim(poem_m[j, 2])
          pauthor <- str_trim(poem_m[j, 3])
          if (nchar(ptitle) >= 3) {
            arts <- rbind(arts, data.frame(
              title=paste(ptitle, pauthor), page=pg, category="Poetry",
              stringsAsFactors=FALSE))
          }
        }
      }
      next
    }

    # LESSONS: split by lesson type prefixes (Theology —, Work Meeting —, etc.)
    if (sec$cat == "Lesson") {
      lesson_prefixes <- c("Theology", "Visiting Teacher",
                           "Work Meeting", "Literature", "Social Science")
      lp_pat <- paste0("(?=(?:", paste(lesson_prefixes, collapse="|"), ")\\s*[\u2014-])")
      parts <- str_split(sec_text, regex(lp_pat, ignore_case=TRUE))[[1]]
      for (part in parts) {
        part <- str_trim(part)
        if (nchar(part) < 5) next
        # Extract full lesson entry: "Type — Topic - Author" or "Type — Topic: Author"
        title_text <- str_replace_all(part, "\\s+", " ")
        if (nchar(title_text) > 100) title_text <- str_sub(title_text, 1, 100)
        arts <- rbind(arts, data.frame(
          title=title_text, page=0L, category="Lesson", stringsAsFactors=FALSE))
      }
      next
    }

    # GENERAL FEATURES: split by known section titles + author boundaries
    if (sec$cat == "General_Features") {
      # Known titles that always appear as standalone sections
      known_gen <- c("From Near and Far", "Birthday Congratulations")
      for (kg in known_gen) {
        if (str_detect(sec_text, regex(kg, ignore_case=TRUE))) {
          arts <- rbind(arts, data.frame(
            title=kg, page=0L, category="General_Features", stringsAsFactors=FALSE))
        }
      }
      # Entries with author: split by "Title - Author" or "Title: Author" pattern
      # Common entries: "Woman's Sphere - Ramona W. Cannon",
      #   "Editorial: Topic - Author", "Notes From the Field - Author"
      entry_m <- str_match_all(sec_text, paste0(
        "((?:Woman's Sphere|Editorial[^-]+|Notes (?:From|to) the Field[^-]*)",
        ")\\s*[-:]\\s*([A-Z][a-z]+(?:\\s+[A-Z][a-z.]+){1,3})"))[[1]]
      if (nrow(entry_m) > 0) {
        for (j in seq_len(nrow(entry_m))) {
          title_text <- str_trim(paste(entry_m[j,2], entry_m[j,3]))
          title_text <- str_replace_all(title_text, "\\s+", " ")
          arts <- rbind(arts, data.frame(
            title=title_text, page=0L, category="General_Features",
            stringsAsFactors=FALSE))
        }
      }
      next
    }

    # SPECIAL FEATURES, FICTION, HOME: split by "- Author" or author-name boundaries
    # Strategy: find "- Author" patterns and split entries at those boundaries
    # The format is typically: "Title - Author Title - Author ..."

    # First try: split at "- AuthorName" boundaries where AuthorName is 2+ capitalized words
    # followed by another title or end-of-section
    dash_entries <- str_match_all(sec_text, paste0(
      "([^-]+?)\\s*-\\s*([A-Z][a-z]+(?:\\s+[A-Z][a-z.]+){1,3})"))[[1]]
    if (nrow(dash_entries) > 0) {
      for (j in seq_len(nrow(dash_entries))) {
        title_text <- str_trim(dash_entries[j,2])
        author_text <- str_trim(dash_entries[j,3])
        full_entry <- paste(title_text, author_text)
        full_entry <- str_replace_all(full_entry, "\\s+", " ")
        if (nchar(full_entry) < 5) next
        arts <- rbind(arts, data.frame(
          title=full_entry, page=0L, category=sec$cat, stringsAsFactors=FALSE))
      }
    }

    # Check for entries without dash separators: "Title Author" (author detected by name pattern)
    # This catches entries like quoted titles followed by author
    nodash_m <- str_match_all(sec_text, paste0(
      '("[^"]+"|"[^"]+")\\s+([A-Z][a-z]+(?:\\.?\\s+[A-Z][a-z.]+){1,3})'))[[1]]
    if (nrow(nodash_m) > 0) {
      for (j in seq_len(nrow(nodash_m))) {
        title_text <- str_trim(nodash_m[j,2])
        author_text <- str_trim(nodash_m[j,3])
        full_entry <- paste(title_text, author_text)
        full_entry <- str_replace_all(full_entry, "\\s+", " ")
        already <- any(sapply(arts$title, function(t)
          nchar(t) >= 10 && str_detect(full_entry, fixed(str_sub(t, 1, min(15, nchar(t)))))))
        if (!already && nchar(full_entry) >= 5) {
          arts <- rbind(arts, data.frame(
            title=full_entry, page=0L, category=sec$cat, stringsAsFactors=FALSE))
        }
      }
    }

    # Serial fiction: Kiss of the Wind, Keep My Own, Out of the Wilderness
    serial_pats <- c(
      "Kiss of the Wind[^A-Z]*Chapter\\s*\\d+",
      "Keep My Own[^A-Z]*Chapter\\s*\\d+",
      "Out of the Wilderness[^A-Z]*Chapter\\s*\\d+"
    )
    for (sp in serial_pats) {
      m <- str_locate(sec_text, regex(sp, ignore_case=TRUE))
      if (!is.na(m[1,1])) {
        title_text <- str_trim(str_sub(sec_text, m[1,1], m[1,2]))
        # Grab author after chapter number
        extra <- str_sub(sec_text, m[1,2]+1,
                         min(nchar(sec_text), m[1,2]+50))
        auth_m <- str_match(extra, "^\\s*[-:]?\\s*([A-Z][a-z]+(?:\\s+[A-Z][a-z.]+){1,2})")
        if (!is.na(auth_m[1,1]))
          title_text <- paste(title_text, str_trim(auth_m[1,2]))
        already <- any(sapply(arts$title, function(t)
          str_detect(title_text, fixed(str_sub(t, 1, min(15, nchar(t)))))))
        if (!already) {
          arts <- rbind(arts, data.frame(
            title=title_text, page=0L, category="Fiction", stringsAsFactors=FALSE))
        }
      }
    }
  }

  arts
}

parse_vol50_contents <- function(contents_text, contents_lines, min_pg, max_pg) {
  # If we have separate lines (multi-line format), use line-based parser
  if (!is.null(contents_lines) && length(contents_lines) > 5) {
    # Check if most lines are short (multi-line) vs one long line
    short_lines <- sum(nchar(contents_lines) < 200)
    if (short_lines > length(contents_lines) * 0.6) {
      cat("  CONTENTS format: multi-line (line-based parser)\n")
      return(parse_contents_multiline(contents_lines))
    }
  }

  # Single-line format: detect PAGE-first vs TITLE-first
  fmt <- detect_contents_format(contents_text)

  # Check if page numbers actually exist in expected range
  all_nums <- as.integer(str_extract_all(contents_text, "\\b\\d{2,3}\\b")[[1]])
  in_range <- sum(all_nums >= min_pg & all_nums <= max_pg, na.rm=TRUE)

  if (fmt == "page_first" && in_range >= 5) {
    cat(sprintf("  CONTENTS format: single-line page_first (%d pages in range)\n", in_range))
    result <- parse_contents_page_first(contents_text, min_pg, max_pg)
    if (nrow(result) >= 5) return(result)
    cat(sprintf("    page_first returned only %d entries — falling back to no-pages parser\n", nrow(result)))
  }

  if (in_range >= 5) {
    cat(sprintf("  CONTENTS format: single-line title_first (%d pages in range)\n", in_range))
    cleaned <- clean_ocr_noise(contents_text)
    result <- parse_contents_page_first(cleaned, min_pg, max_pg)
    if (nrow(result) >= 5) return(result)
    cat(sprintf("    title_first returned only %d entries — falling back to no-pages parser\n", nrow(result)))
  }

  # No page numbers found or parsers returned too few — use no-pages parser
  cat(sprintf("  CONTENTS format: no page numbers (%d in range) — using title/author parser\n",
              in_range))
  parse_contents_no_pages(contents_text)
}

# ------------------------------------------------------------------
# Re-read a saved CONTENTS file back into arts dataframe
# Handles both manually-cleaned and auto-formatted CONTENTS files.
# Format expected: category headers on their own lines, then
# "page_num Title Author" entries, Poetry as "Title, by Author, page" lines.
# ------------------------------------------------------------------
read_contents_file <- function(filepath) {
  arts <- data.frame(title=character(), page=integer(), category=character(),
                     stringsAsFactors=FALSE)
  if (!file.exists(filepath)) return(arts)

  raw <- read_file(filepath)
  file_lines <- str_split(raw, "\n")[[1]]
  file_lines <- str_trim(file_lines)
  file_lines <- file_lines[nchar(file_lines) > 0]
  if (length(file_lines) == 0) return(arts)

  current_cat <- "Article"
  in_poetry <- FALSE

  for (line in file_lines) {
    # Skip cover/frontispiece/art credits
    if (str_detect(line, regex("^The Cover:|^Frontispiece:|^Art Layout|^Illustrations by|^Color Transparency|^Photograph by",
                                ignore_case=TRUE))) next

    # Category header detection
    if (str_detect(line, regex("^(Special Features?|Fiction[^\\d]*?|General Features?|The Home[^\\d]*?|Appointments[^\\d]*?|Lessons?(?:\\s+for\\s+\\w+|\\s+Department)?|Lesson[^\\d]*)\\s*$",
                                ignore_case=TRUE))) {
      cat_text <- str_extract(line, regex("Special Features?|Fiction|General Features?|The Home|Appointments|Lessons?",
                                           ignore_case=TRUE))
      if (str_detect(cat_text, regex("Special Feature", ignore_case=TRUE))) current_cat <- "Special Feature"
      else if (str_detect(cat_text, regex("^Fiction", ignore_case=TRUE))) current_cat <- "Fiction"
      else if (str_detect(cat_text, regex("General Feature", ignore_case=TRUE))) current_cat <- "General Feature"
      else if (str_detect(cat_text, regex("The Home", ignore_case=TRUE))) current_cat <- "The Home"
      else if (str_detect(cat_text, regex("Appointments", ignore_case=TRUE))) current_cat <- "Special Feature"
      else if (str_detect(cat_text, regex("Lesson", ignore_case=TRUE))) current_cat <- "Lesson"
      in_poetry <- FALSE
      next
    }

    # Poetry header
    if (str_detect(line, regex("^Poetry\\s*$", ignore_case=TRUE))) {
      in_poetry <- TRUE
      current_cat <- "Poetry"
      next
    }

    # Poetry entries: multiple formats supported
    if (in_poetry) {
      # Try: "Title, by Author, page" (Vol50-52 format)
      pm <- str_match(line, "^(.+?),\\s*by\\s+(.+?),\\s*(\\d{1,3})")
      if (!is.na(pm[1,1])) {
        ptitle <- str_trim(pm[1,2])
        pauthor <- str_trim(pm[1,3])
        pg <- as.integer(pm[1,4])
        arts <- rbind(arts, data.frame(
          title=paste(ptitle, pauthor), page=pg, category="Poetry",
          stringsAsFactors=FALSE))
        next
      }
      # Try: "Title[,] Author[,] Page[.]" (Vol53 format — no "by" keyword)
      # Author = 2-4 capitalized words (may include initials like "J." or "W.")
      # Trailing page number anchored at end of line
      pm_v53 <- str_match(line,
        "^(.+?),?\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){0,3}),?\\s+(\\d{1,3})\\.?\\s*$")
      if (!is.na(pm_v53[1,1])) {
        ptitle <- str_trim(str_replace(pm_v53[1,2], ",\\s*$", ""))
        pauthor <- str_trim(pm_v53[1,3])
        pg <- as.integer(pm_v53[1,4])
        arts <- rbind(arts, data.frame(
          title=paste(ptitle, pauthor), page=pg, category="Poetry",
          stringsAsFactors=FALSE))
        next
      }
      # Try: "Title[,] Author." (last entry — no page number, ends with period)
      pm_last <- str_match(line,
        "^(.+?),?\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){0,3})\\.\\s*$")
      if (!is.na(pm_last[1,1])) {
        ptitle <- str_trim(str_replace(pm_last[1,2], ",\\s*$", ""))
        pauthor <- str_trim(pm_last[1,3])
        arts <- rbind(arts, data.frame(
          title=paste(ptitle, pauthor), page=0L, category="Poetry",
          stringsAsFactors=FALSE))
        next
      }
      # Try: "page Title..." (page-first format, may have concatenated poems)
      pm2 <- str_match(line, "^(\\d{1,3})\\s+(.+)")
      if (!is.na(pm2[1,1])) {
        pg <- as.integer(pm2[1,2])
        full_title <- str_trim(pm2[1,3])
        # Check for embedded second poem: "Title1 Author1 Title2[,] Author2[,] Page2"
        pm_embed <- str_match(full_title,
          "^(.+?)\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){0,3})\\s+(.+?),?\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){0,3}),?\\s+(\\d{1,3})\\.?\\s*$")
        if (!is.na(pm_embed[1,1])) {
          # First poem at leading page number
          first_title <- paste(str_trim(pm_embed[1,2]), str_trim(pm_embed[1,3]))
          arts <- rbind(arts, data.frame(
            title=first_title, page=pg, category="Poetry", stringsAsFactors=FALSE))
          # Second poem at embedded page number
          second_title <- paste(str_trim(str_replace(pm_embed[1,4], ",\\s*$", "")),
                                str_trim(pm_embed[1,5]))
          second_pg <- as.integer(pm_embed[1,6])
          arts <- rbind(arts, data.frame(
            title=second_title, page=second_pg, category="Poetry", stringsAsFactors=FALSE))
        } else {
          arts <- rbind(arts, data.frame(
            title=full_title, page=pg, category="Poetry", stringsAsFactors=FALSE))
        }
        next
      }
      # Semicolon-separated poetry on one line (old format)
      if (str_detect(line, ";")) {
        parts <- str_split(line, ";\\s*")[[1]]
        for (part in parts) {
          part <- str_trim(part)
          if (nchar(part) < 5) next
          pm3 <- str_match(part, "(.+?),\\s*by\\s+(.+?),\\s*(\\d{1,3})")
          if (!is.na(pm3[1,1])) {
            ptitle <- str_trim(pm3[1,2])
            pauthor <- str_trim(pm3[1,3])
            pg <- as.integer(pm3[1,4])
            arts <- rbind(arts, data.frame(
              title=paste(ptitle, pauthor), page=pg, category="Poetry",
              stringsAsFactors=FALSE))
          }
        }
        next
      }
      next
    }

    # Non-poetry entries: "page_num Title Author"
    em <- str_match(line, "^(\\d{1,3})\\s+(.+)")
    if (!is.na(em[1,1])) {
      pg <- as.integer(em[1,2])
      title_text <- str_trim(em[1,3])

      # Check if line contains semicolons with embedded page numbers (merged Home entries)
      # Pattern: ", 430; ... , 436; ..." — semicolons separating entries with trailing page numbers
      if (str_detect(title_text, ",\\s*\\d{1,3}\\s*;")) {
        # Split by semicolons and parse each part
        parts <- str_split(title_text, ";\\s*")[[1]]
        for (p_idx in seq_along(parts)) {
          part <- str_trim(parts[p_idx])
          if (nchar(part) < 5) next
          # Try to extract trailing page number: "Title text, 430"
          pm_sc <- str_match(part, "^(.+?),\\s*(\\d{1,3})\\s*\\.?\\s*$")
          if (!is.na(pm_sc[1,1])) {
            sc_pg <- as.integer(pm_sc[1,3])
            sc_title <- str_trim(pm_sc[1,2])
            # Remove "by " prefix from author portion if present
            sc_title <- str_replace(sc_title, ",\\s*by\\s+", " ")
            arts <- rbind(arts, data.frame(
              title=sc_title, page=sc_pg, category=current_cat,
              stringsAsFactors=FALSE))
          } else if (p_idx == 1) {
            # First part: might not have trailing page number, use line's page
            first_clean <- str_trim(part)
            arts <- rbind(arts, data.frame(
              title=first_clean, page=pg, category=current_cat,
              stringsAsFactors=FALSE))
          }
        }
      } else {
        arts <- rbind(arts, data.frame(
          title=title_text, page=pg, category=current_cat,
          stringsAsFactors=FALSE))
      }
      next
    }

    # Category header with entries on same line (e.g. "The Home - inside and Out")
    # followed by content — treat as category change, entry may follow
    cat_inline <- str_match(line, regex(
      "^(Special Features?|Fiction|General Features?|The Home[^\\d]*?)\\s*(\\d{1,3})\\s+(.+)",
      ignore_case=TRUE))
    if (!is.na(cat_inline[1,1])) {
      cat_text <- str_trim(cat_inline[1,2])
      if (str_detect(cat_text, regex("Special Feature", ignore_case=TRUE))) current_cat <- "Special Feature"
      else if (str_detect(cat_text, regex("^Fiction", ignore_case=TRUE))) current_cat <- "Fiction"
      else if (str_detect(cat_text, regex("General Feature", ignore_case=TRUE))) current_cat <- "General Feature"
      else if (str_detect(cat_text, regex("The Home", ignore_case=TRUE))) current_cat <- "The Home"
      else if (str_detect(cat_text, regex("Lesson", ignore_case=TRUE))) current_cat <- "Lesson"
      pg <- as.integer(cat_inline[1,3])
      title_text <- str_trim(cat_inline[1,4])
      arts <- rbind(arts, data.frame(
        title=title_text, page=pg, category=current_cat,
        stringsAsFactors=FALSE))
      next
    }
  }

  if (nrow(arts) > 0) {
    arts <- arts[order(arts$page), ]
    cat(sprintf("  Re-read CONTENTS file: %d entries parsed\n", nrow(arts)))
  }
  arts
}


## ----header-cleaner-----------------------------------------------------------
# ============================================================
# RUNNING HEADER CLEANER for Vol50
# Strips "N MONTH YEAR" running headers from extracted text.
# ============================================================

clean_vol50_headers <- function(txt) {
  # Odd-page headers: "N MONTH YEAR"
  for (mu in MONTH_UPPER) {
    txt <- str_replace_all(txt,
      regex(paste0("\\b\\d{1,3}\\s+", mu, "\\s+", YEAR, "\\b")), "\n\n")
  }
  # Even-page bare numbers at paragraph boundaries (only if preceded/followed by \n)
  # These are unreliable; skip for now to avoid removing real numbers.
  # Collapse extra whitespace but preserve paragraph breaks
  txt <- str_replace_all(txt, "[ \\t]{3,}", "  ")
  str_trim(txt)
}

# ============================================================
# OCR BODY TEXT CLEANUP
# Fixes systematic OCR errors in extracted article text.
# Applied after header cleaning, before saving.
# ============================================================

clean_ocr_body <- function(txt) {
  # ----------------------------------------------------------
  # 0. Fix "|" -> "I" (pipe char is OCR misread of capital I)
  #    Virtually every "|" in this corpus is a first-person "I"
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "\\|", "I")

  # ----------------------------------------------------------
  # 1. Fix "Tt" -> "It" everywhere (word boundary)
  #    "Tt" is never a real English word; always OCR for "It"
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "\\bTt\\b", "It")

  # ----------------------------------------------------------
  # 2. Fix "T'" -> "I'" in contractions (T've, T'm, T'd, T'll)
  #    "T'" at a word boundary is never used in English prose
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "\\bT'", "I'")

  # ----------------------------------------------------------
  # 3. Fix standalone "T" -> "I" in safe contexts
  #    Only after sentence-ending punctuation, quotes, or line start,
  #    followed by common pronoun-continuation words (lookahead)
  # ----------------------------------------------------------
  i_followers <- "(?:don't|didn't|can't|couldn't|won't|wouldn't|shouldn't|haven't|wasn't|am|was|had|have|think|thought|know|knew|felt|feel|wish|hope|want|said|asked|tried|went|got|saw|looked|came|took|left|put|made|could|would|should|will|shall|must|may|might|need|suppose|guess|never|just|really)"
  # After period/!/? + optional closing quote + space(s)
  pat_T1 <- paste0("([.!?][\"'\\u201d\\u2019]?\\s+)T (?=", i_followers, ")")
  txt <- str_replace_all(txt, regex(pat_T1), "\\1I ")
  # After opening quote
  pat_T2 <- paste0("([\"'\\u201c])T (?=", i_followers, ")")
  txt <- str_replace_all(txt, regex(pat_T2), "\\1I ")
  # At start of line
  pat_T3 <- paste0("(^|\n)T (?=", i_followers, ")")
  txt <- str_replace_all(txt, regex(pat_T3), "\\1I ")

  # ----------------------------------------------------------
  # 4. Fix "vou/vour/vourself" -> "you/your/yourself"
  #    Common OCR error (y misread as v)
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "\\bvou\\b", "you")
  txt <- str_replace_all(txt, "\\bvour", "your")
  txt <- str_replace_all(txt, "\\bVou\\b", "You")
  txt <- str_replace_all(txt, "\\bVour", "Your")

  # ----------------------------------------------------------
  # 5. Fix common OCR title/abbreviation errors
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "\\bMirs\\.", "Mrs.")
  txt <- str_replace_all(txt, "\\bIrs\\.", "Mrs.")
  txt <- str_replace_all(txt, "\\bM rs\\.", "Mrs.")
  txt <- str_replace_all(txt, "\\bMr s\\.", "Mrs.")

  # ----------------------------------------------------------
  # 6. Strip ALL-CAPS running headers embedded in body text
  #    Even-page headers: article title in ALL CAPS
  #    {1,} = 2+ total words (word+space repeated, then ending word)
  # ----------------------------------------------------------
  # First: "PAGE_NUM ALL_CAPS_TITLE" (e.g., "726 SO GRATEFUL THAT YOU TAUGHT ME")
  txt <- str_replace_all(txt,
    "\n\\s*\\d{1,3}\\s+(?:[A-Z][A-Z',\\u2019-]+\\s+){1,}[A-Z][A-Z',\\u2019-]+\\s*\n",
    "\n\n")
  # Second: standalone ALL-CAPS title lines (2+ words, e.g., "CAROL'S CHRISTMAS")
  txt <- str_replace_all(txt,
    "\n\\s*(?:[A-Z][A-Z',\\u2019-]+\\s+){1,}[A-Z][A-Z',\\u2019-]+\\s*\n",
    "\n\n")
  # Also strip "VOLUME" as standalone word at paragraph boundary
  txt <- str_replace_all(txt, "\n\\s*VOLUME\\s*\n", "\n\n")

  # ----------------------------------------------------------
  # 7. Strip bare page numbers at paragraph boundaries
  #    Pattern: newline + 2-3 digit number + newline (nothing else on the line)
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "\n\\s*\\d{2,3}\\s*\n", "\n\n")

  # ----------------------------------------------------------
  # 8. Collapse excessive blank lines (4+ -> 2)
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "\n{4,}", "\n\n\n")

  # ----------------------------------------------------------
  # 9. Fix double spaces within text (but not indentation)
  # ----------------------------------------------------------
  txt <- str_replace_all(txt, "([a-zA-Z,;:])  +([a-zA-Z])", "\\1 \\2")

  str_trim(txt)
}


## ----process-issue------------------------------------------------------------
# ============================================================
# PROCESS ISSUE — Main extraction function for Vol50+
# ============================================================

process_vol50_issue <- function(filepath, output_dir) {
  filename <- basename(filepath)
  meta     <- get_metadata_vol50(filename)
  if (is.na(meta$month))  meta$month  <- "Unknown"
  if (is.na(meta$volume)) meta$volume <- as.character(VOLUME)
  if (is.na(meta$year))   meta$year   <- as.character(YEAR)
  if (is.na(meta$number)) meta$number <- "0"

  month_upper <- toupper(meta$month)

  text <- read_file_safe(filepath)
  lines_raw <- str_split(text, "\n")[[1]]
  n_lines <- length(lines_raw)

  cat("\n", rep("=",70), "\n", sep="")
  cat("FILE:", filename, "\n")
  cat(sprintf("SIZE: %s chars, %d lines\n", format(nchar(text), big.mark=","), n_lines))

  # Output folder: output/Vol50_1963/January/
  out_folder <- file.path(output_dir,
                          paste0("Vol", meta$volume, "_", meta$year),
                          meta$month)
  # Create output folder (preserve existing files like manually-edited CONTENTS)
  dir.create(out_folder, showWarnings=FALSE, recursive=TRUE)
  # Clean up old article/Misc/Board files from previous runs to avoid stale duplicates
  # but PRESERVE the manually-edited CONTENTS files
  old_files <- list.files(out_folder, full.names=TRUE)
  keep_pat <- "_00_CONTENTS\\.txt$"
  to_remove <- old_files[!str_detect(old_files, keep_pat)]
  if (length(to_remove) > 0) {
    file.remove(to_remove)
    cat(sprintf("  Cleaned %d old files (preserved CONTENTS)\n", length(to_remove)))
  }
  prefix <- paste0("V", meta$volume, "_", sprintf("%02d", as.integer(meta$number)))

  # ----------------------------------------------------------
  # Step 1: Detect file structure
  # Find: cover, masthead, "Contents" header, CONTENTS entries,
  #        publishing info, body text
  # ----------------------------------------------------------
  contents_header_idx <- NA_integer_
  contents_text <- NULL
  contents_lines <- NULL
  masthead_text <- ""
  body_line_idx <- NA_integer_
  publishing_text <- ""

  # Find the "Contents" header line
  for (i in seq_along(lines_raw)) {
    if (str_detect(lines_raw[i], regex("^\\s*Contents?\\s*$", ignore_case=TRUE))) {
      contents_header_idx <- i
      break
    }
  }

  # Fallback: "Contents" embedded mid-line (e.g., "No. 4ContentsSPECIAL FEATURES...")
  # Common in Vol36-40 OCR where masthead + TOC are concatenated
  if (is.na(contents_header_idx)) {
    for (i in seq_along(lines_raw)) {
      cont_loc <- str_locate(lines_raw[i],
        regex("Contents\\s*(?:SPECIAL|Special|FICTION|Fiction|GENERAL|General|POETRY|Poetry|LESSON|Lesson|APRIL|FEATURE)",
              ignore_case=FALSE))
      if (!is.na(cont_loc[1,1]) && cont_loc[1,1] > 5) {
        # Split this line: everything after "Contents" is CONTENTS text
        contents_start <- cont_loc[1,1] + nchar("Contents")
        first_contents_line <- str_sub(lines_raw[i], contents_start, nchar(lines_raw[i]))
        # Insert line breaks before known category headers within the first line
        first_contents_line <- str_replace_all(first_contents_line,
          regex("(?=(?:SPECIAL FEATURES?|FICTION|GENERAL FEATURES?|FEATURES? FOR THE HOME|POETRY|LESSON DEPARTMENT|SERIALS|APRIL SHORT STORIES|SHORT STORIES))",
                ignore_case=FALSE), "\n")
        first_split <- str_split(first_contents_line, "\n")[[1]]
        first_split <- str_trim(first_split[nchar(str_trim(first_split)) > 0])

        # Collect subsequent lines until publishing info
        extra_lines <- character(0)
        for (j in (i+1):min(n_lines, i+40)) {
          lt <- str_trim(lines_raw[j])
          if (nchar(lt) == 0) next
          if (str_detect(lt, regex("^Published|^PUBLISHED|^Entered as", ignore_case=TRUE))) break
          # Also split these lines at category headers
          lt_split <- str_replace_all(lt,
            regex("(?=(?:SPECIAL FEATURES?|FICTION|GENERAL FEATURES?|FEATURES? FOR THE HOME|POETRY|LESSON DEPARTMENT|SERIALS|APRIL SHORT STORIES|SHORT STORIES))",
                  ignore_case=FALSE), "\n")
          lt_parts <- str_split(lt_split, "\n")[[1]]
          lt_parts <- str_trim(lt_parts[nchar(str_trim(lt_parts)) > 0])
          extra_lines <- c(extra_lines, lt_parts)
        }
        all_contents_lines <- c(first_split, extra_lines)
        contents_text <- paste(all_contents_lines, collapse="\n")
        contents_lines <- all_contents_lines
        contents_header_idx <- i  # mark as found so later logic works
        cat(sprintf("  CONTENTS embedded mid-line %d: extracted %d lines, %d chars\n",
                    i, length(all_contents_lines), nchar(contents_text)))
        break
      }
    }
  }

  # Patterns for publishing info, masthead, cover credits (non-CONTENTS lines)
  pub_skip_pattern <- regex(paste0(
    "^Published m(?:onthly|ainthly)|^Entered as second|^Subscription|^Editorial and Bus",
    "|^Jesus Christ of Latter|^the Act of March|^section 1103|^unless return postage",
    "|^numbers can be supplied|^address at once|^The Magazine is not responsible",
    "|^Color Transparency|^Photograph by|^Art Layout|^Cover Lithograph",
    "|^20c a copy|^manuscripts\\.?$|^The Church of Jesus|^Church of Jesus Christ"
  ), ignore_case=TRUE)

  # Find the body text — supports both mega-line and split-line input
  line_lengths <- nchar(lines_raw)
  mega_lines <- which(line_lengths > 50000)
  is_split_input <- length(mega_lines) == 0

  if (!is_split_input) {
    # ==== UNSPLIT INPUT: body is the longest single line ====
    body_line_idx <- mega_lines[which.max(line_lengths[mega_lines])]
    body_raw <- lines_raw[body_line_idx]
  } else {
    # ==== SPLIT INPUT: body is all lines after publishing info ====
    # Find "Published monthly" line
    pub_line_idx <- NA_integer_
    for (i in seq_along(lines_raw)) {
      if (str_detect(lines_raw[i],
                     regex("Published\\s+m(?:onthly|ainthly)", ignore_case=TRUE))) {
        pub_line_idx <- i
        break
      }
    }
    if (!is.na(pub_line_idx)) {
      # Skip publishing continuation lines and blanks
      body_start_idx <- pub_line_idx + 1L
      while (body_start_idx <= n_lines) {
        lt <- str_trim(lines_raw[body_start_idx])
        if (nchar(lt) == 0) { body_start_idx <- body_start_idx + 1L; next }
        if (str_detect(lt, pub_skip_pattern)) {
          body_start_idx <- body_start_idx + 1L; next }
        break
      }
      body_line_idx <- body_start_idx
    } else {
      # No "Published monthly" found — find body start heuristically
      body_line_idx <- n_lines
      if (!is.na(contents_header_idx)) {
        # Scan forward from CONTENTS header to find where CONTENTS entries end.
        # CONTENTS lines match: category headers, page-number entries, poetry entries, or blanks.
        # Body starts at the first non-blank line that doesn't match any CONTENTS pattern.
        contents_cat_pat <- regex(
          "^(Special Feature|Fiction|General Feature|Poetry|The Home|Appointments|Lessons?\\b|Lesson Department)",
          ignore_case=TRUE)
        contents_pg_pat <- "^\\d{1,3}\\s+\\S"   # e.g., "164 The Lord..."
        contents_poetry_pat <- ",\\s*\\d{2,3}[;.]?\\s*$"  # ends with ", 166;" or ", 225."
        last_contents_line <- contents_header_idx
        consecutive_blank <- 0L
        consecutive_nonmatch <- 0L
        for (i in (contents_header_idx + 1):min(n_lines, contents_header_idx + 80)) {
          lt <- str_trim(lines_raw[i])
          if (nchar(lt) == 0) {
            consecutive_blank <- consecutive_blank + 1L
            if (consecutive_blank >= 3) break   # 3+ blank lines = end of CONTENTS
            next
          }
          consecutive_blank <- 0L
          is_contents <- str_detect(lt, contents_cat_pat) ||
                         str_detect(lt, contents_pg_pat) ||
                         str_detect(lt, contents_poetry_pat) ||
                         (nchar(lt) < 200 && str_detect(lt, "\\d{2,3}"))  # short line with page number
          if (is_contents) {
            last_contents_line <- i
            consecutive_nonmatch <- 0L
          } else if (nchar(lt) > 300) {
            break  # long line = definitely body text
          } else {
            # Allow short non-matching lines (wrapped entries, continuation text)
            # but stop after 3 consecutive non-matching lines
            consecutive_nonmatch <- consecutive_nonmatch + 1L
            if (consecutive_nonmatch >= 3) break
          }
        }
        body_line_idx <- last_contents_line + 1L
        while (body_line_idx <= n_lines && nchar(str_trim(lines_raw[body_line_idx])) == 0) {
          body_line_idx <- body_line_idx + 1L
        }
        cat(sprintf("    body_start heuristic: last CONTENTS-like line %d, body at line %d\n",
                    last_contents_line, body_line_idx))
      } else {
        # No CONTENTS header at all — look for first substantial text line
        for (i in 5:n_lines) {
          lt <- str_trim(lines_raw[i])
          if (nchar(lt) > 200 && str_detect(lt, "^\\d{1,3}\\s+[A-Z][a-z]")) {
            body_line_idx <- i
            break
          }
        }
      }
    }
    # Concatenate all body lines (join with space, not newline, to recreate
    # the continuous text that the rest of the pipeline expects)
    body_lines <- lines_raw[body_line_idx:n_lines]
    body_lines <- body_lines[nchar(str_trim(body_lines)) > 0]
    body_raw <- paste(body_lines, collapse=" ")
    # Clean up multiple spaces from joining
    body_raw <- str_replace_all(body_raw, "\\s{2,}", " ")
  }

  cat(sprintf("  Contents header: line %s, Body: line %d (%s chars)%s\n",
              ifelse(is.na(contents_header_idx), "?", as.character(contents_header_idx)),
              body_line_idx, format(nchar(body_raw), big.mark=","),
              if (is_split_input) " [split input]" else ""))

  # ----------------------------------------------------------
  # Step 1b: Extract CONTENTS from lines before the body
  # ----------------------------------------------------------
  contents_merged <- FALSE

  # Common path: CONTENTS lines exist between header and body
  # Skip if contents_text was already set by embedded detection
  if (!is.na(contents_header_idx) && body_line_idx > contents_header_idx + 1 && is.null(contents_text)) {
    contents_line_indices <- (contents_header_idx + 1):(body_line_idx - 1)
    cl <- lines_raw[contents_line_indices]
    cl <- cl[nchar(str_trim(cl)) > 0]
    cl_non_pub <- cl[!str_detect(cl, pub_skip_pattern)]
    if (length(cl_non_pub) > 0) {
      total_cl_chars <- sum(nchar(cl_non_pub))
      if (total_cl_chars > 100) {
        contents_text <- paste(cl_non_pub, collapse="\n")
        contents_lines <- cl_non_pub
        cl_pub <- cl[str_detect(cl, pub_skip_pattern)]
        publishing_text <- paste(cl_pub, collapse="\n")
      }
    }
  }

  # Unsplit: Check if CONTENTS is merged with body line
  if (!is_split_input && !is.na(contents_header_idx) && is.null(contents_text)) {
    if (body_line_idx == contents_header_idx + 1 && nchar(body_raw) > 50000) {
      if (str_detect(str_sub(body_raw, 1, 200),
                     regex("^\\s*(?:SPECIAL|Special|Fiction|General)", ignore_case=TRUE))) {
        contents_merged <- TRUE
        cat("  CONTENTS merged with body line — will split\n")
      }
    }

    # Check for CONTENTS on a medium-length line
    if (!contents_merged) {
      for (i in (contents_header_idx + 1):min(body_line_idx, contents_header_idx + 3)) {
        if (i > n_lines) break
        if (nchar(lines_raw[i]) > 500 && nchar(lines_raw[i]) < 50000) {
          if (str_detect(lines_raw[i], regex("SPECIAL FEATURE|Special Feature|FICTION|Fiction",
                                              ignore_case=TRUE))) {
            contents_text <- lines_raw[i]
            contents_lines <- NULL
            cat(sprintf("  CONTENTS on line %d (%d chars)\n", i, nchar(lines_raw[i])))
            break
          }
        }
      }
    }
  }

  # Fallback: No "Contents" header found — look for category headers directly
  if (is.na(contents_header_idx) && is.null(contents_text)) {
    first_cat_idx <- NA_integer_
    search_end <- min(body_line_idx - 1, n_lines)
    for (i in 1:search_end) {
      if (str_detect(str_trim(lines_raw[i]),
                     regex(paste0("^(?:", cat_pattern, ")\\s*$"), ignore_case=TRUE))) {
        first_cat_idx <- i
        break
      }
    }
    if (!is.na(first_cat_idx) && body_line_idx > first_cat_idx + 1) {
      cat(sprintf("  No 'Contents' header — using category headers starting at line %d\n",
                  first_cat_idx))
      contents_line_indices <- first_cat_idx:(body_line_idx - 1)
      cl <- lines_raw[contents_line_indices]
      cl <- cl[nchar(str_trim(cl)) > 0]
      cl_non_pub <- cl[!str_detect(cl, pub_skip_pattern)]
      if (length(cl_non_pub) > 0) {
        total_cl_chars <- sum(nchar(cl_non_pub))
        if (total_cl_chars > 100) {
          contents_text <- paste(cl_non_pub, collapse="\n")
          contents_lines <- cl_non_pub
          cl_pub <- cl[str_detect(cl, pub_skip_pattern)]
          publishing_text <- paste(cl_pub, collapse="\n")
          cat(sprintf("  Found %d CONTENTS lines (%d chars) from category headers\n",
                      length(cl_non_pub), total_cl_chars))
        }
      }
    }
  }

  # Fallback: CONTENTS embedded within a "Volume NN..." or editor line
  # (e.g., September 1964: editor info + CONTENTS on the same line)
  if (is.null(contents_text)) {
    for (i in 1:min(body_line_idx - 1, 10)) {
      lt <- lines_raw[i]
      sf_pos <- str_locate(lt, regex("Special\\s+Features?", ignore_case=TRUE))
      if (!is.na(sf_pos[1,1]) && sf_pos[1,1] > 30) {
        # "Special Features" appears after preceding text — embedded CONTENTS
        embedded_contents <- str_sub(lt, sf_pos[1,1], nchar(lt))
        if (nchar(embedded_contents) > 200) {
          contents_text <- embedded_contents
          contents_lines <- NULL
          cat(sprintf("  CONTENTS embedded in line %d (extracted %d chars from pos %d)\n",
                      i, nchar(embedded_contents), sf_pos[1,1]))
          break
        }
      }
    }
  }

  # Handle merged CONTENTS+body
  if (contents_merged) {
    # Split at "Published monthly" marker
    pub_pos <- str_locate(body_raw, regex("Published\\s+m(?:onthly|ainthly)",
                                          ignore_case=TRUE))
    if (!is.na(pub_pos[1,1])) {
      contents_text <- str_trim(str_sub(body_raw, 1L, pub_pos[1,1] - 1L))
      contents_lines <- NULL
      # Find where body text starts after publishing info
      # Look for the first page number followed by article text
      after_pub <- str_sub(body_raw, pub_pos[1,1], nchar(body_raw))
      # Find end of publishing block (look for a 2-3 digit number followed by
      # title text, which marks the first article)
      body_start_m <- str_locate(after_pub,
        regex("(?:manuscripts\\.?|unsolicited manuscripts\\.?)\\s*(\\d{2,3}\\s+[A-Z])",
              ignore_case=TRUE))
      if (!is.na(body_start_m[1,1])) {
        # Extract just the page number + article text
        remaining <- str_sub(after_pub, body_start_m[1,1], nchar(after_pub))
        pg_m <- str_locate(remaining, "\\d{2,3}\\s+[A-Z]")
        if (!is.na(pg_m[1,1])) {
          pub_end <- pub_pos[1,1] + body_start_m[1,1] + pg_m[1,1] - 3L
          publishing_text <- str_trim(str_sub(body_raw, pub_pos[1,1], pub_end))
          body_raw <- str_sub(body_raw, pub_end + 1L, nchar(body_raw))
        }
      } else {
        # Fallback: find first 3-digit number that looks like a page
        body_start_m2 <- str_locate(after_pub,
          regex("\\b\\d{3}\\s+[A-Z][a-z]"))
        if (!is.na(body_start_m2[1,1])) {
          pub_section_end <- pub_pos[1,1] + body_start_m2[1,1] - 2L
          publishing_text <- str_trim(str_sub(body_raw, pub_pos[1,1], pub_section_end))
          body_raw <- str_trim(str_sub(body_raw, pub_section_end + 1L, nchar(body_raw)))
        } else {
          # Last resort: just use everything after "Published monthly" as publishing+body
          publishing_text <- ""
          body_raw <- str_trim(str_sub(body_raw, pub_pos[1,2] + 1L, nchar(body_raw)))
        }
      }
      cat(sprintf("  Split: CONTENTS=%s chars, publishing=%s chars, body=%s chars\n",
                  format(nchar(contents_text), big.mark=","),
                  format(nchar(publishing_text), big.mark=","),
                  format(nchar(body_raw), big.mark=",")))
    } else {
      # No "Published monthly" found — treat as best we can
      # Look for transition from CONTENTS-style entries to prose
      contents_text <- str_sub(body_raw, 1, 5000)  # rough estimate
      body_raw <- str_sub(body_raw, 5001, nchar(body_raw))
      cat("  WARNING: Could not find 'Published monthly' split point\n")
    }
  }

  # Collect masthead from lines before contents header
  if (!is.na(contents_header_idx) && contents_header_idx > 1) {
    mh_lines <- lines_raw[1:(contents_header_idx - 1)]
    mh_lines <- mh_lines[nchar(str_trim(mh_lines)) > 0]
    # Separate cover line (first long garbled line) from masthead
    cover_text <- ""
    masthead_lines <- character(0)
    for (ml in mh_lines) {
      if (nchar(ml) > 1000 && cover_text == "") {
        cover_text <- ml  # garbled cover image OCR
      } else if (str_detect(ml, regex("\\bEditor\\b|\\bManager\\b|\\bVOLUME\\b|\\bVol\\s+\\d|\\bNumber\\s+\\d",
                                       ignore_case=TRUE))) {
        masthead_lines <- c(masthead_lines, ml)
      }
      # All other pre-body lines (editorials, reader letters, poetry) go to Misc
    }
    masthead_text <- paste(masthead_lines, collapse="\n")
  }

  # ----------------------------------------------------------
  # Step 2: Save CONTENTS, BOARD (masthead), Misc
  # ----------------------------------------------------------
  saved_count <- 0L

  # Save CONTENTS — format for readability (one entry per line)
  # SKIP if file already exists (preserve manual edits)
  contents_filepath <- file.path(out_folder, paste0(prefix, "_00_CONTENTS.txt"))
  if (file.exists(contents_filepath)) {
    cat(sprintf("  CONTENTS file already exists — preserving (not overwriting)\n"))
  } else if (!is.null(contents_text) && nchar(contents_text) > 10) {
    ct_out <- str_trim(contents_text)

    # Format single-line blobs OR multi-line text for consistent output
    # Step 1: Collapse to single line if multi-line, then re-split consistently
    ct_out <- str_replace_all(ct_out, "\\s*\n\\s*", " ")
    ct_out <- str_replace_all(ct_out, "\\s{2,}", " ")

    # Step 2: Split Poetry semicolons into one-per-line entries first
    poetry_match <- str_locate(ct_out, regex("Poetry\\b", ignore_case=TRUE))
    poetry_section <- ""
    main_section <- ct_out
    if (!is.na(poetry_match[1,1])) {
      main_section <- str_sub(ct_out, 1L, poetry_match[1,1] - 1L)
      poetry_raw <- str_trim(str_sub(ct_out, poetry_match[1,1], nchar(ct_out)))
      poetry_parts <- str_split(poetry_raw, ";\\s*")[[1]]
      poetry_parts <- str_trim(poetry_parts[nchar(str_trim(poetry_parts)) > 0])
      poetry_section <- paste(poetry_parts, collapse="\n")
    }

    # Step 3: Split main section at category headers
    main_section <- str_replace_all(main_section,
      regex("\\s+(Special Features?|Fiction(?:\\s*-\\s*Special[^\\d]*)?|General Features?|The Home\\s*-\\s*[Ii]nside and Out|Lessons? for \\w+)\\b",
            ignore_case=TRUE),
      "\n\n\\1")

    # Step 4: Split at every page number + title
    main_section <- str_replace_all(main_section,
      "(\\s)(\\d{1,3}\\s+[\"'\\u201c]?[A-Z])", "\\1\n\\2")
    main_section <- str_replace_all(main_section,
      "(\\s)(\\d{1,3}\\.\\s+[\"'\\u201c]?[A-Z])", "\\1\n\\2")

    # Step 5: Rejoin "Chapter N" / "Part N" splits
    main_section <- str_replace_all(main_section,
      regex("(Chapter|Part)\\s*\n(\\d{1,2})\\s", ignore_case=FALSE),
      "\\1 \\2 ")

    # Step 6: Clean up
    main_section <- str_replace_all(main_section, "\n\\s+", "\n")
    main_section <- str_replace_all(main_section, "\n{3,}", "\n\n")

    # Combine main + poetry
    ct_final <- if (nchar(poetry_section) > 0) {
      poetry_clean <- str_replace(poetry_section, "^Poetry\\s+", "")
      paste0(str_trim(main_section), "\n\nPoetry\n", poetry_clean)
    } else {
      str_trim(main_section)
    }

    # Extract Cover/Frontispiece credits
    cover_match <- str_locate(ct_final, regex("The Cover:", ignore_case=TRUE))
    if (!is.na(cover_match[1,1])) {
      before_cover <- str_trim(str_sub(ct_final, 1L, cover_match[1,1] - 1L))
      cover_credits <- str_trim(str_sub(ct_final, cover_match[1,1], nchar(ct_final)))
      ct_final <- paste0(before_cover, "\n\n", cover_credits)
    }

    write_file(str_trim(ct_final), contents_filepath)
    cat(sprintf("  Saved CONTENTS: %s chars\n", format(nchar(contents_text), big.mark=",")))
  }

  # Save BOARD (masthead)
  if (nchar(masthead_text) > 10) {
    write_file(str_trim(masthead_text),
               file.path(out_folder, paste0(prefix, "_00_BOARD.txt")))
    cat(sprintf("  Saved BOARD: %s chars\n", format(nchar(masthead_text), big.mark=",")))
  }

  # Save Misc (cover + publishing info + any other pre-body text)
  # Exclude lines already in the Board (masthead) file to avoid duplication
  pre_body_parts <- character(0)
  if (!is.na(contents_header_idx) && contents_header_idx > 1) {
    for (ml in lines_raw[1:(contents_header_idx - 1)]) {
      ml_trim <- str_trim(ml)
      if (nchar(ml_trim) > 0) {
        # Skip if this line is part of the masthead
        in_masthead <- FALSE
        if (length(masthead_lines) > 0) {
          for (mhl in masthead_lines) {
            if (str_detect(mhl, fixed(str_sub(ml_trim, 1, min(40, nchar(ml_trim)))))) {
              in_masthead <- TRUE; break
            }
          }
        }
        if (!in_masthead) pre_body_parts <- c(pre_body_parts, ml)
      }
    }
  }
  if (nchar(publishing_text) > 0) pre_body_parts <- c(pre_body_parts, publishing_text)
  misc_text <- paste(pre_body_parts, collapse="\n\n")
  if (nchar(misc_text) > 10) {
    write_file(clean_ocr_body(str_trim(misc_text)),
               file.path(out_folder, paste0(prefix, "_00_Misc.txt")))
    cat(sprintf("  Saved Misc (pre-body): %s chars\n", format(nchar(misc_text), big.mark=",")))
  }

  # ----------------------------------------------------------
  # Step 3: Parse CONTENTS entries
  # ----------------------------------------------------------
  # Determine expected page range for this month
  # Vol39 (1952): ~70 pages/month (smaller than Vol50+ which had ~80)
  month_num <- match(meta$month, MONTHS)
  if (is.na(month_num)) month_num <- 1L
  PAGES_PER_MONTH <- if (VOLUME <= 45) 70L else 80L
  min_pg <- max(1L, (month_num - 1L) * PAGES_PER_MONTH - 10L)
  max_pg <- month_num * PAGES_PER_MONTH + 30L  # generous overflow
  # Special case: January starts at page 1
  if (month_num == 1L) min_pg <- 1L

  # Priority: re-read existing CONTENTS file (may have manual edits),
  # then fall back to parsing raw contents_text
  arts <- data.frame(title=character(), page=integer(), category=character(),
                     stringsAsFactors=FALSE)
  if (file.exists(contents_filepath)) {
    arts <- read_contents_file(contents_filepath)
  }
  if (nrow(arts) < 3 && !is.null(contents_text) && nchar(contents_text) > 20) {
    if (nrow(arts) > 0) cat("  Re-read yielded only", nrow(arts), "entries — falling back to raw parse\n")
    arts <- parse_vol50_contents(contents_text, contents_lines, min_pg, max_pg)
  }

  cat(sprintf("  Articles in CONTENTS: %d\n", nrow(arts)))
  if (nrow(arts) > 0) {
    for (i in seq_len(nrow(arts))) {
      cat(sprintf("    [%d] pg %3d: %s (%s)\n", i, arts$page[i],
                  str_sub(arts$title[i], 1, 60), arts$category[i]))
    }
  }

  # ----------------------------------------------------------
  # Step 3b: Fix outlier page numbers
  # ----------------------------------------------------------
  if (nrow(arts) >= 5) {
    valid_pgs <- arts$page[arts$page > 0]
    if (length(valid_pgs) >= 5) {
      pg_med <- median(valid_pgs)
      for (i in which(arts$page > 0)) {
        neighbors <- sum(abs(valid_pgs - arts$page[i]) <= 100) - 1L
        if (neighbors < 2L) {
          candidates_pg <- arts$page[i] * 10L + 0:9
          dists <- abs(candidates_pg - pg_med)
          best  <- candidates_pg[which.min(dists)]
          fixed_neighbors <- sum(abs(valid_pgs - best) <= 100)
          if (fixed_neighbors >= 3L) {
            cat(sprintf("  PAGE FIX: '%s' pg %d -> %d\n",
                        str_sub(arts$title[i], 1, 40), arts$page[i], best))
            arts$page[i] <- best
          }
        }
      }
      arts <- arts[order(arts$page), ]
    }
  }

  # ----------------------------------------------------------
  # Step 3c: Split combined poetry frontispiece entries
  # ----------------------------------------------------------
  # Frontispiece lines concatenate 2 poems on one CONTENTS line:
  #   Vol52: "641 Shadowless Interval Dorothy J. Roberts The Master Hand, by Maxine C. Greenwood, 650"
  #   Vol53: "641 His Furrowed Acres Alice Morrey Bailey Arrowhead, Peggy Tangren, 654"
  # Re-read the CONTENTS file line and properly split into individual entries.
  fp_idx <- which(str_detect(arts$title, "^\\d{3}\\s") & arts$category == "Poetry")
  if (length(fp_idx) > 0 && file.exists(contents_filepath)) {
    clines <- read_lines(contents_filepath)
    new_rows <- list()
    remove_idx <- integer(0)

    for (fi in fp_idx) {
      raw_title <- arts$title[fi]
      fp_pg <- as.integer(str_extract(raw_title, "^\\d{3}"))

      # Find the matching line in CONTENTS file
      cl_matches <- clines[str_detect(clines, paste0("^", fp_pg, "\\s"))]
      if (length(cl_matches) == 0) {
        # Just strip the page prefix
        arts$title[fi] <- str_trim(str_replace(raw_title, "^\\d{3}\\s+", ""))
        next
      }
      cl <- cl_matches[1]

      # Parse the trailing second poem from the CONTENTS line
      # Try Vol52 format: ", by Author, page"
      sep_m <- str_match(cl,
        "^(\\d{3})\\s+(.+?)\\s*,\\s*by\\s+(.+?)(?:,\\s*(\\d{1,3}))?[.\\s]*$")

      if (is.na(sep_m[1,1])) {
        # Try Vol53 format: ", Author, page" (no "by")
        sep_m <- str_match(cl,
          "^(\\d{3})\\s+(.+?)\\s*,\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){0,3})\\s*(?:[,\\s]+)?(\\d{1,3})?[.\\s]*$")
      }

      if (is.na(sep_m[1,1])) {
        # No recognizable format — just strip page prefix
        arts$title[fi] <- str_trim(str_replace(raw_title, "^\\d{3}\\s+", ""))
        next
      }

      before_sep <- str_trim(sep_m[1, 3])   # text between page and ", by"/","
      author2    <- str_trim(sep_m[1, 4])    # second poem's author
      page2      <- if (!is.na(sep_m[1, 5])) as.integer(sep_m[1, 5]) else arts$page[fi]

      # Try to split before_sep into: title1 + author1 + title2
      split_found <- FALSE
      title1_author1 <- NULL; title2 <- NULL

      # Strategy C: Period-based author detection (middle initials like "J." "S." "W.")
      split_m <- str_match(before_sep,
        "^(.+?)\\s+([A-Z][a-z]+\\.?\\s+[A-Z]\\.\\s+[A-Z][a-z]+)\\s+(.+)$")
      if (!is.na(split_m[1,1]) && nchar(str_trim(split_m[1, 4])) >= 3) {
        title1_author1 <- paste(str_trim(split_m[1, 2]), str_trim(split_m[1, 3]))
        title2 <- str_trim(split_m[1, 4])
        split_found <- TRUE
      }

      # Strategy B: Common title-starting articles/prepositions
      if (!split_found) {
        for (sw in c("The ", "A ", "An ", "On ", "In ", "Of ", "My ")) {
          locs <- str_locate_all(before_sep, paste0("(?<=\\S\\s)", sw))[[1]]
          if (nrow(locs) == 0) next
          for (j in seq_len(nrow(locs))) {
            pos <- locs[j, 1]
            left <- str_trim(str_sub(before_sep, 1, pos - 1))
            right <- str_trim(str_sub(before_sep, pos))
            if (str_count(left, "\\S+") >= 3 &&
                str_detect(left, "[A-Z][a-z]+$") &&
                str_count(right, "\\S+") >= 2) {
              title1_author1 <- left
              title2 <- right
              split_found <- TRUE
              break
            }
          }
          if (split_found) break
        }
      }

      # Strategy D: Right-to-left author detection (2 or 3 capitalized words)
      # Handles entries like "Place of Prayer Marilyn McMeen Miller Memories"
      # where author has no middle initial and title2 doesn't start with The/A/An.
      if (!split_found) {
        bs_words <- str_split(before_sep, "\\s+")[[1]]
        if (length(bs_words) >= 5) {
          best_split <- NULL
          # Stopwords that cannot end title1 (would indicate incomplete phrase)
          d_stopwords <- c("the","a","an","in","of","to","for","and","or","by","at","on","is","with")
          for (ai in 2:(length(bs_words) - 1)) {
            # Reject if title1 ends with a stopword (incomplete phrase)
            t1_last <- tolower(bs_words[ai - 1])
            if (t1_last %in% d_stopwords) next
            for (alen in c(3L, 2L)) {
              aend <- ai + alen - 1L
              if (aend >= length(bs_words)) next  # need word(s) after author for title2
              author_words <- bs_words[ai:aend]
              title2_start <- bs_words[aend + 1L]
              # All author words must be capitalized and >= 3 chars
              if (!all(str_detect(author_words, "^[A-Z]"))) next
              if (any(nchar(author_words) < 3)) next
              # title2 start must be capitalized
              if (!str_detect(title2_start, "^[A-Z]")) next
              t1 <- paste(bs_words[1:(ai-1)], collapse = " ")
              t2 <- paste(bs_words[(aend+1):length(bs_words)], collapse = " ")
              a1 <- paste(author_words, collapse = " ")
              if (nchar(t2) < 3 || nchar(t1) < 3) next
              # Prefer longer author names; for same length, prefer rightmost position
              if (is.null(best_split) || alen > best_split$alen ||
                  (alen == best_split$alen && nchar(t1) > nchar(best_split$t1))) {
                best_split <- list(t1=t1, a1=a1, t2=t2, alen=alen)
              }
            }
          }
          if (!is.null(best_split)) {
            title1_author1 <- paste(best_split$t1, best_split$a1)
            title2 <- best_split$t2
            split_found <- TRUE
          }
        }
      }

      if (split_found && !is.null(title2)) {
        # Clean frontispiece markers from title1
        fp_title <- str_trim(str_replace(title1_author1,
          "[-\u2014\u2013]+\\s*Frontispiece\\s*", " "))
        fp_title <- str_trim(str_replace(fp_title, "[-\u2014\u2013]+\\s*$", ""))

        # Create two entries: frontispiece poem (first page) + second poem
        # First entry gets "Frontispiece" category so Step 6 uses the
        # frontispiece search path (body_start + 6000L) instead of normal
        # page-bounded search, which fails for first-page poems whose
        # page_to_pos maps to body_start → wrong content extraction.
        new_rows[[length(new_rows)+1]] <- data.frame(
          title = fp_title, page = fp_pg,
          category = "Frontispiece", stringsAsFactors = FALSE)
        new_rows[[length(new_rows)+1]] <- data.frame(
          title = paste(title2, author2), page = page2,
          category = "Poetry", stringsAsFactors = FALSE)
        remove_idx <- c(remove_idx, fi)
        cat(sprintf("  Split frontispiece: '%s' -> '%s' (pg %d) + '%s' (pg %d)\n",
            str_sub(raw_title, 1, 50),
            str_sub(fp_title, 1, 30), fp_pg,
            str_sub(paste(title2, author2), 1, 30), page2))
      } else {
        # Single poem or unsplittable: strip page prefix, clean Frontispiece marker
        clean_title <- str_trim(str_replace(before_sep,
          "[-\u2014\u2013]+\\s*Frontispiece\\s*", " "))
        clean_title <- str_trim(clean_title)
        arts$title[fi] <- paste(clean_title, author2)
        arts$page[fi] <- page2
        cat(sprintf("  Frontispiece cleaned: '%s' -> '%s' (pg %d)\n",
            str_sub(raw_title, 1, 50),
            str_sub(paste(clean_title, author2), 1, 40), page2))
      }
    }

    if (length(new_rows) > 0) {
      new_df <- do.call(rbind, new_rows)
      arts <- rbind(arts[-remove_idx, , drop=FALSE], new_df)
      arts <- arts[order(ifelse(arts$page > 0, arts$page, 9999L)), ]
      rownames(arts) <- NULL
    }
  }

  if (nrow(arts) == 0) {
    write_file(body_raw,
               file.path(out_folder, paste0(prefix, "_Misc_fullbody.txt")))
    cat("  No CONTENTS parsed. Full body saved as Misc.\n")
    return(data.frame(file=filename, n_contents=0L, n_saved=0L,
                      n_missed=0L, total_chars=nchar(text), stringsAsFactors=FALSE))
  }

  # ----------------------------------------------------------
  # Step 4: Pre-process body text
  # Insert \n\n at running header positions to create paragraph breaks.
  # ----------------------------------------------------------
  rh_pat <- paste0("(\\b\\d{1,3}\\s+", month_upper, "\\s+", YEAR, "\\b)")
  rh_count <- str_count(body_raw, regex(rh_pat, ignore_case=TRUE))
  if (rh_count > 0) {
    body_text <- str_replace_all(body_raw,
      regex(rh_pat, ignore_case=TRUE), "\n\n\\1\n\n")
    cat(sprintf("  Inserted %d running header breaks\n", rh_count))
  } else {
    body_text <- body_raw
    cat("  No running headers found in body text\n")
  }

  # Fix pipe -> I in body text (OCR misread of capital I)
  pipe_count <- str_count(body_text, "\\|")
  if (pipe_count > 0) {
    body_text <- str_replace_all(body_text, "\\|", "I")
    cat(sprintf("  Fixed %d pipe-to-I OCR errors in body text\n", pipe_count))
  }

  # Normalize Unicode quotes/apostrophes to ASCII for consistent matching
  # OCR body text often has curly quotes (U+2019) while CONTENTS titles
  # use straight quotes (U+0027). This mismatch breaks find_in_window().
  body_text <- str_replace_all(body_text, "[\u2018\u2019\u201A\u201B\u0060\u00B4]", "'")
  body_text <- str_replace_all(body_text, "[\u201C\u201D\u201E\u201F]", '"')

  body_start <- 1L
  body_end   <- nchar(body_text)

  # ----------------------------------------------------------
  # Step 4b: Detect duplicate next-issue content appended to body
  # Some input files have the next month's pages appended (e.g., May has June pages).
  # Detect by finding a VOLUME + DIFFERENT_MONTH + YEAR masthead pattern in body text.
  # ----------------------------------------------------------
  all_months <- c("JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE",
                  "JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER")
  other_months <- all_months[all_months != month_upper]
  next_issue_pat <- paste0("VOLUME\\s+\\d+\\s+(",
                           paste(other_months, collapse="|"),
                           ")\\s+", YEAR)
  next_issue_loc <- str_locate(body_text, regex(next_issue_pat, ignore_case=FALSE))
  if (!is.na(next_issue_loc[1,1])) {
    # Only truncate if the match is in the latter half of the body (not a stray mention)
    dup_pos <- next_issue_loc[1,1]
    if (dup_pos > body_end * 0.4) {
      old_end <- body_end
      body_end <- dup_pos - 1L
      body_text <- str_sub(body_text, 1L, body_end)
      cat(sprintf("  DUPLICATE ISSUE: Truncated body at pos %d (was %d, removed %d chars of next issue)\n",
                  body_end, old_end, old_end - body_end))
    }
  }

  # ----------------------------------------------------------
  # Step 5: Build page index from running headers
  # ----------------------------------------------------------
  page_index <- build_vol50_page_index(body_text, body_start, month_upper, YEAR)
  cat(sprintf("  Page index: %d markers\n", nrow(page_index)))

  # Synthetic page index when running headers are sparse
  if (nrow(page_index) < 3 && nrow(arts) >= 3) {
    valid_pgs <- arts$page[arts$page > 0]
    if (length(valid_pgs) >= 3) {
      pg_min <- min(valid_pgs); pg_max <- max(valid_pgs)
      pg_range <- pg_max - pg_min
      if (pg_range > 0) {
        body_len <- body_end - body_start + 1L
        synth_pgs <- sort(unique(valid_pgs))
        synth_pos <- body_start +
          as.integer((synth_pgs - pg_min) / pg_range * (body_len - 1L))
        synth_df <- data.frame(page=synth_pgs, position=synth_pos,
                               stringsAsFactors=FALSE)
        if (nrow(page_index) > 0)
          synth_df <- synth_df[!synth_df$page %in% page_index$page, ]
        page_index <- rbind(page_index, synth_df)
        page_index <- page_index[order(page_index$page), ]
        cat(sprintf("  Synthetic page index: %d total markers\n", nrow(page_index)))
      }
    }
  }

  # ----------------------------------------------------------
  # Step 5b: Detect CONTENTS/body page number mismatch
  # ----------------------------------------------------------
  # Some OCR files have CONTENTS page numbers from a different month.
  # If body page markers don't overlap with CONTENTS page numbers,
  # fall back to full-body search (no page windowing).
  page_mismatch <- FALSE
  real_markers <- build_vol50_page_index(body_text, body_start, month_upper, YEAR)
  if (nrow(real_markers) >= 2) {
    contents_pgs <- arts$page[arts$page > 0]
    if (length(contents_pgs) >= 3) {
      c_min <- min(contents_pgs); c_max <- max(contents_pgs)
      b_min <- min(real_markers$page); b_max <- max(real_markers$page)
      if (b_min > c_max + 50 || b_max < c_min - 50) {
        page_mismatch <- TRUE
        cat(sprintf("  PAGE MISMATCH: CONTENTS pages (%d-%d) vs body pages (%d-%d) — using full-body search\n",
                    c_min, c_max, b_min, b_max))
      }
    }
  }

  # ----------------------------------------------------------
  # Step 6: Independent page-bounded title search
  # ----------------------------------------------------------
  positions <- rep(NA_integer_, nrow(arts))
  search_method <- rep(NA_character_, nrow(arts))  # "title" or "page_est"

  for (i in seq_len(nrow(arts))) {
    title <- arts$title[i]
    pg    <- arts$page[i]

    # Frontispiece: near start of body, or in misc/pre-body text
    if (arts$category[i] == "Frontispiece") {
      win_end <- min(body_end, body_start + 6000L)
      ft <- str_trim(str_replace(title, regex("\\s+Frontispiece\\s*$",
                                              ignore_case=TRUE), ""))
      pos <- if (nchar(ft) >= 3)
        find_in_window(body_text, ft, body_start, win_end)
      else NA_integer_
      if (is.na(pos))
        pos <- find_in_window(body_text, title, body_start, win_end)
      if (!is.na(pos)) {
        positions[i] <- max(snap_to_para_start(body_text, pos, body_start, max_back=300L),
                            body_start)
        search_method[i] <- "title"
      } else if (exists("misc_text") && nchar(misc_text) >= 30) {
        # Frontispiece not in body — check misc/pre-body text
        ftw <- get_significant_words(ft)
        ftw <- ftw[nchar(ftw) >= 4]
        if (length(ftw) >= 1) {
          misc_hits <- sum(sapply(ftw[1:min(3, length(ftw))], function(w)
            str_detect(misc_text, fixed(w, ignore_case=TRUE))))
          if (misc_hits >= 1) {
            positions[i] <- body_start  # placeholder; save_article will use misc_text
            search_method[i] <- "misc_frontispiece"
            cat(sprintf("    FRONTISPIECE MISC: '%s' — %d/%d title words in pre-body text\n",
                        str_sub(title,1,40), misc_hits, min(3L, length(ftw))))
          }
        }
      }
      next
    }

    # "From Near and Far" — reader letters column, usually in cover/pre-body text
    # Only search body if page is well WITHIN the body range (not at/before start)
    if (str_detect(title, regex("^From Near and Far", ignore_case=TRUE))) {
      other_pgs <- arts$page[arts$page > 0 &
        !str_detect(arts$title, regex("^From Near and Far", ignore_case=TRUE)) &
        arts$category != "Frontispiece"]
      if (length(other_pgs) > 0 && pg > 0 && pg <= min(other_pgs)) {
        cat(sprintf("    NEAR_FAR: pg %d at/before body start (min body pg %d), will extract from cover\n",
                    pg, min(other_pgs)))
        next  # Will be extracted from cover text in post-processing
      }
      # Page is within the body — search for reader letter signatures
      if (pg > 0L && nrow(page_index) >= 3) {
        pg_est <- page_to_pos(page_index, pg, body_start, body_end)
        if (!is.na(pg_est)) {
          s <- max(body_start, pg_est - 5000L)
          e <- min(body_end, pg_est + 5000L)
          chunk <- str_sub(body_text, s, e)
          rl_pat <- regex("[\u2014\u2013\u2014-]\\s*[A-Z][a-z]+,?\\s+(?:Utah|California|Idaho|England|Canada|Washington|Oregon|Arizona|New York|Wyoming)",
                         ignore_case=FALSE)
          rl_m <- str_locate(chunk, rl_pat)
          if (!is.na(rl_m[1,1])) {
            pos <- s + rl_m[1,1] - 1L
            search_back <- str_sub(body_text, max(body_start, pos - 500L), pos)
            pg_str <- as.character(pg)
            pg_loc <- str_locate(search_back, paste0("\\b", pg_str, "\\b"))
            if (!is.na(pg_loc[1,1])) {
              positions[i] <- max(body_start, pos - 500L) + pg_loc[1,1] - 1L
              search_method[i] <- "title"
              cat(sprintf("    NEAR_FAR: Found reader letters at pos %d\n", positions[i]))
            } else {
              positions[i] <- snap_to_para_start(body_text, pos, body_start, max_back=500L)
              search_method[i] <- "title"
              cat(sprintf("    NEAR_FAR: Found reader letters (no pg marker) at pos %d\n",
                          positions[i]))
            }
          }
        }
      }
      next
    }

    # "Birthday Congratulations" (generic listing) — skip in title search.
    # Extracted as a post-processing step from body end.
    if (str_detect(title, regex("^Birthday Congrat\\w*\\s*$", ignore_case=TRUE))) {
      next
    }

    # No page number (from no-pages parser): search full body
    if (pg <= 0L) {
      pos <- find_in_window(body_text, title, body_start, body_end, allow_s10=TRUE)
      if (!is.na(pos)) {
        positions[i] <- max(snap_to_para_start(body_text, pos, body_start), body_start)
        search_method[i] <- "title"
      }
      next
    }

    # Normal articles: independent page-bounded search
    # Each article searches only within its own page range — no cascading errors
    pos <- NA_integer_
    if (nrow(page_index) >= 3) {
      # Primary search: article's page range with standard buffer
      pr <- get_article_page_range(arts, i, page_index, body_start, body_end)
      pos <- find_in_window(body_text, title, pr$start, pr$end, allow_s10=TRUE)

      # Wider search: double the buffer
      if (is.na(pos)) {
        pr2 <- get_article_page_range(arts, i, page_index, body_start, body_end, buffer = 4000L)
        pos <- find_in_window(body_text, title, pr2$start, pr2$end, allow_s10=TRUE)
      }

      # Page-position validation: reject matches too far from expected page.
      # Catches false positives where common title words match at wrong locations.
      # Poems: tight threshold (~2 pages). Fiction: generous (~10 pages, multi-page chapters).
      # Other articles: moderate (~7 pages).
      if (!is.na(pos)) {
        expected_pos <- page_to_pos(page_index, pg, body_start, body_end)
        match_distance <- abs(pos - expected_pos)
        is_poem <- isTRUE(arts$category[i] == "Poetry")
        is_fiction <- isTRUE(arts$category[i] == "Fiction")
        max_dist <- if (is_poem) 6000L
                    else if (is_fiction) 25000L
                    else 18000L
        if (match_distance > max_dist) {
          cat(sprintf("    [POS_REJECT] #%d '%s' (pg %d): found at %d, expected ~%d (dist %d > %d)\n",
                      i, str_sub(title, 1, 40), pg, pos, expected_pos,
                      match_distance, max_dist))
          pos <- NA_integer_  # reject — will fall through to PAGE_EST
        }
      }

      # Poem CAPS-header validation: check for ALL-CAPS title words in
      # a ±500 char window around the match.  Legitimate poems have their
      # CAPS header within a few hundred chars of the find_in_window hit.
      # False matches (page not OCR'd) lack the CAPS header entirely.
      # Require at least 2 CAPS title words found — this filters out
      # matches near running headers that contain only one title word
      # (e.g. "SEPTEMBER" in "September 1970" header).
      if (!is.na(pos) && isTRUE(arts$category[i] == "Poetry")) {
        author_strip_pv <- str_replace(title,
          "\\s*,?\\s*[A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]*){0,3}$", "")
        pv_title_words <- get_significant_words(author_strip_pv)
        pv_title_words <- pv_title_words[nchar(pv_title_words) >= 4]
        if (length(pv_title_words) >= 2) {
          caps_words_pv <- toupper(pv_title_words)
          pv_start <- max(pos - 500L, body_start)
          pv_end   <- min(pos + 500L, body_end)
          pv_text  <- str_sub(body_text, pv_start, pv_end)
          caps_hits <- sum(sapply(caps_words_pv, function(w)
            str_detect(pv_text, fixed(w))))
          if (caps_hits < 2L) {
            cat(sprintf("    [POEM_CONTENT_REJECT] #%d '%s' (pg %d): only %d/%d CAPS title words near match\n",
                        i, str_sub(title, 1, 40), pg, caps_hits, length(caps_words_pv)))
            pos <- NA_integer_
          }
        }
      }

      # Title-proximity verification: always try finding an ALL-CAPS header
      # version of the title near the expected page. Magazine article headers
      # are typically in ALL CAPS (e.g., "SPIRITUAL LIVING"). If the ALL-CAPS
      # match has MORE title words nearby than the original match, use it.
      # This catches cases where find_in_window matched a partial title
      # (e.g., "spiritual living" in an advisory note) instead of the actual
      # article header.
      if (!is.na(pos)) {
        title_sigs <- get_significant_words(title)
        title_sigs <- title_sigs[nchar(title_sigs) >= 4]
        n_check <- min(5L, length(title_sigs))
        if (n_check >= 3L) {
          nearby <- str_sub(body_text, pos, min(pos + 600L, body_end))
          nearby_hits <- sum(sapply(title_sigs[1:n_check], function(w)
            str_detect(nearby, fixed(w, ignore_case = TRUE))))

          # Always try ALL-CAPS search — prefer it when it scores higher.
          # Use fixed() matching (not regex) because R's ICU regex engine
          # sometimes fails to match \b word boundaries in certain text contexts.
          # The ALL-CAPS + case-sensitive fixed() match is reliable and selective.
          caps_words <- toupper(title_sigs[1:min(2, length(title_sigs))])
          caps_search <- paste(caps_words, collapse = " ")  # e.g. "SPIRITUAL LIVING"
          pr_search <- get_article_page_range(arts, i, page_index, body_start, body_end, buffer = 10000L)
          chunk <- str_sub(body_text, pr_search$start, pr_search$end)
          caps_m <- str_locate(chunk, fixed(caps_search))
          if (!is.na(caps_m[1,1])) {
            new_pos <- pr_search$start + caps_m[1,1] - 1L
            if (new_pos != pos) {  # only check if it's a different position
              new_nearby <- str_sub(body_text, new_pos, min(new_pos + 600L, body_end))
              new_hits <- sum(sapply(title_sigs[1:n_check], function(w)
                str_detect(new_nearby, fixed(w, ignore_case = TRUE))))
              if (new_hits > nearby_hits) {
                cat(sprintf("    [TITLE_VERIFY] #%d '%s': shifted %d -> %d (hits %d->%d)\n",
                            i, str_sub(title, 1, 40), pos, new_pos, nearby_hits, new_hits))
                pos <- new_pos
              }
            }
          }
        }
      }

      if (!is.na(pos)) {
        positions[i] <- max(snap_to_para_start(body_text, pos, body_start), body_start)
        search_method[i] <- "title"
      } else {
        # ALL-CAPS last resort: before falling back to PAGE_EST, try finding
        # the title as an ALL-CAPS header. This catches lesson articles where
        # find_in_window() failed due to "Lesson XX:" between category and title.
        # Use fixed() matching (not regex) because R's ICU regex engine
        # sometimes fails to match \b word boundaries in certain text contexts.
        title_sigs_lr <- get_significant_words(title)
        title_sigs_lr <- title_sigs_lr[nchar(title_sigs_lr) >= 4]
        if (length(title_sigs_lr) >= 2) {
          caps_lr <- toupper(title_sigs_lr[1:min(2, length(title_sigs_lr))])
          caps_search_lr <- paste(caps_lr, collapse = " ")
          pr_lr <- get_article_page_range(arts, i, page_index, body_start, body_end, buffer = 10000L)
          chunk_lr <- str_sub(body_text, pr_lr$start, pr_lr$end)
          caps_m_lr <- str_locate(chunk_lr, fixed(caps_search_lr))
          if (!is.na(caps_m_lr[1,1])) {
            lr_pos <- pr_lr$start + caps_m_lr[1,1] - 1L
            # Validate: at least 3 title words in next 600 chars
            lr_nearby <- str_sub(body_text, lr_pos, min(lr_pos + 600L, body_end))
            lr_hits <- sum(sapply(title_sigs_lr[1:min(5, length(title_sigs_lr))], function(w)
              str_detect(lr_nearby, fixed(w, ignore_case = TRUE))))
            if (lr_hits >= 3L) {
              cat(sprintf("    [CAPS_RESCUE] #%d '%s' (pg %d): found at %d (%d title hits)\n",
                          i, str_sub(title, 1, 40), pg, lr_pos, lr_hits))
              pos <- lr_pos
              positions[i] <- max(snap_to_para_start(body_text, pos, body_start), body_start)
              search_method[i] <- "title"
            }
          }
        }

        if (is.na(pos)) {
          # PAGE_EST fallback: use page estimate position
          # Marked as "page_est" so Step 9 won't use these as boundaries
          # for other articles (prevents false splitting)
          est <- page_to_pos(page_index, pg, body_start, body_end)
          if (!is.na(est)) {
            positions[i] <- max(snap_to_para_start(body_text, est, body_start), body_start)
            search_method[i] <- "page_est"
            cat(sprintf("    [PAGE_EST] #%d '%s' (pg %d) -> pos %d\n",
                        i, str_sub(title, 1, 40), pg, positions[i]))
          }
        }
      }
    } else {
      # Sparse page index: search full body
      pos <- find_in_window(body_text, title, body_start, body_end, allow_s10=TRUE)
      if (!is.na(pos)) {
        positions[i] <- max(snap_to_para_start(body_text, pos, body_start), body_start)
        search_method[i] <- "title"
      }
    }
  }

  found <- sum(!is.na(positions))
  missed <- sum(is.na(positions))
  cat(sprintf("  Found: %d / %d (%d missed)\n", found, nrow(arts), missed))

  # Log missed articles
  miss_idx <- which(is.na(positions))
  if (length(miss_idx) > 0) {
    for (mi in miss_idx) {
      cat(sprintf("    MISSED: [%d] pg %d '%s'\n",
                  mi, arts$page[mi], str_sub(arts$title[mi], 1, 60)))
    }
  }

  # ----------------------------------------------------------
  # Step 8b: Pre-scan for all CONTENTS titles in body text
  # Builds a position map used by find_article_end() for
  # content-aware boundary detection.
  # ----------------------------------------------------------
  title_pos_map <- find_all_contents_in_body(
    body_text, arts, body_start, body_end, positions, search_method
  )
  cat(sprintf("  Title position map: %d/%d titles located\n",
              nrow(title_pos_map), nrow(arts)))

  # ----------------------------------------------------------
  # Step 9: Two-tier article extraction
  # ----------------------------------------------------------
  # Tier 1: Title-found articles (search_method == "title")
  #   - Use natural boundaries: each runs from its position to
  #     the next title-found position. These are reliable.
  #   - Content-aware refinement shortens boundaries using
  #     page caps, "continued on page" markers, title pre-scan,
  #     and advertisement detection.
  # Tier 2: PAGE_EST articles (search_method == "page_est")
  #   - Extracted independently with page-range boundaries.
  #   - Do NOT create boundaries for other articles.
  #   - Prevents false splitting from estimated positions.
  # ----------------------------------------------------------
  saved_flags <- rep(FALSE, nrow(arts))
  saved_files <- rep(NA_character_, nrow(arts))  # track saved file paths per article
  recovery_candidates <- integer(0)
  poem_reject_idx <- integer(0)
  article_num <- 0L
  misc_ad_text <- character(0)  # Collector for ad text trimmed from articles

  # Body text coverage tracker: marks which character positions have been

  # extracted into article files.  After all extraction, uncovered text
  # is appended to the Misc file so no text is lost.
  body_used <- rep(FALSE, body_end)
  mark_used <- function(s, e) {
    s <- max(1L, as.integer(s)); e <- min(body_end, as.integer(e))
    if (e >= s) body_used[s:e] <<- TRUE
  }

  # Track saved article file paths and body text ranges for gap reassignment.
  # After all extraction, uncaptured gaps are matched to the nearest saved

  # article by position and appended/prepended, rather than dumped into Misc.
  saved_ranges <- list()
  record_saved <- function(file_path, bstart, bend) {
    saved_ranges[[length(saved_ranges) + 1L]] <<- list(
      file = file_path, start = as.integer(bstart), end = as.integer(bend)
    )
  }

  # --- Helper: validate, clean, and save an extracted article ---
  save_article <- function(i, art_text, tier_label,
                           body_start_pos = NA_integer_,
                           body_end_pos = NA_integer_,
                           method = "title") {
    art_text <- clean_vol50_headers(art_text)
    art_text <- clean_ocr_body(art_text)
    art_text <- str_trim(art_text)

    # --- LEADING GARBAGE CLEANUP ---
    # If text before the first title word contains OCR noise, trim it.
    if (nchar(art_text) > 200L) {
      tw_lead <- get_significant_words(arts$title[i])
      tw_lead <- tw_lead[nchar(tw_lead) >= 4]
      if (length(tw_lead) >= 1) {
        # Find the first title word in text
        first_title_pos <- NA_integer_
        for (tw in tw_lead[seq_len(min(3L, length(tw_lead)))]) {
          loc <- str_locate(art_text, fixed(tw, ignore_case = TRUE))
          if (!is.na(loc[1,1]) && loc[1,1] <= 500L) {
            first_title_pos <- min(c(first_title_pos, loc[1,1]), na.rm = TRUE)
          }
        }
        # If title starts after position 20, check leading text for garbage
        if (!is.na(first_title_pos) && first_title_pos > 20L) {
          leading <- str_sub(art_text, 1, first_title_pos - 1L)
          # Count OCR garbage patterns: ===, ///, consecutive special chars, etc.
          n_garbage <- str_count(leading, "[=&^°/{}|<>]") +
                       str_count(leading, "[A-Z]{5,}") +  # OCR all-caps gibberish
                       str_count(leading, "\\S{10,}")       # very long non-space runs
          if (n_garbage >= 3L || nchar(leading) > 30L) {
            # Check alpha-space ratio of leading text
            n_alpha_lead <- str_count(leading, "[a-z]")  # lowercase only
            if (n_alpha_lead < nchar(leading) * 0.4 || n_garbage >= 5L) {
              old_len <- nchar(art_text)
              art_text <- str_trim(str_sub(art_text, first_title_pos))
              cat(sprintf("    LEADING GARBAGE TRIM: '%s' removed %d leading chars\n",
                          str_sub(arts$title[i], 1, 40), first_title_pos - 1L))
            }
          }
        }
      }
    }

    # --- TRAILING GENERAL BOARD LIST CLEANUP ---
    # The General Board member list sometimes gets absorbed into articles
    # when it appears between article boundaries in the raw text.
    # Strip it and move to the BOARD file.
    if (!str_detect(arts$title[i], regex("Board|BOARD", ignore_case = TRUE))) {
      board_loc <- str_locate(art_text, "THE GENERAL BOARD\\b")
      if (!is.na(board_loc[1,1]) && board_loc[1,1] > 100L) {
        board_text <- str_sub(art_text, board_loc[1,1])
        art_text <- str_trim(str_sub(art_text, 1L, board_loc[1,1] - 1L))
        board_file <- file.path(out_folder, paste0(prefix, "_00_BOARD.txt"))
        if (file.exists(board_file)) {
          existing_board <- read_file(board_file)
          write_file(paste0(existing_board, "\n\n", str_trim(board_text)), board_file)
        } else {
          write_file(str_trim(board_text), board_file)
        }
        cat(sprintf("    BOARD TRIM: '%s' moved %d chars of board list\n",
                    str_sub(arts$title[i], 1, 40), nchar(board_text)))
      }
    }

    # --- ORPHAN WORD TRIM ---
    # Strip a single leading capitalized word that is NOT in the title
    # and is followed by ALL-CAPS text or a page number. This catches stray
    # name fragments (like "Roberts MATERIALS NEEDED..." or "Skidmore 663...")
    # without stripping legitimate first words of articles (like "This" or "Each").
    first_word_m <- str_match(art_text, "^([A-Z][a-z]+)\\s+")
    if (!is.na(first_word_m[1,1]) && nchar(first_word_m[1,2]) >= 4) {
      first_word <- first_word_m[1,2]
      # Strip punctuation from title words for comparison (e.g. "Research," -> "research")
      all_title_lc <- str_replace_all(
        tolower(str_split(arts$title[i], "\\s+")[[1]]), "[^a-z]", "")
      fw_lower <- tolower(first_word)
      if (!fw_lower %in% all_title_lc) {
        rest_text <- str_sub(art_text, nchar(first_word_m[1,1]) + 1L)
        # Only strip if what follows looks like orphan context:
        # ALL-CAPS word (3+ uppercase chars) or page number (3 digits)
        follows_orphan <- str_detect(rest_text, "^\\s*[A-Z]{3,}") ||
                          str_detect(rest_text, "^\\s*\\d{3}\\b")
        if (follows_orphan) {
          rest_check <- str_sub(rest_text, 1, min(300L, nchar(rest_text)))
          tw_c <- get_significant_words(arts$title[i])
          tw_c <- tw_c[nchar(tw_c) >= 4]
          if (length(tw_c) >= 1 && any(sapply(tw_c, function(w)
              str_detect(rest_check, fixed(w, ignore_case = TRUE))))) {
            art_text <- str_trim(rest_text)
            cat(sprintf("    ORPHAN WORD TRIM: '%s' removed leading '%s'\n",
                        str_sub(arts$title[i], 1, 40), first_word))
          }
        }
      }
    }

    # --- LEADING FRAGMENT CLEANUP (page numbers + orphan names) ---
    # Handles cases like "Skidmore 663 IN MEMORIAM..." where a stray
    # name fragment + page number precedes the actual article.
    if (nchar(art_text) > 50L) {
      tw_frag <- get_significant_words(arts$title[i])
      tw_frag <- tw_frag[nchar(tw_frag) >= 4]
      if (length(tw_frag) >= 1) {
        first_tw_pos <- NA_integer_
        for (tw in tw_frag[seq_len(min(3L, length(tw_frag)))]) {
          loc <- str_locate(art_text, fixed(tw, ignore_case = TRUE))
          if (!is.na(loc[1,1]) && loc[1,1] <= 500L) {
            first_tw_pos <- min(c(first_tw_pos, loc[1,1]), na.rm = TRUE)
          }
        }
        # Snap back to include preceding short title words (e.g. "IN" before "MEMORIAM")
        if (!is.na(first_tw_pos) && first_tw_pos > 3L) {
          all_tw <- str_split(arts$title[i], "\\s+")[[1]]
          short_tw <- all_tw[nchar(all_tw) >= 2 & nchar(all_tw) < 4]
          if (length(short_tw) > 0) {
            pre15 <- str_sub(art_text, max(1L, first_tw_pos - 15L),
                             first_tw_pos - 1L)
            esc_fn <- function(s) str_replace_all(s, "([.?*+^$\\[\\]\\\\(){}|])", "\\\\\\1")
            for (sw in short_tw) {
              sw_loc <- str_locate(pre15,
                regex(paste0("\\b", esc_fn(sw), "\\b"), ignore_case = TRUE))
              if (!is.na(sw_loc[1,1])) {
                first_tw_pos <- max(1L, first_tw_pos - nchar(pre15) +
                                    sw_loc[1,1] - 1L)
                break
              }
            }
          }
        }
        if (!is.na(first_tw_pos) && first_tw_pos > 3L && first_tw_pos <= 100L) {
          lead_frag <- str_trim(str_sub(art_text, 1, first_tw_pos - 1L))
          lead_words <- str_split(lead_frag, "\\s+")[[1]]
          lead_words <- lead_words[nchar(lead_words) > 0]
          has_page <- str_detect(lead_frag, "\\b\\d{3}\\b")
          # Strip if: short fragment with page number, or single orphan name word
          if (nchar(lead_frag) > 0 && nchar(lead_frag) <= 60L &&
              ((has_page && length(lead_words) <= 4) ||
               (length(lead_words) == 1 && str_detect(lead_frag, "^[A-Z][a-z]")))) {
            art_text <- str_trim(str_sub(art_text, first_tw_pos))
            cat(sprintf("    LEADING FRAG TRIM: '%s' removed '%s'\n",
                        str_sub(arts$title[i], 1, 40), lead_frag))
          }
        }
      }
    }

    # Ensure title is visible at the start of extracted text.
    # If the title words are not found in the first 300 chars,
    # prepend the CONTENTS title as a header line.
    title_check_region <- str_sub(art_text, 1, min(300L, nchar(art_text)))
    title_sigs_check <- get_significant_words(arts$title[i])
    title_sigs_check <- title_sigs_check[nchar(title_sigs_check) >= 4]
    if (length(title_sigs_check) >= 2) {
      n_check_t <- min(3L, length(title_sigs_check))
      title_hits_check <- sum(sapply(title_sigs_check[1:n_check_t], function(w)
        str_detect(title_check_region, fixed(w, ignore_case=TRUE))))
      if (title_hits_check < max(1L, ceiling(n_check_t * 0.5))) {
        art_text <- paste0("[", arts$title[i], "]\n\n", art_text)
      }
    }

    # Belt-and-suspenders: trim trailing ads from extracted text
    # Trimmed ad text is collected for the Misc file (not discarded)
    ad_start <- detect_ad_start(art_text, min_article_len = 100L)
    if (!is.na(ad_start) && ad_start > 100L) {
      trimmed_ad <- str_trim(str_sub(art_text, ad_start))
      cat(sprintf("    AD TRIM: '%s' trimmed %d ad chars -> Misc\n",
                  str_sub(arts$title[i], 1, 40), nchar(trimmed_ad)))
      if (nchar(trimmed_ad) > 10L) {
        misc_ad_text <<- c(misc_ad_text, paste0("--- Ad from: ", arts$title[i], " ---\n", trimmed_ad))
      }
      art_text <- str_sub(art_text, 1L, ad_start - 1L)
      art_text <- str_trim(art_text)
    }

    # Ad-only rejection: if the text is dominated by ad patterns,
    # the extraction is almost entirely ad content — wrong article position.
    # Uses direct pattern scoring (not detect_ad_start which has min_article_len guard).
    # Don't apply to poems (which are legitimately short) or Birthday (which has addresses).
    # For short articles (<1500 chars), check the full text; otherwise first 800 chars.
    if (arts$category[i] != "Poetry" && nchar(art_text) < 3000L &&
        !str_detect(arts$title[i], regex("Birthday", ignore_case = TRUE))) {
      ad_check_len <- if (nchar(art_text) < 1500L) nchar(art_text) else 800L
      ad_check <- str_sub(art_text, 1L, min(ad_check_len, nchar(art_text)))
      ad_score <- str_count(ad_check, "\\$\\d+\\.\\d{2}") * 3L +
                  str_count(ad_check, "\\d{3}[-.]?\\d{4}") * 3L +
                  str_count(ad_check, "(?i)\\bpostpaid|postage\\b") * 2L +
                  str_count(ad_check, "(?i)\\bDeseret Book\\b|\\bBeneficial Life\\b") * 3L +
                  str_count(ad_check, "(?i)\\bSecond Class Postage\\b") * 3L +
                  str_count(ad_check, "(?i)\\border\\s+now\\b|sales\\s+tax\\b") * 2L +
                  str_count(ad_check, "[A-Z][a-z]+,\\s+[A-Z]{2}\\s+\\d{5}") * 2L  # City, ST ZIP
      if (ad_score >= 7L) {
        cat(sprintf("    AD COLLECT: '%s' — ad score %d in first %d chars (%d total chars) -> Misc\n",
                    str_sub(arts$title[i], 1, 40), ad_score, ad_check_len, nchar(art_text)))
        if (nchar(art_text) > 10L) {
          misc_ad_text <<- c(misc_ad_text, paste0("--- Ad content (was: ", arts$title[i], ") ---\n", art_text))
        }
        recovery_candidates <<- c(recovery_candidates, i)
        return(invisible(NULL))
      }
    }

    # Poetry trim: poems are typically < 1500 chars.
    # Trigger at 2000 chars (lowered from 3000) to catch more over-long poems.
    if (arts$category[i] == "Poetry" && nchar(art_text) > 2000L) {
      # Search for poem end: triple newline, ALL-CAPS header, or running header
      poem_search_end <- min(nchar(art_text), 2000L)
      poem_break <- str_locate(str_sub(art_text, 100, poem_search_end),
        regex(paste0(
          "\n\\s*\n\\s*\n",                        # triple newline
          "|(?<=\n)[A-Z][A-Z ]{10,}(?=\n)",        # ALL-CAPS header
          "|(?<=\n)\\d{1,3}\\s+[A-Z]{3,}\\s+\\d{4}" # running header (e.g. "666 SEPTEMBER 1966")
        )))
      if (!is.na(poem_break[1,1]) && poem_break[1,1] > 30) {
        trim_at <- 99L + poem_break[1,1] - 1L
        old_len <- nchar(art_text)
        art_text <- str_trim(str_sub(art_text, 1, trim_at))
        cat(sprintf("    POEM TRIM: '%s' %s -> %s chars\n",
                    str_sub(arts$title[i],1,40),
                    format(old_len, big.mark=","),
                    format(nchar(art_text), big.mark=",")))
      } else {
        old_len <- nchar(art_text)
        art_text <- str_trim(str_sub(art_text, 1, 1500L))
        cat(sprintf("    POEM TRIM (hard cap): '%s' %s -> 1,500 chars\n",
                    str_sub(arts$title[i],1,40),
                    format(old_len, big.mark=",")))
      }
    }

    # Post-trim cleanup: if a poem captured the start of the NEXT article,
    # detect and remove the next article's content. Check whether title words
    # from the next higher-page CONTENTS entry appear after the first 200 chars.
    if (arts$category[i] == "Poetry" && nchar(art_text) > 300L) {
      higher_idx <- which(arts$page > arts$page[i])
      if (length(higher_idx) > 0) {
        # Check the 2 closest higher-page articles
        hi_sorted <- higher_idx[order(arts$page[higher_idx])]
        for (hi in hi_sorted[seq_len(min(2L, length(hi_sorted)))]) {
          hi_words <- get_significant_words(arts$title[hi])
          hi_words <- hi_words[nchar(hi_words) >= 5]
          if (length(hi_words) < 2L) next
          # Check for title words in the latter half of the poem text
          check_start <- max(200L, nchar(art_text) %/% 2L)
          check_region <- str_sub(art_text, check_start, nchar(art_text))
          hi_hits <- sum(vapply(hi_words[seq_len(min(3L, length(hi_words)))], function(w)
            str_detect(check_region, fixed(w, ignore_case = TRUE)), logical(1)))
          if (hi_hits >= 2L) {
            # Find the position of the first matching word and truncate before it
            for (hw in hi_words) {
              hloc <- str_locate(str_sub(art_text, check_start, nchar(art_text)),
                                 fixed(hw, ignore_case = TRUE))
              if (!is.na(hloc[1,1])) {
                trunc_pos <- check_start + hloc[1,1] - 2L
                # Walk back to a paragraph break if possible
                pre_trunc <- str_sub(art_text, max(1L, trunc_pos - 200L), trunc_pos)
                pbreak <- str_locate_all(pre_trunc, "\n\\s*\n")[[1]]
                if (nrow(pbreak) > 0) {
                  trunc_pos <- max(1L, trunc_pos - 200L) + pbreak[nrow(pbreak), 1] - 1L
                }
                old_poem_len <- nchar(art_text)
                art_text <- str_trim(str_sub(art_text, 1L, trunc_pos))
                cat(sprintf("    POEM NEXT-ART TRIM: '%s' removed %d chars (next art: '%s')\n",
                            str_sub(arts$title[i], 1, 30), old_poem_len - nchar(art_text),
                            str_sub(arts$title[hi], 1, 30)))
                break
              }
            }
            break  # Only trim for the first matching next article
          }
        }
      }
    }

    # Poetry title validation
    if (arts$category[i] == "Poetry" && nchar(art_text) >= 20) {
      ptv <- get_significant_words(arts$title[i])
      ptv <- ptv[nchar(ptv) >= 4]
      if (length(ptv) >= 2) {
        ptv_hits <- sum(sapply(ptv[1:min(4, length(ptv))], function(w)
          str_detect(art_text, fixed(w, ignore_case=TRUE))))
        if (ptv_hits == 0L) {
          # PAGE_EST poems at body start: the page_to_pos mapped to position 1,
          # extracting wrong content. Try to find the poem in misc/pre-body text.
          at_body_start <- !is.na(body_start_pos) && body_start_pos <= body_start + 500L
          if (method == "page_est" && at_body_start) {
            # Search misc text for poem content before rejecting
            misc_poem_found <- FALSE
            if (exists("misc_text") && nchar(misc_text) >= 30) {
              misc_ptv_hits <- sum(sapply(ptv[1:min(4, length(ptv))], function(w)
                str_detect(misc_text, fixed(w, ignore_case=TRUE))))
              if (misc_ptv_hits >= 1) {
                # Extract just the poem from misc text (not the full pre-body)
                first_w <- ptv[which(sapply(ptv[1:min(4, length(ptv))], function(w)
                  str_detect(misc_text, fixed(w, ignore_case=TRUE))))[1]]
                ploc <- str_locate(misc_text, fixed(first_w, ignore_case=TRUE))
                if (!is.na(ploc[1,1])) {
                  pstart <- max(1L, ploc[1,1] - 100L)
                  pchunk <- str_sub(misc_text, pstart, min(nchar(misc_text), pstart + 2000L))
                  pbrk <- str_locate(pchunk, regex(
                    "\n\\s*\n\\s*\n|(?<=\n)[\u2014\u2013\u2014-]{3,}|(?<=\n)[A-Z][A-Z ]{10,}(?=\n)"))
                  if (!is.na(pbrk[1,1]) && pbrk[1,1] > 20) {
                    art_text <- str_trim(str_sub(pchunk, 1, pbrk[1,1] - 1))
                  } else {
                    art_text <- str_trim(str_sub(pchunk, 1, min(1500L, nchar(pchunk))))
                  }
                  art_text <- clean_ocr_body(art_text)
                  misc_poem_found <- TRUE
                  cat(sprintf("    POEM FROM MISC: '%s' — found %d/%d title words in pre-body text (%d chars)\n",
                              str_sub(arts$title[i],1,40), misc_ptv_hits,
                              min(4L, length(ptv)), nchar(art_text)))
                }
              }
            }
            if (!misc_poem_found) {
              cat(sprintf("    POEM REJECT (PAGE_EST at body start + 0 title words): '%s' — %d chars discarded\n",
                          str_sub(arts$title[i],1,40), nchar(art_text)))
              recovery_candidates <<- c(recovery_candidates, i)
              return(invisible(NULL))
            }
          } else {
            cat(sprintf("    POEM WARN: '%s' — 0/%d title words in full text (saving anyway)\n",
                        str_sub(arts$title[i],1,40), min(4L, length(ptv))))
          }
        }
      }
    }

    # Quality check: reject text that is mostly OCR garbage
    # (low alphanumeric ratio indicates garbled scan)
    art_text_trimmed <- str_trim(art_text)
    if (nchar(art_text_trimmed) >= 20L) {
      alpha_count <- str_count(art_text_trimmed, "[A-Za-z]")
      alpha_ratio <- alpha_count / nchar(art_text_trimmed)
      if (alpha_ratio < 0.35) {
        cat(sprintf("    OCR GARBAGE REJECT: '%s' — only %.0f%% alphabetic (%d chars discarded)\n",
                    str_sub(arts$title[i], 1, 40), alpha_ratio * 100, nchar(art_text_trimmed)))
        recovery_candidates <<- c(recovery_candidates, i)
        return(invisible(NULL))
      }
    }

    if (nchar(art_text) < 10) {
      cat(sprintf("    SILENT DROP: '%s' — extracted only %d chars\n",
                  str_sub(arts$title[i], 1, 40), nchar(art_text)))
      recovery_candidates <<- c(recovery_candidates, i)
      return(invisible(NULL))
    }

    article_num <<- article_num + 1L
    cat_label <- abbrev_category(arts$category[i])
    fn <- file.path(out_folder,
      paste0(prefix, "_", sprintf("%02d", article_num), "_",
             cat_label, "_",
             short_title_filename(arts$title[i]), ".txt"))
    write_file(art_text, fn)
    if (!is.na(body_start_pos) && !is.na(body_end_pos)) {
      record_saved(fn, body_start_pos, body_end_pos)
    }
    saved_count <<- saved_count + 1L
    saved_flags[i] <<- TRUE
    saved_files[i] <<- fn
    cat(sprintf("    Saved [%02d] %s: '%s' (%s chars)\n",
                article_num, tier_label, str_sub(arts$title[i], 1, 50),
                format(nchar(art_text), big.mark=",")))
    invisible(NULL)
  }

  # --- Tier 1: Title-found articles ---
  title_idx <- which(!is.na(positions) & search_method == "title")
  cat(sprintf("  Tier 1 (title-found): %d articles\n", length(title_idx)))

  if (length(title_idx) > 0) {
    title_sorted <- title_idx[order(positions[title_idx])]

    for (k in seq_along(title_sorted)) {
      i <- title_sorted[k]
      start_pos <- positions[i]

      # Naive end: next title-found position or body_end
      if (k < length(title_sorted)) {
        naive_end <- positions[title_sorted[k + 1]]
      } else {
        naive_end <- body_end
      }

      # Content-aware refinement (only shortens, never extends)
      end_pos <- find_article_end(
        body_text, start_pos, i, arts, page_index, body_start, body_end,
        title_pos_map, naive_end, tier = "T1"
      )

      if (end_pos <= start_pos) {
        # Instead of skipping, use minimum extraction window (2000 chars)
        end_pos <- min(body_end, start_pos + 2000L)
        if (end_pos <= start_pos) {
          cat(sprintf("    SKIP: '%s' — end <= start (%d <= %d)\n",
                      str_sub(arts$title[i], 1, 40), end_pos, start_pos))
          recovery_candidates <- c(recovery_candidates, i)
          next
        }
        cat(sprintf("    T1 BOUNDARY FIX: '%s' — end<=start, using %d chars from pos %d\n",
                    str_sub(arts$title[i], 1, 40), end_pos - start_pos, start_pos))
      }

      art_text <- str_sub(body_text, start_pos, end_pos - 1L)

      # --- T1 SIZE VALIDATION ---
      # When a T1 article extracts < 500 chars and it's not a poem, the title
      # match was likely a false positive (wrong position in body). Try page-
      # estimated extraction instead: use get_article_page_range() to find
      # where the article SHOULD be, check if title words appear there, and
      # use the larger page-estimated text if valid.
      art_len <- nchar(str_trim(art_text))
      is_poem_art <- isTRUE(arts$category[i] == "Poetry")
      size_threshold <- if (is_poem_art) 80L else 500L

      if (art_len < size_threshold && art_len > 0L) {
        pr <- get_article_page_range(arts, i, page_index, body_start, body_end,
                                      buffer = 5000L)
        pe_text <- str_sub(body_text, pr$start, pr$end)

        # Check if significant title words appear in the page-estimated text
        sig_words <- get_significant_words(arts$title[i])
        sig_words <- sig_words[nchar(sig_words) >= 4]
        pe_valid <- FALSE

        if (length(sig_words) >= 1) {
          check_words <- sig_words[seq_len(min(3L, length(sig_words)))]
          pe_header <- str_sub(pe_text, 1L, min(1500L, nchar(pe_text)))
          pe_hits <- sum(vapply(check_words, function(w)
            str_detect(pe_header, fixed(w, ignore_case = TRUE)),
            logical(1)))
          pe_valid <- pe_hits >= 1L && nchar(pe_text) > art_len
        }

        if (pe_valid) {
          # Apply find_article_end to the page-estimated range too
          pe_naive_end <- pr$end
          pe_end <- find_article_end(
            body_text, pr$start, i, arts, page_index, body_start, body_end,
            title_pos_map, pe_naive_end, tier = "T1"
          )
          pe_text_refined <- str_sub(body_text, pr$start, pe_end - 1L)

          cat(sprintf("    T1 SIZE FIX: '%s' re-extracted from page est (%d -> %d chars)\n",
                      str_sub(arts$title[i], 1, 40), art_len, nchar(pe_text_refined)))
          start_pos <- pr$start
          end_pos <- pe_end
          art_text <- pe_text_refined
        }
      }

      mark_used(start_pos, end_pos - 1L)
      save_article(i, art_text, "T1", start_pos, end_pos - 1L)
    }
  }

  # --- Step 9b: Follow "continued on page X" markers ---
  # After T1 extraction, check each saved article for continuation markers.
  # If found, extract text from the continuation page and append it to the
  # article file. This captures multi-page articles that were split by
  # find_article_end()'s "continued" signal.
  cat(sprintf("  Step 9b: Checking %d saved articles for continuation markers\n",
              length(saved_ranges)))
  if (length(saved_ranges) > 0) {
    for (sr_i in seq_along(saved_ranges)) {
      sr <- saved_ranges[[sr_i]]
      if (!file.exists(sr$file)) next
      existing <- read_file(sr$file)
      cont_m <- str_match(existing,
        regex("\\(continued on page (\\d+)\\)", ignore_case = TRUE))
      if (is.na(cont_m[1,1])) next

      cont_page <- as.integer(cont_m[1, 2])
      cont_pos <- page_to_pos(page_index, cont_page, body_start, body_end)
      cat(sprintf("    CONT_CHECK: '%s' -> pg %d -> pos %d\n",
                  basename(sr$file), cont_page, cont_pos))
      if (is.na(cont_pos) || cont_pos <= body_start) {
        cat("      SKIP: cont_pos invalid\n")
        next
      }

      # Don't follow if continuation lands in already-used text
      check_start <- min(cont_pos, body_end)
      check_end   <- min(cont_pos + 100L, body_end)
      if (check_end > check_start && all(body_used[check_start:check_end])) {
        cat("      SKIP: continuation area already used\n")
        next
      }

      # Find end of continuation: next found article position or page boundary
      next_art_pos <- body_end
      if (length(title_idx) > 0) {
        later <- positions[title_idx][positions[title_idx] > cont_pos + 200L]
        if (length(later) > 0) next_art_pos <- min(later)
      }
      # Also cap at 2 pages past continuation start (~5000 chars)
      cont_end <- min(next_art_pos, cont_pos + 5000L, body_end)

      if (cont_end <= cont_pos + 50L) next

      cont_text <- str_sub(body_text, cont_pos, cont_end - 1L)
      cont_text <- clean_vol50_headers(cont_text)
      cont_text <- clean_ocr_body(cont_text)
      cont_text <- str_trim(cont_text)

      # Trim trailing ads from continuation
      cont_ad <- detect_ad_start(cont_text, min_article_len = 50L)
      if (!is.na(cont_ad) && cont_ad > 50L) {
        cont_text <- str_trim(str_sub(cont_text, 1L, cont_ad - 1L))
      }

      if (nchar(cont_text) > 50L) {
        # Append continuation to the article file
        write_file(paste0(existing, "\n\n", cont_text), sr$file)
        mark_used(cont_pos, cont_end - 1L)
        record_saved(sr$file, cont_pos, cont_end - 1L)
        cat(sprintf("    CONTINUATION: '%s' +%s chars from pg %d\n",
                    basename(sr$file), format(nchar(cont_text), big.mark = ","),
                    cont_page))
      }
    }
  }

  # --- Misc Frontispiece: poems found in pre-body text ---
  misc_fp_idx <- which(!is.na(positions) & search_method == "misc_frontispiece")
  if (length(misc_fp_idx) > 0 && exists("misc_text") && nchar(misc_text) >= 30) {
    cat(sprintf("  Misc frontispiece: %d poems from pre-body text\n", length(misc_fp_idx)))
    for (i in misc_fp_idx) {
      # Extract just the poem from misc_text, not the entire pre-body text.
      # Search for the poem title, then extract from there to the next section break.
      ft <- str_trim(str_replace(arts$title[i],
        regex("\\s*[-\u2014\u2013]+\\s*Frontispiece\\s*$", ignore_case=TRUE), ""))
      ftw <- get_significant_words(ft)
      ftw <- ftw[nchar(ftw) >= 4]
      fp_text <- ""
      if (length(ftw) >= 1) {
        # Find first title word in misc_text
        first_word <- ftw[1]
        fp_loc <- str_locate(misc_text, fixed(first_word, ignore_case=TRUE))
        if (!is.na(fp_loc[1,1])) {
          # Start extraction from ~100 chars before the title word match
          fp_start <- max(1L, fp_loc[1,1] - 100L)
          # End at next section break or max 2000 chars
          fp_chunk <- str_sub(misc_text, fp_start, min(nchar(misc_text), fp_start + 2000L))
          # Look for a section break (double newline, dash separator, or all-caps header)
          break_loc <- str_locate(fp_chunk, regex(
            "\n\\s*\n\\s*\n|(?<=\n)[\u2014\u2013—-]{3,}|(?<=\n)[A-Z][A-Z ]{10,}(?=\n)"))
          if (!is.na(break_loc[1,1]) && break_loc[1,1] > 20) {
            fp_text <- str_trim(str_sub(fp_chunk, 1, break_loc[1,1] - 1))
          } else {
            fp_text <- str_trim(str_sub(fp_chunk, 1, min(1500L, nchar(fp_chunk))))
          }
        }
      }
      fp_text <- clean_ocr_body(fp_text)
      if (nchar(fp_text) >= 20) {
        article_num <- article_num + 1L
        fn <- file.path(out_folder,
          paste0(prefix, "_", sprintf("%02d", article_num), "_Frnt_",
                 short_title_filename(arts$title[i]), ".txt"))
        write_file(fp_text, fn)
        cat(sprintf("    Saved [%02d] FRNT: '%s' (%s chars from misc text)\n",
                    article_num, str_sub(arts$title[i], 1, 50),
                    format(nchar(fp_text), big.mark=",")))
        saved_count <- saved_count + 1L
        saved_flags[i] <- TRUE
      }
    }
  }

  # --- Tier 2: PAGE_EST articles ---
  # These extract independently using page-range boundaries.
  # They do NOT affect Tier 1 boundaries.
  pest_idx <- which(!is.na(positions) & search_method == "page_est")
  cat(sprintf("  Tier 2 (page_est): %d articles\n", length(pest_idx)))

  if (length(pest_idx) > 0) {
    # Sort T2 articles by position to prevent overlap between consecutive T2s
    pest_sorted <- pest_idx[order(positions[pest_idx])]
    t2_positions <- positions[pest_sorted]

    # ---- T2 clustering detection and pre-scan ----
    # When page index is clustered (many T2 articles map to nearly
    # the same PAGE_EST position), the index is unreliable and T2
    # RE-SEARCH rejects correct matches.  Detect clustering, then
    # pre-scan only the clustered articles for proper extraction.
    t2_prescan <- data.frame(idx = integer(0), position = integer(0),
                             stringsAsFactors = FALSE)
    t2_pest_pos <- positions[pest_idx]
    clustered_pest <- integer(0)
    if (length(t2_pest_pos) >= 5) {
      for (j in seq_along(pest_idx)) {
        pj <- t2_pest_pos[j]
        if (is.na(pj)) next
        nearby <- sum(abs(t2_pest_pos - pj) < 2000, na.rm = TRUE)
        if (nearby >= 5) clustered_pest <- c(clustered_pest, pest_idx[j])
      }
      clustered_pest <- unique(clustered_pest)
    }
    if (length(clustered_pest) > 0) {
      cat(sprintf("  T2 CLUSTER: %d articles have PAGE_EST in a dense cluster\n",
                  length(clustered_pest)))
      for (ai in clustered_pest) {
        hit <- find_in_window(body_text, arts$title[ai], body_start,
                              body_end, allow_s10 = TRUE)
        if (!is.na(hit)) {
          hit_start <- max(snap_to_para_start(body_text, hit, body_start),
                           body_start)
          t2_prescan <- rbind(t2_prescan,
                              data.frame(idx = ai, position = hit_start))
        }
      }
      if (nrow(t2_prescan) > 0) {
        t2_prescan <- t2_prescan[order(t2_prescan$position), ]
        # Remove duplicates: if two articles map to the same position
        # (within 100 chars), remove both — ambiguous match.
        pos_bin <- round(t2_prescan$position / 100)
        dup_mask <- duplicated(pos_bin) | duplicated(pos_bin, fromLast = TRUE)
        if (any(dup_mask)) {
          cat(sprintf("    Removed %d ambiguous duplicate positions\n",
                      sum(dup_mask)))
          t2_prescan <- t2_prescan[!dup_mask, ]
        }
        cat(sprintf("  T2 PRE-SCAN: Located %d / %d clustered articles\n",
                    nrow(t2_prescan), length(clustered_pest)))
        # ---- Monotonicity validation ----
        # Pre-scan positions should follow page order.  If an article's
        # pre-scan position violates the monotonic trend relative to most
        # other pre-scanned articles, it is likely a false title match
        # (e.g., "Editorial" matching "Editorial and Business Office").
        # For out-of-sequence entries, try bounded re-search between
        # page-order neighbors; if not found, drop from the pre-scan.
        if (nrow(t2_prescan) >= 3) {
          t2_prescan$page <- arts$page[t2_prescan$idx]
          n_ps <- nrow(t2_prescan)
          bad_seq <- logical(n_ps)
          for (j in seq_len(n_ps)) {
            pg_j  <- t2_prescan$page[j]
            pos_j <- t2_prescan$position[j]
            violations <- sum(
              (t2_prescan$page[-j] < pg_j & t2_prescan$position[-j] > pos_j) |
              (t2_prescan$page[-j] > pg_j & t2_prescan$position[-j] < pos_j)
            )
            if (violations > (n_ps - 1) / 2) bad_seq[j] <- TRUE
          }
          if (any(bad_seq)) {
            good <- t2_prescan[!bad_seq, ]
            for (j in which(bad_seq)) {
              ai   <- t2_prescan$idx[j]
              pg_j <- t2_prescan$page[j]
              before_pg <- good[good$page < pg_j, ]
              after_pg  <- good[good$page > pg_j, ]
              lo <- if (nrow(before_pg) > 0) max(before_pg$position) else body_start
              hi <- if (nrow(after_pg) > 0) min(after_pg$position) else body_end
              if (hi > lo) {
                re_hit <- find_in_window(body_text, arts$title[ai],
                                         lo, hi, allow_s10 = FALSE)
                if (!is.na(re_hit)) {
                  re_pos <- re_hit
                  # Check overlap with existing good prescan positions
                  good_pos <- t2_prescan$position[!bad_seq]
                  if (any(abs(good_pos - re_pos) < 100)) {
                    # Match overlaps a neighbor: use page-proportional
                    # interpolation instead (the title matched in the
                    # right area but too close to an adjacent article)
                    pg_lo <- if (nrow(before_pg) > 0) max(before_pg$page) else pg_j - 1
                    pg_hi <- if (nrow(after_pg) > 0) min(after_pg$page) else pg_j + 1
                    frac <- (pg_j - pg_lo) / max(1L, pg_hi - pg_lo)
                    re_pos <- as.integer(lo + frac * (hi - lo))
                    t2_prescan$position[j] <- re_pos
                    bad_seq[j] <- FALSE
                    cat(sprintf("    MONO INTERPOLATE: '%s' (pg %d) at pos %d (frac %.2f, range %d-%d)\n",
                                str_sub(arts$title[ai], 1, 40), pg_j, re_pos, frac, lo, hi))
                  } else {
                    t2_prescan$position[j] <- re_pos
                    bad_seq[j] <- FALSE
                    cat(sprintf("    MONO RE-SEARCH: '%s' (pg %d) corrected to pos %d (range %d-%d)\n",
                                str_sub(arts$title[ai], 1, 40), pg_j, re_pos, lo, hi))
                  }
                } else {
                  cat(sprintf("    MONO DROP: '%s' (pg %d) at pos %d — not found in range %d-%d\n",
                              str_sub(arts$title[ai], 1, 40), pg_j,
                              t2_prescan$position[j], lo, hi))
                }
              }
            }
            if (any(bad_seq)) {
              cat(sprintf("    Removed %d out-of-sequence pre-scan positions\n",
                          sum(bad_seq)))
              t2_prescan <- t2_prescan[!bad_seq, ]
            }
            t2_prescan <- t2_prescan[order(t2_prescan$position), ]
          }
          t2_prescan$page <- NULL
        }
      }
    }

    for (ki in seq_along(pest_sorted)) {
      i <- pest_sorted[ki]

      # ---- Prescan fast-path for clustered articles ----
      # When an article is in the pre-scan map, use the pre-scanned
      # position directly.  PAGE_EST positions in clustered indices are
      # unreliable; bypassing them avoids false-positive title validation
      # (e.g., a common word like "Song" appearing by chance at PAGE_EST).
      if (nrow(t2_prescan) > 0 && (i %in% t2_prescan$idx)) {
        ps_ri  <- which(t2_prescan$idx == i)
        ps_pos <- t2_prescan$position[ps_ri[1]]
        ps_later <- t2_prescan$position[t2_prescan$position > ps_pos]
        ps_end <- if (length(ps_later) > 0) ps_later[1] else body_end
        # Cap at next Tier 1 position
        if (length(title_idx) > 0) {
          later_t <- positions[title_idx][positions[title_idx] > ps_pos]
          if (length(later_t) > 0) ps_end <- min(ps_end, min(later_t))
        }
        if (ps_end > ps_pos) {
          art_text <- str_sub(body_text, ps_pos, ps_end - 1L)
          cat(sprintf("    T2 PRESCAN ACCEPT: '%s' at pos %d (end %d, %s chars)\n",
                      str_sub(arts$title[i], 1, 40), ps_pos, ps_end,
                      format(ps_end - ps_pos, big.mark = ",")))
          mark_used(ps_pos, ps_end - 1L)
          save_article(i, art_text, "T2", ps_pos, ps_end - 1L,
                       method = search_method[i])
          next  # skip normal T2 processing
        }
      }

      start_pos <- positions[i]
      # Use page-range boundary with generous buffer
      pr <- get_article_page_range(arts, i, page_index, body_start, body_end, buffer = 3000L)

      # Start from position, end at page-range boundary
      naive_end <- pr$end
      # But also don't extend past the next title-found position
      # (to avoid duplicating content already extracted in Tier 1)
      if (length(title_idx) > 0) {
        title_positions <- positions[title_idx]
        later_titles <- title_positions[title_positions > start_pos]
        if (length(later_titles) > 0) {
          naive_end <- min(naive_end, min(later_titles))
        }
      }
      # Also cap at next T2 article's start position to prevent overlap
      if (ki < length(pest_sorted)) {
        next_t2_pos <- t2_positions[ki + 1]
        if (!is.na(next_t2_pos) && next_t2_pos > start_pos) {
          naive_end <- min(naive_end, next_t2_pos)
        }
      }

      if (naive_end <= start_pos) {
        # Use minimum extraction length (3000 chars) when boundary calc fails
        naive_end <- min(body_end, start_pos + 3000L)
        cat(sprintf("    T2 BOUNDARY FIX: '%s' — using %d chars from pos %d\n",
                    str_sub(arts$title[i], 1, 40), naive_end - start_pos, start_pos))
      }

      # Content-aware refinement (only shortens, never extends)
      end_pos <- find_article_end(
        body_text, start_pos, i, arts, page_index, body_start, body_end,
        title_pos_map, naive_end, tier = "T2"
      )

      t2_used_start <- start_pos; t2_used_end <- end_pos - 1L
      art_text <- str_sub(body_text, start_pos, end_pos - 1L)

      # Tier 2 validation: PAGE_EST positions are approximate.
      # If title words missing from extracted text, try full-body re-search.
      sig_words <- get_significant_words(arts$title[i])
      sig_words <- sig_words[nchar(sig_words) >= 4]
      t2_valid <- TRUE
      if (length(sig_words) >= 1) {
        check_region <- str_sub(art_text, 1, min(1200L, nchar(art_text)))
        t2_hits <- sum(sapply(sig_words[1:min(3, length(sig_words))], function(w)
          str_detect(check_region, fixed(w, ignore_case=TRUE))))
        if (t2_hits == 0L) {
          t2_valid <- FALSE
          # Re-search: try full-body title search to find correct position
          re_pos <- find_in_window(body_text, arts$title[i], body_start, body_end, allow_s10=TRUE)
          if (!is.na(re_pos)) {
            re_start <- max(snap_to_para_start(body_text, re_pos, body_start), body_start)
            # Validate: re-searched position must be near the page estimate
            # Allow generous tolerance: within page-range window ± 5000 chars
            pr_tolerance <- 5000L
            if (re_start >= (pr$start - pr_tolerance) && re_start <= (pr$end + pr_tolerance)) {
              re_end <- pr$end  # use page-range end
              # Also cap at next Tier 1 position
              if (length(title_idx) > 0) {
                later_t <- positions[title_idx][positions[title_idx] > re_start]
                if (length(later_t) > 0) re_end <- min(re_end, min(later_t))
              }
              if (re_end > re_start) {
                t2_used_start <- re_start; t2_used_end <- re_end - 1L
                art_text <- str_sub(body_text, re_start, re_end - 1L)
                t2_valid <- TRUE
                cat(sprintf("    T2 RE-SEARCH: '%s' found at pos %d\n",
                            str_sub(arts$title[i], 1, 40), re_start))
              }
            } else {
              # Use T2 pre-scan map: the full-body title search found
              # this article at a position the page range rejects.
              # Accept it if the pre-scan agrees, using sorted inter-
              # article boundaries from the pre-scan for proper end caps.
              ps_accepted <- FALSE
              if (nrow(t2_prescan) > 0 && (i %in% t2_prescan$idx)) {
                ps_ri  <- which(t2_prescan$idx == i)
                ps_pos <- t2_prescan$position[ps_ri[1]]
                # End at next pre-scan article or body_end
                ps_later <- t2_prescan$position[
                  t2_prescan$position > ps_pos]
                re_end <- if (length(ps_later) > 0) ps_later[1] else
                  body_end
                # Also cap at next Tier 1 position
                if (length(title_idx) > 0) {
                  later_t <- positions[title_idx][
                    positions[title_idx] > ps_pos]
                  if (length(later_t) > 0)
                    re_end <- min(re_end, min(later_t))
                }
                if (re_end > ps_pos) {
                  t2_used_start <- ps_pos
                  t2_used_end   <- re_end - 1L
                  art_text <- str_sub(body_text, ps_pos, re_end - 1L)
                  t2_valid     <- TRUE
                  ps_accepted  <- TRUE
                  cat(sprintf(
                    "    T2 PRESCAN ACCEPT: '%s' at pos %d (end %d, %s chars)\n",
                    str_sub(arts$title[i], 1, 40), ps_pos, re_end,
                    format(re_end - ps_pos, big.mark = ",")))
                }
              }
              if (!ps_accepted) {
                cat(sprintf("    T2 RE-SEARCH REJECTED: '%s' found at pos %d but page range is %d-%d\n",
                            str_sub(arts$title[i], 1, 40), re_start,
                            pr$start, pr$end))
              }
            }
          }
        }
      }

      # --- Birthday content pattern rescue ---
      # Birthday Congratulations sections often lack a visible title in OCR
      # body text (heading was likely a graphic). When title search fails,
      # search for the distinctive age+name list pattern that characterizes
      # Birthday pages: consecutive age numbers (90-102) followed by Mrs./Mr. entries.
      bday_too_short <- str_detect(arts$title[i], regex("Birthday", ignore_case = TRUE)) &&
                        nchar(str_trim(art_text)) < 200L
      if ((!t2_valid || bday_too_short) &&
          str_detect(arts$title[i], regex("Birthday", ignore_case = TRUE))) {
        # Search the latter 50% of body for age-number clusters near name entries
        bday_search_start <- body_start + as.integer((body_end - body_start) * 0.5)
        bday_search_text <- str_sub(body_text, bday_search_start, body_end)
        # Pattern: two consecutive age-like numbers (90-102) near each other
        bday_pat <- "\\b(10[0-2]|9[0-9])\\s+(10[0-2]|9[0-9])\\b"
        bday_m <- str_locate(bday_search_text, regex(bday_pat))
        if (!is.na(bday_m[1,1])) {
          bday_abs <- bday_search_start + bday_m[1,1] - 1L
          # Verify: "Mrs." should appear within 300 chars after the ages
          bday_verify <- str_sub(body_text, bday_abs,
                                 min(bday_abs + 500L, body_end))
          if (str_detect(bday_verify, "Mrs\\.")) {
            bday_end <- body_end
            # Cap at next T1 position
            if (length(title_idx) > 0) {
              later_t1 <- positions[title_idx][positions[title_idx] > bday_abs]
              if (length(later_t1) > 0) bday_end <- min(bday_end, min(later_t1))
            }
            bday_text <- str_sub(body_text, bday_abs, bday_end - 1L)
            # NOTE: Do NOT apply ad trimming to Birthday name lists —
            # they contain city/state/ZIP patterns that look like ad content
            if (nchar(str_trim(bday_text)) > nchar(str_trim(art_text))) {
              cat(sprintf("    T2 BIRTHDAY RESCUE: '%s' found age+name list at pos %d (%d chars)\n",
                          str_sub(arts$title[i], 1, 40), bday_abs, nchar(bday_text)))
              art_text <- bday_text
              t2_used_start <- bday_abs
              t2_used_end <- bday_abs + nchar(bday_text) - 1L
              t2_valid <- TRUE
            }
          }
        }
      }

      if (!t2_valid) {
        # Title not found anywhere — keep PAGE_EST content (approximate)
        # This ensures ALL TEXT is captured even when OCR destroyed the title
        art_text <- str_sub(body_text, start_pos, end_pos - 1L)
        cat(sprintf("    T2 KEEP (no title match): '%s' — using page-est content\n",
                    str_sub(arts$title[i], 1, 40)))
      }

      mark_used(t2_used_start, t2_used_end)
      save_article(i, art_text, "T2", t2_used_start, t2_used_end,
                   method = search_method[i])
    }
  }

  # Extract "From Near and Far" (reader letters) from cover/Misc text if not found in body
  for (mi in miss_idx) {
    if (!str_detect(arts$title[mi], regex("^From Near and Far", ignore_case=TRUE))) next
    if (nchar(misc_text) < 100) next
    # Reader letters are in the cover text — look for signature pattern
    if (str_detect(misc_text, regex("[\u2014\u2013—-]\\s*[A-Z][a-z]+.*(?:Utah|California|Idaho|England|Canada|Washington|Oregon|Arizona)",
                                     ignore_case=TRUE))) {
      # Split misc_text: reader letters (From Near and Far) vs masthead/publishing info
      # Look for masthead marker: "MONTH YEAR VOLUME NN" or "Editor ... Associate Editor"
      masthead_pat <- paste0("(?:", toupper(meta$month), ")\\s+\\d{4}\\s+VOLUME\\s+\\d+")
      split_loc <- str_locate(misc_text, regex(masthead_pat, ignore_case=FALSE))
      fnf_text <- misc_text  # default: full text
      remaining_misc <- ""
      if (!is.na(split_loc[1, "start"])) {
        fnf_text <- str_trim(str_sub(misc_text, 1, split_loc[1, "start"] - 1L))
        remaining_misc <- str_trim(str_sub(misc_text, split_loc[1, "start"]))
      }
      if (nchar(fnf_text) < 50) fnf_text <- misc_text  # safety: if split failed

      article_num <- article_num + 1L
      fn <- file.path(out_folder,
        paste0(prefix, "_", sprintf("%02d", article_num), "_",
               abbrev_category(arts$category[mi]), "_From_Near_and_Far.txt"))
      write_file(clean_ocr_body(str_trim(fnf_text)), fn)
      cat(sprintf("    Saved [%02d]: FROM NEAR AND FAR from cover text (%s chars)\n",
                  article_num, format(nchar(fnf_text), big.mark=",")))
      saved_count <- saved_count + 1L
      saved_flags[mi] <- TRUE
      saved_files[mi] <- fn

      # Update Misc file to contain only masthead/publishing info (avoid duplication)
      # Preserve any [ADS FROM:] content that strip_mid_article_ads appended
      misc_file <- file.path(out_folder, paste0(prefix, "_00_Misc.txt"))
      ads_from_text <- ""
      if (file.exists(misc_file)) {
        old_misc <- read_file(misc_file)
        ads_loc <- str_locate(old_misc, fixed("\n\n[ADS FROM:"))
        if (!is.na(ads_loc[1,1])) {
          ads_from_text <- str_sub(old_misc, ads_loc[1,1])
        }
      }
      if (nchar(remaining_misc) > 10) {
        write_file(paste0(str_trim(remaining_misc), ads_from_text), misc_file)
        cat(sprintf("    Updated Misc: removed From Near and Far letters (%s chars remaining)\n",
                    format(nchar(remaining_misc), big.mark=",")))
      } else if (nchar(ads_from_text) > 0) {
        write_file(str_trim(ads_from_text), misc_file)
        cat("    Removed FNF from Misc (preserved ad content)\n")
      } else {
        # Split failed or trivial remainder — FNF file has same content as Misc
        # Remove the Misc file to avoid duplication
        if (file.exists(misc_file)) {
          file.remove(misc_file)
          cat("    Removed duplicate Misc file (content now in From Near and Far)\n")
        }
      }
    }
  }

  # Extract frontispiece from cover/Misc text if not found in body
  # The frontispiece poem is typically on the magazine cover (pre-body text)
  # NOTE: Don't create a separate file — content is already in _00_Misc.txt
  for (mi in miss_idx) {
    if (arts$category[mi] != "Frontispiece") next
    if (nchar(misc_text) < 20) next

    # Search for frontispiece title words in the cover/Misc text
    fp_title <- str_trim(str_replace(arts$title[mi],
      regex("\\s*[-\u2014]?\\s*Frontispiece\\s*$", ignore_case=TRUE), ""))
    fp_words <- get_significant_words(fp_title)

    if (length(fp_words) >= 1) {
      # Check if any significant word appears in the cover text
      found_in_cover <- FALSE
      for (w in fp_words) {
        if (str_detect(misc_text, regex(w, ignore_case=TRUE))) {
          found_in_cover <- TRUE
          break
        }
      }
      if (found_in_cover) {
        # Content is already in _00_Misc.txt — count as found but don't duplicate
        saved_count <- saved_count + 1L
        saved_flags[mi] <- TRUE
        cat(sprintf("    FRONTISPIECE '%s' found in Misc text (no duplicate file)\n",
                    str_sub(arts$title[mi], 1, 50)))
      }
    }
  }

  # Extract "Birthday Congratulations" (generic birthday listing) from the end of body text
  # Birthday lists always appear at the very end of the issue, with patterns:
  #   - Age words: "Ninety-nine Mrs. Agnes Brack McCleod" (most months)
  #   - Name-only: "Mrs. Apalu Bissell Harrison, Springville, Utah" (Aug/Sep)
  # Only extract the generic listing (title ends with "Congratulations"),
  # NOT feature articles like "Birthday Congratulations to Emma Ray Riggs McKay"
  bday_listing_saved <- FALSE
  for (mi in miss_idx) {
    if (!str_detect(arts$title[mi], regex("^Birthday Congrat\\w*\\s*$", ignore_case=TRUE))) next
    if (bday_listing_saved) next  # only save one birthday listing per issue
    body_len <- body_end - body_start + 1L
    bday_found <- FALSE

    # Hard cap: birthday lists are name lists typically 1-3 pages (2000-4000 chars)
    # Don't use find_article_end() here — name lists (addresses, cities) trigger
    # false positives in ad detection. Use hard cap + next-title trimming instead.
    bday_hard_cap <- 4000L

    # Strategy 1: Search last 35% for age-word + Mrs. + name (handles mixed case)
    bday_s1 <- max(body_start, body_end - as.integer(body_len * 0.35))
    bday_chunk1 <- str_sub(body_text, bday_s1, body_end)
    bday_pat1 <- regex(
      "(?:Ninety|Eighty|Seventy|Sixty|One Hundred|Hundred)[- ]?(?:nine|eight|seven|six|five|four|three|two|one)?\\s+Mrs\\.\\s+[A-Z][a-zA-Z]",
      ignore_case=FALSE)
    bday_m1 <- str_locate(bday_chunk1, bday_pat1)
    if (!is.na(bday_m1[1,1])) {
      bday_pos <- bday_s1 + bday_m1[1,1] - 1L
      bday_end_pos <- min(body_end, bday_pos + bday_hard_cap)
      bday_text <- str_trim(str_sub(body_text, bday_pos, bday_end_pos))
      bday_result <- trim_bday_ads(bday_text)
      bday_text <- bday_result$text
      if (nchar(bday_result$ads) > 10L)
        misc_ad_text <- c(misc_ad_text, paste0("--- Ad from: Birthday Congratulations ---\n", bday_result$ads))
      if (nchar(bday_text) >= 100) {
        mark_used(bday_pos, bday_end_pos)
        article_num <- article_num + 1L
        fn <- file.path(out_folder,
          paste0(prefix, "_", sprintf("%02d", article_num), "_",
                 abbrev_category(arts$category[mi]), "_Bday_Congrats.txt"))
        write_file(clean_ocr_body(bday_text), fn)
        record_saved(fn, bday_pos, bday_end_pos)
        cat(sprintf("    Saved [%02d]: BIRTHDAY CONGRATULATIONS (age-word) from body end (%s chars)\n",
                    article_num, format(nchar(bday_text), big.mark=",")))
        saved_count <- saved_count + 1L
        saved_flags[mi] <- TRUE
        bday_found <- TRUE
        bday_listing_saved <- TRUE
      }
    }

    # Strategy 2: Search last 15% for cluster of "Mrs." + full name + city, state
    # Used when no age markers (Aug, some Sep months)
    if (!bday_found) {
      bday_s2 <- max(body_start, body_end - as.integer(body_len * 0.15))
      bday_chunk2 <- str_sub(body_text, bday_s2, body_end)
      # Look for 3+ "Mrs." entries in close proximity (birthday list signature)
      mrs_locs <- str_locate_all(bday_chunk2, regex("Mrs\\.\\s+[A-Z][a-zA-Z]+\\s+[A-Z]"))[[1]]
      if (nrow(mrs_locs) >= 3) {
        # Check if at least 3 Mrs. entries are within 2000 chars of each other
        for (k in 1:(nrow(mrs_locs) - 2)) {
          if (mrs_locs[k+2, 1] - mrs_locs[k, 1] < 2000L) {
            bday_pos <- bday_s2 + mrs_locs[k, 1] - 1L
            bday_end_pos <- min(body_end, bday_pos + bday_hard_cap)
            bday_text <- str_trim(str_sub(body_text, bday_pos, bday_end_pos))
            bday_result2 <- trim_bday_ads(bday_text)
            bday_text <- bday_result2$text
            if (nchar(bday_result2$ads) > 10L)
              misc_ad_text <- c(misc_ad_text, paste0("--- Ad from: Birthday Congratulations ---\n", bday_result2$ads))
            if (nchar(bday_text) >= 100) {
              mark_used(bday_pos, bday_end_pos)
              article_num <- article_num + 1L
              fn <- file.path(out_folder,
                paste0(prefix, "_", sprintf("%02d", article_num), "_",
                       abbrev_category(arts$category[mi]), "_Bday_Congrats.txt"))
              write_file(clean_ocr_body(bday_text), fn)
              record_saved(fn, bday_pos, bday_end_pos)
              cat(sprintf("    Saved [%02d]: BIRTHDAY CONGRATULATIONS (Mrs. cluster) from body end (%s chars)\n",
                          article_num, format(nchar(bday_text), big.mark=",")))
              saved_count <- saved_count + 1L
              saved_flags[mi] <- TRUE
              bday_found <- TRUE
              bday_listing_saved <- TRUE
            }
            break
          }
        }
      }
    }

    # Strategy 3: heading text "BIRTHDAY CONGRATULATIONS"
    if (!bday_found) {
      bday_s3 <- max(body_start, body_end - as.integer(body_len * 0.40))
      bday_chunk3 <- str_sub(body_text, bday_s3, body_end)
      bday_m3 <- str_locate(bday_chunk3,
        regex("BIRTHDAY\\s+CONGRATULATIONS", ignore_case=TRUE))
      if (!is.na(bday_m3[1,1])) {
        bday_pos <- bday_s3 + bday_m3[1,1] - 1L
        bday_end_pos <- min(body_end, bday_pos + bday_hard_cap)
        bday_text <- str_trim(str_sub(body_text, bday_pos, bday_end_pos))
        bday_result3 <- trim_bday_ads(bday_text)
        bday_text <- bday_result3$text
        if (nchar(bday_result3$ads) > 10L)
          misc_ad_text <- c(misc_ad_text, paste0("--- Ad from: Birthday Congratulations ---\n", bday_result3$ads))
        if (nchar(bday_text) >= 100) {
          mark_used(bday_pos, bday_end_pos)
          article_num <- article_num + 1L
          fn <- file.path(out_folder,
            paste0(prefix, "_", sprintf("%02d", article_num),
                   "_GenFt_Bday_Congrats.txt"))
          write_file(clean_ocr_body(bday_text), fn)
          record_saved(fn, bday_pos, bday_end_pos)
          saved_count <- saved_count + 1L
          saved_flags[mi] <- TRUE
          bday_found <- TRUE
          bday_listing_saved <- TRUE
          cat(sprintf("    Saved [%02d]: BIRTHDAY CONGRATULATIONS (heading) (%s chars)\n",
                      article_num, format(nchar(bday_text), big.mark=",")))
        }
      }
    }

    if (!bday_found) {
      cat(sprintf("    BIRTHDAY: No birthday pattern found in body end\n"))
    }
  }

  # ----------------------------------------------------------
  # Step 10: Extract pre-body poems (pages before body_start)
  # ----------------------------------------------------------
  # Identify the first body page (excluding FNF, Birthday, Frontispiece, Poetry)
  body_pages <- arts$page[arts$page > 0 &
    !str_detect(arts$title, regex("^From Near and Far|^Birthday Congrat", ignore_case=TRUE)) &
    arts$category != "Frontispiece" &
    arts$category != "Poetry"]
  first_body_pg <- if (length(body_pages) > 0) min(body_pages) else 999L

  miss_idx2 <- which(is.na(positions) & !(seq_len(nrow(arts)) %in% which(!is.na(positions))))
  # Rebuild miss index to include newly missed after poem parsing changes
  miss_idx2 <- which(is.na(positions))

  for (mi in miss_idx2) {
    pg <- arts$page[mi]
    if (pg <= 0 || pg >= first_body_pg) next
    if (str_detect(arts$title[mi], regex("^From Near and Far", ignore_case=TRUE))) next
    if (str_detect(arts$title[mi], regex("^Birthday Congrat", ignore_case=TRUE))) next
    if (nchar(misc_text) < 20) next

    title <- arts$title[mi]
    fp_words <- get_significant_words(title)
    if (length(fp_words) < 1) next

    found_in_pre <- FALSE
    for (w in fp_words) {
      if (str_detect(misc_text, regex(w, ignore_case=TRUE))) {
        found_in_pre <- TRUE
        break
      }
    }
    if (found_in_pre) {
      # Content is already in _00_Misc.txt — count as found but don't duplicate
      saved_count <- saved_count + 1L
      saved_flags[mi] <- TRUE
      cat(sprintf("    PRE-BODY '%s' found in Misc text (no duplicate file)\n",
                  str_sub(title, 1, 50)))
    }
  }

  # ----------------------------------------------------------
  # Step 11: Extract missed poems from body text by title search
  # ----------------------------------------------------------
  # Many short poems share pages with articles and get embedded in
  # the preceding article's text. Search for them by title + author.
  poem_positions_used <- integer(0)  # track positions to avoid duplicate extractions
  for (mi in miss_idx2) {
    if (arts$category[mi] != "Poetry") next
    if (str_detect(arts$title[mi], regex("^From Near and Far|^Birthday Congrat", ignore_case=TRUE))) next
    # Skip if already saved as pre-body
    if (arts$page[mi] > 0 && arts$page[mi] < first_body_pg) next

    poem_title <- arts$title[mi]
    # Search full body text for the poem title
    poem_pos <- find_in_window(body_text, poem_title, body_start, body_end, allow_s10=TRUE)
    if (is.na(poem_pos)) next

    # Skip if this position overlaps with a previously extracted poem
    if (length(poem_positions_used) > 0 && any(abs(poem_pos - poem_positions_used) < 50L)) {
      cat(sprintf("    POEM SKIP (overlap): '%s' at %d\n",
                  str_sub(poem_title, 1, 40), poem_pos))
      next
    }

    # Extract poem text: from title to next substantial break
    poem_end_region <- min(body_end, poem_pos + 3000L)
    poem_chunk <- str_sub(body_text, poem_pos, poem_end_region)

    # Find poem end: triple newline, or next all-caps header, or page number marker
    poem_end_m <- str_locate(poem_chunk,
      regex("\n\\s*\n\\s*\n|(?<=\n)[A-Z][A-Z ]{15,}(?=\n)|(?<=\n)\\d{1,3} [A-Z]{2}"))
    if (!is.na(poem_end_m[1,1]) && poem_end_m[1,1] > 20) {
      poem_text <- str_sub(poem_chunk, 1, poem_end_m[1,1] - 1)
    } else {
      # Use a conservative length (most poems are under 1500 chars)
      poem_text <- str_sub(poem_chunk, 1, min(1500L, nchar(poem_chunk)))
    }
    poem_text <- str_trim(poem_text)

    # Round 5: Poem title validation removed — trust find_in_window() match.
    # Log for diagnostics but don't skip.
    if (nchar(poem_text) >= 20) {
      poem_head <- str_sub(poem_text, 1, 600L)
      ptw <- get_significant_words(poem_title)
      ptw <- ptw[nchar(ptw) >= 4]
      if (length(ptw) >= 1) {
        title_hits <- sum(sapply(ptw[1:min(3, length(ptw))], function(w)
          str_detect(poem_head, fixed(w, ignore_case=TRUE))))
        if (title_hits < 1L) {
          cat(sprintf("    POEM WARN (no title match): '%s' — saving anyway\n",
                      str_sub(poem_title, 1, 40)))
        }
      }
    }

    if (nchar(poem_text) >= 20 && nchar(poem_text) <= 5000) {
      mark_used(poem_pos, poem_pos + nchar(poem_text) - 1L)
      article_num <- article_num + 1L
      fn <- file.path(out_folder,
        paste0(prefix, "_", sprintf("%02d", article_num), "_Poem_",
               short_title_filename(poem_title), ".txt"))
      write_file(clean_ocr_body(poem_text), fn)
      record_saved(fn, poem_pos, poem_pos + nchar(poem_text) - 1L)
      cat(sprintf("    Saved [%02d]: POEM '%s' from body search (%s chars)\n",
                  article_num, str_sub(poem_title, 1, 50),
                  format(nchar(poem_text), big.mark=",")))
      saved_count <- saved_count + 1L
      saved_flags[mi] <- TRUE
      poem_positions_used <- c(poem_positions_used, poem_pos)
    }
  }

  # ----------------------------------------------------------
  # Step 11b: Re-extract POEM REJECT'd entries from correct position
  # ----------------------------------------------------------
  # POEM REJECT'd poems were found at positions[mi] but Step 9 extracted
  # starting from prev_art_end+1 (wrong start). Re-extract starting from
  # the actual find_in_window() position with poem-end detection.
  if (length(poem_reject_idx) > 0) {
    for (mi in poem_reject_idx) {
      if (saved_flags[mi]) next
      if (is.na(positions[mi])) next

      poem_pos <- positions[mi]
      poem_title <- arts$title[mi]

      # Extract poem text from the CORRECT position (where title was found)
      poem_end_region <- min(body_end, poem_pos + 3000L)
      poem_chunk <- str_sub(body_text, poem_pos, poem_end_region)

      # Find poem end: triple newline, all-caps header, or page number marker
      poem_end_m <- str_locate(poem_chunk,
        regex("\n\\s*\n\\s*\n|(?<=\n)[A-Z][A-Z ]{15,}(?=\n)|(?<=\n)\\d{1,3} [A-Z]{2}"))
      if (!is.na(poem_end_m[1,1]) && poem_end_m[1,1] > 20) {
        poem_text <- str_sub(poem_chunk, 1, poem_end_m[1,1] - 1)
      } else {
        poem_text <- str_sub(poem_chunk, 1, min(1500L, nchar(poem_chunk)))
      }
      poem_text <- str_trim(poem_text)
      poem_text <- clean_ocr_body(poem_text)

      # Light validation: at least 1 significant title word in first 500 chars
      if (nchar(poem_text) >= 20) {
        ptw <- get_significant_words(poem_title)
        ptw <- ptw[nchar(ptw) >= 4]
        if (length(ptw) >= 1) {
          phead <- str_sub(poem_text, 1, min(500L, nchar(poem_text)))
          phits <- sum(sapply(ptw[1:min(3, length(ptw))], function(w)
            str_detect(phead, fixed(w, ignore_case=TRUE))))
          if (phits == 0) next  # still can't validate, skip
        }
      }

      if (nchar(poem_text) >= 20 && nchar(poem_text) <= 5000) {
        mark_used(poem_pos, poem_pos + nchar(poem_text) - 1L)
        article_num <- article_num + 1L
        fn <- file.path(out_folder,
          paste0(prefix, "_", sprintf("%02d", article_num), "_Poem_",
                 short_title_filename(poem_title), ".txt"))
        write_file(poem_text, fn)
        record_saved(fn, poem_pos, poem_pos + nchar(poem_text) - 1L)
        cat(sprintf("    Saved [%02d]: POEM RE-EXTRACT '%s' (%s chars)\n",
                    article_num, str_sub(poem_title, 1, 50),
                    format(nchar(poem_text), big.mark=",")))
        saved_count <- saved_count + 1L
        saved_flags[mi] <- TRUE
        poem_positions_used <- c(poem_positions_used, poem_pos)
      }
    }
  }

  # ----------------------------------------------------------
  # Step 12: General body-search recovery for unsaved articles
  # ----------------------------------------------------------
  unsaved_idx <- which(!saved_flags)
  unsaved_idx <- unique(c(unsaved_idx, recovery_candidates))
  unsaved_idx <- unsaved_idx[!saved_flags[unsaved_idx]]
  # Skip From Near and Far, Birthday, Frontispiece (handled separately)
  unsaved_idx <- unsaved_idx[!str_detect(arts$title[unsaved_idx],
    regex("^From Near and Far|^Birthday Congrat", ignore_case=TRUE))]
  unsaved_idx <- unsaved_idx[arts$category[unsaved_idx] != "Frontispiece"]

  if (length(unsaved_idx) > 0) {
    cat(sprintf("  Step 12: Attempting recovery for %d unsaved articles\n",
                length(unsaved_idx)))
    recovery_positions_used <- integer(0)

    for (mi in unsaved_idx) {
      if (saved_flags[mi]) next

      title <- arts$title[mi]
      cat_type <- arts$category[mi]

      pos <- find_in_window(body_text, title, body_start, body_end, allow_s10=TRUE)
      if (is.na(pos)) next

      # Check overlap with saved positions and other recoveries
      all_used <- c(positions[!is.na(positions)], recovery_positions_used)
      if (length(all_used) > 0 && any(abs(pos - all_used) < 25L)) {
        cat(sprintf("    RECOVERY SKIP (overlap): '%s' at %d\n",
                    str_sub(title, 1, 40), pos))
        next
      }

      # End boundary: use page estimate of next CONTENTS entry
      next_entries <- which(arts$page > arts$page[mi] & arts$page[mi] > 0)
      if (length(next_entries) > 0 && nrow(page_index) >= 3) {
        next_pg <- min(arts$page[next_entries])
        end_est <- page_to_pos(page_index, next_pg, body_start, body_end)
        if (!is.na(end_est) && end_est > pos) {
          naive_end <- min(body_end, end_est)
        } else {
          naive_end <- min(body_end, pos + 10000L)
        }
      } else {
        max_len <- if (cat_type == "Poetry") 3000L
                   else if (cat_type %in% c("Lesson", "The Home")) 15000L
                   else 10000L
        naive_end <- min(body_end, pos + max_len)
      }

      # Content-aware refinement (only shortens, never extends)
      extract_end <- find_article_end(
        body_text, pos, mi, arts, page_index, body_start, body_end,
        title_pos_map, naive_end, tier = "T1"
      )

      art_text <- str_trim(str_sub(body_text, pos, extract_end))

      # Poetry: trim at poem-end markers
      if (cat_type == "Poetry" && nchar(art_text) > 500) {
        poem_end_m <- str_locate(str_sub(art_text, 100, min(nchar(art_text), 2000L)),
          regex(paste0(
            "\n\\s*\n\\s*\n",
            "|(?<=\n)[A-Z][A-Z ]{15,}(?=\n)",
            "|(?<=\n)\\d{1,3}\\s+[A-Z]{3,}\\s+\\d{4}"
          )))
        if (!is.na(poem_end_m[1,1]) && (99L + poem_end_m[1,1]) > 80) {
          art_text <- str_trim(str_sub(art_text, 1, 99L + poem_end_m[1,1] - 1L))
        }
        if (nchar(art_text) > 1500) art_text <- str_sub(art_text, 1, 1500L)
      }

      art_text <- clean_vol50_headers(art_text)
      art_text <- clean_ocr_body(art_text)
      art_text <- str_trim(art_text)

      # Round 5: Recovery validation removed — trust find_in_window() match.
      # Log for diagnostics but don't skip.
      rec_sigs <- get_significant_words(title)
      rec_sigs <- rec_sigs[nchar(rec_sigs) >= 4]
      if (length(rec_sigs) >= 2) {
        rec_head <- str_sub(art_text, 1, min(1000L, nchar(art_text)))
        rec_hits <- sum(sapply(rec_sigs[1:min(4, length(rec_sigs))], function(w)
          str_detect(rec_head, fixed(w, ignore_case=TRUE))))
        if (rec_hits == 0) {
          cat(sprintf("    RECOVERY WARN: '%s' — no title words in first 1000 chars, saving anyway\n",
                      str_sub(title, 1, 40)))
        }
      }

      if (nchar(art_text) >= 50) {
        mark_used(pos, extract_end)
        article_num <- article_num + 1L
        cat_label <- abbrev_category(cat_type)
        fn <- file.path(out_folder,
          paste0(prefix, "_", sprintf("%02d", article_num), "_",
                 cat_label, "_",
                 short_title_filename(title), ".txt"))
        write_file(art_text, fn)
        record_saved(fn, pos, extract_end)
        saved_count <- saved_count + 1L
        saved_flags[mi] <- TRUE
        recovery_positions_used <- c(recovery_positions_used, pos)
        cat(sprintf("    RECOVERY [%02d]: '%s' (%s chars)\n",
                    article_num, str_sub(title, 1, 50),
                    format(nchar(art_text), big.mark=",")))
      }
    }
  }

  # ----------------------------------------------------------
  # Step 12b: Visiting Teacher heading search
  # ----------------------------------------------------------
  # Some Visiting Teacher articles have garbled OCR titles that can't be
  # found by normal title search. The "VISITING TEACHER" heading is always
  # present in the body text even when the specific title is garbled.
  vt_unsaved <- which(!saved_flags &
    str_detect(arts$title, regex("visiting.*teacher|teacher.*message", ignore_case=TRUE)))
  if (length(vt_unsaved) > 0) {
    cat(sprintf("  Step 12b: Visiting Teacher heading search for %d unsaved articles\n",
                length(vt_unsaved)))
    for (mi in vt_unsaved) {
      if (saved_flags[mi]) next
      title <- arts$title[mi]

      # Search body text for the "VISITING TEACHER" heading
      vt_m <- str_locate(body_text, regex("Visiting\\s+Teacher", ignore_case=TRUE))
      if (is.na(vt_m[1,1])) next
      vt_pos <- vt_m[1,1]
      if (vt_pos < body_start || vt_pos > body_end) next

      # Snap to paragraph start
      vt_start <- snap_to_para_start(body_text, vt_pos, body_start)

      # Check overlap with existing positions
      all_used_vt <- c(positions[!is.na(positions)], recovery_positions_used, poem_positions_used)
      if (length(all_used_vt) > 0 && any(abs(vt_start - all_used_vt) < 50L)) {
        cat(sprintf("    VT SKIP (overlap): '%s' at %d\n", str_sub(title, 1, 40), vt_start))
        next
      }

      # End boundary: use page estimate of next entry or default
      next_entries <- which(arts$page > arts$page[mi] & arts$page[mi] > 0)
      if (length(next_entries) > 0 && nrow(page_index) >= 3) {
        next_pg <- min(arts$page[next_entries])
        end_est <- page_to_pos(page_index, next_pg, body_start, body_end)
        if (!is.na(end_est) && end_est > vt_start) {
          vt_end <- min(body_end, end_est)
        } else {
          vt_end <- min(body_end, vt_start + 10000L)
        }
      } else {
        vt_end <- min(body_end, vt_start + 10000L)
      }

      art_text <- str_trim(str_sub(body_text, vt_start, vt_end))
      art_text <- clean_vol50_headers(art_text)
      art_text <- clean_ocr_body(art_text)
      art_text <- str_trim(art_text)

      if (nchar(art_text) >= 50) {
        mark_used(vt_start, vt_end)
        article_num <- article_num + 1L
        cat_label <- abbrev_category(arts$category[mi])
        fn <- file.path(out_folder,
          paste0(prefix, "_", sprintf("%02d", article_num), "_",
                 cat_label, "_",
                 short_title_filename(title), ".txt"))
        write_file(art_text, fn)
        record_saved(fn, vt_start, vt_end)
        saved_count <- saved_count + 1L
        saved_flags[mi] <- TRUE
        recovery_positions_used <- c(recovery_positions_used, vt_start)
        cat(sprintf("    VT HEADING [%02d]: '%s' (%s chars)\n",
                    article_num, str_sub(title, 1, 50),
                    format(nchar(art_text), big.mark=",")))
      }
    }
  }

  # ----------------------------------------------------------
  # Post-save dedup: remove byte-identical article files
  # ----------------------------------------------------------
  all_art_files <- list.files(out_folder, pattern = "\\.txt$", full.names = TRUE)
  all_art_files <- all_art_files[!str_detect(basename(all_art_files), "_00_")]
  if (length(all_art_files) >= 2) {
    file_contents <- sapply(all_art_files, read_file, USE.NAMES = FALSE)
    removed_dupes <- 0L
    for (i in seq_along(all_art_files)) {
      if (is.na(file_contents[i])) next
      if (i < length(all_art_files)) {
        for (j in (i+1):length(all_art_files)) {
          if (is.na(file_contents[j])) next
          if (identical(file_contents[i], file_contents[j])) {
            file.remove(all_art_files[j])
            file_contents[j] <- NA
            cat(sprintf("    DEDUP: removed '%s' (identical to '%s')\n",
                        basename(all_art_files[j]), basename(all_art_files[i])))
            saved_count <- saved_count - 1L
            removed_dupes <- removed_dupes + 1L
          }
        }
      }
    }
    if (removed_dupes > 0) cat(sprintf("    Removed %d duplicate file(s)\n", removed_dupes))
  }

  # ----------------------------------------------------------
  # Step 13: Post-extraction Quality Control
  # ----------------------------------------------------------
  # Automatic QC on ALL months — detects and fixes:
  #   1. Consecutive articles with overlapping content → merge
  #   2. Articles truncated by poetry hard-cap → extend
  # ----------------------------------------------------------
  qc_files <- sort(list.files(out_folder,
    pattern = paste0("^", prefix, "_\\d+_.*\\.txt$"), full.names = TRUE))
  # Exclude _00_ (Misc/Board) files
  qc_files <- qc_files[!str_detect(basename(qc_files), "_00_")]

  if (length(qc_files) >= 2) {
    cat(sprintf("  QC Pass: scanning %d articles\n", length(qc_files)))
    qc_texts <- sapply(qc_files, read_file, USE.NAMES = FALSE)
    qc_merged <- 0L
    qc_extended <- 0L

    # Pre-compute whitespace-collapsed body (once per issue)
    body_collapsed <- str_replace_all(body_text, "\\s+", " ")

    # --- QC1: Merge consecutive articles with overlapping content ---
    # Content overlap: first 100 chars of next_text found in curr_text
    # (T2 PAGE_EST articles landing in same body region)
    i <- 1L
    while (i < length(qc_files)) {
      curr_text <- str_trim(qc_texts[i])
      next_text <- str_trim(qc_texts[i + 1])
      curr_len <- nchar(curr_text)
      next_len <- nchar(next_text)
      if (curr_len < 100 || next_len < 100) { i <- i + 1L; next }

      needs_merge <- FALSE
      merged_text <- ""

      # Content overlap detection
      probe <- str_sub(next_text, 1, 100)
      overlap_loc <- str_locate(curr_text, fixed(probe))

      if (!is.na(overlap_loc[1, 1])) {
        overlap_start <- overlap_loc[1, 1]
        next_tail <- str_sub(next_text, max(1, next_len - 99), next_len)
        tail_in_curr <- str_locate(curr_text, fixed(next_tail))

        if (!is.na(tail_in_curr[1, 1])) {
          merged_text <- curr_text
        } else {
          merged_text <- paste0(str_sub(curr_text, 1, overlap_start - 1L), next_text)
        }
        needs_merge <- TRUE
        cat(sprintf("    QC MERGE (overlap): '%s' + '%s'\n",
                    basename(qc_files[i]), basename(qc_files[i + 1])))
      }

      if (needs_merge) {
        write_file(merged_text, qc_files[i])
        file.remove(qc_files[i + 1])
        qc_texts[i] <- merged_text
        qc_texts <- qc_texts[-(i + 1)]
        qc_files <- qc_files[-(i + 1)]
        saved_count <- saved_count - 1L
        qc_merged <- qc_merged + 1L
      } else {
        i <- i + 1L
      }
    }

    # --- QC3: Extend articles truncated by poetry hard cap ---
    if (length(qc_files) >= 1) {
      qc_texts <- sapply(qc_files, read_file, USE.NAMES = FALSE)
      for (j in seq_along(qc_files)) {
        txt <- str_trim(qc_texts[j])
        txt_len <- nchar(txt)
        if (txt_len >= 1350 && txt_len <= 1650) {
          last_char <- str_sub(txt, txt_len, txt_len)
          if (str_detect(last_char, "[a-zA-Z,;:]")) {
            search_tail <- str_sub(txt, max(1, txt_len - 60), txt_len)
            tail_loc <- str_locate(body_text, fixed(search_tail))
            if (!is.na(tail_loc[1,1])) {
              extend_from <- tail_loc[1,2] + 1L
              extend_region <- str_sub(body_text, extend_from,
                                       min(body_end, extend_from + 2000L))
              sent_end <- str_locate(extend_region,
                regex("(?<=[.!?])\\s|(?<=[.!?])$|\n\\s*\n"))
              if (!is.na(sent_end[1,1]) && sent_end[1,1] <= 2000) {
                extension <- str_sub(extend_region, 1, sent_end[1,1])
                extended_text <- paste0(txt, extension)
                extended_text <- clean_vol50_headers(extended_text)
                extended_text <- clean_ocr_body(extended_text)
                extended_text <- str_trim(extended_text)
                write_file(extended_text, qc_files[j])
                cat(sprintf("    QC EXTEND: '%s' %d -> %d chars\n",
                            basename(qc_files[j]), txt_len, nchar(extended_text)))
                qc_extended <- qc_extended + 1L
              }
            }
          }
        }
      }
    }

    if (qc_merged + qc_extended > 0) {
      cat(sprintf("    QC Summary: %d merges, %d extended\n",
                  qc_merged, qc_extended))
    } else {
      cat("    QC: No issues detected\n")
    }
  }

  # Recalculate missed count after QC
  missed <- nrow(arts) - saved_count

  # ----------------------------------------------------------
  # Step 13b: Reassign uncaptured body text to nearest articles
  # ----------------------------------------------------------
  # Gap segments (body text not extracted into any article) are matched
  # to the nearest saved article by body-text position and appended or
  # prepended, so that text stays with the article it belongs to.
  # Only truly unmatched segments fall through to Misc.
  uncov_runs <- rle(!body_used[body_start:body_end])
  gap_texts <- character(0)
  gap_positions <- list()
  gap_pos <- body_start
  for (gi in seq_along(uncov_runs$lengths)) {
    run_len <- uncov_runs$lengths[gi]
    if (uncov_runs$values[gi] && run_len >= 80L) {
      gt <- str_trim(str_sub(body_text, gap_pos, gap_pos + run_len - 1L))
      gt_clean <- str_replace_all(gt, "\\s+", " ")
      if (nchar(gt_clean) >= 50L) {
        gap_texts <- c(gap_texts, gt)
        gap_positions[[length(gap_positions) + 1L]] <- c(gap_pos, gap_pos + run_len - 1L)
      }
    }
    gap_pos <- gap_pos + run_len
  }

  if (length(gap_texts) > 0 && length(saved_ranges) > 0) {
    # Filter out stale entries (files removed by dedup/QC)
    saved_ranges <- Filter(function(x) file.exists(x$file), saved_ranges)

    if (length(saved_ranges) > 0) {
      sr_df <- do.call(rbind, lapply(saved_ranges, function(x) {
        data.frame(file = x$file, start = x$start, end = x$end,
                   stringsAsFactors = FALSE)
      }))
      sr_df <- sr_df[order(sr_df$start), ]

      reassigned <- 0L
      reassigned_chars <- 0L
      misc_texts <- character(0)

      for (gi in seq_along(gap_texts)) {
        gap_s <- gap_positions[[gi]][1]
        gap_e <- gap_positions[[gi]][2]
        gt <- gap_texts[gi]

        # Find preceding article: largest sr_df$end that is < gap_s
        prec_idx <- which(sr_df$end < gap_s)
        prec_dist <- Inf
        best_prec <- NA_integer_
        if (length(prec_idx) > 0) {
          best_prec <- prec_idx[which.max(sr_df$end[prec_idx])]
          prec_dist <- gap_s - sr_df$end[best_prec]
        }

        # Find following article: smallest sr_df$start that is > gap_e
        foll_idx <- which(sr_df$start > gap_e)
        foll_dist <- Inf
        best_foll <- NA_integer_
        if (length(foll_idx) > 0) {
          best_foll <- foll_idx[which.min(sr_df$start[foll_idx])]
          foll_dist <- sr_df$start[best_foll] - gap_e
        }

        # Match to nearest article within 5000 chars
        matched_file <- NULL
        append_mode <- "after"
        if (prec_dist <= foll_dist && prec_dist <= 5000L) {
          matched_file <- sr_df$file[best_prec]
          append_mode <- "after"
        } else if (foll_dist < prec_dist && foll_dist <= 5000L) {
          matched_file <- sr_df$file[best_foll]
          append_mode <- "before"
        }

        if (!is.null(matched_file) && file.exists(matched_file)) {
          existing <- read_file(matched_file)
          gt_clean <- clean_vol50_headers(gt)
          gt_clean <- clean_ocr_body(gt_clean)
          gt_clean <- str_trim(gt_clean)

          # Ad-trim gap text: remove trailing advertisements
          gap_ad <- detect_ad_start(gt_clean, min_article_len = 50L)
          if (!is.na(gap_ad) && gap_ad > 50L) {
            gt_clean <- str_trim(str_sub(gt_clean, 1L, gap_ad - 1L))
          }

          # Size cap: don't append gaps that are disproportionately large.
          # Poems (detected by "_Poem_" in filename) get a strict cap
          # since they're short and shouldn't absorb adjacent articles.
          # All other articles get a lenient cap.
          existing_len <- nchar(existing)
          gap_len <- nchar(gt_clean)
          bn <- basename(matched_file)
          is_poem_file <- str_detect(bn, "_Poem_")
          is_bday_file <- str_detect(bn, "_Bday_|_Birthday_")
          if (is_poem_file || is_bday_file) {
            size_cap <- max(existing_len * 0.3, 500L)
          } else {
            size_cap <- max(existing_len * 1.1, 5000L)
          }
          too_large <- (gap_len > size_cap)

          # Poem/birthday bounce: if gap is too large for a poem neighbor,
          # try the opposite direction (a non-poem article can absorb more)
          if (too_large && (is_poem_file || is_bday_file)) {
            alt_file <- NULL
            alt_mode <- "after"
            if (append_mode == "after" && !is.na(best_foll) && foll_dist <= 5000L) {
              alt_file <- sr_df$file[best_foll]
              alt_mode <- "before"
            } else if (append_mode == "before" && !is.na(best_prec) && prec_dist <= 5000L) {
              alt_file <- sr_df$file[best_prec]
              alt_mode <- "after"
            }
            if (!is.null(alt_file) && file.exists(alt_file)) {
              alt_bn <- basename(alt_file)
              if (!str_detect(alt_bn, "_Poem_|_Bday_|_Birthday_")) {
                alt_existing <- read_file(alt_file)
                alt_cap <- max(nchar(alt_existing) * 1.1, 5000L)
                if (gap_len <= alt_cap) {
                  matched_file <- alt_file
                  append_mode <- alt_mode
                  existing <- alt_existing
                  existing_len <- nchar(alt_existing)
                  bn <- alt_bn
                  is_poem_file <- FALSE
                  is_bday_file <- FALSE
                  too_large <- FALSE
                  cat(sprintf("    GAP POEM-BOUNCE: redirected %s chars from poem to %s\n",
                              format(gap_len, big.mark = ","), basename(alt_file)))
                }
              }
            }
          }

          # Check if gap is mostly ad content — send to Misc instead of appending
          gap_is_ad <- FALSE
          if (nchar(gt_clean) >= 100L) {
            gap_ad_check <- str_sub(gt_clean, 1L, min(nchar(gt_clean), 500L))
            gap_ad_score <- str_count(gap_ad_check, "\\$\\d+\\.\\d{2}") * 3L +
              str_count(gap_ad_check, "\\d{3}[-.]?\\d{4}") * 3L +
              str_count(gap_ad_check, "Utah\\s+\\d{5}|[A-Z][a-z]+,\\s+[A-Z]{2}\\s+\\d{5}") * 2L +
              str_count(gap_ad_check, "(?i)(?:Write|Send)\\s+(?:for|to)") * 2L +
              str_count(gap_ad_check, "\\bFREE\\b") * 2L +
              str_count(gap_ad_check, "\\bTOUR\\b|\\bHAWAII\\b") * 2L +
              str_count(gap_ad_check, "\\bBeneficial Life\\b|\\bDeseret Book\\b|\\bZim's\\b") * 3L +
              str_count(gap_ad_check, "(?i)\\bpostpaid|postage\\b") * 2L
            # Scale threshold for large gaps: legitimate articles can contain
            # prices, zip codes, and addresses that inflate the score.
            # Ad scoring only checks the first 500 chars, so large gaps
            # with a few ad-like patterns early on get false positives.
            ad_threshold <- 7L + as.integer(nchar(gt_clean) / 3000L) * 3L
            if (gap_ad_score >= ad_threshold) gap_is_ad <- TRUE
          }

          if (gap_is_ad) {
            cat(sprintf("    GAP AD → Misc: %s chars (ad score %d)\n",
                        format(nchar(gt_clean), big.mark = ","), gap_ad_score))
            misc_texts <- c(misc_texts, gt)
          } else if (nchar(gt_clean) >= 20L && !too_large) {
            if (append_mode == "after") {
              updated <- paste0(str_trim(existing), "\n\n", gt_clean)
            } else {
              updated <- paste0(gt_clean, "\n\n", str_trim(existing))
            }
            write_file(str_trim(updated), matched_file)
            reassigned <- reassigned + 1L
            reassigned_chars <- reassigned_chars + nchar(gt_clean)
            cat(sprintf("    GAP REASSIGN (%s): %s chars → %s\n",
                        append_mode, format(nchar(gt_clean), big.mark = ","),
                        basename(matched_file)))
          } else if (too_large) {
            cat(sprintf("    GAP TOO LARGE: %s chars (article=%s) → Misc\n",
                        format(gap_len, big.mark = ","),
                        format(existing_len, big.mark = ",")))
            misc_texts <- c(misc_texts, gt)
          } else {
            # Cleaned text too short — skip
          }
        } else {
          misc_texts <- c(misc_texts, gt)
        }
      }

      cat(sprintf("  GAP SUMMARY: %d/%d segments (%s chars) reassigned to articles\n",
                  reassigned, length(gap_texts),
                  format(reassigned_chars, big.mark = ",")))

      # ----------------------------------------------------------
      # Step 13b-T3: Recovery pass for large gaps
      # ----------------------------------------------------------
      # For any gap > 15K chars, search for CONTENTS titles within the
      # gap text and split it into article-sized chunks. This recovers
      # articles that were missed because T1/T2 extracted too little.
      #
      # ENHANCED: Also searches for articles that ARE saved but with
      # suspiciously small extractions (< 1000 chars for non-poems,
      # < 200 for poems). When a better extraction is found in the gap,
      # it REPLACES the existing small file.
      if (length(misc_texts) > 0) {
        t3_recovered <- character(0)  # indices of misc_texts entries that were split
        for (mti in seq_along(misc_texts)) {
          mt <- misc_texts[[mti]]
          mt_len <- nchar(mt)
          if (mt_len < 10000L) next  # only process large gaps

          cat(sprintf("  T3 GAP RECOVERY: Processing %s-char gap for CONTENTS titles\n",
                      format(mt_len, big.mark = ",")))

          # Identify poorly-saved articles (small files that might have been
          # extracted from wrong positions). These are candidates for re-extraction.
          small_saved <- integer(0)
          for (ai in seq_len(nrow(arts))) {
            if (!saved_flags[ai]) next  # not saved at all — handled below
            # Check file size
            is_poem_cat <- isTRUE(arts$category[ai] == "Poetry")
            fname <- saved_files[ai]
            if (is.na(fname) || !nzchar(fname) || !file.exists(fname)) next
            fsize <- file.info(fname)$size
            if (is.na(fsize)) next
            size_thresh <- if (is_poem_cat) 200L else 3000L
            if (fsize < size_thresh) {
              small_saved <- c(small_saved, ai)
            }
          }

          # Search for ALL titles: unsaved ones + poorly-saved ones
          gap_title_hits <- data.frame(idx = integer(0), position = integer(0),
                                        is_replace = logical(0),
                                        stringsAsFactors = FALSE)
          for (ai in seq_len(nrow(arts))) {
            is_candidate <- !saved_flags[ai] || (ai %in% small_saved)
            if (!is_candidate) next
            # Try conservative title search within the gap
            hit_pos <- find_in_window(mt, arts$title[ai], 1L, mt_len, allow_s10 = FALSE)
            if (is.na(hit_pos)) {
              # Try aggressive search as fallback for large gaps
              hit_pos <- find_in_window(mt, arts$title[ai], 1L, mt_len, allow_s10 = TRUE)
            }
            if (!is.na(hit_pos)) {
              gap_title_hits <- rbind(gap_title_hits,
                                     data.frame(idx = ai, position = hit_pos,
                                                is_replace = saved_flags[ai]))
            }
          }

          if (nrow(gap_title_hits) == 0) {
            cat("    T3: No recoverable CONTENTS titles found in gap\n")
            next
          }

          gap_title_hits <- gap_title_hits[order(gap_title_hits$position), ]
          # Remove duplicates (same position)
          gap_title_hits <- gap_title_hits[!duplicated(gap_title_hits$position), ]
          n_new <- sum(!gap_title_hits$is_replace)
          n_repl <- sum(gap_title_hits$is_replace)
          cat(sprintf("    T3: Found %d titles in gap (%d new, %d replacements for small files)\n",
                      nrow(gap_title_hits), n_new, n_repl))

          # Extract articles from the gap text using title positions
          t3_any_saved <- FALSE
          for (ti in seq_len(nrow(gap_title_hits))) {
            ai <- gap_title_hits$idx[ti]
            t3_start <- gap_title_hits$position[ti]
            # End at next title position or gap end
            if (ti < nrow(gap_title_hits)) {
              t3_end <- gap_title_hits$position[ti + 1] - 1L
            } else {
              t3_end <- mt_len
            }
            t3_text <- str_sub(mt, t3_start, t3_end)

            # Basic validation: at least 50 chars, contains some alpha text
            t3_text_clean <- str_trim(t3_text)
            if (nchar(t3_text_clean) < 50L) next
            alpha_ratio <- str_count(t3_text_clean, "[A-Za-z]") / max(1L, nchar(t3_text_clean))
            if (alpha_ratio < 0.4) next

            # Clean and save
            t3_text_clean <- clean_vol50_headers(t3_text_clean)
            t3_text_clean <- clean_ocr_body(t3_text_clean)
            t3_text_clean <- str_trim(t3_text_clean)

            # Ad-trim
            t3_ad <- detect_ad_start(t3_text_clean, min_article_len = 100L)
            if (!is.na(t3_ad) && t3_ad > 100L) {
              t3_text_clean <- str_trim(str_sub(t3_text_clean, 1L, t3_ad - 1L))
            }

            if (nchar(t3_text_clean) >= 50L) {
              # For replacements: decide REPLACE vs APPEND based on original size
              if (gap_title_hits$is_replace[ti]) {
                old_file <- saved_files[ai]
                old_size <- if (!is.na(old_file) && file.exists(old_file))
                              file.info(old_file)$size else 0L
                if (nchar(t3_text_clean) > old_size) {
                  if (old_size < 500L) {
                    # Tiny original (likely noise) — full replace
                    if (!is.na(old_file) && file.exists(old_file)) file.remove(old_file)
                    saved_flags[ai] <- FALSE  # mark unsaved so save_article works
                    save_article(ai, t3_text_clean, "T3-REPLACE", NA_integer_, NA_integer_)
                    cat(sprintf("    T3 REPLACE: '%s' (%d -> %d chars)\n",
                                str_sub(arts$title[ai], 1, 40), old_size, nchar(t3_text_clean)))
                  } else {
                    # Significant original — append gap content
                    existing_text <- read_file(old_file)
                    combined <- paste0(str_trim(existing_text), "\n\n", str_trim(t3_text_clean))
                    write_file(str_trim(combined), old_file)
                    cat(sprintf("    T3 APPEND: '%s' (%d + %d = %d chars)\n",
                                str_sub(arts$title[ai], 1, 40), old_size,
                                nchar(t3_text_clean), nchar(combined)))
                  }
                  t3_any_saved <- TRUE
                }
              } else {
                save_article(ai, t3_text_clean, "T3", NA_integer_, NA_integer_)
                t3_any_saved <- TRUE
              }
            }
          }

          if (t3_any_saved) {
            t3_recovered <- c(t3_recovered, mti)
          }
        }

        # Remove recovered gaps from misc_texts
        if (length(t3_recovered) > 0) {
          misc_texts <- misc_texts[-as.integer(t3_recovered)]
          cat(sprintf("  T3 RECOVERY: Processed %d large gaps, %d remain for Misc\n",
                      length(t3_recovered), length(misc_texts)))
        }
      }

      # Any remaining unmatched gaps go to Misc
      if (length(misc_texts) > 0) {
        total_misc_chars <- sum(nchar(misc_texts))
        misc_file <- file.path(out_folder, paste0(prefix, "_00_Misc.txt"))
        existing_misc <- if (file.exists(misc_file)) read_file(misc_file) else ""
        uncaptured_block <- paste(misc_texts, collapse = "\n\n---\n\n")
        combined_misc <- paste0(
          str_trim(existing_misc),
          "\n\n=== UNCAPTURED BODY TEXT (",
          length(misc_texts), " segments, ",
          format(total_misc_chars, big.mark = ","), " chars) ===\n\n",
          uncaptured_block
        )
        write_file(str_trim(combined_misc), misc_file)
        cat(sprintf("  MISC: %d unmatched segments (%s chars) → Misc file\n",
                    length(misc_texts), format(total_misc_chars, big.mark = ",")))
      }
    } else {
      # All saved_ranges files removed — fall back to Misc
      total_gap_chars <- sum(nchar(gap_texts))
      misc_file <- file.path(out_folder, paste0(prefix, "_00_Misc.txt"))
      existing_misc <- if (file.exists(misc_file)) read_file(misc_file) else ""
      uncaptured_block <- paste(gap_texts, collapse = "\n\n---\n\n")
      combined_misc <- paste0(
        str_trim(existing_misc),
        "\n\n=== UNCAPTURED BODY TEXT (",
        length(gap_texts), " segments, ",
        format(total_gap_chars, big.mark = ","), " chars) ===\n\n",
        uncaptured_block
      )
      write_file(str_trim(combined_misc), misc_file)
      cat(sprintf("  MISC: Appended %d uncaptured segments (%s chars) to Misc file\n",
                  length(gap_texts), format(total_gap_chars, big.mark = ",")))
    }
  } else if (length(gap_texts) > 0) {
    # No saved_ranges — fall back to Misc
    total_gap_chars <- sum(nchar(gap_texts))
    misc_file <- file.path(out_folder, paste0(prefix, "_00_Misc.txt"))
    existing_misc <- if (file.exists(misc_file)) read_file(misc_file) else ""
    uncaptured_block <- paste(gap_texts, collapse = "\n\n---\n\n")
    combined_misc <- paste0(
      str_trim(existing_misc),
      "\n\n=== UNCAPTURED BODY TEXT (",
      length(gap_texts), " segments, ",
      format(total_gap_chars, big.mark = ","), " chars) ===\n\n",
      uncaptured_block
    )
    write_file(str_trim(combined_misc), misc_file)
    cat(sprintf("  MISC: Appended %d uncaptured segments (%s chars) to Misc file\n",
                length(gap_texts), format(total_gap_chars, big.mark = ",")))
  } else {
    cat("  MISC: Full body text coverage — no uncaptured text\n")
  }

  # ----------------------------------------------------------
  # Step 13b-extra: Append collected ad text to Misc file
  # ----------------------------------------------------------
  if (length(misc_ad_text) > 0) {
    ad_block <- paste(misc_ad_text, collapse = "\n\n")
    misc_file <- file.path(out_folder, paste0(prefix, "_00_Misc.txt"))
    existing_misc <- if (file.exists(misc_file)) read_file(misc_file) else ""
    combined <- paste0(
      str_trim(existing_misc),
      "\n\n=== ADVERTISEMENTS (",
      length(misc_ad_text), " segments, ",
      format(nchar(ad_block), big.mark = ","), " chars) ===\n\n",
      ad_block
    )
    write_file(str_trim(combined), misc_file)
    cat(sprintf("  ADS: Appended %d ad segments (%s chars) to Misc file\n",
                length(misc_ad_text), format(nchar(ad_block), big.mark = ",")))
  }

  # ----------------------------------------------------------
  # Step 13c: Follow "continued on page X" after gap reassignment
  # ----------------------------------------------------------
  # Gap reassignment (Step 13b) may have appended text with continuation
  # markers to article files. Re-scan all saved files for "continued on
  # page X" and follow them. This is the same logic as Step 9b but runs
  # after gap reassignment so it catches markers in gap-appended text
  # (e.g., "Suffer the Little Children" gets its "(continued on page 694)"
  # via gap reassignment, and this step follows to that page).
  saved_ranges_13c <- Filter(function(x) file.exists(x$file), saved_ranges)
  cont_found_13c <- 0L
  cat(sprintf("  Step 13c: Checking %d saved files for continuation markers\n",
              length(saved_ranges_13c)))
  if (length(saved_ranges_13c) > 0) {
    for (sr_i in seq_along(saved_ranges_13c)) {
      sr <- saved_ranges_13c[[sr_i]]
      if (!file.exists(sr$file)) next
      existing <- read_file(sr$file)
      cont_m <- str_match(existing,
        regex("\\(continued on page (\\d+)\\)", ignore_case = TRUE))
      if (is.na(cont_m[1,1])) next

      cont_page <- as.integer(cont_m[1, 2])
      cont_pos <- page_to_pos(page_index, cont_page, body_start, body_end)
      cat(sprintf("    13c CONT_CHECK: '%s' -> pg %d -> pos %d\n",
                  basename(sr$file), cont_page, cont_pos))
      if (is.na(cont_pos) || cont_pos <= body_start) {
        cat("      13c SKIP: cont_pos invalid\n")
        next
      }

      # Don't follow if continuation lands in already-used text
      check_start <- min(cont_pos, body_end)
      check_end   <- min(cont_pos + 100L, body_end)
      used_pct <- if (check_end > check_start) mean(body_used[check_start:check_end]) else 1.0
      cat(sprintf("      13c body_used[%d:%d] = %.0f%% used\n",
                  check_start, check_end, used_pct * 100))
      if (check_end > check_start && all(body_used[check_start:check_end])) {
        cat("      13c SKIP: continuation area already used\n")
        next
      }

      # Find end of continuation: next found article position or page boundary
      next_art_pos <- body_end
      if (length(title_idx) > 0) {
        later <- positions[title_idx][positions[title_idx] > cont_pos + 200L]
        if (length(later) > 0) next_art_pos <- min(later)
      }
      cont_end <- min(next_art_pos, cont_pos + 5000L, body_end)
      if (cont_end <= cont_pos + 50L) next

      cont_text <- str_sub(body_text, cont_pos, cont_end - 1L)
      cont_text <- clean_vol50_headers(cont_text)
      cont_text <- clean_ocr_body(cont_text)
      cont_text <- str_trim(cont_text)

      # Trim trailing ads from continuation
      cont_ad <- detect_ad_start(cont_text, min_article_len = 50L)
      if (!is.na(cont_ad) && cont_ad > 50L) {
        cont_text <- str_trim(str_sub(cont_text, 1L, cont_ad - 1L))
      }

      if (nchar(cont_text) > 50L) {
        # Check text isn't already in the file (avoid double-appending)
        probe_13c <- str_sub(cont_text, 1L, min(80L, nchar(cont_text)))
        if (!str_detect(existing, fixed(probe_13c))) {
          write_file(paste0(existing, "\n\n", cont_text), sr$file)
          mark_used(cont_pos, cont_end - 1L)
          record_saved(sr$file, cont_pos, cont_end - 1L)
          cont_found_13c <- cont_found_13c + 1L
          cat(sprintf("  Step 13c CONTINUATION: '%s' +%s chars from pg %d\n",
                      basename(sr$file), format(nchar(cont_text), big.mark = ","),
                      cont_page))
        }
      }
    }
  }
  if (cont_found_13c > 0) {
    cat(sprintf("  Step 13c: %d continuation(s) appended\n", cont_found_13c))
  }

  # ----------------------------------------------------------
  # Step 14a: Renumber output files by page order
  # ----------------------------------------------------------
  # Article files are numbered in save order (T1 first, then T2,
  # then T3), which doesn't match magazine page order.  Renumber
  # so that _01_ is the first article by page, _02_ the second, etc.
  rename_idx <- which(!is.na(saved_files) & file.exists(saved_files))
  if (length(rename_idx) > 1) {
    rename_df <- data.frame(
      idx  = rename_idx,
      file = saved_files[rename_idx],
      page = arts$page[rename_idx],
      stringsAsFactors = FALSE
    )
    rename_df <- rename_df[order(rename_df$page, rename_df$idx), ]

    # Build new filenames by replacing the two-digit article number
    rename_df$new_file <- character(nrow(rename_df))
    art_num_pat <- paste0("^", prefix, "_\\d{2}_")
    for (r in seq_len(nrow(rename_df))) {
      new_bn <- sub(art_num_pat,
                    paste0(prefix, "_", sprintf("%02d", r), "_"),
                    basename(rename_df$file[r]))
      rename_df$new_file[r] <- file.path(out_folder, new_bn)
    }

    needs_rename <- rename_df$file != rename_df$new_file
    if (any(needs_rename)) {
      # Two-pass rename to avoid filename collisions
      rename_df$tmp_file <- file.path(out_folder,
                                       paste0("__tmp_", basename(rename_df$file)))
      for (r in seq_len(nrow(rename_df))) {
        file.rename(rename_df$file[r], rename_df$tmp_file[r])
      }
      for (r in seq_len(nrow(rename_df))) {
        file.rename(rename_df$tmp_file[r], rename_df$new_file[r])
      }
      # Update saved_files to reflect new names
      for (r in seq_len(nrow(rename_df))) {
        saved_files[rename_df$idx[r]] <- rename_df$new_file[r]
      }
      cat(sprintf("  Step 14a: Renumbered %d files by page order\n",
                  sum(needs_rename)))
    }
  }

  # ----------------------------------------------------------
  # Step 14b: Summary
  # ----------------------------------------------------------
  data.frame(
    file          = filename,
    n_contents    = nrow(arts),
    n_saved       = saved_count,
    n_missed      = missed,
    total_chars   = nchar(text),
    stringsAsFactors = FALSE
  )
}


## ----main-loop----------------------------------------------------------------
# ============================================================
# MAIN LOOP — Process all monthly files
# ============================================================

input_files <- sort(list.files(INPUT_DIR, pattern="\\.txt$", full.names=TRUE))

# Apply month filter if set
if (exists("MONTHS_FILTER") && !is.null(MONTHS_FILTER)) {
  month_pat <- paste0("(?i)(", paste(MONTHS_FILTER, collapse="|"), ")")
  input_files <- input_files[str_detect(basename(input_files), month_pat)]
  cat(sprintf("MONTHS_FILTER active: processing %s only\n", paste(MONTHS_FILTER, collapse=", ")))
}

cat(sprintf("Found %d input files in %s\n\n", length(input_files), INPUT_DIR))

results <- list()
for (f in input_files) {
  res <- process_vol50_issue(f, OUTPUT_DIR)
  results[[length(results)+1]] <- res
}

# Summary table
summary_df <- do.call(rbind, results)
cat("\n\n")
cat(rep("=",70), "\n", sep="")
cat("SUMMARY\n")
cat(rep("=",70), "\n", sep="")
cat(sprintf("Total files: %d\n", nrow(summary_df)))
cat(sprintf("Total CONTENTS entries: %d\n", sum(summary_df$n_contents)))
cat(sprintf("Total saved: %d\n", sum(summary_df$n_saved)))
cat(sprintf("Total missed: %d\n", sum(summary_df$n_missed)))
if (sum(summary_df$n_contents) > 0) {
  hit_rate <- sum(summary_df$n_saved) / sum(summary_df$n_contents) * 100
  cat(sprintf("Hit rate: %.1f%%\n", hit_rate))
}
cat("\n")
print(kable(summary_df, format="pipe"))

