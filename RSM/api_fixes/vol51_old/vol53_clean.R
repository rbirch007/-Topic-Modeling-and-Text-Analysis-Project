## ----setup, message=FALSE, warning=FALSE--------------------------------------
for (pkg in c("stringr","readr","knitr","kableExtra")) {
  if (!requireNamespace(pkg, quietly=TRUE)) install.packages(pkg)
}
library(stringr); library(readr); library(knitr); library(kableExtra)

# ============================================================
# CONFIGURATION — Template for Vol50-57 (1963-1970)
# Title-search-first architecture: find each article's title
# directly in body text, sort by position, extract between
# consecutive found positions.
# Update VOLUME, YEAR, and paths for each volume.
# ============================================================
VOLUME  <- 53L
YEAR    <- 1966L
INPUT_DIR  <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/input/Vol53split"
OUTPUT_DIR <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/output"

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
  # Strip any remaining non-ASCII characters (C1 control chars like \x97
  # from OCR, stray diacritics, etc.) that break file system tools.
  clean <- str_replace_all(clean, "[^\\x20-\\x7E]", "")
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
    "The Home" = "Home",
    cat_label)
}

# Helper: append ad content to Misc file (ad preservation)
append_ads_to_misc <- function(out_folder, prefix, ad_content, source_label) {
  ad_content <- str_trim(ad_content)
  if (nchar(ad_content) < 10L) return(invisible(NULL))
  misc_file <- file.path(out_folder, paste0(prefix, "_00_Misc.txt"))
  ad_header <- sprintf("\n\n=== ADVERTISEMENTS (from %s) ===\n", source_label)
  if (file.exists(misc_file)) {
    existing <- read_file(misc_file)
    write_file(paste0(existing, ad_header, ad_content), misc_file)
  } else {
    write_file(paste0(ad_header, ad_content), misc_file)
  }
  cat(sprintf("    AD → Misc: %d chars from %s\n", nchar(ad_content), source_label))
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
          position=m[i,1], stringsAsFactors=FALSE))
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
          position=m_even[i,1], stringsAsFactors=FALSE))
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
    # Also check first/last entries against median to catch false positives
    if (sum(keep) > 3) {
      med_page <- median(pages$page[keep])
      for (i in seq_len(nrow(pages))) {
        if (keep[i] && abs(pages$page[i] - med_page) > 150) keep[i] <- FALSE
      }
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

pos_to_page <- function(pi, pos) {
  if (nrow(pi) == 0) return(NA_real_)
  # Exact match
  ex <- pi[pi$position == pos, ]
  if (nrow(ex) > 0) return(ex$page[1])
  # Interpolate between bracketing page markers
  bef <- pi[pi$position <= pos, ]
  aft <- pi[pi$position > pos, ]
  CPP <- 2400L
  if (nrow(bef) > 0 && nrow(aft) > 0) {
    b <- bef[nrow(bef), ]; a <- aft[1, ]
    frac <- (pos - b$position) / (a$position - b$position)
    return(b$page + frac * (a$page - b$page))
  }
  if (nrow(bef) > 0) return(bef$page[nrow(bef)] + (pos - bef$position[nrow(bef)]) / CPP)
  if (nrow(aft) > 0) return(aft$page[1] - (aft$position[1] - pos) / CPP)
  NA_real_
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
  earliest <- nchar(bday_text) + 1L
  for (ap in ad_pats) {
    m <- str_locate(bday_text, regex(ap, ignore_case = FALSE))
    if (!is.na(m[1,1]) && m[1,1] > 200L && m[1,1] < earliest) {
      earliest <- m[1,1]
    }
  }
  if (earliest <= nchar(bday_text)) {
    ad_content <- str_sub(bday_text, earliest)
    # Walk back to last complete name entry (ends with state/province)
    pre <- str_sub(bday_text, 1L, earliest - 1L)
    # Find last line break or Mrs. entry
    last_nl <- max(c(0L, str_locate_all(pre, "\n")[[1]][,1]))
    if (last_nl > 200L) {
      bday_text <- str_trim(str_sub(bday_text, 1L, last_nl - 1L))
    } else {
      bday_text <- str_trim(str_sub(bday_text, 1L, earliest - 1L))
    }
    return(list(clean = bday_text, ads = ad_content))
  }
  list(clean = bday_text, ads = "")
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

read_contents_file <- function(filepath) {
  arts <- data.frame(title=character(), page=integer(), category=character(),
                     stringsAsFactors=FALSE)
  if (!file.exists(filepath)) return(arts)

  raw <- read_file(filepath)
  file_lines <- str_split(raw, "\n")[[1]]
  file_lines <- str_trim(file_lines)
  file_lines <- file_lines[nchar(file_lines) > 0]
  if (length(file_lines) == 0) return(arts)

  # Pre-process: split long lines containing inline category headers.
  # Some OCR CONTENTS files jam multiple category sections onto one line.
  rc_cat_pat <- "(?:SPECIAL FEATURES?|FICTION|GENERAL FEATURES?|THE HOME\\s*[-—:].{0,30}|LESSON\\s*DEPARTMENT|POETRY|FEATURES?\\s+FOR\\s+THE\\s+HOME|FEATURE.{0,5}HOME|LESSONS?\\s+FOR\\s+\\w+)"
  rc_split_at_pages <- function(seg) {
    pg_locs <- str_locate_all(seg, "(?<=\\s)(\\d{2,3})\\s+[A-Z]")[[1]]
    if (nrow(pg_locs) == 0) return(seg)
    result <- character(0)
    # Text before first page number (may be a category header)
    if (pg_locs[1, 1] > 2) {
      pre <- str_trim(str_sub(seg, 1, pg_locs[1, 1] - 1L))
      if (nchar(pre) >= 3) result <- c(result, pre)
    }
    # Each page-number entry
    for (pi in seq_len(nrow(pg_locs))) {
      s <- pg_locs[pi, 1]
      e <- if (pi < nrow(pg_locs)) pg_locs[pi + 1, 1] - 1L else nchar(seg)
      part <- str_trim(str_sub(seg, s, e))
      if (nchar(part) >= 3) result <- c(result, part)
    }
    if (length(result) == 0) return(seg)
    result
  }

  expanded <- character(0)
  for (fl in file_lines) {
    if (nchar(fl) > 100) {
      ic <- str_locate_all(fl, regex(rc_cat_pat, ignore_case = TRUE))[[1]]
      if (nrow(ic) > 0) {
        mid <- ic[ic[, 1] > 1, , drop = FALSE]
        if (nrow(mid) > 0) {
          prev <- 1L
          for (ci in seq_len(nrow(mid))) {
            bef <- str_trim(str_sub(fl, prev, mid[ci, 1] - 1L))
            if (nchar(bef) >= 3) expanded <- c(expanded, rc_split_at_pages(bef))
            prev <- mid[ci, 1]
          }
          tail_t <- str_trim(str_sub(fl, prev, nchar(fl)))
          if (nchar(tail_t) >= 3) expanded <- c(expanded, rc_split_at_pages(tail_t))
          cat(sprintf("    RC_INLINE_SPLIT: split %d-char line into %d parts\n",
                      nchar(fl), length(expanded)))
          next
        }
      }
      expanded <- c(expanded, rc_split_at_pages(fl))
      next
    }
    expanded <- c(expanded, fl)
  }
  file_lines <- expanded

  # Second pass: split lines where a category header at position 1 is followed
  # by article text on the same line.  This handles CONTENTS formats where the
  # category name and first article title share a line, e.g.:
  #   "SPECIAL FEATURES New Year's Greeting General Presidency"
  # becomes two lines:
  #   "SPECIAL FEATURES"
  #   "New Year's Greeting General Presidency"
  # The standalone category regex then detects the header correctly.
  expanded2 <- character(0)
  for (fl in file_lines) {
    # Skip short lines and lines starting with digits (numbered article entries)
    if (nchar(fl) < 10 || str_detect(fl, "^\\d")) {
      expanded2 <- c(expanded2, fl)
      next
    }
    # Match category header at start of line followed by non-digit text.
    # Use a RESTRICTIVE pattern here (not rc_cat_pat) to avoid false
    # positives from article titles like "The Home — Second Prize Story".
    rc_cat_start <- "(?:SPECIAL FEATURES?|FICTION|GENERAL FEATURES?|FEATURES?\\s+FOR\\s+THE\\s+HOME|LESSONS?\\s+(?:FOR\\s+\\w+|DEPARTMENT)|POETRY)"
    cat_split <- str_match(fl, regex(
      paste0("^(", rc_cat_start, ")\\s+(\\D.{4,})"),
      ignore_case = TRUE))
    if (!is.na(cat_split[1,1]) && nchar(fl) < 120) {
      remainder <- str_trim(cat_split[1,3])
      # Don't split masthead lines (contain Editor/Magazine/Publisher keywords)
      if (!str_detect(remainder, regex("Editor|Magazine|Publisher|Associate|Manager",
                                        ignore_case = TRUE))) {
        cat_header <- str_trim(cat_split[1,2])
        expanded2 <- c(expanded2, cat_header, remainder)
        cat(sprintf("    RC_CAT_SPLIT: '%s' -> cat='%s' + entry='%s'\n",
                    str_sub(fl, 1, 60), cat_header, str_sub(remainder, 1, 40)))
        next
      }
    }
    expanded2 <- c(expanded2, fl)
  }
  file_lines <- expanded2

  current_cat <- "Article"
  in_poetry <- FALSE

  # Pre-merge continuation lines: when a line has no trailing page number
  # and the next line is "Author page_num", combine them into one entry.
  merged_fl <- character(0)
  fi <- 1L
  while (fi <= length(file_lines)) {
    fl <- file_lines[fi]
    has_trailing_pg <- str_detect(fl, "\\d{1,3}\\s*\\.?\\s*$")
    is_cat_hdr <- str_detect(fl, regex(
      "^(Special Features?|Fiction|General Features?|The Home|Lessons?|Lesson\\s+Department|Poetry)(?:\\s+\\d{1,3})?\\s*$",
      ignore_case=TRUE))
    is_cover <- str_detect(fl, regex("^(The Cover|Frontispiece|Art Layout)", ignore_case=TRUE))
    if (!has_trailing_pg && !is_cat_hdr && !is_cover && nchar(fl) >= 5 && fi < length(file_lines)) {
      nxt <- file_lines[fi + 1L]
      # Check if next line is "Author page_num" (2-4 capitalized words + number)
      auth_pg <- str_match(nxt, "^([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){0,3})\\s+(\\d{1,3})\\s*$")
      nxt_is_cat <- str_detect(nxt, regex(
          "^(Special Features?|Fiction|General Features?|The Home|Lessons?|Lesson\\s+Department|Poetry)(?:\\s+\\d{1,3})?\\s*$",
          ignore_case=TRUE))
      if (!is.na(auth_pg[1,1]) && !nxt_is_cat) {
        merged_fl <- c(merged_fl, paste(fl, nxt))
        cat(sprintf("    RC_MERGE: '%s' + '%s'\n", str_sub(fl, 1, 40), str_sub(nxt, 1, 30)))
        fi <- fi + 2L
        next
      }
      # Also merge: "Title" + "Author" (no page number, 2-4 capitalized words only)
      auth_only <- str_match(nxt, "^([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){1,3})\\s*$")
      if (!has_trailing_pg && is.na(auth_pg[1,1]) && !is.na(auth_only[1,1]) && !nxt_is_cat) {
        merged_fl <- c(merged_fl, paste(fl, nxt))
        cat(sprintf("    RC_MERGE_AUTH: '%s' + '%s'\n", str_sub(fl, 1, 40), str_sub(nxt, 1, 30)))
        fi <- fi + 2L
        next
      }
    }
    merged_fl <- c(merged_fl, fl)
    fi <- fi + 1L
  }
  file_lines <- merged_fl

  for (line in file_lines) {
    # Parse Frontispiece line into entry; skip other cover/art credits
    if (str_detect(line, regex("^Frontispiece:", ignore_case=TRUE))) {
      fp_m <- str_match(line, regex(
        "^Frontispiece:\\s*(.+?)(?:,\\s*by\\s+|\\s+by\\s+)(.+?)(?:,\\s*lithograph.*)?$",
        ignore_case=TRUE))
      if (!is.na(fp_m[1,1])) {
        fp_title <- paste(str_trim(fp_m[1,2]), str_trim(fp_m[1,3]), "- Frontispiece")
        arts <- rbind(arts, data.frame(
          title=fp_title, page=0L, category="Frontispiece", stringsAsFactors=FALSE))
      }
      next
    }
    if (str_detect(line, regex("^The Cover:|^Art Layout|^Illustrations by|^Color Transparency|^Photograph by",
                                ignore_case=TRUE))) next

    # Category header detection (allow optional trailing page number, e.g. "SPECIAL FEATURES 1")
    if (str_detect(line, regex("^(Special Features?|Fiction(?:\\s*[-—][^\\d]{0,30})?|General Features?|Features?\\s+for\\s+the\\s+Home|The Home(?:\\s*[-—:].{0,25})?|Appointments|Lessons?(?:\\s+for\\s+\\w+|\\s+Department)?|Lesson\\s*Department)(?:\\s+\\d{1,3})?\\s*$",
                                ignore_case=TRUE))) {
      cat_text <- str_extract(line, regex("Special Features?|Fiction|General Features?|Features?\\s+for\\s+the\\s+Home|The Home|Appointments|Lessons?",
                                           ignore_case=TRUE))
      if (str_detect(cat_text, regex("Special Feature", ignore_case=TRUE))) current_cat <- "Special Feature"
      else if (str_detect(cat_text, regex("^Fiction", ignore_case=TRUE))) current_cat <- "Fiction"
      else if (str_detect(cat_text, regex("General Feature", ignore_case=TRUE))) current_cat <- "General Feature"
      else if (str_detect(cat_text, regex("Features?\\s+for\\s+the\\s+Home", ignore_case=TRUE))) current_cat <- "The Home"
      else if (str_detect(cat_text, regex("The Home", ignore_case=TRUE))) current_cat <- "The Home"
      else if (str_detect(cat_text, regex("Appointments", ignore_case=TRUE))) current_cat <- "Special Feature"
      else if (str_detect(cat_text, regex("Lesson", ignore_case=TRUE))) current_cat <- "Lesson"
      in_poetry <- FALSE
      next
    }

    # Poetry header (allow optional trailing page number)
    if (str_detect(line, regex("^Poetry(?:\\s+\\d{1,3})?\\s*$", ignore_case=TRUE))) {
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
      # Try: "page Title..." (page-first format, may have concatenated poems)
      # Check this BEFORE pm_v53 so embedded two-poem lines are split correctly
      pm2 <- str_match(line, "^(\\d{1,3})\\s+(.+)")
      if (!is.na(pm2[1,1])) {
        pg <- as.integer(pm2[1,2])
        full_title <- str_trim(pm2[1,3])
        # Check for embedded second poem using comma-based splitting
        # Pattern: "Title1 Author1 Title2, Author2, Page2"
        comma_locs <- str_locate_all(full_title, ",")[[1]]
        if (nrow(comma_locs) >= 2) {
          c1 <- comma_locs[nrow(comma_locs) - 1, 1]
          c2 <- comma_locs[nrow(comma_locs), 1]
          before <- str_trim(str_sub(full_title, 1, c1 - 1))
          between <- str_trim(str_sub(full_title, c1 + 1, c2 - 1))
          after_c <- str_trim(str_sub(full_title, c2 + 1))
          is_author <- str_detect(between, "^[A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){0,3}$")
          is_page <- str_detect(after_c, "^\\d{1,3}\\.?$")
          if (is_author && is_page) {
            second_author <- between
            second_pg <- as.integer(str_extract(after_c, "\\d+"))
            # Split "before" into first poem + title2
            # Use {1,2} for author (2-3 words) to avoid grabbing title words
            auth_end <- str_match(before,
              "^(.+?)\\s+([A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){1,2})\\s+(.+)$")
            if (!is.na(auth_end[1,1])) {
              first_title <- paste(str_trim(auth_end[1,2]), str_trim(auth_end[1,3]))
              second_title_raw <- str_trim(auth_end[1,4])
            } else {
              first_title <- before
              second_title_raw <- ""
            }
            if (nchar(second_title_raw) > 0) {
              arts <- rbind(arts, data.frame(
                title=first_title, page=pg, category="Poetry", stringsAsFactors=FALSE))
              arts <- rbind(arts, data.frame(
                title=paste(second_title_raw, second_author), page=second_pg,
                category="Poetry", stringsAsFactors=FALSE))
            } else {
              arts <- rbind(arts, data.frame(
                title=before, page=pg, category="Poetry", stringsAsFactors=FALSE))
              arts <- rbind(arts, data.frame(
                title=paste(str_trim(str_sub(full_title, c1+1)), second_author),
                page=second_pg, category="Poetry", stringsAsFactors=FALSE))
            }
            next
          }
        }
        # Handle 1-comma case with trailing page number:
        # "Title1 Author1 Title2, Author2 Page2"
        if (nrow(comma_locs) == 1) {
          trail_pg_m <- str_match(full_title, "\\s+(\\d{1,3})\\.?\\s*$")
          if (!is.na(trail_pg_m[1,1])) {
            second_pg <- as.integer(trail_pg_m[1,2])
            stripped <- str_trim(str_sub(full_title, 1L,
              nchar(full_title) - nchar(trail_pg_m[1,1])))
            c1 <- comma_locs[1, 1]
            before <- str_trim(str_sub(stripped, 1, c1 - 1))
            after_text <- str_trim(str_sub(stripped, c1 + 1))
            # after_text should be author of second poem
            is_author2 <- str_detect(after_text,
              "^[A-Z][a-zA-Z.]+(?:\\s+[A-Z][a-zA-Z.]+){0,3}$")
            if (is_author2) {
              second_author <- after_text
              # Split "before" using initial-dot pattern as author signal
              # Match: FirstName Initial. LastName (e.g., "Vesta P. Crawford")
              init_m <- str_locate(before, "\\b[A-Z][a-z]+\\s+[A-Z]\\.\\s+[A-Z][a-z]+\\b")
              if (!is.na(init_m[1,1])) {
                first_author <- str_sub(before, init_m[1,1], init_m[1,2])
                first_title <- str_trim(str_sub(before, 1, init_m[1,1] - 1))
                second_title_raw <- str_trim(str_sub(before, init_m[1,2] + 1))
                if (nchar(first_title) > 0 && nchar(second_title_raw) > 0) {
                  arts <- rbind(arts, data.frame(
                    title=paste(first_title, first_author), page=pg,
                    category="Poetry", stringsAsFactors=FALSE))
                  arts <- rbind(arts, data.frame(
                    title=paste(second_title_raw, second_author), page=second_pg,
                    category="Poetry", stringsAsFactors=FALSE))
                  next
                }
              }
              # Fallback: try {1,2} author pattern (no initial)
              auth_end <- str_match(before,
                "^(.+?)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+){1,2})\\s+(.+)$")
              if (!is.na(auth_end[1,1])) {
                first_title <- paste(str_trim(auth_end[1,2]), str_trim(auth_end[1,3]))
                second_title_raw <- str_trim(auth_end[1,4])
                if (nchar(second_title_raw) > 0) {
                  arts <- rbind(arts, data.frame(
                    title=first_title, page=pg,
                    category="Poetry", stringsAsFactors=FALSE))
                  arts <- rbind(arts, data.frame(
                    title=paste(second_title_raw, second_author), page=second_pg,
                    category="Poetry", stringsAsFactors=FALSE))
                  next
                }
              }
            }
          }
        }
        # Fallback: add as single entry if comma split didn't work
        arts <- rbind(arts, data.frame(
          title=full_title, page=pg, category="Poetry", stringsAsFactors=FALSE))
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

    # Non-poetry entries: "Title Author page_num" (page at end)
    em_trail <- str_match(line, "^(.+?)\\s+(\\d{1,3})\\s*$")
    if (!is.na(em_trail[1,1])) {
      title_text <- str_trim(em_trail[1,2])
      pg <- as.integer(em_trail[1,3])
      # Don't treat "Chapter N" or "Part N" trailing numbers as page numbers
      if (str_detect(title_text, "(?i)(Chapter|Part|No\\.?)\\s*$")) {
        full_line <- str_trim(paste(title_text, pg))
        if (nchar(full_line) >= 5) {
          arts <- rbind(arts, data.frame(
            title=full_line, page=0L, category=current_cat,
            stringsAsFactors=FALSE))
          cat(sprintf("    CHAPTER_NUM: '%s' (cat=%s, page=0)\n",
                      str_sub(full_line, 1, 60), current_cat))
          next
        }
      }
      if (nchar(title_text) >= 5) {
        arts <- rbind(arts, data.frame(
          title=title_text, page=pg, category=current_cat,
          stringsAsFactors=FALSE))
        next
      }
    }

    # Category header with entries on same line (e.g. "The Home - inside and Out")
    # followed by content — treat as category change, entry may follow
    cat_inline <- str_match(line, regex(
      "^(Special Features?|Fiction|General Features?|Features?\\s+for\\s+the\\s+Home|The Home[^\\d]*?)\\s*(\\d{1,3})\\s+(.+)",
      ignore_case=TRUE))
    if (!is.na(cat_inline[1,1])) {
      cat_text <- str_trim(cat_inline[1,2])
      if (str_detect(cat_text, regex("Special Feature", ignore_case=TRUE))) current_cat <- "Special Feature"
      else if (str_detect(cat_text, regex("^Fiction", ignore_case=TRUE))) current_cat <- "Fiction"
      else if (str_detect(cat_text, regex("General Feature", ignore_case=TRUE))) current_cat <- "General Feature"
      else if (str_detect(cat_text, regex("Features?\\s+for\\s+the\\s+Home", ignore_case=TRUE))) current_cat <- "The Home"
      else if (str_detect(cat_text, regex("The Home", ignore_case=TRUE))) current_cat <- "The Home"
      else if (str_detect(cat_text, regex("Lesson", ignore_case=TRUE))) current_cat <- "Lesson"
      pg <- as.integer(cat_inline[1,3])
      title_text <- str_trim(cat_inline[1,4])
      arts <- rbind(arts, data.frame(
        title=title_text, page=pg, category=current_cat,
        stringsAsFactors=FALSE))
      next
    }

    # Fallback: line with text but no page number -- save as page=0
    # Catches: lesson entries (e.g. "Cultural Refinement -- Title Author"),
    # serial fiction continuations, and other entries without trailing page numbers
    if (nchar(line) >= 15) {
      arts <- rbind(arts, data.frame(
        title=line, page=0L, category=current_cat, stringsAsFactors=FALSE))
      cat(sprintf("    RC_FALLBACK: '%s' (cat=%s, page=0)\n",
                  str_sub(line, 1, 60), current_cat))
    }
  }

  if (nrow(arts) > 0) {
    arts <- arts[order(arts$page), ]
    cat(sprintf("  Re-read CONTENTS file: %d entries parsed\n", nrow(arts)))
  }
  arts
}


## ----find-title-in-body------------------------------------------------------
find_title_in_body <- function(body, title, hint_start, hint_end,
                                body_start = 1L, body_end = nchar(body)) {
  # Clean title for matching
  title_clean <- str_trim(title)
  if (nchar(title_clean) < 3) return(NA_integer_)

  # Helper: search in a window, return first match position or NA
  search_window <- function(pattern, lo, hi, ignore_case = TRUE) {
    window <- str_sub(body, lo, hi)
    m <- str_locate(window, regex(pattern, ignore_case = ignore_case))
    if (!is.na(m[1,1])) return(lo + m[1,1] - 1L)
    NA_integer_
  }

  # Define search windows: hint first, then full body
  windows <- list(
    c(max(body_start, hint_start - 2000L), min(body_end, hint_end + 2000L)),
    c(body_start, body_end)
  )

  # ----------------------------------------------------------
  # Strategy 1: EXACT match (escaped title as literal)
  # ----------------------------------------------------------
  escaped <- str_replace_all(title_clean, "([\\\\\\[\\](){}.*+?^$|])", "\\\\\\1")
  for (w in windows) {
    pos <- search_window(escaped, w[1], w[2])
    if (!is.na(pos)) return(pos)
  }

  # ----------------------------------------------------------
  # Strategy 2: WHITESPACE-FLEX (collapse whitespace to \\s+)
  # ----------------------------------------------------------
  ws_flex <- str_replace_all(escaped, "\\\\s\\+|\\s+", "\\\\s+")
  for (w in windows) {
    pos <- search_window(ws_flex, w[1], w[2])
    if (!is.na(pos)) return(pos)
  }

  # ----------------------------------------------------------
  # Strategy 3: DASH-FLEX (treat dashes/em-dashes as optional)
  # ----------------------------------------------------------
  dash_flex <- str_replace_all(ws_flex, "[-\u2014\u2013]+", "[-\\\\s\u2014\u2013]*")
  if (dash_flex != ws_flex) {
    for (w in windows) {
      pos <- search_window(dash_flex, w[1], w[2])
      if (!is.na(pos)) return(pos)
    }
  }

  # ----------------------------------------------------------
  # Strategy 3b: APOSTROPHE-FLEX (straight/curly apostrophe)
  # ----------------------------------------------------------
  apos_flex <- str_replace_all(dash_flex, "['\\u2019\\u2018]", "['\u2019\u2018]")
  if (apos_flex != dash_flex) {
    for (w in windows) {
      pos <- search_window(apos_flex, w[1], w[2])
      if (!is.na(pos)) return(pos)
    }
  }

  # ----------------------------------------------------------
  # Strategy 4: SIGNIFICANT-WORD proximity search
  # Find 3+ significant words within 200 chars of each other
  # ----------------------------------------------------------
  sig_words <- get_significant_words(title_clean)
  if (length(sig_words) >= 2) {
    # Use the first significant word as anchor, check others nearby
    anchor_escaped <- str_replace_all(sig_words[1], "([\\\\\\[\\](){}.*+?^$|])", "\\\\\\1")
    for (w in windows) {
      window_text <- str_sub(body, w[1], w[2])
      anchor_locs <- str_locate_all(window_text, regex(anchor_escaped, ignore_case = TRUE))[[1]]
      if (nrow(anchor_locs) == 0) next
      for (ai in seq_len(nrow(anchor_locs))) {
        anchor_pos <- anchor_locs[ai, 1]
        # Check neighborhood for other significant words
        nb_lo <- max(1L, anchor_pos - 100L)
        nb_hi <- min(nchar(window_text), anchor_pos + 200L)
        neighborhood <- str_sub(window_text, nb_lo, nb_hi)
        n_found <- sum(sapply(sig_words[-1], function(sw) {
          sw_esc <- str_replace_all(sw, "([\\\\\\[\\](){}.*+?^$|])", "\\\\\\1")
          str_detect(neighborhood, regex(sw_esc, ignore_case = TRUE))
        }))
        # Need at least half of remaining words (min 1) to match
        threshold <- max(1L, floor((length(sig_words) - 1) * 0.5))
        if (n_found >= threshold) {
          return(w[1] + anchor_pos - 1L)
        }
      }
    }
  }

  # ----------------------------------------------------------
  # Strategy 5: ALL-CAPS header match
  # Some articles appear as ALL-CAPS titles in body text
  # ----------------------------------------------------------
  upper_title <- toupper(title_clean)
  # Only try if title is not already all-caps and has 2+ words
  title_words <- str_split(title_clean, "\\s+")[[1]]
  if (length(title_words) >= 2 && title_clean != upper_title) {
    # Match first 3 words in ALL CAPS
    cap_words <- head(title_words, min(3, length(title_words)))
    cap_pat <- paste(toupper(cap_words), collapse = "\\\\s+")
    for (w in windows) {
      pos <- search_window(cap_pat, w[1], w[2], ignore_case = FALSE)
      if (!is.na(pos)) return(pos)
    }
  }

  # ----------------------------------------------------------
  # Strategy 6: CHAPTER-N match for serial fiction
  # ----------------------------------------------------------
  chap_m <- str_match(title_clean, "(?i)(Chapter\\s+\\d+)")
  if (!is.na(chap_m[1,1])) {
    chap_pat <- str_replace_all(chap_m[1,1], "\\s+", "\\\\s+")
    for (w in windows) {
      pos <- search_window(chap_pat, w[1], w[2])
      if (!is.na(pos)) return(pos)
    }
  }

  # ----------------------------------------------------------
  # Strategy 7: OCR-FLEX substitutions
  # Common OCR errors: I/l/1, rn/m, vou/you, etc.
  # ----------------------------------------------------------
  ocr_title <- title_clean
  ocr_title <- str_replace_all(ocr_title, "I", "[Il1|]")
  ocr_title <- str_replace_all(ocr_title, "m", "[m][n]?")
  ocr_title <- str_replace_all(ocr_title, "\\s+", "\\\\s+")
  # Only try if it differs from ws_flex
  if (ocr_title != ws_flex) {
    for (w in windows) {
      pos <- tryCatch(search_window(ocr_title, w[1], w[2]),
                       error = function(e) NA_integer_)
      if (!is.na(pos)) return(pos)
    }
  }

  # ----------------------------------------------------------
  # Strategy 8: KNOWN RECURRING TITLES
  # ----------------------------------------------------------
  recurring <- list(
    "From Near and Far" = "From\\s+Near\\s+and\\s+Far",
    "Notes From the Field" = "Notes\\s+[Ff]rom\\s+the\\s+[Ff]ield",
    "Notes to the Field" = "Notes\\s+to\\s+the\\s+[Ff]ield",
    "Woman.s Sphere" = "Woman.s\\s+Sphere",
    "Visiting Teacher" = "Visiting\\s+Teacher",
    "Birthday" = "\\bBirthday\\b"
  )
  for (rname in names(recurring)) {
    if (str_detect(title_clean, regex(rname, ignore_case = TRUE))) {
      rpat <- recurring[[rname]]
      for (w in windows) {
        pos <- search_window(rpat, w[1], w[2])
        if (!is.na(pos)) return(pos)
      }
    }
  }

  NA_integer_
}



## ----locate-all-articles-----------------------------------------------------
# ============================================================
# LOCATE ALL ARTICLES -- Find each CONTENTS title in body text
# ============================================================

locate_all_articles <- function(body, arts, page_index, body_start, body_end) {
  n <- nrow(arts)
  positions <- rep(NA_integer_, n)
  methods   <- rep("none", n)

  # Phase 1: Full-body search with page-estimated hint windows
  for (i in seq_len(n)) {
    pg <- arts$page[i]

    if (pg > 0 && nrow(page_index) > 0) {
      hint_center <- page_to_pos(page_index, pg, body_start, body_end)
      hint_lo <- max(body_start, hint_center - 5000L)
      hint_hi <- min(body_end, hint_center + 5000L)
    } else {
      hint_lo <- body_start
      hint_hi <- body_end
    }

    # Search hint window first, then full body as fallback
    pos <- find_title_in_body(body, arts$title[i], hint_lo, hint_hi,
                               body_start, body_end)
    if (!is.na(pos)) {
      positions[i] <- pos
      methods[i] <- "title"
    }
  }

  # Phase 2: Fix ordering violations
  # If a page-known article is found BEFORE a lower-page article,
  # re-search it within a constrained window near its page estimate.
  if (nrow(page_index) > 0) {
    positions <- fix_ordering_violations(body, arts, page_index,
                                          positions, methods,
                                          body_start, body_end)
  }

  # Phase 3: Fix CONTENTS-cluster false matches
  # Detect if many articles cluster in a small range (CONTENTS listing text).
  # A cluster of 5+ articles within 4000 chars is a CONTENTS area.
  # Re-search those articles from AFTER the cluster.
  found_pos <- positions[!is.na(positions)]
  if (length(found_pos) > 5) {
    found_sorted <- sort(found_pos)
    # Check first 4000 chars: how many articles fall in this range?
    cluster_end <- found_sorted[1] + 4000L
    n_in_cluster <- sum(found_sorted <= cluster_end)

    if (n_in_cluster >= 5) {
      # Find actual cluster end (max position in cluster)
      cluster_max <- max(found_sorted[found_sorted <= cluster_end])
      # Search-from position: just past the cluster, with a small gap
      search_after <- cluster_max + 500L

      cluster_idx <- which(!is.na(positions) & positions <= cluster_end)
      cat(sprintf("    CONTENTS cluster: %d articles in pos %d-%d, re-searching from %d\n",
                  length(cluster_idx), found_sorted[1], cluster_max, search_after))

      for (ci in cluster_idx) {
        pg <- arts$page[ci]
        if (pg > 0 && nrow(page_index) > 0) {
          hint_center <- page_to_pos(page_index, pg, body_start, body_end)
          hint_lo <- max(search_after, hint_center - 5000L)
          hint_hi <- min(body_end, hint_center + 5000L)
        } else {
          hint_lo <- search_after
          hint_hi <- body_end
        }
        new_pos <- find_title_in_body(body, arts$title[ci],
                                       hint_lo, hint_hi,
                                       search_after, body_end)
        if (!is.na(new_pos)) {
          positions[ci] <- new_pos
        } else {
          # Can't find outside cluster — keep original (handled at save time)
          cat(sprintf("    CONTENTS: '%s' not found after cluster, keeping pos %d\n",
                      str_sub(arts$title[ci], 1, 40), positions[ci]))
        }
      }
    }
  }

  # Build result dataframe
  result <- data.frame(
    idx = seq_len(n),
    title = arts$title,
    page = arts$page,
    category = arts$category,
    position = positions,
    method = methods,
    stringsAsFactors = FALSE
  )

  # Sort by position
  found <- result[!is.na(result$position), ]
  missed <- result[is.na(result$position), ]

  if (nrow(found) > 0) {
    found <- found[order(found$position), ]
  }

  list(found = found, missed = missed)
}


# ============================================================
# FIX ORDERING VIOLATIONS -- Post-processing
# Detects articles whose found position contradicts page order,
# then re-searches offending titles near their expected page.
# ============================================================

fix_ordering_violations <- function(body, arts, page_index,
                                     positions, methods,
                                     body_start, body_end) {
  n <- length(positions)
  # Build page-known pairs: articles with both a page and a found position
  pk <- which(arts$page > 0 & !is.na(positions))
  if (length(pk) < 2) return(positions)

  # Sort page-known articles by their CONTENTS page number
  pk_order <- pk[order(arts$page[pk])]

  # Detect violations: for consecutive page-ordered articles,
  # their found positions should also be monotonically increasing
  max_passes <- 3L
  for (pass in seq_len(max_passes)) {
    violations <- integer(0)

    # Refresh page-known indices after any corrections
    pk <- which(arts$page > 0 & !is.na(positions))
    if (length(pk) < 2) break
    pk_order <- pk[order(arts$page[pk])]

    for (j in 2:length(pk_order)) {
      prev_i <- pk_order[j - 1]
      curr_i <- pk_order[j]

      # If current article has higher/equal page but lower position → violation
      if (arts$page[curr_i] >= arts$page[prev_i] &&
          positions[curr_i] < positions[prev_i]) {
        # The one that's likely wrong is whichever is further from its page estimate
        est_prev <- page_to_pos(page_index, arts$page[prev_i], body_start, body_end)
        est_curr <- page_to_pos(page_index, arts$page[curr_i], body_start, body_end)
        err_prev <- abs(positions[prev_i] - est_prev)
        err_curr <- abs(positions[curr_i] - est_curr)

        # The one with larger error is more likely misplaced
        if (err_prev > err_curr) {
          violations <- c(violations, prev_i)
        } else {
          violations <- c(violations, curr_i)
        }
      }
    }

    violations <- unique(violations)
    if (length(violations) == 0) break

    cat(sprintf("    Ordering pass %d: %d violation(s) to fix\n", pass, length(violations)))

    for (vi in violations) {
      pg <- arts$page[vi]
      hint_center <- page_to_pos(page_index, pg, body_start, body_end)

      # Re-search in constrained window (±15000 chars of page estimate)
      search_lo <- max(body_start, hint_center - 15000L)
      search_hi <- min(body_end, hint_center + 15000L)
      hint_lo <- max(body_start, hint_center - 5000L)
      hint_hi <- min(body_end, hint_center + 5000L)

      new_pos <- find_title_in_body(body, arts$title[vi],
                                     hint_lo, hint_hi,
                                     search_lo, search_hi)

      if (!is.na(new_pos) && new_pos != positions[vi]) {
        cat(sprintf("    REORDER: '%s' (pg %d) moved from pos %d to %d\n",
                    str_sub(arts$title[vi], 1, 40), pg,
                    positions[vi], new_pos))
        positions[vi] <- new_pos
      } else if (is.na(new_pos)) {
        # Could not find in constrained window — keep original position
        cat(sprintf("    REORDER: '%s' (pg %d) not found in window, keeping pos %d\n",
                    str_sub(arts$title[vi], 1, 40), pg, positions[vi]))
      }
    }
  }

  positions
}


## ----extract-articles--------------------------------------------------------
# ============================================================
# EXTRACT ARTICLES -- Simple boundary rule:
# Article N starts at its found position, ends where N+1 starts.
# ============================================================

extract_articles <- function(body, located, body_end) {
  n <- nrow(located)
  if (n == 0) return(list())

  articles <- list()
  for (i in seq_len(n)) {
    start_pos <- located$position[i]

    # End position: where next article starts, or body_end
    if (i < n) {
      end_pos <- located$position[i + 1] - 1L
    } else {
      end_pos <- body_end
    }

    # Extract raw text
    raw_text <- str_sub(body, start_pos, end_pos)

    # Trim trailing ads
    ad_pos <- detect_ad_start(raw_text)
    ad_text <- ""
    if (!is.na(ad_pos)) {
      ad_text <- str_sub(raw_text, ad_pos)
      raw_text <- str_sub(raw_text, 1L, ad_pos - 1L)
    }

    # Strip mid-article ads
    mid_result <- strip_mid_article_ads(raw_text)
    raw_text <- mid_result$clean
    if (nchar(mid_result$ads) > 10) {
      ad_text <- paste0(ad_text, "\n---\n", mid_result$ads)
    }

    # Cap poems at ~2000 chars — route excess to ads (→ Misc), never drop text
    if (located$category[i] == "Poetry" && nchar(raw_text) > 2500L) {
      cap_at <- 2000L
      # Try to cap at a line break
      cap_text <- str_sub(raw_text, 1L, cap_at)
      last_nl <- max(c(0L, str_locate_all(cap_text, "\n")[[1]][,1]))
      if (last_nl > 500L) cap_at <- last_nl
      excess_text <- str_sub(raw_text, cap_at + 1L)
      raw_text <- str_sub(raw_text, 1L, cap_at)
      if (nchar(str_trim(excess_text)) > 10) {
        ad_text <- paste0(ad_text, "\n--- POEM OVERFLOW ---\n", excess_text)
      }
    }

    articles[[i]] <- list(
      idx = located$idx[i],
      title = located$title[i],
      page = located$page[i],
      category = located$category[i],
      position = start_pos,
      method = located$method[i],
      text = str_trim(raw_text),
      ads = str_trim(ad_text)
    )
  }

  articles
}



## ----process-issue-----------------------------------------------------------
# ============================================================
# PROCESS ONE ISSUE -- Main orchestrator per month
# ============================================================

process_vol53_issue <- function(filepath, output_dir) {
  fname <- basename(filepath)
  meta  <- get_metadata_vol50(fname)
  month_name <- meta$month
  month_num  <- match(month_name, MONTHS)
  month_upper <- toupper(month_name)

  cat(rep("=", 70), "\n", sep="")
  cat(sprintf("Processing: %s (%s %d)\n", fname, month_name, YEAR))
  cat(rep("=", 70), "\n", sep="")

  # --- Output folder and file prefix ---
  vol_folder <- sprintf("Vol%d_%d", VOLUME, YEAR)
  out_folder <- file.path(output_dir, vol_folder, month_name)
  if (!dir.exists(out_folder)) dir.create(out_folder, recursive=TRUE)
  prefix <- sprintf("V%d_%02d", VOLUME, month_num)

  # --- Read raw text ---
  raw_text <- read_file_safe(filepath)
  n_chars <- nchar(raw_text)
  cat(sprintf("  Raw text: %s chars\n", format(n_chars, big.mark=",")))

  # --- Detect body start (first running header) ---
  first_rh <- str_locate(raw_text, regex(
    paste0("\\b\\d{1,3}\\s+", month_upper, "\\s+", YEAR, "\\b"),
    ignore_case = TRUE))

  if (!is.na(first_rh[1,1])) {
    body_start <- first_rh[1,1]
  } else {
    body_start <- max(1L, as.integer(n_chars * 0.05))
  }

  body_end <- n_chars
  body_text <- str_sub(raw_text, body_start, body_end)
  cat(sprintf("  Body: chars %d-%d (%s chars)\n",
              body_start, body_end, format(nchar(body_text), big.mark=",")))

  # --- Parse CONTENTS file (preserve manual edits) ---
  contents_filepath <- file.path(out_folder, paste0(prefix, "_00_CONTENTS.txt"))

  if (!file.exists(contents_filepath)) {
    cat("  Extracting CONTENTS from raw text...\n")
    ct_start <- str_locate(raw_text, regex("CONTENTS|TABLE OF CONTENTS", ignore_case=TRUE))
    if (!is.na(ct_start[1,1])) {
      ct_text <- str_sub(raw_text, ct_start[1,1], min(body_start + 2000L, n_chars))
      write_file(str_trim(ct_text), contents_filepath)
      cat(sprintf("  Wrote CONTENTS file: %s\n", basename(contents_filepath)))
    } else {
      cat("  WARNING: No CONTENTS section found in raw text\n")
      write_file("", contents_filepath)
    }
  } else {
    cat("  CONTENTS file exists -- preserving manual edits\n")
  }

  arts <- read_contents_file(contents_filepath)
  cat(sprintf("  CONTENTS entries: %d\n", nrow(arts)))

  if (nrow(arts) == 0) {
    cat("  WARNING: No entries parsed from CONTENTS file.\n")
    return(data.frame(
      file = fname, month = month_name, n_contents = 0L,
      n_found = 0L, n_saved = 0L, n_missed = 0L,
      stringsAsFactors = FALSE))
  }

  # --- Adjust body_start for early articles (before first running header) ---
  if (nrow(arts) > 0 && !is.na(first_rh[1,1])) {
    rh_text <- str_sub(raw_text, first_rh[1,1], first_rh[1,2])
    first_rh_pg <- as.integer(str_extract(rh_text, "\\d+"))
    early_pages <- arts$page[arts$page > 0]
    min_art_pg <- if (length(early_pages) > 0) min(early_pages) else first_rh_pg

    # Only trigger for pages >= 50 (to exclude chapter numbers like 1-12)
    if (!is.na(min_art_pg) && min_art_pg >= 50 && min_art_pg < first_rh_pg) {
      cat(sprintf("  Early articles: CONTENTS starts page %d, first RH page %d\n",
                  min_art_pg, first_rh_pg))
      early_arts <- arts[arts$page >= 50 & arts$page < first_rh_pg, ]
      early_arts <- early_arts[order(early_arts$page), ]
      # Search only the post-CONTENTS portion of pre-body (avoid matching CONTENTS text)
      ct_pos <- str_locate(raw_text, regex("CONTENTS", ignore_case=TRUE))
      search_from <- if (!is.na(ct_pos[1,2])) ct_pos[1,2] + 1000L else nchar(raw_text) %/% 3
      search_from <- min(search_from, body_start - 1L)
      pre_text <- str_sub(raw_text, 1L, body_start - 1L)
      best_start <- body_start
      for (ea_i in seq_len(nrow(early_arts))) {
        pos <- find_title_in_body(pre_text, early_arts$title[ea_i],
                                  search_from, nchar(pre_text),
                                  search_from, nchar(pre_text))
        if (!is.na(pos)) {
          new_start <- snap_to_para_start(pre_text, pos, 1L)
          if (new_start < best_start && new_start > search_from) {
            cat(sprintf("  Found early article '%s' at raw position %d\n",
                        str_sub(early_arts$title[ea_i], 1, 40), new_start))
            best_start <- new_start
          }
        }
      }
      if (best_start < body_start) {
        cat(sprintf("  Moving body_start from %d to %d\n", body_start, best_start))
        body_start <- best_start
        body_text <- str_sub(raw_text, body_start, body_end)
        cat(sprintf("  New body: chars %d-%d (%s chars)\n",
                    body_start, body_end, format(nchar(body_text), big.mark=",")))
      }
    }
  }

  # --- Build page index ---
  page_index <- build_vol50_page_index(body_text, body_start, month_upper, YEAR)
  cat(sprintf("  Page index: %d markers (pages %s-%s)\n",
              nrow(page_index),
              if(nrow(page_index)>0) page_index$page[1] else "?",
              if(nrow(page_index)>0) tail(page_index$page,1) else "?"))

  # --- Create header-free search text (blank headers, preserve positions) ---
  search_body <- body_text

  # Blank odd-page headers: "NUM MONTH YEAR"
  rh_odd_pat <- paste0("\\b\\d{1,3}\\s+", month_upper, "\\s+", YEAR, "\\b")
  m_rh_odd <- str_locate_all(search_body, regex(rh_odd_pat, ignore_case=TRUE))[[1]]
  if (nrow(m_rh_odd) > 0) {
    for (ri in seq_len(nrow(m_rh_odd))) {
      str_sub(search_body, m_rh_odd[ri,1], m_rh_odd[ri,2]) <-
        strrep(" ", m_rh_odd[ri,2] - m_rh_odd[ri,1] + 1L)
    }
  }

  # Blank even-page headers: "RELIEF SOCIETY MAGAZINE NUM"
  rh_even_pat <- "(?:THE\\s+)?RELIEF\\s+SOCIETY\\s+MAGAZINE\\s+\\d{1,3}"
  m_rh_even <- str_locate_all(search_body, regex(rh_even_pat, ignore_case=TRUE))[[1]]
  if (nrow(m_rh_even) > 0) {
    for (ri in seq_len(nrow(m_rh_even))) {
      str_sub(search_body, m_rh_even[ri,1], m_rh_even[ri,2]) <-
        strrep(" ", m_rh_even[ri,2] - m_rh_even[ri,1] + 1L)
    }
  }

  # Blank ALL-CAPS article-title running headers: "NUM ALL_CAPS_TITLE"
  rh_caps_pat <- "\n\\s*\\d{1,3}\\s+(?:[A-Z][A-Z',\u2019-]+\\s+){1,}[A-Z][A-Z',\u2019-]+\\s*(?=\n)"
  m_rh_caps <- str_locate_all(search_body, regex(rh_caps_pat))[[1]]
  if (nrow(m_rh_caps) > 0) {
    for (ri in seq_len(nrow(m_rh_caps))) {
      str_sub(search_body, m_rh_caps[ri,1], m_rh_caps[ri,2]) <-
        strrep(" ", m_rh_caps[ri,2] - m_rh_caps[ri,1] + 1L)
    }
  }

  # Blank standalone ALL-CAPS title lines (even-page article headers)
  rh_caps2_pat <- "\n\\s*(?:[A-Z][A-Z',\u2019-]+\\s+){1,}[A-Z][A-Z',\u2019-]+\\s+\\d{1,3}\\s*(?=\n)"
  m_rh_caps2 <- str_locate_all(search_body, regex(rh_caps2_pat))[[1]]
  if (nrow(m_rh_caps2) > 0) {
    for (ri in seq_len(nrow(m_rh_caps2))) {
      str_sub(search_body, m_rh_caps2[ri,1], m_rh_caps2[ri,2]) <-
        strrep(" ", m_rh_caps2[ri,2] - m_rh_caps2[ri,1] + 1L)
    }
  }

  n_blanked <- nrow(m_rh_odd) + nrow(m_rh_even) + nrow(m_rh_caps) + nrow(m_rh_caps2)
  cat(sprintf("  Blanked %d running headers for title search\n", n_blanked))

  # --- Locate all articles (using header-free search text) ---
  located <- locate_all_articles(search_body, arts, page_index, 1L, nchar(search_body))

  n_found <- nrow(located$found)
  n_missed <- nrow(located$missed)
  cat(sprintf("  Located: %d/%d (%.1f%%), missed: %d\n",
              n_found, nrow(arts),
              if(nrow(arts)>0) n_found/nrow(arts)*100 else 0,
              n_missed))

  if (n_found > 0) {
    method_tbl <- table(located$found$method)
    cat("  Methods:", paste(names(method_tbl), method_tbl, sep="=", collapse=", "), "\n")
  }

  if (n_missed > 0) {
    cat("  MISSED:\n")
    for (mi in seq_len(nrow(located$missed))) {
      cat(sprintf("    [pg %d] %s\n",
                  located$missed$page[mi],
                  str_sub(located$missed$title[mi], 1, 60)))
    }
  }

  # --- Extract articles ---
  articles <- extract_articles(body_text, located$found, nchar(body_text))

  # --- Clean up old article files (keep CONTENTS) ---
  old_files <- list.files(out_folder, pattern = paste0("^", prefix, "_\\d{2}_"),
                          full.names = TRUE)
  old_files <- old_files[!grepl("_00_CONTENTS\\.txt$", old_files)]
  if (length(old_files) > 0) {
    file.remove(old_files)
    cat(sprintf("  Cleaned up %d old article files\n", length(old_files)))
  }

  # --- Save Misc file (pre-body text) ---
  misc_file <- file.path(out_folder, paste0(prefix, "_00_Misc.txt"))
  if (body_start > 100L) {
    pre_body <- str_sub(raw_text, 1L, body_start - 1L)
    pre_body <- clean_ocr_body(pre_body)
    misc_header <- sprintf("[Vol%d | %s %d | Pre-body | Misc]\n\n",
                           VOLUME, month_name, YEAR)
    write_file(paste0(misc_header, str_trim(pre_body)), misc_file)
    cat(sprintf("  Saved: %s (%d chars)\n", basename(misc_file), nchar(pre_body)))
  }

  # --- Save each article ---
  n_saved <- 0L
  for (ai in seq_along(articles)) {
    art <- articles[[ai]]
    text <- art$text

    text <- clean_vol50_headers(text)
    text <- clean_ocr_body(text)

    if (nchar(text) < 5L) {
      cat(sprintf("    SKIP (empty): %s\n", str_sub(art$title, 1, 50)))
      next
    }

    # Redirect CONTENTS stubs to Misc: very short articles (< 100 chars)
    # with page-number-like patterns are CONTENTS listing text, not real articles.
    # Route to Misc so no text is lost.
    n_lines <- str_count(text, "\n") + 1L
    has_page_nums <- str_detect(text, "\\b\\d{3}\\s+[A-Z]")
    if (nchar(text) < 100L && (has_page_nums || n_lines <= 3)) {
      cat(sprintf("    STUB → Misc: %s (%d chars)\n", str_sub(art$title, 1, 50), nchar(text)))
      # Append to Misc file
      stub_text <- sprintf("\n--- CONTENTS STUB: %s ---\n%s\n", art$title, text)
      if (file.exists(misc_file)) {
        existing <- read_file(misc_file)
        write_file(paste0(existing, stub_text), misc_file)
      }
      next
    }

    category <- classify_vol50_article(art$title, art$category)
    cat_abbr <- abbrev_category(category)

    title_slug <- short_title_filename(art$title)
    file_num <- sprintf("%02d", ai)
    out_name <- sprintf("%s_%s_%s_%s.txt", prefix, file_num, cat_abbr, title_slug)
    out_path <- file.path(out_folder, out_name)

    est_page <- art$page
    if (est_page == 0 && nrow(page_index) > 0) {
      est_page <- round(pos_to_page(page_index, art$position))
    }
    bracket <- sprintf("[Vol%d | %s %d | Page %s | %s | %s]",
                        VOLUME, month_name, YEAR,
                        if(!is.na(est_page) && est_page > 0) as.character(est_page) else "?",
                        category, art$title)

    write_file(paste0(bracket, "\n\n", str_trim(text), "\n"), out_path)
    n_saved <- n_saved + 1L
    cat(sprintf("    %s: %s (%s, %d chars, %s)\n",
                file_num, str_sub(art$title, 1, 40),
                art$method, nchar(text), out_name))

    if (nchar(art$ads) > 10L) {
      append_ads_to_misc(out_folder, prefix, art$ads, art$title)
    }
  }

  cat(sprintf("\n  SAVED: %d/%d articles\n\n", n_saved, nrow(arts)))

  data.frame(
    file = fname, month = month_name,
    n_contents = nrow(arts), n_found = n_found,
    n_saved = n_saved, n_missed = n_missed,
    stringsAsFactors = FALSE
  )
}


## ----main-loop----------------------------------------------------------------
# ============================================================
# MAIN LOOP -- Process all monthly files
# ============================================================

input_files <- sort(list.files(INPUT_DIR, pattern="\\.txt$", full.names=TRUE))
cat(sprintf("Found %d input files in %s\n\n", length(input_files), INPUT_DIR))

results <- list()
for (f in input_files) {
  res <- process_vol53_issue(f, OUTPUT_DIR)
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
cat(sprintf("Total found: %d\n", sum(summary_df$n_found)))
cat(sprintf("Total saved: %d\n", sum(summary_df$n_saved)))
cat(sprintf("Total missed: %d\n", sum(summary_df$n_missed)))
if (sum(summary_df$n_contents) > 0) {
  hit_rate <- sum(summary_df$n_found) / sum(summary_df$n_contents) * 100
  cat(sprintf("Hit rate: %.1f%%\n", hit_rate))
}
cat("\n")
print(kable(summary_df, format="pipe"))
