# apply_all_api.R - Title-based API merge for Vol 51 (v7)
# Strategy: Build full month text from API pages, identify and exclude
# CONTENTS page(s), find article titles, split by boundaries
# v7: Key-distance validation - reject matches where the text position
#     falls in an API page key that's too far from the article's page number
library(stringr)
library(readr)

VOLUME <- 51L
YEAR <- 1964L
BASE_DIR <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
OUTPUT_DIR <- file.path(BASE_DIR, "output", paste0("Vol", VOLUME, "_", YEAR))
API_DIR <- file.path(BASE_DIR, "api_fixes", paste0("vol", VOLUME))

MONTHS <- c("January","February","March","April","May","June",
            "July","August","September","October","November","December")
MONTH_STARTS <- c(1L, 81L, 161L, 241L, 321L, 401L, 481L, 561L, 641L, 721L, 801L, 881L)
MONTH_ENDS   <- c(80L,160L,240L, 320L, 400L, 480L, 560L, 640L, 720L, 800L, 880L, 960L)

# Max allowed distance between article page number and the API page key
# at the match position. Accounts for +4 offset in mislabeled files + some flex.
MAX_KEY_DIST <- 12L

cat(sprintf("=== Title-Based API Merge - Vol%d (%d) v7 ===\n", VOLUME, YEAR))

# STEP 1: Build page->text map from ALL API fix files
cat("--- Step 1: Building page text map ---\n")
api_page_text <- list()
api_files <- list.files(API_DIR, pattern = "^vol\\d+_[A-Za-z]+_p\\d+\\.txt$", full.names = TRUE)
cat(sprintf("  Found %d API fix files\n", length(api_files)))

for (af in api_files) {
  content <- tryCatch(read_file(af), error = function(e) "")
  if (nchar(content) < 50) next
  if (grepl("QC:\\s*FAIL", content)) next
  page_nums <- as.integer(str_match_all(content, "---\\s*PAGE\\s+(\\d+)")[[1]][, 2])
  page_sections <- str_split(content, "---\\s*PAGE\\s+\\d+\\s*\\(PDF page \\d+\\)\\s*---")[[1]]
  for (pi in seq_along(page_nums)) {
    if (pi + 1 <= length(page_sections)) {
      page_text <- str_trim(page_sections[pi + 1])
      page_text <- str_replace(page_text, "\\n\\d{3}\\s*$", "")
      if (nchar(page_text) > 10) {
        key <- as.character(page_nums[pi])
        if (is.null(api_page_text[[key]]) || nchar(page_text) > nchar(api_page_text[[key]])) {
          api_page_text[[key]] <- page_text
        }
      }
    }
  }
}
all_page_nums <- sort(as.integer(names(api_page_text)))
cat(sprintf("  Page text map: %d unique pages (range %d-%d)\n\n",
            length(api_page_text), min(all_page_nums), max(all_page_nums)))

# Helper: build search pattern from title words
make_title_pat <- function(title_str) {
  words <- str_extract_all(title_str, "[A-Za-z]{4,}")[[1]]
  if (length(words) < 2) {
    words <- str_extract_all(title_str, "[A-Za-z]{3,}")[[1]]
  }
  if (length(words) == 0) return(NULL)
  words <- words[1:min(5, length(words))]
  paste(words, collapse = ".{0,30}")
}

# STEP 2: Process each month
total_replaced <- 0L
total_kept <- 0L
total_key_rejected <- 0L

for (mi in seq_along(MONTHS)) {
  month_name <- MONTHS[mi]
  month_dir <- file.path(OUTPUT_DIR, month_name)
  if (!dir.exists(month_dir)) next
  cat(sprintf("\n=== %s ===\n", month_name))

  # 2a: Build full month text from API pages in expanded range
  lo <- MONTH_STARTS[mi] - 6L
  hi <- MONTH_ENDS[mi] + 6L
  month_pages <- all_page_nums[all_page_nums >= lo & all_page_nums <= hi]

  if (length(month_pages) == 0) {
    cat("  No API pages in range\n")
    next
  }

  month_texts <- character(length(month_pages))
  for (j in seq_along(month_pages)) {
    month_texts[j] <- api_page_text[[as.character(month_pages[j])]]
  }
  full_month_text <- paste(month_texts, collapse = "\n\n")
  text_len <- nchar(full_month_text)
  cat(sprintf("  Full month text: %d API pages, %d chars\n",
              length(month_pages), text_len))

  # 2a.2: Build position-to-key map AND identify CONTENTS/masthead pages
  exclude_ranges <- list()
  page_key_ranges <- list()  # list of c(start, end, key) for position-to-key lookup
  page_pos <- 1L
  for (j in seq_along(month_pages)) {
    page_end <- page_pos + nchar(month_texts[j]) - 1L
    pt <- month_texts[j]

    # Record position range for this key
    page_key_ranges <- c(page_key_ranges, list(c(page_pos, page_end, month_pages[j])))

    # Check for CONTENTS/masthead
    is_contents <- grepl("Special Features", pt) && grepl("General Features|The Home", pt)
    is_masthead <- grepl("Editor\\s+.*\\s+Associate Editor|General Manager", pt) &&
                   grepl("\\d{3}\\s+[A-Z]", pt)
    if (is_contents || is_masthead) {
      exclude_ranges <- c(exclude_ranges, list(c(page_pos, page_end)))
      cat(sprintf("  Excluding page key %d (pos %d-%d): %s\n",
                  month_pages[j], page_pos, page_end,
                  if (is_contents) "CONTENTS" else "masthead"))
    }
    page_pos <- page_end + 3L
  }

  # Helper: check if position is in any excluded range
  in_excluded <- function(pos) {
    for (r in exclude_ranges) {
      if (pos >= r[1] && pos <= r[2]) return(TRUE)
    }
    FALSE
  }

  # Helper: find page key for a text position
  find_key_at_pos <- function(pos) {
    for (r in page_key_ranges) {
      if (pos >= r[1] && pos <= r[2]) return(r[3])
    }
    NA_integer_
  }

  # 2b: Get article list from output files
  txt_files <- sort(list.files(month_dir, pattern = "\\.txt$"))
  art_files <- txt_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", txt_files)]
  if (length(art_files) == 0) next

  n <- length(art_files)
  art_page <- rep(NA_integer_, n)
  art_title <- rep("", n)
  art_author <- rep("", n)
  art_is_poem <- rep(FALSE, n)
  art_body_len <- rep(0L, n)
  art_content <- rep("", n)

  for (i in seq_len(n)) {
    filepath <- file.path(month_dir, art_files[i])
    art_content[i] <- tryCatch(read_file(filepath), error = function(e) "")
    pg_m <- str_match(art_content[i], "\\|\\s*Page\\s+(\\d+)\\s*\\|")
    if (!is.na(pg_m[1, 2])) art_page[i] <- as.integer(pg_m[1, 2])
    hdr_parts <- str_match(art_content[i], "\\|([^|]+)\\|([^|\\]]*)\\]")
    if (!is.na(hdr_parts[1, 2])) {
      art_title[i] <- str_trim(hdr_parts[1, 2])
      art_author[i] <- str_trim(hdr_parts[1, 3])
    }
    if (nchar(art_title[i]) < 3) {
      fn_m <- str_match(art_files[i], "V\\d+_\\d+_\\d+_[^_]+_(.+)\\.txt$")
      if (!is.na(fn_m[1, 2])) art_title[i] <- str_replace_all(fn_m[1, 2], "_", " ")
    }
    art_is_poem[i] <- grepl("_Poem_|_Frnt_", art_files[i])
    body <- str_replace_all(art_content[i], "\\[[^\\]]+\\]\\s*", "")
    art_body_len[i] <- nchar(str_trim(body))
  }

  # 2c: Find each article's title in the full month text
  art_pos <- rep(NA_integer_, n)
  ms <- MONTH_STARTS[mi]
  me <- MONTH_ENDS[mi]

  for (i in seq_len(n)) {
    if (is.na(art_page[i])) next
    frac <- (art_page[i] - ms) / max(1L, me - ms)
    expected_pos <- as.integer(frac * text_len)

    # Try title match
    pat <- make_title_pat(art_title[i])
    if (!is.null(pat)) {
      all_locs <- tryCatch(
        str_locate_all(full_month_text, regex(pat, ignore_case = TRUE))[[1]],
        error = function(e) matrix(NA, 0, 2)
      )
      if (nrow(all_locs) > 0 && !is.na(all_locs[1, 1])) {
        is_body <- !sapply(all_locs[, 1], in_excluded)
        if (any(is_body)) {
          body_locs <- all_locs[is_body, , drop = FALSE]
          dists <- abs(body_locs[, 1] - expected_pos)
          best <- which.min(dists)
          art_pos[i] <- as.integer(body_locs[best, 1])
          next
        }
      }
    }

    # Fallback: try author match
    if (nchar(art_author[i]) >= 4) {
      aw <- str_extract_all(art_author[i], "[A-Za-z]{3,}")[[1]]
      if (length(aw) >= 1) {
        apat <- paste(aw, collapse = "\\s+")
        all_alocs <- tryCatch(
          str_locate_all(full_month_text, regex(apat, ignore_case = TRUE))[[1]],
          error = function(e) matrix(NA, 0, 2)
        )
        if (nrow(all_alocs) > 0 && !is.na(all_alocs[1, 1])) {
          is_body <- !sapply(all_alocs[, 1], in_excluded)
          if (any(is_body)) {
            body_locs <- all_alocs[is_body, , drop = FALSE]
            dists <- abs(body_locs[, 1] - expected_pos)
            best <- which.min(dists)
            art_pos[i] <- max(1L, as.integer(body_locs[best, 1]) - 200L)
          }
        }
      }
    }
  }

  found <- !is.na(art_pos)
  cat(sprintf("  Titles found: %d / %d\n", sum(found), n))

  # 2c.2: KEY-DISTANCE VALIDATION
  # For each match, check if the API page key at the match position is
  # reasonably close to the article's page number. If not, the match
  # is in the wrong part of the text (e.g., expanded range from adjacent month).
  for (i in seq_len(n)) {
    if (is.na(art_pos[i]) || is.na(art_page[i])) next
    match_key <- find_key_at_pos(art_pos[i])
    if (!is.na(match_key)) {
      key_dist <- abs(as.integer(match_key) - art_page[i])
      if (key_dist > MAX_KEY_DIST) {
        cat(sprintf("  REJECT KEY: %s (page %d, matched at key %d, dist %d)\n",
                    art_files[i], art_page[i], match_key, key_dist))
        art_pos[i] <- NA_integer_
        total_key_rejected <- total_key_rejected + 1L
      }
    }
  }

  found <- !is.na(art_pos)
  if (total_key_rejected > 0 || sum(found) != sum(!is.na(art_pos)))
    cat(sprintf("  After key check: %d / %d\n", sum(found), n))

  # 2d: Sort found articles by position and extract between boundaries
  found_idx <- which(found)
  if (length(found_idx) < 2) {
    for (i in seq_len(n)) total_kept <- total_kept + 1L
    cat("  Too few matches for splitting\n")
    next
  }

  pos_order <- found_idx[order(art_pos[found_idx])]

  for (oi in seq_along(pos_order)) {
    i <- pos_order[oi]
    start_pos <- art_pos[i]

    if (oi < length(pos_order)) {
      next_i <- pos_order[oi + 1]
      end_pos <- art_pos[next_i] - 1L
    } else {
      end_pos <- text_len
    }

    extracted <- str_trim(str_sub(full_month_text, start_pos, end_pos))

    # Sanity: skip if too short
    if (nchar(extracted) < 20) {
      total_kept <- total_kept + 1L
      cat(sprintf("  SKIP short: %s (%d chars)\n", art_files[i], nchar(extracted)))
      next
    }

    # For poems, cap at reasonable length
    if (art_is_poem[i] && nchar(extracted) > 5000) {
      extracted <- str_trim(str_sub(extracted, 1L, 3000L))
    }

    # Build output preserving bracket header
    header_lines <- str_extract_all(art_content[i], "\\[[^\\]]+\\]")[[1]]
    header <- ""
    if (length(header_lines) > 0) {
      header <- paste0(paste(header_lines, collapse = "\n"), "\n\n")
    }
    patched <- paste0(header, extracted, "\n")

    filepath <- file.path(month_dir, art_files[i])
    write_file(patched, filepath)
    total_replaced <- total_replaced + 1L

    # Flag suspicious size changes
    ratio <- nchar(extracted) / max(1L, art_body_len[i])
    flag <- ""
    if (ratio < 0.1 && art_body_len[i] > 100) flag <- " [WARN: shrunk >90%]"
    if (ratio > 5 && art_body_len[i] > 100) flag <- " [WARN: grew >5x]"

    cat(sprintf("  MERGED: %s (pos %d, %d->%d chars)%s\n",
                art_files[i], start_pos, art_body_len[i], nchar(extracted), flag))
  }

  # Mark unfound articles as kept
  for (i in seq_len(n)) {
    if (!found[i]) {
      total_kept <- total_kept + 1L
      cat(sprintf("  KEPT: %s\n", art_files[i]))
    }
  }
}

cat("\n============================================================\n")
cat("SUMMARY\n")
cat("============================================================\n")
cat(sprintf("Articles replaced with API text: %d\n", total_replaced))
cat(sprintf("Articles kept (R extraction):    %d\n", total_kept))
cat(sprintf("Key-distance rejected matches:   %d\n", total_key_rejected))
cat("\nDone.\n")
