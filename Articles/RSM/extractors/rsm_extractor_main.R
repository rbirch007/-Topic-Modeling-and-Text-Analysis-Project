# rsm_extractor_main.R - Main loop and QC for RSM extraction
# Source this after rsm_extractor_core.R

## ----main-loop----------------------------------------------------------------
# ============================================================
# MAIN LOOP — Process all monthly files
# ============================================================

input_files <- sort(list.files(INPUT_DIR, pattern="\\.txt$", full.names=TRUE))
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


## ----post-extraction-qc-------------------------------------------------------
# ============================================================
# POST-EXTRACTION QUALITY CONTROL
# ============================================================
# Scans all output files and flags potential issues:
#   1. Undersized articles (much smaller than page-span suggests)
#   2. Oversized articles (possible over-appending)
#   3. Ad contamination in article files
#   4. Article-like text leaked into Misc files
#   5. Missing or malformed bracket headers
#   6. Near-duplicate content across files
#   7. Empty or near-empty article files
# ============================================================

cat("\n\n")
cat(rep("=", 70), "\n", sep = "")
cat("POST-EXTRACTION QC REPAIR & QUALITY CONTROL\n")
cat(rep("=", 70), "\n", sep = "")

# ============================================================
# PHASE 1: QC AUTO-REPAIR
# Fixes common structural issues before QC reporting:
#   A. Strip pre-header content (text before bracket header)
#   B. Remove duplicate content from adjacent article files
# All removed content goes to Misc to preserve the "no text lost" rule.
# ============================================================

vol_output_dir <- file.path(OUTPUT_DIR, paste0("Vol", VOLUME, "_", YEAR))
month_dirs <- sort(list.dirs(vol_output_dir, recursive = FALSE))

repair_count <- 0L

if (length(month_dirs) > 0) {
  cat("\n--- Phase 1: QC Auto-Repair ---\n")

  for (mdir in month_dirs) {
    month_name <- basename(mdir)
    txt_files <- sort(list.files(mdir, pattern = "\\.txt$", full.names = TRUE))
    article_files <- txt_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", txt_files)]
    misc_file <- txt_files[grepl("_Misc\\.txt$", txt_files)]
    if (length(misc_file) == 0) next

    # ---- REPAIR A: Strip pre-header content ----
    # If content appears before the bracket header, check whether it
    # belongs to THIS article (via page markers) and reattach after
    # the header, or route to Misc if it belongs elsewhere.
    for (af in article_files) {
      content <- tryCatch(read_file(af), error = function(e) "")
      if (nchar(content) < 10) next

      # Check if file starts with bracket header
      if (str_detect(content, "^\\[Vol\\d+")) next  # already correct

      # Find bracket header elsewhere in file
      hdr_pos <- str_locate(content, "\\[Vol\\d+\\s*\\|")
      if (is.na(hdr_pos[1, 1])) next  # no header to anchor to

      pre_header <- str_sub(content, 1L, hdr_pos[1, 1] - 1L)
      pre_header <- str_trim(pre_header)
      if (nchar(pre_header) < 10) next  # trivial whitespace, skip

      # Extract article page from bracket header
      hdr_text <- str_sub(content, hdr_pos[1, 1],
                          min(hdr_pos[1, 1] + 200L, nchar(content)))
      art_page_m <- str_match(hdr_text, "Page\\s+(\\d+)")
      art_page <- if (!is.na(art_page_m[1, 2])) as.integer(art_page_m[1, 2])
                  else NA_integer_

      # Check for page markers in pre-header content
      pm_pat <- "(\\d{2,3})\\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}"
      pre_pages <- as.integer(str_match_all(pre_header, pm_pat)[[1]][, 2])

      # Decide: reattach or route to Misc
      reattach <- FALSE
      if (!is.na(art_page) && length(pre_pages) > 0) {
        # If any page marker in the pre-header is within ±3 pages of
        # the article's CONTENTS page, the content likely belongs here
        page_diffs <- abs(pre_pages - art_page)
        if (any(page_diffs <= 3L)) reattach <- TRUE
      } else if (is.na(art_page) || length(pre_pages) == 0) {
        # No page markers or no article page — reattach by default
        # (gap prepend fix means this content was likely assigned correctly)
        reattach <- TRUE
      }

      if (reattach) {
        # Move pre-header content AFTER the bracket header
        header_line <- str_sub(content, hdr_pos[1, 1], nchar(content))
        # Find end of bracket header line(s)
        hdr_end <- str_locate(header_line, "\\[Vol\\d+[^\\]]*\\]\n")
        if (!is.na(hdr_end[1, 2])) {
          h_part <- str_sub(header_line, 1L, hdr_end[1, 2])
          b_part <- str_trim(str_sub(header_line, hdr_end[1, 2] + 1L,
                                     nchar(header_line)))
          fixed_content <- paste0(h_part, pre_header, "\n\n", b_part)
        } else {
          fixed_content <- paste0(header_line)  # fallback: just strip
        }
        write_file(str_trim(fixed_content), af)
        repair_count <- repair_count + 1L
        cat(sprintf("  REPAIR-A: %s/%s — reattached %d pre-header chars (pages: %s, art pg %s)\n",
                    month_name, basename(af), nchar(pre_header),
                    paste(pre_pages, collapse = ","),
                    ifelse(is.na(art_page), "?", as.character(art_page))))
      } else {
        # Strip pre-header content to Misc
        fixed_content <- str_sub(content, hdr_pos[1, 1], nchar(content))
        write_file(fixed_content, af)

        misc_content <- tryCatch(read_file(misc_file[1]), error = function(e) "")
        misc_addition <- paste0("\n\n=== QC PRE-HEADER (from ", basename(af), ") ===\n",
                                pre_header)
        write_file(paste0(str_trim(misc_content), misc_addition), misc_file[1])

        repair_count <- repair_count + 1L
        cat(sprintf("  REPAIR-A: %s/%s — stripped %d pre-header chars to Misc (pages: %s vs art pg %s)\n",
                    month_name, basename(af), nchar(pre_header),
                    paste(pre_pages, collapse = ","),
                    ifelse(is.na(art_page), "?", as.character(art_page))))
      }
    }

    # ---- REPAIR B: Overlap detection + active trim ----
    # Adjacent articles are checked for n-gram overlap.  When overlap
    # exceeds 50%, the FIRST file is trimmed to remove the bleed-forward
    # content (the overlapping tail that belongs to the next article).
    # Trimmed content goes to Misc for preservation.
    article_files <- sort(list.files(mdir, pattern = "\\.txt$", full.names = TRUE))
    article_files <- article_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", article_files)]
    if (length(article_files) >= 2) {
      ng_len <- 8L
      make_ngrams <- function(words) {
        if (length(words) < ng_len) return(character(0))
        unique(sapply(seq_len(length(words) - ng_len + 1L),
                      function(i) paste(words[i:(i + ng_len - 1L)], collapse = " ")))
      }
      for (fi in seq_len(length(article_files) - 1L)) {
        f_curr <- article_files[fi]
        f_next <- article_files[fi + 1L]
        curr_content <- tryCatch(read_file(f_curr), error = function(e) "")
        next_content <- tryCatch(read_file(f_next), error = function(e) "")
        if (nchar(curr_content) < 200 || nchar(next_content) < 200) next
        curr_words <- str_extract_all(tolower(curr_content), "[a-z]+")[[1]]
        next_words <- str_extract_all(tolower(next_content), "[a-z]+")[[1]]
        if (length(curr_words) < 20 || length(next_words) < 20) next
        ng_curr <- make_ngrams(curr_words)
        ng_next <- make_ngrams(next_words)
        if (length(ng_curr) < 5 || length(ng_next) < 5) next
        shared <- intersect(ng_curr, ng_next)
        smaller_set <- min(length(ng_curr), length(ng_next))
        overlap <- length(shared) / smaller_set
        if (overlap >= 0.30) {
          cat(sprintf("  OVERLAP: %s/%s <-> %s — %.0f%% (%d shared n-grams)\n",
                      month_name, basename(f_curr), basename(f_next),
                      overlap * 100, length(shared)))
        }
        # Active trim: when overlap is severe (>50%), trim the first file
        if (overlap >= 0.50) {
          # Skip bracket headers to find the actual article body of next file
          # Headers are lines starting with [ — skip them to get the real opening text
          next_lines <- str_split(next_content, "\n")[[1]]
          body_start_idx <- 1L
          for (li in seq_along(next_lines)) {
            line_trimmed <- str_trim(next_lines[li])
            if (nchar(line_trimmed) == 0) next  # skip blank lines
            if (str_starts(line_trimmed, fixed("["))) {
              body_start_idx <- li + 1L
              next  # skip bracket header lines
            }
            break  # first non-bracket, non-blank line = body start
          }
          # Get the first 120 chars of actual body text (after headers)
          body_text_next <- paste(next_lines[body_start_idx:min(length(next_lines), body_start_idx + 5L)],
                                  collapse = " ")
          body_text_next <- str_squish(body_text_next)
          if (nchar(body_text_next) > 120) body_text_next <- str_sub(body_text_next, 1L, 120L)

          # Extract significant words (4+ letters) to search for
          next_body_words <- str_extract_all(body_text_next, "[A-Za-z]{4,}")[[1]]
          if (length(next_body_words) >= 3) {
            # Build flexible search: any 3-5 consecutive significant words
            n_search <- min(5, length(next_body_words))
            search_phrase <- paste(next_body_words[1:n_search], collapse = "\\s+.*?")
            bleed_loc <- str_locate(curr_content, regex(search_phrase, ignore_case = TRUE))

            # If 5 words didn't match, try 3 words
            if (is.na(bleed_loc[1, 1]) && n_search > 3) {
              search_phrase <- paste(next_body_words[1:3], collapse = "\\s+.*?")
              bleed_loc <- str_locate(curr_content, regex(search_phrase, ignore_case = TRUE))
            }

            if (!is.na(bleed_loc[1, 1]) && bleed_loc[1, 1] > 100) {
              # Trim current article at the bleed point
              trimmed_text <- str_trim(str_sub(curr_content, 1L, bleed_loc[1, 1] - 1L))
              excess_text <- str_sub(curr_content, bleed_loc[1, 1], nchar(curr_content))
              if (nchar(trimmed_text) >= 50) {
                write_file(trimmed_text, f_curr)
                # Route excess to Misc
                misc_content <- tryCatch(read_file(misc_file[1]), error = function(e) "")
                misc_addition <- paste0("\n\n=== OVERLAP TRIM (from ", basename(f_curr), ") ===\n",
                                        excess_text)
                write_file(paste0(str_trim(misc_content), misc_addition), misc_file[1])
                repair_count <- repair_count + 1L
                cat(sprintf("  OVERLAP TRIM: %s/%s — removed %d bleed chars (kept %d)\n",
                            month_name, basename(f_curr),
                            nchar(excess_text), nchar(trimmed_text)))
              }
            } else {
              cat(sprintf("  OVERLAP NOFIND: %s/%s — could not locate bleed point (searched: '%s')\n",
                          month_name, basename(f_curr),
                          str_sub(body_text_next, 1L, 60L)))
            }
          }
        }
      }
    }
  }

  cat(sprintf("  Phase 1 complete: %d repairs applied\n\n", repair_count))
}

# ============================================================
# PHASE 2: QC REPORTING
# ============================================================

cat("--- Phase 2: QC Reporting ---\n")

# Configuration
CPP_QC <- 2400L   # expected chars per page (same as extraction)
QC_UNDERSIZE_THRESH <- 0.20   # flag if < 20% of expected size
QC_OVERSIZE_THRESH  <- 3.0    # flag if > 3x expected size
QC_AD_SCORE_THRESH  <- 7L     # ad score threshold (same as detect_ad_start)
QC_EMPTY_THRESH     <- 50L    # files with fewer chars are "empty"
QC_DUP_NGRAM_LEN    <- 8L     # word n-gram length for duplicate detection
QC_DUP_OVERLAP      <- 0.30   # flag if 30%+ of n-grams overlap between files

# Refresh month_dirs after repairs
month_dirs <- sort(list.dirs(vol_output_dir, recursive = FALSE))
if (length(month_dirs) == 0) {
  cat(sprintf("  No output directories found in %s — skipping QC.\n", vol_output_dir))
} else {

  all_qc_issues <- list()
  issue_count <- 0L

  for (mdir in month_dirs) {
    month_name <- basename(mdir)
    txt_files <- sort(list.files(mdir, pattern = "\\.txt$", full.names = TRUE))
    if (length(txt_files) == 0) next

    # Separate file types
    board_files    <- txt_files[grepl("_BOARD\\.txt$", txt_files)]
    contents_files <- txt_files[grepl("_CONTENTS\\.txt$", txt_files)]
    misc_files     <- txt_files[grepl("_Misc\\.txt$", txt_files)]
    article_files  <- txt_files[!grepl("_(BOARD|CONTENTS|Misc)\\.txt$", txt_files)]

    # Read all article files
    art_data <- list()
    for (af in article_files) {
      content <- tryCatch(read_file(af), error = function(e) "")
      bn <- basename(af)

      # Parse bracket header for page number and category
      hdr_match <- str_match(content, "^\\[Vol(\\d+)\\s*\\|\\s*([^|]+)\\|\\s*Page\\s+(\\d+)\\s*\\|\\s*([^|]+)\\|")
      pg <- if (!is.na(hdr_match[1, 4])) as.integer(hdr_match[1, 4]) else NA_integer_
      cat_label <- if (!is.na(hdr_match[1, 5])) str_trim(hdr_match[1, 5]) else NA_character_
      has_header <- !is.na(hdr_match[1, 1])

      art_data[[bn]] <- list(
        file      = af,
        basename  = bn,
        content   = content,
        size      = nchar(content),
        page      = pg,
        category  = cat_label,
        has_header = has_header
      )
    }

    # Sort by page number for page-span calculation
    pages <- sapply(art_data, function(x) x$page)
    page_order <- order(pages, na.last = TRUE)
    art_keys <- names(art_data)[page_order]

    # ---- CHECK 1: Undersized & Oversized articles ----
    for (k in seq_along(art_keys)) {
      ad <- art_data[[art_keys[k]]]
      if (is.na(ad$page) || ad$page <= 0) next

      # Estimate page span from next article's page
      next_page <- NA_integer_
      if (k < length(art_keys)) {
        np <- art_data[[art_keys[k + 1]]]$page
        if (!is.na(np) && np > ad$page) next_page <- np
      }
      if (is.na(next_page)) next_page <- ad$page + 2L  # default 2 pages

      page_span <- max(1L, next_page - ad$page)
      expected_size <- page_span * CPP_QC

      # Skip poems (naturally short) for undersize check
      is_poem <- isTRUE(grepl("Poem|Poetry|Frontispiece", ad$category, ignore.case = TRUE)) ||
                 isTRUE(grepl("_Poem_", ad$basename))

      # Undersize check
      if (!is_poem && page_span >= 2 && ad$size < expected_size * QC_UNDERSIZE_THRESH) {
        issue_count <- issue_count + 1L
        all_qc_issues[[issue_count]] <- data.frame(
          month    = month_name,
          file     = ad$basename,
          issue    = "UNDERSIZED",
          detail   = sprintf("Size %d chars, expected ~%d (pg %d, span %d pages, %.0f%% of expected)",
                             ad$size, expected_size, ad$page, page_span,
                             ad$size / expected_size * 100),
          severity = "WARNING",
          stringsAsFactors = FALSE
        )
      }

      # Oversize check (multi-page articles only)
      if (page_span >= 1 && ad$size > expected_size * QC_OVERSIZE_THRESH && ad$size > 10000L) {
        issue_count <- issue_count + 1L
        all_qc_issues[[issue_count]] <- data.frame(
          month    = month_name,
          file     = ad$basename,
          issue    = "OVERSIZED",
          detail   = sprintf("Size %d chars, expected ~%d (pg %d, span %d pages, %.0f%% of expected)",
                             ad$size, expected_size, ad$page, page_span,
                             ad$size / expected_size * 100),
          severity = "WARNING",
          stringsAsFactors = FALSE
        )
      }
    }

    # ---- CHECK 2: Ad contamination in article files ----
    ad_patterns <- list(
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

    for (ad in art_data) {
      # Score the last 500 chars of the file for trailing ads
      tail_text <- str_sub(ad$content, max(1L, ad$size - 499L), ad$size)
      tail_score <- 0L
      for (p in ad_patterns) {
        tail_score <- tail_score + str_count(tail_text, p$pat) * p$w
      }
      if (tail_score >= QC_AD_SCORE_THRESH) {
        issue_count <- issue_count + 1L
        all_qc_issues[[issue_count]] <- data.frame(
          month    = month_name,
          file     = ad$basename,
          issue    = "AD_CONTAMINATION",
          detail   = sprintf("Trailing 500 chars have ad score %d (threshold %d)",
                             tail_score, QC_AD_SCORE_THRESH),
          severity = "INFO",
          stringsAsFactors = FALSE
        )
      }

      # Also score entire file for heavy ad presence
      total_score <- 0L
      for (p in ad_patterns) {
        total_score <- total_score + str_count(ad$content, p$pat) * p$w
      }
      # Normalize by file size (score per 1000 chars)
      if (ad$size > 500L) {
        score_density <- total_score / (ad$size / 1000)
        if (score_density > 5.0) {
          issue_count <- issue_count + 1L
          all_qc_issues[[issue_count]] <- data.frame(
            month    = month_name,
            file     = ad$basename,
            issue    = "HIGH_AD_DENSITY",
            detail   = sprintf("Ad score density %.1f per 1K chars (total score %d, size %d)",
                               score_density, total_score, ad$size),
            severity = "WARNING",
            stringsAsFactors = FALSE
          )
        }
      }
    }

    # ---- CHECK 3: Article-like text leaked into Misc ----
    for (mf in misc_files) {
      misc_content <- tryCatch(read_file(mf), error = function(e) "")
      misc_size <- nchar(misc_content)

      # Look for page markers that suggest article text
      pm_pat <- "\\b(\\d{2,3})\\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}"
      pm_matches <- str_extract_all(misc_content, pm_pat)[[1]]

      # Look for sections that DON'T have ad markers (i.e., article text)
      # Split by === ADVERTISEMENTS === headers
      sections <- str_split(misc_content, "===\\s*ADVERTISEMENTS[^=]*===")[[1]]
      non_ad_sections <- character(0)
      for (sec in sections) {
        sec_trimmed <- str_trim(sec)
        if (nchar(sec_trimmed) < 100) next
        # Score this section for ad patterns
        sec_score <- 0L
        for (p in ad_patterns) {
          sec_score <- sec_score + str_count(sec_trimmed, p$pat) * p$w
        }
        sec_density <- sec_score / max(1, nchar(sec_trimmed) / 1000)
        if (sec_density < 2.0 && nchar(sec_trimmed) > 200) {
          non_ad_sections <- c(non_ad_sections, str_sub(sec_trimmed, 1, 80))
        }
      }

      # Also check for uncaptured body text markers
      uncaptured_count <- str_count(misc_content,
                                     "===\\s*UNCAPTURED BODY TEXT")

      if (length(non_ad_sections) > 0 || uncaptured_count > 0) {
        detail_parts <- character(0)
        if (length(non_ad_sections) > 0) {
          detail_parts <- c(detail_parts,
                            sprintf("%d low-ad-score section(s) in Misc (possible article text)",
                                    length(non_ad_sections)))
        }
        if (uncaptured_count > 0) {
          detail_parts <- c(detail_parts,
                            sprintf("%d UNCAPTURED BODY TEXT segment(s)", uncaptured_count))
        }
        if (length(pm_matches) > 0) {
          detail_parts <- c(detail_parts,
                            sprintf("Page markers found: %s",
                                    paste(unique(pm_matches), collapse = ", ")))
        }
        issue_count <- issue_count + 1L
        all_qc_issues[[issue_count]] <- data.frame(
          month    = month_name,
          file     = basename(mf),
          issue    = "MISC_ARTICLE_LEAK",
          detail   = paste(detail_parts, collapse = "; "),
          severity = "INFO",
          stringsAsFactors = FALSE
        )
      }
    }

    # ---- CHECK 4: Missing or malformed bracket headers ----
    for (ad in art_data) {
      if (!ad$has_header) {
        issue_count <- issue_count + 1L
        all_qc_issues[[issue_count]] <- data.frame(
          month    = month_name,
          file     = ad$basename,
          issue    = "NO_BRACKET_HEADER",
          detail   = sprintf("File starts with: %.60s...", str_sub(ad$content, 1, 60)),
          severity = "WARNING",
          stringsAsFactors = FALSE
        )
      }
    }

    # ---- CHECK 5: Empty or near-empty article files ----
    for (ad in art_data) {
      if (ad$size <= QC_EMPTY_THRESH) {
        issue_count <- issue_count + 1L
        all_qc_issues[[issue_count]] <- data.frame(
          month    = month_name,
          file     = ad$basename,
          issue    = "EMPTY_FILE",
          detail   = sprintf("Only %d chars", ad$size),
          severity = "ERROR",
          stringsAsFactors = FALSE
        )
      }
    }

    # ---- CHECK 6: Near-duplicate content across files ----
    # Use word n-gram fingerprinting for efficiency
    if (length(art_data) >= 2) {
      # Build n-gram sets for each article (skip very small files)
      ngram_sets <- list()
      for (ad in art_data) {
        if (ad$size < 200) {
          ngram_sets[[ad$basename]] <- character(0)
          next
        }
        # Extract words, lowercase, take n-grams
        words <- str_extract_all(tolower(ad$content), "[a-z]+")[[1]]
        if (length(words) < QC_DUP_NGRAM_LEN) {
          ngram_sets[[ad$basename]] <- character(0)
          next
        }
        ngrams <- character(length(words) - QC_DUP_NGRAM_LEN + 1L)
        for (wi in seq_len(length(ngrams))) {
          ngrams[wi] <- paste(words[wi:(wi + QC_DUP_NGRAM_LEN - 1L)], collapse = " ")
        }
        ngram_sets[[ad$basename]] <- unique(ngrams)
      }

      # Compare pairs (only files with substantial n-gram sets)
      art_names <- names(ngram_sets)
      for (ii in seq_len(length(art_names) - 1L)) {
        ng_a <- ngram_sets[[art_names[ii]]]
        if (length(ng_a) < 10) next
        for (jj in (ii + 1L):length(art_names)) {
          ng_b <- ngram_sets[[art_names[jj]]]
          if (length(ng_b) < 10) next

          shared <- length(intersect(ng_a, ng_b))
          smaller <- min(length(ng_a), length(ng_b))
          overlap <- shared / smaller

          if (overlap >= QC_DUP_OVERLAP) {
            issue_count <- issue_count + 1L
            all_qc_issues[[issue_count]] <- data.frame(
              month    = month_name,
              file     = art_names[ii],
              issue    = "DUPLICATE_CONTENT",
              detail   = sprintf("%.0f%% n-gram overlap with %s (%d shared of %d)",
                                 overlap * 100, art_names[jj], shared, smaller),
              severity = "WARNING",
              stringsAsFactors = FALSE
            )
          }
        }
      }
    }

    # ---- CHECK 7: File count sanity ----
    if (length(board_files) == 0) {
      issue_count <- issue_count + 1L
      all_qc_issues[[issue_count]] <- data.frame(
        month = month_name, file = "(missing)",
        issue = "NO_BOARD_FILE",
        detail = "No BOARD file found for this month",
        severity = "ERROR", stringsAsFactors = FALSE
      )
    }
    if (length(contents_files) == 0) {
      issue_count <- issue_count + 1L
      all_qc_issues[[issue_count]] <- data.frame(
        month = month_name, file = "(missing)",
        issue = "NO_CONTENTS_FILE",
        detail = "No CONTENTS file found for this month",
        severity = "ERROR", stringsAsFactors = FALSE
      )
    }
    if (length(misc_files) == 0) {
      issue_count <- issue_count + 1L
      all_qc_issues[[issue_count]] <- data.frame(
        month = month_name, file = "(missing)",
        issue = "NO_MISC_FILE",
        detail = "No Misc file found for this month",
        severity = "ERROR", stringsAsFactors = FALSE
      )
    }
  }

  # ---- QC RESULTS ----
  if (issue_count == 0) {
    cat("\n  *** All checks passed — no issues found. ***\n\n")
  } else {
    qc_df <- do.call(rbind, all_qc_issues)

    # Summary by severity
    cat("\n")
    sev_counts <- table(qc_df$severity)
    for (s in c("ERROR", "WARNING", "INFO")) {
      if (s %in% names(sev_counts)) {
        cat(sprintf("  %s: %d issue(s)\n", s, sev_counts[s]))
      }
    }

    # Summary by issue type
    cat("\n  Issues by type:\n")
    type_counts <- sort(table(qc_df$issue), decreasing = TRUE)
    for (tn in names(type_counts)) {
      cat(sprintf("    %-22s %d\n", tn, type_counts[tn]))
    }

    # Print errors and warnings (skip INFO unless few)
    cat("\n")
    errors_warnings <- qc_df[qc_df$severity %in% c("ERROR", "WARNING"), ]
    if (nrow(errors_warnings) > 0) {
      cat(rep("-", 70), "\n", sep = "")
      cat("ERRORS & WARNINGS:\n")
      cat(rep("-", 70), "\n", sep = "")
      for (r in seq_len(nrow(errors_warnings))) {
        cat(sprintf("  [%s] %s / %s: %s — %s\n",
                    errors_warnings$severity[r],
                    errors_warnings$month[r],
                    errors_warnings$file[r],
                    errors_warnings$issue[r],
                    errors_warnings$detail[r]))
      }
    }

    info_issues <- qc_df[qc_df$severity == "INFO", ]
    if (nrow(info_issues) > 0) {
      cat("\n")
      cat(rep("-", 70), "\n", sep = "")
      cat("INFO (lower priority):\n")
      cat(rep("-", 70), "\n", sep = "")
      for (r in seq_len(nrow(info_issues))) {
        cat(sprintf("  [INFO] %s / %s: %s — %s\n",
                    info_issues$month[r],
                    info_issues$file[r],
                    info_issues$issue[r],
                    info_issues$detail[r]))
      }
    }

    # Write QC report to file
    qc_report_file <- file.path(vol_output_dir, paste0("Vol", VOLUME, "_QC_Report.txt"))
    sink(qc_report_file)
    cat(sprintf("Quality Control Report — Vol%d (%d)\n", VOLUME, YEAR))
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(rep("=", 70), "\n", sep = "")
    cat(sprintf("\nTotal issues: %d\n", nrow(qc_df)))
    for (s in c("ERROR", "WARNING", "INFO")) {
      if (s %in% names(sev_counts)) {
        cat(sprintf("  %s: %d\n", s, sev_counts[s]))
      }
    }
    cat("\n")
    for (r in seq_len(nrow(qc_df))) {
      cat(sprintf("[%s] %s / %s: %s — %s\n",
                  qc_df$severity[r], qc_df$month[r], qc_df$file[r],
                  qc_df$issue[r], qc_df$detail[r]))
    }
    sink()
    cat(sprintf("\n  QC report written to: %s\n", qc_report_file))
  }
}

