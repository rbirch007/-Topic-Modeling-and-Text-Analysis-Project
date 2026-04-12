# ============================================================
# AD CLEANER — Strip trailing advertisement content from articles
# Moves ad text to Misc file. Fixes many END_CUT issues since
# articles ending mid-ad appear truncated but the article text
# is actually complete.
# ============================================================

library(stringr)
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/OutputExtractedarticles"
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

# ---- Ad detection patterns ----
# High-confidence ad indicators
ad_patterns <- c(
  "\\$\\d+\\.\\d{2}",                        # prices ($12.95)
  "\\d{3}[-.]\\d{4}",                         # phone numbers
  "Salt Lake City.{0,20}Utah",                # address
  "(?i)(?:Write|Send)\\s+(?:for|to)\\s",      # call-to-action
  "\\bFREE\\b",                               # FREE (caps)
  "(?i)\\bpostpaid\\b",                        # mailing terms
  "(?i)\\border\\s+now\\b",                    # order now
  "(?i)\\bDeseret Book\\b",                    # known advertiser
  "(?i)\\bBeneficial Life\\b",                 # known advertiser
  "(?i)\\bZim.s\\b",                           # known advertiser
  "(?i)\\bZCMI\\b",                            # known advertiser
  "(?i)\\bMail Orders?\\b",                    # mail order
  "(?i)\\bSecond.Class Postage\\b",            # postage notice
  "(?i)When Buying From Advertisers",          # ad mention
  "(?i)Mention.{0,10}Relief Society Magazine", # ad mention
  "(?i)\\bINTRODUCTORY OFFER\\b"              # promo
)

# Detect where ad content starts in text
# Returns position of ad start, or NA if no ads found
detect_ad_start <- function(text) {
  n <- nchar(text)
  if (n < 300) return(NA_integer_)

  # Sliding window: 400 chars, step 200
  win_start <- max(1, n %/% 3)
  win_end <- max(win_start, n - 399)
  for (start in seq(win_start, win_end, by = 200)) {
    chunk <- str_sub(text, start, min(start + 399, n))
    score <- 0
    for (pat in ad_patterns) {
      hits <- tryCatch(str_count(chunk, pat), error = function(e) 0L)
      score <- score + hits
    }
    # 3+ ad indicators in a 400-char window = likely ad content
    if (score >= 3) {
      # Walk back to find last sentence end before ad
      pre_start <- max(1, start - 200)
      pre <- str_sub(text, pre_start, start)
      sent_ends <- str_locate_all(pre, "[.!?\"')][\\s\n]")[[1]]
      if (nrow(sent_ends) > 0) {
        return(pre_start + sent_ends[nrow(sent_ends), 2])
      }
      return(start)
    }
  }

  NA_integer_
}

# ============================================================
# MAIN PROCESSING
# ============================================================

cat("=", rep("=", 69), "\n", sep="")
cat("  AD CLEANER — Strip trailing ads from articles\n")
cat("=", rep("=", 69), "\n\n", sep="")

total_cleaned <- 0
total_ad_chars <- 0

for (vol_info in VOLUMES) {
  vol <- vol_info$vol
  year <- vol_info$year
  if (!(vol %in% TARGET_VOLS)) next

  vol_dir <- file.path(BASE, sprintf("Vol%d_%d", vol, year))
  if (!dir.exists(vol_dir)) next

  cat(sprintf("\n=== Vol %d (%d) ===\n", vol, year))
  vol_cleaned <- 0

  for (month in MONTHS) {
    month_dir <- file.path(vol_dir, month)
    if (!dir.exists(month_dir)) next

    misc_file <- file.path(month_dir, sprintf("V%d_%02d_00_Misc.txt", vol, which(MONTHS == month)))
    # Also try existing misc file names
    if (!file.exists(misc_file)) {
      existing_misc <- list.files(month_dir, pattern = "(?i)_Misc\\.txt$", full.names = TRUE)
      if (length(existing_misc) > 0) misc_file <- existing_misc[1]
    }

    # Read existing misc content
    misc_content <- ""
    if (file.exists(misc_file)) {
      misc_content <- read_file_safe(misc_file)
    }

    art_files <- list.files(month_dir, pattern = sprintf("^V%d_", vol), full.names = TRUE)
    art_files <- art_files[!str_detect(basename(art_files), "_(BOARD|CONTENTS|Misc)\\.txt$")]

    month_cleaned <- 0
    new_ad_text <- ""

    for (f in art_files) {
      text <- read_file_safe(f)
      fname <- basename(f)

      # Separate header from body
      header <- ""
      body <- text
      if (str_detect(text, "^\\s*\\[.+\\]")) {
        m <- str_locate(text, "^\\s*\\[.+?\\]")
        if (!is.na(m[1,1])) {
          header <- str_sub(text, m[1,1], m[1,2])
          body <- str_trim(str_sub(text, m[1,2] + 1, nchar(text)))
        }
      }

      ad_pos <- detect_ad_start(body)
      if (!is.na(ad_pos) && ad_pos > 100 && ad_pos < nchar(body) - 20) {
        # Extract ad text
        ad_text <- str_trim(str_sub(body, ad_pos, nchar(body)))
        clean_body <- str_trim(str_sub(body, 1, ad_pos - 1))

        if (nchar(ad_text) > 30 && nchar(clean_body) > 50) {
          # Write cleaned article
          if (nchar(header) > 0) {
            write_file(paste0(header, "\n\n", clean_body), f)
          } else {
            write_file(clean_body, f)
          }

          # Accumulate ad text
          new_ad_text <- paste0(new_ad_text,
            sprintf("\n--- Ad content from %s ---\n%s\n", fname, ad_text))

          month_cleaned <- month_cleaned + 1
          total_ad_chars <- total_ad_chars + nchar(ad_text)
        }
      }
    }

    # Append ad text to Misc file
    if (nchar(new_ad_text) > 0) {
      updated_misc <- paste0(misc_content, "\n\n=== EXTRACTED ADS ===\n", new_ad_text)
      write_file(updated_misc, misc_file)
    }

    if (month_cleaned > 0) {
      cat(sprintf("  %s: %d articles cleaned\n", month, month_cleaned))
      vol_cleaned <- vol_cleaned + month_cleaned
    }
  }
  total_cleaned <- total_cleaned + vol_cleaned
}

cat("\n")
cat("=", rep("=", 69), "\n", sep="")
cat(sprintf("  COMPLETE: %d articles cleaned, %d ad chars moved to Misc\n",
            total_cleaned, total_ad_chars))
cat("=", rep("=", 69), "\n", sep="")
