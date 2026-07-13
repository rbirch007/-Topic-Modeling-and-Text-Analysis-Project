# Smoke-test the §9b hybrid-search chunk logic against the real CSV.
suppressPackageStartupMessages({ library(tidyverse) })
adv_path <- "C:/Users/birch/Rachel/GMU/Dissertation/textanalysis/Claudepythonexperimentadvtext/output_advanced_WE"
K <- 20L
hybrid_path <- file.path(adv_path, "search", sprintf("hybrid_search_K%d.csv", K))
hits <- readr::read_csv(hybrid_path, show_col_types = FALSE)
the_query <- if ("query" %in% names(hits)) hits$query[1] else "(unknown)"
show <- hits |>
  mutate(Content = stringr::str_squish(dplyr::coalesce(text, ""))) |>
  transmute(Rank = row_number(), Year = year, Author = author, Title = title,
            `Keyword?` = ifelse(keyword_hit, "yes", "-"),
            `sem#` = sem_rank, `lex#` = lex_rank,
            Score = round(fused_score, 4), Content)
content_col <- which(names(show) == "Content") - 1L
cat("query:", the_query, "| rows:", nrow(show), "| cols:", ncol(show),
    "| content_col(0-based):", content_col, "\n")
print(head(show |> select(Rank, Year, Title, `Keyword?`, `sem#`, `lex#`, Score), 5))
stopifnot(content_col == 8, nrow(show) > 0)
cat("HYBRID_RMD_CHUNK_CHECK_PASSED\n")
