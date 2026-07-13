# generate_handoff.R  (CROSS-CORPUS: Woman's Exponent vs Relief Society Magazine)
# Bridge between the comparison notebook (regression_compare_2_corpora.Rmd) and
# the cross-corpus advanced-analysis pipeline. Reads comparison_session_K{K}.RData
# (written by §27b of that notebook) and writes the row-aligned, PUBLICATION-tagged
# handoff artifacts the Python contrast layer + the advanced .Rmd consume.
#
# You normally do NOT need this: §27b already writes the four advanced_* files in
# one pass. Use this only when you fit the joint model on the Linux server and
# want to regenerate the handoff on Windows from the copied .RData.
#
# K is NOT hardcoded:
#   Rscript generate_handoff.R            -> newest comparison_session_K*.RData
#   Rscript generate_handoff.R 30         -> force K=30
#
# Inputs  (in output_linked/):  comparison_session_K{K}.RData
# Outputs (in output_linked/):  advanced_input_K{K}.rds
#                               advanced_docs_K{K}.csv     (has `corpus` tag)
#                               advanced_theta_K{K}.csv
#                               advanced_topics_K{K}.csv
# ---------------------------------------------------------------------------
suppressPackageStartupMessages({
  library(stm); library(dplyr); library(tibble); library(stringr); library(readr)
})

BASE <- "C:/Users/birch/Rachel/GMU/Dissertation/textanalysis/Claudepythonexperimentadvtext_compare"

# --- resolve scenario + K ---------------------------------------------------
#   Rscript generate_handoff.R                 -> newest K, scenario compare_full
#   Rscript generate_handoff.R 35              -> K=35, scenario compare_full
#   Rscript generate_handoff.R 20 compare_WE1892-1912_RSM1914-1934
args <- commandArgs(trailingOnly = TRUE)
SCENARIO <- if (length(args) >= 2 && nzchar(args[2])) args[2]
            else Sys.getenv("ADV_SCENARIO", "compare_full")
output_path <- file.path(BASE, "output_linked", SCENARIO)
if (!dir.exists(output_path)) stop("Scenario folder not found: ", output_path)
cat("Scenario:", SCENARIO, "| folder:", output_path, "\n")

if (length(args) >= 1 && nzchar(args[1])) {
  chosen_K <- as.integer(args[1])
  session <- file.path(output_path, sprintf("comparison_session_K%d.RData", chosen_K))
} else {
  cands <- list.files(output_path, pattern = "^comparison_session_K\\d+\\.RData$",
                      full.names = TRUE)
  if (!length(cands)) stop("No comparison_session_K*.RData found in ", output_path)
  session  <- cands[which.max(file.mtime(cands))]
  chosen_K <- as.integer(str_match(basename(session), "_K(\\d+)\\.RData$")[, 2])
}
cat("Loading", session, "...\n")
load(session)  # stm_model_corpus, meta, topic_labels, processed_texts, modeled_raw, modeled_clean, chosen_K

model_K <- stm_model_corpus$settings$dim$K
chosen_K <- model_K
cat("K =", chosen_K, " | theta rows =", nrow(stm_model_corpus$theta),
    " | meta rows =", nrow(meta), "\n")

# --- align modeled docs back to their source text --------------------------
if (!exists("modeled_raw")) {
  mi <- match(meta$doc_id, processed_texts$doc_id)
  modeled_raw   <- processed_texts$text[mi]
  modeled_clean <- processed_texts$text_final[mi]
}
stopifnot(nrow(meta) == nrow(stm_model_corpus$theta),
          length(modeled_raw) == nrow(meta))

col <- function(df, nm, default = NA) if (nm %in% names(df)) df[[nm]] else default

adv_docs <- tibble(
  row_index    = seq_len(nrow(meta)),
  doc_id       = meta$doc_id,
  filename     = col(meta, "filename"),
  title        = col(meta, "Title"),
  author       = col(meta, "Author"),
  article_type = col(meta, "Article_Type"),
  corpus       = col(meta, "corpus"),
  publication  = col(meta, "publication"),
  volume_num   = col(meta, "volume"),
  year         = col(meta, "year"),
  month        = col(meta, "month"),
  n_words      = str_count(modeled_raw, "\\S+"),
  text         = modeled_raw
)

theta_df <- as.data.frame(stm_model_corpus$theta)
colnames(theta_df) <- paste0("Topic", seq_len(ncol(theta_df)))
theta_df <- bind_cols(row_index = seq_len(nrow(theta_df)), theta_df)

.tw <- labelTopics(stm_model_corpus, n = 12)
topics_df <- tibble(
  topic_num = seq_len(chosen_K),
  label     = if (!is.null(topic_labels) && length(topic_labels) == chosen_K)
                str_remove(topic_labels, "^Topic \\d+: ") else paste0("Topic ", seq_len(chosen_K)),
  frex      = apply(.tw$frex, 1, paste, collapse = ", "),
  prob      = apply(.tw$prob, 1, paste, collapse = ", ")
)

ksfx <- paste0("_K", chosen_K)
saveRDS(list(stm_model = stm_model_corpus, meta = meta, topic_labels = topic_labels,
             modeled_raw = modeled_raw, modeled_clean = modeled_clean,
             fit_date = Sys.time()),
        file.path(output_path, paste0("advanced_input", ksfx, ".rds")))
write_csv(adv_docs,  file.path(output_path, paste0("advanced_docs",   ksfx, ".csv")))
write_csv(theta_df,  file.path(output_path, paste0("advanced_theta",  ksfx, ".csv")))
write_csv(topics_df, file.path(output_path, paste0("advanced_topics", ksfx, ".csv")))

cat("\nOK. Wrote advanced_*", ksfx, " files into output_linked/.\n", sep = "")
cat("Docs:", nrow(adv_docs),
    "| WE:", sum(adv_docs$corpus == "Womans_Exponent", na.rm = TRUE),
    "| RSM:", sum(adv_docs$corpus == "Relief_Society_Magazine", na.rm = TRUE), "\n")
