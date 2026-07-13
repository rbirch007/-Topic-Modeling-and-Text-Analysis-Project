# generate_handoff.R --------------------------------------------------------
# Bridge between the (Linux) topic linker and the Windows advanced-analysis
# pipeline. Reads topic_linker_session_K{K}.RData and writes the row-aligned
# handoff artifacts the Python compute layer + the .Rmd consume.
#
# Neither K nor scenario is hardcoded:
#   Rscript generate_handoff.R                 -> newest session, scenario "WEfull"
#   Rscript generate_handoff.R 35              -> force K=35, scenario "WEfull"
#   Rscript generate_handoff.R 35 WE1872-1891  -> force K=35, scenario WE1872-1891
#   set ADV_SCENARIO=WE1892-1912; Rscript generate_handoff.R 35   -> scenario via env
# (scenario: arg 2 > env ADV_SCENARIO > "WEfull"; matches the linker + config.py)
#
# Inputs  (in output_linked/<scenario>/):  topic_linker_session_K{K}.RData
# Outputs (in output_linked/<scenario>/):  advanced_input_K{K}.rds
#                                           advanced_docs_K{K}.csv
#                                           advanced_theta_K{K}.csv
#                                           advanced_topics_K{K}.csv
# ---------------------------------------------------------------------------
suppressPackageStartupMessages({
  library(stm); library(dplyr); library(tibble); library(stringr); library(readr)
})

BASE <- "C:/Users/birch/Rachel/GMU/Dissertation/textanalysis/Claudepythonexperimentadvtext"

# --- resolve scenario + K ---------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)

# scenario: arg 2 > env ADV_SCENARIO > "WEfull". Each scenario is its own
# output_linked/<scenario>/ subfolder, matching the linker and config.py.
scenario <- if (length(args) >= 2 && nzchar(args[2])) {
  args[2]
} else {
  env_s <- Sys.getenv("ADV_SCENARIO"); if (nzchar(env_s)) env_s else "WEfull"
}
output_path <- file.path(BASE, "output_linked", scenario)
if (!dir.exists(output_path)) dir.create(output_path, recursive = TRUE)
cat("Scenario:", scenario, "->", output_path, "\n")

# K: arg 1 forces it; else newest topic_linker_session_K*.RData in the scenario folder.
if (length(args) >= 1 && nzchar(args[1])) {
  chosen_K <- as.integer(args[1])
  session <- file.path(output_path, sprintf("topic_linker_session_K%d.RData", chosen_K))
} else {
  cands <- list.files(output_path, pattern = "^topic_linker_session_K\\d+\\.RData$",
                      full.names = TRUE)
  if (!length(cands)) stop("No topic_linker_session_K*.RData found in ", output_path)
  session  <- cands[which.max(file.mtime(cands))]
  chosen_K <- as.integer(str_match(basename(session), "_K(\\d+)\\.RData$")[, 2])
}
cat("Loading", session, "...\n")
load(session)  # stm_model, meta, processed_texts, topic_labels, ...

# K from the model is authoritative; warn if the filename disagreed
model_K <- stm_model$settings$dim$K
if (!is.na(chosen_K) && chosen_K != model_K)
  cat("NOTE: filename K=", chosen_K, " but model K=", model_K,
      " -> using model K.\n", sep = "")
chosen_K <- model_K

cat("K =", chosen_K, " | theta rows =", nrow(stm_model$theta),
    " | meta rows =", nrow(meta), "\n")
cat("processed_texts cols:", paste(names(processed_texts), collapse = ", "), "\n")
cat("meta cols:", paste(names(meta), collapse = ", "), "\n")

# --- align modeled docs back to their source text --------------------------
modeled_idx   <- match(meta$doc_id, processed_texts$doc_id)
modeled_raw   <- processed_texts$text[modeled_idx]
modeled_clean <- processed_texts$text_final[modeled_idx]

stopifnot(nrow(meta) == nrow(stm_model$theta),
          length(modeled_raw) == nrow(meta),
          !anyNA(modeled_idx))

# defensive column access: meta schema can vary slightly across linker versions
col <- function(df, nm, default = NA) if (nm %in% names(df)) df[[nm]] else default

adv_docs <- tibble(
  row_index    = seq_len(nrow(meta)),
  doc_id       = meta$doc_id,
  filename     = col(meta, "filename"),
  title        = col(meta, "Title"),
  author       = col(meta, "Author"),
  article_type = col(meta, "Article_Type"),
  volume_num   = col(meta, "volume_num"),
  year         = col(meta, "year"),
  issue        = col(meta, "issue"),
  n_words      = str_count(modeled_raw, "\\S+"),
  text         = modeled_raw
)

theta_df <- as.data.frame(stm_model$theta)
colnames(theta_df) <- paste0("Topic", seq_len(ncol(theta_df)))
theta_df <- bind_cols(row_index = seq_len(nrow(theta_df)), theta_df)

.tw <- labelTopics(stm_model, n = 12)
topics_df <- tibble(
  topic_num = seq_len(chosen_K),
  label     = if (!is.null(topic_labels) && length(topic_labels) == chosen_K)
                topic_labels else paste0("Topic ", seq_len(chosen_K)),
  frex      = apply(.tw$frex, 1, paste, collapse = ", "),
  prob      = apply(.tw$prob, 1, paste, collapse = ", ")
)

ksfx <- paste0("_K", chosen_K)
saveRDS(list(stm_model = stm_model, meta = meta, topic_labels = topic_labels,
             modeled_raw = modeled_raw, modeled_clean = modeled_clean,
             fit_date = Sys.time()),
        file.path(output_path, paste0("advanced_input", ksfx, ".rds")))
write_csv(adv_docs,  file.path(output_path, paste0("advanced_docs",   ksfx, ".csv")))
write_csv(theta_df,  file.path(output_path, paste0("advanced_theta",  ksfx, ".csv")))
write_csv(topics_df, file.path(output_path, paste0("advanced_topics", ksfx, ".csv")))

cat("\nOK. Wrote advanced_*", ksfx, " files into ", output_path, "\n", sep = "")
cat("Docs:", nrow(adv_docs), "| Year range:",
    paste(range(adv_docs$year, na.rm = TRUE), collapse = "-"), "\n")
cat("n_words summary:\n"); print(summary(adv_docs$n_words))
cat("year NA count:", sum(is.na(adv_docs$year)), "\n")
