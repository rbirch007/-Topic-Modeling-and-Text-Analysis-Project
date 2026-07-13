# Smoke-test the R foundation of WEadvancedtextanalysis.Rmd against real K=20 data.
# Mirrors sections 2-7 + the stylo sample-builder (without the heavy BCT run),
# so we catch dplyr/base-R errors before the user knits.
suppressPackageStartupMessages({
  library(stm); library(tidyverse); library(scales); library(philentropy)
})

base_path   <- "C:/Users/birch/Rachel/GMU/Dissertation/textanalysis/Claudepythonexperimentadvtext"
linked_path <- file.path(base_path, "output_linked")
K <- 20L

adv_in <- readRDS(file.path(linked_path, sprintf("advanced_input_K%d.rds", K)))
model        <- adv_in$stm_model
meta         <- adv_in$meta
topic_labels_manual <- adv_in$topic_labels
modeled_raw  <- adv_in$modeled_raw

stopifnot(model$settings$dim$K == K,
          nrow(meta) == nrow(model$theta),
          length(modeled_raw) == nrow(meta))

vocab <- model$vocab
beta  <- exp(model$beta$logbeta[[1]]); colnames(beta) <- vocab
theta <- model$theta
top_words <- labelTopics(model, n = 3)$prob |> apply(1, paste, collapse = " ")
topic_labels <- if (!is.null(topic_labels_manual) && length(topic_labels_manual) == K)
  paste0("T", seq_len(K), ": ", topic_labels_manual) else paste0("T", seq_len(K), ": ", top_words)
topic_prevalence <- colMeans(theta)
docs <- readr::read_csv(file.path(linked_path, sprintf("advanced_docs_K%d.csv", K)),
                        show_col_types = FALSE)
stopifnot(nrow(docs) == nrow(theta))
cat("OK load: K", K, "docs", nrow(docs), "vocab", length(vocab), "\n")

# JSD geometry
jsd <- function(p, q) { m <- 0.5*(p+q)
  sqrt(0.5*(sum(p*log((p+1e-12)/(m+1e-12))) + sum(q*log((q+1e-12)/(m+1e-12))))) }
D_jsd <- matrix(0, K, K); for (i in 1:K) for (j in 1:K) D_jsd[i,j] <- jsd(beta[i,], beta[j,])
n_clusters <- max(4L, round(K/7))
hc <- hclust(as.dist(D_jsd), method = "ward.D2"); clusters <- cutree(hc, k = n_clusters)
mds <- cmdscale(as.dist(D_jsd), k = 2)
cat("OK geometry: clusters", n_clusters, "jsd range", round(range(D_jsd[D_jsd>0]),3), "\n")

# mds-interpret loop (the bit I just fixed)
mds_df <- tibble(topic=seq_len(K), Dim1=mds[,1], Dim2=mds[,2],
                 prevalence=topic_prevalence, cluster=factor(clusters), label=topic_labels)
for (ax in c("Dim1","Dim2")) {
  hi <- mds_df |> slice_max(.data[[ax]], n=3) |> pull(label)
  lo <- mds_df |> slice_min(.data[[ax]], n=3) |> pull(label)
  cat(ax, "+:", paste(hi, collapse=" | "), "\n")
}
cat("OK mds-interpret\n")

# topicCorr network
tcorr <- topicCorr(model, method="simple", cutoff=0.05)
adj <- tcorr$cor; adj[abs(adj)<0.05] <- 0; diag(adj) <- 0
cat("OK topicCorr: nonzero edges", sum(adj!=0)/2, "\n")

# dominant-topic join used by atlas + bertopic sections
stm_dom <- tibble(row_index = docs$row_index, stm_topic = max.col(theta))
cat("OK dominant-topic: distinct", dplyr::n_distinct(stm_dom$stm_topic), "\n")

# stylo sample-builder logic (no stylo call)
author_blocklist <- c("unknown", "editor", "the editor", "selected", "anonymous")
org_keywords <- "exponent|publisher|office|ticket|company|\\bco\\b|union|depot|emporium|store|bureau|society|association|department"
is_real_author <- function(a) {
  s <- str_squish(a); al <- str_replace_all(str_to_lower(s), "[’‘']", "'")
  has_surname <- str_detect(s, "[A-Za-z]{3,}")
  !is.na(a) & s != "" & !str_starts(s, "\\[") & !str_ends(s, ",") &
    !(al %in% author_blocklist) & !str_detect(al, org_keywords) & has_surname
}
sty_tbl <- docs |>
  mutate(text = modeled_raw, n_words = str_count(text, "\\S+"),
         author_clean = str_squish(author)) |>
  filter(is_real_author(author_clean), n_words >= 150)
top_authors <- sty_tbl |> count(author_clean, sort=TRUE) |> filter(n>=10) |>
  slice_head(n=12) |> pull(author_clean)
cat("OK stylo prep: signed docs", nrow(sty_tbl), "| top authors", length(top_authors), "\n")
print(head(top_authors, 12))

make_samples_per_author <- function(df, max_words=5000) {
  out <- list()
  for (a in top_authors) {
    txt <- df |> filter(author_clean==a) |> pull(text) |> paste(collapse=" ")
    words <- str_split(txt, "\\s+")[[1]]; words <- words[words!=""]
    n_chunk <- min(max(1, floor(length(words)/max_words)), 4)
    if (n_chunk < 1) next
    sizes <- rep(floor(length(words)/n_chunk), n_chunk); idx <- c(0, cumsum(sizes))
    for (i in seq_len(n_chunk)) {
      seg <- words[(idx[i]+1):idx[i+1]]
      key <- sprintf("%s_%d", str_replace_all(a, "[^A-Za-z]", ""), i)
      out[[key]] <- paste(seg, collapse=" ")
    }
  }
  out
}
sty_samples <- make_samples_per_author(sty_tbl)
cat("OK stylo samples:", length(sty_samples), "\n")
cat("\nALL_FOUNDATION_CHECKS_PASSED\n")
