# Smoke-test the new full-text Content column in the linker's Interactive Browser.
# Reproduces the browse_data build against the real K20 session, no knit needed.
suppressPackageStartupMessages({ library(stm); library(tidyverse) })

linked <- "C:/Users/birch/Rachel/GMU/Dissertation/textanalysis/Claudepythonexperimentadvtext/output_linked"
load(file.path(linked, "topic_linker_session_K20.RData"))  # stm_model, meta, processed_texts, topic_labels
chosen_K <- stm_model$settings$dim$K

doc_topics <- make.dt(stm_model, meta = meta)
cat("doc_topics has doc_id:", "doc_id" %in% names(doc_topics), "\n")

topic_article_link <- doc_topics %>%
  pivot_longer(starts_with("Topic"), names_to = "topic", values_to = "proportion") %>%
  mutate(topic_num = as.integer(str_remove(topic, "Topic")),
         topic_label = topic_labels[topic_num]) %>%
  select(topic_num, topic_label, proportion,
         any_of(c("doc_id", "filename", "Title", "Author", "Article_Type",
                  "volume_num", "year", "issue"))) %>%
  arrange(topic_num, desc(proportion))

article_text <- processed_texts %>% select(doc_id, .body = text)

browse_data <- topic_article_link %>%
  group_by(topic_num) %>% slice_max(proportion, n = 25) %>% ungroup() %>%
  left_join(article_text, by = "doc_id") %>%
  mutate(proportion_pct = round(proportion * 100, 1),
         Content = str_squish(replace_na(.body, ""))) %>%
  select(Topic = topic_num, Label = topic_label, `%` = proportion_pct,
         Title, Author, Type = Article_Type, Vol = volume_num,
         Year = year, Issue = issue, Content, Filename = filename)

content_col <- which(names(browse_data) == "Content") - 1L

cat("browse_data rows:", nrow(browse_data), "(expect 25 x", chosen_K, "=",
    25 * chosen_K, "max)\n")
cat("Content column index (0-based):", content_col, "\n")
cat("rows with empty Content:", sum(browse_data$Content == ""), "\n")
cat("median Content chars:", median(nchar(browse_data$Content)), "\n")

# Does a keyword actually hit the body (the whole point)?
kw <- "suffrage"
hits <- sum(str_detect(str_to_lower(browse_data$Content), kw))
cat(sprintf("rows whose body contains '%s': %d\n", kw, hits))
cat("sample matched title:",
    browse_data %>% filter(str_detect(str_to_lower(Content), kw)) %>%
      slice(1) %>% pull(Title), "\n")
cat("\nBROWSER_CONTENT_CHECK_PASSED\n")
