library(stringr); library(readr)
f <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/input/Vol39split_preprocessed/Vol39_No03_March_1952.txt"
txt <- read_file(f)
body_start <- 3613L
body <- str_sub(txt, body_start, nchar(txt))

# 1. "The Least of These" story title
m <- str_locate(body, regex("Least.{1,20}These", ignore_case=TRUE))
cat(sprintf("First 'Least of These' in body at pos %d\n", m[1,1]))
# There are likely multiple - find the one that starts the actual story
all_m <- str_locate_all(body, regex("Least.{1,10}These", ignore_case=TRUE))[[1]]
cat(sprintf("All occurrences: %d\n", nrow(all_m)))
for (i in seq_len(min(5, nrow(all_m)))) {
  pos <- all_m[i,1]
  snip <- str_sub(body, max(1, pos-30), min(pos+80, nchar(body)))
  snip <- str_replace_all(snip, "\n", " ")
  cat(sprintf("  [%d] pos %d: %s\n", i, pos, snip))
}

# 2. "Wearing of the Green" story
# The story is by Frances Carter Yost, starts with Granny O'Donnell
m2 <- str_locate(body, "Wearing.{1,20}G.{1,5}y\\s+Frances")
if (is.na(m2[1,1])) {
  m2 <- str_locate(body, "The Wearing of the G")
  if (is.na(m2[1,1])) {
    m2 <- str_locate(body, "Wearing of the")
  }
}
cat(sprintf("\nWearing of the Green title at body pos %d\n", m2[1,1]))
if (!is.na(m2[1,1])) {
  snip <- str_sub(body, m2[1,1], min(m2[1,1]+150, nchar(body)))
  cat(str_replace_all(snip, "\n", " "), "\n")
}

# 3. Find where Granny story actually begins
m3 <- str_locate(body, "RANNY O")
cat(sprintf("\nGranny story body pos %d\n", m3[1,1]))
snip3 <- str_sub(body, max(1, m3[1,1]-200), min(m3[1,1]+50, nchar(body)))
cat(str_replace_all(snip3, "\n", " "), "\n")

# 4. Find "A Family Is What You Make It" actual article position
m4 <- str_locate(body, "Family.{1,10}What.{1,10}Make")
all_m4 <- str_locate_all(body, "Family.{1,10}What.{1,10}Make")[[1]]
cat(sprintf("\nFamily Is What You Make: %d occurrences\n", nrow(all_m4)))
for (i in seq_len(min(5, nrow(all_m4)))) {
  pos <- all_m4[i,1]
  snip <- str_sub(body, max(1, pos-30), min(pos+80, nchar(body)))
  snip <- str_replace_all(snip, "\n", " ")
  cat(sprintf("  [%d] pos %d: %s\n", i, pos, snip))
}
