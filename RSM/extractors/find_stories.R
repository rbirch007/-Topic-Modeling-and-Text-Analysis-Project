library(stringr); library(readr)
f <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/input/Vol39split_preprocessed/Vol39_No03_March_1952.txt"
txt <- read_file(f)
n <- nchar(txt)

# 1. Find "The Least of These" story - protagonist is Timothy
m <- str_locate_all(txt, "Timothy")[[1]]
cat(sprintf("Timothy appears %d times, first at pos %d\n", nrow(m), m[1,1]))
snippet <- str_sub(txt, max(1, m[1,1]-100), min(m[1,1]+50, n))
snippet <- str_replace_all(snippet, "\n", " ")
cat(sprintf("  Context: ...%s...\n\n", snippet))

# 2. Find "Wearing of the Green" story - protagonist is Granny O'Donnell
m2 <- str_locate(txt, "Granny O")
cat(sprintf("Granny O appears at pos %d\n", m2[1,1]))
snippet2 <- str_sub(txt, max(1, m2[1,1]-100), min(m2[1,1]+50, n))
snippet2 <- str_replace_all(snippet2, "\n", " ")
cat(sprintf("  Context: ...%s...\n\n", snippet2))

# 3. Find "A Family Is What You Make It" - author Elsie Sim Hansen
m3 <- str_locate(txt, "Elsie Sim")
if (!is.na(m3[1,1])) {
  cat(sprintf("Elsie Sim at pos %d\n", m3[1,1]))
  snippet3 <- str_sub(txt, max(1, m3[1,1]-100), min(m3[1,1]+100, n))
  snippet3 <- str_replace_all(snippet3, "\n", " ")
  cat(sprintf("  Context: ...%s...\n\n", snippet3))
}

# 4. Check what body_start is
m_pub <- str_locate(txt, "Acceptance for mailing")
cat(sprintf("Body starts after char ~%d\n", m_pub[1,2]))

# Show char positions relative to body
body_start <- 3613L
cat(sprintf("\nRelative to body_start=%d:\n", body_start))
cat(sprintf("  Timothy: body pos %d\n", m[1,1] - body_start))
cat(sprintf("  Granny O: body pos %d\n", m2[1,1] - body_start))
