library(stringr); library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles"
INPUT <- file.path(BASE, "input/Vol39split_preprocessed/Vol39_No03_March_1952.txt")
OUTDIR <- file.path(BASE, "OutputExtractedarticles/Vol39_1952/March")

txt <- read_file(INPUT)
body_start <- 3613L
body <- str_sub(txt, body_start, nchar(txt))

# ============================================================
# FIX 1: "The Least of These" by Margery S. Stewart
# Story starts at body pos ~22532 (the actual title heading)
# Story ends before "Truth Beareth Record" at body pos ~34444
# ============================================================
story1_start <- 22532L  # "THE LEAST OF THESE" heading
# Walk back to find the actual article title line
# The heading appears as: 'ntest "The = of These'' Margery S. Stewart'
# Actually let's find better start - search for Stewart before Timothy
m_title <- str_locate(body, "Margery S\\. Stewart")
# The story title block starts a bit before the first Timothy
# Let's find: pattern near pos 22532 that starts the story
pre <- str_sub(body, story1_start - 200, story1_start + 50)
cat("Story 1 context:\n", str_replace_all(pre, "\n", " "), "\n\n")

# Story ends at "Truth Beareth" which is at 34444
story1_end <- 34444L - 1L
story1_text <- str_trim(str_sub(body, story1_start, story1_end))

# Clean running headers
story1_text <- str_replace_all(story1_text, "RELIEF SOCIETY MAGAZINE[^\n]{0,30}1952", "")
story1_text <- str_replace_all(story1_text, "\\bPage\\s+\\d{1,3}\\b", "")
story1_text <- str_replace_all(story1_text, "\n{3,}", "\n\n")
story1_text <- str_trim(story1_text)

story1_file <- file.path(OUTDIR, "V39_03_03_Fict_The_Least_of_Stewart.txt")
write_file(paste0("[\"The Least of These\" — Third Prize Story by Margery S. Stewart]\n\n", story1_text), story1_file)
cat(sprintf("FIX 1: Wrote %s (%d chars)\n", basename(story1_file), nchar(story1_text)))

# ============================================================
# FIX 2: "The Wearing of the Green" by Frances Carter Yost
# In OCR it appears as "Wearing of the Gay" (OCR error)
# Story starts at body pos ~62144, after "Royal Raiment" poem
# Story ends before "Room for Phyllis" at body pos ~89147
# ============================================================
story2_start <- 62144L
story2_end <- 89147L - 1L
story2_text <- str_trim(str_sub(body, story2_start, story2_end))

# Clean running headers
story2_text <- str_replace_all(story2_text, "RELIEF SOCIETY MAGAZINE[^\n]{0,30}1952", "")
story2_text <- str_replace_all(story2_text, "\\bPage\\s+\\d{1,3}\\b", "")
story2_text <- str_replace_all(story2_text, "\n{3,}", "\n\n")
story2_text <- str_trim(story2_text)

story2_file <- file.path(OUTDIR, "V39_03_34_Fict_The_Wearing_of_Yost.txt")
write_file(paste0("[The Wearing of the Green by Frances Carter Yost]\n\n", story2_text), story2_file)
cat(sprintf("FIX 2: Wrote %s (%d chars)\n", basename(story2_file), nchar(story2_text)))

# ============================================================
# FIX 3: Trim Art 07 (Spirit of Relief Society) which absorbed
# "The Least of These" and other content.
# Art 07 should be "The Spirit of Relief Society" by Amy Brown Lyman
# It currently runs from body pos ~15040 to pos ~34444 (too long)
# The actual article should end at pos ~22532 where "Least of These" starts
# ============================================================
art07_file <- file.path(OUTDIR, "V39_03_07_Art_The_Spirit_of_Lyman.txt")
art07_text <- read_file(art07_file)
# Extract just the Spirit of Relief Society article
# It runs from body pos 15040 to 22532
spirit_text <- str_trim(str_sub(body, 15040L, 22531L))
spirit_text <- str_replace_all(spirit_text, "RELIEF SOCIETY MAGAZINE[^\n]{0,30}1952", "")
spirit_text <- str_replace_all(spirit_text, "\\bPage\\s+\\d{1,3}\\b", "")
spirit_text <- str_replace_all(spirit_text, "\n{3,}", "\n\n")
spirit_text <- str_trim(spirit_text)

write_file(paste0("[The Spirit of Relief Society by Amy Brown Lyman]\n\n", spirit_text), art07_file)
cat(sprintf("FIX 3: Trimmed Art 07 from %d to %d chars\n", nchar(art07_text), nchar(spirit_text)))

# ============================================================
# FIX 4: Trim Art 33 (Faded Memory) which absorbed "Wearing of Green"
# Faded Memory is a short poem, should be ~200 chars max
# Currently 11KB because it grabbed everything from pos ~138393 to 150138
# Actually, Faded Memory poem + "A Family" article + other content got merged
# The poem should just be the poem text
# ============================================================
art33_file <- file.path(OUTDIR, "V39_03_33_Poem_Faded_Memory_by.txt")
# The Faded Memory poem is at body pos 138393
poem_text <- str_sub(body, 138393L, 138393L + 300L)
# Find end of poem - look for next title
m_next <- str_locate(str_sub(body, 138393L, 138393L + 2000L), "Family.{1,5}What.{1,5}You")
if (is.na(m_next[1,1])) {
  # Just take first 500 chars and find a natural break
  m_next2 <- str_locate(str_sub(body, 138393L, 138393L + 1000L), "\n\\s*\n")
  if (!is.na(m_next2[1,1])) {
    poem_end <- 138393L + m_next2[1,2]
  } else {
    poem_end <- 138393L + 300L
  }
} else {
  poem_end <- 138393L + m_next[1,1] - 2L
}
faded_poem <- str_trim(str_sub(body, 138393L, poem_end))
write_file(paste0("[Faded Memory by Alice R. Rich]\n\n", faded_poem), art33_file)
cat(sprintf("FIX 4: Trimmed Art 33 from 11KB to %d chars\n", nchar(faded_poem)))

# ============================================================
# FIX 5: Fix Art 05 "A Family Is What You Make It"
# Currently at wrong position. The actual article by Elsie Sim Hansen
# is in the body. Search for it.
# ============================================================
m_family <- str_locate(body, "Family.{1,5}What You.{1,5}ake")
if (is.na(m_family[1,1])) {
  # Search differently - the article has OCR title "Family Ys What You Bake Lt"
  m_family <- str_locate(body, "Family.{1,5}What You")
}
if (!is.na(m_family[1,1])) {
  # Find Elsie Sim Hansen
  m_elsie <- str_locate(str_sub(body, m_family[1,1], m_family[1,1] + 200), "Elsie|Hansen")
  if (!is.na(m_elsie[1,1])) {
    family_start <- m_family[1,1]
    # Find end - next article is "Let's Stuff a Rug" at pos 150599
    # But "Multiple Hobbies" is at 137167 and this is at ~138636
    # Actually check - Stuff a Rug is at 150599
    family_end <- 150599L - 1L
    # But that would be too large. Find a closer end.
    m_stuff <- str_locate(str_sub(body, family_start, family_start + 20000), "Stuff.{1,5}Rug")
    if (!is.na(m_stuff[1,1])) {
      family_end <- family_start + m_stuff[1,1] - 2L
    }
    family_text <- str_trim(str_sub(body, family_start, family_end))
    family_text <- str_replace_all(family_text, "RELIEF SOCIETY MAGAZINE[^\n]{0,30}1952", "")
    family_text <- str_replace_all(family_text, "\\bPage\\s+\\d{1,3}\\b", "")
    family_text <- str_replace_all(family_text, "\n{3,}", "\n\n")
    family_text <- str_trim(family_text)

    art05_file <- file.path(OUTDIR, "V39_03_05_Art_A_Family_Is_Hansen.txt")
    write_file(paste0("[A Family Is What You Make It by Elsie Sim Hansen]\n\n", family_text), art05_file)
    cat(sprintf("FIX 5: Rewrote Art 05 (%d chars)\n", nchar(family_text)))
  }
}

cat("\nAll fixes applied.\n")
