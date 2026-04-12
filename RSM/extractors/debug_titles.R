library(stringr); library(readr)
f <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/input/Vol39split_preprocessed/Vol39_No03_March_1952.txt"
txt <- read_file(f)
m <- str_locate(txt, "Acceptance for mailing")
body_start <- m[1,2] + 1L
search <- str_sub(txt, body_start, nchar(txt))
cat(sprintf("Body starts at char %d, body length: %d\n", body_start, nchar(search)))

titles <- c("Room for Phyllis", "Wearing of the Green", "Crowning Touch",
            "Price for Wheat", "Woman.s Sphere", "Editorial.{1,5}Message",
            "Red Cross", "Notes.{1,5}Field", "Gardening.{1,5}Food",
            "Multiple Hobbies", "Family.{1,5}What", "Stuff.{1,5}Rug",
            "Near and Far", "Sixty Years Ago", "In Memoriam",
            "Patriarch", "March Afternoon", "Royal Raiment",
            "Valley Hills", "All Her Ways")
for (t in titles) {
  m2 <- tryCatch(str_locate(search, regex(t, ignore_case=TRUE)),
                  error = function(e) matrix(NA, 1, 2))
  if (!is.na(m2[1,1])) {
    snippet <- str_sub(search, m2[1,1], min(m2[1,1] + 60, nchar(search)))
    snippet <- str_replace_all(snippet, "\n", " ")
    cat(sprintf("  pos %6d: %s => %.60s\n", m2[1,1], t, snippet))
  } else {
    cat(sprintf("  NOT FOUND: %s\n", t))
  }
}
