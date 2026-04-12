library(knitr)
r_file <- purl("vol57_extractor.Rmd", output = tempfile(fileext = ".R"), quiet = TRUE)
source(r_file, local = TRUE)
