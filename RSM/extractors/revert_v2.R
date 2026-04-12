BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/OutputExtractedarticles"
vols <- list.dirs(BASE, recursive = FALSE)
reverted <- 0
for (vol_dir in vols) {
  months <- list.dirs(vol_dir, recursive = FALSE)
  for (month_dir in months) {
    backup_dir <- file.path(month_dir, "_v2_backups")
    if (!dir.exists(backup_dir)) next
    bf <- list.files(backup_dir, pattern = "\\.txt$", full.names = TRUE)
    for (b in bf) {
      file.copy(b, file.path(month_dir, basename(b)), overwrite = TRUE)
      reverted <- reverted + 1
    }
    unlink(backup_dir, recursive = TRUE)
    cat(sprintf("Reverted from %s/%s\n", basename(vol_dir), basename(month_dir)))
  }
}
cat(sprintf("Total reverted: %d files\n", reverted))
