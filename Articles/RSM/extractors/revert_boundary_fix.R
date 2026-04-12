# Revert all boundary fixer changes from _boundary_fix_backups
library(readr)

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/OutputExtractedarticles"

vols <- list.dirs(BASE, recursive = FALSE)
reverted <- 0

for (vol_dir in vols) {
  months <- list.dirs(vol_dir, recursive = FALSE)
  for (month_dir in months) {
    backup_dir <- file.path(month_dir, "_boundary_fix_backups")
    if (!dir.exists(backup_dir)) next

    backup_files <- list.files(backup_dir, pattern = "\\.txt$", full.names = TRUE)
    for (bf in backup_files) {
      dest <- file.path(month_dir, basename(bf))
      file.copy(bf, dest, overwrite = TRUE)
      reverted <- reverted + 1
    }
    # Remove backup dir after restoring
    unlink(backup_dir, recursive = TRUE)
    cat(sprintf("Reverted %d files from %s/%s\n", length(backup_files),
                basename(vol_dir), basename(month_dir)))
  }
}

cat(sprintf("\nTotal reverted: %d files\n", reverted))
