# ============================================================
# CLEANUP — Remove old files that were superseded by re-extraction
# Only removes files that have backups in _reextract_backups/
# ============================================================

BASE <- "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Articleextractionrfiles/OutputExtractedarticles"

vols <- list.dirs(BASE, recursive = FALSE)
total_removed <- 0

for (vol_dir in vols) {
  months <- list.dirs(vol_dir, recursive = FALSE)
  for (month_dir in months) {
    backup_dir <- file.path(month_dir, "_reextract_backups")
    if (!dir.exists(backup_dir)) next

    backed_up <- list.files(backup_dir, pattern = "\\.txt$")
    removed <- 0

    for (fname in backed_up) {
      target <- file.path(month_dir, fname)
      if (file.exists(target)) {
        file.remove(target)
        removed <- removed + 1
      }
    }

    if (removed > 0) {
      cat(sprintf("  %s/%s: removed %d old files\n",
                  basename(vol_dir), basename(month_dir), removed))
      total_removed <- total_removed + removed
    }
  }
}

cat(sprintf("\nTotal removed: %d old files\n", total_removed))
