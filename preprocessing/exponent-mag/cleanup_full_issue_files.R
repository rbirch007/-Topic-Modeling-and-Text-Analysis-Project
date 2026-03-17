# cleanup_full_issue_files.R
# ─────────────────────────────────────────────────────────────────────────────
# Deletes all FULL_ISSUE and FRONT_MATTER .txt files produced by earlier runs
# of split_issues_into_articles.Rmd from the Topicmodel input tree.
#
# HOW TO RUN:
#   1. Open this file in RStudio.
#   2. Read the dry-run output carefully (it lists every file that will go).
#   3. Change dry_run <- FALSE and source the file again to delete.
# ─────────────────────────────────────────────────────────────────────────────

# ── Configuration ─────────────────────────────────────────────────────────────

# Root folder to scan (all sub-folders are included automatically)
root_dir <- normalizePath(
  "C:/Users/birch/OneDrive - George Mason University - O365 Production/Dissertation/textanalysis/Topicmodel/input",
  winslash = "/",
  mustWork = FALSE
)

# TRUE  → list files only, nothing is deleted   (run this first)
# FALSE → actually delete the files             (run after reviewing the list)
dry_run <- TRUE

# ── Sanity check ──────────────────────────────────────────────────────────────
if (!dir.exists(root_dir)) {
  stop("root_dir not found:\n  ", root_dir,
       "\nCheck the path above.")
}

# ── Find matching files ───────────────────────────────────────────────────────
# Matches filenames that end with:
#   _FULL_ISSUE.txt
#   _FRONT_MATTER.txt
#   _FULL_ISSUE_2.txt   (duplicate-slug variants)
#   _FRONT_MATTER_2.txt
pattern <- "_(FULL_ISSUE|FRONT_MATTER)(_\\d+)?\\.txt$"

targets <- list.files(
  root_dir,
  pattern   = pattern,
  recursive = TRUE,
  full.names = TRUE,
  ignore.case = FALSE
)

# ── Report ────────────────────────────────────────────────────────────────────
cat(sprintf("\n=== %s ===\n\n",
            if (dry_run) "DRY RUN — files that WOULD be deleted"
            else         "DELETING files"))

cat("Root scanned:", root_dir, "\n")
cat("Files found :", length(targets), "\n\n")

if (length(targets) == 0L) {
  cat("Nothing to delete. All clean!\n")
  stop("(Stopping early — no files matched.)", call. = FALSE)
}

# Group by parent folder for readable output
parents <- unique(dirname(targets))
for (p in sort(parents)) {
  rel <- sub(paste0(root_dir, "/?"), "", p, fixed = FALSE)
  cat(sprintf("  %s/\n", rel))
  these <- targets[dirname(targets) == p]
  for (f in sort(these)) {
    cat(sprintf("    %s\n", basename(f)))
  }
}

cat(sprintf("\nTotal: %d file(s)\n", length(targets)))

# ── Delete ────────────────────────────────────────────────────────────────────
if (dry_run) {
  cat("\n[DRY RUN] Nothing was deleted.\n")
  cat("Set  dry_run <- FALSE  and source this file again to delete.\n")
} else {
  cat("\nDeleting...\n")
  deleted <- 0L
  failed  <- character(0)

  for (f in targets) {
    ok <- tryCatch(file.remove(f), error = function(e) FALSE)
    if (isTRUE(ok)) {
      deleted <- deleted + 1L
    } else {
      failed <- c(failed, f)
      cat("  FAILED:", f, "\n")
    }
  }

  cat(sprintf("\nDone — deleted %d of %d file(s).\n", deleted, length(targets)))

  if (length(failed) > 0L) {
    cat("\nFiles that could not be deleted (check if open in another program):\n")
    cat(paste0("  ", failed, "\n"), sep = "")
  }
}
