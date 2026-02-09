#!/usr/bin/env python3
"""
Reset failed entries in progress.json so they can be retried.

This script:
1. Reads progress.json
2. Removes all entries from the "failed" dict
3. Saves the updated progress.json

The "completed" entries are preserved, so successfully processed files
won't be reprocessed.
"""

import json
from pathlib import Path

PROGRESS_FILE = Path(__file__).resolve().parent / "progress.json"

def main():
    if not PROGRESS_FILE.exists():
        print("No progress.json file found.")
        return

    with open(PROGRESS_FILE, "r") as f:
        progress = json.load(f)

    failed_count = len(progress.get("failed", {}))

    if failed_count == 0:
        print("No failed entries to clear.")
        return

    print(f"Found {failed_count} failed entries:")
    for file in progress["failed"].keys():
        print(f"  - {file}")

    print()
    response = input(f"Clear all {failed_count} failed entries? (yes/no): ").strip().lower()

    if response in ["yes", "y"]:
        progress["failed"] = {}

        with open(PROGRESS_FILE, "w") as f:
            json.dump(progress, f, indent=2)

        print(f"✓ Cleared {failed_count} failed entries from progress.json")
        print(f"✓ Keeping {len(progress['completed'])} completed entries")
        print()
        print("You can now run: python ocr_cleanup.py")
    else:
        print("Cancelled.")

if __name__ == "__main__":
    main()
