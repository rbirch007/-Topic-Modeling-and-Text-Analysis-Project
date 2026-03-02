#!/usr/bin/env python3
"""Extract TOC from Vol40 files and print Python dict."""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
VOL_DIR = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth" / "Vol40"

months = ["January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December"]

toc_dict = {}

for month_num, month in enumerate(months, 1):
    filename = f"Vol40_No{month_num:02d}_{month}_1953.txt"
    filepath = VOL_DIR / filename

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    # Find TOC content (first ~60 lines)
    toc_lines = lines[:60]

    entries = []
    current_section = None

    for line in toc_lines:
        line = line.strip()

        # Detect section headers
        if line in ["SPECIAL FEATURES", "FICTION", "GENERAL FEATURES",
                    "FEATURES FOR THE HOME", "FEATURE FOR THE HOME",
                    "LESSON DEPARTMENT", "POETRY"]:
            current_section = line
            continue

        if not line or line.startswith("["):
            continue

        # Parse entries: "Title by Author pagenum" or "Title pagenum"
        # Skip page-number-only lines
        if re.match(r'^\d+$', line):
            continue

        # Extract title and author
        # Pattern: "text by Author pagenum" or "text pagenum"
        match = re.match(r'^(.+?)\s+by\s+(.+?)\s+\d+$', line)
        if match:
            title = match.group(1).strip()
            author = match.group(2).strip()
        else:
            # No author
            match = re.match(r'^(.+?)\s+\d+$', line)
            if match:
                title = match.group(1).strip()
                author = None
            else:
                continue  # Skip lines that don't match

        # Map section to etype
        if current_section == "SPECIAL FEATURES":
            etype = "article"
        elif current_section == "FICTION":
            etype = "fiction"
        elif current_section in ["GENERAL FEATURES"]:
            if title.startswith("Editorial"):
                etype = "editorial"
            else:
                etype = "article"
        elif current_section in ["FEATURES FOR THE HOME", "FEATURE FOR THE HOME"]:
            etype = "article"
        elif current_section == "LESSON DEPARTMENT":
            etype = "lesson"
        elif current_section == "POETRY":
            etype = "poem"
        else:
            etype = "article"

        # Clean title
        title = title.replace(""", '"').replace(""", '"').replace("—", ", ")

        entries.append({
            "title": title,
            "author": author,
            "etype": etype
        })

    key = ("Vol40", f"No{month_num:02d}_{month}_1953")
    toc_dict[key] = entries

# Print the dict
print("VOL40_TOC = {")
for key, entries in toc_dict.items():
    print(f'    {key}: [')
    for entry in entries:
        author_str = f'"{entry["author"]}"' if entry["author"] else "None"
        print(f'        {{"title": "{entry["title"]}", "author": {author_str}, "etype": "{entry["etype"]}"}},')
    print("    ],")
    print()
print("}")
