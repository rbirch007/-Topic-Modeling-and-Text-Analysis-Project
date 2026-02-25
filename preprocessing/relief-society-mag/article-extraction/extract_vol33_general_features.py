#!/usr/bin/env python3
"""
Specialized extraction for Vol33 GENERAL FEATURES entries.

Problem: General Features entries only matched in TOC section, not in article body.
Solution: When finding a TOC entry with page number, use that page as anchor to find actual content.

Strategy:
1. Find TOC entry for title (may be in TOC section)
2. Extract page number from TOC entry
3. Search for title AFTER that page number in actual body
4. Extract content from there using page boundaries
"""

import json
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CLEAN_DIR = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"
OUTPUT_DIR = PROJECT_ROOT / "processed"

# Vol33 General Features TOC entries
GF_ENTRIES = [
    {"month": "January", "title": "Sixty Years Ago", "page": 36},
    {"month": "January", "title": "Woman's Sphere", "author": "Ramona W. Cannon", "page": 37},
    {"month": "January", "title": "Editorials", "author": "Marianne C. Sharp", "page": 38},
    {"month": "February", "title": "Sixty Years Ago", "page": 106},
    {"month": "February", "title": "Woman's Sphere", "author": "Ramona W. Cannon", "page": 107},
    {"month": "March", "title": "Sixty Years Ago", "page": 168},
    {"month": "March", "title": "Woman's Sphere", "author": "Ramona W. Cannon", "page": 169},
    {"month": "April", "title": "Sixty Years Ago", "page": 248},
    {"month": "April", "title": "Woman's Sphere", "author": "Ramona W. Cannon", "page": 249},
]

def find_content_after_page(text: str, title: str, start_page: int) -> str:
    """Find article content starting from given page number."""

    # Find the page number marker in text
    page_pattern = rf'\b{start_page}\b'
    page_match = re.search(page_pattern, text)

    if not page_match:
        return ""

    # Search for title after this page marker
    search_start = page_match.end()
    remaining = text[search_start:]

    # Build loose title pattern (allow optional descriptors)
    title_clean = re.sub(r'(Editorials?|Chapter\s+\d+|Conclusion)', '', title, flags=re.IGNORECASE)
    title_clean = title_clean.strip()

    # Search for title with flexible spacing
    title_pattern = re.escape(title_clean).replace(r'\ ', r'\s+')
    title_match = re.search(title_pattern, remaining, re.IGNORECASE)

    if not title_match:
        return ""

    # Extract content from title to next page number
    content_start = title_match.start()
    content_rest = remaining[content_start:]

    # Find next page number (3-4 digits)
    next_page = re.search(r'\b(\d{3,4})\b', content_rest[len(title_clean):])
    if next_page:
        content_end = next_page.start() + len(title_clean)
    else:
        content_end = len(content_rest)

    content = content_rest[:content_end].strip()

    # Remove page number from end
    content = re.sub(r'\b(\d{3,4})\b\s*$', '', content)

    return content.strip()


def sanitize_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    s = re.sub(r'[\s\-,;.!\'()]+', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')[:80]


# Process each month
output_file = OUTPUT_DIR / "regex_and_llm" / "vol33" / "vol33_entries.json"

with open(output_file, 'r') as f:
    data = json.load(f)

print("="*80)
print("EXTRACTING GENERAL FEATURES")
print("="*80)

extracted_count = 0

for entry_info in GF_ENTRIES:
    month = entry_info["month"]
    title = entry_info["title"]
    page = entry_info["page"]

    # Load source file
    fname = f"vol33_No{list(range(1, 13))[['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'].index(month)]}_" + month + "_1946.txt"

    source_file = CLEAN_DIR / "vol33" / fname
    if not source_file.exists():
        print(f"✗ {title} ({month}): File not found")
        continue

    with open(source_file, 'r', encoding='utf-8', errors='ignore') as f:
        text = f.read()

    # Extract content
    content = find_content_after_page(text, title, page)

    if content and len(content) > 50:
        print(f"✓ {title:30s} ({month:10s}): {len(content):5d} chars")
        extracted_count += 1

        # Update the entry in the JSON
        if month in data['months']:
            month_data = data['months'][month]
            for entry in month_data.get('entries', []):
                if entry.get('title') == title:
                    entry['strict_match']['content'] = content
                    entry['loose_match']['content'] = content
                    break
    else:
        print(f"✗ {title:30s} ({month:10s}): No content found")

# Write updated JSON
with open(output_file, 'w') as f:
    json.dump(data, f, indent=2)

print(f"\n✓ Extracted {extracted_count}/{len(GF_ENTRIES)} General Features")
print(f"✓ Updated: {output_file}")

PYTHON
