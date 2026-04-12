#!/usr/bin/env python3
"""Write the final consolidated extraction report."""

import os
import re
from collections import defaultdict

BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles"
OUTPUT_BASE = os.path.join(BASE, "OutputExtractedarticles")
OUTPUT_REPORT = os.path.join(BASE, "pdf_poem_extraction_report_vol30_40.txt")

VOL_YEARS = {
    30: 1943, 31: 1944, 32: 1945, 33: 1946, 34: 1947,
    35: 1948, 36: 1949, 37: 1950, 38: 1951, 39: 1952, 40: 1953
}

MONTHS_ORDER = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"]

def count_poem_files():
    """Count all _Poem_ files created across all passes by scanning output directories."""
    vol_counts = defaultdict(lambda: defaultdict(int))
    all_files = []

    for vol in range(30, 41):
        year = VOL_YEARS[vol]
        vol_dir = os.path.join(OUTPUT_BASE, f"Vol{vol}_{year}")
        if not os.path.exists(vol_dir):
            continue

        for month in MONTHS_ORDER:
            month_dir = os.path.join(vol_dir, month)
            if not os.path.exists(month_dir):
                continue

            for f in os.listdir(month_dir):
                # Count files with _Poem_ in name and sequence >= 15
                # (to avoid counting pre-existing poem files)
                m = re.match(rf'^{month}_Vol{vol}_(\d+)_Poem_', f)
                if m:
                    seq = int(m.group(1))
                    # Files created by our scripts have higher sequence numbers
                    # Let's count ALL poem files to be accurate
                    all_files.append((vol, month, f, seq))
                    vol_counts[vol]['total'] += 1

    return vol_counts, all_files


def main():
    vol_counts, all_files = count_poem_files()

    # Count files we created (sequences >= 15 in most months, but varies)
    # Better: count the files from the extraction runs by checking creation pattern
    # Actually, let's just count files created by reading the interim report

    # Read the existing report to get pass-by-pass counts
    with open(OUTPUT_REPORT, 'r', encoding='utf-8') as f:
        existing = f.read()

    # Now rewrite the report completely
    # Count extracted files per volume across ALL poem files created by our scripts
    # Our scripts created files with specific patterns
    new_poem_files = defaultdict(list)
    for vol, month, filename, seq in all_files:
        # Check if file has our header format
        filepath = os.path.join(OUTPUT_BASE, f"Vol{vol}_{VOL_YEARS[vol]}", month, filename)
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                first_line = f.readline()
            # Our files start with [VolN | Month Year | Sequence: ...
            if first_line.startswith('[Vol') and 'Sequence:' in first_line:
                new_poem_files[vol].append((month, filename))
        except:
            pass

    # Non-poem items that were in the 306 count
    non_poems = [
        "40th ANNIVERSARY Parade of Progress",
        "ZIONS COOPERATIVE MERCANTILE INSTITUTION",
        "ZCMI Willard Luce GATEWAY",
        "MADONNA GRANDUCA A Perry Picture",
        "THE COVER: Tulips, Photograph",
        "Your Subscription EXPIRES With This Issue",
        "Post-office Address",
        "ZCMI T. J. Howells",
    ]

    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + '\n')
        f.write("PDF POEM EXTRACTION REPORT - Volumes 30-40\n")
        f.write("CONSOLIDATED FINAL REPORT\n")
        f.write("=" * 70 + '\n\n')

        total_created = sum(len(v) for v in new_poem_files.values())

        f.write(f"Original missing poems from TOC: 306\n")
        f.write(f"Non-poem items in count (ads, cover descriptions): ~{len(non_poems)}\n")
        f.write(f"Actual poems to extract: ~{306 - len(non_poems)}\n")
        f.write(f"Total poem files created from PDFs: {total_created}\n")
        f.write(f"Poems not found in PDF (OCR issues): 4\n\n")

        f.write("Not found (OCR quality too poor):\n")
        f.write("  1. Vol31 July: 'I-30' by Dott J. Sartori\n")
        f.write("  2. Vol37 August: 'Promontory' by Maude O. Cook\n")
        f.write("  3. Vol37 August: 'Chalk Cliff' by Ruth Harwood\n")
        f.write("  4. Vol40 November: 'Woman-Power' by Emily Carey Alleman\n\n")

        f.write("-" * 70 + '\n')
        f.write("EXTRACTION COUNTS BY VOLUME\n")
        f.write("-" * 70 + '\n')

        for vol in range(30, 41):
            year = VOL_YEARS[vol]
            files = new_poem_files.get(vol, [])
            f.write(f"  Vol{vol} ({year}): {len(files)} poems extracted\n")

            # List by month
            by_month = defaultdict(list)
            for month, fn in files:
                by_month[month].append(fn)

            for month in MONTHS_ORDER:
                if month in by_month:
                    f.write(f"    {month}: {len(by_month[month])} files\n")
                    for fn in sorted(by_month[month]):
                        f.write(f"      {fn}\n")

        f.write("\n" + "=" * 70 + '\n')
        f.write("COMPLETE FILE LISTING\n")
        f.write("=" * 70 + '\n\n')

        for vol in range(30, 41):
            year = VOL_YEARS[vol]
            files = new_poem_files.get(vol, [])
            if files:
                f.write(f"\nVol{vol} ({year}) - {len(files)} files:\n")
                for month, fn in sorted(files, key=lambda x: (MONTHS_ORDER.index(x[0]) if x[0] in MONTHS_ORDER else 99, x[1])):
                    f.write(f"  {fn}\n")

    print(f"Report written to: {OUTPUT_REPORT}")
    print(f"\nFINAL TOTALS:")
    print(f"  Total poem files created: {total_created}")
    for vol in range(30, 41):
        files = new_poem_files.get(vol, [])
        print(f"  Vol{vol} ({VOL_YEARS[vol]}): {len(files)} poems")

if __name__ == '__main__':
    main()
