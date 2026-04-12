#!/usr/bin/env python3
"""Count only newly created poem files (from our extraction passes)."""
import os, re
from collections import defaultdict

BASE = r"C:\Users\birch\OneDrive - George Mason University - O365 Production\Dissertation\textanalysis\Articleextractionrfiles"
OUTPUT_BASE = os.path.join(BASE, "OutputExtractedarticles")
REPORT_FILE = os.path.join(BASE, "misc_poem_extraction_report.txt")
OUTPUT_REPORT = os.path.join(BASE, "pdf_poem_extraction_report_vol30_40.txt")

VOL_YEARS = {
    30: 1943, 31: 1944, 32: 1945, 33: 1946, 34: 1947,
    35: 1948, 36: 1949, 37: 1950, 38: 1951, 39: 1952, 40: 1953
}
MONTHS = ["January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December"]

# Read the original report to find which files existed BEFORE
pre_existing = set()
with open(REPORT_FILE, 'r', encoding='utf-8') as f:
    for line in f:
        m = re.search(r'\[EXISTS\].*->\s+(.+\.txt)', line)
        if m:
            pre_existing.add(m.group(1).strip())
        m = re.search(r'\[CREATED\]\s+(.+\.txt)', line)
        if m:
            pre_existing.add(m.group(1).strip())

# Now count poem files that are NEW (not in pre_existing)
new_files = defaultdict(list)
all_new = 0

for vol in range(30, 41):
    year = VOL_YEARS[vol]
    vol_dir = os.path.join(OUTPUT_BASE, f"Vol{vol}_{year}")
    if not os.path.exists(vol_dir):
        continue

    for month in MONTHS:
        month_dir = os.path.join(vol_dir, month)
        if not os.path.exists(month_dir):
            continue

        for f in sorted(os.listdir(month_dir)):
            if '_Poem_' in f and f.endswith('.txt'):
                if f not in pre_existing:
                    # Also check it has our header
                    fp = os.path.join(month_dir, f)
                    try:
                        with open(fp, 'r', encoding='utf-8', errors='replace') as fh:
                            first = fh.readline()
                        if first.startswith('[Vol') and 'Sequence:' in first:
                            new_files[vol].append((month, f))
                            all_new += 1
                    except:
                        pass

print(f"Total NEW poem files created from PDFs: {all_new}")
print()

# Write proper report
with open(OUTPUT_REPORT, 'w', encoding='utf-8') as out:
    out.write("=" * 70 + '\n')
    out.write("PDF POEM EXTRACTION REPORT - Volumes 30-40\n")
    out.write("=" * 70 + '\n\n')
    out.write(f"Original missing poems listed in TOC: 306\n")
    out.write(f"Non-poem items (ads/cover descriptions): ~8\n")
    out.write(f"Total NEW poem files extracted from PDFs: {all_new}\n")
    out.write(f"Poems not extractable (OCR quality): 4\n\n")

    out.write("Unextractable poems (OCR too poor or title not in PDF text):\n")
    out.write("  1. Vol31 July: 'I-30' by Dott J. Sartori\n")
    out.write("  2. Vol37 August: 'Promontory' by Maude O. Cook\n")
    out.write("  3. Vol37 August: 'Chalk Cliff' by Ruth Harwood\n")
    out.write("  4. Vol40 November: 'Woman-Power' by Emily Carey Alleman\n\n")

    out.write("-" * 70 + '\n')
    out.write("EXTRACTION COUNTS BY VOLUME\n")
    out.write("-" * 70 + '\n')

    for vol in range(30, 41):
        year = VOL_YEARS[vol]
        files = new_files.get(vol, [])
        count = len(files)
        print(f"  Vol{vol} ({year}): {count} poems extracted from PDF")
        out.write(f"\n  Vol{vol} ({year}): {count} poems extracted\n")

        by_month = defaultdict(list)
        for month, fn in files:
            by_month[month].append(fn)

        for month in MONTHS:
            if month in by_month:
                out.write(f"    {month}: {len(by_month[month])}\n")
                for fn in sorted(by_month[month]):
                    out.write(f"      {fn}\n")

    out.write("\n" + "=" * 70 + '\n')

print(f"\nReport: {OUTPUT_REPORT}")
