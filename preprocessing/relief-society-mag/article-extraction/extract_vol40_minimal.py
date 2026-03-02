#!/usr/bin/env python3
"""Minimal test for Vol40 - just January"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

# Import everything from vol39
from extract_vol39 import *

# Override with Vol40 data
VOL40_TOC = {
    ("Vol40", "No01_January_1953"): [
        {"title": "New Snow", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "New Years Greetings", "author": None, "etype": "article"},
        {"title": "Testimony Through Reading The Book of Mormon", "author": "Matthew Cowley", "etype": "article"},
        {"title": "Elder John A. Widtsoe", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Award Winners, Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "No Barren Bough", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Goodbye to Her Son", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "The Greater Love", "author": "Eleanor W. Schow", "etype": "poem"},
        {"title": "Biographical Sketches of Award Winners", "author": None, "etype": "article"},
        {"title": "Award Winners, Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Forever After", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "We Must Unite to Conquer Polio", "author": "Basil O'Connor", "etype": "article"},
        {"title": "The Value and Use of Audio-Visual Teaching Aids in Relief Society", "author": "Leone G. Layton", "etype": "article"},
        {"title": "The Cleruet", "author": "Mary C. Martineau", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 7", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Gifts of Prayer", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Elder Ezra Taft Benson Appointed Secretary of Agriculture", "author": None, "etype": "article"},
        {"title": "Irene Burch Sutton, Woman of Many Hobbies", "author": None, "etype": "article"},
        {"title": "Theology: Nephi's Predictions, Promises, and Instructions", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For They Who Are Not for Me Are Against Me", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Kinds of Income", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Jane Austen", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Kingdom of God", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Christmas", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Beyond Confining Bounds", "author": "Elena Hassell Stanley", "etype": "poem"},
        {"title": "Reflection", "author": "Mirla Greenwood Thayne", "etype": "poem"},
        {"title": "Twelfth Night", "author": "Margaret Evelyn Singleton", "etype": "poem"},
    ],
}

# Update globals
ISSUE_FILES = {("Vol40", "No01_January_1953"): ("Vol40_No01_January_1953.txt", "January")}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    OUTPUT_DIR = PROJECT_ROOT / "processed" / "regex_and_llm"
    data_dir = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"

    for (vol, issue_key), entries in VOL40_TOC.items():
        filename, month = ISSUE_FILES[(vol, issue_key)]
        source_path = data_dir / vol / filename

        print(f"Processing: {vol} / {month}")
        text = source_path.read_text(encoding="utf-8", errors="replace")

        stats = extract_issue(text, entries, "vol40", month, filename,
                              OUTPUT_DIR, dry_run=args.dry_run)

        print(f"Matched: {stats['matched']}")
        coverage = ((stats['total_bytes'] - stats['misc_bytes']) / stats['total_bytes'] * 100)
        print(f"Coverage: {coverage:.1f}%")
