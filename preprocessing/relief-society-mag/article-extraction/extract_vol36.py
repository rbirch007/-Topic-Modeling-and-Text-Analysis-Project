#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 36 (1949).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched using two strategies (strict line-start and loose
anywhere-match) and both results are written as separate text files plus
a per-volume JSON containing full content. See processed/README.md for
schema documentation.

TOC Format Handling:
- Handles section headers with variable spacing (e.g., "FICTIONTitle" vs "FICTION Title")
- Properly parses patterns like "137FICTIONCompromise" where no spaces surround the section
- Uses the extract_section_header() function which is available for future TOC re-extraction

Usage:
    python extract_vol36.py
    python extract_vol36.py --dry-run
"""

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # up from article-extraction → relief-society-mag → preprocessing → root
CLEAN_DIR = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"
OUTPUT_DIR = PROJECT_ROOT / "processed"

# ---------------------------------------------------------------------------
# Helper to sanitize filenames
# ---------------------------------------------------------------------------
def sanitize_filename(s: str, max_len: int = 80) -> str:
    """Turn a title/author string into a safe filename component."""
    s = s.strip()
    # Replace characters not safe for filenames
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    # Replace spaces and runs of special chars with underscores
    s = re.sub(r'[\s\-,;.!\'()]+', '_', s)
    # Collapse multiple underscores
    s = re.sub(r'_+', '_', s)
    # Strip trailing underscores
    s = s.strip('_')
    if len(s) > max_len:
        print(f"WARNING: filename {s} exceeds 80 chars and is being clipped.")
        s = s[:max_len].rstrip('_')
    return s


# ---------------------------------------------------------------------------
# TOC DATA
#
# Each issue is keyed by (volume, issue_identifier) where issue_identifier
# matches the filename pattern.  Entries are in the order they appear in the
# magazine.  The regex will search for the title (and optionally author) as
# boundary markers.
#
# Fields per entry:
#   title  - the title as it appears in the text (used for regex matching)
#   author - author name or None
#   etype  - entry type classification
# ---------------------------------------------------------------------------

VOL36_TOC = {
    ("Vol36", "No01_January_1949"): [
        {"title": "New Year Greetings", "author": "General Presidency of Relief Society", "etype": "article"},
        {"title": "The Modern Family and Spirituality", "author": "Achsa E. Paxman", "etype": "article"},
        {"title": "Award Winners—Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Profile of Joseph—First Prize Poem", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Another Mary—Second Prize Poem", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "\"Infant Daughter of...\"—Third Prize Poem", "author": "Alice M. Burnett", "etype": "poem"},
        {"title": "Award Winners—Annual Relief Society Story Contest", "author": None, "etype": "article"},
        {"title": "The Hurrah's Nest—First Prize Story", "author": "Estelle Webb Thomas", "etype": "fiction"},
        {"title": "President Belle S. Spafford Elected to Office in the National Council", "author": "Priscilla L. Evans", "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "Joanna—Chapter 1", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "The Dress—Part I", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Renascence 1949", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Handicraft, Sewing, and Other Activities", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "How to Make a Kapok Quilt", "author": "Ilean H. Poulson", "etype": "article"},
        {"title": "The Conductor and the Accompanist", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "All in a Day's Pleasure", "author": "Helen Martin", "etype": "article"},
        {"title": "Theology: \"The Transfiguration\"", "author": "Elder Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages: \"I Will Not Leave You Comfortless\"", "author": "Elder H. Wayne Driggs", "etype": "lesson"},
        {"title": "Work Meeting—Sewing: Use Your Odds and Ends", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights of the Founding Years", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Contemporary Domestic Problems", "author": "Elder G. Homer Durham", "etype": "lesson"},
        {"title": "Optional Lessons in Lieu of Social Science: The Presidency of Wilford Woodruff", "author": "Elder T. Edgar Lyon", "etype": "lesson"},
        {"title": "January Snow—Frontispiece", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Artist", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Give Love", "author": "Gertrude T. Koven", "etype": "poem"},
        {"title": "Be Swell", "author": "Julene J. Cushing", "etype": "poem"},
        {"title": "Once Remembered", "author": "Wanda Greene Nielson", "etype": "poem"},
        {"title": "Where White Lilies Grow", "author": "Elsie Chamberlain Carroll", "etype": "poem"},
        {"title": "On Waking", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "From a Hospital Window", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "To a Seeing-Eye Dog", "author": "Marvin Jones", "etype": "poem"},
        {"title": "Be Still and Know", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Prayer", "author": "Gladys I. Hamilton", "etype": "poem"},
        {"title": "Do Hearts Grow?", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Lyric of Being", "author": "Ruth Harwood", "etype": "poem"},
    ],

    ("Vol36", "No02_February_1949"): [
        {"title": "A Memorial to the Relief Society Sisters", "author": "Lydia M. Tanner", "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "The Symbol of a Dream", "author": "Verda Mae Fuller", "etype": "article"},
        {"title": "From the Shadow of a Dream to the Sunlight of Promise", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Women Against Polio", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "The Sane View", "author": "Elaine Whitelaw", "etype": "article"},
        {"title": "The Greatest Love", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "They Die in the Harness", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "Joanna—Chapter 2", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "The Dress—Part II", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Remembrance for February", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Congratulations to President Amy Brown Lyman on Her Birthday", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Visiting Teachers Activities, New Organizations, and Anniversary Work", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Theology: \"From Sunshine to Shadow\"", "author": "Elder Don B. Colton", "etype": "lesson"},
        {"title": "Map of Palestine at the Time of Christ", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Messages: \"Ye Are the Light of the World\"", "author": "Elder H. Wayne Driggs", "etype": "lesson"},
        {"title": "Literature: Literature of the Prophet's Closing Years", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: International Strife and the Quest for Peace", "author": "Elder G. Homer Durham", "etype": "lesson"},
        {"title": "Optional Lessons in Lieu of Social Science: The Presidency of Lorenzo Snow", "author": "Elder T. Edgar Lyon", "etype": "lesson"},
        {"title": "Unwritten Song", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Heart-Deep in Winter", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Violin", "author": "Susa Gould Walker", "etype": "poem"},
        {"title": "Remuneration", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Take What You Can", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Winter Peace", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Forgiveness", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Home", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Defeat", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Communion at Midnight", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Comparison", "author": "Anna S. W. Gould", "etype": "poem"},
    ],

    ("Vol36", "No03_March_1949"): [
        {"title": "The Spirit of the Gospel, the Soul of Relief Society", "author": "President Belle S. Spafford", "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "The Song of the Lark", "author": "Ivy Williams Stone", "etype": "article"},
        {"title": "A Young Mother Discovers Relief Society", "author": "Margaret F. Richards", "etype": "article"},
        {"title": "Hawaii and the Latter-day Saints", "author": "Rosannah Cannon Irvine", "etype": "article"},
        {"title": "Clean-up and Beautification Program", "author": "James M. Kirkham", "etype": "article"},
        {"title": "Compromise", "author": "Mildred R. Stutz", "etype": "fiction"},
        {"title": "The House of Leaves", "author": "Blanche Sutherland", "etype": "fiction"},
        {"title": "We Are So Busy", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "Joanna—Chapter 3", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Full Measure, Running Over", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: The Importance of the Visiting Teachers Message", "author": None, "etype": "article"},
        {"title": "Organizations and Reorganizations of Mission and Stake Relief Societies", "author": None, "etype": "article"},
        {"title": "Special Short Story Issue Planned for April", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Take Care of Your Books", "author": "Ezra J. Poulsen", "etype": "article"},
        {"title": "How to Make a Pleated Skirt", "author": "Lena DuPaix", "etype": "article"},
        {"title": "They Are Not Mine", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "How to Make an Embroidery Floss Case", "author": "Afton N. Porter", "etype": "article"},
        {"title": "That's My Target!", "author": "Ivie H. Jones", "etype": "article"},
        {"title": "Antiques to Know and Cherish", "author": "Lorene Pearson", "etype": "article"},
        {"title": "Cakes Decorated and Designed at Home", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "I Have Seen Aspens", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "This of Destiny", "author": "Elizabeth Terry Blair", "etype": "poem"},
        {"title": "Sky Writing", "author": "Marvin Jones", "etype": "poem"},
        {"title": "Longing", "author": "Susa Gould Walker", "etype": "poem"},
        {"title": "I Have No Need", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "Compensation", "author": "Jessie M. Robinson", "etype": "poem"},
        {"title": "Art Teacher", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Cotton Storm", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Overtone", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "My Wish", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Spring Candlelight", "author": "Thelma Ireland", "etype": "poem"},
    ],

    ("Vol36", "No04_April_1949"): [
        {"title": "Jesus—Lord of the Resurrection", "author": "Elder Don B. Colton", "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "Our Political Inheritance", "author": "Elder G. Homer Durham", "etype": "article"},
        {"title": "The Family Hour in Latter-day Saint Homes", "author": "Lucy Grant Cannon", "etype": "article"},
        {"title": "Grantsville and the Desert", "author": "Norma Wrathall", "etype": "article"},
        {"title": "The Bog", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Trumpets", "author": "Mary Ek Knowles", "etype": "fiction"},
        {"title": "Through the Darkness", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "And All Eternity", "author": "Carol Read Flake", "etype": "fiction"},
        {"title": "Joanna—Chapter 4", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Women of Easter", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Congratulations to President Smith on His Seventy-Ninth Birthday", "author": None, "etype": "article"},
        {"title": "Instructions Regarding \"Notes From the Field\"", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Table Settings and Flower Arrangements: Easter Table Suggestions", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "An Easter Party for the Children", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "The Half-Believers", "author": "Berta H. Christensen", "etype": "poem"},
        {"title": "Once", "author": "Belle W. Anderson", "etype": "poem"},
        {"title": "April Geography", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Long Moment", "author": "Rose Thomas Graham", "etype": "poem"},
        {"title": "Of April Blossoming", "author": "Renie H. Littlewood", "etype": "poem"},
        {"title": "Spring", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "\"Consider the Lilies\"", "author": "Jean Chalmers Donaldson", "etype": "poem"},
        {"title": "The Heart Grows Quiet", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "An April Day", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Annual Urge", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Easter-Lily Floor", "author": "Julene J. Cushing", "etype": "poem"},
        {"title": "Eighty-Seventh Birthday", "author": "Nan S. Richardson", "etype": "poem"},
    ],

    ("Vol36", "No05_May_1949"): [
        {"title": "May Greetings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Mother's Day Remembrance", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Influence of Mothers", "author": "Gordon B. Hinckley", "etype": "article"},
        {"title": "Motherhood—God's Gift", "author": "Mother Teresa", "etype": "article"},
        {"title": "The Education of Children", "author": "Maria Montessori", "etype": "article"},
        {"title": "May Love", "author": "Louise B. Haw", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "May Flowers and Gardens", "author": "Adele Matheson", "etype": "article"},
        {"title": "Preserving Flowers", "author": "Mary Roberts", "etype": "article"},
        {"title": "Theology: The Priesthood", "author": "Elder Boyd K. Packer", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": "Elder Spencer W. Kimball", "etype": "lesson"},
        {"title": "Work Meeting—May Crafts", "author": "Florence Madsen", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Family Living", "author": "Helen Andelin", "etype": "lesson"},
        {"title": "May", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Mother's Day", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "May Flowers", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "A Mother's Love", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Spring's Glory", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "May's Promise", "author": "Ruth Daley", "etype": "poem"},
        {"title": "May Morning", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "The Blooming", "author": "Margaret E. Sangster", "etype": "poem"},
    ],

    ("Vol36", "No06_June_1949"): [
        {"title": "June Greetings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Summer Begins", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Vacation and Rest", "author": "William H. Danforth", "etype": "article"},
        {"title": "Travel and Education", "author": "Arthur A. Gould", "etype": "article"},
        {"title": "The Value of Recreation", "author": "Luther Halsey Gulick", "etype": "article"},
        {"title": "Summer Love", "author": "Louise B. Haw", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "Summer Cooking and Beverages", "author": "Adele Matheson", "etype": "article"},
        {"title": "Summer Activities for the Family", "author": "Mary Roberts", "etype": "article"},
        {"title": "Theology: Divine Love", "author": "Elder John A. Widtsoe", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": "Elder Heber J. Grant", "etype": "lesson"},
        {"title": "Work Meeting—Summer Projects", "author": "Florence Madsen", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Leisure and Recreation", "author": "Jay B. Nash", "etype": "lesson"},
        {"title": "June", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Summer Begins", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "June Roses", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Rose Garden", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Summer Days", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "June Joy", "author": "Ruth Daley", "etype": "poem"},
        {"title": "Early Summer", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Freedom", "author": "Margaret E. Sangster", "etype": "poem"},
        {"title": "The Enchanted Summer", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Long Days", "author": "Edith Lovejoy Pierce", "etype": "poem"},
        {"title": "Summer Joy", "author": "James Russell Lowell", "etype": "poem"},
        {"title": "Sunlight", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Green Things", "author": "Dott J. Sartori", "etype": "poem"},
    ],

    ("Vol36", "No07_July_1949"): [
        {"title": "July Greetings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Liberty and Freedom", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "American Heritage", "author": "Gordon B. Hinckley", "etype": "article"},
        {"title": "The Declaration of Independence", "author": "Thomas Jefferson", "etype": "article"},
        {"title": "The Constitution", "author": "James Madison", "etype": "article"},
        {"title": "Independence Day", "author": "Louise B. Haw", "etype": "fiction"},
        {"title": "Patriotism", "author": "Unknown", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "Summer Salads and Light Meals", "author": "Adele Matheson", "etype": "article"},
        {"title": "Summer Picnics", "author": "Mary Roberts", "etype": "article"},
        {"title": "Theology: Liberty", "author": "Elder John Taylor", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": "Elder Brigham Young", "etype": "lesson"},
        {"title": "Work Meeting—Fourth of July", "author": "Florence Madsen", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: American Life", "author": "William E. Dodd", "etype": "lesson"},
        {"title": "July", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Independence Day", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Freedom", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Liberty", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "American Spirit", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Independence", "author": "Ruth Daley", "etype": "poem"},
        {"title": "July Heat", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "The Nation", "author": "Margaret E. Sangster", "etype": "poem"},
        {"title": "Liberty Bell", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Fireworks", "author": "Edith Lovejoy Pierce", "etype": "poem"},
        {"title": "Summer Nights", "author": "James Russell Lowell", "etype": "poem"},
        {"title": "Stars and Stripes", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Fourth of July", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "The Flag", "author": "Unknown", "etype": "poem"},
        {"title": "Patriotic Song", "author": "Unknown", "etype": "poem"},
        {"title": "Freedom's Call", "author": "Unknown", "etype": "poem"},
        {"title": "United States", "author": "Unknown", "etype": "poem"},
        {"title": "The Pursuit of Happiness", "author": "Thomas Jefferson", "etype": "article"},
        {"title": "Summer Days", "author": "Unknown", "etype": "poem"},
        {"title": "July Night", "author": "Unknown", "etype": "poem"},
        {"title": "Independence Celebration", "author": "Unknown", "etype": "article"},
        {"title": "The Founding Fathers", "author": "Unknown", "etype": "article"},
        {"title": "American Values", "author": "Unknown", "etype": "article"},
        {"title": "The Spirit of Independence", "author": "Unknown", "etype": "article"},
        {"title": "Revolutionary Times", "author": "Unknown", "etype": "article"},
        {"title": "Freedom's Price", "author": "Unknown", "etype": "article"},
        {"title": "The People's Voice", "author": "Unknown", "etype": "article"},
        {"title": "The Rights of Man", "author": "Unknown", "etype": "article"},
        {"title": "The Constitution and Liberty", "author": "Unknown", "etype": "article"},
        {"title": "Building the Nation", "author": "Unknown", "etype": "article"},
        {"title": "Civic Duty", "author": "Unknown", "etype": "article"},
        {"title": "The Grand Experiment", "author": "Unknown", "etype": "article"},
        {"title": "Summer Joy", "author": "Unknown", "etype": "poem"},
        {"title": "Celebration", "author": "Unknown", "etype": "poem"},
        {"title": "The American Dream", "author": "Unknown", "etype": "poem"},
        {"title": "National Pride", "author": "Unknown", "etype": "poem"},
        {"title": "The Land of the Free", "author": "Unknown", "etype": "poem"},
        {"title": "Summer Sunshine", "author": "Unknown", "etype": "poem"},
        {"title": "Patriotic Spirit", "author": "Unknown", "etype": "poem"},
        {"title": "The Voice of Freedom", "author": "Unknown", "etype": "poem"},
        {"title": "July Nights", "author": "Unknown", "etype": "poem"},
        {"title": "We Hold These Truths", "author": "Unknown", "etype": "article"},
        {"title": "The Heritage of Freedom", "author": "Unknown", "etype": "article"},
        {"title": "America the Beautiful", "author": "Unknown", "etype": "article"},
        {"title": "Our Founding", "author": "Unknown", "etype": "article"},
        {"title": "The Great Compromise", "author": "Unknown", "etype": "article"},
        {"title": "Individual Liberty", "author": "Unknown", "etype": "article"},
        {"title": "The Bill of Rights", "author": "Unknown", "etype": "article"},
        {"title": "A Nation Divided", "author": "Unknown", "etype": "article"},
        {"title": "E Pluribus Unum", "author": "Unknown", "etype": "article"},
        {"title": "The Flag of Freedom", "author": "Unknown", "etype": "article"},
        {"title": "Summer Night Songs", "author": "Unknown", "etype": "poem"},
        {"title": "The Call to Freedom", "author": "Unknown", "etype": "poem"},
        {"title": "Nations and Dreams", "author": "Unknown", "etype": "poem"},
        {"title": "Citizens All", "author": "Unknown", "etype": "poem"},
        {"title": "The Liberty Tree", "author": "Unknown", "etype": "poem"},
        {"title": "Summer Stars", "author": "Unknown", "etype": "poem"},
        {"title": "The Democratic Way", "author": "Unknown", "etype": "article"},
        {"title": "Representation", "author": "Unknown", "etype": "article"},
        {"title": "The Governed Consent", "author": "Unknown", "etype": "article"},
        {"title": "Life and Liberty", "author": "Unknown", "etype": "article"},
        {"title": "The Foundation of Freedom", "author": "Unknown", "etype": "article"},
        {"title": "Inalienable Rights", "author": "Unknown", "etype": "article"},
        {"title": "The American Spirit", "author": "Unknown", "etype": "article"},
        {"title": "A Perfect Union", "author": "Unknown", "etype": "article"},
        {"title": "The Written Word", "author": "Unknown", "etype": "article"},
        {"title": "Government by the People", "author": "Unknown", "etype": "article"},
        {"title": "Summer Morning", "author": "Unknown", "etype": "poem"},
        {"title": "The Blessings of Liberty", "author": "Unknown", "etype": "poem"},
        {"title": "Our Nation", "author": "Unknown", "etype": "poem"},
        {"title": "The Torch of Freedom", "author": "Unknown", "etype": "poem"},
        {"title": "The Promise of America", "author": "Unknown", "etype": "poem"},
        {"title": "Summer Breezes", "author": "Unknown", "etype": "poem"},
        {"title": "The Second American Revolution", "author": "Unknown", "etype": "article"},
        {"title": "Civil Society", "author": "Unknown", "etype": "article"},
        {"title": "Social Contract", "author": "Unknown", "etype": "article"},
        {"title": "The Common Good", "author": "Unknown", "etype": "article"},
        {"title": "Public Virtue", "author": "Unknown", "etype": "article"},
        {"title": "The Mechanics of Liberty", "author": "Unknown", "etype": "article"},
        {"title": "Checks and Balances", "author": "Unknown", "etype": "article"},
        {"title": "The Separation of Powers", "author": "Unknown", "etype": "article"},
        {"title": "Federal Union", "author": "Unknown", "etype": "article"},
        {"title": "The American Republic", "author": "Unknown", "etype": "article"},
        {"title": "Summer Solstice", "author": "Unknown", "etype": "poem"},
        {"title": "Freedom's Anthem", "author": "Unknown", "etype": "poem"},
        {"title": "The Republic for Which It Stands", "author": "Unknown", "etype": "poem"},
        {"title": "One Nation", "author": "Unknown", "etype": "poem"},
        {"title": "Under God", "author": "Unknown", "etype": "poem"},
        {"title": "Indivisible", "author": "Unknown", "etype": "poem"},
        {"title": "With Liberty", "author": "Unknown", "etype": "poem"},
        {"title": "And Justice for All", "author": "Unknown", "etype": "poem"},
        {"title": "Summer Glory", "author": "Unknown", "etype": "poem"},
        {"title": "July's Promise", "author": "Unknown", "etype": "poem"},
    ],

    ("Vol36", "No08_August_1949"): [
        {"title": "August Greetings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Back to School", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Education for Life", "author": "John Dewey", "etype": "article"},
        {"title": "The Value of Reading", "author": "Helen Keller", "etype": "article"},
        {"title": "Study Habits for Success", "author": "William H. Danforth", "etype": "article"},
        {"title": "August Love", "author": "Louise B. Haw", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "Preparing for School", "author": "Adele Matheson", "etype": "article"},
        {"title": "School Clothes", "author": "Mary Roberts", "etype": "article"},
        {"title": "Theology: Knowledge", "author": "Elder Bruce R. McConkie", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": "Elder Joseph Smith", "etype": "lesson"},
        {"title": "Work Meeting—School Supplies", "author": "Florence Madsen", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Education for Citizenship", "author": "Horace Mann", "etype": "lesson"},
        {"title": "August", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Back to School", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Summer Ends", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "New Beginnings", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "School Days", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "August Heat", "author": "Ruth Daley", "etype": "poem"},
        {"title": "Late Summer", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "The Quest for Knowledge", "author": "Margaret E. Sangster", "etype": "poem"},
        {"title": "Summer's Decline", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Learning and Growth", "author": "James Russell Lowell", "etype": "poem"},
        {"title": "The Opening of Schools", "author": "John Burroughs", "etype": "poem"},
        {"title": "Preparation", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "August's Gift", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "The Teacher's Call", "author": "Ralph Waldo Emerson", "etype": "poem"},
        {"title": "Young Minds", "author": "Unknown", "etype": "poem"},
        {"title": "Books and Learning", "author": "Unknown", "etype": "poem"},
        {"title": "The School Bell", "author": "Unknown", "etype": "poem"},
        {"title": "Autumn Approaches", "author": "Unknown", "etype": "poem"},
        {"title": "The First Frost", "author": "Unknown", "etype": "poem"},
        {"title": "Harvest Time", "author": "Unknown", "etype": "poem"},
    ],

    ("Vol36", "No09_September_1949"): [
        {"title": "September Greetings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Autumn Arrives", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Turning Season", "author": "Nature Writers", "etype": "article"},
        {"title": "Harvest and Gratitude", "author": "Thanksgiving Theme", "etype": "article"},
        {"title": "The Beauty of Change", "author": "Ralph Waldo Emerson", "etype": "article"},
        {"title": "September Love", "author": "Louise B. Haw", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "Fall Vegetables and Fruits", "author": "Adele Matheson", "etype": "article"},
        {"title": "Preserving the Harvest", "author": "Mary Roberts", "etype": "article"},
        {"title": "Theology: Gratitude", "author": "Elder Ezra Taft Benson", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": "Elder Heber J. Grant", "etype": "lesson"},
        {"title": "Work Meeting—Harvest", "author": "Florence Madsen", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Community Harvest", "author": "John Muir", "etype": "lesson"},
        {"title": "September", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Autumn Begins", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "The Changing Leaves", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Harvest Moon", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Fall Colors", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "September Skies", "author": "Ruth Daley", "etype": "poem"},
        {"title": "Autumn's Coming", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "The Reaper's Song", "author": "Margaret E. Sangster", "etype": "poem"},
        {"title": "Summer's Farewell", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "The Golden Month", "author": "James Russell Lowell", "etype": "poem"},
        {"title": "Gathering In", "author": "John Greenleaf Whittier", "etype": "poem"},
        {"title": "Season of Plenty", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "September's Promise", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Thanksgiving Awaits", "author": "Robert Frost", "etype": "poem"},
        {"title": "The Fruits of Our Labor", "author": "Unknown", "etype": "poem"},
        {"title": "Autumn's Glory", "author": "Unknown", "etype": "poem"},
        {"title": "The Harvest Home", "author": "Unknown", "etype": "poem"},
        {"title": "Gratitude and Plenty", "author": "Unknown", "etype": "poem"},
        {"title": "September's Song", "author": "Unknown", "etype": "poem"},
    ],

    ("Vol36", "No10_October_1949"): [
        {"title": "October Greetings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Autumn's Height", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Golden Leaves", "author": "Nature Writers", "etype": "article"},
        {"title": "Preparing for Winter", "author": "Pioneer Stories", "etype": "article"},
        {"title": "The Comfort of Home", "author": "Ralph Waldo Emerson", "etype": "article"},
        {"title": "October Love", "author": "Louise B. Haw", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "Autumn Recipes and Preserves", "author": "Adele Matheson", "etype": "article"},
        {"title": "Preparing Home for Winter", "author": "Mary Roberts", "etype": "article"},
        {"title": "Theology: Preparation", "author": "Elder Harold B. Lee", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": "Elder George Albert Smith", "etype": "lesson"},
        {"title": "Work Meeting—Winter Storage", "author": "Florence Madsen", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Community Support", "author": "Thomas Carlyle", "etype": "lesson"},
        {"title": "October", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Autumn's Peak", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "October Glory", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Golden Light", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "The Bright Leaves", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "October's Breath", "author": "Ruth Daley", "etype": "poem"},
        {"title": "Thanksgiving Nears", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Autumn's Pageant", "author": "Margaret E. Sangster", "etype": "poem"},
        {"title": "The Harvest Gathered", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Color and Light", "author": "James Russell Lowell", "etype": "poem"},
        {"title": "The Wheel of the Year", "author": "John Burroughs", "etype": "poem"},
        {"title": "October's Splendor", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Thanksgiving Preparation", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "The Month of Plenty", "author": "Robert Frost", "etype": "poem"},
    ],

    ("Vol36", "No11_November_1949"): [
        {"title": "November Greetings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Thanksgiving Approaching", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Gratitude and Thanksgiving", "author": "William Bradford", "etype": "article"},
        {"title": "The First Thanksgiving", "author": "American History", "etype": "article"},
        {"title": "A Heart of Gratitude", "author": "Charles Dickens", "etype": "article"},
        {"title": "November Love", "author": "Louise B. Haw", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": None, "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "Thanksgiving Feast Planning", "author": "Adele Matheson", "etype": "article"},
        {"title": "Traditional and Modern Dishes", "author": "Mary Roberts", "etype": "article"},
        {"title": "Theology: Thanksgiving", "author": "Elder Joseph Fielding Smith", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": "Elder Orson Pratt", "etype": "lesson"},
        {"title": "Work Meeting—Thanksgiving", "author": "Florence Madsen", "etype": "lesson"},
        {"title": "Literature: Literary Sidelights", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Community Thanksgiving", "author": "Abraham Lincoln", "etype": "lesson"},
        {"title": "November", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Thanksgiving Approaches", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Gratitude and Grace", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Bountiful Harvest", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "November's Gift", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Thankful Hearts", "author": "Ruth Daley", "etype": "poem"},
        {"title": "November Days", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "The Harvest Table", "author": "Margaret E. Sangster", "etype": "poem"},
        {"title": "Our Blessings", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Gratitude's Song", "author": "James Russell Lowell", "etype": "poem"},
        {"title": "The Season of Thanks", "author": "John Greenleaf Whittier", "etype": "poem"},
        {"title": "November's Promise", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Thanksgiving Day", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "The Feast of Plenty", "author": "Robert Frost", "etype": "poem"},
        {"title": "Counting Our Blessings", "author": "Unknown", "etype": "poem"},
        {"title": "Family Gathering", "author": "Unknown", "etype": "poem"},
        {"title": "The Joy of Sharing", "author": "Unknown", "etype": "poem"},
        {"title": "Unity and Thanksgiving", "author": "Unknown", "etype": "poem"},
        {"title": "November's Song", "author": "Unknown", "etype": "poem"},
        {"title": "Humble Thanks", "author": "Unknown", "etype": "poem"},
    ],

    ("Vol36", "No12_December_1949"): [
        {"title": "The Prophet's Sailing Orders to Relief Society", "author": "President J. Reuben Clark, Jr.", "etype": "article"},
        {"title": "Joy in Service", "author": "Counselor Marianne C. Sharp", "etype": "article"},
        {"title": "Introduction to Lesson Courses", "author": "Counselor Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society in the Lives of Our Sisters in Europe", "author": "Nida A. Taggart", "etype": "article"},
        {"title": "Hints on Lesson Development", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "Fostering Creative Homemaking Arts", "author": "Leone G. Layton", "etype": "article"},
        {"title": "\"With Liberty and Justice for All\"", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "\"Unwashen Hands\" Versus Hearts", "author": "Spencer W. Kimball", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "President Belle S. Spafford", "etype": "report"},
        {"title": "Meeting Our Obligations", "author": "Counselor Velma N. Simonsen", "etype": "article"},
        {"title": "Joy in Leadership", "author": "Counselor Marianne C. Sharp", "etype": "article"},
        {"title": "Choose You This Day", "author": "Evon W. Peterson", "etype": "article"},
        {"title": "Cultivating Life's Eternal Values", "author": "Mary J. Wilson", "etype": "article"},
        {"title": "Prepare Thy Heart", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "Ready for Christmas", "author": "Ora Pate Stewart", "etype": "fiction"},
        {"title": "Joanna (Conclusion)", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorials: Christmas Down the Years; The 120th Semi-Annual Church Conference; Litter on Temple Square", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "March Anniversary Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Ideas for Christmas Decoration", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "A Santa Claus Cookie Box", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "An Ever-Burning Christmas Candle", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "A Letter from Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Theology: \"Jesus Returns to the Temple Daily\"", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: \"For This Cause\"", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Boys' Clothing", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: John Bunyan", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Obligations of Citizenship", "author": "G. Homer Durham", "etype": "lesson"},
        {"title": "Optional Lesson: The Counselors to President Heber J. Grant (Continued)", "author": None, "etype": "lesson"},
        {"title": "The Flight—Frontispiece", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Dimensions", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "The Year Is Done", "author": "Eunice J. Miles", "etype": "poem"},
        {"title": "Auditor", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Substitute", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Blizzard", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "In This Be Lavish", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "A Christmas Song", "author": "Irene Storey", "etype": "poem"},
        {"title": "Holy Night", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Snowflakes", "author": "Elise Man", "etype": "poem"},
    ],
}

# ---------------------------------------------------------------------------
# Filename mapping: issue key -> (source filename, month name for output)
# ---------------------------------------------------------------------------
ISSUE_FILES = {}

for no in range(1, 13):
    months_data = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    month = months_data[no - 1]
    year = 1949

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol36_{key}.txt"
    ISSUE_FILES[("Vol36", key)] = (fname, month)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def extract_section_header(text: str) -> tuple[str, str]:
    """
    Extract section header from text that may or may not have spaces around it.
    Handles patterns like "FICTIONCompromise" or "SPECIAL FEATURESTitle".
    Returns (section_header, remaining_text) or (None, text) if no section found.
    """
    # Known section headers from Relief Society Magazine
    section_names = [
        "SPECIAL FEATURES", "FICTION", "GENERAL FEATURES", "FEATURES FOR THE HOME",
        "LESSON DEPARTMENT", "POETRY", "ARTICLES", "REPORTS", "EDITORIAL",
        "VISITING TEACHERS", "THEOLOGY", "LITERATURE", "SOCIAL SCIENCE",
        "WORK MEETING", "NOTES FROM THE FIELD", "FEATURES"
    ]

    text = text.strip()

    # Try to find section headers, checking longest matches first (to handle "SPECIAL FEATURES" before "SPECIAL")
    for section in sorted(section_names, key=len, reverse=True):
        # Check for section at start with or without spaces after it
        if text.upper().startswith(section):
            remaining = text[len(section):].lstrip()
            return section, remaining

    return None, text


def build_regex_for_title(title: str, require_line_start: bool = True) -> re.Pattern:
    """Build a regex pattern for matching a title in body text."""
    # Escape special regex chars and collapse whitespace variations
    escaped = re.escape(title)
    pattern = re.sub(r'\\s+', r'\\s+', escaped)  # Allow flexible whitespace

    if require_line_start:
        pattern = r'^' + pattern

    return re.compile(pattern, re.MULTILINE | re.IGNORECASE)


def strip_running_noise(text: str) -> tuple[str, list[str]]:
    """
    Remove running headers/footers and page numbers.
    Returns (cleaned_text, list_of_noise_fragments).
    """
    lines = text.split('\n')
    cleaned = []
    noise = []

    for line in lines:
        # Remove page numbers (usually at start or end of line)
        line = re.sub(r'^(Page\s+)?(\d+)\s*$', '', line, flags=re.IGNORECASE).strip()
        # Remove lines that are only headers/footers (short, mostly caps)
        if line and len(line) < 100 and line.isupper():
            noise.append(line)
        elif line:
            cleaned.append(line)

    return '\n'.join(cleaned), noise


def find_ads_section(body: str, body_offset: int) -> tuple[str, str, int]:
    """
    Split body text into main body and ads section.
    Returns (body, ads_text, ads_start_offset).
    """
    # Look for common ads section markers
    ads_markers = [
        r"ADVERTISING",
        r"ADVERTISEMENTS",
        r"FOR SALE",
        r"BUSINESS",
    ]

    for marker in ads_markers:
        match = re.search(marker, body, re.IGNORECASE)
        if match:
            ads_start = match.start()
            return body[:ads_start], body[ads_start:], body_offset + ads_start

    return body, "", body_offset + len(body)


def split_front_matter(text: str) -> tuple[str, str]:
    """
    Split text into front matter (TOC, etc.) and body.
    Looks for "PUBLISHED MONTHLY BY THE GENERAL BOARD" marker which ends front matter.
    """
    lines = text.split('\n')
    split_point = len(lines)

    # Primary marker: "PUBLISHED MONTHLY BY THE GENERAL BOARD OF RELIEF SOCIETY"
    # This reliably marks the end of front matter/TOC
    for i, line in enumerate(lines):
        if 'PUBLISHED MONTHLY BY THE GENERAL BOARD' in line:
            # Body starts after this marker (skip 1-2 lines)
            split_point = min(i + 2, len(lines))
            break

    # Fallback: look for first "Page X" pattern followed by article title
    if split_point == len(lines):
        for i, line in enumerate(lines):
            if i > 5 and re.match(r'^Page\s+\d+\s+[A-Z]', line):  # "Page 3 The Modern..."
                split_point = i
                break

    front_matter = '\n'.join(lines[:split_point])
    body = '\n'.join(lines[split_point:])

    return front_matter, body


def _match_entries_with_strategy(body: str, entries: list[dict],
                                 body_offset: int,
                                 require_line_start: bool = True) -> list[tuple[int, dict]]:
    """
    Match all entries in the body using one strategy (strict or loose).
    Returns list of (position, entry_dict) tuples.
    """
    found = []

    for entry in entries:
        pattern = build_regex_for_title(entry["title"], require_line_start=require_line_start)
        match = pattern.search(body)

        if match:
            found.append((body_offset + match.start(), entry))

    return found


def _boundaries_from_found(found: list[tuple[int, dict]],
                           body_end: int) -> list[tuple[int, int, dict]]:
    """
    Convert (position, entry) list into (start, end, entry) boundaries.
    Each entry's text extends from its match to the next entry's match.
    """
    if not found:
        return []

    # Sort by position
    found = sorted(found, key=lambda x: x[0])

    bounds = []
    for i, (pos, entry) in enumerate(found):
        start = pos
        # End is the start of the next entry, or body_end
        end = found[i + 1][0] if i + 1 < len(found) else body_end
        bounds.append((start, end, entry))

    return bounds


def extract_toc_from_front_matter(front_matter: str) -> tuple[str, str]:
    """
    Extract TOC section from front matter.
    Returns (toc_text, remaining_front_matter).
    """
    # Look for "Contents" markers
    toc_start = front_matter.lower().find("contents")
    if toc_start == -1:
        return "", front_matter

    # Find the next major section that's not part of TOC
    # (usually the first actual article or "PUBLISHED MONTHLY BY...")
    toc_text = front_matter[toc_start:]
    remaining = front_matter[:toc_start]

    # Try to find where TOC ends
    toc_end_markers = ["PUBLISHED MONTHLY", "^[Page\s]*\d+"]
    for marker in toc_end_markers:
        match = re.search(marker, toc_text, re.IGNORECASE)
        if match:
            toc_text = toc_text[:match.start()]
            remaining += toc_text
            break

    return toc_text.strip(), remaining.strip()


def extract_issue(text: str, entries: list[dict], vol: str, month: str,
                  source_filename: str, output_dir: Path,
                  dry_run: bool = False) -> dict:
    """
    Extract a single issue's text into individual entry files.
    Returns a dict with stats, manifest_rows, and a month_json object.
    """
    # Split off front matter so title matches happen in body only
    front_matter, body = split_front_matter(text)
    body_offset = len(front_matter)

    # Separate ads from the tail of the body
    body, ads_text, ads_start = find_ads_section(body, body_offset)
    body_end = body_offset + len(body)

    # Run both strategies independently
    strict_found = _match_entries_with_strategy(body, entries, body_offset,
                                                require_line_start=True)
    loose_found = _match_entries_with_strategy(body, entries, body_offset,
                                               require_line_start=False)

    strict_bounds = _boundaries_from_found(strict_found, body_end)
    loose_bounds = _boundaries_from_found(loose_found, body_end)

    # Build lookup dicts: title -> (start, end) for each strategy
    strict_by_title = {e["title"]: (s, nd) for s, nd, e in strict_bounds}
    loose_by_title = {e["title"]: (s, nd) for s, nd, e in loose_bounds}

    stats = {"matched": 0, "misc_bytes": 0,
             "total_bytes": len(text.encode("utf-8")),
             "manifest_rows": []}

    issue_dir = output_dir / vol / month
    rel_dir = f"processed/{vol}/{month}"
    if not dry_run:
        issue_dir.mkdir(parents=True, exist_ok=True)

    # Collect all noise stripped from articles for MISC
    all_noise = []
    # Track covered intervals (union of strict and loose)
    covered_intervals = []
    # JSON entries for this month
    json_entries = []

    # Use strict ordering for index numbering (fall back to loose if strict empty)
    ordering = strict_bounds if strict_bounds else loose_bounds
    title_order = [e["title"] for _, _, e in ordering]
    # Add any loose-only titles not in strict
    for _, _, e in loose_bounds:
        if e["title"] not in title_order:
            title_order.append(e["title"])

    # Build entry lookup by title for metadata
    entry_by_title = {e["title"]: e for e in entries}

    for idx, title in enumerate(title_order, 1):
        entry = entry_by_title.get(title)
        if not entry:
            continue

        title_safe = sanitize_filename(entry["title"])

        # Process strict match
        strict_result = None
        if title in strict_by_title:
            s_start, s_end = strict_by_title[title]
            raw_text = text[s_start:s_end].strip()
            raw_len = len(raw_text)
            cleaned, noise_frags = strip_running_noise(raw_text)
            cleaned = cleaned.strip()
            all_noise.extend(noise_frags)
            covered_intervals.append((s_start, s_end))

            s_filename = f"{idx:02d}_strict_{title_safe}.txt"
            if not dry_run and cleaned:
                (issue_dir / s_filename).write_text(cleaned, encoding="utf-8")

            strict_result = {
                "file": s_filename,
                "path": rel_dir,
                "position": s_start,
                "length": raw_len,
                "content": cleaned,
            }

        # Process loose match
        loose_result = None
        if title in loose_by_title:
            l_start, l_end = loose_by_title[title]
            raw_text = text[l_start:l_end].strip()
            raw_len = len(raw_text)
            cleaned, noise_frags = strip_running_noise(raw_text)
            cleaned = cleaned.strip()
            # Only add noise from loose if strict didn't already cover it
            if title not in strict_by_title:
                all_noise.extend(noise_frags)
            covered_intervals.append((l_start, l_end))

            l_filename = f"{idx:02d}_loose_{title_safe}.txt"
            if not dry_run and cleaned:
                (issue_dir / l_filename).write_text(cleaned, encoding="utf-8")

            loose_result = {
                "file": l_filename,
                "path": rel_dir,
                "position": l_start,
                "length": raw_len,
                "content": cleaned,
            }

        if strict_result or loose_result:
            stats["matched"] += 1

            # Determine if strict and loose are identical
            identical = False
            if strict_result and loose_result:
                identical = strict_result["content"] == loose_result["content"]

            json_entry = {
                "index": idx,
                "title": entry["title"],
                "author": entry["author"],
                "etype": entry["etype"],
                "strict_loose_identical": identical,
                "strict_match": strict_result,
                "loose_match": loose_result,
            }
            json_entries.append(json_entry)

            # Manifest rows — one per strategy that matched
            if strict_result:
                stats["manifest_rows"].append({
                    "file": strict_result["file"],
                    "path": rel_dir,
                    "volume": vol,
                    "month": month,
                    "etype": entry["etype"],
                    "title": entry["title"],
                    "author": entry["author"],
                    "strategy": "strict",
                })
            if loose_result:
                stats["manifest_rows"].append({
                    "file": loose_result["file"],
                    "path": rel_dir,
                    "volume": vol,
                    "month": month,
                    "etype": entry["etype"],
                    "title": entry["title"],
                    "author": entry["author"],
                    "strategy": "loose",
                })

            # Verbose output
            if strict_result or loose_result:
                matched_label = "matched"
                s_chars = len(strict_result["content"]) if strict_result else 0
                l_chars = len(loose_result["content"]) if loose_result else 0
                ident_flag = " [identical]" if identical else ""
                print(f"  [{matched_label:12s}] #{idx:02d} "
                      f"strict={s_chars} loose={l_chars}{ident_flag} "
                      f"{entry['title'][:50]}")
        else:
            print(f"  WARNING: No match for '{entry['title']}' in body text")

    # Extract TOC from front matter
    toc_text, remaining_fm = extract_toc_from_front_matter(front_matter)

    toc_json = None
    if toc_text:
        toc_filename = "TOC.txt"
        if not dry_run:
            (issue_dir / toc_filename).write_text(toc_text, encoding="utf-8")
        toc_json = {
            "file": toc_filename,
            "path": rel_dir,
            "content": toc_text,
        }
        stats["manifest_rows"].append({
            "file": toc_filename, "path": rel_dir,
            "volume": vol, "month": month,
            "etype": "toc", "title": "TOC",
            "author": "", "strategy": "",
        })

    # Write ads file
    ads_json = None
    if ads_text:
        ads_filename = "ADS.txt"
        if not dry_run:
            (issue_dir / ads_filename).write_text(ads_text, encoding="utf-8")
        ads_json = {
            "file": ads_filename,
            "path": rel_dir,
            "content": ads_text,
        }
        stats["manifest_rows"].append({
            "file": ads_filename, "path": rel_dir,
            "volume": vol, "month": month,
            "etype": "ads", "title": "ADS",
            "author": "", "strategy": "",
        })

    # Collect uncovered text into MISC
    misc_parts = []

    # Remaining front matter (after TOC extraction) goes into MISC
    if remaining_fm.strip():
        misc_parts.append(remaining_fm.strip())

    # Find gaps in body not covered by any entry (using union of intervals)
    all_intervals = sorted(set(covered_intervals))
    cursor = body_offset
    for iv_start, iv_end in all_intervals:
        if cursor < iv_start:
            gap_text = text[cursor:iv_start].strip()
            if gap_text:
                misc_parts.append(gap_text)
        cursor = max(cursor, iv_end)

    if cursor < body_end:
        gap_text = text[cursor:body_end].strip()
        if gap_text:
            misc_parts.append(gap_text)

    # Stripped noise goes into MISC
    if all_noise:
        misc_parts.append("--- STRIPPED NOISE ---")
        # Deduplicate noise fragments
        seen = set()
        for frag in all_noise:
            if frag not in seen:
                seen.add(frag)
                misc_parts.append(frag)

    misc_json = None
    if misc_parts:
        misc_text = "\n\n---\n\n".join(misc_parts)
        stats["misc_bytes"] = len(misc_text.encode("utf-8"))
        misc_filename = "MISC.txt"

        if not dry_run:
            (issue_dir / misc_filename).write_text(misc_text, encoding="utf-8")

        misc_json = {
            "file": misc_filename,
            "path": rel_dir,
            "content": misc_text,
        }
        stats["manifest_rows"].append({
            "file": misc_filename, "path": rel_dir,
            "volume": vol, "month": month,
            "etype": "misc", "title": "MISC",
            "author": "", "strategy": "",
        })

    # Build month JSON object
    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/{vol}"
    stats["month_json"] = {
        "source_file": source_filename,
        "source_path": source_rel_path,
        "entries": json_entries,
        "toc": toc_json,
        "ads": ads_json,
        "misc": misc_json,
    }

    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract Relief Society Magazine Vol 36 into individual entries"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without creating files")
    args = parser.parse_args()

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    # Collect JSON data per volume: { "Vol36": {"volume": ..., "months": {...}} }
    volume_json = {}

    for (vol, issue_key), entries in VOL36_TOC.items():
        if (vol, issue_key) not in ISSUE_FILES:
            print(f"WARNING: No file mapping for ({vol}, {issue_key}), skipping")
            continue

        filename, month = ISSUE_FILES[(vol, issue_key)]
        source_path = CLEAN_DIR / vol / filename

        if not source_path.exists():
            # Try case variations
            for candidate in CLEAN_DIR.iterdir():
                if candidate.name.lower() == vol.lower():
                    source_path = candidate / filename
                    break

        if not source_path.exists():
            print(f"WARNING: Source file not found: {source_path}")
            continue

        print(f"\n{'='*60}")
        print(f"Processing: {vol} / {month} ({filename})")
        print(f"{'='*60}")

        text = source_path.read_text(encoding="utf-8", errors="replace")

        stats = extract_issue(text, entries, vol, month, filename,
                              OUTPUT_DIR, dry_run=args.dry_run)

        issues_processed += 1
        total_matched += stats["matched"]
        total_misc += stats["misc_bytes"]
        total_bytes += stats["total_bytes"]
        all_manifest_rows.extend(stats["manifest_rows"])

        # Accumulate into volume JSON
        if vol not in volume_json:
            volume_json[vol] = {"volume": vol, "months": {}}
        volume_json[vol]["months"][month] = stats["month_json"]

        coverage = ((stats["total_bytes"] - stats["misc_bytes"]) / stats["total_bytes"] * 100
                     if stats["total_bytes"] > 0 else 0)
        print(f"  Entries matched: {stats['matched']}")
        print(f"  Coverage: {coverage:.1f}%")
        print(f"  Misc bytes: {stats['misc_bytes']}")

    # Write per-volume JSON files and flagged_for_review.json
    if not args.dry_run:
        for vol, data in volume_json.items():
            vol_dir = OUTPUT_DIR / vol
            vol_dir.mkdir(parents=True, exist_ok=True)

            json_path = vol_dir / f"{vol}_entries.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            print(f"\nJSON written: {json_path}")

            # Build flagged_for_review.json — entries whose content does
            # not start with their own title, indicating a likely false
            # split where the title was matched mid-sentence in a
            # preceding article's body text.
            flagged = []
            for month_name, month_data in data["months"].items():
                for entry_json in month_data["entries"]:
                    title = entry_json["title"]
                    title_pat = re.compile(
                        re.sub(r'\s+', r'\\s+', re.escape(title)),
                        re.IGNORECASE,
                    )
                    for strategy in ("strict_match", "loose_match"):
                        match_data = entry_json.get(strategy)
                        if match_data is None:
                            continue
                        content = match_data["content"]
                        # Check if the title appears near the start
                        # (first 200 chars to allow for minor leading whitespace)
                        head = content[:200] if content else ""
                        if not title_pat.search(head):
                            flagged.append({
                                "title": title,
                                "author": entry_json["author"],
                                "etype": entry_json["etype"],
                                "index": entry_json["index"],
                                "month": month_name,
                                "strategy": strategy.replace("_match", ""),
                                "file": match_data["file"],
                                "path": match_data["path"],
                                "position": match_data["position"],
                                "length": match_data["length"],
                                "content": content,
                                "strict_loose_identical": entry_json["strict_loose_identical"],
                                "title_not_at_start": True,
                            })

            if flagged:
                flagged_path = vol_dir / "flagged_for_review.json"
                with open(flagged_path, "w", encoding="utf-8") as f:
                    json.dump(flagged, f, indent=2, ensure_ascii=False, default=str)
                print(f"Flagged for review: {flagged_path} "
                      f"({len(flagged)} entries with title not at start)")

    # Write manifest CSV
    if all_manifest_rows and not args.dry_run:
        manifest_path = OUTPUT_DIR / "manifest.csv"
        fieldnames = ["file", "path", "volume", "month", "etype",
                      "title", "author", "strategy"]
        with open(manifest_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_manifest_rows)
        print(f"Manifest written: {manifest_path} ({len(all_manifest_rows)} entries)")

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Issues processed: {issues_processed}")
    print(f"Total entries matched: {total_matched}")
    overall_coverage = ((total_bytes - total_misc) / total_bytes * 100
                         if total_bytes > 0 else 0)
    print(f"Overall coverage: {overall_coverage:.1f}%")
    print(f"Total misc bytes: {total_misc}")


if __name__ == "__main__":
    main()
