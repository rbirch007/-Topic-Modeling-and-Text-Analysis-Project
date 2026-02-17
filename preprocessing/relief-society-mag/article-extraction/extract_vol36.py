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
        {"title": "Infant Daughter of...—Third Prize Poem", "author": "Alice M. Burnett", "etype": "poem"},
        {"title": "Award Winners—Annual Relief Society Story Contest", "author": None, "etype": "article"},
        {"title": "The Hurrah's Nest—First Prize Story", "author": "Estelle Webb Thomas", "etype": "fiction"},
        {"title": "President Belle S. Spafford Elected to Office in the National Council", "author": "Priscilla L. Evans", "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "Joanna—Chapter 1", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "The Dress—Part I", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Renascence 1949", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Handicraft, Sewing, and Other Activities", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Quick and Easy Dinners", "author": "Sara Mills", "etype": "article"},
        {"title": "How to Make a Kapok Quilt", "author": "Ilean H. Poulson", "etype": "article"},
        {"title": "The Conductor and the Accompanist", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "All in a Day's Pleasure", "author": "Helen Martin", "etype": "article"},
        {"title": "The Transfiguration", "author": "Elder Don B. Colton", "etype": "lesson"},
        {"title": "I Will Not Leave You Comfortless", "author": "Elder H. Wayne Driggs", "etype": "lesson"},
        {"title": "Work Meeting—Sewing: Use Your Odds and Ends", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literary Sidelights of the Founding Years", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "Contemporary Domestic Problems", "author": "Elder G. Homer Durham", "etype": "lesson"},
        {"title": "The Presidency of Wilford Woodruff", "author": "Elder T. Edgar Lyon", "etype": "lesson"},
        {"title": "January Snow", "author": "Eva Willes Wangsgaard", "etype": "poem"},
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
        {"title": "They Die in the Harness—Second Prize Story", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "Joanna—Chapter 2", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "The Dress—Part II", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Remembrance for February", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Congratulations to President Amy Brown Lyman on Her Birthday", "author": None, "etype": "article"},
        {"title": "Visiting Teachers Activities, New Organizations, and Anniversary Work", "author": "General Secretary-Treasurer, Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "From Sunshine to Shadow", "author": "Elder Don B. Colton", "etype": "lesson"},
        {"title": "Map of Palestine at the Time of Christ", "author": None, "etype": "lesson"},
        {"title": "Ye Are the Light of the World", "author": "Elder H. Wayne Driggs", "etype": "lesson"},
        {"title": "Literature of the Prophet's Closing Years", "author": "Elder Howard R. Driggs", "etype": "lesson"},
        {"title": "International Strife and the Quest for Peace", "author": "Elder G. Homer Durham", "etype": "lesson"},
        {"title": "The Presidency of Lorenzo Snow", "author": "Elder T. Edgar Lyon", "etype": "lesson"},
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
        {"title": "Compromise—Third Prize Story", "author": "Mildred R. Stutz", "etype": "fiction"},
        {"title": "The House of Leaves", "author": "Blanche Sutherland", "etype": "fiction"},
        {"title": "We Are So Busy", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "Joanna—Chapter 3", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Full Measure, Running Over", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Importance of the Visiting Teachers Message", "author": None, "etype": "article"},
        {"title": "Organizations and Reorganizations of Mission and Stake Relief Societies", "author": None, "etype": "article"},
        {"title": "Special Short Story Issue Planned for April", "author": None, "etype": "article"},
        {"title": "Relief Society Socials, Bazaars, and Other Activities", "author": "General Secretary-Treasurer, Margaret C. Pickering", "etype": "article"},
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
        {"title": "The Women of Easter", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Congratulations to President Smith on His Seventy-Ninth Birthday", "author": None, "etype": "article"},
        {"title": "Instructions Regarding Notes From the Field", "author": None, "etype": "article"},
        {"title": "Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Easter Table Suggestions", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "An Easter Party for the Children", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "The Half-Believers", "author": "Berta H. Christensen", "etype": "poem"},
        {"title": "Once", "author": "Belle W. Anderson", "etype": "poem"},
        {"title": "April Geography", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Long Moment", "author": "Rose Thomas Graham", "etype": "poem"},
        {"title": "Of April Blossoming", "author": "Renie H. Littlewood", "etype": "poem"},
        {"title": "Spring", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Consider the Lilies", "author": "Jean Chalmers Donaldson", "etype": "poem"},
        {"title": "The Heart Grows Quiet", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "An April Day", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Annual Urge", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Easter-Lily Floor", "author": "Julene J. Cushing", "etype": "poem"},
        {"title": "Eighty-Seventh Birthday", "author": "Nan S. Richardson", "etype": "poem"},
    ],

    ("Vol36", "No05_May_1949"): [
        {"title": "Our Responsibility to Inactive Members", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Civil Liberty Today", "author": "G. Homer Durham", "etype": "article"},
        {"title": "American Cancer Society Volunteers", "author": "Bertha Hare Long", "etype": "article"},
        {"title": "Planning the Work Meetings", "author": "Leone G. Layton", "etype": "article"},
        {"title": "A Pattern for Christine", "author": "Dorothy J. Roberts", "etype": "fiction"},
        {"title": "You Might Have Waited", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Joanna—Chapter 5", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Not in the Abundance of the Things Which He Possesseth", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Magazine Subscriptions for 1948", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Magazine Honor Roll for 1948", "author": None, "etype": "article"},
        {"title": "Relief Society Bazaars, Socials, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Luncheon Cloths for the Making", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Braided Rugs—New-Fashioned", "author": "Drucilla S. Howard", "etype": "article"},
        {"title": "Entertaining the Bride", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "Handle With Gloves", "author": "Ivie H. Jones", "etype": "article"},
        {"title": "This Happy Land", "author": "Nyal W. Anderson", "etype": "poem"},
        {"title": "Mothers of Destiny", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "May Day", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Pansy Faces", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Formula", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Released to Spring", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Midas and the Five Senses", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Tears First Dried", "author": "Jeanette P. Parry", "etype": "poem"},
        {"title": "Spring Planting", "author": "LeRoy Burke Meagher", "etype": "poem"},
        {"title": "Dawn", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Rainbow Land", "author": "Marvin Jones", "etype": "poem"},
    ],

    ("Vol36", "No06_June_1949"): [
        {"title": "A Measuring Rod for Growth", "author": "Blanche B. Stoddard", "etype": "article"},
        {"title": "Contest Announcements—1949", "author": None, "etype": "article"},
        {"title": "Points for Poets to Remember", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "On Writing a Short Story", "author": "Fay Tarlock", "etype": "article"},
        {"title": "El Paso and the Latter-day Saints", "author": "Sadie Ollorton Clark", "etype": "article"},
        {"title": "Orchid Hunting in Honduras", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Young Hearts Are Fragile", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "A Family's a Family", "author": "Estelle Webb Thomas", "etype": "fiction"},
        {"title": "Joanna—Chapter 6", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The One Hundred Nineteenth Annual General Conference", "author": "Leone G. Layton", "etype": "article"},
        {"title": "Mary Ah Ping Ty", "author": None, "etype": "article"},
        {"title": "Deseret Sunday School Union Centennial Conference", "author": None, "etype": "article"},
        {"title": "Corrections in Magazine Honor Roll", "author": None, "etype": "article"},
        {"title": "Anniversary Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Pleasure From Simple Things", "author": "A. J. Neff", "etype": "article"},
        {"title": "Pressed Flower Pictures—a 'Designing' Hobby", "author": None, "etype": "article"},
        {"title": "Inner Resources", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Rescue That Little Rocker", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Carrot Marmalade", "author": "Frances Kolarik", "etype": "article"},
        {"title": "Sweet Memory", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Temple Shadows", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Roses Wet With Rain", "author": "Grace Sayre", "etype": "poem"},
        {"title": "By Memory", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "A Song the Heart Must Sing", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Impressions", "author": "Elise B. Maness", "etype": "poem"},
        {"title": "June", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Prayer", "author": "Gene Romolo", "etype": "poem"},
        {"title": "My Mountain", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "If", "author": "Catherine Renstrom", "etype": "poem"},
        {"title": "Selective Memory", "author": "Clarence Edwin Flynn", "etype": "poem"},
        {"title": "Of Silver Laughter", "author": "Renie H. Littlewood", "etype": "poem"},
        {"title": "Margery", "author": "Katherine F. Larsen", "etype": "poem"},
    ],

    ("Vol36", "No07_July_1949"): [
        {"title": "For Those Who Come After", "author": "Anna Prince Redd", "etype": "article"},
        {"title": "A Tribute to the Pioneer Mother", "author": "Lucy Fryer Vance", "etype": "article"},
        {"title": "Nellie Ward Neal Appointed to the Relief Society General Board", "author": "Roxey Robson Heslop", "etype": "article"},
        {"title": "The Land of the High Uintahs", "author": "Olive W. Burt", "etype": "article"},
        {"title": "Suggestions for a Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "Mama and Papa", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Joanna—Chapter 7", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The New Frontier", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Birthday Greetings to Sister Augusta Winters Grant", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Table Settings for the Canyon", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "Louisa Allen Thomas Makes a Pioneer Quilt", "author": None, "etype": "article"},
        {"title": "Let's Vary the Picnic Menu", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Make Use of the Rocks", "author": "Ezra J. Poulsen", "etype": "article"},
        {"title": "Salads of Infinite Variety", "author": "Sara Mills", "etype": "article"},
        {"title": "Notes on Authors of the Lessons", "author": None, "etype": "lesson"},
        {"title": "The Life and Ministry of the Savior", "author": None, "etype": "lesson"},
        {"title": "Jesus Began in Galilee", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Our Savior Speaks, Preview", "author": None, "etype": "lesson"},
        {"title": "Work Meeting—Sewing Children's Clothing, Preview", "author": None, "etype": "lesson"},
        {"title": "Proper Selection of Style, Choice of Material and Pattern", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "The Literature of England, Preview", "author": None, "etype": "lesson"},
        {"title": "Introduction", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "The Progress of Man, Preview", "author": None, "etype": "lesson"},
        {"title": "President Joseph F. Smith", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Fundamentals of Musicianship, Preview", "author": None, "etype": "lesson"},
        {"title": "The First Presidencies, Preview", "author": None, "etype": "lesson"},
        {"title": "Pioneer Mother", "author": "Ruth H. Chadwick", "etype": "poem"},
    ],

    ("Vol36", "No08_August_1949"): [
        {"title": "The Tasks of Modern Citizenship", "author": "G. Homer Durham", "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "Rugmaking in the New England Mission", "author": "S. Dilworth Young", "etype": "article"},
        {"title": "What The Relief Society Magazine Means to Me", "author": "Alice Morrey Bailey", "etype": "article"},
        {"title": "Suggestions for a Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "Music in the Home", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "The Jumpher Family: Part II—Ready", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Words and Music", "author": "Bernice Brown", "etype": "fiction"},
        {"title": "Joanna—Chapter 8", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Television in the Home", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society Handbook of Instructions Ready for Distribution", "author": None, "etype": "article"},
        {"title": "Duplicate Lists of Contributors to Relief Society Building Fund", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Table Settings and Flower Arrangements", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "Glamour Weeds", "author": "Frances Aveson Wiscomb", "etype": "article"},
        {"title": "A Circus is Always Fun", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Theology: Our Lord's Ministry in Perea and Judea", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Behold, I Stand at the Door and Knock", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting—Sewing: Layettes and Baby Sewing", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: Legends of Chivalry", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Significance of the Declaration of Belief", "author": "G. Homer Durham", "etype": "lesson"},
        {"title": "Optional Lessons: The Counselors to President Joseph F. Smith", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Sand and Flame", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Immortality", "author": "Jessie M. Robinson", "etype": "poem"},
        {"title": "More at Peace", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "August", "author": "Mary Rigby", "etype": "poem"},
        {"title": "Moment", "author": "Georgia Moore Eberling", "etype": "poem"},
        {"title": "Remembrance", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "Wisteria Blossoms", "author": "Grace Sayre", "etype": "poem"},
        {"title": "All Serene", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Dream", "author": "Edna S. Dustin", "etype": "poem"},
        {"title": "Blind and Blues", "author": "Wanda Green Nielson", "etype": "poem"},
        {"title": "Lullaby", "author": "Jo Adelaide Stock", "etype": "poem"},
    ],

    ("Vol36", "No09_September_1949"): [
        {"title": "The Mormon Handicraft Shop", "author": "Ethel C. Smith", "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "Zion National Park", "author": "Willard Luce", "etype": "article"},
        {"title": "Suggestions for a Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "Tempered—Part I", "author": "Gladys I. Hamilton", "etype": "fiction"},
        {"title": "Martha, Papoose of the Great Spirit", "author": "James C. Young", "etype": "fiction"},
        {"title": "Joanna—Chapter 9", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Women Who Sew Together", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Annual General Relief Society Conference", "author": None, "etype": "article"},
        {"title": "Buying Textbooks for Relief Society Lessons", "author": None, "etype": "article"},
        {"title": "Annual Report—1948", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "Notes From the Field: Anniversary Parties, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Patchwork Quilts", "author": "Alice Whitson Norton", "etype": "article"},
        {"title": "The Making and Use of Decorative Candles", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "Edith Bunn Makes Unique Wagon-Wheel Rugs", "author": "Mirinda Knapp", "etype": "article"},
        {"title": "Wastebaskets Where You Want Them", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Theology: Continuation of the Perean and Judean Ministry", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: These Things I Have Spoken Unto You", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting—Sewing: Children's Clothing", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: The Poet Shakespeare", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Poplar Magic", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Tranquility", "author": "Celia Van Cott", "etype": "poem"},
        {"title": "Autumn Winds", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Autumn", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "That He May Know", "author": "LeRoy Burke Meagher", "etype": "poem"},
        {"title": "Song", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "The Heart", "author": "Essie M. Robinson", "etype": "poem"},
        {"title": "Marriage", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Blue Hills", "author": "Helen Gee Woods", "etype": "poem"},
        {"title": "Reprint of Shakespeare's Sonnet 73", "author": "William Shakespeare", "etype": "poem"},
        {"title": "Interval", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "A Friend Like You", "author": "Hilde Wittemund", "etype": "poem"},
        {"title": "My Valley", "author": "Jeanette P. Parry", "etype": "poem"},
    ],

    ("Vol36", "No10_October_1949"): [
        {"title": "Making Our Homes More Livable", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "Adoption of Children", "author": "Mary L. Dillman", "etype": "article"},
        {"title": "Suggestions for a Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "You Can Learn—Part I—A is for Adult—B is for Bride", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Tempered—Part II", "author": "Gladys I. Hamilton", "etype": "fiction"},
        {"title": "Joanna—Chapter 10", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "\"The Best of Friends\"", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society Handbook Obtainable Only at Office of General Board", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Bazaars, Singing Mothers, Socials, and Other Activities", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "The Interest and Beauty of Fruit", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "A Halloween Spook Table", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Crocheting Is Her Hobby", "author": None, "etype": "article"},
        {"title": "Home Safety—a Family Affair", "author": "Nellie O. Parker", "etype": "article"},
        {"title": "Let's Do Better Embroidery", "author": "Doris Feil", "etype": "article"},
        {"title": "Theology: \"The Perean and Judean Ministry\" and \"The Last Winter\"", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: \"Lovest Thou Me? Feed My Lambs\"", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Underwear and Nightwear", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: The English Bible", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: \"Governments Instituted for the Benefit of Man\"", "author": "G. Homer Durham", "etype": "lesson"},
        {"title": "Optional Lessons in Lieu of Social Science: President Heber J. Grant", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Time for Sorting", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Veteran", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Just Naturally", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "A Sidewalk Sketch", "author": "Elaine Swain", "etype": "poem"},
        {"title": "October", "author": "Elizabeth Johnson", "etype": "poem"},
        {"title": "Opportunity", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Against the Years", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Bride's Illusion for a Home", "author": "Lael W. Hill", "etype": "poem"},
    ],

    ("Vol36", "No11_November_1949"): [
        {"title": "Woman's Role as Homemaker", "author": "President Belle S. Spafford", "etype": "article"},
        {"title": "The Loving Friend of Children", "author": "Preston Nibley", "etype": "article"},
        {"title": "Ancient Rites at a Modern Wedding", "author": "Martha Toronto", "etype": "article"},
        {"title": "I Saw Palestine", "author": "Ann Rich", "etype": "article"},
        {"title": "Suggestions for a Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "The Pongee Dress", "author": "Grace M. Candland", "etype": "fiction"},
        {"title": "Joanna—Chapter 11", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "You Can Learn—Part II", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Thanksgiving Without a Feast", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Notes From the Field: Activities in the Missions, Socials, and Handicraft", "author": "Margaret C. Pickering", "etype": "report"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Fun With Spools", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Pioneer Recipe", "author": "Anna Prince Redd", "etype": "article"},
        {"title": "The Interest and Beauty of Vegetables", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Theology: \"On to Jerusalem\"", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: \"Is Not the Life More Than Meat?\"", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Girls' Clothing", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: Sir Francis Bacon", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Public Administration and Good Government", "author": "G. Homer Durham", "etype": "lesson"},
        {"title": "Optional Lessons: The Counselors to President Heber J. Grant", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Rain on November Night", "author": "Marvin Jones", "etype": "poem"},
        {"title": "Today Is All of Time", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Alien and Lost", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Poem", "author": "Gertrude T. Koven", "etype": "poem"},
        {"title": "Silence", "author": "Mary Pack Triplett", "etype": "poem"},
        {"title": "Ascendant Autumn", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Earth Decorator", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Long Wait", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Night", "author": "Gene Romolo", "etype": "poem"},
        {"title": "November Moon", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "November Day", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Beauty in Brown", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Star Music", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Thanksgiving Day", "author": "Nan S. Richardson", "etype": "poem"},
    ],

    ("Vol36", "No12_December_1949"): [
        {"title": "The Prophet's Sailing Orders to Relief Society", "author": "President J. Reuben Clark, Jr.", "etype": "article"},
        {"title": "Joy in Service", "author": "Counselor Marianne C. Sharp", "etype": "article"},
        {"title": "Introduction to Lesson Courses", "author": "Counselor Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society in the Lives of Our Sisters in Europe", "author": "Nida A. Taggart", "etype": "article"},
        {"title": "Hints on Lesson Development", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "Fostering Creative Homemaking Arts", "author": "Leone G. Layton", "etype": "article"},
        {"title": "With Liberty and Justice for All", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "Unwashen Hands Versus Hearts", "author": "Spencer W. Kimball", "etype": "article"},
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
        {"title": "Christmas Down the Years; The 120th Semi-Annual Church Conference; Litter on Temple Square", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "March Anniversary Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Ideas for Christmas Decoration", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "A Santa Claus Cookie Box", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "An Ever-Burning Christmas Candle", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "A Letter from Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Jesus Returns to the Temple Daily", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "For This Cause", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Boys' Clothing", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "John Bunyan", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "The Obligations of Citizenship", "author": "G. Homer Durham", "etype": "lesson"},
        {"title": "The Counselors to President Heber J. Grant (Continued)", "author": None, "etype": "lesson"},
        {"title": "The Flight", "author": "Dorothy J. Roberts", "etype": "poem"},
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
