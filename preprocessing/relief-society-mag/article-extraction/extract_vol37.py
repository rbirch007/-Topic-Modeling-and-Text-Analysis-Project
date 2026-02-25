#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 37 (1950).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol37.py
    python extract_vol37.py --dry-run
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
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # up from article-extraction -> relief-society-mag -> preprocessing -> root
OUTPUT_DIR = PROJECT_ROOT / "processed" / "regex_and_llm"

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

VOL37_TOC = {
    ("Vol37", "No01_January_1950"): [
        {"title": "The Singing Snow", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "New Year Greetings", "author": "General Presidency of Relief Society", "etype": "article"},
        {"title": "Relief Society Women as Mothers in Zion", "author": "George Albert Smith", "etype": "article"},
        {"title": "Award Winners: Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Desert Pioneer", "author": "Evelyn H. Hughes", "etype": "poem"},
        {"title": "My Recompense", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "The Broken Bay", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Award Winners: Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Grass in the Market Place", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "A Banner From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Support the March of Dimes", "author": None, "etype": "article"},
        {"title": "Dark in the Chrysalis, Chapter 1", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "You Can Learn to Pray", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Open the Book of the Year", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Relief Society Assigned Evening Meeting of Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Bound Volumes of 1949 Relief Society Magazine", "author": None, "etype": "article"},
        {"title": "Temporary Binders Available", "author": None, "etype": "article"},
        {"title": "Awards Subscriptions Presented in April", "author": None, "etype": "article"},
        {"title": "Suggestive List of Songs for Singing Mothers", "author": "Florence J. Madsen", "etype": "article"},
        {"title": "Suggestions for a Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Singing Mothers, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Theology: The Close of Our Lord's Public Ministry", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Nevertheless Not My Will, but Thine Be Done", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Songs and Snowsuits", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: Some Seventeenth Century Poets", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Conditions for Achieving the Kingdom of God", "author": None, "etype": "lesson"},
        {"title": "Optional Lessons in Lieu of Social Science: President George Albert Smith", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Request", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "From New Year, Twelve O'clock", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "A New Wish", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "Music", "author": "Gene Romolo", "etype": "poem"},
        {"title": "To Mother", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Plea", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Heritage", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "The Miracle", "author": "LaVerne J. Stallings", "etype": "poem"},
        {"title": "Splendor", "author": "Edith Russell Oliphant", "etype": "poem"},
    ],

    ("Vol37", "No02_February_1950"): [
        {"title": "Lien on the Land", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Preservation of Our Blessings of Freedom", "author": None, "etype": "article"},
        {"title": "Relief Society Building News", "author": None, "etype": "article"},
        {"title": "The Enjoyment of Literature", "author": "Anna Prince Redd", "etype": "article"},
        {"title": "The House That Jim Built", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "I Know Where You Are", "author": "Inez Bagnell", "etype": "fiction"},
        {"title": "Dark in the Chrysalis, Chapter 2", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: On the Spending of Time", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Congratulations to President Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Suggestions for a Work Meeting Luncheon", "author": "Christine Eaton", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Singing Mothers, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Oriental China, Ancient and Modern", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Early Spring Planting", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Entertaining on Valentine's Day", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Further Instruction to the Apostles", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Be of Good Cheer", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Literature: John Milton the Lesser Works", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Achieving the Kingdom of God", "author": "G. Homer Durham", "etype": "lesson"},
        {"title": "Optional Lessons in Lieu of Social Science: Review of the Two-Year Course", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "The Tease", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "In My Father's House", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Memo to an Old Love", "author": "LeRoy Burke Meagher", "etype": "poem"},
        {"title": "Living Design", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Flaming Power", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Winter Night", "author": "Beth B. Johnson", "etype": "poem"},
        {"title": "The Cynic Said", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Tranquil Path", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "No Mountains", "author": "Lydia Hall", "etype": "poem"},
    ],

    ("Vol37", "No03_March_1950"): [
        {"title": "Remember Spring", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Relief Society and the Keys of the Kingdom", "author": "Bruce R. McConkie", "etype": "article"},
        {"title": "Give Me Your Benediction", "author": "Rose Lee Bond", "etype": "article"},
        {"title": "Women Pioneers of the Press", "author": "Carlton Culmsee", "etype": "article"},
        {"title": "A Modern Crusade for The Relief Society Magazine", "author": "Camilla E. Kimball", "etype": "article"},
        {"title": "Newcomers in Zion", "author": "Lonne Heaton Nave", "etype": "article"},
        {"title": "The American Red Cross and Its Program", "author": None, "etype": "article"},
        {"title": "To Lighten", "author": "Jeanne Tenney", "etype": "article"},
        {"title": "Gifts From the Mormon Handicraft Shop", "author": "Josie B. Bay", "etype": "article"},
        {"title": "The Place of Music in the Lives of the Women of the Church", "author": "Melissa Glade Behunin", "etype": "article"},
        {"title": "The Hee-Haw Pony", "author": "Florence Berrett Dunford", "etype": "fiction"},
        {"title": "An Afternoon With Mollby", "author": "Alice Whitson Norton", "etype": "fiction"},
        {"title": "You Can Learn, Part VI", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Dark in the Chrysalis, Chapter 3", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "A Hymn", "author": "Ezra J. Poulsen", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Handmaid to the Priesthood", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Announcing the Special April Short Story Issue", "author": None, "etype": "article"},
        {"title": "Notes to the Field: The Importance of the Visiting Teacher Message", "author": None, "etype": "article"},
        {"title": "Summer Work Meetings", "author": None, "etype": "article"},
        {"title": "Organizations and Reorganizations of Mission and Stake Relief Societies", "author": None, "etype": "article"},
        {"title": "Suggestions to Contributors", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Bazaars, Conventions, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Josephine Ortiz Makes Cloth Dolls for Fun and for Profit", "author": None, "etype": "article"},
        {"title": "Oriental China, Ancient and Modern II: Japanese Wares", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "For That Rainy Day", "author": "Gertrude LeWarne Parker", "etype": "article"},
        {"title": "I Watch Winter Pass", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Faith", "author": "Helen M. Horne", "etype": "poem"},
        {"title": "Spice", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Rain", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "Winter Was Long", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Poised Moment", "author": "Marvin Jones", "etype": "poem"},
        {"title": "More Than the Law", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "When I Am Old", "author": "Hannah C. Ashby", "etype": "poem"},
        {"title": "The Valley Train", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "On Borrowed Wings", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "The Desert Is a Lady", "author": "LaVerne J. Stallings", "etype": "poem"},
        {"title": "New Face", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Goodbye", "author": "Helen S. Hughes", "etype": "poem"},
        {"title": "This I Know", "author": "Arvilla Bennett Ashby", "etype": "poem"},
        {"title": "Love Is Music", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "Silent Wings", "author": "Gene Romolo", "etype": "poem"},
        {"title": "My Baby", "author": "Jessie J. Dalton", "etype": "poem"},
    ],

    ("Vol37", "No04_April_1950"): [
        {"title": "Sharing Your Treasure", "author": "Berta H. Christensen", "etype": "poem"},
        {"title": "An Exemplar to All Men: A Birthday Greeting to President George Albert Smith", "author": "David O. McKay", "etype": "article"},
        {"title": "And This Is Life Eternal", "author": "Harold B. Lee", "etype": "article"},
        {"title": "Plants of the New World", "author": "Willard Luce", "etype": "article"},
        {"title": "Unwrapping the Cancer Enigma", "author": "Wm. H. Kalis, Jr.", "etype": "article"},
        {"title": "Save the Magazines", "author": "Cleopha J. Jensen", "etype": "article"},
        {"title": "The Thickness of Water", "author": "Nellie Iverson Cox", "etype": "fiction"},
        {"title": "That Monson Kid", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "The Oldest Girl of the Oldest Girl", "author": "Blanche Kendall McKey", "etype": "fiction"},
        {"title": "A Chaperon for Miss Farry", "author": "Pansye H. Powell", "etype": "fiction"},
        {"title": "Our April Short Story Writers", "author": None, "etype": "article"},
        {"title": "Dark in the Chrysalis: Chapter 4", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Time For Rejoicing", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Regulations Governing the Submittal of Material", "author": None, "etype": "article"},
        {"title": "Bazaars, Socials, Singing Mothers", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "The April Garden", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Goodbye Kitchen Curtains!", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Hannah Davis Foster Makes Aprons", "author": "Fae D. Dix", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "From Your Believing", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Dresden Day", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Then Easter Came", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Image of Joseph", "author": "Alta Leafty Dew", "etype": "poem"},
        {"title": "Possession", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "Departure", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Renewal", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Diamonds", "author": "Katie Harris Lewis", "etype": "poem"},
        {"title": "Nothing So Lowly", "author": "Margaret T. Goff", "etype": "poem"},
        {"title": "To My Three-Year-Old on a Boat", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Give Me Words", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Great Salt Lake", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "The Song", "author": "Lydia M. Sorensen", "etype": "poem"},
        {"title": "The Birth of Harvest", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "Beyond Discovering", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Announcement", "author": "Hilda V. Cameron", "etype": "poem"},
    ],

    ("Vol37", "No05_May_1950"): [
        {"title": "Songs for David", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "A Mother's Influence", "author": "Joseph L. Wirthlin", "etype": "article"},
        {"title": "The Household of Faith", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "A Convert's Granddaughter Returns", "author": "Helen and Cyril Pearson", "etype": "article"},
        {"title": "A Pattern For Mother", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "The Recital", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Dark in the Chrysalis: Chapter 5", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Memorial Days", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "The One Hundredth Anniversary of the Founding of the University of Utah", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Magazine Subscriptions for 1949", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Magazine Honor Roll for 1949", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Storing Food in a Two-Room Apartment", "author": "Esther Clark Naylor", "etype": "article"},
        {"title": "Skirt Hangers in a Jiffy", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Pastel of Spring", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Meditation", "author": "Bessie G. Hale", "etype": "poem"},
        {"title": "Mother", "author": "Florence Berrett Dunford", "etype": "poem"},
        {"title": "The Childless Mother", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Stay With Me Now!", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Daily Bread", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "In These Hills", "author": "Evelyn Wooster Viner", "etype": "poem"},
        {"title": "The Landmark", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Reflections", "author": "Rose Lee Bond", "etype": "poem"},
        {"title": "Without Price", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "My Inland Sea", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Swinging", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Neighborhood", "author": "Margery S. Stewart", "etype": "poem"},
    ],

    ("Vol37", "No06_June_1950"): [
        {"title": "Floral Offering", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Brigham Young", "author": "Levi Edgar Young", "etype": "article"},
        {"title": "Contest Announcements: 1950", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "On Building a Poem", "author": "Anna Prince Redd", "etype": "article"},
        {"title": "The Short Story With a Plot", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "A Convert's Granddaughter Returns: Part II", "author": "Helen and Cyril Pearson", "etype": "article"},
        {"title": "The Vow of Oberammergau", "author": "Mirla Greenwood Thayne", "etype": "article"},
        {"title": "Hall of Fulfillment", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Postlude to Spring", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Dark in the Chrysalis: Chapter 6", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Brigham Young--Loyal and True", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Bazaars, Socials, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Immunize Against Accidents", "author": "Evelyn Kidneigh", "etype": "article"},
        {"title": "Garden Meditation", "author": "Ezra J. Poulsen", "etype": "article"},
        {"title": "European Pottery and Porcelain", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Temple at Dusk", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Paradox", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "A Gray Hawk Circling", "author": "Marvin Jones", "etype": "poem"},
        {"title": "April", "author": "Lurene Gates Wilkinson", "etype": "poem"},
        {"title": "Friend of Nature", "author": "Clarence Edwin Flynn", "etype": "poem"},
        {"title": "There Is No Sign", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Language of the Trees", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Mountain River", "author": "Elizabeth Waters", "etype": "poem"},
    ],

    ("Vol37", "No07_July_1950"): [
        {"title": "Pillars of Freedom", "author": "Alma Sonne", "etype": "article"},
        {"title": "Mission to Moapa -- Part of the Mormon Epic", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Through Thick and Thin", "author": "Nellie Iverson Cox", "etype": "fiction"},
        {"title": "Dark in the Chrysalis -- Chapter 7", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "You Can Learn -- Part V", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Our Pioneer Heritage", "author": "Velma N. Simonsen", "etype": "editorial"},
        {"title": "Congratulations to Sister Augusta Winters Grant", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "The Story of English China", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
        {"title": "Theology: The Life and Ministry of the Savior, Preview", "author": None, "etype": "lesson"},
        {"title": "Theology: The Last Supper and the Betrayal", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Our Savior Speaks, Preview", "author": None, "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Come and Follow Me", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting -- The Art of Homemaking, Preview", "author": None, "etype": "lesson"},
        {"title": "Work Meeting: Let's Be Homemakers as Well as Housekeepers", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature -- The Literature of England, Preview", "author": None, "etype": "lesson"},
        {"title": "Literature: Appreciating Poetry", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- The Progress of Man, Preview", "author": None, "etype": "lesson"},
        {"title": "Social Science: The God-Given Agency of Man", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music -- Fundamentals of Musicianship, Preview of Lessons for Union Meeting", "author": None, "etype": "lesson"},
        {"title": "Music: Fundamentals of Conducting", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Night Vigil", "author": "Nyal W. Anderson", "etype": "poem"},
        {"title": "Sycamore", "author": "Nina Folsom Moss", "etype": "poem"},
        {"title": "Commute", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Gifts", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "My Way", "author": "Evelyn Wooster Viner", "etype": "poem"},
        {"title": "Metamorphosis", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Dreams Are Song", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "The Might of God Extends the Arm of Man", "author": "Dorothy J. Roberts", "etype": "poem"},
    ],

    ("Vol37", "No08_August_1950"): [
        {"title": "Navajo Weaver", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "The Lamanites", "author": "Antoine R. Ivins", "etype": "article"},
        {"title": "Organizing the Relief Society Magazine Campaign", "author": "N. Florence Hanny", "etype": "article"},
        {"title": "The Best Time of Your Life", "author": "Ezra J. Poulsen", "etype": "article"},
        {"title": "The Jumpher Family Camps", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Where Love Abideth", "author": "Alice Whitson Norton", "etype": "fiction"},
        {"title": "Dark in the Chrysalis (Conclusion)", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Work Is a Blessing", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Centennials for 1950", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Notes From the Field: Bazaars, Socials, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "American Pottery and Porcelain", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Anna Mae Branch Makes Collection of Magazines", "author": None, "etype": "article"},
        {"title": "Just a Housewife", "author": "Winifred N. Jones", "etype": "article"},
        {"title": "The Washstand -- 1950 Version", "author": "Kathryn Jane Collins", "etype": "article"},
        {"title": "Learn to Labor and to Wait", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Theology: The Trial and Condemnation", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Take Ye Heed, Watch and Pray", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Color in the Home", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: Paradise Regained and Samson Agonistes", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The First Earth Government", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Baton Patterns and Their Application", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Promontory", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Chalk Cliff", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "Man", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Incarnation", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Things to Remember", "author": "Eleanor W. Schow", "etype": "poem"},
        {"title": "I Knew a Lad", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Complacent", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "Beauty Is in Everything", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Lovelier", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "White Plate", "author": "Irene Storey", "etype": "poem"},
        {"title": "Still", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Hope", "author": "Nelouise Fisher Judd", "etype": "poem"},
        {"title": "Below the Mountain", "author": "C. Cameron Johns", "etype": "poem"},
    ],

    ("Vol37", "No09_September_1950"): [
        {"title": "For Early Autumn", "author": "Berta H. Christensen", "etype": "poem"},
        {"title": "And They Shall Also Teach Their Children", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "The Practical Nurse -- A New Opportunity", "author": "Elaine Mellor", "etype": "article"},
        {"title": "The Constitution of the United States", "author": "Grace M. Candland", "etype": "article"},
        {"title": "One September -- Chapter 1", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "Apple for the Teacher", "author": "Lydia Bennett Egbert", "etype": "fiction"},
        {"title": "You Can Learn -- Part VI", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Voting Is Our Responsibility", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Annual General Relief Society Conference", "author": None, "etype": "article"},
        {"title": "Buying Textbooks for Relief Society Lessons", "author": None, "etype": "article"},
        {"title": "Annual Report -- 1949", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Theology: Death and Burial and In the Realm of Departed Spirits", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Search the Scriptures for They Are They Which Testify Of Me", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Draperies and Curtains", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: John Dryden", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Music: Baton Technique, Singing, and Interpretation", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Learned by Heart", "author": "Elizabeth Waters", "etype": "poem"},
        {"title": "On the Beach", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Influence", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Love Is Music", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "Beyond the Spring", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "Leaving a House", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "My Native City", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Heritage", "author": "Lurene Gates Wilkinson", "etype": "poem"},
    ],

    ("Vol37", "No10_October_1950"): [
        {"title": "October", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Crisis in Constitutionalism", "author": "G. Homer Durham", "etype": "article"},
        {"title": "President George F. Richards", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Home Is Where You Make It", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "One September -- Chapter 2", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "You Can Learn", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Quest for Peace", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Request for Copies of The Woman's Exponent", "author": None, "etype": "article"},
        {"title": "Relief Society Handbook Available", "author": None, "etype": "article"},
        {"title": "Wells Stake Completes Unique Map Project", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "American Pottery and Porcelain -- Part II", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "On Being One's Best Self", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Theology: The Resurrection and the Ascension", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For What Shall It Profit a Man?", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Slipcovers and Dressing Table Skirts", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: Richard Steele and Joseph Addison", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Evil Forces in the World", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: The Accompanist, Her Responsibility, Efficiency, and Art", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "The Living Bread", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "When I Leave", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "After the Harvest", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Autumn", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Songs of Gladness", "author": "Grace B. Wilson", "etype": "poem"},
        {"title": "Once Again", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "October Winds", "author": "Rose Thomas Graham", "etype": "poem"},
        {"title": "Enchantment in Oils", "author": "Elaine Swain", "etype": "poem"},
        {"title": "October Is Forever", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Keepers of the Hearth", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "October Song", "author": "Marvin Jones", "etype": "poem"},
        {"title": "Autumn Day", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Rain", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Storm Warning", "author": "Virginia Ellis Newman", "etype": "poem"},
    ],

    ("Vol37", "No11_November_1950"): [
        {"title": "For After Much Tribulation Come the Blessings", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Mildred Bennion Eyring Appointed to the Relief Society General Board", "author": "Leone G. Layton", "etype": "article"},
        {"title": "Thanksgiving", "author": "Louise Spencer", "etype": "article"},
        {"title": "A Star Is Shining", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "Poor Little Rosalee", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "One September -- Chapter 3", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "You Can Learn", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorials: Gracious Living", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Ernest L. Wilkinson Appointed President of Brigham Young University", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Socials, Bazaars, and Singing Mothers", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "A Simple Shade for an Antique Lamp", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Hobbies for Happiness", "author": None, "etype": "article"},
        {"title": "Kathleen Learns to Paint", "author": "Helen S. Martin", "etype": "article"},
        {"title": "Homemade Laundry Soap", "author": "Bernice Stookey Linford", "etype": "article"},
        {"title": "Teamwork Versus Slavery", "author": "Alice Whitson Norton", "etype": "article"},
        {"title": "Recipe for Corned Beef", "author": "Christine Eaton", "etype": "article"},
        {"title": "More Than Tolerance", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "The Great Balance Wheel", "author": "Sadie W. Adamson", "etype": "article"},
        {"title": "Theology: The Apostolic Ministry", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Forgive, and Ye Shall Be Forgiven", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Choosing Appropriate Floor Coverings", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: Jonathan Swift", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Ancient Political Despotisms", "author": "Archibald F. Bennett", "etype": "lesson"},
    ],

    ("Vol37", "No12_December_1950"): [
        {"title": "Who Watched in Faith", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Woman's Influence", "author": "David O. McKay", "etype": "article"},
        {"title": "President David O. McKay Named President of the Council of the Twelve", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Elder Joseph Fielding Smith Appointed Acting President of the Council of the Twelve", "author": "Henry A. Smith", "etype": "article"},
        {"title": "Delbert Leon Stapley, Sixty-Seventh Apostle", "author": "Camilla Eyring Kimball", "etype": "article"},
        {"title": "Helen Woodruff Anderson Appointed to General Board of Relief Society", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "Conference Proceedings", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Gaining Knowledge and Intelligence", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Culmination of Our Theology Course", "author": "Ethel C. Smith", "etype": "article"},
        {"title": "The Theology Lesson as It Affects Testimony", "author": "Leone G. Layton", "etype": "article"},
        {"title": "My Testimony", "author": "Viola B. Parkinson", "etype": "article"},
        {"title": "Lo, I Am With You Always", "author": "Mary G. Judd", "etype": "article"},
        {"title": "Concluding Statement -- Literature Panel Discussion", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "Eternal Values", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Introducing the New Social Science Course", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "The Accomplishments of Our Lamanite Sisters", "author": "Golden R. Buchanan", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Our Sisters in the Pacific Mission", "author": "Elva T. Cowley", "etype": "article"},
        {"title": "Our Sisters in the European Mission", "author": "Leona B. Sonne", "etype": "article"},
        {"title": "The Spiritual Power of Music", "author": "Florence J. Madsen", "etype": "article"},
        {"title": "Unto the Least of These", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Semi-Annual Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Ethel C. Smith Released as General Board Member", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Ring in Your Christmas Cards", "author": "Clara Laster", "etype": "article"},
        {"title": "Quilting Is Her Hobby", "author": None, "etype": "article"},
        {"title": "Used Yarn For Needlepoint Gifts", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Theology: Ministry of the Resurrected Christ on the Western Hemisphere", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: A New Commandment", "author": "Mary G. Judd", "etype": "lesson"},
        {"title": "Work Meeting: Choosing and Arranging Furniture", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: Alexander Pope", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Nations Which Rose and Fell", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: The Singing Mothers Chorus", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Peace, As on the Hills", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Child and the Tree", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Dear Santa: Now We Have a Daughter", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Silences", "author": "LaVerne J. Stallings", "etype": "poem"},
        {"title": "Where Shepherds Knelt", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Old Year", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Communications", "author": "Edwin S. Flynn", "etype": "poem"},
        {"title": "What Is Tomorrow?", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Old Age", "author": "Abbie R. Madsen", "etype": "poem"},
        {"title": "Poetry", "author": "Evelyn W. Viner", "etype": "poem"},
        {"title": "Count Not the Years", "author": "C. Frank Steele", "etype": "poem"},
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
    year = 1950

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol37_{key}.txt"
    ISSUE_FILES[("Vol37", key)] = (fname, month)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# OCR initial-letter substitution table.
# The OCR in Vol37 consistently misreads decorative/large-font initials:
#   Th -> Sh, Ch       T -> S, J, (       F -> St
# We handle these by emitting alternations for the first letter(s) of each
# word when the word starts with one of the affected patterns.
# ---------------------------------------------------------------------------
_OCR_WORD_START_ALTS = {
    # Two-char prefix: "Th" may OCR as "Sh" or "Ch"
    "Th": r"(?:Th|Sh|Ch)",
    "th": r"(?:th|sh|ch)",
    "TH": r"(?:TH|SH|CH)",
}

_OCR_SINGLE_CHAR_ALTS = {
    # Single-char prefix: "T" may OCR as "S", "J", or "("
    "T": r"(?:T|S|J|\()",
    "t": r"(?:t|s|j)",
    # "F" may OCR as "St" (e.g. "From" -> "Strom")
    "F": r"(?:F|St)",
    "f": r"(?:f|st)",
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers.  These recurring headers are so badly
# corrupted that character-level regex isn't enough; we provide handcrafted
# alternation patterns that cover the observed OCR variants.
# ---------------------------------------------------------------------------
# Shared sub-patterns for serial fiction chapter matching
_DARK_CHRYSALIS_CHAPTER_PAT = (
    r"Dark\s+[Ii]n\s+(?:Th|Sh|Ch)e\s+Chrysalis"
    r".{0,60}?"  # skip author name
    r"(?:CHAPTER|Chapter|CuaPTER|CuapTER)\s+"
)
_WHITE_SEPTEMBER_CHAPTER_PAT = (
    r"White\s+September"
    r".{0,60}?"  # skip author name
    r"(?:CHAPTER|Chapter)\s+"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—The Life and Ministry of the Savior Lesson NN—"SUBTITLE"
    # TOC has:  Theology: SUBTITLE
    # We match the Sheology/Theology prefix, skip the series title and lesson number,
    # then match the subtitle from the TOC.
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s\-\u2014\u2013:]*"          # dash/colon/space after Theology
        r"(?:.*?Lesson\s+\d+[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"  # optional series + lesson num
    ),
    # Body has: Ussiting Seacher ITlessages—Our Savior Speaks Lesson NN—"SUBTITLE"
    # TOC has:  Visiting Teacher Messages: SUBTITLE
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s)?s?iting\s+"
        r"(?:T|S)(?:e|l)(?:a|e)cher\s+"
        r"(?:M|IT|m)(?:l)?essages"
        r"[\s\-\u2014\u2013:]*"          # dash/colon/space after Messages
        r"(?:.*?Lesson\s+\d+[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"  # optional series + lesson num
    ),
    "From Near and Far": (
        r"(?:F|St)rom\s+Near\s+and\s+(?:F|S)ar"
    ),
    # Body has: EDITORIAL D9 VOL. 37 MONTH 1950 NO. N <actual title>
    # TOC has:  Editorial: <actual title>
    # We match EDITORIAL then skip the vol/date noise before the subtitle.
    "Editorial: ": (
        r"EDITORIAL"
        r"[\s\S]{0,80}?"  # skip noise (D9, VOL. 37, MONTH 1950, NO. N)
    ),
    "Editorials: ": (
        r"EDITORIAL[S]?"
        r"[\s\S]{0,80}?"
    ),
    # Body has: The Literature of England [Lesson NN—]SUBTITLE
    # TOC has:  Literature: SUBTITLE
    "Literature: ": (
        r"(?:Th|Sh|Ch)e\s+Literature\s+of\s+England"
        r"[\s\S]{0,60}?"  # skip optional "Lesson NN—"
    ),
    # Body has: Social Science—Latter-day Saint Political Thought Lesson NN—SUBTITLE
    # TOC has:  Social Science: SUBTITLE
    "Social Science: ": (
        r"Social\s+Science"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?Lesson\s+\d+[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Work Meeting[)] Lesson NN—SUBTITLE
    # TOC has:  Work Meeting: SUBTITLE
    "Work Meeting: ": (
        r"Work\s+Meeting[)\s\-\u2014\u2013:]*"
        r"(?:.*?Lesson\s+\d+[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Notes From the Field: or Notes to the Field: (sometimes OCR-mangled)
    # TOC has:  Notes From the Field: SUBTITLE or Notes to the Field: SUBTITLE
    "Notes From the Field: ": (
        r"Notes?\s+(?:F|St)rom\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    # Body has: Optional Lessons in Lieu of Social Science
    "Optional Lessons in Lieu of Social Science: ": (
        r"Optional\s+Lessons?\s+in\s+Lieu\s+of\s+Social\s+Science"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?Lesson\s+\d+[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Serial fiction: Dark in the Chrysalis
    # Body has: "Dark in the Chrysalis Alice Morrey Bailey CHAPTER N"
    # with N as Arabic or Roman numeral, and OCR-garbled CHAPTER spellings.
    # Use full title as key so remainder is empty.
    "Dark in the Chrysalis, Chapter 1": _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:1|I)\b",
    "Dark in the Chrysalis, Chapter 2": _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:2|II)\b",
    "Dark in the Chrysalis, Chapter 3": _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:3|III)\b",
    "Dark in the Chrysalis: Chapter 4": _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:4|IV)\b",
    "Dark in the Chrysalis: Chapter 5": _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:5|V)\b",
    "Dark in the Chrysalis: Chapter 6": _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:6|VI)\b",
    "Dark in the Chrysalis -- Chapter 7": _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:7|VII)\b",
    "Dark in the Chrysalis (Conclusion)": (
        _DARK_CHRYSALIS_CHAPTER_PAT + r"(?:8|VIII)\b"
    ),
    # Serial fiction: White September (TOC incorrectly says "One September")
    "One September -- Chapter 1": _WHITE_SEPTEMBER_CHAPTER_PAT + r"(?:1|I)\b",
    "One September -- Chapter 2": _WHITE_SEPTEMBER_CHAPTER_PAT + r"(?:2|II)\b",
    "One September -- Chapter 3": _WHITE_SEPTEMBER_CHAPTER_PAT + r"(?:3|III)\b",
}


def build_regex_for_title(title: str) -> re.Pattern:
    """Build a flexible regex pattern for matching a title in OCR'd body text.

    Handles common OCR artifacts:
    - Em/en dashes may be dropped or replaced with spaces
    - Apostrophes/quotes may be dropped or change form
    - Colons may be dropped (with or without surrounding text)
    - Whitespace varies (extra spaces, missing spaces at punctuation)
    - Word-initial letter substitutions (Th->Sh, T->S/J, F->St)
    - Known badly-mangled recurring section headers
    """
    # Check if the title starts with a known mangled header; if so,
    # substitute the handcrafted pattern for that prefix and build
    # the remainder normally.
    for header_text, header_pat in _KNOWN_HEADER_PATTERNS.items():
        if title.startswith(header_text):
            remainder = title[len(header_text):]
            remainder_pat = _build_regex_chars(remainder) if remainder else ""
            pattern = header_pat + remainder_pat
            return re.compile(pattern, re.IGNORECASE)

    pattern = _build_regex_chars(title)
    return re.compile(pattern, re.IGNORECASE)


def _build_regex_chars(title: str) -> str:
    """Character-by-character regex builder with OCR-aware word-start handling."""
    parts = []
    i = 0
    # Track whether we're at the start of a word
    at_word_start = True

    while i < len(title):
        ch = title[i]

        if ch in '\u2014\u2013-':  # em dash, en dash, hyphen
            parts.append(r'[\s\-\u2014\u2013]*')
            at_word_start = True
        elif ch in "\u2018\u2019\u201c\u201d'\"":  # curly/straight quotes
            parts.append(r'[\u2018\u2019\u2032\u0027]?')
        elif ch == ':':
            parts.append(r':?\s*')
            at_word_start = True
        elif ch == ',':
            # Commas may be present or absent in OCR text
            parts.append(r',?\s*')
        elif ch == '?':
            # Question marks may be absent or in different position in OCR
            parts.append(r'\??')
        elif ch == '!':
            # Exclamation marks may be absent in OCR
            parts.append(r'!?')
        elif ch == ' ':
            # Allow optional comma/semicolon before whitespace (OCR may add
            # punctuation not present in the TOC title)
            parts.append(r'[,;]?\s+')
            at_word_start = True
        elif at_word_start and ch.isalpha():
            # Try two-char prefix first (e.g. "Th")
            two = title[i:i+2]
            if two in _OCR_WORD_START_ALTS:
                parts.append(_OCR_WORD_START_ALTS[two])
                i += 2
                at_word_start = False
                continue
            # Try single-char prefix
            if ch in _OCR_SINGLE_CHAR_ALTS:
                parts.append(_OCR_SINGLE_CHAR_ALTS[ch])
            else:
                parts.append(re.escape(ch))
            at_word_start = False
        else:
            parts.append(re.escape(ch))
            at_word_start = False

        i += 1

    return ''.join(parts)


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


def find_ads_section(body: str) -> str:
    """
    Discover ads text and return it.  Leave body unchanged.
    Returns (ads_text).
    """
    ads_markers = [
        r"ADVERTISING",
        r"ADVERTISEMENTS",
        r"FOR SALE",
        r"BUSINESS",
    ]

    for marker in ads_markers:
        match = re.search(marker, body, re.IGNORECASE)
        if match:
            return body[match.start():]

    return ""


def split_front_matter(text: str) -> tuple[str, str]:
    """
    Split text into front matter (TOC, etc.) and body.
    Looks for "PUBLISHED MONTHLY BY THE GENERAL BOARD" marker which ends front matter.
    """
    toc_end_markers = [
        "PUBLISHED MONTHLY BY THE GENERAL BOARD",
        "ISHED MONTHLY BY THE GENERAL BOARD",
        "MONTHLY BY THE GENERAL BOARD"
    ]

    split_point = -1
    for marker in toc_end_markers:
        match = re.search(marker, text, re.IGNORECASE)
        if match:
            split_point = match.start()
            front_matter = text[:split_point]
            body = text[split_point:]
            return front_matter, body

    if split_point == -1:
        raise Exception("Unable to find 'PUBLISHED MONTHLY BY THE GENERAL BOARD' and so couldn't split text.")


def _match_entries_with_strategy(body: str, entries: list[dict]) -> list[tuple[int, dict]]:
    """
    Match all entries in the body.
    Returns list of (position, entry_dict) tuples.
    """
    found = []

    for entry in entries:
        pattern = build_regex_for_title(entry["title"])
        match = pattern.search(body)

        if match:
            found.append((match.start(), entry))

    return found


def _boundaries_from_found(body: str, found: list[tuple[int, dict]]) -> list[tuple[int, int, dict]]:
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
        end = found[i + 1][0] if i + 1 < len(found) else len(body)
        bounds.append((start, end, entry))

    return bounds


def extract_toc_from_front_matter(front_matter: str) -> tuple[str, str]:
    """
    Extract TOC section from front matter.
    Returns (toc_text, remaining_front_matter).
    """
    toc_start = front_matter.lower().find("contents")
    if toc_start == -1:
        return "", front_matter

    toc_text = front_matter[toc_start:]
    remaining = front_matter[:toc_start]

    toc_end_markers = [
        "PUBLISHED MONTHLY BY THE GENERAL BOARD",
        "ISHED MONTHLY BY THE GENERAL BOARD",
        "MONTHLY BY THE GENERAL BOARD",
        r"^[Page\s]*\d+"
    ]
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

    # Separate ads from the tail of the body
    ads_text = find_ads_section(body)

    # Run matching (single strategy - no line-start dependency)
    found = _match_entries_with_strategy(body, entries)
    bounds = _boundaries_from_found(body, found)

    # Build lookup dict: title -> (start, end)
    by_title = {e["title"]: (s, nd) for s, nd, e in bounds}

    stats = {"matched": 0, "misc_bytes": 0,
             "total_bytes": len(text.encode("utf-8")),
             "manifest_rows": []}

    issue_dir = output_dir / vol / month
    rel_dir = f"processed/regex_and_llm/{vol}/{month}"
    if not dry_run:
        issue_dir.mkdir(parents=True, exist_ok=True)

    # Collect all noise stripped from articles for MISC
    all_noise = []
    # Track covered intervals
    covered_intervals = []
    # JSON entries for this month
    json_entries = []

    # Ordering for index numbering
    title_order = [e["title"] for _, _, e in bounds]

    # Build entry lookup by title for metadata
    entry_by_title = {e["title"]: e for e in entries}

    for idx, title in enumerate(title_order, 1):
        entry = entry_by_title.get(title)
        if not entry:
            continue

        title_safe = sanitize_filename(entry["title"])

        # Process match
        match_result = None
        if title in by_title:
            start, end = by_title[title]
            raw_text = body[start:end].strip()
            raw_len = len(raw_text)
            cleaned, noise_frags = strip_running_noise(raw_text)
            cleaned = cleaned.strip()
            all_noise.extend(noise_frags)
            covered_intervals.append((start, end))

            filename = f"{idx:02d}_{title_safe}.txt"
            if not dry_run and cleaned:
                (issue_dir / filename).write_text(cleaned, encoding="utf-8")

            match_result = {
                "file": filename,
                "path": rel_dir,
                "position": start,
                "length": raw_len,
                "content": cleaned,
            }

        if match_result:
            stats["matched"] += 1

            json_entry = {
                "index": idx,
                "title": entry["title"],
                "author": entry["author"],
                "etype": entry["etype"],
                "match": match_result,
            }
            json_entries.append(json_entry)

            # Manifest row
            stats["manifest_rows"].append({
                "file": match_result["file"],
                "path": rel_dir,
                "volume": vol,
                "month": month,
                "etype": entry["etype"],
                "title": entry["title"],
                "author": entry["author"],
                "strategy": "match",
            })

            # Verbose output
            matched_label = "matched"
            chars = len(match_result["content"])
            print(f"  [{matched_label:12s}] #{idx:02d} "
                  f"chars={chars} "
                  f"{entry['title'][:50]}")
        else:
            print(f"  WARNING: No match for '{entry['title']}' in body text")

    # Report unmatched TOC entries
    matched_titles = {e["title"] for e in entries}
    toc_matched = {e["title"] for e in json_entries}
    unmatched = matched_titles - toc_matched

    if unmatched:
        print(f"\n  TOC entries NOT found in body ({len(unmatched)}):")
        for title in sorted(unmatched):
            toc_entry = next((e for e in entries if e["title"] == title), {})
            print(f"    - {title} ({toc_entry.get('etype', '?')})")
    else:
        print(f"\n  All {len(matched_titles)} TOC entries matched in body")

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

    # Find gaps in body not covered by any entry
    all_intervals = sorted(set(covered_intervals))
    cursor = 0
    for iv_start, iv_end in all_intervals:
        if cursor < iv_start:
            gap_text = body[cursor:iv_start].strip()
            if gap_text:
                misc_parts.append(gap_text)
        cursor = max(cursor, iv_end)

    if cursor < len(body):
        gap_text = body[cursor:].strip()
        if gap_text:
            misc_parts.append(gap_text)

    # Stripped noise goes into MISC
    if all_noise:
        misc_parts.append("--- STRIPPED NOISE ---")
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
        description="Extract Relief Society Magazine Vol 37 into individual entries"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without creating files")
    parser.add_argument("--use-raw-data", action="store_true",
                        help="Use raw-data instead of cleaned-data")
    args = parser.parse_args()

    # Set data directory based on flag
    if args.use_raw_data:
        data_dir = PROJECT_ROOT / "raw-data" / "relief-society" / "txtvolumesbymonth"
        print("Using raw-data directory")
    else:
        data_dir = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"
        print("Using cleaned-data directory")

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    volume_json = {}

    for (vol, issue_key), entries in VOL37_TOC.items():
        if (vol, issue_key) not in ISSUE_FILES:
            print(f"WARNING: No file mapping for ({vol}, {issue_key}), skipping")
            continue

        filename, month = ISSUE_FILES[(vol, issue_key)]
        source_path = data_dir / vol / filename

        if not source_path.exists():
            # Try case variations
            for candidate in data_dir.iterdir():
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

        out_vol = vol.lower()
        stats = extract_issue(text, entries, out_vol, month, filename,
                              OUTPUT_DIR, dry_run=args.dry_run)

        issues_processed += 1
        total_matched += stats["matched"]
        total_misc += stats["misc_bytes"]
        total_bytes += stats["total_bytes"]
        all_manifest_rows.extend(stats["manifest_rows"])

        # Accumulate into volume JSON
        if out_vol not in volume_json:
            volume_json[out_vol] = {"volume": out_vol, "months": {}}
        volume_json[out_vol]["months"][month] = stats["month_json"]

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

            # Build flagged_for_review.json
            flagged = []
            for month_name, month_data in data["months"].items():
                for entry_json in month_data["entries"]:
                    title = entry_json["title"]
                    title_pat = re.compile(
                        re.sub(r'\s+', r'\\s+', re.escape(title)),
                        re.IGNORECASE,
                    )
                    match_data = entry_json.get("match")
                    if match_data is None:
                        continue
                    content = match_data["content"]
                    head = content[:200] if content else ""
                    if not title_pat.search(head):
                        flagged.append({
                            "title": title,
                            "author": entry_json["author"],
                            "etype": entry_json["etype"],
                            "index": entry_json["index"],
                            "month": month_name,
                            "file": match_data["file"],
                            "path": match_data["path"],
                            "position": match_data["position"],
                            "length": match_data["length"],
                            "content": content,
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
