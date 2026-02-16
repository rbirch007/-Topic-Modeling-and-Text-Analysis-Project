#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volumes 30, 31, and 32.

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched using two strategies (strict line-start and loose
anywhere-match) and both results are written as separate text files plus
a per-volume JSON containing full content.  See processed/README.md for
schema documentation.

Usage:
    python extract_vol30_31_32.py
    python extract_vol30_31_32.py --dry-run
    python extract_vol30_31_32.py --volume 30
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

TOC = {
    # ===================================================================
    # VOLUME 30 (1943)
    # ===================================================================

    ("Vol30", "No1_January_1943"): [
        {"title": "Messages for the New Year", "author": None, "etype": "editorial"},
        {"title": "Snowflakes", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Your Heritage", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "While the Earth Turns", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Symphony of the Snow", "author": "Linda S. Fletcher", "etype": "poem"},
        {"title": "Award Winners", "author": None, "etype": "front_matter"},
        {"title": "All That Glitters", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "Little Stones that Change the Course of Rivers", "author": "Lella M. Hoggan", "etype": "article"},
        {"title": "Justice is a Group Affair", "author": "Eva Willes Wangsgaard", "etype": "fiction"},
        {"title": "Beyond This Night", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "A New Heart and a New Spirit", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Point Rationing", "author": "Justin C. Stewart", "etype": "article"},
        {"title": "This Night", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Theology and Testimony", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "General Presidents", "author": "Louise Y. Robison", "etype": "lesson"},
    ],

    ("Vol30", "No2_February_1943"): [
        {"title": "Tribute to Washington", "author": "Daniel Webster", "etype": "front_matter"},
        {"title": "Lucy Mack Smith", "author": "Preston Nibley", "etype": "article"},
        {"title": "No Priorities", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Brotherhood", "author": "Harold B. Lee", "etype": "article"},
        {"title": "Silent Strings", "author": "Blanche Kendall McKey", "etype": "fiction"},
        {"title": "Shadows", "author": "Roberta Bates", "etype": "poem"},
        {"title": "George D. Pyper", "author": "Milton Bennion", "etype": "article"},
        {"title": "To Marry or Not To Marry In Time of War", "author": "Harold T. Christensen", "etype": "article"},
        {"title": "Confession", "author": "Ruth Louise Partridge", "etype": "poem"},
        {"title": "Gardening For Victory", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "The Road Builder", "author": "Rita Skousen Miller", "etype": "fiction"},
        {"title": "Superfine", "author": "Elsie E. Barrett", "etype": "poem"},
        {"title": "How Grownup Are You", "author": "Mark Allen", "etype": "article"},
        {"title": "Amy Brown Lyman Honored", "author": "Marcia K. Howells", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Our Liberty and Responsibilities", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Human Equation", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Planned Spring Cleaning", "author": "Gertrude R. Garff", "etype": "article"},
        {"title": "Flag Etiquette", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Theology and Testimony", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "General Presidents of Relief Society", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "War", "author": "Boneta LeBeau", "etype": "poem"},
        {"title": "In God We Trust", "author": None, "etype": "article"},
    ],

    ("Vol30", "No3_March_1943"): [
        {"title": "Triumph", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Compassionate Service", "author": "George Albert Smith", "etype": "article"},
        {"title": "Awakening", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "The House of the Lord Shall Be Established", "author": "Ann P. Nibley", "etype": "article"},
        {"title": "The Angel Moroni", "author": "Levi Edgar Young", "etype": "article"},
        {"title": "The Gold Watch", "author": "Mary E Knowles", "etype": "fiction"},
        {"title": "Contentment", "author": "Fern Ostler", "etype": "poem"},
        {"title": "On That First Morning", "author": "Vesta Crawford", "etype": "poem"},
        {"title": "Solace", "author": "Van Cott", "etype": "poem"},
        {"title": "Milton Bennion, General Superintendent", "author": "A. Hamer Reiser", "etype": "article"},
        {"title": "Crossing the River into Arizona", "author": "Pearl Udall Nelson", "etype": "article"},
        {"title": "The Evening Primrose", "author": None, "etype": "poem"},
        {"title": "Tomorrow's Cup", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Precedence of Relief Society Service", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Anniversary of Salt Lake Temple Dedication", "author": None, "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "The Abuse of Hate in Wartime", "author": "Mark K. Allen", "etype": "article"},
        {"title": "Fruits of the Good Earth", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Winter Clothes in Summertime", "author": "Priscilla L. Evans", "etype": "article"},
        {"title": "There's Always a Way", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Notes From the Field", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Work-and-Business", "author": None, "etype": "lesson"},
        {"title": "Sleeping Puppy", "author": "Courtney Cottam", "etype": "poem"},
    ],

    ("Vol30", "No4_April_1943"): [
        {"title": "Rachel Ridgway Ivins Grant", "author": None, "etype": "front_matter"},
        {"title": "My Mother's Hands", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "The Resurrection", "author": "Oscar W. McConkie", "etype": "article"},
        {"title": "Women and The Home Today", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "The Lion House", "author": "Edgar B. Brossard", "etype": "article"},
        {"title": "He Lives!", "author": "Genet Bingham Dee", "etype": "article"},
        {"title": "Keep Close to the Church", "author": "Rachel G. Taylor", "etype": "article"},
        {"title": "The Gardener's Best and Oldest Friend", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Song of Spring", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Birthday Congratulations", "author": "Lula Greene Richards", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Love One Another", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "New General Superintendency", "author": None, "etype": "article"},
        {"title": "The Felt Recreation Center", "author": "Marie Fox Felt", "etype": "article"},
        {"title": "Mother Takes a Holiday", "author": "Virginia Drew", "etype": "fiction"},
        {"title": "Tribute", "author": "Ethel VanHook Hopper", "etype": "poem"},
        {"title": "Making Over", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "American Husband", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Home Entertainment", "author": "Nellie O. Parker", "etype": "article"},
        {"title": "Halfway is not Enough", "author": "Vaunee T. Larson", "etype": "fiction"},
        {"title": "After Springtime Return", "author": "Beatrice E. Linford", "etype": "poem"},
        {"title": "An Adequate Daily Diet", "author": "Edna K. Ward", "etype": "article"},
        {"title": "Unemployed Clothing Called Into Service", "author": "Leda T. Jensen", "etype": "article"},
        {"title": "Tomorrow's Cup Chapter II", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Thinking of You", "author": "Bertha A. Kleinman", "etype": "poem"},
    ],

    ("Vol30", "No5_May_1943"): [
        {"title": "Spring", "author": "Julina B. Green", "etype": "poem"},
        {"title": "Lord, Let My Prayer", "author": "Irene R. Davis", "etype": "poem"},
        {"title": "Teach Tithing in the Home", "author": "LeGrand Richards", "etype": "article"},
        {"title": "She Let Her Light So Shine", "author": "Edith Smith Elliott", "etype": "article"},
        {"title": "A Tribute to May Green Hinckley", "author": None, "etype": "article"},
        {"title": "Rachel Ridgway Ivins Grant Concluded", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "Concerning Antique Furniture", "author": "Adele C. Howells", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "113th Annual Conference", "author": None, "etype": "editorial"},
        {"title": "A Free People", "author": "Rose Adeana Openshaw", "etype": "poem"},
        {"title": "Agnes M. Bolton", "author": None, "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Informative Labeling of Hosiery", "author": "Justin C. Stewart", "etype": "article"},
        {"title": "Care of Children in Wartime", "author": "Katharine F. Lenroot", "etype": "letter"},
        {"title": "Planned Strategy for the Garden Pests", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Notes From the Field", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Tomorrow's Cup Chapter III", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Purple Sage", "author": "Lydia Hall", "etype": "poem"},
        {"title": "Magazine Subscription Report", "author": "Vera White Pohlman", "etype": "report"},
    ],

    ("Vol30", "No6and7_JuneJuly_1943"): [
        {"title": "Messages from the Wives of the General Authorities", "author": None, "etype": "front_matter"},
        {"title": "The Mormon Woman", "author": "John A. Widtsoe", "etype": "article"},
        {"title": "Pioneer Day 1847", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Relationship of the Y.W.M.I.A. to the Relief Society", "author": "Lucy Grant Cannon", "etype": "article"},
        {"title": "Relationship of the Relief Society to the Primary", "author": "May Green Hinckley", "etype": "article"},
        {"title": "Grandma", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Elder Sylvester Q. Cannon", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "On Enlarging the Soul With Music", "author": "Alexander Schreiner", "etype": "article"},
        {"title": "Agnes McMillan Bolton", "author": None, "etype": "article"},
        {"title": "Steps in the Development of Our Flag", "author": None, "etype": "article"},
        {"title": "The Story of Old Glory", "author": "Mabel Harmer", "etype": "article"},
        {"title": "Short-story Writing", "author": "Pearl Spencer", "etype": "lesson"},
        {"title": "Eliza Roxey Snow Memorial Prize Poem Contest", "author": None, "etype": "front_matter"},
        {"title": "Annual Relief Society Short-story Contest", "author": None, "etype": "front_matter"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "To Our Fathers", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Personal Letters Protest", "author": None, "etype": "editorial"},
        {"title": "Tch! Tch! Grandma!", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Tomorrow's Cup Chapter IV", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Messages From the Missions", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Annual Report 1942", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Theology: The Dispersion of Israel", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home: A Firm Belief in God", "author": None, "etype": "lesson"},
        {"title": "Home Topics: Home Management", "author": None, "etype": "lesson"},
        {"title": "Literature: The Bible Story", "author": None, "etype": "lesson"},
        {"title": "Stories From the Book of Mormon", "author": None, "etype": "lesson"},
        {"title": "Social Science: American Statesmen", "author": None, "etype": "lesson"},
        {"title": "Excerpts from Letter of First Presidency", "author": None, "etype": "front_matter"},
    ],

    ("Vol30", "No8_August_1943"): [
        {"title": "Diurnal Ensemble", "author": "Celia A. Van Cott", "etype": "poem"},
        {"title": "The Latter-day Saint Family", "author": "Milton Bennion", "etype": "article"},
        {"title": "Sincerity", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "President Rudger Clawson", "author": "George Albert Smith", "etype": "article"},
        {"title": "Pioneer Homemaker", "author": "Julia A. Caine", "etype": "article"},
        {"title": "My Words", "author": "Roberta Bates", "etype": "poem"},
        {"title": "Ideals", "author": "Edna K. Ward", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Train Up a Child", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Tomorrow's Cup Chapter V", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "The Princess", "author": "Ruby Scranton Jones", "etype": "fiction"},
        {"title": "General Secretary-Treasurer", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Table of Magazine Subscriptions", "author": None, "etype": "report"},
        {"title": "Pictographs of Magazine Subscriptions", "author": None, "etype": "report"},
        {"title": "Magazine Promotion Work", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Relief Society Magazine Campaign", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Common Things", "author": "Elsie Sim Hansen", "etype": "article"},
        {"title": "Theology: The Gathering of Israel", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home: Ordered Living", "author": None, "etype": "lesson"},
        {"title": "Home Topics: Tools for Housecleaning", "author": None, "etype": "lesson"},
        {"title": "Literature: The Bible Epic", "author": None, "etype": "lesson"},
        {"title": "Social Science: American Statesmen", "author": None, "etype": "lesson"},
        {"title": "Stories From the Book of Mormon: Lehi", "author": None, "etype": "lesson"},
    ],

    ("Vol30", "No9_September_1943"): [
        {"title": "The Constitution", "author": None, "etype": "front_matter"},
        {"title": "To the Parents of Service Men", "author": "Hugh B. Brown", "etype": "article"},
        {"title": "Day End", "author": "Noel Bayliss", "etype": "poem"},
        {"title": "Elder George Albert Smith", "author": "Richard R. Lyman", "etype": "article"},
        {"title": "Spencer W. Kimball", "author": None, "etype": "article"},
        {"title": "Ezra Taft Benson Called to the Apostleship", "author": None, "etype": "article"},
        {"title": "New General Presidency of The Primary", "author": "Bryant S. Hinckley", "etype": "article"},
        {"title": "L.D.S. Men In The Armed Service", "author": "Milton Bennion", "etype": "article"},
        {"title": "The Bee Hive House", "author": "Rachel G. Taylor", "etype": "article"},
        {"title": "Short-story Writing", "author": "Pearl Spencer", "etype": "lesson"},
        {"title": "Eighteen in August", "author": "Roma Rose", "etype": "fiction"},
        {"title": "Dixie Pioneers", "author": "Florence Ivins Hyde", "etype": "article"},
        {"title": "Chastity Statements", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Seek Knowledge", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Pruning Rules", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Tomorrow's Cup Chapter VI", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "A City's Lone Old Cedar Tree", "author": "Edna S. Dustin", "etype": "poem"},
        {"title": "Relief Society Entertainments", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "So Young A Bride", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Theology: The Gathering of Israel", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home: Freedom of Choice", "author": None, "etype": "lesson"},
        {"title": "Literature: The Bible Lyric", "author": None, "etype": "lesson"},
        {"title": "Stumps of Opportunities", "author": "John M. Freckleton", "etype": "article"},
    ],

    ("Vol30", "No10_October_1943"): [
        {"title": "The Landing of Columbus", "author": None, "etype": "front_matter"},
        {"title": "Importance of the Sacrament Meeting", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Autumn", "author": "Edna S. Dustin", "etype": "poem"},
        {"title": "Marianne C. Sharp Appointed Associate Editor", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Periodic Medical Examinations", "author": "H. L. Marshall", "etype": "article"},
        {"title": "An Appeal to Mothers", "author": "Marvin O. Ashton", "etype": "article"},
        {"title": "She Is Ours", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "Rhapsody", "author": "Martha Fereday Harrison", "etype": "poem"},
        {"title": "L.D.S. Girls and Women in Wartime", "author": "Milton Bennion", "etype": "article"},
        {"title": "Madame Chiang Kai-shek", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Spiritual Strength Through Sabbath Observance", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "The Gracious Years", "author": "Kathleen B. Nelson", "etype": "fiction"},
        {"title": "Tomorrow's Cup Chapter VII", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Sunset", "author": "Lillian Jensen", "etype": "poem"},
        {"title": "Relief Society War and Welfare Activities", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Theology: Christ's Reign on Earth", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home: The Development of Strength", "author": None, "etype": "lesson"},
        {"title": "Home Topics: Floors and Floor Coverings", "author": None, "etype": "lesson"},
        {"title": "Home Topics: Walls, Ceilings", "author": None, "etype": "lesson"},
        {"title": "Literature: The Bible Ballad", "author": None, "etype": "lesson"},
        {"title": "Social Science: The Sacredness of American Citizenship", "author": None, "etype": "lesson"},
        {"title": "Stories from the Book of Mormon: Nephi", "author": None, "etype": "lesson"},
    ],

    ("Vol30", "No11_November_1943"): [
        {"title": "Wherever Pine Trees Grow", "author": "Martha Lu Tucker", "etype": "poem"},
        {"title": "President Heber J. Grant Reaches Another Milestone", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Altars of Life", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "The University of the Home", "author": "Joseph L. Wirthlin", "etype": "article"},
        {"title": "They Were Called Pilgrims", "author": "Vesta Crawford", "etype": "article"},
        {"title": "Autumn Sunset", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "Moral and Religious Integrity", "author": "Milton Bennion", "etype": "article"},
        {"title": "On a Milkweed Seed", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Pioneer Life in Old Mexico", "author": "Florence Ivins Hyde", "etype": "article"},
        {"title": "For Christmas This Year", "author": "Nellie O. Parker", "etype": "article"},
        {"title": "Happy Ending", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Giving Thanks", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Organizations and Reorganizations", "author": None, "etype": "article"},
        {"title": "Tomorrow's Cup Chapter VIII", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Relief Society War and Welfare Activities", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Mountain Beauty", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Theology: Regeneration and Resurrection", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home: Cooperation", "author": None, "etype": "lesson"},
        {"title": "Home Topics: Daily and Seasonal Housecleaning", "author": None, "etype": "lesson"},
        {"title": "Literature: Bible Wisdom", "author": None, "etype": "lesson"},
        {"title": "Social Science: Our American Culture", "author": None, "etype": "lesson"},
        {"title": "Stories from the Book of Mormon: Rebellion", "author": None, "etype": "lesson"},
    ],

    ("Vol30", "No12_December_1943"): [
        {"title": "Jesus in the Temple", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Wist Ye Not That I Must Be About My Father's Business", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "Places Jesus Loved", "author": "Thomas Cottam Romney", "etype": "article"},
        {"title": "Thoughts on the Prophet Joseph Smith", "author": "A. William Lund", "etype": "article"},
        {"title": "The 114th Semi-annual Conference", "author": "Richard L. Evans", "etype": "report"},
        {"title": "Look at the Star", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Across the Plains of Bethany", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "The Blessed Refrain", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Women and the Church Canning Program", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Care of the Christmas Gift Plant", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "One Star", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Christmas on Tubuai", "author": "Nettie Hunt Rencher", "etype": "article"},
        {"title": "Christmas Prayer", "author": "Claire S. Boyer", "etype": "poem"},
        {"title": "Tomorrow's Cup Chapter IX", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Theology: Religious Liberty and Toleration", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home: Companionship", "author": None, "etype": "lesson"},
        {"title": "Home Topics: Windows", "author": None, "etype": "lesson"},
        {"title": "Literature: A Bible Drama", "author": None, "etype": "lesson"},
        {"title": "Social Science: Standards of Success", "author": None, "etype": "lesson"},
        {"title": "Stories from the Book of Mormon: Deliverance", "author": None, "etype": "lesson"},
    ],

    # ===================================================================
    # VOLUME 31 (1944)
    # ===================================================================

    ("Vol31", "No1_January_1944"): [
        {"title": "Message", "author": "General Board of Relief Society", "etype": "editorial"},
        {"title": "Given", "author": "Olive C. Wehr", "etype": "poem"},
        {"title": "Wist Ye Not That I Must Be About My Father's Business", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "In Times of War", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Night Shadows", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Benison", "author": "Linda S. Fletcher", "etype": "poem"},
        {"title": "Undeclared Dividends", "author": "Vesta P. Crawford", "etype": "fiction"},
        {"title": "The Chimes", "author": "Pliny A. Wiley", "etype": "poem"},
        {"title": "The Least of the Flock", "author": "M. Lynn Bennion", "etype": "fiction"},
        {"title": "To A New House", "author": "Etta A. Christensen", "etype": "poem"},
        {"title": "Woman's World", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Honour Thy Father and Thy Mother", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Tomorrow's Cup Chapter X", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "Notes from the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Messages to the Home", "author": None, "etype": "lesson"},
        {"title": "Home Topics", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "Stories from the Book of Mormon", "author": None, "etype": "lesson"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No2_February_1944"): [
        {"title": "Abraham Lincoln", "author": "Bryant S. Hinckley", "etype": "article"},
        {"title": "Gettysburg Address", "author": "Abraham Lincoln", "etype": "front_matter"},
        {"title": "Wist Ye Not That I Must Be About My Father's Business", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "The Heart Must Kneel", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "We're Not So Different", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "The Poet Has a Birthday", "author": "Anna B. Hart", "etype": "article"},
        {"title": "A Bride Goes West", "author": "Julia Alleman Caine", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The Sabbath", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "The Exercise of Religious Liberty", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Food Fights For Freedom", "author": None, "etype": "article"},
        {"title": "My Window Garden", "author": "Florence Grant Smith", "etype": "article"},
        {"title": "Winter in the Canyon", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Notes From the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Winter Sleep", "author": "Clara Tanner", "etype": "poem"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home", "author": None, "etype": "lesson"},
        {"title": "Home Topics", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "Stories from the Book of Mormon", "author": None, "etype": "lesson"},
        {"title": "Rebinding", "author": "Alice Morrey Bailey", "etype": "poem"},
    ],

    ("Vol31", "No3_March_1944"): [
        {"title": "Snow In The Orchard", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Sketch Of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Relief Society In Action", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Our Relief Society President", "author": "Marguerite Burnhope Harris", "etype": "poem"},
        {"title": "Wist Ye Not That I Must Be About My Father's Business", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "The Pink Angel", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Glimpses of an Old Town", "author": "Katherine Palmer Macfarlane", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": None, "etype": "article"},
        {"title": "Gratitude to Our Leaders", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Lula Greene Richards", "author": "Louisa Wells Luke", "etype": "article"},
        {"title": "The Rock and the River", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "Soil Improvement in the Home Garden", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Early Spring", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "A Brief History of the Spring City", "author": "Bergetta Jensen", "etype": "article"},
        {"title": "How I Help Ward Literature Leaders", "author": "Fae J. Nielsen", "etype": "article"},
        {"title": "Forever", "author": "Jeanette P. Parry", "etype": "poem"},
        {"title": "The Soldier and His Health Overseas", "author": None, "etype": "article"},
    ],

    ("Vol31", "No4_April_1944"): [
        {"title": "Spring Silhouette", "author": "Agnes K. Morgan", "etype": "poem"},
        {"title": "Easter", "author": "Stephen L Richards", "etype": "article"},
        {"title": "Wist Ye Not That I Must Be About My Father's Business", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "Music on Easter", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Sketch of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "When We Must Say Goodby", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "The Child's Voice", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Salt Lake Region Welfare Meeting", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Rose Growing in the Home Garden", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "The Rock and the River", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "General Secretary-Treasurer", "author": "Vera W. Pohlman", "etype": "report"},
    ],

    ("Vol31", "No5_May_1944"): [
        {"title": "Your Spring and Mine", "author": "Olive Maiben Nicholes", "etype": "poem"},
        {"title": "The Polish Question", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "Wist Ye Not That I Must Be About My Father's Business", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "Passover", "author": "Weneta Leatham Nielsen", "etype": "poem"},
        {"title": "This I Know To Be True", "author": "Leone G. Layton", "etype": "article"},
        {"title": "The Faith of Motherhood", "author": "Ann Young Hughes", "etype": "poem"},
        {"title": "Lissa's Song", "author": "Mary Ek Knowles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Women Courageous", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "So Much To Do", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Sketch Of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Sisters, Let Your Own Hands Adorn Your Homes", "author": "William R. Palmer", "etype": "article"},
        {"title": "The First Born", "author": "Abbie R. Madsen", "etype": "poem"},
        {"title": "The Rock and the River", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "Love's the Bond", "author": "Merling Dennis Clyde", "etype": "poem"},
    ],

    ("Vol31", "No6_June_1944"): [
        {"title": "Offering", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "The Friendship Between the Prophets Joseph Smith and Brigham Young", "author": "Preston Nibley", "etype": "article"},
        {"title": "Report on the 114th Annual Conference", "author": "Antoine R. Ivins", "etype": "report"},
        {"title": "Mark E. Petersen Named Apostle", "author": "Henry A. Smith", "etype": "article"},
        {"title": "Contest Announcements", "author": None, "etype": "front_matter"},
        {"title": "The Republic of Czechoslovakia", "author": "Leland H. Creer", "etype": "article"},
        {"title": "Convoy", "author": "Grace A. Cooper", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Adapted to the Weakest of All Saints", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Sketch of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Table and Pictographs on Magazine Subscriptions", "author": None, "etype": "report"},
        {"title": "Magazine Honor Roll", "author": None, "etype": "front_matter"},
        {"title": "Notes From The Field", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Living Magic", "author": "Ora Lee Parthesius", "etype": "poem"},
    ],

    ("Vol31", "No7_July_1944"): [
        {"title": "So Let It Ring", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Freedom of Choice", "author": "David O. McKay", "etype": "article"},
        {"title": "I-30", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Yugoslavia", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "New Honor Comes to Tabernacle Choir", "author": "J. Spencer Cornwall", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Annual Report", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Time, Energy, and Fatigue Costs", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Map of the United States", "author": None, "etype": "misc"},
        {"title": "Appropriate Relief Society Programs", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Sketch of My Life Chapter V", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Suggestions and Instructions on the Educational Program", "author": None, "etype": "lesson"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Joseph Smith's First Vision", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Blessed Are the Poor in Spirit", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Historical Literature of the New Testament", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "Morality in Our Day", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No8_August_1944"): [
        {"title": "Declare the Glory", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Mormon Pioneer Culture", "author": "Alice Merrill Horne", "etype": "article"},
        {"title": "Leone Gedge Layton", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Pioneering in Southern Alberta", "author": "Jennie B. Knight", "etype": "article"},
        {"title": "Summer Rain", "author": "Louise Larson Cornish", "etype": "poem"},
        {"title": "The Three Witnesses of the Book of Mormon", "author": "Preston Nibley", "etype": "article"},
        {"title": "Fruition", "author": "Leone G. Layton", "etype": "poem"},
        {"title": "Rumania", "author": "Leland H. Creer", "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Honest and Wise Men Should Be Sought For", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Forward, Without Fear Chapter I", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Sketch of My Life Chapter VI", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Notes From the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Income Management in Wartime", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No9_September_1944"): [
        {"title": "I Will Look Unto the Hills", "author": "Della Adams Leitner", "etype": "article"},
        {"title": "Types of New Testament Literature", "author": "Russel B. Swensen", "etype": "article"},
        {"title": "Luacine Savage", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Rumania", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "Help Wanted", "author": "Nora Wayne", "etype": "fiction"},
        {"title": "Food Fights For Freedom", "author": "Rose H. Widtsoe", "etype": "article"},
        {"title": "Fall", "author": "Lottie Hammer Singley", "etype": "poem"},
        {"title": "Sketch of My Life Chapter VII", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Back to School", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "The Framers of the Constitution", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Forward, Without Fear Chapter II", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Home Safety", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No10_October_1944"): [
        {"title": "Song of America", "author": "Elizabeth Burningham", "etype": "article"},
        {"title": "Death and Sin", "author": "Albert E. Bowen", "etype": "article"},
        {"title": "Magic in Memory", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Toward Richer Living", "author": "Franklin S. Harris", "etype": "article"},
        {"title": "October", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Rheumatic Heart Disease", "author": "Frederic M. Kriete", "etype": "article"},
        {"title": "Depot", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "The New Turkey", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "The Captain's Biggest Battle", "author": "Sadie Willis Adamson", "etype": "fiction"},
        {"title": "She Steps Next Door", "author": "Edna S. Dustin", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Home After the War", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "They Also Serve", "author": "Alice Eddy LeCornu", "etype": "poem"},
        {"title": "Forward, Without Fear", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Sketch of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Yes, I Would Be A Saint", "author": "Eliza R. Snow Smith", "etype": "poem"},
        {"title": "Bury Me Quietly When I Die", "author": "Eliza R. Snow Smith", "etype": "poem"},
        {"title": "As a Grain of Mustard Seed", "author": "Leslie L. Sudweeks", "etype": "article"},
        {"title": "Let Us Take the Time", "author": "Margaret E. Maslin", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Where Brave Men Kneel", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Laundering", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Cloud Shadows", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "Ballet", "author": "Alice Morrey Bailey", "etype": "poem"},
    ],

    ("Vol31", "No11_November_1944"): [
        {"title": "The Pearl of Great Price", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Thankful Thoughts", "author": "Gertrude Perry Stanton", "etype": "poem"},
        {"title": "Why a Singer Should Keep the Word of Wisdom", "author": "Emma Lucy Gates Bowen", "etype": "article"},
        {"title": "For Thy Bounties", "author": "Marguerite J. Griffin", "etype": "article"},
        {"title": "Money Isn't Everything", "author": "Olive C. Wehr", "etype": "article"},
        {"title": "Finland", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "In November", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Birthday Greetings to President Heber J. Grant", "author": "Amy Brown Lyman", "etype": "editorial"},
        {"title": "Wise Spending", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "All These Remain", "author": "Betty Wall Madsen", "etype": "poem"},
        {"title": "What an Old Trunk May Bring Forth", "author": "William R. Palmer", "etype": "article"},
        {"title": "Forward, Without Fear", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Notes from the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Ironing Procedures", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Superscription", "author": "Jessie Miller Robinson", "etype": "poem"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "God's Garden", "author": "Daphne Jemmett", "etype": "poem"},
    ],

    ("Vol31", "No12_December_1944"): [
        {"title": "She Trims Her Tree", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "The Spirit of Christmas", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "A Few of Our Gifted Women Artists", "author": "Alice Merrill Horne", "etype": "article"},
        {"title": "Call to Service", "author": "Gertrude Perry Stanton", "etype": "poem"},
        {"title": "Palestine", "author": "Leland H. Creer", "etype": "article"},
        {"title": "The Lost Child", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Be Strong, My Heart", "author": "Marguerite J. Griffin", "etype": "article"},
        {"title": "Bethlehem the Blessed", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "It Isn't the Gifts", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "115th Semi-Annual Conference", "author": "Joseph J. Cannon", "etype": "report"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "A Cry of Repentance", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Forward, Without Fear", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Notes From the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "Home Lighting", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
    ],

    # ===================================================================
    # VOLUME 32 (1945)
    # ===================================================================

    ("Vol32", "No1_January_1945"): [
        {"title": "Frosted Trees", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "New Year's Message", "author": "General Presidency of Relief Society", "etype": "editorial"},
        {"title": "Sunset on New Year's Eve", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "For Recompense", "author": "Vesta P. Crawford", "etype": "poem"},
        {"title": "Aftermath", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "That Which Sustains", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Rock Roses of Nazareth", "author": "Ethel Newman Eccles", "etype": "fiction"},
        {"title": "A Few of Our Gifted Utah Women Artists", "author": "Alice Merrill Horne", "etype": "article"},
        {"title": "Unobliterated Still", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Palestine Part II", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "A Stone, in Place of Bread", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The Y.W.M.I.A. Observes Its Diamond Jubilee", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Victory", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Mama's Boy", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Pebbles", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "Forward, Without Fear", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Your Son and Mine", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "Notes from the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "The Flexible House", "author": "Adeline J. Haws", "etype": "poem"},
        {"title": "The Salvage of Tin Is Essential", "author": None, "etype": "article"},
        {"title": "Special Problems in Housekeeping", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
        {"title": "Ode to Child Psychology", "author": "Mildred R. Stutz", "etype": "poem"},
    ],

    ("Vol32", "No2_February_1945"): [
        {"title": "Two Men", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "How to Choose a Painting", "author": "Alice Merrill Horne", "etype": "article"},
        {"title": "Your Part in Utah's Centennial", "author": None, "etype": "article"},
        {"title": "I Am Peace", "author": "Ivy Houtz Woolley", "etype": "poem"},
        {"title": "The Bureau", "author": "Vesta Pierce Crawford", "etype": "fiction"},
        {"title": "The Big Horn Basin", "author": "Pliny A. Wiley", "etype": "poem"},
        {"title": "A Pioneer Story", "author": "Pauline Taggart Pingree", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Honesty", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Last Night They Played His Songs", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Mother-in-Law", "author": "Edith Lovell", "etype": "fiction"},
        {"title": "Work Meeting", "author": None, "etype": "article"},
        {"title": "Song for a Kitchen", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "Forward Without Fear", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "February", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Notes from the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "She Laughs at Miracles", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Repairs an Amateur Can Make", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Theology", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "author": None, "etype": "lesson"},
        {"title": "Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science", "author": None, "etype": "lesson"},
    ],

    ("Vol32", "No3_March_1945"): [
        {"title": "Notice of First Presidency", "author": None, "etype": "front_matter"},
        {"title": "The Relief Society in Church Welfare", "author": "Marion G. Romney", "etype": "article"},
        {"title": "Easter Time", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Are We Getting Ready for 1947", "author": "Herbert B. Maw", "etype": "article"},
        {"title": "The Relief Society President", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "Easter and the Second Coming", "author": "Leo J. Muir", "etype": "article"},
        {"title": "March Moods", "author": "Jeanette P. Parry", "etype": "poem"},
        {"title": "Though a Host Encamp Against Thee", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "To a Lost Flyer", "author": "Bessie Jarvis Payne", "etype": "poem"},
        {"title": "The Story of a Gifted Lady", "author": "Leah D. Widtsoe", "etype": "article"},
        {"title": "Gifts", "author": "Grace A. Cooper", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Gratitude for Church Guidance", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "In Memoriam: Sarah Eddington", "author": None, "etype": "obituary"},
        {"title": "Praying Hands", "author": "Clarence Edwin Flynn", "etype": "poem"},
        {"title": "Marshalling Our Forces for Home Safety", "author": "Nellie O. Parker", "etype": "article"},
        {"title": "To a Little Girl", "author": "Jo Adelaide Stock", "etype": "poem"},
        {"title": "March Designs for Summer", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Released", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "The Lonesome House", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "The Visiting Teacher's Prayer", "author": "Eva M. R. Salway", "etype": "poem"},
        {"title": "Rehabilitation of Disabled Soldiers", "author": None, "etype": "article"},
        {"title": "Early Spring Rain", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "The Bright Light of the Clouds", "author": "Alice Whitson Norton", "etype": "article"},
        {"title": "No Tongue Can Tell", "author": "Marguerite J. Griffin", "etype": "article"},
        {"title": "The Snowballs", "author": "Olive Rose Helton", "etype": "poem"},
        {"title": "Work Meeting", "author": None, "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "The Shape of Peace", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol32", "No4_April_1945"): [
        {"title": "Gold-green Day", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Look Unto the Rock", "author": "Daryl Chase", "etype": "article"},
        {"title": "Bridges", "author": "Grace A. Cooper", "etype": "poem"},
        {"title": "Rufus Kay Hardy", "author": "Levi Edgar Young", "etype": "article"},
        {"title": "Samuel Otis Bennion", "author": "Antoine R. Ivins", "etype": "article"},
        {"title": "Let Us Honor Them", "author": "Charles R. Mabey", "etype": "article"},
        {"title": "Come and Grow Old with Me", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "We Will Give the Tenth Unto Thee", "author": "Camilla Eyring Kimball", "etype": "article"},
        {"title": "These Are the Things I Hold", "author": "Roberta Bates", "etype": "poem"},
        {"title": "Remember", "author": "Rodney B. Taylor", "etype": "article"},
        {"title": "The Easter Gift", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "An Appreciation", "author": "Amy Brown Lyman", "etype": "editorial"},
        {"title": "To My Grandchildren", "author": "Maude Hatch Benedict", "etype": "poem"},
        {"title": "Come in for Dessert", "author": "Edna K. Ward", "etype": "article"},
        {"title": "The Cupboard of My Mind", "author": "Ann Young Hughes", "etype": "poem"},
        {"title": "Perennials for All-Year Blossoming", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "The Restless Heart", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "The Strength of the Pack", "author": "Mary Petterson", "etype": "fiction"},
        {"title": "And Once to Grandma", "author": "Doris Palmer", "etype": "fiction"},
        {"title": "This, too, Shall Pass Away", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Ten Miles to Calico", "author": "Martha Robeson Wright", "etype": "fiction"},
        {"title": "Notes from the Field", "author": "Vera W. Pohlman", "etype": "report"},
        {"title": "The Ways of Her Household", "author": "Della Adams Leitner", "etype": "poem"},
    ],

    ("Vol32", "No5_May_1945"): [
        {"title": "Belle Smith Spafford Called", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society", "author": "Amy Jeanne Donner", "etype": "poem"},
        {"title": "Marianne Clark Sharp", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Parallel", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Gertrude Ryberg Garff", "author": "Edith Smith Elliott", "etype": "article"},
        {"title": "Fulfillment", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Amy Brown Lyman", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "From Unnamed Mountains", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Marcia Knowlton Howells", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Meadow Lark", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Vera White Pohlman", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Forever Beauty", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Relief Society Achievements", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Howard S. McDonald Appointed", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "He's Coming Home", "author": "Earl J. Glade", "etype": "article"},
        {"title": "All-Year Care of the Rose Beds", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Ten Miles to Calico", "author": "Martha Robeson Wright", "etype": "fiction"},
        {"title": "The Magazine Honor Roll", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Notes From the Field", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "A Soldier's Tribute to Mother", "author": "May H. Marsh", "etype": "poem"},
    ],

    ("Vol32", "No6_June_1945"): [
        {"title": "Testimony of President Heber J. Grant", "author": None, "etype": "article"},
        {"title": "In Memoriam: President Heber J. Grant", "author": "Bryant S. Hinckley", "etype": "obituary"},
        {"title": "Remember This", "author": "Vesta Pierce Crawford", "etype": "poem"},
        {"title": "The 115th Annual General Conference", "author": "Joseph J. Cannon", "etype": "report"},
        {"title": "Seymour Dilworth Young", "author": "Pearl O. Allred", "etype": "article"},
        {"title": "Old Glory", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Milton R. Hunter", "author": "Daryl Chase", "etype": "article"},
        {"title": "President Franklin D. Roosevelt Passes", "author": None, "etype": "article"},
        {"title": "Contest Announcements", "author": None, "etype": "front_matter"},
        {"title": "We'll Live Those Days Again", "author": "Gus P. Backman", "etype": "article"},
        {"title": "The New Latter-day Saint Hymn Book", "author": "Tracy Y. Cannon", "etype": "article"},
        {"title": "And For Eternity", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "President Heber J. Grant", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Home Problems in Wartime", "author": "Edna K. Ward", "etype": "article"},
        {"title": "Unanswered Prayers", "author": "Marguerite S. Griffin", "etype": "article"},
        {"title": "Courage Is the Word for It", "author": "Mildred Steelman", "etype": "fiction"},
        {"title": "V-E Day Observed", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Living Safely At Home", "author": "Nellie O. Parker", "etype": "article"},
        {"title": "Daughters and Heirs", "author": "Grace S. Cozzens", "etype": "fiction"},
        {"title": "Sand-Pile Advantages", "author": "Alice Whitson Norton", "etype": "article"},
        {"title": "Flag Etiquette", "author": None, "etype": "article"},
        {"title": "Notes From The Field", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "A Parent's Place", "author": "Inez Stevens Cooper", "etype": "poem"},
    ],

    ("Vol32", "No7_July_1945"): [
        {"title": "This Is the Place", "author": "C. Frank Steele", "etype": "poem"},
        {"title": "President George Albert Smith", "author": "Preston Nibley", "etype": "article"},
        {"title": "President J. Reuben Clark, Jr", "author": "Albert E. Bowen", "etype": "article"},
        {"title": "President David O. McKay", "author": "Jeanette Morrell", "etype": "article"},
        {"title": "President George F. Richards", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Of Plain and Hill", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "General Board of Relief Society Reorganized", "author": None, "etype": "article"},
        {"title": "I Love This Great, Wide World", "author": "Essie Bateman Sisk", "etype": "poem"},
        {"title": "Blanche Black Stoddard", "author": "Gwendolyn H. Resell", "etype": "article"},
        {"title": "Evon Waspe Peterson", "author": "Leone G. Layton", "etype": "article"},
        {"title": "Leone Openshaw Jacobs", "author": "Alice B. Steinicke", "etype": "article"},
        {"title": "Things Beautiful", "author": "Rose T. Graham", "etype": "poem"},
        {"title": "Velma Nebeker Simonsen", "author": "Virginia Cannon Nelson", "etype": "article"},
        {"title": "Utah Shall Blossom Abundantly", "author": "Irvin T. Nelson", "etype": "article"},
        {"title": "July", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Augusta Winters Grant", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The New First Presidency", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Time For the Canning Program", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Soliloquy", "author": "Celia A. Van Cott", "etype": "poem"},
        {"title": "This Night to Remember", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Pioneer Wife", "author": "Margaret Jenkins", "etype": "poem"},
        {"title": "And For Eternity Chapter 2", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "Suggestions and Instructions for the Education Program", "author": None, "etype": "lesson"},
        {"title": "The Martyrdom of Joseph Smith", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Salt Flat Desert", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Christ's Example", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Our Literature Before the Bible Came", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Alleviation", "author": "Fae Decker Dix", "etype": "poem"},
        {"title": "Patterns of Prejudice and Persecution", "author": "Harold T. Christensen", "etype": "lesson"},
        {"title": "Missing", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "My Home", "author": "Minnie I. Hodapp", "etype": "poem"},
    ],

    ("Vol32", "No8_August_1945"): [
        {"title": "Navajo Fair", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Missions to the Indians", "author": "A. William Lund", "etype": "article"},
        {"title": "Worship", "author": "Oliver K. Meservy", "etype": "poem"},
        {"title": "A Navajo Conference", "author": "Emma B. Evans", "etype": "article"},
        {"title": "Navajo Headdress", "author": "Willard Luce", "etype": "article"},
        {"title": "Words in a Canyon", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Not to the Swift", "author": "Olive M. Nicholes", "etype": "fiction"},
        {"title": "The Vacant Lot", "author": "Randall L. Jones", "etype": "article"},
        {"title": "Farmer", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Gratitude Should Be Expressed", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "And for Eternity", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "A Child's Prayer", "author": "Sue Hopkins", "etype": "poem"},
        {"title": "Annual Report, 1944", "author": "Vera White Pohlman", "etype": "report"},
        {"title": "Theology: Brigham Young's Call to Leadership", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages: Charity Suffereth Long", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Literature: How the Bible is Woven Into Our Literature", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Problems of Personal Conduct", "author": "Harold T. Christensen", "etype": "lesson"},
        {"title": "Before Dawn", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
    ],

    ("Vol32", "No9_September_1945"): [
        {"title": "Bear Lake", "author": "Luella Nebeker Adams", "etype": "poem"},
        {"title": "The Missionary Work of the Church", "author": "Thomas C. Romney", "etype": "article"},
        {"title": "New Testament", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Susan B. Anthony", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Postwar Planning", "author": "J. Reuben Clark", "etype": "article"},
        {"title": "Settling Raymond, Alberta", "author": "Jennie B. Knight", "etype": "article"},
        {"title": "Stay As Sweet As You Are", "author": "Ada B. Monson", "etype": "fiction"},
        {"title": "Desert Born", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Regular Weekly Relief Society Meetings Begin", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "The Constitution of the United States", "author": None, "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Today", "author": "Dorothy Mae Halls", "etype": "poem"},
        {"title": "Beyond This", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "The Lane", "author": "Daphne Jemmett", "etype": "poem"},
        {"title": "Planning For Safety", "author": "Nellie O. Parker", "etype": "article"},
        {"title": "His First Teacher", "author": "Mildred Y. Hunt", "etype": "article"},
        {"title": "Mates", "author": "Edward R. Tuttle", "etype": "poem"},
        {"title": "The Radiant Spirit", "author": "Miranda Snow Walton", "etype": "misc"},
        {"title": "The Song", "author": "Helen Kimball Orgill", "etype": "poem"},
        {"title": "And For Eternity Chapter 4", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "Trial and Error", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Hollyhocks", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Presence", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Notes from the Field", "author": "Blanche B. Stoddard", "etype": "report"},
        {"title": "Work-Meeting Plan", "author": None, "etype": "article"},
        {"title": "My Kitchen Windows", "author": "Violet Harris Hendrickson", "etype": "poem"},
        {"title": "Theology: The Journey Westward", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages: Charity Envieth Not", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Literature: Bible Influence As Revealed in Our Lyric Literature", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "The Close of Life", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Mountain Song", "author": "Ivy Houtz Woolley", "etype": "poem"},
    ],

    ("Vol32", "No10_October_1945"): [
        {"title": "Walk down a Road in Autumn", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Family Prayer", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Prayer", "author": "Edwin C. Bliss", "etype": "poem"},
        {"title": "Shall I Be an Active Member of Relief Society", "author": "Kate M. Barker", "etype": "article"},
        {"title": "Challenge", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "The Bottom Drawer", "author": "Ruby Scranton Jones", "etype": "fiction"},
        {"title": "The Fight Against Infantile Paralysis", "author": "Frank S. Emery", "etype": "article"},
        {"title": "Follow Me", "author": "Rose Thomas Graham", "etype": "poem"},
        {"title": "Our Public Buildings", "author": "Howard Barker", "etype": "article"},
        {"title": "October Magic", "author": "Jeanette P. Parry", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Comfort Ye My People", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The Autumn Hails Anew", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "An Unweeded Garden", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Tawdry Jade", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "And for Eternity", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "Flowers and Fruits for the Table", "author": "Adele Matheson", "etype": "article"},
        {"title": "Orphaned", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Ochre Leaves", "author": "Ruby Baird Andersen", "etype": "poem"},
        {"title": "Notes from the Field", "author": "Blanche B. Stoddard", "etype": "report"},
        {"title": "Work Meeting Plan", "author": None, "etype": "article"},
        {"title": "Patterns", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "Theology: Building an Empire in the West", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages: Charity Vaunteth Not Itself", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Literature: Bible Influence as Revealed in Our Narrative", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Leisure and Morals", "author": "Harold T. Christensen", "etype": "lesson"},
        {"title": "Midas of Autumn", "author": "Margaret Jenkins", "etype": "poem"},
    ],

    ("Vol32", "No11_November_1945"): [
        {"title": "Autumn", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Thanks Be to God", "author": "Christie Lund Coles", "etype": "article"},
        {"title": "Excerpts from the Idaho Falls Temple Dedicatory Prayer", "author": "George Albert Smith", "etype": "article"},
        {"title": "Remembering the Birthday of President Grant", "author": None, "etype": "article"},
        {"title": "Dinner at Nora's", "author": "Martha Robeson Wright", "etype": "fiction"},
        {"title": "Number of Magazine Pages Increased", "author": None, "etype": "front_matter"},
        {"title": "Sports and Utah's Centennial", "author": "Ab Jenkins", "etype": "article"},
        {"title": "Seasonal", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Better Book Buying for the Home", "author": "Howard R. Driggs", "etype": "article"},
        {"title": "Chrysanthemums", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "November", "author": "Courtney Cottam", "etype": "poem"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The Idaho Falls Temple Dedicated", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "The Star Burns Bright", "author": "Sibyl Spande Bowen", "etype": "fiction"},
        {"title": "He Careth For Thee", "author": "Georgia Moore Eberling", "etype": "poem"},
        {"title": "Tables That Tell the Christmas Story", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "You Are Never Alone", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "And For Eternity Chapter VI", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "Their Trysting Place", "author": "Winnifred M. Tibbs", "etype": "poem"},
        {"title": "Notes from the Field", "author": "Blanche B. Stoddard", "etype": "report"},
        {"title": "The Faith I Know", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "Theology: Stakes of Zion Increased", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Literature: Bible Influence as Revealed in Our Drama", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Women and Morals", "author": "Harold T. Christensen", "etype": "lesson"},
        {"title": "Waiting Spring", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Prayer of Thanks", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "A Night With the Wind", "author": "Lottie Hammer Singley", "etype": "poem"},
        {"title": "Tide", "author": "Myrtle E. Henderson", "etype": "poem"},
    ],

    ("Vol32", "No12_December_1945"): [
        {"title": "Good Will Toward Men", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Address to the Members of the Relief Society", "author": "George Albert Smith", "etype": "article"},
        {"title": "Elder Matthew Cowley", "author": "Thomas E. Towler", "etype": "article"},
        {"title": "Christmas at North-Head Light", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "General Relief Society Conference", "author": None, "etype": "report"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "report"},
        {"title": "Homemaking Skills Developed Through the Work-Meeting Program", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Present-day Visiting Teaching Opportunities", "author": "Leone G. Layton", "etype": "article"},
        {"title": "The Gospel in the Relief Society Educational Program", "author": "Howard R. Driggs", "etype": "article"},
        {"title": "Relief Society's Expanding Welfare Service", "author": "Henry D. Moyle", "etype": "article"},
        {"title": "Relief Society Leadership", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Relief Society Membership", "author": "Louise Y. Robison", "etype": "article"},
        {"title": "Feed My Lambs", "author": "LeGrand Richards", "etype": "article"},
        {"title": "Wartime Activities of Our European Relief Society", "author": "Gertrude R. Garff", "etype": "article"},
        {"title": "Dream For a Relief Society Building", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "A Relief Society Building to Be Erected", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Success to a Relief Society Building", "author": "Marvin O. Ashton", "etype": "article"},
        {"title": "Relief Society Wheat Project", "author": "Joseph L. Wirthlin", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "misc"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "116th Semi-Annual Church Conference", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Christmas Wish", "author": "Helen Kimball Orgill", "etype": "poem"},
        {"title": "Christmas in the Bible", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "And For Eternity Chapter 7", "author": "Olive Woolley Burt", "etype": "fiction"},
        {"title": "Volunteer Price Control Service", "author": None, "etype": "article"},
        {"title": "Work Meeting", "author": None, "etype": "article"},
        {"title": "Theology: The Missionary System Broadened", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages: Charity Rejoiceth", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Literature: Bible Influence as Revealed in Our Fiction", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Morals in Marriage and Family Relationships", "author": "Harold T. Christensen", "etype": "lesson"},
        {"title": "Dishes", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Peace", "author": "Christie Lund Coles", "etype": "poem"},
    ],
}


# ---------------------------------------------------------------------------
# Filename mapping: issue key -> (source filename, month name for output)
# ---------------------------------------------------------------------------
ISSUE_FILES = {}

# Volume 30
for no, month, year in [
    ("No1", "January", "1943"), ("No2", "February", "1943"),
    ("No3", "March", "1943"), ("No4", "April", "1943"),
    ("No5", "May", "1943"), ("No6and7", "JuneJuly", "1943"),
    ("No8", "August", "1943"), ("No9", "September", "1943"),
    ("No10", "October", "1943"), ("No11", "November", "1943"),
    ("No12", "December", "1943"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol30_{key}.txt"
    ISSUE_FILES[("Vol30", key)] = (fname, month)

# Volume 31
for no, month, year in [
    ("No1", "January", "1944"), ("No2", "February", "1944"),
    ("No3", "March", "1944"), ("No4", "April", "1944"),
    ("No5", "May", "1944"), ("No6", "June", "1944"),
    ("No7", "July", "1944"), ("No8", "August", "1944"),
    ("No9", "September", "1944"), ("No10", "October", "1944"),
    ("No11", "November", "1944"), ("No12", "December", "1944"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol31_{key}.txt"
    ISSUE_FILES[("Vol31", key)] = (fname, month)

# Volume 32
for no, month, year in [
    ("No1", "January", "1945"), ("No2", "February", "1945"),
    ("No3", "March", "1945"), ("No4", "April", "1945"),
    ("No5", "May", "1945"), ("No6", "June", "1945"),
    ("No7", "July", "1945"), ("No8", "August", "1945"),
    ("No9", "September", "1945"), ("No10", "October", "1945"),
    ("No11", "November", "1945"), ("No12", "December", "1945"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol32_{key}.txt"
    ISSUE_FILES[("Vol32", key)] = (fname, month)


# ---------------------------------------------------------------------------
# Extraction engine
# ---------------------------------------------------------------------------

def build_regex_for_title(title: str, require_line_start: bool = True) -> re.Pattern:
    """
    Build a regex pattern that finds the title in the text.
    Allows for minor OCR variation and flexible whitespace.
    When require_line_start is True, the title must appear at the
    beginning of a line (after a newline or at position 0) to avoid
    matching common phrases buried mid-sentence in article body text.
    """
    # Escape regex special chars in the title
    escaped = re.escape(title)
    # Allow flexible whitespace (OCR may have inserted extra spaces)
    escaped = re.sub(r'\\ ', r'\\s+', escaped)
    if require_line_start:
        escaped = r'(?:^|\n)\s*' + escaped
    return re.compile(escaped, re.IGNORECASE)


def strip_running_noise(text: str) -> tuple[str, list[str]]:
    """
    Remove running headers and mailing statements from article body text.
    Returns (cleaned_text, list_of_stripped_fragments).
    """
    noise = []

    # Running headers: "RELIEF SOCIETY MAGAZINE" optionally followed by
    # month/year with optional punctuation (OCR often merges them)
    header_pat = re.compile(
        r'\d*\s*RELIEF SOCIETY MAGAZINE\s*[\W]*\s*'
        r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|'
        r'SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)?\s*\d{0,4}',
        re.IGNORECASE,
    )
    for m in header_pat.finditer(text):
        noise.append(m.group().strip())
    text = header_pat.sub('', text)

    # "LESSON DEPARTMENT" running section headers (appear mid-page)
    lesson_dept_pat = re.compile(r'\n\s*LESSON DEPARTMENT\s*\n')
    for m in lesson_dept_pat.finditer(text):
        noise.append(m.group().strip())
    text = lesson_dept_pat.sub('\n', text)

    # Mailing statement block
    mailing_pat = re.compile(
        r'Entered as second-class matter.*?authorized\s+June\s+29,\s+1918\.',
        re.DOTALL | re.IGNORECASE,
    )
    for m in mailing_pat.finditer(text):
        noise.append(m.group().strip())
    text = mailing_pat.sub('', text)

    # "Stamps should accompany manuscripts for their return."
    stamps_pat = re.compile(r'Stamps should accompany manuscripts for their return\.?')
    for m in stamps_pat.finditer(text):
        noise.append(m.group().strip())
    text = stamps_pat.sub('', text)

    # Collapse runs of blank lines left behind
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text, noise


def find_ads_section(body: str, body_offset: int) -> tuple[str, str, int]:
    """
    Look for advertising content at the tail of the body text.
    Returns (body_without_ads, ads_text, ads_start_in_full_text).
    If no ads found, ads_text is empty and body is unchanged.
    """
    # Search in the last 30% of the body for ad markers
    search_start = int(len(body) * 0.7)
    search_region = body[search_start:]

    ad_markers = [
        r"When Buying Mention Relief Society Magazine",
        r"DESERET NEWS PRESS",
        r"DESERET BOOK COMPANY",
        r"DAYNES\S?\s*MUSIC\s*CO",
        r"L\.\s*D\.\s*S\.\s*BUSINESS COLLEGE",
        r"MORMON HANDICRAFT",
        r"Brigham Young University",
    ]

    earliest_pos = None
    for marker in ad_markers:
        m = re.search(marker, search_region, re.IGNORECASE)
        if m:
            pos = search_start + m.start()
            if earliest_pos is None or pos < earliest_pos:
                earliest_pos = pos

    if earliest_pos is None:
        return body, "", body_offset + len(body)

    # Walk backwards from earliest_pos to find a paragraph break
    # that likely starts the ads section
    newline_pos = body.rfind('\n\n', 0, earliest_pos)
    if newline_pos != -1 and earliest_pos - newline_pos < 500:
        earliest_pos = newline_pos

    ads_text = body[earliest_pos:].strip()
    body_trimmed = body[:earliest_pos]

    return body_trimmed, ads_text, body_offset + earliest_pos


def split_front_matter(text: str) -> tuple[str, str]:
    """
    Split the issue text into front matter (TOC, board listing, ads, subscription
    info) and body content.  The marker is 'MAGAZINE CIRCULATION' which appears
    in every issue of Vol 30-32 at the end of the front matter section.

    Returns (front_matter, body).  If the marker is not found, front_matter is
    empty and body is the full text.
    """
    marker = re.search(r'MAGAZINE CIRCULATION[^\n]*', text)
    if marker:
        split_pos = marker.end()
        return text[:split_pos], text[split_pos:]
    else:
        print("  WARNING: MAGAZINE CIRCULATION marker not found, searching full text")
        return "", text


def _match_entries_with_strategy(body: str, entries: list[dict],
                                 body_offset: int,
                                 require_line_start: bool) -> list[tuple[int, dict]]:
    """
    Match TOC entries in body text using one strategy.
    Returns list of (position_in_full_text, entry_dict) for found entries.
    """
    found = []
    for entry in entries:
        pattern = build_regex_for_title(entry["title"],
                                        require_line_start=require_line_start)
        match = pattern.search(body)
        if match:
            pos = match.start()
            if require_line_start:
                # Adjust past the newline/whitespace prefix to point at the title
                matched_text = match.group()
                first_word = entry["title"].split()[0]
                title_in_match = re.search(re.escape(first_word),
                                           matched_text, re.IGNORECASE)
                if title_in_match:
                    pos += title_in_match.start()
            found.append((pos + body_offset, entry))
    return found


def _boundaries_from_found(found: list[tuple[int, dict]],
                           body_end: int) -> list[tuple[int, int, dict]]:
    """Convert sorted (pos, entry) list into (start, end, entry) triples."""
    found_sorted = sorted(found, key=lambda x: x[0])
    boundaries = []
    for i, (start, entry) in enumerate(found_sorted):
        end = found_sorted[i + 1][0] if i + 1 < len(found_sorted) else body_end
        boundaries.append((start, end, entry))
    return boundaries


def extract_toc_from_front_matter(front_matter: str) -> tuple[str, str]:
    """
    Extract the CONTENTS/TOC section from front matter.
    Returns (toc_text, remaining_front_matter).
    """
    # Look for CONTENTS header through the next major section boundary
    toc_match = re.search(
        r'(CONTENTS.*?)(?=GENERAL\s+BOARD|PUBLISHED\s+MONTHLY|'
        r'MAGAZINE\s+CIRCULATION|$)',
        front_matter, re.DOTALL | re.IGNORECASE,
    )
    if toc_match:
        toc_text = toc_match.group(1).strip()
        remaining = (front_matter[:toc_match.start()] +
                     front_matter[toc_match.end():]).strip()
        return toc_text, remaining
    return "", front_matter


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

            if strict_result or loose_result:
                matched_label = entry["etype"]
                s_chars = len(strict_result["content"]) if strict_result else 0
                l_chars = len(loose_result["content"]) if loose_result else 0
                ident_flag = " [identical]" if identical else ""
                if dry_run:
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
        elif dry_run:
            print(f"  [{'toc':12s}] {toc_filename} ({len(toc_text)} chars)")
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
        elif dry_run:
            print(f"  [{'ads':12s}] {ads_filename} ({len(ads_text)} chars)")
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
        elif dry_run:
            print(f"  [{'misc':12s}] {misc_filename} ({len(misc_text)} chars)")

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
        description="Extract Relief Society Magazine Vol 30-32 into individual entries"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without creating files")
    parser.add_argument("--volume", type=int, choices=[30, 31, 32],
                        help="Process only one volume")
    args = parser.parse_args()

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    # Collect JSON data per volume: { "Vol30": {"volume": ..., "months": {...}} }
    volume_json = {}


    for (vol, issue_key), entries in TOC.items():
        vol_num = int(vol.replace("Vol", ""))
        if args.volume and vol_num != args.volume:
            continue

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
