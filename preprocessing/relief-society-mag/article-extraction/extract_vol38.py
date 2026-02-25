#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 38 (1951).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol38.py
    python extract_vol38.py --dry-run
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
# ---------------------------------------------------------------------------

VOL38_TOC = {
    ("Vol38", "No01_January_1951"): [
        {"title": "The Heart Will Find It", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "A Birthday Greeting", "author": "General Presidency of Relief Society", "etype": "article"},
        {"title": "Ernest L. Wilkinson, President of Brigham Young University", "author": "Ivor Sharp", "etype": "article"},
        {"title": "Award Winners: Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Let's Relate", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "City Home", "author": "Julia M. Nelson", "etype": "poem"},
        {"title": "Pioneer Wagon Wheels", "author": "Ruth Horsley Chadwick", "etype": "poem"},
        {"title": "Award Winners: Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "But Covet Earnestly", "author": "Mirla Greenwood Thayne", "etype": "fiction"},
        {"title": "Polio Strikes Egonia", "author": None, "etype": "article"},
        {"title": "Pioneering in Wire Bend Territory", "author": None, "etype": "article"},
        {"title": "We Stayed to the End", "author": "Fae Decker Dix", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Old and the New", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Relief Society Assigned Evening Meeting of Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Theology: The Long Night of Apostasy", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Jesus Answering Saith Unto Them", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Pictures, Mirrors, and Wall Accessories", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: Oliver Goldsmith", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Role of Ancient Israel", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Theories Underlying Singing, Accompanying, and Conducting", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Blending Old and New", "author": "Phyllis Snow", "etype": "article"},
        {"title": "The Low Cost of Happiness", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "From Commodities to Beauty", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Crocheting Keeps Her Busy and Happy", "author": "Rosella F. Larkin", "etype": "article"},
        {"title": "Boys Are Dear", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Letter From a Daughter", "author": "Clara Laster", "etype": "poem"},
        {"title": "Rosemary", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "The Wild Geese Fly", "author": "Marvin Jones", "etype": "poem"},
        {"title": "Progress", "author": "Agnes Just Reid", "etype": "poem"},
        {"title": "The Dying Year", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Sketches", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Recompense", "author": "Matia McClelland Burk", "etype": "poem"},
        {"title": "Mirror, Mirror", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "My Choice", "author": "Marion W. Garibaldi", "etype": "poem"},
        {"title": "My Child", "author": "Marylou Shaver", "etype": "poem"},
        {"title": "Within My Heart", "author": "Grace Sayre", "etype": "poem"},
    ],

    ("Vol38", "No02_February_1951"): [
        {"title": "February Thaw", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "The Great Mission of Relief Society", "author": "LeGrand Richards", "etype": "article"},
        {"title": "A Key to the Occurrences of History", "author": "Archibald F. Bennett", "etype": "article"},
        {"title": "We'll Always Remember", "author": "Inez B. Bagnell", "etype": "fiction"},
        {"title": "For the Strength of the Hills, Chapter 1", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Miss Breech's Boy", "author": "Pansye H. Powell", "etype": "fiction"},
        {"title": "In the Twinkling of a Toe", "author": "Maryhale Woolsey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Say Nothing But Repentance Unto This Generation", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Birthday Greetings to President Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Conventions, Welfare Work, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Theology: Personal Manifestations of God and of His Son Jesus Christ in Modern Times", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Lo, I am With You Alway, Even Unto the End of the World", "author": "Mary Grant Judd", "etype": "lesson"},
        {"title": "Work Meeting: Table Settings and Service", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: Samuel Johnson and James Boswell", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Universal Peace Must Come From God", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: New Hymns, Anthems, and Standard Literature About Music", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Buying Food for the Family", "author": "Ruth P. Tippetts", "etype": "article"},
        {"title": "Dawn Is the Gateway", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Too Bound of Earth", "author": "Berta H. Christensen", "etype": "poem"},
        {"title": "The Recall", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Where Glory Lies", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Blackbirds in Winter", "author": "Clara Laster", "etype": "poem"},
        {"title": "The Perishables", "author": "C. Cameron Johns", "etype": "poem"},
    ],

    ("Vol38", "No03_March_1951"): [
        {"title": "The Miracle Returns", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Highlights of the Past", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "The Prophet Joseph Smith", "author": "M. Isabella Horne", "etype": "article"},
        {"title": "The American National Red Cross", "author": None, "etype": "article"},
        {"title": "She Shall Have Music", "author": "Frances Carter Yost", "etype": "fiction"},
        {"title": "The Sewing Kit Speaks", "author": "Lillian S. Feltman", "etype": "fiction"},
        {"title": "Growing Pains", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "For the Strength of the Hills, Chapter 2", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Anniversary Aims", "author": "Mildred R. Stutz", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Now, Let Us Rejoice", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Announcing the Special April Short Story Issue", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Organizations and Reorganizations of Mission and Stake Relief Societies", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Bazaars, Conventions, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Spring House Cleaning", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Flower Arrangements for Springtime", "author": "Dorothy J. Roberts", "etype": "article"},
        {"title": "Be a Guest at Your Own Party", "author": "Phyllis Snow", "etype": "article"},
        {"title": "A Book Review", "author": "Gladys K. Wagner", "etype": "article"},
        {"title": "Soup Makes the Meal", "author": "Sara Mills", "etype": "article"},
        {"title": "Pull a New Apron From the Rag Bag", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Immortal", "author": "Sadie O. Clark", "etype": "poem"},
        {"title": "March", "author": "Grace Sayre", "etype": "poem"},
        {"title": "After Deep Winter", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Beyond This Moment", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "To a Fountain", "author": "Dana Benson", "etype": "poem"},
        {"title": "A Square of Grass", "author": "Ida L. Belnap", "etype": "poem"},
        {"title": "Another Spring", "author": "Nyal W. Anderson", "etype": "poem"},
        {"title": "The Silent", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "The Story Hour", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "The Awakening", "author": "Celia Van Cott", "etype": "poem"},
        {"title": "Against the Breath", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "Visiting Teachers", "author": "Eva J. Lillywhite", "etype": "poem"},
    ],

    ("Vol38", "No04_April_1951"): [
        {"title": "Living Water", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "Congratulations to President George Albert Smith on His Eighty-First Birthday", "author": None, "etype": "article"},
        {"title": "Meet Together Often", "author": "Preston Nibley", "etype": "article"},
        {"title": "Enlist in the Fight Against Cancer", "author": "Lucybeth C. Rampton", "etype": "article"},
        {"title": "Herman and the Birthday Dinner", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "A Girl's Point of View", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "My Mother's Daughter", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Now Is the Time", "author": "Carol Read Flake", "etype": "fiction"},
        {"title": "For the Strength of the Hills, Chapter 3", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Therewith to Be Content", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Socials, Bazaars, and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Meals of Our Time", "author": "Sara Mills", "etype": "article"},
        {"title": "Gardens in Pattern", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Favors for Baby Showers", "author": "Clara Laster", "etype": "article"},
        {"title": "Increase That Shelf Space", "author": "Rachel K. Laurgaard", "etype": "article"},
        {"title": "Apple Sauce or Fruitcake", "author": "Alice Bartlett", "etype": "article"},
        {"title": "Jane Bybee Coltrin Makes Quilt Tops as a Hobby", "author": None, "etype": "article"},
        {"title": "A New Outlook", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "April Mood", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Utah Gulls in Spring", "author": "Richard F. Armknecht", "etype": "poem"},
        {"title": "Showered Petals in the Spring", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "New Tenant", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Garden Riches", "author": "Jeanette P. Parry", "etype": "poem"},
        {"title": "Miracle", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Two Sunsets", "author": "Juliaetta B. Jensen", "etype": "poem"},
        {"title": "Anticipation", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "That I May See", "author": "LeRoy Burke Meagher", "etype": "poem"},
        {"title": "April", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Peach Trees in Bloom", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "One Time Glimpse", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Who Has Loved the Earth", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Spring Fashions", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "The Silence", "author": "Margery S. Stewart", "etype": "poem"},
    ],

    ("Vol38", "No05_May_1951"): [
        {"title": "Blue Spring", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "President George Albert Smith", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "First Presidency Reorganized", "author": None, "etype": "article"},
        {"title": "Mother -- Three Pictures", "author": "Stephen L Richards", "etype": "article"},
        {"title": "Joy in Service", "author": "Achsa E. Paxman", "etype": "article"},
        {"title": "Seek After These Things", "author": "Ruth M. McKay", "etype": "article"},
        {"title": "The Spirit of Motherhood", "author": "Hazel McAllister", "etype": "article"},
        {"title": "A Mother's Day Surprise", "author": "Lydia Bennett Egbert", "etype": "fiction"},
        {"title": "For the Strength of the Hills, Chapter 4", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "You Can Learn", "author": "Katherine Kelly", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Safe Harbor of Home", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Magazine Subscriptions for 1950", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Magazine Honor Roll for 1950", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Floral Arrangements for the Home", "author": "Inez R. Allen", "etype": "article"},
        {"title": "Start With Yourself", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "In a Very New Garden", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Dewdrop", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "My Joy", "author": "Gene Romolo", "etype": "poem"},
        {"title": "I Have Returned to the Valley", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "What Shall We Hold?", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "May", "author": "Grace Sayre", "etype": "poem"},
        {"title": "The Ancient Prayer", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Lullaby", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "My Mother", "author": "Mirla Greenwood Thayne", "etype": "poem"},
        {"title": "The Promise", "author": "Maud Miller Cook", "etype": "poem"},
        {"title": "Blossoming", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "The Road Is Marked", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "My Heart Is Bound", "author": "Josephine J. Harvey", "etype": "poem"},
    ],

    ("Vol38", "No06_June_1951"): [
        {"title": "How Much a Heart Can Gather!", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "President David O. McKay -- An Appreciation", "author": "Stephen L Richards", "etype": "article"},
        {"title": "Stephen L Richards Sustained First Counselor in the First Presidency", "author": "John A. Widtsoe", "etype": "article"},
        {"title": "The Most Recent Call of President J. Reuben Clark to Service in the First Presidency", "author": "Henry D. Moyle", "etype": "article"},
        {"title": "Elder Joseph Fielding Smith Sustained as President of the Twelve Apostles", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Tribute to Adele Cannon Howells", "author": "LaVern W. Parmley", "etype": "article"},
        {"title": "Contest Announcements -- 1951", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Let's Write a Poem", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Let's Write a Story", "author": "Alice Morrey Bailey", "etype": "article"},
        {"title": "Nursing -- A Lifetime of Satisfaction", "author": "Elaine Mellor", "etype": "article"},
        {"title": "Conservation Looks to the Future", "author": "Helen C. Payne", "etype": "article"},
        {"title": "Polly Played for Keeps", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "For the Strength of the Hills, Chapter 5", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "How Dad Became a Salesman", "author": "Luzelle S. Eliason", "etype": "fiction"},
        {"title": "Christine's Extravagance", "author": "Beth Bernice Johnson", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Blessing of Work", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "In Memoriam -- Emma Lucy Gates Bowen", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Let's Bake Bread", "author": "Josie B. Bay", "etype": "article"},
        {"title": "Bad Habits Are Like Weeds", "author": "Naomi B. MacCabe", "etype": "article"},
        {"title": "June", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Charmer", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Sunday in the Country", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Yet Man Heeds Not", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Fragrant Memories", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Night From Capital Hill", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "Summer Noon", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Creative Gardener", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Question", "author": "Matia McClelland Burk", "etype": "poem"},
        {"title": "Lost Love", "author": "Nelouise Fisher Judd", "etype": "poem"},
        {"title": "Mother and Daughter", "author": "Genevieve J. VanWagenen", "etype": "poem"},
        {"title": "Only the Bees Remain", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "The Unraveling", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "I Love My Little Kitchen", "author": "Lydia B. Egbert", "etype": "poem"},
    ],

    ("Vol38", "No07_July_1951"): [
        {"title": "Hill Treasure", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Fasting and the Fast Day", "author": "Milton R. Hunter", "etype": "article"},
        {"title": "In Memoriam -- Sister Augusta Winters Grant", "author": None, "etype": "article"},
        {"title": "The Family Hour With Grandmother Who Lives Alone", "author": "Rosetta Wallace Bennett", "etype": "article"},
        {"title": "Vacation in Mexico", "author": "Olive W. Burt", "etype": "article"},
        {"title": "A Vacation for Mother", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "For the Strength of the Hills, Chapter 6", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Celestia", "author": "Agnes C. Beebe", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Covenant of Freedom", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Relief Society Congratulates Newly Appointed Primary Presidency", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "New Ways With Vegetables", "author": "Evelyn Hansen", "etype": "article"},
        {"title": "An Inexpensive Outfit for the Baby", "author": "Helen Martin", "etype": "article"},
        {"title": "Graham Crackers for Summertime Desserts", "author": "Sara Mills", "etype": "article"},
        {"title": "These Things Are Ours Forever", "author": "Betty Zieve", "etype": "article"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
        {"title": "Theology: Characters and Teachings of the Book of Mormon, Preview", "author": None, "etype": "lesson"},
        {"title": "Theology: Origin and Analysis of The Book of Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Book of Mormon Gems of Truth, Preview", "author": None, "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Behold, I Tell You These Things", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Sewing -- The Art of Mending, Preview", "author": None, "etype": "lesson"},
        {"title": "Work Meeting: Introductory", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: The Literature of England, Preview", "author": None, "etype": "lesson"},
        {"title": "Literature: Introduction to Romanticism", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Progress of Man, Preview", "author": None, "etype": "lesson"},
        {"title": "Social Science: Apostasy and Degeneration", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Fundamentals of Musicianship, Preview", "author": None, "etype": "lesson"},
        {"title": "Music: Conducting, Accompanying, and Program Planning", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Splendid Hour", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "The Gardener", "author": "Grace Sayre", "etype": "poem"},
        {"title": "On Petals Falling", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "With Him So Near", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Father's Shoes", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Golden Wedding", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Summer Storm", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Compensation", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Growing Days", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "Gypsy Soul", "author": "Grace B. Wilson", "etype": "poem"},
        {"title": "Light and Shadows", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "First Steps", "author": "Bernice T. Clayton", "etype": "poem"},
        {"title": "Desert Scene", "author": "Bertha Cragun", "etype": "poem"},
        {"title": "Prayer", "author": "Virginia L. Morris", "etype": "poem"},
        {"title": "On Old Faces", "author": "Mabel Law Atkinson", "etype": "poem"},
    ],

    ("Vol38", "No08_August_1951"): [
        {"title": "Before I Sleep", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "The Constitution and the United Nations Charter", "author": "Albert E. Bowen", "etype": "article"},
        {"title": "Book of Mormon Witnesses and Their Testimony Meet Legal Standards", "author": "David J. Wilson", "etype": "article"},
        {"title": "Through This Door, Chapter 1", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "The Silken Bond", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Cease to Find Fault One With Another", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Can When You Can't", "author": "Angelyn W. Wadley", "etype": "article"},
        {"title": "Jam and Jelly Making -- An Art", "author": "Ethel C. Smith", "etype": "article"},
        {"title": "Oil Paintings", "author": "Helen H. Jones", "etype": "article"},
        {"title": "Boxes For Beauty", "author": "Celia L. Luce", "etype": "article"},
        {"title": "How to Make Fluffy Rugs", "author": "Beatrice Squires Poelman", "etype": "article"},
        {"title": "Diana Ellsworth Layton Enjoys Painting and Quiltmaking Hobbies", "author": "Thelma L. Hunsaker", "etype": "article"},
        {"title": "Salad Days", "author": "Sara Mills", "etype": "article"},
        {"title": "Waver Not", "author": "Kate Richards", "etype": "article"},
        {"title": "Theology: The Witnesses to The Book of Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Remember That My Spirit Will Not Always Strive With Man", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Patching", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: Two Pre-Romantic Poets: James Thomson and William Collins", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Revival of Learning", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Efficiency in Teaching and Practicing Hymns and Anthems", "author": "Florence Jepperson Madsen", "etype": "lesson"},
        {"title": "To Teachers", "author": "Agda Gronbech Harlow", "etype": "poem"},
        {"title": "August", "author": "Lella N. Smith", "etype": "poem"},
        {"title": "Heartbeat", "author": "Phyllis Juhlin Park", "etype": "poem"},
        {"title": "Deep in This Night", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Among the Ferns", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Song", "author": "Gene Romolo", "etype": "poem"},
        {"title": "New Horizons", "author": "Margaret Smith Larson", "etype": "poem"},
        {"title": "Poppies in Summer", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Prayer", "author": "Mary Gustafson", "etype": "poem"},
    ],

    ("Vol38", "No09_September_1951"): [
        {"title": "Desert Balm", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "The Constitution and the United Nations Charter: II. Rights Guaranteed by the Constitution", "author": "Albert E. Bowen", "etype": "article"},
        {"title": "Cathedral Valley", "author": "Winifred N. Jones", "etype": "article"},
        {"title": "Broken Keys", "author": "Blanche Sutherland", "etype": "fiction"},
        {"title": "Powder Puffs", "author": "Sylvia Pezolt", "etype": "fiction"},
        {"title": "Through This Door, Chapter 2", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: What Shall We Read to Children?", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Annual Report -- 1950", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "How to Make Continuous Bias", "author": "Eva Ricks", "etype": "article"},
        {"title": "Theology: The Migration", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Feast Upon the Words of Christ", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Strengthening Ready-Mades", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "A Good Recipe for Work Meeting Luncheons", "author": "Christine Eaton", "etype": "article"},
        {"title": "Literature: Two Other Pre-Romantic Poets: Thomas Gray and William Cowper", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Music: Increased Proficiency in Conducting and Hymn Singing", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "My Wish", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Eternal Enchantment", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Goodbye Little Boy", "author": "Angelyn W. Wadley", "etype": "poem"},
        {"title": "Boys in September", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "From Some Far Yesterday", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Healing", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "All Lowly Flowers", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Lying in the Sun", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Summer Turning", "author": "Ing Smith", "etype": "poem"},
    ],

    ("Vol38", "No10_October_1951"): [
        {"title": "October's Dress", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Amber Clock and Desk Set Presented to Relief Society by the Sisters of the West German Mission", "author": None, "etype": "article"},
        {"title": "Gifts From the Swiss and Japanese Relief Society Sisters", "author": None, "etype": "article"},
        {"title": "The Constitution and the United Nations Charter: III. Some Possible Effects of World Government Upon Constitutional Liberty", "author": "Albert E. Bowen", "etype": "article"},
        {"title": "This Is the Way, Walk Ye in It", "author": "James R. Clark", "etype": "article"},
        {"title": "Home -- Our First Line of Defense", "author": "William H. Boyle", "etype": "article"},
        {"title": "Every Step of the Way", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Mirror, Mirror", "author": "Marjorie Linthurst", "etype": "fiction"},
        {"title": "Perfect Sunday", "author": "Elaine Swain", "etype": "fiction"},
        {"title": "Through This Door, Chapter 3", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Sabbath for Man", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "The Family Reunion", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "How to Make an Angora Baby Bonnet", "author": "Marian Richards Boyer", "etype": "article"},
        {"title": "Mother's Cookbook", "author": "Norma Wrathall", "etype": "article"},
        {"title": "Theology: The Migration (Continued)", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Look Unto God With Firmness", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Knitted Goods", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: Robert Burns", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Religious Reformation", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Review of the Four-four Baton Pattern", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Alien to the City", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Autumn Morning", "author": "Lydia Hall", "etype": "poem"},
        {"title": "Kitchen", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "My Faith", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Summer Is Over", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Of Loss", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Valley Pheasants", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Autumn", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "Monument", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Anniversaries", "author": "Evelyn Wooster Viner", "etype": "poem"},
        {"title": "As Colors Move", "author": "Margaret B. Shomaker", "etype": "poem"},
    ],

    ("Vol38", "No11_November_1951"): [
        {"title": "Finds the Canyon", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "If You Live Up to Your Privileges", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "The Constitution and the United Nations Charter: IV. Some Effects of World Government Upon Constitutional Liberty", "author": "Albert E. Bowen", "etype": "article"},
        {"title": "A Faith-Promoting Memory", "author": "Esther L. Warburton", "etype": "article"},
        {"title": "Such Stuff", "author": "Leone E. McCune", "etype": "fiction"},
        {"title": "Hour of Decision", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Through This Door, Chapter 4", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Beginning of Harvest", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Watch This Date: It Is Important", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Just One Step at a Time", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Preparing Your Child for School and for Life", "author": "Celia Luce", "etype": "article"},
        {"title": "Puddings for Thanksgiving", "author": None, "etype": "article"},
        {"title": "Minnie Campion Has Music for a Hobby", "author": None, "etype": "article"},
        {"title": "Add Sparkle and Zest to Your Menus", "author": "Winifred Wilkinson", "etype": "article"},
        {"title": "The Neglected Art of Photo-Coloring", "author": "Clara Laster", "etype": "article"},
        {"title": "Grow Old Splendidly", "author": "Evelyn Wooster Viner", "etype": "article"},
        {"title": "Work Meeting: Mending Woolens", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: William Blake", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Struggle for Independence", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Music Appreciation Programs for Relief Society and the Home", "author": "Florence Jepperson Madsen", "etype": "lesson"},
        {"title": "November Wind", "author": "Zera Wilde Earl", "etype": "poem"},
        {"title": "The Lost Words", "author": "Dorothy J. Roberts", "etype": "poem"},
    ],

    ("Vol38", "No12_December_1951"): [
        {"title": "Song for Midnight", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Responsibility of Relief Society Members to Their Homes and the Priesthood", "author": "Stephen L Richards", "etype": "article"},
        {"title": "Marion George Romney -- Our New Apostle", "author": "Harold B. Lee", "etype": "article"},
        {"title": "New Appointees to Be Assistants to the Council of the Twelve", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society and the Church Welfare Program", "author": "Marion G. Romney", "etype": "article"},
        {"title": "The Annual General Relief Society Conference", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Christmas at Welcome Inn", "author": "Pansye H. Powell", "etype": "fiction"},
        {"title": "Through This Door, Chapter 5", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Another Mary", "author": "Mary R. Ross", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorials: The One Hundred Twenty-second Semi-annual Conference of the Church", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Ernest L. Wilkinson Inaugurated President of Brigham Young University", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Importance of Articles on The Constitution and the United Nations Charter", "author": None, "etype": "article"},
        {"title": "Notes to the Field: New Record Book for Ward Magazine Representatives", "author": None, "etype": "article"},
        {"title": "Use of Material for Theology Lessons", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Recipes for Christmas", "author": "Ethel C. Smith", "etype": "article"},
        {"title": "Planning a Christmas Table", "author": "Marian R. Boyer", "etype": "article"},
        {"title": "Christmas Gift Wrapping Is Fun", "author": "Florence S. Jacobsen", "etype": "article"},
        {"title": "The Gift Without the Giver", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "A Christmas Star Tree", "author": "Bertha Lorentzen", "etype": "article"},
        {"title": "Theology: Coriantumr and Ether", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Now It Is Better That a Man Should Be Judged of God", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Mending Men's Suits", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: Two Romantic Essayists: Lamb and Hazlitt", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: New Races in America", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Conducting With the Six-eight Baton Pattern", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Breviary", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "Let the Holly Wreath Be Hung", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "The Gentle Artisan", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "December Has Her Loveliness", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Winter Witchery", "author": "Alice Whitson Norton", "etype": "poem"},
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
    year = 1951

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol38_{key}.txt"
    ISSUE_FILES[("Vol38", key)] = (fname, month)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# OCR initial-letter substitution table.
# The OCR in Vol38 consistently misreads decorative/large-font initials:
#   Th -> Sh, Ch       T -> S, J, (       F -> St
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
# Known OCR-mangled section headers for Vol38.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
_STRENGTH_HILLS_CHAPTER_PAT = (
    r"(?:F|St)or\s+(?:th|sh|ch)e\s+(?:S|J|\()?trength\s+of\s+(?:th|sh|ch)e\s+Hills"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)
_THROUGH_DOOR_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)rough\s+(?:Th|Sh|Ch)is\s+Door"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—Characters and Teachings of The Book of Mormon Lesson NN—"SUBTITLE"
    # or: Sheology—The Life and Ministry of the Savior Lesson NN—"SUBTITLE"
    # TOC has: Theology: SUBTITLE
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Visiting Seacher ITlessages—Book of Mormon Gems of Truth Lesson NN—"SUBTITLE"
    # or: Ussiting Seacher Tlessages—Our Savior Speaks Lesson NN—"SUBTITLE"
    # TOC has: Visiting Teacher Messages: SUBTITLE
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:T|S)(?:e|l)(?:a|e)cher\s+"
        r"(?:M|IT|T|m)(?:l)?essages"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|]?)?"
    ),
    "From Near and Far": (
        r"(?:F|St)rom\s+Near\s+and\s+(?:F|S)ar"
    ),
    # Body has: EDITORIAL o~9 VOL. 38 MONTH 1951 NO. N <actual title>
    # TOC has: Editorial: <actual title>
    "Editorial: ": (
        r"EDITORIAL"
        r"[\s\S]{0,80}?"
    ),
    "Editorials: ": (
        r"EDITORIAL[S]?"
        r"[\s\S]{0,80}?"
    ),
    # Body has: OLiterature—The Literature of England Lesson NN—SUBTITLE
    # or: Ie eagle — ine Literature of England Lesson NN—SUBTITLE
    # TOC has: Literature: SUBTITLE
    "Literature: ": (
        r"(?:(?:Ie\s+eagle\s*[\-\u2014\u2013]*\s*ine\s+)|(?:O|Of?\s+i|of\s+i)?)?"
        r"[Ll]?iterature"
        r"[\s\-\u2014\u2013:]*"
        r"(?:(?:Th|Sh|Ch)e\s+Literature\s+of\s+England)?"
        r"[\s\S]{0,60}?"
    ),
    # Body has: Social Sctence—The Progress of Man Part N—SUBTITLE Lesson NN—SUBTITLE
    # TOC has: Social Science: SUBTITLE
    "Social Science: ": (
        r"Social\s+Sc(?:i|t)ence"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Work ITleeting—Sewing THE Art oF MENDING Lesson NN—SUBTITLE
    # or: Work Tleeting—The Art of Homemaking ... Lesson NN—SUBTITLE
    # TOC has: Work Meeting: SUBTITLE
    "Work Meeting: ": (
        r"Work\s+(?:IT|T|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St)rom\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    # Music: body has Nlustec/Vlusic/Mlusic/Lusic/Nlusic/emic/IT lusec — Fundamentals of Musicianship ...
    # TOC has: Music: SUBTITLE
    "Music: ": (
        r"(?:N(?:l)?ust(?:e|i)c|Vlusic|Mlusic|Lusic|Nlusic|emic|IT\s*lusec|Music)"
        r"[\s\-\u2014\u2013:]*"
        r"(?:Fun(?:s|d)?(?:l)?amentals\s+of\s+Musicianship)?"
        r"[\s\S]{0,120}?"
        r"(?:Lesson\s+\d+|Preview)?"
        r"[\s\-\u2014\u2013]*"
    ),
    # Serial fiction: For the Strength of the Hills
    "For the Strength of the Hills, Chapter 1": _STRENGTH_HILLS_CHAPTER_PAT + r"(?:1|I)\b",
    "For the Strength of the Hills, Chapter 2": _STRENGTH_HILLS_CHAPTER_PAT + r"(?:2|II)\b",
    "For the Strength of the Hills, Chapter 3": _STRENGTH_HILLS_CHAPTER_PAT + r"(?:3|III)\b",
    "For the Strength of the Hills, Chapter 4": _STRENGTH_HILLS_CHAPTER_PAT + r"(?:4|IV)\b",
    "For the Strength of the Hills, Chapter 5": _STRENGTH_HILLS_CHAPTER_PAT + r"(?:5|V)\b",
    "For the Strength of the Hills, Chapter 6": (
        _STRENGTH_HILLS_CHAPTER_PAT + r"(?:6|VI)\b"
    ),
    # Serial fiction: Through This Door
    "Through This Door, Chapter 1": _THROUGH_DOOR_CHAPTER_PAT + r"(?:1|I)\b",
    "Through This Door, Chapter 2": _THROUGH_DOOR_CHAPTER_PAT + r"(?:2|II)\b",
    "Through This Door, Chapter 3": _THROUGH_DOOR_CHAPTER_PAT + r"(?:3|III)\b",
    "Through This Door, Chapter 4": _THROUGH_DOOR_CHAPTER_PAT + r"(?:4|IV)\b",
    "Through This Door, Chapter 5": _THROUGH_DOOR_CHAPTER_PAT + r"(?:5|V)\b",
}


def build_regex_for_title(title: str) -> re.Pattern:
    """Build a flexible regex pattern for matching a title in OCR'd body text."""
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
            parts.append(r',?\s*')
        elif ch == '?':
            parts.append(r'\??')
        elif ch == '!':
            parts.append(r'!?')
        elif ch == ' ':
            # Allow optional comma/semicolon before whitespace and optional
            # stray opening paren (OCR artifact)
            parts.append(r'[,;]?\s+\(?')
            at_word_start = True
        elif at_word_start and ch.isalpha():
            two = title[i:i+2]
            if two in _OCR_WORD_START_ALTS:
                parts.append(r'f?' + _OCR_WORD_START_ALTS[two])
                i += 2
                at_word_start = False
                continue
            if ch in _OCR_SINGLE_CHAR_ALTS:
                parts.append(r'f?' + _OCR_SINGLE_CHAR_ALTS[ch])
            else:
                # Allow optional stray 'f' prefix from OCR artifacts
                parts.append(r'f?' + re.escape(ch))
            at_word_start = False
        else:
            parts.append(re.escape(ch))
            at_word_start = False

        i += 1

    return ''.join(parts)


def strip_running_noise(text: str) -> tuple[str, list[str]]:
    """Remove running headers/footers and page numbers."""
    lines = text.split('\n')
    cleaned = []
    noise = []

    for line in lines:
        line = re.sub(r'^(Page\s+)?(\d+)\s*$', '', line, flags=re.IGNORECASE).strip()
        if line and len(line) < 100 and line.isupper():
            noise.append(line)
        elif line:
            cleaned.append(line)

    return '\n'.join(cleaned), noise


def find_ads_section(body: str) -> str:
    """Discover ads text and return it."""
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
    """Split text into front matter (TOC, etc.) and body."""
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
    """Match all entries in the body."""
    found = []

    for entry in entries:
        pattern = build_regex_for_title(entry["title"])
        match = pattern.search(body)

        if match:
            found.append((match.start(), entry))

    return found


def _boundaries_from_found(body: str, found: list[tuple[int, dict]]) -> list[tuple[int, int, dict]]:
    """Convert (position, entry) list into (start, end, entry) boundaries."""
    if not found:
        return []

    found = sorted(found, key=lambda x: x[0])

    bounds = []
    for i, (pos, entry) in enumerate(found):
        start = pos
        end = found[i + 1][0] if i + 1 < len(found) else len(body)
        bounds.append((start, end, entry))

    return bounds


def extract_toc_from_front_matter(front_matter: str) -> tuple[str, str]:
    """Extract TOC section from front matter."""
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
    """Extract a single issue's text into individual entry files."""
    front_matter, body = split_front_matter(text)

    ads_text = find_ads_section(body)

    found = _match_entries_with_strategy(body, entries)
    bounds = _boundaries_from_found(body, found)

    by_title = {e["title"]: (s, nd) for s, nd, e in bounds}

    stats = {"matched": 0, "misc_bytes": 0,
             "total_bytes": len(text.encode("utf-8")),
             "manifest_rows": []}

    issue_dir = output_dir / vol / month
    rel_dir = f"processed/regex_and_llm/{vol}/{month}"
    if not dry_run:
        issue_dir.mkdir(parents=True, exist_ok=True)

    all_noise = []
    covered_intervals = []
    json_entries = []

    title_order = [e["title"] for _, _, e in bounds]

    entry_by_title = {e["title"]: e for e in entries}

    for idx, title in enumerate(title_order, 1):
        entry = entry_by_title.get(title)
        if not entry:
            continue

        title_safe = sanitize_filename(entry["title"])

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

            matched_label = "matched"
            chars = len(match_result["content"])
            print(f"  [{matched_label:12s}] #{idx:02d} "
                  f"chars={chars} "
                  f"{entry['title'][:50]}")
        else:
            print(f"  WARNING: No match for '{entry['title']}' in body text")

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

    misc_parts = []

    if remaining_fm.strip():
        misc_parts.append(remaining_fm.strip())

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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol38"
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
        description="Extract Relief Society Magazine Vol 38 into individual entries"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without creating files")
    parser.add_argument("--use-raw-data", action="store_true",
                        help="Use raw-data instead of cleaned-data")
    args = parser.parse_args()

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

    for (vol, issue_key), entries in VOL38_TOC.items():
        if (vol, issue_key) not in ISSUE_FILES:
            print(f"WARNING: No file mapping for ({vol}, {issue_key}), skipping")
            continue

        filename, month = ISSUE_FILES[(vol, issue_key)]
        source_path = data_dir / vol / filename

        if not source_path.exists():
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

        if out_vol not in volume_json:
            volume_json[out_vol] = {"volume": out_vol, "months": {}}
        volume_json[out_vol]["months"][month] = stats["month_json"]

        coverage = ((stats["total_bytes"] - stats["misc_bytes"]) / stats["total_bytes"] * 100
                     if stats["total_bytes"] > 0 else 0)
        print(f"  Entries matched: {stats['matched']}")
        print(f"  Coverage: {coverage:.1f}%")
        print(f"  Misc bytes: {stats['misc_bytes']}")

    if not args.dry_run:
        for vol, data in volume_json.items():
            vol_dir = OUTPUT_DIR / vol
            vol_dir.mkdir(parents=True, exist_ok=True)

            json_path = vol_dir / f"{vol}_entries.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            print(f"\nJSON written: {json_path}")

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
