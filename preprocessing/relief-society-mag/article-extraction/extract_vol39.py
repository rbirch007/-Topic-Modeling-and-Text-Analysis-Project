#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 39 (1952).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol39.py
    python extract_vol39.py --dry-run
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

VOL39_TOC = {
    ("Vol39", "No01_January_1952"): [
        {"title": "A New Year", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "A Birthday Greeting", "author": "General Presidency of Relief Society", "etype": "article"},
        {"title": "Purpose of Writing The Progress of Man", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Latter-day Saint Education to Build Faith", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Award Winners: Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Shine Softly, Stars", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Words", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Barren Woman's Cry", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Award Winners: Annual Relief Society Story Contest", "author": None, "etype": "article"},
        {"title": "Biographical Sketches of Award Winners", "author": None, "etype": "article"},
        {"title": "Pride of the Indian", "author": "Mabel S. Harmer", "etype": "fiction"},
        {"title": "Sarah, Where Are You?", "author": None, "etype": "fiction"},
        {"title": "Uncertain Possession, Chapter 1", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Best Foot Forward", "author": "Jeanette McKay Morrell", "etype": "fiction"},
        {"title": "Salt of the Earth", "author": "Cecil G. Pugmire", "etype": "fiction"},
        {"title": "Through This Door, Chapter 6", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: We Seek After These Things", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Bound Volumes of 1951 Relief Society Magazines", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Protect Your Child", "author": "Celia Luce", "etype": "article"},
        {"title": "Theology: The Formation and Dispersion of the House of Israel", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: I Have None Other Object", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Sleeves, Underarm Patches, and Mending Sheers", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: William Wordsworth", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Groping Toward Liberty of Conscience", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Review of the 6/8 Baton Pattern", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Resolution", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Journey", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Stars Are Better", "author": "Bernice T. Clayton", "etype": "poem"},
        {"title": "Renewal", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Dearly Beloved", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Now and Then", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "View From a Hill", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Love Eternal", "author": "Dorothy R. Salyer", "etype": "poem"},
    ],

    ("Vol39", "No02_February_1952"): [
        {"title": "Seed of Laman", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "The Place of Relief Society in the Indian Program", "author": "Delbert Leon Stapley", "etype": "article"},
        {"title": "Teach the Lamanites the Gospel of Jesus Christ", "author": "Spencer W. Kimball", "etype": "article"},
        {"title": "Loving and Working With Our Lamanite Sisters", "author": "Myrle Fowler", "etype": "article"},
        {"title": "No Tears, Beloved", "author": "Blanche Kendall McKey", "etype": "fiction"},
        {"title": "The House With the Blue Roof", "author": "Hannah Smith", "etype": "fiction"},
        {"title": "To Thine Own Self", "author": "Kay Islaub", "etype": "fiction"},
        {"title": "Uncertain Possession, Chapter 2", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Not After the Manner of the World", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Congratulations to President Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Good Afternoon, My Lady", "author": "Mirla Greenwood Thayne", "etype": "article"},
        {"title": "I Made My Own Family Heirlooms", "author": "M. Garrett Enos", "etype": "article"},
        {"title": "Her Hobby Brings Happiness", "author": None, "etype": "article"},
        {"title": "Thoughts On Patience", "author": "Florence S. Glines", "etype": "article"},
        {"title": "A Little World or a Big One", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Theology: Lehi, Man of Visions", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: He That Will Not Believe My Words", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Linens and Household Articles", "author": "Jean Ridges Jennings", "etype": "lesson"},
        {"title": "Literature: William Wordsworth (Concluded)", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The American Revolution", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Music: Joy and Service Through Singing and Playing", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Afterglow", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Vast Horizons Calling", "author": "Harriette Grace Eaton", "etype": "poem"},
        {"title": "Jonquil in February", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "My People", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Science", "author": "Gertrude Kovan", "etype": "poem"},
        {"title": "Elms in Winter", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Forgotten Ladder", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Storm-Bent Pine", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Advice to a Daughter", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Spring", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Household Hint", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Where Drift Logs Lie", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Promise and Fulfillment", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Night Encounter", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol39", "No03_March_1952"): [
        {"title": "The Women of Nauvoo", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Relief Society in the Lives of Latter-day Saint Women", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Individual Influence and Responsibility", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "The Spirit of Relief Society", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Truth Beareth Record of Truth", "author": "Lillie C. Adams", "etype": "article"},
        {"title": "Relief Society and the Gospel Message", "author": "Sai Lang Aki", "etype": "article"},
        {"title": "The Red Cross and Its Field of Service", "author": None, "etype": "article"},
        {"title": "A Price for Wheat", "author": "Angelyn W. Wadley", "etype": "article"},
        {"title": "The Least of These", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Dear Conquest, Part I", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "The Wearing of the Green", "author": "Frances Carter Yost", "etype": "fiction"},
        {"title": "Room for Phyllis", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Uncertain Possession, Chapter 3", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Message to Relief Society", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "In Memoriam -- Martha Jones Ballard", "author": None, "etype": "article"},
        {"title": "Announcing the April Special Short Story Issue", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Organizations and Reorganizations", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "House Plants", "author": "Dorthea N. Newbold", "etype": "article"},
        {"title": "The Crowning Touch", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Gardening -- Food for the Soul", "author": "Pauline M. Henderson", "etype": "article"},
        {"title": "Multiple Hobbies Make Her Happy", "author": None, "etype": "article"},
        {"title": "A Family Is What You Make It", "author": "Elsie Sim Hansen", "etype": "article"},
        {"title": "Let's Stuff a Rug", "author": "Thalia Black", "etype": "article"},
        {"title": "Hymn for Afternoon", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "March Afternoon", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Last Snow", "author": "Ouida J. Pedersen", "etype": "poem"},
        {"title": "Royal Raiment", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Valley Hills", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "All Her Ways Are Peace", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Patriarch", "author": "Nellie B. Brenchley", "etype": "poem"},
        {"title": "Command", "author": "Matia McClelland Burk", "etype": "poem"},
        {"title": "Sound", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Old Road", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Valley Harbor", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Words for Spring", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Inefficacy", "author": "Hazel M. Thomson", "etype": "poem"},
        {"title": "Twilight Hour", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Faded Memory", "author": "Alice R. Rich", "etype": "poem"},
        {"title": "Lovely Things", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Gift From Spring", "author": "Ora Lee Parthesius", "etype": "poem"},
    ],

    ("Vol39", "No04_April_1952"): [
        {"title": "Concerto", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "The Women and the Resurrection", "author": "Matthew Cowley", "etype": "article"},
        {"title": "Elder Joseph F. Merrill", "author": "Ezra Taft Benson", "etype": "article"},
        {"title": "The Family Hour", "author": "Alberta O. and Roy W. Doxey", "etype": "article"},
        {"title": "Save These Lives", "author": None, "etype": "article"},
        {"title": "Smitty", "author": "Maryhale Woolsey", "etype": "fiction"},
        {"title": "Father Was a Good Provider", "author": "Inez Bagnell", "etype": "fiction"},
        {"title": "A Frame for Happiness", "author": "Ora Pate Stewart", "etype": "fiction"},
        {"title": "Our April Short Story Writers", "author": None, "etype": "article"},
        {"title": "Dear Conquest, Part II", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Uncertain Possession, Chapter 4", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Tabernacle in the Wilderness", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Delay in Magazine Subscriptions", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Your Flower Garden", "author": "Dorthea Newbold", "etype": "article"},
        {"title": "The Second Spring", "author": "Elsie Sim Hansen", "etype": "article"},
        {"title": "Two Hobbies -- Music and Needlecraft", "author": None, "etype": "article"},
        {"title": "Low-Cost Meat and Other Protein Dishes", "author": "Elna Miller", "etype": "article"},
        {"title": "A New Life for Old Greeting Cards", "author": "Clara Laster", "etype": "article"},
        {"title": "Better Looking Patches -- Quicker", "author": "Thalia Black", "etype": "article"},
        {"title": "Easter", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Do You Remember?", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Easter on the Hills", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Exhortation", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Holiness", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Storm Over the Valley", "author": "Dorothy J. Roberts", "etype": "poem"},
    ],

    ("Vol39", "No05_May_1952"): [
        {"title": "Overtones", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "The Spirit of a Mother's Day", "author": "Mildred B. Eyring", "etype": "article"},
        {"title": "I Am Partial to Maytime", "author": "Grace M. Candland", "etype": "article"},
        {"title": "Things My Mother Taught Me", "author": "Sylvia Probst Young", "etype": "article"},
        {"title": "A Tribute to Miss Tracy", "author": "Edna Rae Madsen", "etype": "article"},
        {"title": "It Happened One Day", "author": "Thelma Hatch", "etype": "article"},
        {"title": "Every Night at Our House", "author": "Matia McClelland Burk", "etype": "article"},
        {"title": "Uncertain Possession, Chapter 5", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Dear Conquest, Part III", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Pursuit", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Magazine Subscriptions for 1951", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Lily Pool -- Rock Garden Combination", "author": "Willard Luce", "etype": "article"},
        {"title": "Sharing Sunshine", "author": "Helen A. Nielsen", "etype": "article"},
        {"title": "A Needle and Thread Hobby", "author": "Lila B. Smith", "etype": "article"},
        {"title": "Recipe for American Chop Suey", "author": "Alice Morrey Bailey", "etype": "article"},
        {"title": "Forever", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Not Happiness Alone", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Revelation", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Our Young Mother", "author": "Wanda E. Rhodes", "etype": "poem"},
        {"title": "Morning", "author": "Doris Riter", "etype": "poem"},
        {"title": "A Portrait", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Some Spring", "author": "Harriett L. George", "etype": "poem"},
        {"title": "A Certain Pledge", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Re-Entrance of Spring", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Kindness", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Invited Compensation", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "Desert Spring", "author": "Jessie M. Robinson", "etype": "poem"},
    ],

    ("Vol39", "No06_June_1952"): [
        {"title": "At the Final Moment", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "An Appreciation of The Book of Mormon", "author": "Leland H. Monson", "etype": "article"},
        {"title": "Here Am I; Send Me", "author": "Joseph L. Wirthlin", "etype": "article"},
        {"title": "The Church Sustains a New Presiding Bishopric", "author": None, "etype": "article"},
        {"title": "Contest Announcements -- 1952", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Creation of Poetry", "author": "Margery S. Stewart", "etype": "article"},
        {"title": "Basic Recipe for a Short Story", "author": "Martha Robeson Wright", "etype": "article"},
        {"title": "Vacation Plans", "author": "Dorothy Boys Killian", "etype": "fiction"},
        {"title": "Uncertain Possession, Chapter 6", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Dear Conquest, Part IV", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 122d Annual Church Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Theology: Characters and Teachings of The Book of Mormon, Preview", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Book of Mormon Gems of Truth, Preview", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Home Management, Preview", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: The Literature of England, Preview", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Progress of Man, Preview", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
        {"title": "Adventure in Glass", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Picnic Meals", "author": "Evelyn Hansen", "etype": "article"},
        {"title": "June", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Wind in the Pine Trees", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Mountain Cabin", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Summer Walk", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Lonesome Things", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Beauty Lives in the Heart", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Alchemy", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "A Flower's Way", "author": "Evelyn Fjeldsted", "etype": "poem"},
    ],

    ("Vol39", "No07_July_1952"): [
        {"title": "Westward in This Era", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Girders of Strength", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Canadian Rockies Cradle Church Settlements", "author": "Claire W. Noall", "etype": "article"},
        {"title": "A Time to Forget, Chapter 1", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "A Message in Action", "author": "Maggie Tolman Porter", "etype": "fiction"},
        {"title": "Uncertain Possession, Chapter 7", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Narrow Room", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "What Might Have Been", "author": "Winona Powers", "etype": "article"},
        {"title": "We Are Seven", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "The Tale of a Shirt", "author": "Mary R. Reynolds", "etype": "article"},
        {"title": "Adventure in Glass", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "By Early Candlelight", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Theology: Preparation for the Journey", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Wherefore, Do Not Spend Money for That Which Is of No Worth", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: The Management of My Home", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Samuel Taylor Coleridge", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Moment on a Plateau", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Pattern for Loveliness", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Landholder", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Far Echo", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Sudden Flight", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Rosepetal Carpet", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Deeper Images", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "The Summer-Hearted", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Memory", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Serenity", "author": "Maude O. Cook", "etype": "poem"},
    ],

    ("Vol39", "No08_August_1952"): [
        {"title": "Dry Season", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Place of Brigham Young University in Church Education", "author": "Ernest L. Wilkinson", "etype": "article"},
        {"title": "Fisher Towers", "author": "Celia Luce", "etype": "article"},
        {"title": "Papa Took Us Fishing", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Real Estate", "author": "Luzelle S. Eliason", "etype": "fiction"},
        {"title": "A Prairie Breeze, A Prairie Fire", "author": "Agnes C. Beebe", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 2", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Uncertain Possession, Chapter 8", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Present Day Need for Nurses", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Adventure in Glass", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "How to Renew an Old Suit", "author": "Thalia Black", "etype": "article"},
        {"title": "Children Can Cook", "author": "Melba S. Payne", "etype": "article"},
        {"title": "Theology: Journeying in the Land of Promise", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: But to Be Learned Is Good", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: The Use of Time", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Coleridge the Poet", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The British Constitution", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Dispossessed", "author": "Blanche Sutherland", "etype": "poem"},
        {"title": "Today", "author": "Annie M. Ellsworth", "etype": "poem"},
        {"title": "Remembered Inspiration", "author": "Maud Miller Cook", "etype": "poem"},
        {"title": "Affinity", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Of My Own Asking", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Home Light", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Think of Me Sometimes", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "When Morning Comes", "author": "Evelyn Wooster Viner", "etype": "poem"},
        {"title": "Nocturne", "author": "Ruth Harwood", "etype": "poem"},
        {"title": "No Word", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Summer Prayer", "author": "Gene Romolo", "etype": "poem"},
        {"title": "To a Teacher", "author": "Alta J. Vance", "etype": "poem"},
        {"title": "Garden Romance", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "In Unity", "author": "Maude O. Cook", "etype": "poem"},
    ],

    ("Vol39", "No09_September_1952"): [
        {"title": "Aftermath of Summer", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Decisions and Free Agency", "author": "Marion G. Romney", "etype": "article"},
        {"title": "A Case for Martha, Part I", "author": "Hannah Smith", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 3", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Heritage of a Day", "author": "Marjorie Linthurst", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Vote in the November Election", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Annual Report -- 1951", "author": None, "etype": "article"},
        {"title": "Your Child Is an Individual", "author": "Celia Luce", "etype": "article"},
        {"title": "Adventure in Glass", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Theology: The Value of Records", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: I Work Not Among the Children of Men Save It Be According to Their Faith", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Using Energy Wisely", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Sir Walter Scott", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Sublimity", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Autumn", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "In Lieu of Brush", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Opportunity", "author": "Grace Sayre", "etype": "poem"},
        {"title": "To My Absent Neighbor", "author": "Zera Wilde Earl", "etype": "poem"},
        {"title": "A Magic in Making Jelly", "author": "Norma Morris", "etype": "poem"},
        {"title": "Precaution", "author": "Pansye H. Powell", "etype": "poem"},
    ],

    ("Vol39", "No10_October_1952"): [
        {"title": "Autumn Drive", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Evils and Designs of Conspiring Men in the Last Days", "author": "LeRoy A. Wirthlin", "etype": "article"},
        {"title": "Our Land", "author": "Ida Isaacson", "etype": "article"},
        {"title": "New Values in the Family Hour", "author": "Fay Knight Jones", "etype": "article"},
        {"title": "Uncertain Heart", "author": "Leone E. McCune", "etype": "fiction"},
        {"title": "A Case for Martha, Part II", "author": "Hannah Smith", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 4", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Seedtime and Harvest", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "In Memoriam -- Elder Don B. Colton", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Adventure in Glass", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Packed Lunch Suggestions", "author": "Hazel Stevens", "etype": "article"},
        {"title": "Make Your Own Fall Hat", "author": "Winifred P. Sanders", "etype": "article"},
        {"title": "Aunt Emma Webb Makes Six Quilts in One Year", "author": None, "etype": "article"},
        {"title": "Theology: Lehi's Later Exhortations", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Wo Be Unto Him That Is at Ease in Zion", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Providing Order and Convenience in the Kitchen", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: George Noel Gordon, Lord Byron", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Church Attitude on Government and Law", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Lost Season", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Child by a Stormy Sea", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Aster", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Caterpillar", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Late Harvest", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Beyond the Season's Turning", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Design for Leaves", "author": "Mirla Greenwood Thayne", "etype": "poem"},
        {"title": "I Would Sing", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Rainmaker", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Indian Summer", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Intervenings", "author": "Susan T. Jennings", "etype": "poem"},
        {"title": "Autumn Leaves", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "The Hunt", "author": "Evelyn Fjeldsted", "etype": "poem"},
    ],

    ("Vol39", "No11_November_1952"): [
        {"title": "Valleyward", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Announcement of Relief Society Building Site", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "As for Me and My House, We Will Serve the Lord", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Joy Here and Hereafter", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Let Us Magnify Our Callings and Enlarge Our Souls", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "The Discarded Dishes", "author": "Angelyn W. Wadley", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 5", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Praise Ye the Lord", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "The Greatest Is Love", "author": "Betty Zieve", "etype": "article"},
        {"title": "Adventure in Glass", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Adventures in Eating", "author": "Gladys R. Winter", "etype": "article"},
        {"title": "Make Quilt Blocks Easily", "author": "Thalia Black", "etype": "article"},
        {"title": "A Parent Is Always a Teacher", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Theology: Separation Into Nephites and Lamanites", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Yea, I Know That God Will Give Liberally", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Orderly and Convenient Storage Areas", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Percy Bysshe Shelley", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Modern Trends in Government", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "A Story in Snow", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Still Life", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "House of Happiness", "author": "Jennie Brimhall Knight", "etype": "poem"},
        {"title": "Purpose", "author": "Laura Lyman", "etype": "poem"},
        {"title": "First Snow", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "Open Your Window", "author": "Kate Richards", "etype": "poem"},
        {"title": "The Path to Home", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "If You Should Be the One", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Memorable", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Autumn Night", "author": "Elizabeth Waters", "etype": "poem"},
        {"title": "Heart at Rest", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Song of Loneliness", "author": "Vesta N. Lukei", "etype": "poem"},
    ],

    ("Vol39", "No12_December_1952"): [
        {"title": "They Looked for Him", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Home, and the Building of Home Life", "author": "J. Reuben Clark, Jr.", "etype": "article"},
        {"title": "The Annual General Relief Society Conference", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Fast Offerings and the Welfare Plan", "author": "Harold B. Lee", "etype": "article"},
        {"title": "Activities of Latter-day Saint Women in Europe", "author": "Emma Ray McKay", "etype": "article"},
        {"title": "The Gift Without the Giver", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "The Wise Men", "author": "Ida Isaacson", "etype": "article"},
        {"title": "Stars for Molly's Tree", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Under Thy Protecting Care", "author": "Angelyn W. Wadley", "etype": "fiction"},
        {"title": "The Fabric of Christmas", "author": "Mildred Garrett Enos", "etype": "fiction"},
        {"title": "The Christmas Tree", "author": "Lillian S. Feltman", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 6", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: From a Far Country", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Christmas Candies to Make at Home", "author": "Ethel C. Smith", "etype": "article"},
        {"title": "Conserving Ourselves", "author": "Anne S. W. Gould", "etype": "article"},
        {"title": "Christmas Packages for Children", "author": "Clara Laster", "etype": "article"},
        {"title": "Fruitcake -- Through the Holidays", "author": "Marie Gifford", "etype": "article"},
        {"title": "Recipe for Date Nut Wreath Pudding", "author": "Phyllis Snow", "etype": "article"},
        {"title": "Christmas Aprons From Scraps", "author": "Celia Luce", "etype": "article"},
        {"title": "Make Mittens From a Worn-Out Sweater", "author": "Thalia Black", "etype": "article"},
        {"title": "Not Wholly Yourself", "author": "Kate Richards", "etype": "article"},
        {"title": "A New Method of Map Construction", "author": "Rose A. Openshaw", "etype": "article"},
        {"title": "Theology: Nephi", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Wherefore, the Lord God Gave Unto Man", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Household Cleaning", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: John Keats", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Preparing the Way", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Glad Tidings", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Encompassed in the Rays", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Journey to the Holy Land", "author": "Martha Whiting Brown", "etype": "poem"},
        {"title": "The Star", "author": "Gene Romolo", "etype": "poem"},
        {"title": "First Snow", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Snowfall", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Winter", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Today", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "For Christmas Eve", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Blue", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Forget Them Not", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Low Burning", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Pageant of Night", "author": "Eunice J. Miles", "etype": "poem"},
        {"title": "The Child of Peace", "author": "Margaret Evelyn Shingleton", "etype": "poem"},
        {"title": "Early Sunrise", "author": "Gertrude Kovan", "etype": "poem"},
        {"title": "Emergence", "author": "Margery S. Stewart", "etype": "poem"},
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
    year = 1952

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol39_{key}.txt"
    ISSUE_FILES[("Vol39", key)] = (fname, month)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# OCR initial-letter substitution table.
# The OCR in Vol39 consistently misreads decorative/large-font initials:
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
    # "F" may OCR as "St" or "S" (e.g. "From" -> "Strom", "Far" -> "Sar")
    "F": r"(?:F|St|S)",
    "f": r"(?:f|st|s)",
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers for Vol39.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
_UNCERTAIN_POSSESSION_CHAPTER_PAT = (
    r"Uncertain\s+Possession"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)
_THROUGH_DOOR_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)rough\s+(?:Th|Sh|Ch)is\s+Door"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|Carter)\s+)?"  # "Carter" is OCR for "Chapter" in Jan
)
_TIME_TO_FORGET_CHAPTER_PAT = (
    r"A\s+(?:T|S|J|\()ime\s+(?:t|s|j)o\s+(?:F|St)orget"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)
_DEAR_CONQUEST_PART_PAT = (
    r"Dear\s+Conquest"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:PART|Part)\s+)?"
)
_CASE_FOR_MARTHA_PART_PAT = (
    r"A\s+Case\s+(?:f|st)or\s+(?:M|IT|Nl|Vl)artha"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:PART|Part)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—Characters and Teachings of The Book of Mormon Lesson NN—"SUBTITLE"
    # TOC has: Theology: SUBTITLE
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Visiting Seacher ITlessages—Book of Mormon Gems of Truth Lesson NN—"SUBTITLE"
    # or: (siting Seacher ITlessages, Ussiting Sleacher 7Tlessages, Uisiting Seacher ITlessages
    # TOC has: Visiting Teacher Messages: SUBTITLE
    "Visiting Teacher Messages: Book of Mormon Gems of Truth, Preview": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:T|S)(?:e|l)(?:a|e)cher\s+"
        r"(?:M|IT|7T|T|m)(?:l)?ess(?:a|i)?ges"
        r"[\s\-\u2014\u2013:]*"
        r"Book\s+of\s+Mormon\s+Gems\s+of\s+(?:Th|Sh|Ch)ruth"
        r"[\s\S]{0,80}?"
        r"[Pp]review"
    ),
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:T|S)(?:e|l)(?:a|e)cher\s+"
        r"(?:M|IT|7T|T|m)(?:l)?ess(?:a|i)?ges"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|]?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+Near\s+and\s+(?:F|S)ar"
    ),
    # Body has: EDITORIAL noise VOL. 39 MONTH 1952 <actual title>
    # or: EDIT ORIAL, ITORIAL (truncated), ED ITORIAL, etc.
    # TOC has: Editorial: <actual title>
    "Editorial: ": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL"
        r"[\s\S]{0,100}?"
    ),
    # Body has: various corrupted "Literature—The Literature of England Lesson NN—SUBTITLE"
    # OCR variants: eee —, Bi ae —, Sbelerature—, of iterature—, SL iertire—, ud ath rate =, rege aie, etc.
    # The category prefix is too garbled to match. Match on "Literature of England" series title.
    # TOC has: Literature: SUBTITLE  or  Literature: The Literature of England, Preview
    "Literature: The Literature of England, Preview": (
        r"(?:[A-Za-z\s\-\u2014\u2013:=]{0,30})?"
        r"(?:(?:Th|Sh|Ch)?e\s+)?Literature\s+of\s+England"
        r"[\s\S]{0,80}?"
        r"[Pp]review"
    ),
    "Literature: ": (
        r"(?:[A-Za-z\s\-\u2014\u2013:=]{0,30})?"
        r"(?:(?:Th|Sh|Ch)?e\s+)?Literature\s+of\s+England"
        r"[\s\S]{0,80}?"
        r"Lesson\s+\d+"
        r"[\s\-\u2014\u2013=]*"
    ),
    # Body has: Social Science/Sosialt Song/Seccal Soenco/Womeel "hecenas/Scena ere ike/Social Omonaa, etc.
    # The prefix is too garbled to match reliably. Instead match the series title.
    # TOC has: Social Science: SUBTITLE  or  Social Science: The Progress of Man, Preview
    "Social Science: The Progress of Man, Preview": (
        r"(?:(?:Social|Sosialt|Seccal|Womeel|Scena)\s+(?:Science|Sc(?:i|t)ence|Song\?|Soenco|\"hecenas|Swienes|ere|Omonaa)"
        r"[\s\-\u2014\u2013:]*"
        r"|"
        r"[A-Za-z\s\"]{0,30})"
        r"(?:[Tt](?:h|li)?e\s+)?Progress\s+of\s+Man"
        r"[\s\S]{0,120}?"
        r"[Pp]review"
    ),
    "Social Science: ": (
        r"(?:(?:Social|Sosialt|Seccal|Womeel|Scena)\s+(?:Science|Sc(?:i|t)ence|Song\?|Soenco|\"hecenas|Swienes|ere|Omonaa)"
        r"[\s\-\u2014\u2013:]*"
        r"|"
        # Fallback: match on "Progress of Man" series title directly
        r"[A-Za-z\s\"]{0,30})"
        r"(?:[Tt](?:h|li)?e\s+)?Progress\s+of\s+Man"
        r"[\s\S]{0,120}?"
        r"Lesson\s+\d+"
        r"[\s\-\u2014\u2013]*"
    ),
    # Body has: Work Neeting/Work Tleeting/Vork Meeting/Work ITleeting/(Vork Neeting
    # TOC has: Work Meeting: SUBTITLE  or  Work Meeting: Home Management, Preview
    "Work Meeting: Home Management, Preview": (
        r"[\(]?(?:W|V)ork\s+(?:IT|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"Home\s+Management"
        r"[\s\S]{0,120}?"
        r"[Pp]review"
    ),
    "Work Meeting: ": (
        r"[\(]?(?:W|V)ork\s+(?:IT|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St)rom\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"(?:(?:N|W|V)ot(?:e|ed)?s?\.?\s+)?(?:T|S|J|\(|1)(?:O|o|0)\s+(?:TH|Th|Sh|Ch|T)(?:H|h)?(?:E|e)\s+(?:F|St)(?:I|i|l)(?:E|e)(?:L|l)(?:D|d)"
        r":?\s*"
    ),
    # Music: body has Tlusic/IT lustc/Music — Fundamentals of Musicianship ...
    # Only in Jan-Feb for this volume
    "Music: ": (
        r"(?:Tlusic|IT\s*lust[ci]|Music|Nlusic|Vlusic)"
        r"[\s\-\u2014\u2013:]*"
        r"(?:Fun(?:s|d)?(?:l)?amentals\s+of\s+Musicianship)?"
        r"[\s\S]{0,120}?"
        r"(?:Lesson\s+\d+|Preview)?"
        r"[\s\-\u2014\u2013]*"
    ),
    # Serial fiction: Uncertain Possession (Chapters 1-8)
    "Uncertain Possession, Chapter 1": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:1|I)\b",
    "Uncertain Possession, Chapter 2": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:2|II)\b",
    "Uncertain Possession, Chapter 3": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:3|III)\b",
    "Uncertain Possession, Chapter 4": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:4|IV)\b",
    "Uncertain Possession, Chapter 5": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:5|V)\b",
    "Uncertain Possession, Chapter 6": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:6|VI)\b",
    "Uncertain Possession, Chapter 7": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:7|VII)\b",
    "Uncertain Possession, Chapter 8": _UNCERTAIN_POSSESSION_CHAPTER_PAT + r"(?:8|VIII|§)\b",
    # Serial fiction: Through This Door (Chapter 6 conclusion only, carried from Vol38)
    "Through This Door, Chapter 6": _THROUGH_DOOR_CHAPTER_PAT + r"(?:6|VI)\b",
    # Serial fiction: Dear Conquest (Parts I-IV)
    # Longer matches must come first to avoid prefix-matching bug
    "Dear Conquest, Part IV": _DEAR_CONQUEST_PART_PAT + r"(?:IV|4)\b",
    "Dear Conquest, Part III": _DEAR_CONQUEST_PART_PAT + r"(?:III|3)\b",
    "Dear Conquest, Part II": _DEAR_CONQUEST_PART_PAT + r"(?:II|2)\b",
    "Dear Conquest, Part I": _DEAR_CONQUEST_PART_PAT + r"(?:1|I)\b",
    # Serial fiction: A Time to Forget (Chapters 1-6)
    "A Time to Forget, Chapter 1": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:1|I)\b",
    "A Time to Forget, Chapter 2": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:2|II)\b",
    "A Time to Forget, Chapter 3": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:3|III)\b",
    "A Time to Forget, Chapter 4": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:4|IV)\b",
    "A Time to Forget, Chapter 5": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:5|V)\b",
    "A Time to Forget, Chapter 6": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:6|VI)\b",
    # Serial fiction: A Case for Martha (Parts I-II)
    # Part II must come before Part I to avoid prefix-matching bug
    "A Case for Martha, Part II": _CASE_FOR_MARTHA_PART_PAT + r"(?:II|2)\b",
    "A Case for Martha, Part I": _CASE_FOR_MARTHA_PART_PAT + r"(?:I|1)\b",
    # In Memoriam entries
    "In Memoriam -- Martha Jones Ballard": (
        r"(?:I|Y)n\s+(?:M|N)(?:l)?emor(?:i|t)(?:a|t)(?:m|am)"
        r"[\s\-\u2014\u2013~]*"
        r"(?:M|N)artha"
    ),
    "In Memoriam -- Elder Don B. Colton": (
        r"(?:I|Y)n\s+(?:M|N)(?:l)?emor(?:i|t)(?:a|t)(?:m|am)"
        r"[\s\-\u2014\u2013~]*"
        r"(?:E|6)lder\s+Don"
    ),
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
    """Split text into front matter (TOC, etc.) and body.

    For files with TWO occurrences of the marker (e.g. Aug/Sep 1952 where
    body content precedes the TOC), the TOC/masthead section between the
    two markers becomes front matter, and the body is the content before
    the first marker concatenated with the content after the second marker.
    """
    toc_end_markers = [
        "PUBLISHED MONTHLY BY THE GENERAL BOARD",
        "ISHED MONTHLY BY THE GENERAL BOARD",
        "MONTHLY BY THE GENERAL BOARD"
    ]

    for marker in toc_end_markers:
        occurrences = list(re.finditer(marker, text, re.IGNORECASE))
        if len(occurrences) >= 2:
            # Two markers: TOC is between them; body is outside
            first_start = occurrences[0].start()
            last_start = occurrences[-1].start()
            pre_content = text[:first_start]
            front_matter = text[first_start:last_start]
            body = text[last_start:]
            # Prepend the pre-TOC content to the body
            body = pre_content + "\n\n" + body
            return front_matter, body
        elif len(occurrences) == 1:
            split_point = occurrences[0].start()
            front_matter = text[:split_point]
            body = text[split_point:]
            return front_matter, body

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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol39"
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
        description="Extract Relief Society Magazine Vol 39 into individual entries"
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

    for (vol, issue_key), entries in VOL39_TOC.items():
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
