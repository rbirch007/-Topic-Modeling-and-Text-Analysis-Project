#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 45 (1958).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol45.py
    python extract_vol45.py --dry-run
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

VOL45_TOC = {
    ("Vol45", "No01_January_1958"): [
        # SPECIAL FEATURES
        {"title": "A New Year's Greeting", "author": None, "etype": "article"},
        # OCR note: Kimball title garbled in TOC as "BO hee ial Ea li ae ek ee te!"
        {"title": "Women, Wonderful Women!", "author": "Spencer W. Kimball", "etype": "article"},
        {"title": "Award Winners -- Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Pray Without Ceasing -- First Prize Poem", "author": "Helen Candland Stark", "etype": "poem"},
        {"title": "Song of the Weathervanes -- Second Prize Poem", "author": "Lael W. Hill", "etype": "poem"},
        # OCR note: title garbled as "The Extmined Fia" in TOC
        {"title": "The Examined Life -- Third Prize Poem", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Award Winners -- Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "The Day We All Went to Rainbow Springs -- First Prize Story", "author": "Deone R. Sutherland", "etype": "fiction"},
        # OCR note: Nibley title garbled in TOC
        {"title": "The Scandinavian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Biographical Sketches of Contest Winners", "author": None, "etype": "article"},
        {"title": "Survival Is Not Enough", "author": "Basil O'Connor", "etype": "article"},
        # OCR note: TOC entry "Bieeeec mmetottes Of (Contest Winners" garbled
        {"title": "Photographs of Contest Winners", "author": None, "etype": "article"},
        {"title": "Three-Part Songs for Relief Society Singing Mothers", "author": "Florence J. Madsen", "etype": "article"},
        # FICTION
        {"title": "Elizabeth's Children -- Chapter 1", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Regulations Governing the Submittal of Material for Notes From the Field", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "It's a New Year", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Recipes From the Swedish Mission", "author": "Ruth T. Oscarson", "etype": "article"},
        {"title": "Mercy Waters Peay Writes and Memorizes Poetry", "author": None, "etype": "article"},
        {"title": "Making Potato Starch", "author": "Alice R. Rich", "etype": "article"},
        # LESSON DEPARTMENT
        {"title": "Theology: The Three Special Book of Mormon Witnesses", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Look Unto Me in Every Thought; Doubt Not, Fear Not", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Savings and Added Happiness", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: Macbeth, Villain or Hero?", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Love -- A Basic Ingredient", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "Perfect Prelude", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "White With Silence", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Two Visitors", "author": "Hazel M. Thomson", "etype": "poem"},
        {"title": "There Is a Winter Now", "author": "Helen H. Winn", "etype": "poem"},
        # OCR note: title completely garbled in TOC ("Cg ie pad Se Pe a ES ae a ee ere")
        {"title": "Pansye H. Powell poem (title unrecoverable from OCR)", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Winter", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Old Organ", "author": "Ida Elaine James", "etype": "poem"},
    ],

    ("Vol45", "No02_February_1958"): [
        # Frontispiece
        {"title": "Song for Winterset", "author": "Lizabeth Wall Madsen", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Family Unity", "author": "Dantzel W. Nelson", "etype": "article"},
        {"title": "The Swiss-Austrian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "A Relief Society Tribute", "author": "Maurine R. Goold", "etype": "article"},
        {"title": "Joy in Service", "author": "Jennie Brown Rawlins", "etype": "article"},
        {"title": "We Are All Alike -- But Different", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "On Starting the Day Right", "author": "Wilma Boyle Bunker", "etype": "article"},
        {"title": "Life Is Not a Holiday", "author": "Anne S. W. Gould", "etype": "article"},
        # FICTION
        {"title": "Fifty Singing Aprils -- Second Prize Story", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Never Wish for the Moon", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "The Gift and the Giver", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Elizabeth's Children -- Chapter 2", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Be My Valentine", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Birthday Congratulations to Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Swiss-Austrian Mission", "author": "LaVelle D. Curtis", "etype": "article"},
        {"title": "Mary Bauer Clark Makes Quilts of Exquisite Workmanship", "author": None, "etype": "article"},
        {"title": "Scissor Happy", "author": "Helen H. Jones", "etype": "article"},
        {"title": "An Easy Way to Make Rugs", "author": "Eugenie Daniels", "etype": "article"},
        # LESSON DEPARTMENT
        {"title": "Theology: The Restoration of the Aaronic Priesthood", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Be Faithful and Diligent in Keeping the Commandments of God", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Debts and Trouble", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: The Tempest", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Families Have Neighbors", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "Winter Etching", "author": "Grace Ingles Frost", "etype": "poem"},
        # OCR note: title garbled as "TOTS TTI" in TOC; "Eatclentess" in body text also garbled
        {"title": "Loneliness", "author": "Linda Crane", "etype": "poem"},
        {"title": "Refuge", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Assignment for Tomorrow", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "This One Day", "author": "Constance Timberlake", "etype": "poem"},
        {"title": "On Entering a Small Boy in the Hospital", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Heart's Oasis", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "To a Child Pushing a Doll Carriage", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Proposal", "author": "Helen H. Winn", "etype": "poem"},
        {"title": "Winter Time", "author": "Stella Craft Tremble", "etype": "poem"},
    ],

    ("Vol45", "No03_March_1958"): [
        # Frontispiece
        {"title": "Repeat in Silence", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Relief Societies in Primitive Times?", "author": "Spencer W. Kimball", "etype": "article"},
        {"title": "In Memoriam -- Elder Thomas E. McKay", "author": "Clifford E. Young", "etype": "article"},
        {"title": "The Tahitian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "The Red Cross and Its Field of Service", "author": "O. C. Duckett", "etype": "article"},
        {"title": "It's Smart to Take Part", "author": None, "etype": "article"},
        {"title": "The Winds of March", "author": "Helen S. Williams", "etype": "article"},
        # FICTION
        {"title": "Wee Pine Knot -- Third Prize Story", "author": "Edna H. Day", "etype": "fiction"},
        {"title": "Inherit the Earth", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "One Day in Spring", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "Only Sage and More Sage", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Elizabeth's Children -- Chapter 3", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Feed My Sheep", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Index for 1957 Relief Society Magazine Available", "author": None, "etype": "article"},
        {"title": "Announcing the Special April Short Story Issue", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Tahitian Mission", "author": "Dorothy P. Christensen", "etype": "article"},
        {"title": "If the Thumb Is Green", "author": "C. W. McCullough", "etype": "article"},
        {"title": "You Can Sew -- Cutting", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Nellie Porter Workman Makes Beautiful Gifts", "author": None, "etype": "article"},
        {"title": "From Shirt to Slips", "author": "Hazel Brackett", "etype": "article"},
        {"title": "Lunch at Grandma's", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "Yellow Padding", "author": "Leone E. McCune", "etype": "article"},
        # POETRY
        {"title": "Let There Be Silence", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "Woodland Cathedral", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Another Spring", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Clothesline in Spring", "author": "Helen H. Winn", "etype": "poem"},
        {"title": "A Pink Shawl", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Unlimited", "author": "Alice Whitson Norton", "etype": "poem"},
        # OCR note: garbled in TOC; title unrecoverable
        {"title": "Urban Spring", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Ideals", "author": "Grace Ingles Frost", "etype": "poem"},
        # OCR note: title garbled as "Gig" in body text; poem about fog
        {"title": "Fog", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol45", "No04_April_1958"): [
        # Frontispiece
        {"title": "Pattern of Wings", "author": "Vesta P. Crawford", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Resurrection and Things After", "author": "S. Dilworth Young", "etype": "article"},
        {"title": "In Memoriam -- Adam S. Bennion", "author": "Harold B. Lee", "etype": "article"},
        {"title": "Pearle M. Olsen Appointed to General Board", "author": "Josie B. Bay", "etype": "article"},
        {"title": "Margaret Elsa T. Peterson Appointed to General Board", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "Irene B. Woodford Appointed to General Board", "author": "Hulda Parker", "etype": "article"},
        {"title": "The Tongan Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "It's Smart to Take Part", "author": None, "etype": "article"},
        {"title": "Fight Cancer With a Checkup and a Check", "author": "Walter J. Kohler", "etype": "article"},
        # FICTION -- SPECIAL APRIL SHORT STORIES
        {"title": "To Be a Queen", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "The Apricot Tree", "author": "Mabel Harmer", "etype": "fiction"},
        # OCR note: title garbled in TOC
        {"title": "More Than Money", "author": "Ruth M. Ostegar", "etype": "fiction"},
        {"title": "The Heart of the Home", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "To Have the Peace", "author": "Betty Ventura", "etype": "fiction"},
        # SERIAL
        {"title": "Elizabeth's Children -- Chapter 4", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Swifter Than a Weaver's Shuttle", "author": "Helen W. Anderson", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Tongan Mission", "author": "Sylvia R. Stone", "etype": "article"},
        {"title": "You Can Sew -- II. Marking, Pinning, and Basting", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Sarah A. Sanders Makes Quilts for Contentment", "author": None, "etype": "article"},
        # OCR note: one garbled FEATURES FOR HOME entry by Dorthea N. Newbold and Blanche B. Peterson
        {"title": "Dorthea N. Newbold article (title garbled in OCR)", "author": "Dorthea N. Newbold", "etype": "article"},
        # POETRY
        {"title": "Unfoldment", "author": "Amy Viau", "etype": "poem"},
        {"title": "The Gift of Rain", "author": "Dora Toone Brough", "etype": "poem"},
        {"title": "Portico to Beauty", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Note to a Woodpecker", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Sounding Light", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Flowering Peach Tree", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Confidential", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Interim", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "True Sight", "author": "Mabel Law Atkinson", "etype": "poem"},
        # OCR note: Roberts poem title garbled in TOC
        {"title": "Dorothy J. Roberts poem (title unrecoverable from OCR)", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Green Day", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "In Spring", "author": "Sudie Stuart Hager", "etype": "poem"},
        {"title": "Proclamation", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Spring", "author": "Enola Chamberlin", "etype": "poem"},
    ],

    ("Vol45", "No05_May_1958"): [
        # Frontispiece
        {"title": "Bird on an Orchard Bough", "author": "Christie Lund Coles", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Mother's Influence in the Home", "author": "Mary R. Young", "etype": "article"},
        {"title": "In Memoriam -- Oscar Ammon Kirkham", "author": "Antoine R. Ivins", "etype": "article"},
        {"title": "Song for a Daughter", "author": "Mabel Law Atkinson", "etype": "article"},
        {"title": "The Prophet Joseph Smith's Tribute to His Mother", "author": None, "etype": "article"},
        {"title": "The Uruguayan Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Contest Announcements -- 1958", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "The Gift and the Desire", "author": "Alice Morrey Bailey", "etype": "article"},
        {"title": "Why Not Write a Story?", "author": "Helen Hinckley Jones", "etype": "article"},
        {"title": "All Out -- All Ages -- Polio Vaccination Roundup", "author": "Virginia L. Blood", "etype": "article"},
        {"title": "Young Men Shall See Visions", "author": None, "etype": "article"},
        # FICTION
        {"title": "Papa and His Grapes", "author": "Florence B. Dunford", "etype": "fiction"},
        {"title": "The Finish", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Elizabeth's Children -- Chapter 5", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Making Right Choices", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Magazine Honor Roll for 1957", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Uruguayan Mission", "author": "Louise B. Parry", "etype": "article"},
        {"title": "You Can Sew -- III. Fitting", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Mollie McClain Boel Enjoys Lifelong Hobbies", "author": None, "etype": "article"},
        # POETRY
        {"title": "White Lilacs", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "My Mother's Trust", "author": "Hannah C. Ashby", "etype": "poem"},
        {"title": "Recompense", "author": "Hazel M. Thomson", "etype": "poem"},
        {"title": "Precious Episode", "author": "Dora Toone Brough", "etype": "poem"},
        # OCR note: title garbled as "EXOT SMMC" in TOC; unrecoverable
        {"title": "Grace Ingles Frost poem (title unrecoverable from OCR)", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "A Mother's Way", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Canyon Wind", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Mother's Letters", "author": "Emma Ann Bigelow", "etype": "poem"},
        {"title": "Tendrils", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "God's Gift to Me", "author": "Bernice D. Savage", "etype": "poem"},
    ],

    ("Vol45", "No06_June_1958"): [
        # Frontispiece
        {"title": "As I Pass By", "author": "Margery S. Stewart", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Father's True Place in the Home", "author": "Bruce R. McConkie", "etype": "article"},
        {"title": "The Prophet Joseph Smith's Tribute to His Father", "author": None, "etype": "article"},
        {"title": "Hugh B. Brown -- New Apostle", "author": "Harold B. Lee", "etype": "article"},
        {"title": "Gordon B. Hinckley -- An Impression", "author": "S. Dilworth Young", "etype": "article"},
        {"title": "Henry Dixon Taylor Appointed Assistant to the Council of the Twelve", "author": "LeGrand Richards", "etype": "article"},
        {"title": "A. Theodore Tuttle Appointed to the First Council of Seventy", "author": "William E. Berrett", "etype": "article"},
        {"title": "The West German Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Young Men Shall See Visions", "author": None, "etype": "article"},
        {"title": "Accidents: The Greatest Threat to Our Children", "author": "Alan K. Done", "etype": "article"},
        # FICTION
        {"title": "Without Doubt", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Sweet Is the Faith of a Child", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Elizabeth's Children -- Chapter 6", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 128th Annual Church Conference", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "The Magic Hour", "author": "Ruth H. Solomon", "etype": "article"},
        {"title": "Recipes From the West German Mission", "author": "Bernice O. Dyer", "etype": "article"},
        {"title": "You Can Sew -- IV. Seams and Seam Finishes", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Lucinda B. Bair Makes Stoles in the Star Stitch", "author": None, "etype": "article"},
        {"title": "Sunday Dinner an Institution in Our Home", "author": "Mabel Law Atkinson", "etype": "article"},
        # LESSON DEPARTMENT -- PREVIEWS FOR 1958-59
        {"title": "Teaching Aids for the 1958-59 Lessons", "author": "Mary R. Young", "etype": "lesson"},
        {"title": "Theology -- The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Truth to Live By From The Doctrine and Covenants", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Managing a Home", "author": "Vesta Wright Barnett", "etype": "lesson"},
        {"title": "Literature -- America's Literature -- Meet the New World", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- Latter-day Saint Family Life", "author": "John Farr Larson", "etype": "lesson"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
        # POETRY (listed inline with semicolons for some entries)
        {"title": "Wheat Grower's Children", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Garden Rendezvous", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "And Seek No More", "author": "Velma N. Simonsen", "etype": "poem"},
        {"title": "Lullaby of Silence", "author": "Maude Rubin", "etype": "poem"},
        {"title": "A Son Receives His Diploma", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "My House", "author": "Vesta Ball Ward", "etype": "poem"},
        {"title": "Blueprint", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Vandal", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Love Is Enough", "author": "Eunice J. Miles", "etype": "poem"},
    ],

    ("Vol45", "No07_July_1958"): [
        # Frontispiece
        {"title": "It Was the Common Things", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        # SPECIAL FEATURES
        # OCR note: title garbled in TOC; confirmed from body text context about Leah D. Widtsoe
        {"title": "Leah D. Widtsoe article (title garbled in OCR)", "author": "Leah D. Widtsoe", "etype": "article"},
        {"title": "The Western Canadian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "The Olympian Rain Forest", "author": "Dorothy J. Roberts", "etype": "article"},
        {"title": "Honor Thy Father and Mother", "author": None, "etype": "article"},
        {"title": "The Yellow Line", "author": "Thelma Groneman", "etype": "article"},
        # FICTION
        {"title": "Not to the Swift -- Chapter 1", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "The Long View", "author": "Sara O. Moss", "etype": "fiction"},
        {"title": "Elizabeth's Children -- Chapter 7", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Necessary Conditions for the Restoration of the Gospel", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "A House for Living", "author": "Olive A. Hanks", "etype": "article"},
        {"title": "Recipes From the Western Canadian Mission", "author": "Annie Ruth Larson", "etype": "article"},
        {"title": "You Can Sew -- V. Pressing", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Mother Made Buckskin Gloves", "author": "Alice R. Rich", "etype": "article"},
        {"title": "Clara Scoville Ware Crochets Exquisite Designs", "author": None, "etype": "article"},
        # LESSON DEPARTMENT
        {"title": "Theology -- He That Hath Eternal Life Is Rich", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Yea, Whosoever Will Thrust in His Sickle and Reap, the Same Is Called of God", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Homemaking Is the Administrative Side of Family Living", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature -- The American Dream", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- The Family Ties", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "Summer Growing", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "On Full-Sunned Path", "author": "Amy Viau", "etype": "poem"},
        {"title": "The Weight of Years", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Unfulfilled", "author": "Patricia Lentz", "etype": "poem"},
        {"title": "A Key to Wisdom", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "With You Away", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Bequest", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "From the Old Days", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Twilight", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Where Jesus Walked", "author": "Remelda Nielsen Gibson", "etype": "poem"},
        {"title": "Gathering Driftwood", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Repose", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Infinity Speaks", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Pamela Is Two", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Pioneer Bonnet", "author": "Filindia S. Huff", "etype": "poem"},
    ],

    ("Vol45", "No08_August_1958"): [
        # Frontispiece
        {"title": "When Threshers Came", "author": "Alice Morrey Bailey", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "The Temple in New Zealand", "author": "Wealtha S. Mendenhall", "etype": "article"},
        {"title": "The Northern Mexican Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Everyday Life in Colonial New England -- Part I", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Honor Thy Father and Thy Mother", "author": None, "etype": "article"},
        {"title": "Annual Report 1957", "author": "Hulda Parker", "etype": "article"},
        # FICTION
        {"title": "Not to the Swift -- Chapter 2", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Elizabeth's Children -- Chapter 8", "author": "Olive W. Burt", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The World-Wide Sisterhood", "author": "Helen W. Anderson", "etype": "editorial"},
        {"title": "Dowager Marchioness of Reading Visits in Salt Lake City", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Northern Mexican Mission", "author": "Rhoda C. Taylor", "etype": "article"},
        {"title": "You Can Sew -- VI. Zippers", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Sylvia Draper's Hobby Brings Joy to Others", "author": None, "etype": "article"},
        # LESSONS FOR NOVEMBER
        {"title": "Theology -- The Desire to Serve in the Gospel Plan", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Remember the Worth of Souls Is Great in the Sight of God", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Setting Goals in Homemaking", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature -- Calvinist, Puritan, Pilgrim", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Suggested Visual Aids for the Literature Lessons", "author": None, "etype": "lesson"},
        {"title": "Music Suggested for the Literature Course", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Social Science -- The Law Also Maketh You Free", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "Heard on Horeb", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Breaking Cord", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Wayside Incident", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Summer", "author": "Frances C. Yost", "etype": "poem"},
        {"title": "For a Lacemaker", "author": "Maryhale Woolsey", "etype": "poem"},
        # OCR note: title garbled as "HBTS OGTOM" / "Selegra nl" in TOC/body
        {"title": "Linnie F. Robinson poem (title unrecoverable from OCR)", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Discovery", "author": "Anna W. Fox", "etype": "poem"},
        {"title": "Mountain Meadow", "author": "Ethel Jacobson", "etype": "poem"},
    ],

    ("Vol45", "No09_September_1958"): [
        # Frontispiece
        {"title": "Harvest-Colored Hills", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Tribute to President David O. McKay, A Great Missionary", "author": "Jeanette McKay Morrell", "etype": "article"},
        # OCR note: title garbled in TOC
        # "The Power of Faith" not found in September source - may be misattributed
        {"title": "The West Spanish-American Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Everyday Life in Colonial New England -- Part II", "author": "Alberta H. Christensen", "etype": "article"},
        # FICTION
        {"title": "The Bazaar", "author": "Florence B. Dunford", "etype": "fiction"},
        {"title": "Not to the Swift -- Chapter 3", "author": "Deone R. Sutherland", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        # OCR note: garbled in TOC
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Grandparents in the Family Pattern", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the West Spanish-American Mission", "author": "McNone N. Perry", "etype": "article"},
        {"title": "You Can Sew -- VII. Hems", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Eliza Brockbank Hales Specializes in Flower Growing", "author": None, "etype": "article"},
        # LESSONS FOR DECEMBER
        {"title": "Theology -- The Worth of Souls", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- I Will Bless All Those Who Labor in My Vineyard With a Mighty Blessing", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Managerial Responsibilities of the Homemaker", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature -- Roger Williams and the Puritan Theocracy", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- No Lesson Outlined", "author": None, "etype": "lesson"},
        # POETRY
        {"title": "Mystic Word", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Mother-By-Love", "author": "Maude Rubin", "etype": "poem"},
        {"title": "The Apple Packer", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "Between Anniversaries", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "In a Swing", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "To Mozart", "author": "Bernice Ames", "etype": "poem"},
        # OCR note: title garbled as "C) Xo) <-Yol" in TOC; poem about chokecherry tree
        {"title": "Roxana Farnsworth Hase poem (title unrecoverable from OCR)", "author": "Roxana Farnsworth Hase", "etype": "poem"},
        {"title": "I Still Remember", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Daughter's First Wedding", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Tranquility", "author": "Winona Frandsen Thomas", "etype": "poem"},
        {"title": "Only Hearts Can Hear", "author": "Maude O. Cook", "etype": "poem"},
    ],

    ("Vol45", "No10_October_1958"): [
        # Frontispiece
        {"title": "Sung in October", "author": "Dorothy J. Roberts", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Whom and Where Will You Marry?", "author": "ElRay L. Christiansen", "etype": "article"},
        {"title": "In Memoriam -- Elder Clifford Earle Young", "author": "S. Dilworth Young", "etype": "article"},
        {"title": "Elen Louise Wallace Madsen Appointed Second Counselor in the General Presidency of Relief Society", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Helen Woodruff Anderson Released as Second Counselor in the General Presidency of Relief Society", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "The New Zealand South Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Practical Nursing, a Dignified Lifetime Career", "author": "Maria Johnson", "etype": "article"},
        # FICTION
        {"title": "Silent Carillons", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Not to the Swift -- Chapter 4", "author": "Deone R. Sutherland", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Children in Relief Society", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Gladys Sessions Boyer Released From the General Board", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the New Zealand South Mission", "author": "Wealtha S. Mendenhall", "etype": "article"},
        {"title": "You Can Sew -- VIII. Setting in Sleeves", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Julia Sullivan Greene Enjoys Many Hobbies", "author": None, "etype": "article"},
        # LESSONS FOR JANUARY
        {"title": "Theology -- A Message From Him Who Is Eternal", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Learn of Me, and Listen to My Words", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Qualities of the Effective Home Manager", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature -- Some Puritan Women", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- Weighed in the Balances", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "At Twilight", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Nostalgic Balm", "author": "Elizabeth MacDougall", "etype": "poem"},
        {"title": "Fall Fantasy", "author": "Annie Atkin Tanner", "etype": "poem"},
        {"title": "Blue Spruces", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Homecoming", "author": "Florence A. Clark", "etype": "poem"},
        {"title": "Desert", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Last Campfire", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Quilting Chairman", "author": "Colleen Pinegar", "etype": "poem"},
        {"title": "Hands", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Baking Day", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "October Wheatfields", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "The Transient", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Beautiful Fall Tour", "author": "Belle W. Anderson", "etype": "poem"},
    ],

    ("Vol45", "No11_November_1958"): [
        # Frontispiece
        {"title": "On the Rim", "author": "Christie Lund Coles", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "The Mission of Womankind", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "The Grand Key-Words of Relief Society", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Key Is Turned", "author": None, "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Danger -- Curve Ahead", "author": "Louise W. Madsen", "etype": "article"},
        # FICTION
        {"title": "The Right Climate", "author": "Vera H. Mayhew", "etype": "fiction"},
        {"title": "Not to the Swift -- Chapter 5", "author": "Deone R. Sutherland", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Accepting a Call to Service", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Festive Foods for Fun and Fancy", "author": "Maren E. Hardy", "etype": "article"},
        {"title": "You Can Sew -- IX. Fitted Facings", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Olive Rhoades Westenschow Enjoys Handwork Hobbies", "author": None, "etype": "article"},
        # LESSONS FOR FEBRUARY
        {"title": "Theology -- The Church Organized in the Last Dispensation", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Seek Not to Counsel Your God", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Operating the Household", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature -- Two Puritan Poets: Wigglesworth and Taylor", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- The Restraining Hand", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "As Tangible As Grass", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "A Song for Thanksgiving", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Young Mother", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Like a Kernel", "author": "Vesta Nickerson Lukei", "etype": "poem"},
        {"title": "Prairie Autumn", "author": "Bernice Ames", "etype": "poem"},
        {"title": "Thanksgiving", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "The Auditing", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Footsteps", "author": "Annie M. Ellsworth", "etype": "poem"},
        {"title": "Plymouth Winter", "author": "Maude Rubin", "etype": "poem"},
        {"title": "In Perspective", "author": "Beulah H. Sadleir", "etype": "poem"},
    ],

    ("Vol45", "No12_December_1958"): [
        # Frontispiece
        {"title": "Even a Christmas Tree", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        # SPECIAL FEATURES
        {"title": "Three Realms in Which Woman's Influence Should Ever Be Paramount", "author": "David O. McKay", "etype": "article"},
        {"title": "Annual General Relief Society Conference -- 1958", "author": "Hulda Parker", "etype": "article"},
        {"title": "Elder William James Critchlow, Jr.", "author": "Alma Sonne", "etype": "article"},
        {"title": "Elder Alvin R. Dyer", "author": "Joseph T. Bentley", "etype": "article"},
        {"title": "A New Spire Toward Heaven", "author": "Donna T. Smart", "etype": "article"},
        {"title": "Christmas Seal Sale -- 1958", "author": "Dorthea M. Lindsey", "etype": "article"},
        # FICTION
        {"title": "Christmas Is Where the Heart Is", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "Not to the Swift -- Chapter 6 -- Conclusion", "author": "Deone R. Sutherland", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 128th Semi-Annual Church Conference", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "New Serial The Silver Leash to Begin in January", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Deck the Halls", "author": "Helen Spencer Williams", "etype": "article"},
        {"title": "A Christmas Card Tree for Your Window", "author": "Sylvia Pezoldt", "etype": "article"},
        {"title": "You Can Sew -- X -- Other Edge Finishes", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Rachel Ann Giles Specializes in Fine Cut-Work Embroidery", "author": None, "etype": "article"},
        {"title": "Quick Little Gifts for Girls", "author": "Clara Laster", "etype": "article"},
        # LESSONS FOR MARCH
        {"title": "Theology -- The Responsibilities of the Members of the Church", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Beware of Pride, Lest Thou Shouldst Enter Into Temptation", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Making the Most of Time and Energy", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature -- The Mather Dynasty", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- We Seek After These Things", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "The Holy Land", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Weaving", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "No Lack of Miracle", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Grannies", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Teacher of Music", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Who Can Know the Stars?", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Winter Morning", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Reciprocity", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "December Violets", "author": "Mabel Law Atkinson", "etype": "poem"},
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
    year = 1958

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol45_{key}.txt"
    ISSUE_FILES[("Vol45", key)] = (fname, month)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# OCR initial-letter substitution table.
# ---------------------------------------------------------------------------
_OCR_WORD_START_ALTS = {
    "Th": r"(?:Th|Sh|Ch)",
    "th": r"(?:th|sh|ch)",
    "TH": r"(?:TH|SH|CH)",
}

_OCR_SINGLE_CHAR_ALTS = {
    "T": r"(?:T|S|J|\()",
    "t": r"(?:t|s|j)",
    "F": r"(?:F|St)",
    "f": r"(?:f|st)",
    "H": r"(?:H|Fl)",
    "h": r"(?:h|fl)",
    "I": r"(?:I|\|)",
    "M": r"(?:M|IT|Il|Nl)l?",
    "N": r"(?:N|V|T|l)",
    "J": r"(?:J|Y)",
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers for Vol45.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
# "Elizabeth's Children" by Olive W. Burt (Chapters 1-8, Jan-Aug)
_ELIZABETHS_CHILDREN_PAT = (
    r"Elizabeth[\u2018\u2019\u0027\"]?s\s+Children"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|Crapter|Cuapter)\s+)?"
)
# "Not to the Swift" by Deone R. Sutherland (Chapters 1-6, Jul-Dec)
_NOT_TO_SWIFT_PAT = (
    r"[\"\u201c\u2018]?(?:N|V|T|l)ot\s+to\s+the\s+Swift[\"\u201d\u2019]?"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|Crapter|Cuapter)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—The Doctrine and Covenants Lesson N
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Theology -- ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s]*[\-\u2014\u2013][\s]*"
    ),
    # Body has: Visiting Seacher ITlessages — Truths to Live By From The Doctrine and Covenants
    # OCR variants: Visiting Seacher ITlessages, Uisiting Seacher ITlessages, (siting cleacher ITlessages
    # General VTM pattern — match header, optionally skip to Message N, consume quotes/ellipsis
    "Visiting Teacher Messages: ": (
        r"(?:o|O)?(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:[A-Za-z]{4,12})\s+"
        r"(?:M|IT|I|T|m)(?:\s+)?(?:l)?essages"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Message\s+\d+)[\s\-\u2014\u2013]*)?"
        r"[\"\u201c\u2018\u2019\u0027\|\.]{0,10}\s*"
    ),
    "Visiting Teacher Messages -- ": (
        r"(?:o|O)?(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:[A-Za-z]{4,12})\s+"
        r"(?:M|IT|I|T|m)(?:\s+)?(?:l)?essages"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Message\s+\d+)[\s\-\u2014\u2013]*)?"
        r"[\"\u201c\u2018\u2019\u0027\|\.]{0,10}\s*"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+(?:N|V|T|l)[a-z]*ear\s+(?:A|a)nd\s+(?:F|S)ar"
    ),
    "From Near And Far": (
        r"(?:F|St|S)rom\s+(?:N|V|T|l)[a-z]*ear\s+(?:A|a)nd\s+(?:F|S)ar"
    ),
    # Body has: EDITORIAL cr VOL. 45 MONTH 1958 NO. N Title
    # OCR variants: IIT ORIAL (July), EDITORIAL, with noise: C->, QD, CQ, Q->, COD, cmC2
    "Editorial: ": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Body has: Literature—Shakespeare in Our Lives Lesson NN (Jan-Jun)
    # or: Literature—Meet the New World Lesson N (Jul-Dec)
    "Literature: ": (
        r"(?:"
            r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s\-\u2014\u2013:]*"
            r"(?:Shakespea[rt]e\s+in\s+Our\s+Lives|Meet\s+the\s+New\s+World)?\s*"
        r"|"
            r"[\S\s]{0,30}?(?:Shakespea[rt]e\s+in\s+Our\s+Lives|Meet\s+the\s+New\s+World)\s*"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Literature -- ": (
        r"(?:"
            r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s\-\u2014\u2013:]*"
            r"(?:Shakespea[rt]e\s+in\s+Our\s+Lives|Meet\s+the\s+New\s+World)?\s*"
        r"|"
            r"[\S\s]{0,30}?(?:Shakespea[rt]e\s+in\s+Our\s+Lives|Meet\s+the\s+New\s+World)\s*"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    # Body has: Social wience / Otial Science — Latter-day Saint Family Life Lesson N
    "Social Science: ": (
        r"(?:"
            r"(?:S|O)[a-z]*(?:c|o)ial\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"|"
            r"[A-Za-z!\s]{0,25}Latter[\-\s]day\s+Saint\s+Family\s+Life"
        r")"
        r"[\s]*[\-\u2014\u2013:]*[\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3})?"
    ),
    "Social Science -- ": (
        r"(?:"
            r"(?:S|O)[a-z]*(?:c|o)ial\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"|"
            r"[A-Za-z!\s]{0,25}Latter[\-\s]day\s+Saint\s+Family\s+Life"
        r")"
        r"[\s]*[\-\u2014\u2013:]*[\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3})?"
    ),
    # Body has: Work Tleeting / Work IT leeting / Work ITleeting — series title ... Discussion N
    # Series: "Living More Abundantly" (Jan-Jun), "Managing a Home" (Jul-Dec)
    "Work Meeting: ": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|I|M)?(?:\s+)?(?:l)?(?:e|E|c)[a-z]?ting"
        r"(?:"
        r"[)\s]*\)\s*"
        r"|"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Work Meeting -- ": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|I|M)?(?:\s+)?(?:l)?(?:e|E|c)[a-z]?ting"
        r"(?:"
        r"[)\s]*\)\s*"
        r"|"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Notes From the Field: Relief Society Activities": (
        r"NOTES\s+FROM\s+THE\s+FIELD[\s\d]+"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    # Prize stories/poems: body doesn't have "First Prize Story/Poem" suffix
    "The Day We All Went to Rainbow Springs -- First Prize Story": (
        r"(?:Th|Sh|Ch)e[\s\-\u2014\u2013]+Day[\s\-\u2014\u2013]+We[\s\-\u2014\u2013]+All[\s\-\u2014\u2013]+Went"
    ),
    "Pray Without Ceasing -- First Prize Poem": (
        r"Pray[\s\-\u2014\u2013]+Without[\s\-\u2014\u2013]+Ceasing"
    ),
    "Song of the Weathervanes -- Second Prize Poem": (
        r"Song[\s\-\u2014\u2013]+of[\s\-\u2014\u2013]+the[\s\-\u2014\u2013]+Weathervanes"
    ),
    "The Examined Life -- Third Prize Poem": (
        r"(?:Th|Sh|Ch)e[\s\-\u2014\u2013]+Examined[\s\-\u2014\u2013]+Life"
    ),
    # Award Winners
    "Award Winners -- Eliza R. Snow Poem Contest": (
        r"Award[\s\-\u2014\u2013]+Winners[\s\S]{0,30}?(?:E|6)liza[\s\-\u2014\u2013]+R\.?[\s\-\u2014\u2013]+Snow[\s\-\u2014\u2013]+Poem"
    ),
    # Generic lesson preview entries (June Teaching Aids section)
    "Literature -- America's Literature -- Meet the New World": (
        r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s\-\u2014\u2013:]*"
        r"(?:America[\u2018\u2019\u0027]?s\s+Literature[\s\-\u2014\u2013:]*)?Meet\s+the\s+New\s+World"
    ),
    "Work Meeting -- Managing a Home": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|I|M)?(?:\s+)?(?:l)?(?:e|E|c)[a-z]?ting"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"Managing\s+a\s+Home"
    ),
    # In Memoriam entries — OCR: "emonai" for "Memoriam"
    "In Memoriam -- Adam S. Bennion": (
        r"(?:(?:I|(?:I|\|)n)\s+(?:M|IT|Nl)l?emoriam|emonai)"
        r"[\s\-\u2014\u2013]+"
        r"Adam\s+(?:8|S)?S?\.?\s*Bennion"
    ),
    # "We Are All Alike -- But Different" — body has "Wo Are All Alike"
    "We Are All Alike -- But Different": (
        r"(?:W|Wo)(?:e|o)\s+Are\s+All\s+Alike"
    ),
    # Macbeth literature lesson: body has "Macheth" (OCR b→h)
    "Literature: Macbeth, Villain or Hero?": (
        r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s\-\u2014\u2013:]*"
        r"(?:Shakespea[rt]e\s+in\s+Our\s+Lives\s*)?"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
        r"Mac[bh]eth"
    ),
    # Theology with "Eternal Life" — body has Sheology—The Doctrine and Covenants Lesson 9—"He That Hath Eternal Life Is Rich"
    "Theology -- He That Hath Eternal Life Is Rich": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"[\s\S]*?(?:Lesson\s+\d+)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,5}\s*"
        r"(?:H|Fl)e\s+(?:Th|Sh|Ch)at\s+(?:H|Fl)ath\s+Eternal\s+Life"
    ),
    # Wee Pine Knot (body has no "The" prefix)
    "Wee Pine Knot -- Third Prize Story": (
        r"Wee[\s\-\u2014\u2013]+Pine[\s\-\u2014\u2013]+Knot"
    ),
    # Examined Life - OCR: "Extmined "Fia" for "Examined Life" (quote between words)
    "The Examined Life -- Third Prize Poem": (
        r"(?:Th|Sh|Ch)e[\s\-\u2014\u2013]+(?:Ex(?:t|a)mined|Extmined)[\s\-\u2014\u2013\"\u201c\u201d]+(?:Life|Fia|Lie)"
    ),
    # Fifty Singing Aprils
    "Fifty Singing Aprils -- Second Prize Story": (
        r"(?:F|St)ifty[\s\-\u2014\u2013]+Singing[\s\-\u2014\u2013]+Aprils"
    ),
    # February editorial: body has "Be My Valentine" OCR'd as various forms
    "Editorial: Be My Valentine": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
        r"Be\s+My\s+(?:V|U)ale(?:n|m)t(?:i|l)(?:n|m)e"
    ),
    # June editorial: 128th Annual Church Conference (OCR: "She 128th Jaa Church Conference")
    "Editorial: The 128th Annual Church Conference": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|MIT\s*ORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
        r"128th"
    ),
    # Everyday Life in Colonial New England
    "Everyday Life in Colonial New England -- Part I": (
        r"EVERYDAY[\s\-\u2014\u2013]+LIFE[\s\-\u2014\u2013]+IN[\s\-\u2014\u2013]+COLONIAL"
    ),
    "Everyday Life in Colonial New England -- Part II": (
        r"EVERYDAY[\s\-\u2014\u2013]+LIFE[\s\-\u2014\u2013]+IN[\s\-\u2014\u2013]+COLONIAL"
    ),
    # Not to the Swift Chapter 6 (Conclusion) — OCR: "Cuapter"
    "Not to the Swift -- Chapter 6 -- Conclusion": (
        r"[\"\u201c\u2018]?(?:N|V|T|l)ot\s+to\s+the\s+Swift[\"\u201d\u2019]?"
        r".{0,80}?"
        r"(?:(?:CHAPTER|Chapter|Crapter|Cuapter)\s+)?(?:6|VI)\b"
    ),
    # Serial fiction: Elizabeth's Children (Chapters 1-8)
    "Elizabeth's Children -- Chapter 1": _ELIZABETHS_CHILDREN_PAT + r"(?:1|I)\b",
    "Elizabeth's Children -- Chapter 2": _ELIZABETHS_CHILDREN_PAT + r"(?:2|II)\b",
    "Elizabeth's Children -- Chapter 3": _ELIZABETHS_CHILDREN_PAT + r"(?:3|III)\b",
    "Elizabeth's Children -- Chapter 4": _ELIZABETHS_CHILDREN_PAT + r"(?:4|IV)\b",
    "Elizabeth's Children -- Chapter 5": _ELIZABETHS_CHILDREN_PAT + r"(?:5|V)\b",
    "Elizabeth's Children -- Chapter 6": _ELIZABETHS_CHILDREN_PAT + r"(?:6|VI)\b",
    "Elizabeth's Children -- Chapter 7": _ELIZABETHS_CHILDREN_PAT + r"(?:7|VII)\b",
    "Elizabeth's Children -- Chapter 8": _ELIZABETHS_CHILDREN_PAT + r"(?:8|VIII)\b",
    # Serial fiction: Not to the Swift (Chapters 1-6)
    "Not to the Swift -- Chapter 1": _NOT_TO_SWIFT_PAT + r"(?:1|I)\b",
    "Not to the Swift -- Chapter 2": _NOT_TO_SWIFT_PAT + r"(?:2|II)\b",
    "Not to the Swift -- Chapter 3": _NOT_TO_SWIFT_PAT + r"(?:3|III)\b",
    "Not to the Swift -- Chapter 4": _NOT_TO_SWIFT_PAT + r"(?:4|IV)\b",
    "Not to the Swift -- Chapter 5": _NOT_TO_SWIFT_PAT + r"(?:5|V)\b",
    "Not to the Swift -- Chapter 6": _NOT_TO_SWIFT_PAT + r"(?:6|VI)\b",
}


def build_regex_for_title(title: str) -> re.Pattern:
    """Build a flexible regex pattern for matching a title in OCR'd body text."""
    # Sort by key length descending so more specific (longer) keys match first
    for header_text, header_pat in sorted(_KNOWN_HEADER_PATTERNS.items(),
                                           key=lambda x: len(x[0]), reverse=True):
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

        if ch in '\u2014\u2013-' or ch == ' ':
            # Collect entire run of spaces, dashes, hyphens
            has_space = (ch == ' ')
            has_dash = (ch in '\u2014\u2013-')
            j = i + 1
            while j < len(title) and title[j] in ' \u2014\u2013-':
                if title[j] == ' ':
                    has_space = True
                else:
                    has_dash = True
                j += 1
            # Emit a single flexible separator pattern
            # At least one whitespace or dash char required
            parts.append(r'[,;]?[\s\-\u2014\u2013]+[\(\u2018\u2019\u0027"]*')
            at_word_start = True
            i = j
            continue
        elif ch in "\u2018\u2019\u201c\u201d'\"":  # curly/straight quotes
            parts.append(r'[\u2018\u2019\u201c\u201d\u2032\u0027"\']*\s*')
        elif ch == ':':
            parts.append(r':?\s*')
            at_word_start = True
        elif ch == ',':
            parts.append(r',?\s*')
        elif ch == '?':
            parts.append(r'\??')
        elif ch == '!':
            parts.append(r'!?')
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
        line = re.sub(r'^\s*(?:Page\s+)?\d+\s*$', '', line, flags=re.IGNORECASE).strip()
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
        "MONTHLY BY THE GENERAL BOARD",
    ]

    for marker in toc_end_markers:
        match = re.search(marker, text, re.IGNORECASE)
        if match:
            front_matter = text[:match.start()]
            body = text[match.start():]
            return front_matter, body

    # Fallback: split on subscription info
    fallback_markers = [
        r"Payable\s+in\s+advance",
        r"Entered\s+\w+\s+second.class\s+matter",
    ]
    for pat in fallback_markers:
        match = re.search(pat, text, re.IGNORECASE)
        if match:
            end_pos = text.find('\n', match.end())
            if end_pos == -1:
                end_pos = match.end()
            front_matter = text[:end_pos]
            body = text[end_pos:]
            return front_matter, body

    # Last resort: find the longest line (body text) and split before it
    lines = text.split('\n')
    if len(lines) > 2:
        max_len = 0
        max_idx = 0
        for idx, line in enumerate(lines):
            if len(line) > max_len:
                max_len = len(line)
                max_idx = idx
        if max_len > 10000:  # body line is very long
            split_pos = sum(len(lines[i]) + 1 for i in range(max_idx))
            front_matter = text[:split_pos]
            body = text[split_pos:]
            return front_matter, body

    print("WARNING: Unable to find front matter split marker, treating all text as body")
    return "", text


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

    # Second pass: try matching unmatched entries against full text
    matched_titles = {e["title"] for _, e in found}
    unmatched_entries = [e for e in entries if e["title"] not in matched_titles]
    front_matter_found = []  # entries found in front matter (separate extraction)
    if unmatched_entries:
        full_text_found = _match_entries_with_strategy(text, unmatched_entries)
        body_offset = len(front_matter)
        for pos, entry in full_text_found:
            adjusted_pos = pos - body_offset
            if adjusted_pos < 0:
                # Entry is in front matter — extract separately, don't place in body
                front_matter_found.append((pos, entry))
            else:
                found.append((adjusted_pos, entry))

    # Compute boundaries for front-matter entries and extract their content
    front_matter_extracts = {}  # title -> content string
    if front_matter_found:
        fm_bounds = _boundaries_from_found(front_matter, front_matter_found)
        for start, end, entry in fm_bounds:
            raw_text = front_matter[start:end].strip()
            cleaned, _ = strip_running_noise(raw_text)
            front_matter_extracts[entry["title"]] = cleaned.strip()

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

    # Build ordered list: body entries first (by position), then front-matter entries
    title_order = [e["title"] for _, _, e in bounds]
    # Add front-matter entries at the end (they appear before body content in the issue)
    fm_titles = [e["title"] for _, e in sorted(front_matter_found, key=lambda x: x[0])]
    title_order = fm_titles + title_order  # front matter first, then body

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
        elif title in front_matter_extracts:
            cleaned = front_matter_extracts[title]
            raw_len = len(cleaned)
            filename = f"{idx:02d}_{title_safe}.txt"
            if not dry_run and cleaned:
                (issue_dir / filename).write_text(cleaned, encoding="utf-8")

            match_result = {
                "file": filename,
                "path": rel_dir,
                "position": -1,  # indicates front-matter origin
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol45"
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
        description="Extract Relief Society Magazine Vol 45 into individual entries"
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

    for (vol, issue_key), entries in VOL45_TOC.items():
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
