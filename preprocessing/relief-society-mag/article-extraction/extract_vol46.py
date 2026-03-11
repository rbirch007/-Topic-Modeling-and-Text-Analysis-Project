#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 46 (1959).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol46.py
    python extract_vol46.py --dry-run
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

VOL46_TOC = {
    ("Vol46", "No01_January_1959"): [
        # SPECIAL FEATURES
        {"title": "A New Year's Greeting", "author": None, "etype": "article"},
        {"title": "Relief Society -- An Aid to the Priesthood", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Mountain Peaks", "author": "Celia Luce", "etype": "article"},
        {"title": "Award Winners -- Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "The Telling -- First Prize Poem", "author": "Lael Woolsey Hill", "etype": "poem"},
        {"title": "Portrait of Lincoln's Second Mother -- Second Prize Poem", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Parting on the Prairie -- Third Prize Poem", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Award Winners -- Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Good Bye and Good Luck, Mrs. Kelsey -- First Prize Story", "author": "Norma A. Wrathall", "etype": "fiction"},
        {"title": "Exploring New Frontiers in Health", "author": "Basil O'Connor", "etype": "article"},
        {"title": "The California Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "No, Thank You!", "author": None, "etype": "article"},
        {"title": "The Rewarding Time", "author": "Elsie Sim Hansen", "etype": "article"},
        # FICTION
        # OCR note: TOC says "Chapter 2]" but body text confirms Chapter 1
        {"title": "The Silver Leash -- Chapter 1", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        # OCR note: "VAPTU OS" is garbled OCC for "Virtues"
        {"title": "Editorial: Strengthening Community Virtues", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Four Color Covers -- A New Feature for The Relief Society Magazine", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the California Mission", "author": "Alta H. Taylor", "etype": "article"},
        {"title": "You Can Sew -- XI -- Bound Buttonholes", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Celestia Chadwick Tracy's Quilts Have Made Many Homes Beautiful", "author": None, "etype": "article"},
        # LESSONS FOR APRIL
        {"title": "Theology: The Sacrament", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Pray Always, and I Will Pour Out My Spirit Upon You", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Managerial Aspects of Food Planning and Preparation", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature: Young Jonathan Edwards", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: A Principle With Promise", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "Every Good Gift", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Cloud Feathers", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Quiescence", "author": "June N. Ashton", "etype": "poem"},
        {"title": "Deserted Farm Yard", "author": "Maude Rubin", "etype": "poem"},
        # OCR note: TOC title garbled as "WCU MMtetey WANES"; confirmed from body text as "January"
        {"title": "January", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Song of Subsequence", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Winter Tree", "author": "Bernice Ames", "etype": "poem"},
    ],

    ("Vol46", "No02_February_1959"): [
        # SPECIAL FEATURES
        {"title": "Our Homes -- An Individual Responsibility", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "The Central Atlantic States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "A Fireside Chat on a Burning Question", "author": None, "etype": "article"},
        # FICTION
        {"title": "We Can't All Be Generals -- Second Prize Story", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "Contentment, Thou Art Priceless!", "author": "Leone O. Jacobs", "etype": "fiction"},
        {"title": "The Silver Leash -- Chapter 2", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Obligations of a Mother's Authority", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Birthday Congratulations to Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Central Atlantic States Mission", "author": "Lovell W. Smith", "etype": "article"},
        {"title": "Cooking With Dry Milk", "author": "Marian Bennion", "etype": "article"},
        {"title": "You Can Sew -- XII -- Plackets", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Chloe V. Hatch Daines Makes Rose Design Quilts", "author": None, "etype": "article"},
        {"title": "My Loveliest Valentine", "author": "Mabel Law Atkinson", "etype": "article"},
        # LESSONS FOR MAY
        {"title": "Theology: The Revelation to Emma Hale Smith", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Be Patient in Afflictions, for Thou Shalt Have Many", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Managerial Aspects of Clothing the Family", "author": "Vesta Barnett", "etype": "lesson"},
        {"title": "Literature: Jonathan Edwards, Puritan", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: How Do I Rate?", "author": "John Farr Larson", "etype": "lesson"},
        # POETRY
        {"title": "Hills Made Low", "author": "Alice Morrey Bailey", "etype": "poem"},
        # OCR note: "GeNefOSity" = "Generosity"
        {"title": "Generosity", "author": "Jane B. Wunderlich", "etype": "poem"},
        {"title": "Winter Comes to the Hills", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        # OCR note: "Beckomed" = "Beckoned"
        {"title": "When Greatness Beckoned", "author": "Iris W. Schow", "etype": "poem"},
        # OCR note: TOC title "BUI tc Sy, peep NUTT 0g" is garbled; confirmed from body as "Inimitable"
        {"title": "Inimitable", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "The Leaven of Laughter", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Reasons Manifold", "author": "Margaret B. Shomaker", "etype": "poem"},
        # OCR note: TOC title garbled as "Fira) WraOVAW Ge patie"; confirmed from body as "An Untold Sale"
        {"title": "An Untold Sale", "author": "June N. Ashton", "etype": "poem"},
        # OCR note: TOC title garbled as "TS FRsyANCICCAIRIGL"; confirmed from body as "The Pyracantha"
        {"title": "The Pyracantha", "author": "Christie Lund Coles", "etype": "poem"},
        # OCR note: TOC title garbled as "MVS COMA VOT CMEC"; confirmed from body as "A Listening Face"
        {"title": "A Listening Face", "author": "Alice R. Rich", "etype": "poem"},
        # OCR note: TOC title garbled as "NSCON Dy Camere"; title unrecoverable from OCR
        {"title": "Cherry McKay poem (title unrecoverable from OCR)", "author": "Cherry McKay", "etype": "poem"},
    ],

    ("Vol46", "No03_March_1959"): [
        # SPECIAL FEATURES
        {"title": "Let Us Cherish One Another", "author": "Hulda Parker", "etype": "article"},
        # OCR note: "Presion" = "Preston"
        {"title": "The Central States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "The Old Man of the Mountain", "author": "Martha Robeson Wright", "etype": "article"},
        {"title": "A Fireside Chat On a Burning Question", "author": None, "etype": "article"},
        {"title": "The American National Red Cross and Its Field of Service", "author": "O. C. Duckett", "etype": "article"},
        {"title": "The Second Mile", "author": "Effie K. Driggs", "etype": "article"},
        # FICTION
        {"title": "The House on Cherry Lane Drive -- Third Prize Story", "author": "Sarah O. Moss", "etype": "fiction"},
        {"title": "Not of This Fold", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Rachel Goes to Relief Society", "author": "Elizabeth C. McCrimmon", "etype": "fiction"},
        {"title": "Love Me Tomorrow", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "The Silver Leash -- Chapter 3", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Now, Let Us Rejoice", "author": None, "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Central States Mission", "author": "May E. J. Dyer", "etype": "article"},
        {"title": "What Is a Home For?", "author": "Leona F. Wintch", "etype": "article"},
        # OCR note: "Argel] Tree" = "Angel Tree"
        {"title": "The Angel Tree", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Hold Everything", "author": "Sylvia Pezoldt", "etype": "article"},
        {"title": "You Can Sew -- XIII -- Selection of Children's Clothes", "author": "Jean R. Jennings", "etype": "article"},
        # OCR note: "co Srmile" = "a Smile"
        {"title": "The Value of a Smile", "author": "Myrtle S. Hyde", "etype": "article"},
        {"title": "Lily E. A. Minner Makes Satin Quilts for Her Grandchildren", "author": None, "etype": "article"},
        {"title": "A Mother's Prayer", "author": "Verla R. Hull", "etype": "article"},
        {"title": "Security", "author": "Vernessa M. Nagle", "etype": "article"},
        # POETRY
        {"title": "The Edge of Spring", "author": "Renie H. Littlewood", "etype": "poem"},
        # OCR note: TOC just lists "Dorothy J. Roberts, 181" without title; confirmed from body as "Song of a Tree"
        {"title": "Song of a Tree", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Silhouette", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Window Lilies", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Grandma's Crazy Quilt", "author": "Elizabeth MacDougall", "etype": "poem"},
        {"title": "Faith", "author": "Iris W. Schow", "etype": "poem"},
        # Additional poems found in body text (TOC was truncated):
        # OCR note: "Tlountain (Born" = "Mountain Born"
        {"title": "Mountain Born", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Your Name Was Clarissa", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        # OCR note: "She Prairie Wind" = "The Prairie Wind"
        {"title": "The Prairie Wind", "author": "June N. Ashton", "etype": "poem"},
        {"title": "Boy With a Book", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol46", "No04_April_1959"): [
        # SPECIAL FEATURES
        {"title": "Messengers of Faith and Charity", "author": "Spencer W. Kimball", "etype": "article"},
        {"title": "The East Central States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "The Right Circles", "author": None, "etype": "article"},
        {"title": "Guard Your Family -- Fight Cancer With a Checkup and a Check", "author": "Esther Allegretti", "etype": "article"},
        {"title": "About Twilight", "author": "Amy Viau", "etype": "article"},
        # FICTION — SPECIAL APRIL SHORT STORIES
        {"title": "Unto the Hills", "author": "Helen Hooper", "etype": "fiction"},
        {"title": "The Bishop's Wife", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "The Day I Turned Eight", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "Great-Grandmother's Notebook", "author": "Arlene D. Cloward", "etype": "fiction"},
        {"title": "The Silver Leash -- Chapter 4", "author": "Beatrice R. Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: School Thy Feelings", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Seven Months of Color", "author": "Eva Willes Wangsgaard", "etype": "article"},
        # OCR note: garbled title, likely "Lightweight Garments" or similar
        {"title": "Lightweight Garments", "author": "Marion Winterbottom", "etype": "article"},
        {"title": "Recipes From the East Central States Mission", "author": "Marie Curtis Richards", "etype": "article"},
        # OCR note: "WOKS" is garbled
        {"title": "Walks", "author": "Grace Ingles Frost", "etype": "article"},
        {"title": "You Can Sew -- XIV -- Children's Clothes -- Infants and Toddlers", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Rozella Dowdle Kingsford Makes Lace Tablecloths and Braided Rugs", "author": None, "etype": "article"},
        {"title": "Easy Soap Recipe", "author": "Vera C. Stratford", "etype": "article"},
        {"title": "Now You Know You're Living", "author": "Mary E. Knowles", "etype": "article"},
        {"title": "Home Decorators", "author": "Joyce K. MacKabe", "etype": "article"},
        {"title": "The Hole in the Fence", "author": "Dorothy Oakley Rea", "etype": "article"},
        # POETRY
        {"title": "Above the Morning", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "April Evening", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Nature's Prayer", "author": "Helen Hurr", "etype": "poem"},
        {"title": "Wild Primrose", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Wake Me", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Song for Her Soul", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Old Logging Road", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Benediction", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "My Love Is You", "author": "Maxene Jennings", "etype": "poem"},
        {"title": "Grandma Reminisces", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "This Year's Spring", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Precious Token", "author": "Rowena Jensen Bills", "etype": "poem"},
    ],

    ("Vol46", "No05_May_1959"): [
        # SPECIAL FEATURES
        {"title": "Abraham Lincoln -- A Study in Adversity", "author": "A. Hamer Reiser", "etype": "article"},
        {"title": "The Eastern States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Contest Announcements -- 1959", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Would You Write Poetry?", "author": "Sylvia Probst Young", "etype": "article"},
        {"title": "What's in a Story?", "author": "Norma A. Wrathall", "etype": "article"},
        {"title": "The Right Circles", "author": None, "etype": "article"},
        # FICTION
        {"title": "Louise", "author": "Helen M. Livingston", "etype": "fiction"},
        {"title": "Gem of the Hills", "author": "Lydia M. Sorensen", "etype": "fiction"},
        # OCR note: TOC says "Chapter 9" but body confirms Chapter 5
        {"title": "The Silver Leash -- Chapter 5", "author": "Beatrice R. Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Books -- Recorders for the Ages", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Magazine Honor Roll for 1958", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Eastern States Mission", "author": "Florence S. Jacobsen", "etype": "article"},
        {"title": "Weed", "author": "Celia Luce", "etype": "article"},
        {"title": "You Can Sew -- XV -- Children's Clothing", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Maggie Richards Wood Specializes in Making Lace Tablecloths", "author": None, "etype": "article"},
        # POETRY
        {"title": "Promise", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "To My Daughter", "author": "Camilla Woodbury Judd", "etype": "poem"},
        {"title": "Dream Come True", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "My Mother", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Grandmother's Pinks", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Old Fishermen", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "When Deserts Bloom in Arizona", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "The Temple", "author": "Winona F. Thomas", "etype": "poem"},
        {"title": "Spring's Golden Web", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Sun in Bloom", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Temple Marriage", "author": "Ann Barber Fletcher", "etype": "poem"},
    ],

    ("Vol46", "No06_June_1959"): [
        # SPECIAL FEATURES
        {"title": "A Tribute to Fathers", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "The Great Lakes Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "More Precious Than Rubies", "author": None, "etype": "article"},
        {"title": "Country Summer", "author": "Rodello Hunter", "etype": "article"},
        {"title": "Green-Willow Days", "author": "Shirley Sealy", "etype": "article"},
        {"title": "Relief Society for Mothers and Daughters", "author": "Permella Haggard", "etype": "article"},
        # FICTION
        {"title": "By All Means", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Peach-Tree Poem", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "One of the Best", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Granny Will Be Writing", "author": "Betty Martin", "etype": "fiction"},
        {"title": "The Ladder of Love", "author": "Margaret Russell", "etype": "fiction"},
        {"title": "The Silver Leash -- Chapter 6", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 129th Annual Church Conference", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Great Lakes Mission", "author": "Vonda L. Christensen", "etype": "article"},
        {"title": "A Handy Pincushion", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "A Challenge to Mothers", "author": "Leona Fetzer Wintch", "etype": "article"},
        {"title": "The Wedding-Ring Tree", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Carlota de Yalibat's Unique Hobbies", "author": None, "etype": "article"},
        {"title": "The Pansy-Picker", "author": "Vernessa M. Nagle", "etype": "article"},
        # LESSON DEPARTMENT -- PREVIEWS FOR 1959-60
        {"title": "Theology: The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Truths to Live By From the Doctrine and Covenants", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Physical Safety Factors in the Home", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: America's Literature -- A New Nation Speaks", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Spiritual Living in the Nuclear Age", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
        # POETRY
        {"title": "My Thoughts Kneel Down", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "The Handcart Child", "author": "Orvene B. Holman", "etype": "poem"},
        {"title": "The Superstition Mountain", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Lilac's Journey", "author": "Lula Walker", "etype": "poem"},
        {"title": "For Grandmothers Who Baby Sit", "author": "Camilla Woodbury Judd", "etype": "poem"},
        {"title": "Heart of a House", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Blue Morning-Glories", "author": "Josie B. Bay", "etype": "poem"},
        {"title": "The Hummingbird", "author": "Winona Frandsen Thomas", "etype": "poem"},
        {"title": "If This Is Peace", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "So Thought Unfolds", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Heritage", "author": "Viola A. Cornwall", "etype": "poem"},
        {"title": "Where the Gull Goes", "author": "Gwen Marler Barney", "etype": "poem"},
    ],

    ("Vol46", "No07_July_1959"): [
        # SPECIAL FEATURES
        {"title": "In Memoriam -- President Stephen L Richards", "author": None, "etype": "article"},
        {"title": "America's Challenge -- Individual Righteousness", "author": "A. Theodore Tuttle", "etype": "article"},
        {"title": "Fanny Steenblik Kienitz Appointed to General Board of Relief Society", "author": "Elsie M. Belliston", "etype": "article"},
        {"title": "The Gulf States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "More Precious Than Rubies", "author": None, "etype": "article"},
        # FICTION
        {"title": "Blue Voile for Dreams", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "Pretending Garden", "author": "Helen H. Trutton", "etype": "fiction"},
        {"title": "Such a Heavenly Secret", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "The Silver Leash -- Chapter 7", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Planning Summer Activities for Children", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Gulf States Mission", "author": "Emma A. Hanks", "etype": "article"},
        {"title": "Jewels for the Table", "author": "Marian Gardner Nielson", "etype": "article"},
        {"title": "Meletia Miles Makes Sweaters in Unusual Designs", "author": None, "etype": "article"},
        {"title": "Recipe for Cold Water Soap", "author": None, "etype": "article"},
        # LESSONS FOR OCTOBER
        {"title": "Theology: Magnify Thine Office", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: ... Take Upon You My Whole Armor...", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Child Safety", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: Introduction: Light and Fire", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Spiritual Road", "author": "Blaine M. Porter", "etype": "lesson"},
        # POETRY
        {"title": "Summer Cry", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Summit", "author": "Maude Rubin", "etype": "poem"},
        {"title": "To Benjamin Franklin", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "First in the Valley", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Search Not Stars", "author": "Leona Fetzer Wintch", "etype": "poem"},
        {"title": "Coit Tower, San Francisco", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Elizabeth", "author": "Ethel Jacobson", "etype": "poem"},
    ],

    ("Vol46", "No08_August_1959"): [
        # NOTE: August 1959 has no formal Table of Contents in the OCR text.
        # Entries below are reconstructed from body text and article headers.
        # SPECIAL FEATURES
        {"title": "President J. Reuben Clark, Jr., First Counselor in the First Presidency", "author": "Marion G. Romney", "etype": "article"},
        {"title": "The New England Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "How Can We Effectively Delegate Responsibility?", "author": "Belle S. Spafford", "etype": "article"},
        # OCR note: "RANCES G. KNIGHT" = Frances G. Knight profile
        {"title": "Frances G. Knight", "author": None, "etype": "article"},
        {"title": "Fresh Up With Sunday", "author": None, "etype": "article"},
        # FICTION
        {"title": "The Silver Leash -- Chapter 8 (Conclusion)", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Reverence", "author": None, "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": None, "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the New England Mission", "author": "Margaret Romney Jackson", "etype": "article"},
        # LESSONS FOR NOVEMBER
        {"title": "Theology: Thou Shalt Not Command Him Who Is at Thy Head", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Again I Say Unto You, Let Every Man Esteem His Brother as Himself", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Electricity", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: Benjamin Franklin, Printer", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Developing Emotional Maturity", "author": "Blaine M. Porter", "etype": "lesson"},
        # POETRY
        # OCR note: "6n olding QO wietude" = "Enfolding Quietude"
        {"title": "Enfolding Quietude", "author": "Renie H. Littlewood", "etype": "poem"},
        # OCR note: "olalr: Gucha nge" = garbled; poem about grandmother's apron; title unrecoverable
        {"title": "Polar Change", "author": "Maude Rubin", "etype": "poem"},
        # OCR note: "Oo immy Meets His Tlew Sister Jimmy" = "Jimmy Meets His New Sister"
        {"title": "Jimmy Meets His New Sister", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Challenge", "author": "Vesta N. Lukei", "etype": "poem"},
        # OCR note: "Ya rring llote" likely = "Jarring Note"
        {"title": "Jarring Note", "author": "Ethel Jacobson", "etype": "poem"},
        # OCR note: "Begin N ing" = "Beginning"
        {"title": "Beginning", "author": "Gwen Marler Barney", "etype": "poem"},
        {"title": "Staff for the Aged", "author": "Mabel Law Atkinson", "etype": "poem"},
        # OCR note: "Delayed Homecom ing" = "Delayed Homecoming"
        {"title": "Delayed Homecoming", "author": "Alice R. Rich", "etype": "poem"},
        {"title": "Strange Reward", "author": "Ida Elaine James", "etype": "poem"},
    ],

    ("Vol46", "No09_September_1959"): [
        # SPECIAL FEATURES
        {"title": "Birthday Congratulations to President David O. McKay", "author": None, "etype": "article"},
        {"title": "Care of the Aged in Relation to Eternal Values", "author": "Delbert L. Stapley", "etype": "article"},
        {"title": "The North Central States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "The Real Worth of The Relief Society Magazine", "author": "Daniel H. Ludlow", "etype": "article"},
        {"title": "Fresh Up With Sunday", "author": None, "etype": "article"},
        # FICTION
        {"title": "Next Door", "author": "Verona T. Bowen", "etype": "fiction"},
        {"title": "Mother's Late Day", "author": "Elizabeth C. McCrimmon", "etype": "fiction"},
        {"title": "A Is for Apron -- Part I", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Standard for Government", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "The Four Standard Works of the Church to Be Issued in New Edition", "author": None, "etype": "article"},
        {"title": "New Serial The New Day to Begin in October", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the North Central States Mission", "author": "Diana F. Child", "etype": "article"},
        {"title": "Rocking-Chair Lamp", "author": "Vernessa M. Nagle", "etype": "article"},
        {"title": "Bulbs for Winter Blooming", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Althea Bingham Bitton -- Woman of Artistic Hobbies", "author": None, "etype": "article"},
        # LESSONS FOR DECEMBER
        {"title": "Theology: Some Future Events", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: He That Receiveth My Law and Doeth It, the Same Is My Disciple", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Fire Hazards", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: Benjamin Franklin, Retired", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: No Lesson Outlined", "author": None, "etype": "lesson"},
        # POETRY
        {"title": "Harvest", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "September Afternoon", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Enlisted", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "The Edge of Summer", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Summer Was Too Brief", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "The Windows of Heaven", "author": "Vera Geddes Merrill", "etype": "poem"},
    ],

    ("Vol46", "No10_October_1959"): [
        # SPECIAL FEATURES
        # OCR note: "Lecrning" = "Learning"
        {"title": "... Seek Learning Even by Study and Also by Faith", "author": "Roy W. Doxey", "etype": "article"},
        {"title": "The Northern California Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Fair or False Faces", "author": None, "etype": "article"},
        # FICTION
        # OCR note: "Chapter 1]" likely = Chapter 1
        {"title": "The New Day -- Chapter 1", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "A Is for Apron -- Part II (Conclusion)", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "Quilts and Mothers-in-Law", "author": "Mabel Law Atkinson", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Establishing Family Patterns", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Recipes From the Northern California Mission", "author": "Hazel S. Love", "etype": "article"},
        {"title": "Making Christmas Ornaments Can Be Fun", "author": "Hannah Nowell", "etype": "article"},
        {"title": "Margaret Pitts Finds Self-Expression in Crochet Work", "author": None, "etype": "article"},
        {"title": "The Watch", "author": "Olive Peabody", "etype": "article"},
        # LESSONS FOR JANUARY
        {"title": "Theology: Spiritual Creations", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Therefore He That Lacketh Wisdom", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Household Hazards", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: Two Early American Quakers: Penn and Woolman", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Role of Emotional Maturity in Spiritual Living", "author": "Blaine M. Porter", "etype": "lesson"},
        # POETRY
        {"title": "These Cool Acres", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Yet Beauty Comes", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Call Me a Squirrel If You Like", "author": "Gwen Marler Barney", "etype": "poem"},
        {"title": "Distraction", "author": "Winona F. Thomas", "etype": "poem"},
        {"title": "All Is Well", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        # OCR note: "IB ey crm C, COMM LY ATMO" garbled; confirmed from body as "Baja California"
        {"title": "Baja California", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Last Born", "author": "Ora Pate Stewart", "etype": "poem"},
        # OCR note: "Autumn FSS or tere" garbled; confirmed from body as "Autumn Leases"
        {"title": "Autumn Leases", "author": "Katherine B. Bowles", "etype": "poem"},
        {"title": "They Shall Find Peace", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Open the Door of Your Heart", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Sounds He Missed", "author": "Lula Walker", "etype": "poem"},
        # OCR note: "PRATT, VATA" garbled; confirmed from body as "Autumn Wind"
        {"title": "Autumn Wind", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        # OCR note: "Berett Of SOMG" = "Bereft of Song"
        {"title": "Bereft of Song", "author": "Maude O. Cook", "etype": "poem"},
    ],

    ("Vol46", "No11_November_1959"): [
        # SPECIAL FEATURES
        {"title": "The Strength of Prayer", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Study My Word", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Strengthening the Family Through Observance of the Sabbath", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "Elizabeth Bennett Winters Appointed to the General Board of Relief Society", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "LaRue H. Rosell Appointed to the General Board of Relief Society", "author": "Edith P. Backman", "etype": "article"},
        {"title": "Jennie R. Scott Appointed to the General Board of Relief Society", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Fair or False Faces", "author": None, "etype": "article"},
        {"title": "A Message and an Answer", "author": None, "etype": "article"},
        # FICTION
        {"title": "The Shining Gift", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "The New Day -- Chapter 2", "author": "Hazel K. Todd", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Thanksgiving", "author": "Louise W. Madsen", "etype": "editorial"},
        # FEATURES FOR THE HOME
        {"title": "In-Store Shopping", "author": "Avonell S. Rappleye", "etype": "article"},
        {"title": "Five Ways to Put You in Yuletide", "author": "Barbara Williams", "etype": "article"},
        {"title": "Selma Larson Spjut Makes Ski Sweaters and Crocheted Tablecloths", "author": None, "etype": "article"},
        # LESSONS FOR FEBRUARY
        {"title": "Theology: Lessons in Obedience", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Wherefore, Hear My Voice", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Hobby Hazards", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: Two Eighteenth Century Observers: Byrd and Crevecoeur", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Individual and Religious Maturity -- Part I", "author": "Blaine M. Porter", "etype": "lesson"},
        # POETRY
        {"title": "According to My Needs", "author": "Caroline E. Miner", "etype": "poem"},
        {"title": "Josie", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Not Always Will They Come", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Unused Gifts", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Mountain Climber", "author": "Lula Walker", "etype": "poem"},
        {"title": "Frosty Verse", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Familiar Face", "author": "Gwen Marler Barney", "etype": "poem"},
        {"title": "Cliff-Dwelling Mother", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Lace", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Free Gifts", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "The Little Musician", "author": "Evelyn Fjeldsted", "etype": "poem"},
    ],

    ("Vol46", "No12_December_1959"): [
        # SPECIAL FEATURES
        {"title": "Every Family a Missionary Family", "author": "Henry D. Moyle", "etype": "article"},
        {"title": "Howard William Hunter of the Council of the Twelve", "author": "Hugh B. Brown", "etype": "article"},
        {"title": "Annual General Relief Society Conference", "author": "Hulda Parker", "etype": "article"},
        {"title": "Relief Society Magazine Awarded Cover Citation", "author": None, "etype": "article"},
        {"title": "The Literature of Christmas", "author": "Mabel Harmer", "etype": "article"},
        {"title": "Make This Caller Welcome", "author": None, "etype": "article"},
        # FICTION
        {"title": "The Miracle Mile", "author": "Leola Seely Anderson", "etype": "fiction"},
        {"title": "The New Day -- Chapter 3", "author": "Hazel K. Todd", "etype": "fiction"},
        # GENERAL FEATURES
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Supreme Gift", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The 129th Semi-Annual Church Conference", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        # FEATURES FOR THE HOME
        {"title": "Flower Arrangement -- New Fashioned -- Old Fashioned", "author": "Sylvia Pezoldt", "etype": "article"},
        {"title": "Apricot Salad", "author": "Edna Lind Cole", "etype": "article"},
        {"title": "Say Merry Christmas With Fancy Yeast Rolls", "author": "Ruby K. Smith", "etype": "article"},
        {"title": "Elva M. Tingey Makes Beautiful and Useful Articles With Plastic Foam", "author": None, "etype": "article"},
        {"title": "Fruit Salad", "author": "Joan Staley", "etype": "article"},
        {"title": "When Mothers Sing", "author": "Leona F. Smith", "etype": "article"},
        # LESSONS FOR MARCH
        {"title": "Theology: And the Kingdom Grew", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And The Book of Mormon and the Holy Scriptures Are Given of Me for Your Instruction", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Safety Precautions in Medicine and Household Items", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: Thomas Paine, Revolutionist", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Individual and Religious Maturity -- Part II", "author": "Blaine M. Porter", "etype": "lesson"},
        # POETRY
        {"title": "Dividers of the Stars", "author": "Vesta P. Crawford", "etype": "poem"},
        {"title": "To Hold Us Fast", "author": "Clara Steen Chesnutt", "etype": "poem"},
        {"title": "No Christmas Here?", "author": "Maude Rubin", "etype": "poem"},
        {"title": "These Cliffs Hold Melody", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        # OCR note: "Messin" = "Messiah"
        {"title": "Handel's Messiah", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Home Impressions", "author": "Hannah C. Ashby", "etype": "poem"},
        {"title": "Night Sky Before Snowfall", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Oh, Traveler!", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "We Walk With Angels", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "A Story Never Old", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Winter Bride", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Seven", "author": "Gwen Marler Barney", "etype": "poem"},
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
    year = 1959

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol46_{key}.txt"
    ISSUE_FILES[("Vol46", key)] = (fname, month)


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
# Known OCR-mangled section headers for Vol46.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
# "The Silver Leash" by Beatrice Rordame Parsons (Chapters 1-8, Jan-Aug)
_SILVER_LEASH_PAT = (
    r"Silver\s+Leash"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|Crapter|Cuapter)\s+)?"
)
# "The New Day" by Hazel K. Todd (Chapters 1-3, Oct-Dec)
_NEW_DAY_PAT = (
    r"(?:Th|Sh|Ch)e\s+(?:N|V|T|l)ew\s+Day"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|Crapter|Cuapter)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—The Doctrine and Covenants Lesson N (also clean Theology in some months)
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
    # Body has: EDITORIAL m2/c2O/c~2/cmC VOL. 46 MONTH 1959 NO. N Title
    # OCR variants: EDITORIAL with noise prefixes
    "Editorial: ": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Body has: Literature — A New Nation Speaks Lesson N (Jul-Dec, OCR: "AL", "A,", "A.")
    # or: Literature — Meet the New World Lesson N (Jan-Jun, from previous year's curriculum)
    # Pattern requires em-dash + series title to avoid matching casual mentions
    "Literature: ": (
        r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s]*[\-\u2014\u2013][\s]*"
        r"(?:A[L.,\s]+(?:N|V|T|l)ew\s+(?:N|V|T|l)ation\s+Speaks|Meet\s+the\s+(?:N|V|T|l)ew\s+World)\s+"
        r"(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013=]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Literature -- ": (
        r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s]*[\-\u2014\u2013][\s]*"
        r"(?:A[L.,\s]+(?:N|V|T|l)ew\s+(?:N|V|T|l)ation\s+Speaks|Meet\s+the\s+(?:N|V|T|l)ew\s+World)\s+"
        r"(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013=]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    # Body has: Social Science—Latter-day Saint Family Life Lesson N (Jan-Jun)
    # or: Social Science—Spiritual Living in the Nuclear Age Lesson N (Jul-Dec)
    # Pattern requires em-dash + series title immediately after "Social Science" to avoid casual mentions
    "Social Science: ": (
        r"(?:So|Se|O)[a-z]?(?:c|t|o)ial\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"(?:Latter[\-\s]day\s+Saint\s+Family\s+Life|Spi\s*[rt]\s*i?tual\s+Living\s+in\s+the\s+Nuclear\s+Age)"
        r"\s+(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Social Science -- ": (
        r"(?:So|Se|O)[a-z]?(?:c|t|o)ial\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"(?:Latter[\-\s]day\s+Saint\s+Family\s+Life|Spi\s*[rt]\s*i?tual\s+Living\s+in\s+the\s+Nuclear\s+Age)"
        r"\s+(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    # Body has: Vork IT leeting / Work Meeting) / Vork ITleeting — series title ... Discussion N
    # Series: "Managerial Aspects" (Jan-Jun), "Physical Safety Factors in the Home" (Jul-Dec)
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
    # Prize stories: body doesn't have "First/Second/Third Prize Story" suffix
    "Good Bye and Good Luck, Mrs. Kelsey -- First Prize Story": (
        r"Good\s+Bye\s+and\s+Good\s+Luck"
    ),
    "We Can't All Be Generals -- Second Prize Story": (
        r"We\s+Can[\u2019\u0027]?t\s+All\s+Be\s+Generals"
    ),
    "The House on Cherry Lane Drive -- Third Prize Story": (
        r"(?:Th|Sh|Ch)e\s+(?:H|Fl)ouse\s+on\s+Cherry\s+Lane"
    ),
    # Award Winners
    "Award Winners -- Eliza R. Snow Poem Contest": (
        r"Award[\s\-\u2014\u2013]+Winners[\s\S]{0,30}?(?:E|6)liza[\s\-\u2014\u2013]+R\.?[\s\-\u2014\u2013]+Snow[\s\-\u2014\u2013]+Poem"
    ),
    "Award Winners -- Annual Relief Society Short Story Contest": (
        r"Award[\s\-\u2014\u2013]+Winners[\s\S]{0,30}?(?:Short|Annual)"
    ),
    # Generic lesson preview entries (June Teaching Aids section)
    "Literature: America's Literature -- A New Nation Speaks": (
        r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s]*[\-\u2014\u2013][\s]*"
        r"America[\u2018\u2019\u0027]?s\s+Literature[\s\-\u2014\u2013:]*A\s+(?:N|V|T|l)ew\s+(?:N|V|T|l)ation"
    ),
    "Work Meeting: Physical Safety Factors in the Home": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|I|M)?(?:\s+)?(?:l)?(?:e|E|c)[a-z]?ting"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"Physical\s+Sa[fl]ety"
    ),
    # In Memoriam entry for President Richards
    "In Memoriam -- President Stephen L Richards": (
        r"(?:(?:I|(?:I|\|)n)\s+(?:M|IT|Nl)l?emoriam|emonai)"
        r"[\s\-\u2014\u2013]+"
        r"(?:President\s+)?Stephen"
    ),
    # The Silver Leash -- Chapter 8 (Conclusion)
    "The Silver Leash -- Chapter 8 (Conclusion)": (
        _SILVER_LEASH_PAT + r"(?:8|VIII)\b"
    ),
    # A Is for Apron -- Part II (Conclusion)
    "A Is for Apron -- Part II (Conclusion)": (
        r"A\s+(?:I|(?:I|\|))s\s+for\s+Apron"
        r".{0,80}?"
        r"(?:Part\s+)?(?:II|2)"
    ),
    # Social Science: No Lesson Outlined (September)
    "Social Science: No Lesson Outlined": (
        r"(?:So|Se|O)[a-z]?(?:c|t|o)ial\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"[\s]*[\-\u2014\u2013:]*[\s]*"
        r"(?:N|V|T|l)o\s+Lesson"
    ),
    # June Social Science preview: full title match needed
    "Social Science: Spiritual Living in the Nuclear Age": (
        r"(?:So|Se|O)[a-z]?(?:c|t|o)ial\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"[\s]*[\-\u2014\u2013:]*[\s]*"
        r"Spi\s*[rt]\s*i?tual\s+Living\s+in\s+the\s+Nuclear\s+Age"
        r"[\s\S]*?Preview"
    ),
    # June editorial: 129th Annual Church Conference
    "Editorial: The 129th Annual Church Conference": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
        r"129th"
    ),
    # December: 129th Semi-Annual Church Conference
    "The 129th Semi-Annual Church Conference": (
        r"(?:Th|Sh|Ch)e\s+129th\s+Semi"
    ),
    # --- Specific OCR fixes for unmatched entries ---
    # Jan: "Strengthening Community Virtues" — body: "Trengthening Commu nity UV igae"
    "Editorial: Strengthening Community Virtues": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IIT\s*ORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
        r"(?:S|T)(?:t|s)rengthening"
    ),
    # Jan: "Quiescence" — body: "QUIeCSCENCE"
    "Quiescence": (
        r"QUI(?:e|E)?(?:C|c)S(?:C|c)ENCE"
    ),
    # Feb: "Inimitable" — body: "oGhimitable" (In→Gh, with prefix o artifact)
    "Inimitable": (
        r"(?:In|Gh)imitable"
    ),
    # Feb: "When Greatness Beckoned" — body: "Beckomed" (n→m)
    "When Greatness Beckoned": (
        r"When\s+Greatness\s+Beck(?:on|om)ed"
    ),
    # Mar: "Mountain Born" — body: "Tlountain (Born" (M→Tl)
    "Mountain Born": (
        r"(?:M|Tl|IT|Il|Nl)l?ountain\s*[\(\u2018\u2019\u0027\"]*\s*Born"
    ),
    # Apr: "April Evening" — body: "April Everimg" (nin→rim)
    "April Evening": (
        r"April\s+Eve(?:ning|rimg|n[il]ng)"
    ),
    # Apr: "Easy Soap Recipe" — body: '"Easy Soap\'! Recipe'
    "Easy Soap Recipe": (
        r"[\"\u201c\u2018]*Easy\s+Soap[\u2019\u0027!\"]*\s*Recipe"
    ),
    # Apr: "Lightweight Garments" — OCR garbled title; body has "Jo a Sriend" = "To a Friend"
    "Lightweight Garments": (
        r"(?:J|T)o\s+a\s+(?:S|F)riend\s*(?:\S+\s+)?Marion\s+Winterbottom"
    ),
    # Jun: Theology preview (no lesson number)
    "Theology: The Doctrine and Covenants": (
        r"(?:Th|Sh|Ch)eology[\s]*[\-\u2014\u2013][\s]*"
        r"(?:Th|Sh|Ch)e\s+Doctrine\s+and\s+Covenants"
    ),
    # Jul: Literature with "Introduction: Light and Fire" — body: Lesson 9—= Introduction
    "Literature: Introduction: Light and Fire": (
        r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s]*[\-\u2014\u2013][\s]*"
        r"(?:A[L.,\s]+(?:N|V|T|l)ew\s+(?:N|V|T|l)ation\s+Speaks|Meet\s+the\s+(?:N|V|T|l)ew\s+World)\s+"
        r"(?:Lesson\s+\d+)[\s\-\u2014\u2013=]*[\"\u201c\u2018\u2019\u0027\s]{0,10}"
        r"Introduction"
    ),
    # Aug: "Enfolding Quietude" — body: "6n olding QO wietude" (front matter area)
    "Enfolding Quietude": (
        r"(?:E|6)n\s+olding\s+QO\s+wietude"
    ),
    # Aug: "Polar Change" — body: "olalr: Gucha nge"
    "Polar Change": (
        r"olalr:\s*Gucha\s*nge"
    ),
    # Aug: "Jimmy Meets His New Sister" — body: "immy Meets His Tlew Sister"
    "Jimmy Meets His New Sister": (
        r"(?:J|Y|O)?(?:o\s+)?immy\s+(?:M|IT|Nl)l?eets\s+(?:H|Fl)is\s+(?:N|V|T|Tl)(?:l)?ew\s+Sister"
    ),
    # Aug: "Jarring Note" — body: "Ya rring llote" or "arring Note"
    "Jarring Note": (
        r"(?:J|Y)a?\s*rring\s+(?:N|l|V)l?ote"
    ),
    # Aug: "Delayed Homecoming" — body: "Delayed Homecom ing"
    "Delayed Homecoming": (
        r"Delayed\s+(?:H|Fl)omecom\s*ing"
    ),
    # Aug: "Frances G. Knight" — body: "RANCES G. KNIGHT"
    "Frances G. Knight": (
        r"(?:F|St|R)?(?:R|r)?ANCES\s+G\.?\s+KNIGHT"
    ),
    # Aug: "Theology: Thou Shalt Not Command..." — body: Lesson 18—"... Thou Shalt Not Command
    "Theology: Thou Shalt Not Command Him Who Is at Thy Head": (
        r"(?:Th|Sh|Ch)eology[\s]*[\-\u2014\u2013][\s]*"
        r"[\s\S]*?(?:Lesson\s+\d+)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\.]{0,10}\s*"
        r"(?:Th|Sh|Ch)ou\s+Shalt\s+(?:N|V|T|l)ot\s+Command"
    ),
    # Sep: "A Is for Apron -- Part I" — body: '"A" Is for Apron'
    "A Is for Apron -- Part I": (
        r"[\"\u201c\u2018]*A[\"\u201d\u2019]*\s+(?:I|(?:I|\|))s\s+for\s+Apron"
        r".{0,80}?"
        r"(?:Part\s+)?(?:I|1)\b"
    ),
    # Sep: "New Serial The New Day to Begin in October"
    "New Serial The New Day to Begin in October": (
        r"(?:N|V|T|l)ew\s+Serial\s*[\"\u201c\u2018]*\s*(?:Th|Sh|Ch)e\s+(?:N|V|T|l)ew\s+Day"
    ),
    # Oct: "... Seek Learning Even by Study and Also by Faith" — body: "Seek Lecrning"
    "... Seek Learning Even by Study and Also by Faith": (
        r"Seek\s+Le(?:ar|cr)ning\s+Even\s+by\s+Study"
    ),
    # Nov: "Five Ways to Put You in Yuletide" — body: 'Five Ways to Put "You" in Yuletide'
    "Five Ways to Put You in Yuletide": (
        r"(?:F|St)ive\s+Ways\s+to\s+Put\s*[\"\u201c\u2018]*\s*(?:Y|J)ou[\"\u201d\u2019]*\s+in\s+Yuletide"
    ),
    # Dec: "Night Sky Before Snowfall" — body: "Snowlal)"
    "Night Sky Before Snowfall": (
        r"(?:N|V|T|l)ight\s+Sky\s+Before\s+Snow(?:fall|lal\))"
    ),
    # Oct: "Baja California" — body: "Baja Ca lifo m ia" (broken)
    "Baja California": (
        r"Baja\s+Ca\s*lifo\s*r?\s*n?\s*m?\s*ia"
    ),
    # Dec: "Say Merry Christmas With Fancy Yeast Rolls" — body: "Say ''Merry Christmas''"
    "Say Merry Christmas With Fancy Yeast Rolls": (
        r"Say\s*[\"\u201c\u2018\u0027]*\s*(?:M|IT|Nl)l?erry\s+Christmas[\"\u201d\u2019\u0027]*\s+With"
    ),
    # Serial fiction: The Silver Leash (Chapters 1-8)
    "The Silver Leash -- Chapter 1": _SILVER_LEASH_PAT + r"(?:1|I)\b",
    "The Silver Leash -- Chapter 2": _SILVER_LEASH_PAT + r"(?:2|II)\b",
    "The Silver Leash -- Chapter 3": _SILVER_LEASH_PAT + r"(?:3|III)\b",
    "The Silver Leash -- Chapter 4": _SILVER_LEASH_PAT + r"(?:4|IV)\b",
    "The Silver Leash -- Chapter 5": _SILVER_LEASH_PAT + r"(?:5|V)\b",
    "The Silver Leash -- Chapter 6": _SILVER_LEASH_PAT + r"(?:6|VI)\b",
    "The Silver Leash -- Chapter 7": _SILVER_LEASH_PAT + r"(?:7|VII)\b",
    # Serial fiction: The New Day (Chapters 1-3)
    "The New Day -- Chapter 1": _NEW_DAY_PAT + r"(?:1|I)\b",
    "The New Day -- Chapter 2": _NEW_DAY_PAT + r"(?:2|II)\b",
    "The New Day -- Chapter 3": _NEW_DAY_PAT + r"(?:3|III)\b",
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol46"
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
        description="Extract Relief Society Magazine Vol 46 into individual entries"
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

    for (vol, issue_key), entries in VOL46_TOC.items():
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
