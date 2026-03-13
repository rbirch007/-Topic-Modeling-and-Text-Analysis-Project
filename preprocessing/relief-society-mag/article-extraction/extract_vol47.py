#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 47 (1960).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol47.py
    python extract_vol47.py --dry-run
"""

import argparse
import csv
import json
import os
import re
import shutil
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

VOL47_TOC = {
    ("Vol47", "No01_January_1960"): [
        {"title": "New Year's Greeting", "author": None, "etype": "article"},
        {"title": "In Memoriam: President Amy Brown Lyman", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Obedience to the Truth", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Award Winners -- Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Immigrant's Child", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "According to the Day", "author": "Lucille R. Perry", "etype": "poem"},
        {"title": "Loam-Stained", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Award Winners -- Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Summer's Grace", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "The Northern States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Oh Say, What Is Truth?", "author": None, "etype": "article"},
        {"title": "Prevent Crippling Diseases", "author": "Basil O'Connor", "etype": "article"},
        {"title": "More Precious Than Riches", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "The New Day, Chapter 4", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Days of a", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Dust of Every-Dayness", "author": "Celia Luce", "etype": "article"},
        {"title": "Recipes From the Northern States Mission", "author": "Vera C. Stratford", "etype": "article"},
        {"title": "Rosella Jenkins Makes Quilts and Rugs", "author": None, "etype": "article"},
        {"title": "Theology: A Trial of Faith", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Govern Your House in Meekness, and Be Steadfast", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Food Care and Preservation", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: The Federalists (and the Great Transition)", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Creative and Spiritual Living -- Pathways to Peace -- Part I", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "No One Too Poor", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Years", "author": "Roxana Farnsworth Hase", "etype": "poem"},
        {"title": "What Gifts I Bring", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "I Could Not Cry", "author": "Gladys Hesser Burnham", "etype": "poem"},
        {"title": "Ruth to Boaz", "author": "Katherine F. Larsen", "etype": "poem"},
    ],

    ("Vol47", "No02_February_1960"): [
        {"title": "Hour of Waiting", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Responsibility of Relief Society Officers in the Salvation of Relief Society Members", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Relief Society and the Church Welfare Program", "author": "Henry D. Taylor", "etype": "article"},
        {"title": "The Northwestern States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Oh Say, What Is Truth?", "author": None, "etype": "article"},
        {"title": "Grandpa's Red Suspenders", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "Only the Essentials", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "The New Day, Chapter 5", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Greatness From Righteous", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the Northwestern States Mission", "author": "Effie K. Driggs", "etype": "article"},
        {"title": "In Time for Gertrude Lacy", "author": None, "etype": "article"},
        {"title": "Kindness", "author": "Ida Isaacson", "etype": "article"},
        {"title": "Anchor", "author": "Celia Luce", "etype": "article"},
        {"title": "Theology: The Great I Am", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Be Faithful Unto the End, and Lo, I Am With You", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Simple First Aid Helps", "author": "Charlotte A. Larsen", "etype": "lesson"},
        {"title": "Literature: Thomas Jefferson (1743-1826)", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Creative and Spiritual Living -- Pathways to Peace", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "Letter from a Missionary", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Solace in Prayer", "author": "Rowena Jensen Bills", "etype": "poem"},
        {"title": "With Nothing in His Hands", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Alberta Revisited", "author": "Helen Kimball Orgill", "etype": "poem"},
        {"title": "Winter Garden", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "What Can I Give You?", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol47", "No03_March_1960"): [
        {"title": "Cup of Faith", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Beauty in the Home", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "Spiritual Living -- Pathway to Peace", "author": "Blaine M. Porter", "etype": "article"},
        {"title": "The Southern States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "The American Red Cross and Its Campaign for Members and Funds", "author": "Theodore V. Houser", "etype": "article"},
        {"title": "An Ounce of Precaution", "author": "Mabel Harmer", "etype": "article"},
        {"title": "The Relief Society Magazine in Durban, South Africa", "author": "Muriel Wilson", "etype": "article"},
        {"title": "The Fishbite Story", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "A Place for Everything", "author": "Charmaine Kohler", "etype": "fiction"},
        {"title": "Offerings of the Heart", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "With a Song in My Heart", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "The New Day, Chapter 6", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Influence of", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the Southern States Mission", "author": "Lucile W. Bunker", "etype": "article"},
        {"title": "Whys, Wherefores, and Fun With Green Plants", "author": "Maude N. Howard", "etype": "article"},
        {"title": "Dreams", "author": "Celia Luce", "etype": "article"},
        {"title": "A Peppermint-Stick Party", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Kathryn A. Carne -- Artist, Nurse, Homemaker", "author": None, "etype": "article"},
        {"title": "A Quick Fade-Out", "author": "Sylvia Pezoldt", "etype": "article"},
        {"title": "Reward of Obedience", "author": "Flora J. Isgreen", "etype": "article"},
        {"title": "Bluebird", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "March Time", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Miraculous Advent", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "This I Know", "author": "Mabel Jones Gabbott", "etype": "poem"},
    ],

    ("Vol47", "No04_April_1960"): [
        {"title": "Words of Easter", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "The Restoration", "author": "Antoine R. Ivins", "etype": "article"},
        {"title": "The West Central States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Using the Blackboard in Teaching Lessons in Relief Society", "author": "William E. Berrett", "etype": "article"},
        {"title": "The Widening Circle", "author": "Charlotte R. Leyden", "etype": "article"},
        {"title": "That Special Flavor", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "Uncle Matt and the China Doll", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "The Blue Bowl, Part I", "author": "Loya Beck", "etype": "fiction"},
        {"title": "Room in Her Heart", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "To Die Before Thy Time", "author": "Helen Bay Gibbons", "etype": "fiction"},
        {"title": "The New Day, Chapter 7", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: They Shall Speak With New Tongues", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the West Central States Mission", "author": "Anna C. Merrill", "etype": "article"},
        {"title": "Cosmetics for Grandma", "author": "Esther H. Lamb", "etype": "article"},
        {"title": "Planters for the Patio", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Thirteen Don'ts in Sewing for a Best-Dressed You", "author": "Wilma M. Rich", "etype": "article"},
        {"title": "Moonlight", "author": "Celia Luce", "etype": "article"},
        {"title": "Christening the New Carriage", "author": "Lula Walker", "etype": "article"},
        {"title": "The Old Red Couch", "author": "Helen B. Morris", "etype": "article"},
        {"title": "Applesauce", "author": "Myrtle Ainsworth", "etype": "article"},
        {"title": "Anna Whitney Johnson -- Gifted Artist", "author": None, "etype": "article"},
        {"title": "A Touch of the Divine", "author": "Wilma Boyle Bunker", "etype": "article"},
        {"title": "A Christmas Chest for All the Year", "author": "Elizabeth C. McCrimmon", "etype": "article"},
        {"title": "April Road", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Blue Talisman of Spring", "author": None, "etype": "poem"},
    ],

    ("Vol47", "No05_May_1960"): [
        {"title": "For Mother", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "What the Gospel Means to Me", "author": "Irene B. Woodford", "etype": "article"},
        {"title": "The Western States Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Contest Announcements -- 1960", "author": None, "etype": "article"},
        {"title": "I, Too, Want to Be Useful", "author": "Aslaug S. Vaieland", "etype": "article"},
        {"title": "Magazine Honor Roll for 1959", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Orchids in the Snow, Chapter 1", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "Second Baby", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "Standing Pat", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "The Blue Bowl, Part II", "author": "Loya Beck", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Wife and", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the Western States Mission", "author": "Daisy R. Romney", "etype": "article"},
        {"title": "The Golden Years", "author": "Maggie Tolman Porter", "etype": "article"},
        {"title": "Not Only By Bread", "author": "Dorothy J. Roberts", "etype": "article"},
        {"title": "Crossed Wires", "author": "Genevieve Van Wagenen", "etype": "article"},
        {"title": "Annie Maria Spray Steel Makes Many Braided Rugs", "author": None, "etype": "article"},
        {"title": "When Parents Play", "author": "Ruby Dee Christensen", "etype": "article"},
        {"title": "These Small Things", "author": "Maude Rubin", "etype": "poem"},
        {"title": "The Native Currant", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "From a Canyon Retreat", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Mother", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Your Sacred Presence", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "My Gifts", "author": "May H. Marsh", "etype": "poem"},
        {"title": "A Case for Contrast", "author": "Evalyn Miller Sandberg", "etype": "poem"},
        {"title": "Respite", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Contemplation", "author": None, "etype": "poem"},
    ],

    ("Vol47", "No06_June_1960"): [
        {"title": "Where Summer Goes", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Emma Ray Riggs McKay", "author": "Emma Rae McKay Ashton", "etype": "article"},
        {"title": "Annual Report for 1959", "author": "Hulda Parker", "etype": "article"},
        {"title": "Needed by Someone", "author": "Helen H. Trutton", "etype": "fiction"},
        {"title": "You'll Always Be Rich", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "Orchids in the Snow, Chapter 2", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "Fiddlers Three", "author": "Lula Walker", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: 130th Annual", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "For a Flower", "author": "Leone H. Simms", "etype": "article"},
        {"title": "Menus for Special Dinners", "author": "Ruby K. Smith", "etype": "article"},
        {"title": "Mary Sorenson Johnsen Finds Enough Hobbies to Make Her Happy", "author": None, "etype": "article"},
        {"title": "My Mother and Her Hemstitching Machine", "author": "Fay McCurdy Bailey", "etype": "article"},
        {"title": "Theology: The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Truths to Live By From The Doctrine and Covenants", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Caring for the Sick in the Home", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: America's Literature Comes of Age", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Spiritual Living in the Nuclear Age", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "Swallows", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Sounds in Summer", "author": "Renie H. Littlewood", "etype": "poem"},
        {"title": "Summer Night", "author": "Maxine R. Jennings", "etype": "poem"},
        {"title": "Prayer", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "Forecast", "author": "Evalyn M. Sandberg", "etype": "poem"},
        {"title": "Smoke-Warm Grasses", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Vista", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Give Me the Mesa", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Small Gypsy", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Mountain Cabin", "author": None, "etype": "poem"},
    ],

    ("Vol47", "No07_July_1960"): [
        {"title": "Song for My Country", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "History Turns Back Its Pages -- The Bee Hive House Restored", "author": "Helen Young Spencer Williams", "etype": "article"},
        {"title": "Summer Picnic", "author": "Leone E. McCune", "etype": "article"},
        {"title": "No Comments, Please", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "In Memory of Miss Ollie", "author": "Pauline L. Jensen", "etype": "fiction"},
        {"title": "Where Is Johnny?", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Orchids in the Snow, Chapter 3", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Vacationing", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Bazaar Sewing -- Multi-Purpose Bag", "author": "Jean R. Jennings", "etype": "article"},
        {"title": "Yarn Stitched Bag Made of Chair Webbing", "author": "Melba Larson", "etype": "article"},
        {"title": "Anna M. Jensen Begins Housekeeping -- Again -- At Eighty-Nine", "author": None, "etype": "article"},
        {"title": "Joy of Sorrow", "author": "Celia Luce", "etype": "article"},
        {"title": "Theology: A Promise Fulfilled", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: If Thou Lovest Me", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Safeguarding the Health of Your Family", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: Expanding Horizons", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Expanding Our Religious Horizons", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "Her Patterned Lace", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Frontier Garden", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Poverty", "author": "Grace Ingles Frost", "etype": "poem"},
        {"title": "To a Pioneer Mother", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Testimony", "author": "Marvel Sharp Crookston", "etype": "poem"},
        {"title": "Solitude", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Pioneer Woman", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Sunflowers", "author": "Eva M. Bird", "etype": "poem"},
    ],

    ("Vol47", "No08_August_1960"): [
        {"title": "With Inner Balm", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "The Sixth White House Conference on Children and Youth", "author": "Marion D. Hanks", "etype": "article"},
        {"title": "Alice Ludlow Wilkinson Appointed to the General Board of Relief Society", "author": "Lyman S. Tyler", "etype": "article"},
        {"title": "LaPriel Strong Bunker Appointed to the General Board of Relief Society", "author": "Zina Y. C. Brown", "etype": "article"},
        {"title": "Marie Curtis Richards Appointed to the General Board of Relief Society", "author": "Hulda Parker", "etype": "article"},
        {"title": "Irene Williams Buehner Appointed to the General Board of Relief Society", "author": "Conrad Harrison", "etype": "article"},
        {"title": "History Turns Back Its Pages -- The Bee Hive House Restored -- Part II", "author": "Helen Young Spencer Williams", "etype": "article"},
        {"title": "Handwork for Happiness", "author": None, "etype": "article"},
        {"title": "Orchids in the Snow, Chapter 4", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "One of Them", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Value of Work", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Cutting Patterns", "author": "Evelyn Dorio", "etype": "article"},
        {"title": "A Musical Hobby -- Florence Bellows, Organist for Everything", "author": None, "etype": "article"},
        {"title": "Theology: The Law of Moral Conduct", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Ye Must Give Thanks Unto God", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Manifestations of Illness", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: Washington Irving, Polished Paradox", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Expanding Our Religious Horizons -- Part II: Concepts of Man", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "Day at Galilee", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Hasten Slowly", "author": "Roberta L. Theobald", "etype": "poem"},
        {"title": "Of the Night", "author": "Zara Sabin", "etype": "poem"},
        {"title": "First Grandchild", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Close of Day", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Night Song in August", "author": "Beulah Huish Sadleir", "etype": "poem"},
    ],

    ("Vol47", "No09_September_1960"): [
        {"title": "Lombardy Poplars", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Birthday Congratulations to President David O. McKay", "author": None, "etype": "article"},
        {"title": "Sleep When the Wind Blows", "author": "Mildred B. Eyring", "etype": "article"},
        {"title": "Are You Happy?", "author": "Marvel Sharp Crookston", "etype": "article"},
        {"title": "Evening Incident", "author": "Helen S. Hughes", "etype": "article"},
        {"title": "My Third Grandma, Part I -- Sari", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "The Rich, Full Years", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "A Problem, a Pet, and the Picture", "author": "Louise Morris Kelley", "etype": "fiction"},
        {"title": "Orchids in the Snow, Chapter 5", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Family Living", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Fall Bounty", "author": "Roberta L. Theobald", "etype": "article"},
        {"title": "Best Sellers for the Bazaar", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Freezing Food at Home", "author": "Ruby K. Smith", "etype": "article"},
        {"title": "Something Different for Dinner", "author": "Emma A. Hanks", "etype": "article"},
        {"title": "Mary Bartholomew Stewart Makes Her Home Beautiful With Handwork", "author": None, "etype": "article"},
        {"title": "Theology: The Law of Moral Conduct (continued)", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Every Man Is Given a Gift", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Moving and Lifting the Patient", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: James Fenimore Cooper, Critic", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Late Summer", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Child Blowing a Dandelion", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Grandmother's Art", "author": "Lula Walker", "etype": "poem"},
        {"title": "Green Thumb", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "A Stake Is Born", "author": "Padda M. Speller", "etype": "poem"},
        {"title": "Gratitude", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "September Road", "author": "Katherine F. Larsen", "etype": "poem"},
    ],

    ("Vol47", "No10_October_1960"): [
        {"title": "Tentacles of Time", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Temple Square in Salt Lake City", "author": "Preston Nibley", "etype": "article"},
        {"title": "Faith", "author": "Pauline L. Jensen", "etype": "article"},
        {"title": "Three Silver Boxes", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "My Third Grandma, Part II -- Butcher Knife", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "Orchids in the Snow, Chapter 6", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Daughters of Zion", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Christmas Aprons", "author": "Shirley Thulin", "etype": "article"},
        {"title": "A Strawberry Pincushion", "author": "Melba Larson", "etype": "article"},
        {"title": "Recipes for Autumn Luncheons", "author": "Florence S. Jacobsen", "etype": "article"},
        {"title": "June I. Hunsaker's Hobby: Human Relationships", "author": None, "etype": "article"},
        {"title": "Theology: The Law of Consecration", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Thou Shalt Not Be Idle", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Making the Patient Comfortable", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: Natty Bumppo, American Fiction-Hero", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Expanding Our Religious Horizons -- Part III -- Man's Relatedness to the World", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "This Sun-Walled Hour", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Afterglow", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Thy Word", "author": "Nancy Wilcox", "etype": "poem"},
        {"title": "Not Anything Is Lost", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Small Son", "author": "Aretta N. Ricks", "etype": "poem"},
        {"title": "Vase of Autumn Leaves", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Recompense", "author": "Velda Allphin Neilson", "etype": "poem"},
        {"title": "Willow in Autumn", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "A Poet's Prayer", "author": "Matia McClelland Burk", "etype": "poem"},
    ],

    ("Vol47", "No11_November_1960"): [
        {"title": "Need of Pilgrims", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Serving With Full Intent of Heart", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Making a Heaven at Home", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Homemaking -- A Noble Calling and Privilege", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Temple Square in Salt Lake City -- Part II", "author": "Preston Nibley", "etype": "article"},
        {"title": "Home for Christmas", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "My Third Grandma, Part III -- Ants", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "Orchids in the Snow, Chapter 7", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Thankfulness for a Prophet", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Jessie Evans Smith's Hobby -- A Useful Invention", "author": None, "etype": "article"},
        {"title": "Playthings From Costa Rica", "author": "Holly B. Keddington", "etype": "article"},
        {"title": "Holiday Food Suggestions", "author": "Ethel C. Smith", "etype": "article"},
        {"title": "Theology: The Law of Administration to the Sick", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Wherefore, I Am in Your Midst, and I Am the Good Shepherd", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Routine Comfort Measures", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: William Cullen Bryant", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Stages of Religious Development", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "November Instant", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Silver Web", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Earning Learning", "author": "Roberta L. Theobald", "etype": "poem"},
        {"title": "To an Aged, Bereft Mother", "author": "Zara Sabin", "etype": "poem"},
        {"title": "The Listening Ones", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Wild Geese", "author": "Matia McClelland Burk", "etype": "poem"},
        {"title": "Snowbird", "author": "Ethel Jacobson", "etype": "poem"},
    ],

    ("Vol47", "No12_December_1960"): [
        {"title": "From a Far Country", "author": "Vesta P. Crawford", "etype": "poem"},
        {"title": "God's Greatest Gift", "author": "Henry D. Moyle", "etype": "article"},
        {"title": "Nathan Eldon Tanner Appointed Assistant to the Council of the Twelve", "author": "Hugh B. Brown", "etype": "article"},
        {"title": "Franklin D. Richards Appointed Assistant to the Council of the Twelve", "author": "LeGrand Richards", "etype": "article"},
        {"title": "Theodore Moyle Burton Appointed Assistant to the Council of the Twelve", "author": "Gordon B. Hinckley", "etype": "article"},
        {"title": "The Annual General Relief Society Conference", "author": "Hulda Parker", "etype": "article"},
        {"title": "The Need Is Urgent -- A Message From the National Tuberculosis Association", "author": "Franklin K. Brough", "etype": "article"},
        {"title": "Grandma's Surprise Packages", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "My Third Grandma, Part IV -- Nandi", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "Orchids in the Snow, Chapter 8", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: 130th Semi-Annual", "author": None, "etype": "editorial"},
        {"title": "Christmas for the Children", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Christmas Treats for Company", "author": "Winnifred C. Jardine", "etype": "article"},
        {"title": "The Trees the Family Made", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Let's Dress Dolls for Christmas", "author": "Shirley Thulin", "etype": "article"},
        {"title": "The Enchanted Clothesline Christmas", "author": "Alice M. Graves", "etype": "article"},
        {"title": "Theology: The Past, Present, and Future", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Ye Must Grow in Grace", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Elimination of Body Wastes", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: Ralph Waldo Emerson, American Idealist", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Values -- Their Growth and Meaning", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "First Snow", "author": "Blanche Kendall McKey", "etype": "poem"},
        {"title": "Wearing the Smell of Woods", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Christmas Morning Child", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Memories", "author": "Zara Sabin", "etype": "poem"},
        {"title": "At Winter's Edge", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "What Would I Do?", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Thankfulness", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "December Rain", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "White Decree", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Wealth", "author": "Diane Montgomery", "etype": "poem"},
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
    year = 1960

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol47_{key}.txt"
    ISSUE_FILES[("Vol47", key)] = (fname, month)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# OCR initial-letter substitution table.
# The OCR consistently misreads decorative/large-font initials:
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
    # "M" may OCR as "IT", "ITl", "Tl" (e.g. "Made" -> "ITlade")
    "M": r"(?:M|IT(?:l)?|Tl)",
    "m": r"(?:m|it(?:l)?|tl)",
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers for Vol47.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns

# The New Day: Chapters 4-7 (Jan-Apr 1960)
_NEW_DAY_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)e\s+New\s+Day"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Orchids in the Snow: Chapters 1-8 (May-Dec 1960)
_ORCHIDS_CHAPTER_PAT = (
    r"Orchids\s+in\s+(?:th|sh|ch)e\s+Snow"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# The Blue Bowl: Parts I-II (Apr-May 1960)
_BLUE_BOWL_PAT = (
    r"(?:Th|Sh|Ch)e\s+Blue\s+Bowl"
    r".{0,80}?"
    r"(?:(?:Part|PART)\s+)?"
)

# My Third Grandma: Parts I-IV (Sep-Dec 1960)
_MY_THIRD_GRANDMA_PAT = (
    r"(?:My|Ty|Mly|M)\s*(?:Th|Sh|Ch)ird\s+G\s*randma"
    r".{0,80}?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—Tthe Doctrine and Covenants Lesson NN—SUBTITLE
    # or: Sheology—The Doctrine and Covenants Lesson NN—SUBTITLE
    # TOC has: Theology: SUBTITLE
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:(?:Lesson|Discussion)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: siting Seacher ITlessages—Truths to Live By From The Doctrine and Covenants Message NN—SUBTITLE
    # TOC has: Visiting Teacher Messages: SUBTITLE
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:(?:T|S)(?:e|l|a)(?:a|e|c)(?:ch|l!|h)?(?:er)?|Seal!?\s*He)\s+"
        r"(?:M|IT|T|m|He|HeIT)(?:l)?essages?"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:(?:Message|Lesson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|\.…]?)?"
    ),
    "From Near and Far": (
        r"(?:F|St)rom\s+Near\s+(?:and|Gnd|And|and)\s+(?:F|S|t)ar[r]?"
    ),
    # Body has: EDITORIAL ... VOL. 47 MONTH 1960 NO. N <actual title>
    # TOC has: Editorial: <actual title>
    "Editorial: ": (
        r"EDITORIAL"
        r"[\s\S]{0,80}?"
    ),
    "Editorials: ": (
        r"EDITORIAL[S]?"
        r"[\s\S]{0,80}?"
    ),
    # Body has: Fo iterature—America's Literature — A New Nation Speaks Lesson NN—SUBTITLE
    # or: Literature—America's Literature Comes of Age Lesson NN—SUBTITLE
    # TOC has: Literature: SUBTITLE
    "Literature: ": (
        r"(?:(?:Fo\s+i|S\s+i|O|Of?\s+i|of\s+i)?)?"
        r"[Ll]?iterature"
        r"[\s\-\u2014\u2013:]*"
        r"(?:America'?s?\s+Literature)?"
        r"[\s\S]{0,120}?"
        r"(?:(?:Lesson|Preview)\s+\d*)?"
        r"[\s\-\u2014\u2013]*"
    ),
    # Body has: Social Science—Spiritual Living in the Nuclear Age Lesson NN—SUBTITLE
    # TOC has: Social Science: SUBTITLE
    "Social Science: ": (
        r"Social\s+Sc(?:i|t|l)ence"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:(?:Lesson|Preview)\s+\d+)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Vork ITleeting—Caring for the Sick in the Home ... Lesson N—SUBTITLE
    # or: Work ITleeting—The Art of Homemaking ... Discussion N—SUBTITLE
    # TOC has: Work Meeting: SUBTITLE
    "Work Meeting: ": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St)rom\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St)(?:i|e)?eld"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St)(?:i|e)?eld"
        r":?\s*"
    ),
    # Serial fiction: The New Day (Chapters 4-7, Jan-Apr)
    "The New Day, Chapter 4": _NEW_DAY_CHAPTER_PAT + r"(?:4|IV)\b",
    "The New Day, Chapter 5": _NEW_DAY_CHAPTER_PAT + r"(?:5|V)\b",
    "The New Day, Chapter 6": _NEW_DAY_CHAPTER_PAT + r"(?:6|VI)\b",
    "The New Day, Chapter 7": _NEW_DAY_CHAPTER_PAT + r"(?:(?:7|VII)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: Orchids in the Snow (Chapters 1-8, May-Dec)
    "Orchids in the Snow, Chapter 1": _ORCHIDS_CHAPTER_PAT + r"(?:1|I)\b",
    "Orchids in the Snow, Chapter 2": _ORCHIDS_CHAPTER_PAT + r"(?:2|II)\b",
    "Orchids in the Snow, Chapter 3": _ORCHIDS_CHAPTER_PAT + r"(?:3|III)\b",
    "Orchids in the Snow, Chapter 4": _ORCHIDS_CHAPTER_PAT + r"(?:4|IV)\b",
    "Orchids in the Snow, Chapter 5": _ORCHIDS_CHAPTER_PAT + r"(?:5|V)\b",
    "Orchids in the Snow, Chapter 6": _ORCHIDS_CHAPTER_PAT + r"(?:6|VI)\b",
    "Orchids in the Snow, Chapter 7": _ORCHIDS_CHAPTER_PAT + r"(?:7|VII)\b",
    "Orchids in the Snow, Chapter 8": _ORCHIDS_CHAPTER_PAT + r"(?:(?:8|VIII|§)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: The Blue Bowl (Parts I-II, Apr-May)
    "The Blue Bowl, Part I": _BLUE_BOWL_PAT + r"(?:I|1)\b",
    "The Blue Bowl, Part II": _BLUE_BOWL_PAT + r"(?:II|2)\b",
    # Serial fiction: My Third Grandma (Parts I-IV, Sep-Dec)
    "My Third Grandma, Part I -- Sari": _MY_THIRD_GRANDMA_PAT + r"(?:(?:(?:Part|Parr)\s+)?(?:I|1)\b|Sari)",
    "My Third Grandma, Part II -- Butcher Knife": _MY_THIRD_GRANDMA_PAT + r"(?:(?:(?:Part|Parr)\s+)?(?:II|2)\b|Butcher)",
    "My Third Grandma, Part III -- Ants": _MY_THIRD_GRANDMA_PAT + r"(?:(?:(?:Part|Parr)\s+)?(?:III|3)\b|Ants)",
    "My Third Grandma, Part IV -- Nandi": _MY_THIRD_GRANDMA_PAT + r"(?:(?:(?:Part|Parr)\s+)?(?:IV|4)\b|Nandi|\(?\s*(?:C|c)onclusion\s*\)?)",
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
                parts.append(r'f?' + _OCR_WORD_START_ALTS[two] + r'\s?')
                i += 2
                at_word_start = False
                continue
            if ch in _OCR_SINGLE_CHAR_ALTS:
                parts.append(r'f?' + _OCR_SINGLE_CHAR_ALTS[ch] + r'\s?')
            else:
                # Allow optional stray 'f' prefix from OCR artifacts
                parts.append(r'f?' + re.escape(ch) + r'\s?')
            at_word_start = False
        else:
            # Allow optional OCR space within words
            if ch.isalpha():
                parts.append(re.escape(ch) + r'\s?')
            else:
                parts.append(re.escape(ch))
            at_word_start = False

        i += 1

    return ''.join(parts)


def strip_running_noise(text: str) -> tuple[str, list[str]]:
    """Remove running headers/footers and page numbers.

    Uses inline regex patterns rather than line splitting, since the
    source text is often only 1-2 long lines.
    """
    noise = []
    cleaned = text

    # Page NNN references (with possible OCR junk around them)
    page_pat = re.compile(
        r'(?:^|\s)(?:Page\s+)?\d{1,3}(?:\s+RELIEF|\s*$)',
        re.IGNORECASE | re.MULTILINE
    )
    for m in page_pat.finditer(cleaned):
        noise.append(m.group().strip())
    cleaned = page_pat.sub(' ', cleaned)

    # RELIEF SOCIETY MAGAZINE running headers (month/year variants)
    header_pat = re.compile(
        r'RELIEF\s+SOCIETY\s+MAGAZINE'
        r'(?:[\s\u2014\u2013\-]*'
        r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|'
        r'JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)'
        r'[\s,]*(?:19\d{2})?)?',
        re.IGNORECASE
    )
    for m in header_pat.finditer(cleaned):
        noise.append(m.group().strip())
    cleaned = header_pat.sub(' ', cleaned)

    # Standalone page numbers at sentence boundaries
    standalone_num = re.compile(r'(?<=[.!?"\u201d])\s+\d{1,3}\s+(?=[A-Z])')
    cleaned = standalone_num.sub(' ', cleaned)

    # Short ALL-CAPS lines that are likely headers/footers
    lines = cleaned.split('\n')
    filtered = []
    for line in lines:
        stripped = line.strip()
        if stripped and len(stripped) < 100 and stripped.isupper():
            noise.append(stripped)
        else:
            filtered.append(line)
    cleaned = '\n'.join(filtered)

    # Collapse multiple whitespace (but preserve paragraph breaks)
    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

    return cleaned.strip(), noise


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

            # Skip past the copyright boilerplate paragraph that follows
            # "PUBLISHED MONTHLY..." — it ends near "unsolicited manuscripts"
            # and no real articles start within it.  This eliminates false
            # matches for short titles that happen to appear in the legalese.
            copyright_end = re.search(
                r'unsolicited\s+manuscripts', body, re.IGNORECASE
            )
            if copyright_end:
                body = body[copyright_end.end():]

            return front_matter, body

    if split_point == -1:
        raise Exception("Unable to find 'PUBLISHED MONTHLY BY THE GENERAL BOARD' and so couldn't split text.")


def _build_author_regex(author: str) -> re.Pattern:
    """Build an OCR-tolerant regex for an author name.

    Allows each name part to be slightly fuzzy (1-char OCR substitution)
    and permits flexible whitespace / punctuation between parts.
    """
    parts = re.split(r'[\s.]+', author.strip())
    pat_parts = []
    for part in parts:
        if not part:
            continue
        if len(part) <= 2:
            # Initials — just match the letter loosely
            pat_parts.append(re.escape(part[0]) + r'\.?\s*')
        else:
            # Allow first char to be OCR-substituted, rest literal but
            # with optional extra chars between
            escaped = re.escape(part)
            pat_parts.append(escaped)
    full = r'[\s,.]*'.join(pat_parts)
    return re.compile(full, re.IGNORECASE)


# Patterns that indicate a structural break (page boundary / section separator)
_STRUCTURAL_BREAK_RE = re.compile(
    r'(?:Page\s+\d+|'
    r'RELIEF\s+SOCIETY\s+MAGAZINE|'
    r'[_=\-]{5,}|'
    r'\f)',                       # form feed
    re.IGNORECASE
)

# Words that indicate we're mid-sentence (i.e. the "title" match is embedded
# in prose rather than being a real heading)
_MID_SENTENCE_WORDS = re.compile(
    r'\b(?:the|for|many|with|from|that|this|and|but|his|her|'
    r'was|were|been|have|had|into|also|such|very|some|most|'
    r'their|about|would|could|than|only|just|more|each|upon|'
    r'those|these|like|over|when|they|will|shall|being|made|'
    r'where|which|while|after|other|what|your|before)\s*$',
    re.IGNORECASE
)


# Lesson entry types that should only match within the Lesson Department section
_LESSON_TITLE_PREFIXES = {
    "Theology: ", "Visiting Teacher Messages: ", "Work Meeting: ",
    "Literature: ", "Social Science: ",
}

# Pattern to find the "LESSON DEPARTMENT" section header in the body
_LESSON_DEPT_RE = re.compile(
    r'LESS.?N\s+DEPARTMENT',
    re.IGNORECASE
)


def _find_lesson_dept_start(body: str) -> int:
    """Find the start of the Lesson Department section.

    Returns the character position of the first LESSON DEPARTMENT header,
    or 0 if not found (allowing full-body search as fallback).
    """
    m = _LESSON_DEPT_RE.search(body)
    return m.start() if m else 0


def _is_lesson_entry(entry: dict) -> bool:
    """Check if an entry is a lesson-type that should be constrained."""
    title = entry.get("title", "")
    return any(title.startswith(prefix) for prefix in _LESSON_TITLE_PREFIXES)


def _match_entries_with_strategy(body: str, entries: list[dict]) -> list[tuple[int, dict]]:
    """Match all entries in the body using scored candidate selection.

    For each TOC entry, find *all* regex matches (via finditer) then score
    each candidate based on contextual signals.  The highest-scoring
    candidate is selected.

    Lesson entries (Theology, VTM, Work Meeting, Literature, Social Science)
    are constrained to match only after the "LESSON DEPARTMENT" marker,
    preventing false matches on photo captions in Notes From the Field.
    """
    found = []

    # Find lesson department boundary — lessons must match after this point
    lesson_dept_start = _find_lesson_dept_start(body)

    # First pass: collect best position for each entry
    # We'll do two passes — first to get preliminary positions, then refine
    # using sequential-order information.

    # Preliminary: pick best candidate ignoring sequential ordering
    preliminary: list[tuple[int, dict]] = []

    for entry in entries:
        pattern = build_regex_for_title(entry["title"])
        is_lesson = _is_lesson_entry(entry)

        # For lessons, only search within the lesson department section
        search_start = lesson_dept_start if is_lesson else 0
        candidates = list(pattern.finditer(body, search_start))

        if not candidates:
            continue

        if len(candidates) == 1:
            preliminary.append((candidates[0].start(), entry))
            continue

        author = entry.get("author")
        author_re = _build_author_regex(author) if author else None

        best_pos = candidates[0].start()
        best_score = -9999

        for m in candidates:
            score = 0
            pos = m.start()

            # Signal: author name within 200 chars after the title match
            if author_re:
                after_title = body[m.end():m.end() + 200]
                if author_re.search(after_title):
                    score += 100
            else:
                # No-author entry without structural break gets a penalty
                before_text = body[max(0, pos - 80):pos]
                if not _STRUCTURAL_BREAK_RE.search(before_text):
                    score -= 30

            # Signal: structural break (page number, header, separator)
            # in the 80 chars preceding the match
            before_text = body[max(0, pos - 80):pos]
            if _STRUCTURAL_BREAK_RE.search(before_text):
                score += 30

            # Signal: preceded by sentence-ending punctuation
            before_trimmed = before_text.rstrip()
            if before_trimmed and before_trimmed[-1] in '.!?""\u201d\u2019':
                score += 20

            # Signal: preceded by a mid-sentence word (strong false-match indicator)
            if _MID_SENTENCE_WORDS.search(before_text):
                score -= 40

            if score > best_score:
                best_score = score
                best_pos = pos

        preliminary.append((best_pos, entry))

    # Second pass: re-score using sequential position information.
    # Sort preliminary by position to establish expected order.
    preliminary_sorted = sorted(preliminary, key=lambda x: x[0])
    pos_lookup = {id(e): p for p, e in preliminary_sorted}

    for entry in entries:
        pattern = build_regex_for_title(entry["title"])
        is_lesson = _is_lesson_entry(entry)
        search_start = lesson_dept_start if is_lesson else 0
        candidates = list(pattern.finditer(body, search_start))

        if not candidates or len(candidates) == 1:
            # Already handled correctly in preliminary pass
            continue

        author = entry.get("author")
        author_re = _build_author_regex(author) if author else None

        # Determine expected position from TOC-order neighbors
        toc_idx = entries.index(entry)
        prev_pos = None
        next_pos = None
        for pi in range(toc_idx - 1, -1, -1):
            for pp, pe in preliminary:
                if pe["title"] == entries[pi]["title"]:
                    prev_pos = pp
                    break
            if prev_pos is not None:
                break
        for ni in range(toc_idx + 1, len(entries)):
            for pp, pe in preliminary:
                if pe["title"] == entries[ni]["title"]:
                    next_pos = pp
                    break
            if next_pos is not None:
                break

        best_pos = candidates[0].start()
        best_score = -9999

        for m in candidates:
            score = 0
            pos = m.start()

            # Author proximity
            if author_re:
                after_title = body[m.end():m.end() + 200]
                if author_re.search(after_title):
                    score += 100
            else:
                before_text = body[max(0, pos - 80):pos]
                if not _STRUCTURAL_BREAK_RE.search(before_text):
                    score -= 30

            # Sequential ordering
            if prev_pos is not None:
                if pos > prev_pos:
                    score += 50
                else:
                    score -= 50
            if next_pos is not None:
                if pos < next_pos:
                    score += 50
                else:
                    score -= 50

            # Structural break
            before_text = body[max(0, pos - 80):pos]
            if _STRUCTURAL_BREAK_RE.search(before_text):
                score += 30

            # Sentence ending
            before_trimmed = before_text.rstrip()
            if before_trimmed and before_trimmed[-1] in '.!?""\u201d\u2019':
                score += 20

            # Mid-sentence word
            if _MID_SENTENCE_WORDS.search(before_text):
                score -= 40

            if score > best_score:
                best_score = score
                best_pos = pos

        # Update the preliminary entry
        for i, (p, e) in enumerate(preliminary):
            if e["title"] == entry["title"]:
                preliminary[i] = (best_pos, entry)
                break

    return preliminary


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


def _validate_and_repair_bounds(body: str, entries: list[dict],
                                bounds: list[tuple[int, int, dict]]) -> list[tuple[int, int, dict]]:
    """Verify each extracted chunk starts with its title; re-match failures.

    For each boundary, check that the title appears within the first 300
    chars of the extracted text.  Entries that fail are re-matched using
    the neighboring good boundaries as search-range anchors.
    """
    if not bounds:
        return bounds

    # Build a quick title checker for each entry
    def title_in_head(start: int, end: int, entry: dict) -> bool:
        head = body[start:min(start + 300, end)]
        pat = build_regex_for_title(entry["title"])
        return bool(pat.search(head))

    good = []
    bad_indices = []

    for i, (s, e, entry) in enumerate(bounds):
        if title_in_head(s, e, entry):
            good.append(i)
        else:
            bad_indices.append(i)

    if not bad_indices:
        return bounds

    # Re-match bad entries using neighboring good bounds as anchors
    new_bounds = list(bounds)

    for bi in bad_indices:
        s, e, entry = bounds[bi]
        pattern = build_regex_for_title(entry["title"])

        # Determine search range from neighbors
        search_start = 0
        search_end = len(body)

        # Find nearest good bound before this one
        for gi in reversed(good):
            if gi < bi:
                search_start = bounds[gi][0]
                break

        # Find nearest good bound after this one
        for gi in good:
            if gi > bi:
                search_end = bounds[gi][1]
                break

        # Search in the constrained range
        candidates = list(pattern.finditer(body, search_start, search_end))
        if not candidates:
            # Keep the original if we can't find anything better
            continue

        author = entry.get("author")
        author_re = _build_author_regex(author) if author else None

        best_match = None
        best_score = -9999

        for m in candidates:
            score = 0
            pos = m.start()

            if author_re:
                after_title = body[m.end():m.end() + 200]
                if author_re.search(after_title):
                    score += 100

            before_text = body[max(0, pos - 80):pos]
            if _STRUCTURAL_BREAK_RE.search(before_text):
                score += 30
            if _MID_SENTENCE_WORDS.search(before_text):
                score -= 40

            if score > best_score:
                best_score = score
                best_match = m

        if best_match and best_match.start() != s:
            new_bounds[bi] = (best_match.start(), e, entry)

    # Recompute end positions based on new starts
    new_bounds.sort(key=lambda x: x[0])
    result = []
    for i, (s, _e, entry) in enumerate(new_bounds):
        end = new_bounds[i + 1][0] if i + 1 < len(new_bounds) else len(body)
        result.append((s, end, entry))

    return result


# Recognizable section-start patterns that always indicate a new article/section.
# Used by the trim function to detect boundaries for non-lesson content too.
_SECTION_START_PATTERNS = [
    # "Notes From/To the Field" (common recurring section)
    re.compile(
        r'Notes?\s+(?:TO|FROM|(?:F|St)rom)\s+(?:THE|(?:Th|Sh|Ch)e)\s+(?:F|St)(?:i|e)?eld',
        re.IGNORECASE
    ),
    # "From Near and Far" (recurring section)
    re.compile(
        r'(?:F|St)rom\s+Near\s+(?:and|Gnd|And)\s+(?:F|S|t)ar',
        re.IGNORECASE
    ),
    # "Sixty Years Ago" (recurring section)
    re.compile(r'Sixty\s+Years\s+Ago', re.IGNORECASE),
    # "LESSON DEPARTMENT" header
    re.compile(r'LESS.?N\s+DEPARTMENT', re.IGNORECASE),
]

# Section-header patterns for detecting lesson boundaries within chunks.
# These match the OCR-garbled headers without requiring the specific lesson title.
_LESSON_SECTION_HEADERS = [
    # Theology
    re.compile(
        r'(?:Th|Sh|Ch)eology[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Visiting Teacher Messages
    re.compile(
        r'(?:V|U|\()?(?:i|s|u)?s?iting\s+'
        r'(?:(?:T|S)(?:e|l|a)(?:a|e|c)(?:ch|l!|h)?(?:er)?|Seal!?\s*He)\s+'
        r'(?:M|IT|T|m|He|HeIT)(?:l)?essages?',
        re.IGNORECASE
    ),
    # Work Meeting
    re.compile(
        r'(?:W|V)ork\s+(?:IT|T|M)?(?:l)?eeting[)\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Literature
    re.compile(
        r'(?:(?:Fo\s+i|S\s+i|O|Of?\s+i|of\s+i)?)?'
        r'[Ll]?iterature[\s\u2014\u2013\-:]+'
        r'(?:America|Come|Expand|Lesson)',
        re.IGNORECASE
    ),
    # Social Science
    re.compile(
        r'Social\s+Sc(?:i|t|l)ence[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
]


def _trim_boundary_bleed(body: str, entries: list[dict],
                         bounds: list[tuple[int, int, dict]]) -> list[tuple[int, int, dict]]:
    """Trim each chunk so it doesn't bleed into subsequent articles.

    When TOC entries B, C are unmatched, entry A's boundary extends to
    entry D's start, swallowing B and C's content.  This function searches
    for other entries' titles within each chunk and trims at the first
    one found.  It also detects author-bio separators, new title+author
    blocks, and lesson section headers as boundary signals.
    """
    if not bounds:
        return bounds

    matched_titles = {e["title"] for _, _, e in bounds}

    # Build list of ALL other entries with their regex patterns, tracking
    # whether each is matched or unmatched.  We check matched entries too
    # because a matched entry's title can appear at a secondary location
    # inside another article's chunk (the primary match is elsewhere).
    all_other_entries = []
    for entry in entries:
        pat = build_regex_for_title(entry["title"])
        author = entry.get("author")
        author_re = _build_author_regex(author) if author else None
        is_matched = entry["title"] in matched_titles
        all_other_entries.append((entry, pat, author_re, is_matched))

    # Author-bio separator pattern: <—>- or <->- or similar dash/arrow combos
    _BIO_SEPARATOR_RE = re.compile(
        r'<[\u2014\u2013\-]+>[\u2014\u2013\-]*',  # <—>- or <->- etc.
    )

    # Build a set of all author names from TOC entries for detection
    _all_author_names = set()
    _all_author_res = {}
    for e in entries:
        if e.get("author"):
            _all_author_names.add(e["author"])
            if e["author"] not in _all_author_res:
                _all_author_res[e["author"]] = _build_author_regex(e["author"])

    # Build SHORT title patterns (first 3 significant words) for partial
    # matching.  This catches cases where a running header or section label
    # uses an abbreviated form of the title.
    _short_title_pats = []
    for e in entries:
        title = e["title"]
        # Strip common prefixes (Theology:, Editorial:, etc.)
        for prefix in _LESSON_TITLE_PREFIXES:
            if title.startswith(prefix):
                title = title[len(prefix):]
                break
        if title.startswith("Editorial: "):
            title = title[len("Editorial: "):]
        # Take first 3 significant words (enough to be distinctive)
        words = title.split()
        if len(words) >= 3:
            short = ' '.join(words[:3])
            if len(short) >= 8:  # Skip very short titles
                short_pat = _build_regex_chars(short)
                _short_title_pats.append((
                    e,
                    re.compile(short_pat, re.IGNORECASE),
                    _build_author_regex(e["author"]) if e.get("author") else None,
                ))

    result = []
    for i, (start, end, entry) in enumerate(bounds):
        chunk = body[start:end]
        best_trim = len(chunk)  # default: no trim
        current_title = entry.get("title", "")

        # Skip the first 150 chars (the current entry's own title/header region)
        search_offset = min(150, len(chunk))

        # --- Strategy 1: search for OTHER TOC entries' titles ---
        # Checks both unmatched and matched entries (a matched entry's title
        # may appear at a secondary position within this chunk).
        for oe_entry, oe_pat, oe_author_re, oe_is_matched in all_other_entries:
            # Skip the current entry itself
            if oe_entry["title"] == current_title:
                continue

            for m in oe_pat.finditer(chunk, search_offset):
                pos = m.start()

                # Verify this is a real article start, not a passing mention
                score = 0
                if oe_author_re:
                    after = chunk[m.end():m.end() + 200]
                    if oe_author_re.search(after):
                        score += 100

                before = chunk[max(0, pos - 80):pos]
                if _STRUCTURAL_BREAK_RE.search(before):
                    score += 30
                if _MID_SENTENCE_WORDS.search(before):
                    score -= 40

                # For matched entries appearing in another chunk, require
                # slightly less confidence (they're known real entries)
                threshold = 20 if oe_is_matched else 30
                if score >= threshold and pos < best_trim:
                    best_trim = pos
                    break  # Take the first good match for this entry

        # --- Strategy 2a: detect common section-start patterns ---
        # These are recurring sections (Notes From the Field, From Near and
        # Far, etc.) that always indicate a new article is starting.
        for sec_re in _SECTION_START_PATTERNS:
            for m in sec_re.finditer(chunk, search_offset):
                pos = m.start()
                # Make sure the current entry's own title doesn't match
                # this pattern (e.g., "From Near and Far" entry itself)
                if pos < 20:
                    continue
                before = chunk[max(0, pos - 80):pos]
                if _MID_SENTENCE_WORDS.search(before):
                    continue
                if pos < best_trim:
                    best_trim = pos
                break

        # --- Strategy 2b: for lesson entries, detect section headers ---
        # If the current entry IS a lesson, look for the next lesson section
        # header within the chunk.  This catches cases where the specific
        # title is too OCR-mangled to match but the section header is clear.
        current_title = entry.get("title", "")
        if _is_lesson_entry(entry):
            # Determine which section header belongs to THIS entry so we
            # don't accidentally trim at our own header
            own_prefix = None
            for prefix in _LESSON_TITLE_PREFIXES:
                if current_title.startswith(prefix):
                    own_prefix = prefix.rstrip(": ")
                    break

            for header_re in _LESSON_SECTION_HEADERS:
                for m in header_re.finditer(chunk, search_offset):
                    # Skip if this matches our own section header
                    matched_text = m.group()
                    if own_prefix and own_prefix.lower().replace(" ", "") in \
                       matched_text.lower().replace(" ", ""):
                        continue

                    pos = m.start()
                    # Lesson headers in the Lesson Department always follow
                    # a page number or are at a structural boundary
                    before = chunk[max(0, pos - 80):pos]
                    if _MID_SENTENCE_WORDS.search(before):
                        continue  # embedded in prose, not a real header

                    if pos < best_trim:
                        best_trim = pos
                    break

        # --- Strategy 3: for poems, detect trailing non-poem content ---
        entry_type = entry.get("etype", "")
        if entry_type == "poem" and best_trim > 2000:
            # Look for structural breaks that indicate the poem ended.
            for m in _STRUCTURAL_BREAK_RE.finditer(chunk, min(200, len(chunk))):
                after_break = chunk[m.end():m.end() + 100].strip()
                if not after_break:
                    continue
                if after_break[0].isupper() and m.start() > 150:
                    if m.start() < best_trim:
                        best_trim = m.start()
                    break

            # Also look for ad/filler content that commonly follows poems
            # at the end of issue pages.
            ad_pat = re.compile(
                r'(?:BENEFICIAL\s+LIFE|Cable.Nelson|UTAH.IDAHO\s+SUGAR|'
                r'TOWARD\s+A\s+BETTER|Gentlemen:?\s+Enclosed|'
                r'Please\s+(?:send|tell)\s+me\s+more|'
                r'Salt\s+Lake\s+City,?\s+Utah\s+Please|'
                r'\bcheck\b.*\bmoney\s+order\b)',
                re.IGNORECASE
            )
            ad_match = ad_pat.search(chunk, min(200, len(chunk)))
            if ad_match and ad_match.start() < best_trim:
                # Back up to find a clean break point before the ad
                before_ad = chunk[:ad_match.start()].rstrip()
                # Find last sentence-ending punctuation before the ad
                last_sent = max(
                    before_ad.rfind('.'),
                    before_ad.rfind('!'),
                    before_ad.rfind('"'),
                    before_ad.rfind('\u201d'),
                )
                if last_sent > 150:
                    best_trim = last_sent + 1
                else:
                    best_trim = ad_match.start()

        # --- Strategy 4: detect author-bio separators ---
        # Patterns like <—>- followed by author bio text indicate end of
        # article.  After the bio, any new content is bleed.
        bio_separator_pos = None  # Track for Strategy 6
        for m in _BIO_SEPARATOR_RE.finditer(chunk, search_offset):
            sep_end = m.end()
            bio_separator_pos = m.start()
            bio_region = chunk[sep_end:sep_end + 1500]

            bio_end_pos = None

            # Find the LAST closing double-quote in the bio region.
            for q in ['\u201d', '"']:
                qi = bio_region.rfind(q, 0, 1200)
                if qi != -1 and qi > 50:
                    bio_end_pos = sep_end + qi + 1
                    break

            if bio_end_pos is None:
                bio_end_pos = min(sep_end + 800, len(chunk))

            if bio_end_pos < best_trim and bio_end_pos > search_offset:
                best_trim = bio_end_pos
            break

        # --- Strategy 5: detect other entries' author names in
        # non-prose positions ---
        # If an author name from a DIFFERENT TOC entry appears after
        # the current article's content in a structural position
        # (not embedded in prose), it signals bleed.
        current_author = entry.get("author", "")
        if best_trim == len(chunk) and len(chunk) > 400:
            late_offset = max(search_offset, len(chunk) * 2 // 5)
            for author_name, author_re in _all_author_res.items():
                if author_name == current_author:
                    continue
                for m in author_re.finditer(chunk, late_offset):
                    pos = m.start()
                    before = chunk[max(0, pos - 120):pos]
                    # Check that this isn't in prose — author names in
                    # structural positions are preceded by a title or
                    # whitespace, not mid-sentence words
                    if _MID_SENTENCE_WORDS.search(before):
                        continue
                    # Look backwards for a title-like string before
                    # this author name (within 120 chars)
                    # Find the start of this title+author block
                    title_start = before.rstrip()
                    # The title should be the last line/phrase before the
                    # author name — look for the last sentence break
                    last_break = max(
                        title_start.rfind('.'),
                        title_start.rfind('"'),
                        title_start.rfind('\u201d'),
                        title_start.rfind('!'),
                    )
                    if last_break != -1:
                        candidate_before = title_start[last_break + 1:].strip()
                    else:
                        candidate_before = title_start.strip()
                    # If there's a short capitalized phrase before the
                    # author name, this is likely a new article/poem title
                    if (candidate_before and
                            len(candidate_before) > 2 and
                            candidate_before[0].isupper()):
                        trim_at = max(0, pos - 120) + (
                            before.find(candidate_before) if candidate_before in before
                            else len(before) - len(candidate_before)
                        )
                        if trim_at > search_offset and trim_at < best_trim:
                            best_trim = trim_at
                            break
            # end author detection

        # --- Strategy 6: detect trailing poems/articles by title+author block ---
        # Poems often appear at the end of articles with format:
        #   Title  Author Name  poem text...
        # Detect these by looking for a short capitalized title followed by
        # a plausible author name (First [Middle] Last pattern) that is NOT
        # the current article's author.
        if len(chunk) > 500 and (bio_separator_pos is None or
                                  best_trim > len(chunk) * 9 // 10):
            _trailing_title_author = re.compile(
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,4}?)'  # title (non-greedy)
                r'\s+'
                r'([A-Z][a-z]+\s+'                     # first name
                r'(?:[A-Z]\.?\s+)?'                    # optional middle initial
                r'[A-Z][a-z]{2,})',                     # last name
                re.MULTILINE
            )
            # Only search the last 25% of effective content, skip bio regions
            effective_len = min(best_trim, len(chunk))
            late_offset = max(search_offset, effective_len * 3 // 4)
            if bio_separator_pos is not None:
                # Don't search within the bio — start after it
                late_offset = max(late_offset, best_trim)
            for m in _trailing_title_author.finditer(chunk, late_offset):
                candidate_title = m.group(1).strip()
                candidate_author = m.group(2).strip()
                # Skip if the "author" matches the current entry's author
                if current_author and current_author.lower() in candidate_author.lower():
                    continue
                if len(candidate_title) < 3:
                    continue
                before = chunk[max(0, m.start() - 80):m.start()]
                if _MID_SENTENCE_WORDS.search(before):
                    continue
                # Check that text AFTER the author name doesn't start
                # with a lowercase word (which would mean it's prose).
                after_author = chunk[m.end():m.end() + 50].lstrip()
                if after_author and after_author[0].islower():
                    continue
                # Reject matches preceded by biographical/prose context
                before_trimmed = before.rstrip()
                if before_trimmed:
                    last_ch = before_trimmed[-1]
                    # Reject if before ends in lowercase letter (mid-prose)
                    if last_ch.isalpha() and last_ch.islower():
                        continue
                    # Reject if preceded by biographical context words
                    bio_context = re.search(
                        r'\b(?:Dr|Mrs?|husband|wife|daughter|son|'
                        r'brother|sister|mother|father|President|'
                        r'Bishop|Elder|Professor|children|parents)'
                        r'[.\s]*$',
                        before_trimmed, re.IGNORECASE
                    )
                    if bio_context:
                        continue
                pos = m.start()
                if pos > search_offset and pos < best_trim:
                    best_trim = pos
                    break

        # --- Strategy 7: partial title matches with author confirmation ---
        # Search for the first few words of other TOC entry titles.
        # This catches running headers / abbreviated titles that appear
        # within a chunk (e.g. "YARN STITCHED BAG" for the full title
        # "Yarn Stitched Bag Made of Chair Webbing").
        if best_trim == len(chunk):
            late_offset = max(search_offset, len(chunk) // 2)
            for sp_entry, sp_pat, sp_author_re in _short_title_pats:
                if sp_entry["title"] == current_title:
                    continue
                for m in sp_pat.finditer(chunk, late_offset):
                    pos = m.start()
                    before = chunk[max(0, pos - 80):pos]
                    if _MID_SENTENCE_WORDS.search(before):
                        continue
                    # Require author name nearby for confirmation
                    score = 0
                    if sp_author_re:
                        after = chunk[m.end():m.end() + 300]
                        if sp_author_re.search(after):
                            score += 100
                    # Structural break before
                    if _STRUCTURAL_BREAK_RE.search(before):
                        score += 30
                    # If match is near the very end of the chunk (last 5%),
                    # it's almost certainly a boundary marker, not prose
                    near_end = (len(chunk) - pos) < max(100, len(chunk) // 20)
                    if near_end:
                        score += 30
                    if score >= 30 and pos < best_trim:
                        best_trim = pos
                        break

        new_end = start + best_trim
        # Don't trim to less than 50 chars — that would destroy the entry
        if new_end - start < 50:
            new_end = end

        result.append((start, new_end, entry))

    return result


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

    # Post-extraction validation: verify each chunk starts with its title.
    # Entries that fail get re-matched with tighter constraints using
    # neighboring good matches as anchors.
    bounds = _validate_and_repair_bounds(body, entries, bounds)

    # Trim boundary bleed: search for unmatched TOC entries within each
    # chunk and truncate before them.
    bounds = _trim_boundary_bleed(body, entries, bounds)

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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol47"
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
        description="Extract Relief Society Magazine Vol 47 into individual entries"
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

    # Clean output directory to remove stale files from prior runs
    vol47_out = OUTPUT_DIR / "vol47"
    if vol47_out.exists() and not args.dry_run:
        print(f"Cleaning output directory: {vol47_out}")
        shutil.rmtree(vol47_out)

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    volume_json = {}

    for (vol, issue_key), entries in VOL47_TOC.items():
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
                    # Use the same OCR-tolerant regex used for extraction
                    title_pat = build_regex_for_title(title)
                    match_data = entry_json.get("match")
                    if match_data is None:
                        continue
                    content = match_data["content"]
                    head = content[:300] if content else ""
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
