#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 49 (1962).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol49.py
    python extract_vol49.py --dry-run
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

VOL49_TOC = {
    ("Vol49", "No01_January_1962"): [
        {"title": "Keep the Commandments", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "A New Year's Greeting from the General Presidency", "author": "General Presidency", "etype": "article"},
        {"title": "New General Presidency of the Young Women's Mutual Improvement Association", "author": None, "etype": "article"},
        {"title": "Irene Cannon Lloyd Appointed to the General Board of Relief Society", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "Award Winners — Annual Relief Society Poem Contest", "author": None, "etype": "article"},
        {"title": "The Other Mother", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "Ultimatum", "author": "Bernice Burton Holmes", "etype": "poem"},
        {"title": "Recess — School for the Deaf", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Award Winners — Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Ten Dollars Will Buy Many Things", "author": "Mary E. Knowles", "etype": "fiction"},
        {"title": "Color Comes to Inside Pages of the Relief Society Magazine", "author": None, "etype": "article"},
        {"title": "The Priceless Gift of Humor", "author": "George P. Voss", "etype": "article"},
        {"title": "Sow the Field With Roses, Chapter 1", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Because of the Word, Chapter 6", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Multitude of the Promises", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Homemade Candy for Winter Evenings", "author": "Caroline Layton Naylor", "etype": "article"},
        {"title": "Braided Rugs of Unique Design", "author": None, "etype": "article"},
        {"title": "Housekeeper in a Hurry", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Theology: The Revelation to William W. Phelps", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: I Will Be Merciful Unto You", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Attitudes Make the Difference", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: Edgar Allan Poe — The Pathos of His Life and Poetry", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: How Women Share in the Blessings of the Priesthood", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Winter in the Wasatch Mountains", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Splendor Born", "author": "Viola Ashton Candland", "etype": "poem"},
        {"title": "Building a Life", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Song for a Spring Day", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "Sanctuary", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Muted Music", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Interlude", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Matins", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Elm Tree Who Grew", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Challenge", "author": "Florence S. Glines", "etype": "poem"},
        {"title": "Reflection", "author": "Linda Clarke", "etype": "poem"},
    ],

    ("Vol49", "No02_February_1962"): [
        {"title": "Fellowshipping Through Relief Society", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Funeral and Burial of President Brigham Young", "author": "Preston Nibley", "etype": "article"},
        {"title": "We Help Build a Church at Potomac", "author": "Violet M. Evans", "etype": "article"},
        {"title": "Splendor Before Dawn", "author": "Sara O. Moss", "etype": "fiction"},
        {"title": "The Turning Point", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "The Unfinished House", "author": "Maryhale Woolsey", "etype": "fiction"},
        {"title": "A Legacy of Lilacs", "author": "Pauline L. Jensen", "etype": "fiction"},
        {"title": "Sow the Field With Roses, Chapter 2", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Refinement of the Soul Through Tribulation", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Homemaking, Care, and Storage", "author": "Ruth P. Tippetts", "etype": "article"},
        {"title": "Make It Out of Imagination", "author": "Sylvia W. Dixon", "etype": "article"},
        {"title": "Roast Pork Griddle Cakes", "author": "Chet Switell", "etype": "article"},
        {"title": "Cauliflower Casserole", "author": "Edna Lind Cole", "etype": "article"},
        {"title": "Cafe Curtains Are Versatile", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Flavor Your Lunch", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Sweeter the Thoughts of Love Expressed", "author": "Mabel Law Atkinson", "etype": "article"},
        {"title": "Pearl Bunnel Newell Specializes in Knitting Sweaters and Making Hairpin Lace", "author": None, "etype": "article"},
        {"title": "Theology: Put the Kingdom of God First", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Continue in Steadfastness", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Hello and Goodbye", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: Edgar Allan Poe — Artist of Word and Sentence", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Fullness of Life and Exaltation", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Invoke the Miracles", "author": "Alice Morrey Bailey", "etype": "poem"},
    ],

    ("Vol49", "No03_March_1962"): [
        {"title": "Where Your Treasure Is", "author": "Sterling W. Sill", "etype": "article"},
        {"title": "Leadership by Example", "author": "Mildred B. Eyring", "etype": "article"},
        {"title": "The Young Child and His Books", "author": "May C. Hammond", "etype": "article"},
        {"title": "The Red Cross — A Universal Symbol", "author": "Fred A. Bantz", "etype": "article"},
        {"title": "Do You Want to Increase Relief Society Attendance?", "author": "Margaret Fitzpatrick", "etype": "article"},
        {"title": "The Lamplighters", "author": "Alice Gubler", "etype": "article"},
        {"title": "Cheshire Cat", "author": "Linda S. Fletcher", "etype": "fiction"},
        {"title": "Good Morning, Mrs. Romaine!", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "So Great the Calling", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "Sow the Field With Roses, Chapter 3", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: We Must Cherish One Another", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Christine H. Robinson Released From the General Board of Relief Society", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "About Grandmothers", "author": "Linnie F. Robinson", "etype": "article"},
        {"title": "Beverages Before a Dinner", "author": "Winnifred C. Jardine", "etype": "article"},
        {"title": "Bits of Odds and Ends", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Stuffed Toys Are Delightful", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Johanna Sofie Farstead Specializes in Hardanger Work", "author": None, "etype": "article"},
        {"title": "To Follow the Flowers", "author": "Dorothy J. Neilson", "etype": "article"},
        {"title": "Scars", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "A Song", "author": "Patricia Robinson King", "etype": "poem"},
        {"title": "Perennial", "author": "Lila L. Smith", "etype": "poem"},
        {"title": "Sonnet for a Somber Day", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "To a Yellow Crocus", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "His Art", "author": "Gladys Hesser Burnham", "etype": "poem"},
        {"title": "Renewal", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Another Spring", "author": "Annie Atkin Tanner", "etype": "poem"},
        {"title": "View From the Pass", "author": "Martha Tucker Fugate", "etype": "poem"},
        {"title": "Kindness", "author": "Viola Ashton Candland", "etype": "poem"},
        {"title": "Snowdrops", "author": "Ethel Jacobson", "etype": "poem"},
    ],

    ("Vol49", "No04_April_1962"): [
        {"title": "Welfare and the Relief Society", "author": "Howard W. Hunter", "etype": "article"},
        {"title": "She Knew the Prophet Joseph Smith — Part I — Emmeline B. Wells", "author": "Preston Nibley", "etype": "article"},
        {"title": "Thanks for the Magazine", "author": "Linnie F. Robinson", "etype": "article"},
        {"title": "Cancer Education, Research, and Service", "author": "Rutherford L. Ellis", "etype": "article"},
        {"title": "Portraits on a Mission", "author": "Agnes K. Morgan", "etype": "article"},
        {"title": "A Latter-day Saint Schoolteacher in Beaver, Alaska", "author": "Elizabeth P. Zabriskie", "etype": "article"},
        {"title": "The Mischief Makers", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Dovetail", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "A Name Before the Lord", "author": "Ellen Taylor Hazard", "etype": "fiction"},
        {"title": "Moving Faces", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "Sow the Field With Roses, Chapter 4", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: I Will Pour Out My Spirit", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Build Yourself a Box", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "A Compliment Cast on the Waters", "author": "Evelyn Dorio", "etype": "article"},
        {"title": "From My Window I Watch", "author": "Cleo Jones Johnson", "etype": "article"},
        {"title": "Candy for Your Easter Basket", "author": "Caroline L. Naylor", "etype": "article"},
        {"title": "Two Recipes for a Luncheon", "author": "Ruth L. Jones", "etype": "article"},
        {"title": "Recipes From a Pioneer Kitchen", "author": "Anne McCall", "etype": "article"},
        {"title": "The Little Silver Thimble", "author": "Sherry Crookston", "etype": "article"},
        {"title": "Anna Eckloff Makes Her Life Happy With Hobbies", "author": None, "etype": "article"},
        {"title": "Keep Your End of the Handle Up", "author": "Olive Sharp", "etype": "article"},
        {"title": "What Did You Say?", "author": "Maude Proctor", "etype": "article"},
        {"title": "Potted Plants Complete a Picture", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Savior", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Starless Interlude", "author": "Annie Atkin Tanner", "etype": "poem"},
        {"title": "Doing Good", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Point of View", "author": "Gladys Hesser Burnham", "etype": "poem"},
        {"title": "I Would Follow Thee", "author": "Mildred Wentworth", "etype": "poem"},
        {"title": "Fame's Prayer", "author": "Leora Larsen", "etype": "poem"},
        {"title": "Enough", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "No Half Loaf This", "author": "Virginia Newman", "etype": "poem"},
        {"title": "Note to Carvel", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "From My Window", "author": "Evalyn Sandberg", "etype": "poem"},
        {"title": "The Blossoming", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Return", "author": "Henrietta B. McNeely", "etype": "poem"},
        {"title": "Prelude to Easter", "author": "Linda Clarke", "etype": "poem"},
        {"title": "My Legacy", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Grandmothers Know", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Beauty", "author": "Ida Isaacson", "etype": "poem"},
    ],

    ("Vol49", "No05_May_1962"): [
        {"title": "Portrait of a Mother", "author": "William J. Critchlow, Jr.", "etype": "article"},
        {"title": "Hazel Sowards Cannon Appointed to the General Board of Relief Society", "author": "Luella F. Okeson", "etype": "article"},
        {"title": "Hazel Sperry Love Appointed to the General Board of Relief Society", "author": "Mary R. Young", "etype": "article"},
        {"title": "Contest Announcements — 1962 Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "She Knew the Prophet Joseph Smith — Part II — Mercy Fielding Thompson", "author": "Preston Nibley", "etype": "article"},
        {"title": "Magazine Honor Roll for 1961", "author": None, "etype": "article"},
        {"title": "Hand to the Plow, Part I", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "A Dog and His Maid", "author": "Shirley Sargent", "etype": "fiction"},
        {"title": "Little Teamstress", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Dare to Be Different", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Sow the Field With Roses, Chapter 5", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Cultural Values of Relief Society", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "My Husband's Box", "author": "Violet Nimmo", "etype": "article"},
        {"title": "How to Give a Magazine to Someone Who Is Ill", "author": "Evelyn Witter", "etype": "article"},
        {"title": "Home Canning", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "The Reward of a Thing Well Done", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "My Mother's Hands", "author": "Esther H. Lamb", "etype": "article"},
        {"title": "Patio Breakfasts for Summertime", "author": "Linnie F. Robinson", "etype": "article"},
        {"title": "Souffle Sandwich", "author": "Helene B. Ray", "etype": "article"},
        {"title": "Surfside Luncheon Theme", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Rhubarb Recipes", "author": "Grace V. Price", "etype": "article"},
        {"title": "A Song of the Sewing Machine", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Rachel Kirkham Wanlass Makes Unique Gifts for MIA Girls", "author": None, "etype": "article"},
        {"title": "Your Pre-School Playmates", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "On a May Morning", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Memories", "author": "Lela Foster Morris", "etype": "poem"},
        {"title": "The Constant Generation", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Sentinel", "author": "Zara Sabin", "etype": "poem"},
        {"title": "I Love You", "author": "Florence S. Glines", "etype": "poem"},
        {"title": "For Mother's Day", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Mine for Keeps", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Spring Cleaning", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "New House in Old Orchard", "author": "Maude Rubin", "etype": "poem"},
        {"title": "New Day", "author": "Leora Larsen", "etype": "poem"},
        {"title": "Party", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Wait for Me, Sun", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Seeking", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Each Day", "author": "Mae L. Curtis", "etype": "poem"},
    ],

    ("Vol49", "No06_June_1962"): [
        {"title": "A Tribute to Emma Ray McKay", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "The Blessings of Family Unity", "author": "Irene W. Buehner", "etype": "article"},
        {"title": "In Memoriam — Elder George Q. Morris", "author": None, "etype": "article"},
        {"title": "She Knew the Prophet Joseph Smith — Part III — Bathsheba W. Smith", "author": "Preston Nibley", "etype": "article"},
        {"title": "Annual Report for 1961", "author": "Hulda Parker", "etype": "article"},
        {"title": "Hand to the Plow, Part II", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "To You With Love", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "Sow the Field With Roses, Chapter 6", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 132d Annual Church Conference", "author": None, "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes for a Brunch", "author": "Linnie Fisher Robinson", "etype": "article"},
        {"title": "Toddler's Cover-Apron", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Alma Anderson — Specialist With Needle and Crochet Hook", "author": None, "etype": "article"},
        {"title": "Leftover Disguises", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Pie-Tin Therapy", "author": "Pauline L. Jensen", "etype": "article"},
        {"title": "Peace in a Troubled World", "author": "Ruth L. Jones", "etype": "article"},
        {"title": "Theology: The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Truths to Live By From The Doctrine and Covenants", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Latter-day Saint Home", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: America's Literature", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Divine Law and Church Government", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "This House Is Heritage", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "View From the Trees", "author": "Lucille R. Perry", "etype": "poem"},
        {"title": "I Know a Thing", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "June and the Rose", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Before the Word Goes Forth", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Bonus Prize", "author": "Viola Ashton Candland", "etype": "poem"},
        {"title": "Narrow Valley", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Morning Song", "author": "Leora Larsen", "etype": "poem"},
        {"title": "Memories of Home", "author": "Geneva H. Williams", "etype": "poem"},
        {"title": "Navigator", "author": "Rose Thomas Graham", "etype": "poem"},
        {"title": "Trees of Mystery", "author": "Lela Foster Morris", "etype": "poem"},
        {"title": "I Pity the Child", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol49", "No07_July_1962"): [
        {"title": "In a Time of Flood", "author": "Frances C. Yost", "etype": "article"},
        {"title": "She Knew the Prophet Joseph Smith — Part IV — Mary Alice Cannon Lambert", "author": "Preston Nibley", "etype": "article"},
        {"title": "Jesus and the Land He Loved", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "Out of the Wilderness, Chapter 1", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "Mama and the Heavenly Father", "author": "Alice Gubler", "etype": "fiction"},
        {"title": "One of the Pioneers", "author": "Adrian Hansen", "etype": "fiction"},
        {"title": "Hand to the Plow, Part III", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Sister to 231,174 Members", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "The Pea Patch", "author": "Doris Duncan", "etype": "article"},
        {"title": "Quick Recipes for Busy Days", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "A Stitch in Time", "author": "Adelle Ashby", "etype": "article"},
        {"title": "Helen H. Allen — Historian and Needlecraft Artist", "author": None, "etype": "article"},
        {"title": "Theology: The Center Place of Zion", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Men Should Be Anxiously Engaged in a Good Cause", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Latter-day Saint Home Is Well-Organized — Part I", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: Thoreau, Man in Nature", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Meaning of Divine Law", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Summer Interval", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Prayer for the Fourth Generation", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Summer Evening in the City", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "To the Sea Gull", "author": "Clara Laster", "etype": "poem"},
        {"title": "Commonplace Beauty", "author": "Susa Gould Walker", "etype": "poem"},
        {"title": "David on the Low Hills", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Beauty's Bright Persistence", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Beach Morning-Glories", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Pioneer Woman", "author": "Marjorie Reay", "etype": "poem"},
        {"title": "At Summer's Edge", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "The Apple Tree", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Child Swinging", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Mount Timpanogos", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Blessed Point of No Return", "author": "Iris W. Schow", "etype": "poem"},
    ],

    ("Vol49", "No08_August_1962"): [
        {"title": "She Knew the Prophet Joseph Smith — Part V — Margaret McIntire Burgess", "author": "Preston Nibley", "etype": "article"},
        {"title": "My Shadow", "author": "Celia Larsen Luce", "etype": "article"},
        {"title": "Sunday Morning on Temple Hill", "author": "Mabel Luke Anderson", "etype": "article"},
        {"title": "Jerusalem, City of Peace", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "Company Best", "author": "Lael Jensen Littke", "etype": "fiction"},
        {"title": "Out of the Wilderness, Chapter 2", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "Hand to the Plow, Part IV", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: In the Family There Is Strength", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Vacation by the Day", "author": "Betty G. Spencer", "etype": "article"},
        {"title": "My Family Likes All Kinds of Fruit — Just So They Are Peaches", "author": "Vilate R. McAllister", "etype": "article"},
        {"title": "Thou Shalt Not Be Proud", "author": "Martha Tucker Fugate", "etype": "article"},
        {"title": "Family Desserts", "author": "Mabel Harmer", "etype": "article"},
        {"title": "Make a Drip-Dry Apron", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Clara Partridge Stevens — Artist With Needle and Brush", "author": None, "etype": "article"},
        {"title": "Theology: Engaged in a Good Cause", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: He Who Doeth the Works of Righteousness", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Latter-day Saint Home Is Well Organized — Part II", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: Henry David Thoreau — Individualist", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Divine Law and Human Welfare", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "The Little Things", "author": "Dorothy J. Roberts", "etype": "poem"},
    ],

    ("Vol49", "No09_September_1962"): [
        {"title": "Birthday Congratulations to President David O. McKay", "author": None, "etype": "article"},
        {"title": "Our Redeemer Lives", "author": "David O. McKay", "etype": "article"},
        {"title": "Portrait of Freedom", "author": "Alberta Huish Christensen", "etype": "article"},
        {"title": "Please Call Me Timmy", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "Relief Society — A Must", "author": "Jeannene J. Nelson", "etype": "fiction"},
        {"title": "Out of the Wilderness, Chapter 3", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "Hand to the Plow, Part V", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Upon... The Parents", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Irene B. Woodford Released from General Board", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Magic of Indoor Gardening", "author": "Maude W. Howard", "etype": "article"},
        {"title": "Baking Day at Home", "author": "Linnie Fisher Robinson", "etype": "article"},
        {"title": "It Is Never Too Early For Making Gifts", "author": "Clara Laster", "etype": "article"},
        {"title": "Mae Martindale Johnson — Organist and Teacher of Music", "author": None, "etype": "article"},
        {"title": "Theology: The Day of Rest and Devotions", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Be of Good Cheer", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Latter-day Saint Home Is Well Organized (Part III)", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: The Alcott Family", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Before the Falling Leaf", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol49", "No10_October_1962"): [
        {"title": "Gospel Restoration — Part of the Eternal Plan", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Patterns of Family Life and Organization", "author": "Lucile Dimond Smith", "etype": "article"},
        {"title": "A Permanent Home", "author": "Norma Dee Ryan", "etype": "article"},
        {"title": "Hands Across a Quilt", "author": "Aleine M. Young", "etype": "article"},
        {"title": "The Spirit of Thanksgiving", "author": "Helen H. Trutton", "etype": "fiction"},
        {"title": "Out of the Wilderness, Chapter 4", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "Hand to the Plow, Part VI", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Wages of Indulgence", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "The Joy of Working", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Bits for Baby", "author": "May Walkenhorst", "etype": "article"},
        {"title": "Recipes From Calgary Stake", "author": "Virginia N. Myers", "etype": "article"},
        {"title": "Two Handy Gifts for Christmas", "author": "Adelle Ashby", "etype": "article"},
        {"title": "Dorthea Strom Knits Sweaters for Missionaries", "author": None, "etype": "article"},
        {"title": "Theology: Rewards of Keeping the Commandments", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Thou Shalt Not Idle Away Thy Time", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Latter-day Saint Home Is Well Organized — Part IV", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: Oliver Wendell Holmes, Amiable Amateur", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Divine Law and Human Welfare (Continued)", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "October", "author": "Dorothy J. Roberts", "etype": "poem"},
    ],

    ("Vol49", "No11_November_1962"): [
        {"title": "Search the Scriptures; for in Them Ye Think Ye Have Eternal Life", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Relief Society — Builder of Testimonies", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Union of Feeling", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "Fawn Hansen Sharp Appointed to the General Board of Relief Society", "author": "Irene C. Lloyd", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "The Little Blue Bag", "author": "Helen C. Warr", "etype": "fiction"},
        {"title": "Out of the Wilderness, Chapter 5", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Relief Society Gives Thanks for New Members", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "In Memoriam — Fern Tanner Lee", "author": None, "etype": "article"},
        {"title": "In Memoriam — Amy Whipple Evans", "author": None, "etype": "article"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Beauty Again", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Menu for Thanksgiving Dinner", "author": "Mary J. Wilson", "etype": "article"},
        {"title": "Cut and Paste Christmas Cards", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Elizabeth Welker Collects Samples of Antique and Modern Lace", "author": None, "etype": "article"},
        {"title": "Theology: Talents and Testimonies", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Pray Always That You Enter Not Into Temptation", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Latter-day Saint Home Is Clean (Part I)", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: The Literary Lincoln", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Divine Law and Priesthood", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "The Mendicant Hour", "author": "Lucille R. Perry", "etype": "poem"},
    ],

    ("Vol49", "No12_December_1962"): [
        {"title": "Parental Responsibility", "author": "David O. McKay", "etype": "article"},
        {"title": "Nathan Eldon Tanner Appointed to the Council of the Twelve", "author": None, "etype": "article"},
        {"title": "Bernard P. Brockbank Appointed Assistant to the Council of the Twelve", "author": None, "etype": "article"},
        {"title": "The Relief Society Annual General Conference", "author": "Hulda Parker", "etype": "article"},
        {"title": "The Breath of Life", "author": "James E. Perkins", "etype": "article"},
        {"title": "Christmas With Auntie", "author": "Helen H. Trutton", "etype": "fiction"},
        {"title": "One Little Christmas Gift", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "Out of the Wilderness, Chapter 6", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorials: The 132d Semi-Annual Conference", "author": None, "etype": "editorial"},
        {"title": "Editorial: Christmas in the Home", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "A Christmas Eve Family Party", "author": "Lorraine Hyde Clawson", "etype": "article"},
        {"title": "Handiwork for Christmas", "author": "Adele Williams Worsley", "etype": "article"},
        {"title": "Easy Christmas Cookies", "author": "Myrtle E. Henderson", "etype": "article"},
        {"title": "Cora E. Cook — Specialist in Household Handicraft", "author": None, "etype": "article"},
        {"title": "Theology: The Waters and the Land", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: He Who Has Repented of His Sins", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Latter-day Saint Home Is Clean (Part II)", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: Herman Melville", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Application of the Law in the Dispensations of Man", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "When Day Is Done", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "What Is Christmas", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "The Pheasant", "author": "Gladys Hesser Burnham", "etype": "poem"},
        {"title": "The Street You'd Love to Live On", "author": "Helen F. Parker", "etype": "poem"},
        {"title": "Words After Snow", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Whistling Carols", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Room With the Red Woodbox", "author": "Maude Rubin", "etype": "poem"},
        {"title": "The Fog", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Seagulls", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Where Are the Nine?", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Plea", "author": "Betty G. Spencer", "etype": "poem"},
        {"title": "Thoughts", "author": "Catherine B. Bowles", "etype": "poem"},
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
    year = 1962

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol49_{key}.txt"
    ISSUE_FILES[("Vol49", key)] = (fname, month)


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
    # "F" may OCR as "St" or "S" (e.g. "From" -> "Strom" or "Srom")
    "F": r"(?:F|St|S)",
    "f": r"(?:f|st|s)",
    # "M" may OCR as "IT", "ITl", "Tl" (e.g. "Made" -> "ITlade")
    "M": r"(?:M|IT(?:l)?|Tl)",
    "m": r"(?:m|it(?:l)?|tl)",
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers for Vol49.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns

# Sow the Field With Roses: Chapters 1-6 (Jan-Jun 1962)
_SOW_FIELD_PAT = (
    r"(?:S|J)ow\s+(?:th|sh|ch)e\s+(?:F|St|S)ield\s+(?:W|V)ith\s+Roses"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Because of the Word: Chapter 6/Conclusion (Jan 1962, continued from vol49)
_BECAUSE_OF_WORD_PAT = (
    r"Because\s+of\s+(?:th|sh|ch)e\s+Word"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Out of the Wilderness: Chapters 1-6 (Jul-Dec 1962)
_OUT_OF_WILDERNESS_PAT = (
    r"Out\s+of\s+(?:th|sh|ch)e\s+Wilderness"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Hand to the Plow: Parts I-VI (May-Oct 1962)
_HAND_TO_PLOW_PAT = (
    r"Hand\s+to\s+(?:th|sh|ch)e\s+Plow"
    r".{0,80}?"  # skip author/subtitle
    r"(?:(?:Part|PART)\s+)?"
)

# Editorial header prefix (matches OCR variants of "EDITORIAL")
_EDITORIAL_HDR = (
    r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)"
    r"[\s\S]{0,120}?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—The Doctrine and Covenants Lesson NN—SUBTITLE
    # or: THEOLOGY e The Doctrine and Covenants Lesson NN—SUBTITLE
    # TOC has: Theology: SUBTITLE
    "Theology: ": (
        r"(?:Th|Sh|Ch|THEOL)eology"
        r"[\s\-\u2014\u2013:e¢]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion|esson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?\s*(?:The\s+|A\s+)?)?"
    ),
    # Body has: VISITING TEACHER MESSAGES Truths to Live By ... Message NN—SUBTITLE
    # TOC has: Visiting Teacher Messages: SUBTITLE
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?|Seal!?\s*He)\s*"
        r"(?:M|N|IT|T|m|He|HeIT)\s*(?:l)?essages?"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Message|Lesson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|\.…]?\s*(?:The\s+|A\s+)?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+(?:N|Vl)?ear\s+(?:and|Gnd|And|and)\s+(?:F|S|St|t)ar[r]?"
    ),
    # Body has: EDITORIAL ... VOL. 49 MONTH 1962 NO. N <actual title>
    # TOC has: Editorial: <actual title>
    "Editorial: ": (
        r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Body has: LITERATURE e America's Literature ... Lesson NN—SUBTITLE
    # TOC has: Literature: SUBTITLE
    "Literature: ": (
        r"(?:(?:Fo\s+i|S\s+i|O|Of?\s+i|of\s+i)?)?"
        r"[Ll]?iterature"
        r"[\s\-\u2014\u2013:e@]*"
        r"(?:America'?s?\s+Literature)?"
        r"[\s\S]{0,150}?"
        r"(?:(?:Lesson|Preview)\s+\d*)?"
        r"[\s\-\u2014\u2013]*"
    ),
    # Body has: SOCIAL SCIENCE Divine Law and Church Government ... Lesson N—SUBTITLE
    # TOC has: Social Science: SUBTITLE
    "Social Science: ": (
        r"(?:Social\s+(?:Sc(?:i|t|l)ence|eicHiee|Stustiee|SCIENCE|ae|[A-Za-z]e))"
        r"[\s\-\u2014\u2013:@e]*"
        r"(?:Divine\s+Law\s+and\s+Church\s+Government)?"
        r"[\s\S]{0,250}?"
        r"(?:(?:Lesson|Preview)\s+\d+)"
        r"[\s\-\u2014\u2013]*"
        r"[\"\u201c\u2018\u2019\u0027]?"
    ),
    # Body has: WORK MEETING The Latter-day Saint Home ... Discussion N—SUBTITLE
    # TOC has: Work Meeting: SUBTITLE
    "Work Meeting: ": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?(?:ee|ec)?ting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St|S)rom\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St|S)(?:i|e)?eld"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St|S)(?:i|e)?eld"
        r":?\s*"
    ),
    # "She Knew the Prophet Joseph Smith" multi-part series
    "She Knew the Prophet Joseph Smith — Part I — Emmeline B. Wells": (
        r"(?:Sh|Th|Ch)e\s+Knew\s+(?:th|sh|ch)e\s+Prophet"
        r"[\s\S]{0,100}?"
        r"(?:Part\s+I\b|Emmeline)"
    ),
    "She Knew the Prophet Joseph Smith — Part II — Mercy Fielding Thompson": (
        r"(?:Sh|Th|Ch)e\s+Knew\s+(?:th|sh|ch)e\s+Prophet"
        r"[\s\S]{0,100}?"
        r"(?:Part\s+II\b|Mercy)"
    ),
    "She Knew the Prophet Joseph Smith — Part III — Bathsheba W. Smith": (
        r"(?:Sh|Th|Ch)e\s+Knew\s+(?:th|sh|ch)e\s+Prophet"
        r"[\s\S]{0,100}?"
        r"(?:Part\s+III\b|Bathsheba)"
    ),
    "She Knew the Prophet Joseph Smith — Part IV — Mary Alice Cannon Lambert": (
        r"(?:Sh|Th|Ch)e\s+Knew\s+(?:th|sh|ch)e\s+Prophet"
        r"[\s\S]{0,100}?"
        r"(?:Part\s+IV\b|Mary\s+Alice)"
    ),
    "She Knew the Prophet Joseph Smith — Part V — Margaret McIntire Burgess": (
        r"(?:Sh|Th|Ch)e\s+Knew\s+(?:th|sh|ch)e\s+Prophet"
        r"[\s\S]{0,100}?"
        r"(?:Part\s+V\b|Margaret\s+McIntire)"
    ),
    # Prize poems: match actual poem headers, not the award announcement
    "The Other Mother": (
        r"(?:First|1st)\s+Prize\s+(?:Poem|Winn)"
        r"[\s\S]{0,50}?"
        r"(?:Th|Sh|Ch)e\s+Other\s+(?:M|IT|Tl)other"
    ),
    "Ultimatum": (
        r"(?:Second|2nd)\s+Prize\s+Poem"
        r"[\s\S]{0,50}?"
        r"Ultimatum"
    ),
    "Recess — School for the Deaf": (
        r"(?:Third|3rd)\s+Prize\s+Poem"
        r"[\s\S]{0,50}?"
        r"Recess"
    ),
    # First-prize-winning story: match the story heading, not the award announcement
    "Ten Dollars Will Buy Many Things": (
        r"(?:(?:F|St|S)irst\s+)?(?:Prize|Winn)"
        r"[\s\-\u2014\u2013]*(?:ing)?\s*(?:S|J)(?:\s*t)?ory"
        r"[\s\S]{0,120}?"
        r"(?:T|S|J|\()en\s+Dollars\s+Will\s+Buy"
    ),
    # Titles where leading article is dropped in body text
    "A Permanent Home": r"Permanent\s+Home",
    # OCR drops "The" and garbles punctuation: 'Little "Blue Bag'
    "The Little Blue Bag": r"(?:(?:Th|Sh|Ch)e\s+)?Little\s+[\"\u201c\u2018]?Blue\s+Bag",
    # Work Meeting lessons with Part numbers — body uses either "— Part N" or "(Part N)"
    "Work Meeting: The Latter-day Saint Home Is Well-Organized — Part I": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?(?:ee|ec)?ting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
        r"[\s\S]{0,80}?(?:Th|Sh|Ch)e\s+Latter[\s\-\u2014\u2013]*day\s+Saint\s+Home\s+Is\s+Well[\s\-\u2014\u2013]*Organized"
        r"\s*[\(\u2014\u2013\-]+\s*Part\s+(?:I\b|1\b)"
    ),
    "Work Meeting: The Latter-day Saint Home Is Well Organized — Part II": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?(?:ee|ec)?ting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
        r"[\s\S]{0,80}?(?:Th|Sh|Ch)e\s+Latter[\s\-\u2014\u2013]*day\s+Saint\s+Home\s+Is\s+Well[\s\-]*Organized"
        r"\s*[\(\u2014\u2013\-]+\s*Part\s+(?:II\b|2\b)"
    ),
    "Work Meeting: The Latter-day Saint Home Is Well Organized (Part III)": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?(?:ee|ec)?ting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
        r"[\s\S]{0,80}?(?:Th|Sh|Ch)e\s+Latter[\s\-\u2014\u2013]*day\s+Saint\s+Home\s+Is\s+Well[\s\-]*Organized"
        r"\s*[\(\u2014\u2013\-]+\s*Part\s+(?:III\b|3\b)"
    ),
    "Work Meeting: The Latter-day Saint Home Is Well Organized — Part IV": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?(?:ee|ec)?ting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
        r"[\s\S]{0,80}?(?:Th|Sh|Ch)e\s+Latter[\s\-\u2014\u2013]*day\s+Saint\s+Home\s+Is\s+Well[\s\-]*Organized"
        r"\s*[\(\u2014\u2013\-]+\s*Part\s+(?:IV\b|4\b)"
    ),
    "Work Meeting: The Latter-day Saint Home Is Clean (Part I)": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?(?:ee|ec)?ting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
        r"[\s\S]{0,80}?(?:Th|Sh|Ch)e\s+Latter[\s\-\u2014\u2013]*day\s+Saint\s+Home\s+Is\s+Clean"
        r"\s*[\(\u2014\u2013\-]+\s*Part\s+(?:I\b|1\b)"
    ),
    "Work Meeting: The Latter-day Saint Home Is Clean (Part II)": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?(?:ee|ec)?ting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
        r"[\s\S]{0,80}?(?:Th|Sh|Ch)e\s+Latter[\s\-\u2014\u2013]*day\s+Saint\s+Home\s+Is\s+Clean"
        r"\s*[\(\u2014\u2013\-]+\s*Part\s+(?:II\b|2\b)"
    ),
    # Serial fiction: Sow the Field With Roses (Chapters 1-6, Jan-Jun)
    "Sow the Field With Roses, Chapter 1": _SOW_FIELD_PAT + r"(?:1|I)\b",
    "Sow the Field With Roses, Chapter 2": _SOW_FIELD_PAT + r"(?:2|II)\b",
    "Sow the Field With Roses, Chapter 3": _SOW_FIELD_PAT + r"(?:3|III)\b",
    "Sow the Field With Roses, Chapter 4": _SOW_FIELD_PAT + r"(?:4|IV)\b",
    "Sow the Field With Roses, Chapter 5": _SOW_FIELD_PAT + r"(?:5|V)\b",
    "Sow the Field With Roses, Chapter 6": _SOW_FIELD_PAT + r"(?:(?:6|VI)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: Because of the Word, Chapter 6/Conclusion (Jan only)
    "Because of the Word, Chapter 6": _BECAUSE_OF_WORD_PAT + r"(?:(?:6|VI)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: Out of the Wilderness (Chapters 1-6, Jul-Dec)
    "Out of the Wilderness, Chapter 1": _OUT_OF_WILDERNESS_PAT + r"(?:1|I)\b",
    "Out of the Wilderness, Chapter 2": _OUT_OF_WILDERNESS_PAT + r"(?:2|II)\b",
    "Out of the Wilderness, Chapter 3": _OUT_OF_WILDERNESS_PAT + r"(?:3|III)\b",
    "Out of the Wilderness, Chapter 4": _OUT_OF_WILDERNESS_PAT + r"(?:4|IV)\b",
    "Out of the Wilderness, Chapter 5": _OUT_OF_WILDERNESS_PAT + r"(?:5|V)\b",
    "Out of the Wilderness, Chapter 6": _OUT_OF_WILDERNESS_PAT + r"(?:(?:6|VI)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: Hand to the Plow (Parts I-VI, May-Oct)
    "Hand to the Plow, Part I": _HAND_TO_PLOW_PAT + r"(?:I\b|1\b)",
    "Hand to the Plow, Part II": _HAND_TO_PLOW_PAT + r"(?:II\b|2\b)",
    "Hand to the Plow, Part III": _HAND_TO_PLOW_PAT + r"(?:III\b|3\b)",
    "Hand to the Plow, Part IV": _HAND_TO_PLOW_PAT + r"(?:IV\b|4\b)",
    "Hand to the Plow, Part V": _HAND_TO_PLOW_PAT + r"(?:V\b|5\b)",
    "Hand to the Plow, Part VI": (
        r"(?:"
        + _HAND_TO_PLOW_PAT + r"(?:(?:VI\b|6\b)|\(?\s*(?:C|c)onclusion\s*\)?)"
        + r"|"
        # Oct 1962: body has "Part VI — 'Bid Them Farewell' (Conclusion)"
        # without the series title "Hand to the Plow"
        + r"Part\s+VI\s*[\u2014\u2013\-]+\s*[\"\u201c\u2018]?Bid\s+(?:Th|Sh|Ch)em\s+Farewell"
        + r")"
    ),
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
            # Include parens since OCR sometimes renders "(Part N)" instead of "— Part N"
            # Use \s* separately so it doesn't compete with the space handler
            parts.append(r'[\-\u2014\u2013()]*\s*')
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
        elif ch == '.':
            # Dots often vanish or become other chars in OCR
            parts.append(r'.?')
        elif ch == ' ':
            # Allow optional comma/semicolon before whitespace and optional
            # stray opening paren or quote (OCR artifact)
            parts.append(r'[,;]?\s+[(\"\u201c\u2018\u0027]?')
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

    # RELIEF SOCIETY MAGAZINE running headers (month/year variants)
    # MUST run before page_pat to avoid orphaning "SOCIETY MAGAZINE—..."
    header_pat = re.compile(
        r'(?:RELIEF\s+)?SOCIETY\s+MAGAZINE'
        r'(?:[\s\u2014\u2013\-]*'
        r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|'
        r'JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)'
        r'[\s,]*(?:19\d{2})?)?',
        re.IGNORECASE
    )
    for m in header_pat.finditer(cleaned):
        noise.append(m.group().strip())
    cleaned = header_pat.sub(' ', cleaned)

    # Page NNN references (with possible OCR junk around them)
    page_pat = re.compile(
        r'(?:^|\s)(?:Page\s+)?\d{1,3}(?:\s+RELIEF|\s*$)',
        re.IGNORECASE | re.MULTILINE
    )
    for m in page_pat.finditer(cleaned):
        noise.append(m.group().strip())
    cleaned = page_pat.sub(' ', cleaned)

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


def find_ads_section(body: str) -> tuple[str, int]:
    """Discover ads text and return (ads_text, start_position).

    Only searches in the back portion of the body to avoid false positives
    from phrases like "for sale" appearing in fiction narrative text.
    Returns ("", -1) if no ads section is found.
    """
    # Strong markers (unambiguous ad section headers) — search last 40%
    strong_markers = [r"ADVERTISING", r"ADVERTISEMENTS"]
    search_start_strong = len(body) * 3 // 5

    for marker in strong_markers:
        match = re.search(marker, body[search_start_strong:], re.IGNORECASE)
        if match:
            pos = search_start_strong + match.start()
            return body[pos:], pos

    # Known ad brand names — search last 15% only (very end of issues)
    brand_markers = [
        r"DAYNES\s+MUSIC\s+COMPANY",
        r"ESTHER\s+JAMES\s+TOURS",
        r"BENEFICIAL\s+LIFE\s+INSURANCE",
        r"Cable[\.\-]?Nelson",
    ]
    search_start_brands = len(body) * 85 // 100

    for marker in brand_markers:
        match = re.search(marker, body[search_start_brands:], re.IGNORECASE)
        if match:
            pos = search_start_brands + match.start()
            return body[pos:], pos

    # Weaker markers — require structural position, search last 20%
    weak_markers = [r"FOR SALE", r"BUSINESS"]
    search_start_weak = len(body) * 4 // 5

    for marker in weak_markers:
        for m in re.finditer(marker, body[search_start_weak:], re.IGNORECASE):
            pos = search_start_weak + m.start()
            before = body[max(0, pos - 80):pos]
            # Only accept if NOT preceded by mid-sentence words (i.e. prose)
            if _MID_SENTENCE_WORDS.search(before):
                continue
            return body[pos:], pos

    return "", -1


def split_front_matter(text: str) -> tuple[str, str]:
    """Split text into front matter (TOC, etc.) and body."""
    toc_end_markers = [
        "PUBLISHED MONTHLY BY THE GENERAL BOARD",
        "ISHED MONTHLY BY THE GENERAL BOARD",
        "MONTHLY BY THE GENERAL BOARD",
        "THE GENERAL BOARD OF RELIEF SOCIETY",
    ]

    # Only search in the first 15000 chars to avoid matching mid-article
    # references (e.g. December 1962 mentions "the General Board of Relief
    # Society" at position 36645 inside the General Conference report).
    search_region = text[:15000]

    split_point = -1
    for marker in toc_end_markers:
        match = re.search(marker, search_region, re.IGNORECASE)
        if match:
            split_point = match.start()
            front_matter = text[:split_point]
            body = text[split_point:]

            # Skip past the copyright boilerplate paragraph that follows
            # "PUBLISHED MONTHLY..." — it ends near "unsolicited manuscripts"
            # and no real articles start within it.  This eliminates false
            # matches for short titles that happen to appear in the legalese.
            copyright_end = re.search(
                r'unsolicited\s+manuscripts', body[:3000], re.IGNORECASE
            )
            if copyright_end:
                body = body[copyright_end.end():]

            return front_matter, body

    # Fallback: look for the end of the TOC poetry index (Dec 1962 has no
    # standard marker).  The poetry listing ends with "NNN.\n\n" followed
    # by article content.  Also try "General Manager" + TOC block end.
    poetry_end = re.search(r'\d{3}\.\n\n', search_region)
    if poetry_end:
        split_point = poetry_end.end()
        return text[:split_point], text[split_point:]

    # Last resort: split after the Content/TOC header block
    content_hdr = re.search(r'\nContent\n', search_region)
    if content_hdr:
        # Find the end of the TOC (double newline after last entry)
        toc_end = re.search(r'\n\n', text[content_hdr.end():content_hdr.end() + 8000])
        if toc_end:
            split_point = content_hdr.end() + toc_end.end()
            return text[:split_point], text[split_point:]

    raise Exception("Unable to find front matter boundary — no standard markers or fallbacks matched.")


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
    r'SOCIETY\s+MAGAZINE[\s\u2014\u2013\-]+|'
    r'[_=\-\u2014\u2013]{5,}|'
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

# Pattern to find the "LESSON DEPARTMENT" section header in the body.
# The OCR garbles this header in many ways:
#   "LESSON DEPARTMENT", "LESS N DEPARTMENT", "LESSON DE N DEPARTMENT",
#   "LESSON DEP. DEPARTMENT", "Lnsson DEPARTMENT", "SON DEPARTMENT"
# The most reliable anchor is DEPARTMENT immediately followed by Sheology/Theology.
_LESSON_DEPT_RE = re.compile(
    r'(?:LESS.{0,10}N\s+(?:DE.{0,6}\s+)?)?DEPARTMENT[\s|:]+(?:Sh|Th|Ch)eology',
    re.IGNORECASE
)

# Broader fallback if the Sheology-anchored pattern doesn't match
_LESSON_DEPT_FALLBACK_RE = re.compile(
    r'LESS.{0,3}N\s+DEPARTMENT',
    re.IGNORECASE
)


def _find_lesson_dept_start(body: str) -> int:
    """Find the start of the Lesson Department section.

    Returns the character position of the first LESSON DEPARTMENT header,
    or a heuristic fallback.  Lessons are always in the back half of
    each issue, so if no marker is found (or the marker is suspiciously
    deep into the body) we fall back to the 40% mark, which still
    prevents false matches on "theology class leader"-type text in the
    Notes From the Field section earlier in the issue.
    """
    midpoint = len(body) * 2 // 5  # 40% heuristic fallback

    m = _LESSON_DEPT_RE.search(body)
    if m:
        return m.start()

    m = _LESSON_DEPT_FALLBACK_RE.search(body)
    if m:
        # Running headers like "LESSON DEPARTMENT" may appear mid-lesson
        # rather than at the section start. Use the earlier of the marker
        # position and the 40% fallback to avoid cutting off early lessons.
        return min(m.start(), midpoint)

    return midpoint


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

            # Signal: match starts with lowercase (headings are capitalized)
            match_text = m.group()
            if match_text and match_text[0].islower():
                score -= 50

            if score > best_score:
                best_score = score
                best_pos = pos

        # Reject single candidates that score poorly (likely false matches
        # embedded in narrative prose rather than real article headings).
        # For entries with authors, require author found nearby OR structural
        # break before the match. For entries without authors, reject negatives.
        # Short, common-word titles (single word, < 15 chars) are prone to
        # false matches in prose. Require strong structural signals for these.
        title_words = entry["title"].split()
        is_short_common = len(title_words) == 1 and len(entry["title"]) < 15
        is_poem_with_author = entry.get("etype") == "poem" and entry.get("author")
        if is_short_common:
            if is_poem_with_author and best_score >= 50:
                # Poems with author confirmation: author proximity (+100) plus
                # any structural signal is strong enough for short titles
                pass
            elif best_score < 30:
                # Require at least a structural break for short single-word titles
                continue
        elif best_score < -30:
            # Reject any match with very poor scoring
            continue

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

    # Third pass: author-based matching for unmatched poems.
    # OCR frequently garbles short poem titles beyond recognition, but the
    # author name (printed right after the title) is usually legible.
    # For poem entries that weren't matched by title, find the author name
    # in the body and look backwards for the title start.
    matched_titles = {e["title"] for _, e in preliminary}

    for entry in entries:
        if entry["title"] in matched_titles:
            continue
        if entry.get("etype") != "poem" or not entry.get("author"):
            continue

        author_re = _build_author_regex(entry["author"])
        candidates = list(author_re.finditer(body))
        if not candidates:
            continue

        best_pos = None
        best_score = -9999

        for m in candidates:
            author_pos = m.start()
            score = 0

            # The author name should appear in a structural position:
            # preceded by a short capitalized title (the garbled poem title),
            # not embedded in prose.
            before = body[max(0, author_pos - 200):author_pos]
            before_stripped = before.rstrip()

            # Reject if preceded by biographical context (this is an author
            # mentioned in an article body, not a poem byline)
            if re.search(
                r'\b(?:by|Dr|Mrs?|Mr|husband|wife|daughter|son|brother|'
                r'sister|mother|father|President|Bishop|Elder|Professor|'
                r'children|parents|wrote|author|written|said)\s*$',
                before_stripped, re.IGNORECASE
            ):
                score -= 80

            # Reject if preceded by mid-sentence words
            if _MID_SENTENCE_WORDS.search(before):
                score -= 40

            # Boost if preceded by a structural break
            if _STRUCTURAL_BREAK_RE.search(before[-80:] if len(before) > 80 else before):
                score += 30

            # Boost if what follows the author name looks like verse
            # (short lines, capitalized, not prose)
            after = body[m.end():m.end() + 300].strip()
            if after:
                lines = after.split('\n')[:5]
                short_lines = sum(1 for l in lines if 0 < len(l.strip()) < 60)
                if short_lines >= 2:
                    score += 20

            # The text before the author should be short (poem title is
            # typically 1-5 words). Look for the last sentence boundary.
            last_break = -1
            for sep in ['.', '!', '?', '\u201d', '"']:
                idx = before_stripped.rfind(sep)
                if idx > last_break:
                    last_break = idx
            if last_break != -1:
                candidate_title = before_stripped[last_break + 1:].strip()
                # Poem titles are short; if the gap is huge it's not a poem
                if len(candidate_title) > 80:
                    score -= 30

            if score > best_score:
                best_score = score
                # Position: start of the poem title (look back from author)
                # Find the beginning of the title text before the author
                title_start = before.rstrip()
                for sep in ['.', '!', '?', '\u201d', '"', '\n\n']:
                    idx = title_start.rfind(sep)
                    if idx != -1:
                        title_start = title_start[idx + 1:]
                        break
                # Calculate absolute position
                title_offset = len(before) - len(before.rstrip()) - len(title_start.lstrip())
                best_pos = max(0, author_pos - 200) + max(0, len(before) - len(title_start.lstrip()))

        if best_pos is not None and best_score >= 0:
            preliminary.append((best_pos, entry))
            matched_titles.add(entry["title"])

    return preliminary


def _boundaries_from_found(body: str, found: list[tuple[int, dict]],
                           ads_start: int = -1) -> list[tuple[int, int, dict]]:
    """Convert (position, entry) list into (start, end, entry) boundaries.

    If ads_start >= 0, the last entry's boundary is capped at the ads section
    start rather than extending to the end of the body.
    """
    if not found:
        return []

    found = sorted(found, key=lambda x: x[0])

    # Cap the body end at the ads section if known, so the last entry
    # doesn't absorb advertising content.
    body_end = ads_start if ads_start >= 0 else len(body)

    bounds = []
    for i, (pos, entry) in enumerate(found):
        start = pos
        end = found[i + 1][0] if i + 1 < len(found) else body_end
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
    # "Sixty Years Ago" (recurring section) — also match its common opening
    re.compile(r'Sixty\s+Years\s+Ago', re.IGNORECASE),
    re.compile(r'Excerpts?\s+(?:F|St)rom\s+(?:the\s+)?Woman.s\s+Exponent', re.IGNORECASE),
    # "LESSON DEPARTMENT" header (various OCR garbles)
    re.compile(r'(?:LESS.{0,10}N\s+(?:DE.{0,6}\s+)?)?DEPARTMENT[\s|:]+(?:Sh|Th|Ch)eology', re.IGNORECASE),
    # "Woman's Sphere" (recurring section)
    re.compile(r"(?:W|V)oman.s\s+(?:Sp|S)here", re.IGNORECASE),
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
        r'(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?|Seal!?\s*He)\s*'
        r'(?:M|IT|T|m|He|HeIT)\s*(?:l)?essages?',
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
        r'(?:America|Come|Expand|Lesson|Cosmopolitan)',
        re.IGNORECASE
    ),
    # Social Science (OCR garbles "Science" as "ae", "eicHiee", etc.)
    re.compile(
        r'Social\s+(?:Sc(?:i|t|l)ence|eicHiee|Stustiee|ae|[A-Za-z]e)[\s\u2014\u2013\-:]+',
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

    # Author-bio separator pattern: <—>- or <->- or <> or similar combos
    _BIO_SEPARATOR_RE = re.compile(
        r'<[\u2014\u2013\-]*>[\u2014\u2013\-]*',  # <—>- or <->- or <> etc.
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
        #
        # Extract the serial base title (e.g. "Sow the Field With Roses")
        # from the current entry so we can skip other chapters/parts of
        # the same serial — running headers repeat the serial name without
        # the chapter number and would false-match other chapters' regexes.
        _serial_base = None
        for sep in [", Chapter ", ", Part ", " Chapter ", " Part "]:
            if sep in current_title:
                _serial_base = current_title[:current_title.index(sep)]
                break

        for oe_entry, oe_pat, oe_author_re, oe_is_matched in all_other_entries:
            # Skip the current entry itself
            if oe_entry["title"] == current_title:
                continue
            # Skip other chapters/parts of the same serial
            if _serial_base and oe_entry["title"].startswith(_serial_base):
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

        # --- Strategy 2b: detect lesson section headers in any entry ---
        # Lesson section headers (Theology, Visiting Teacher Messages, etc.)
        # indicate a boundary regardless of whether the current entry is a
        # lesson.  For lesson entries, skip their own header; for non-lesson
        # entries (poems, articles), any lesson header is a trim point.
        current_title = entry.get("title", "")
        own_prefix = None
        if _is_lesson_entry(entry):
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
        if entry_type == "poem":
            # Ad/filler content that commonly follows poems
            _ad_pat = re.compile(
                r'(?:BENEFICIAL\s+LIFE|Cable.Nelson|UTAH.IDAHO\s+SUGAR|'
                r'TOWARD\s+A\s+BETTER|Gentlemen:?\s+Enclosed|'
                r'Please\s+(?:send|tell)\s+me\s+more|'
                r'Salt\s+Lake\s+City,?\s+Utah\s+Please|'
                r'\bcheck\b.*\bmoney\s+order\b|'
                r'All\s+material\s+submitted\s+for\s+publication|'
                r'HANDY\s+.{0,5}\s+DURABLE|'
                r'TOURS\s+FOR\s+\d{4})',
                re.IGNORECASE
            )
            # For poems, look for structural breaks after the poem ends.
            # Use a minimum offset proportional to estimated poem size
            min_offset = max(50, min(200, len(chunk) // 4))
            if best_trim > min_offset * 3:
                for m in _STRUCTURAL_BREAK_RE.finditer(chunk, min_offset):
                    after_break = chunk[m.end():m.end() + 100].strip()
                    if not after_break:
                        continue
                    # Trim at the break if followed by uppercase text
                    if after_break[0].isupper() and m.start() > min_offset:
                        if m.start() < best_trim:
                            best_trim = m.start()
                        break
                    # Also trim if the break appears to be a separator
                    # between poem and non-poem content (even if followed
                    # by OCR garble)
                    if m.start() > min_offset:
                        # Find last sentence end before the break
                        before_break = chunk[:m.start()].rstrip()
                        last_end = max(
                            before_break.rfind('.'),
                            before_break.rfind('!'),
                            before_break.rfind('?'),
                        )
                        if last_end > min_offset and last_end + 1 < best_trim:
                            best_trim = last_end + 1
                        elif m.start() < best_trim:
                            best_trim = m.start()
                        break

            # Also look for ad/admin footer content
            ad_search_start = max(50, min(150, len(chunk) // 4))
            ad_match = _ad_pat.search(chunk, ad_search_start)
            if ad_match and ad_match.start() < best_trim:
                before_ad = chunk[:ad_match.start()].rstrip()
                last_sent = max(
                    before_ad.rfind('.'),
                    before_ad.rfind('!'),
                    before_ad.rfind('"'),
                    before_ad.rfind('\u201d'),
                )
                if last_sent > 50:
                    best_trim = last_sent + 1
                else:
                    best_trim = ad_match.start()

            # Aggressive length cap for poems: poems are rarely > 1500 chars.
            # If the chunk is still huge after all poem-specific strategies,
            # trim at the last sentence boundary before 1500 chars.
            MAX_POEM_CHARS = 1500
            if best_trim > MAX_POEM_CHARS:
                before_cap = chunk[:MAX_POEM_CHARS].rstrip()
                last_sent = max(
                    before_cap.rfind('.'),
                    before_cap.rfind('!'),
                    before_cap.rfind('?'),
                    before_cap.rfind('\u201d'),
                    before_cap.rfind('"'),
                )
                if last_sent > 100:
                    best_trim = last_sent + 1
                else:
                    best_trim = MAX_POEM_CHARS

        # --- Strategy 3b: detect "(To be continued)" markers ---
        # Serialized fiction chapters end with this marker.  Any content
        # after it (plus a short trailing region for the author name/bio)
        # belongs to a different article.
        _to_be_continued_re = re.compile(
            r'\(\s*(?:T|S|J)o\s+be\s+continued\s*\)',
            re.IGNORECASE
        )
        tbc_match = _to_be_continued_re.search(chunk, search_offset)
        if tbc_match:
            # Allow up to 800 chars after the marker for author bio text,
            # then trim.  Look for a structural break or new title+author
            # block after the marker+bio region.
            tbc_end = tbc_match.end()
            # Find the end of any trailing bio/poem after TBC
            post_tbc = chunk[tbc_end:tbc_end + 1500]
            # Look for a bio separator (<—>-) or structural break
            bio_in_tbc = _BIO_SEPARATOR_RE.search(post_tbc)
            if bio_in_tbc:
                # Trim after the bio (allow ~800 chars for bio text)
                bio_end = tbc_end + bio_in_tbc.end()
                bio_text = chunk[bio_end:bio_end + 1000]
                # Find the closing quote of the bio
                for q in ['\u201d', '"']:
                    qi = bio_text.find(q, 50)
                    if qi != -1:
                        trim_pos = bio_end + qi + 1
                        if trim_pos > search_offset and trim_pos < best_trim:
                            best_trim = trim_pos
                        break
                else:
                    # No closing quote — trim after a conservative bio length
                    trim_pos = min(bio_end + 500, len(chunk))
                    if trim_pos > search_offset and trim_pos < best_trim:
                        best_trim = trim_pos
            else:
                # No bio separator — trim after the TBC marker.
                # Allow a buffer for a trailing poem (author + short verse),
                # then look for signs of a new section: structural breaks,
                # ALL-CAPS headings, or title+author blocks.
                post_tbc = chunk[tbc_end:tbc_end + 1000]
                trim_pos = min(tbc_end + 200, len(chunk))

                # Look for an ALL-CAPS word (3+ chars) indicating a new
                # section heading after the trailing poem
                caps_heading = re.search(
                    r'(?:^|\s)([A-Z]{3,}(?:\s+[A-Z]{3,})*)',
                    post_tbc[50:]  # skip immediate post-TBC text
                )
                if caps_heading:
                    trim_pos = tbc_end + 50 + caps_heading.start()
                else:
                    # Fall back to structural break
                    for sb in _STRUCTURAL_BREAK_RE.finditer(chunk, tbc_end):
                        if sb.start() > tbc_end + 50:
                            trim_pos = sb.start()
                            break

                if trim_pos > search_offset and trim_pos < best_trim:
                    best_trim = trim_pos

        # --- Strategy 4: detect author-bio separators ---
        # Patterns like <—>- followed by author bio text indicate end of
        # article.  After the bio, any new content is bleed.
        # Also handles <> as a simple content separator between pieces.
        bio_separator_pos = None  # Track for Strategy 6
        for m in _BIO_SEPARATOR_RE.finditer(chunk, search_offset):
            sep_end = m.end()
            bio_separator_pos = m.start()
            bio_region = chunk[sep_end:sep_end + 1500]

            # Check if this is a simple content separator (e.g. "<>")
            # vs a bio separator (e.g. "<—>- Author Name is a...")
            is_simple_sep = (m.group() == '<>' or len(m.group()) <= 2)

            if is_simple_sep:
                # Simple separator: trim right at the separator
                if m.start() > search_offset and m.start() < best_trim:
                    best_trim = m.start()
                break

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
        if len(chunk) > 400:
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
                    # Strip leading page numbers and non-alpha prefixes
                    candidate_before = re.sub(
                        r'^(?:Page\s+)?\d{1,3}\s+', '', candidate_before
                    ).strip()
                    candidate_before = re.sub(
                        r'^[^A-Za-z]+', '', candidate_before
                    ).strip()
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
                    # Fallback: if candidate_before is empty/short (e.g. a
                    # quote char is the last break right before author name),
                    # look for the last sentence-ending punctuation before
                    # the title+author block and trim there
                    if not candidate_before or len(candidate_before) <= 2:
                        sent_end = max(
                            title_start.rfind('.', 0, last_break if last_break != -1 else len(title_start)),
                            title_start.rfind('!', 0, last_break if last_break != -1 else len(title_start)),
                            title_start.rfind('?', 0, last_break if last_break != -1 else len(title_start)),
                        )
                        if sent_end != -1:
                            trim_at = max(0, pos - 120) + sent_end + 1
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
                # Require title to be meaningful (not a stray word from prose)
                if len(candidate_title) < 5:
                    continue
                # Reject common prose words that look like capitalized titles
                if candidate_title.lower() in {
                    'that', 'this', 'there', 'these', 'those', 'their',
                    'after', 'before', 'would', 'could', 'should',
                    'about', 'again', 'still', 'where', 'which', 'while',
                }:
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

        # --- Strategy 8: cap the last entry's trailing content ---
        # The last matched entry's boundary defaults to len(body), absorbing
        # all unmatched trailing content (other lessons, poems, ads).
        # If none of the earlier strategies trimmed it, estimate a reasonable
        # max length from the other entries and look for structural breaks.
        if i == len(bounds) - 1 and best_trim == len(chunk):
            other_lengths = sorted(
                [e - s for s, e, _ in bounds[:i] if e - s > 100]
            )
            if other_lengths:
                median_len = other_lengths[len(other_lengths) // 2]
                min_article = max(500, median_len // 2)
                max_article = max(median_len * 3, 8000)
            else:
                min_article = 500
                max_article = 15000

            # Look for structural breaks that signal the article has ended
            for m in _STRUCTURAL_BREAK_RE.finditer(chunk, min_article):
                # If we're past max_article, trim at the first break
                if m.start() >= max_article:
                    best_trim = m.start()
                    break
                # Otherwise, check if what follows looks like a new section
                after = chunk[m.end():m.end() + 300].strip()
                if not after:
                    continue
                # New article: capitalized title-like text or known section header
                if after[0].isupper():
                    for sec_re in _SECTION_START_PATTERNS:
                        if sec_re.match(after):
                            best_trim = m.start()
                            break
                    if best_trim != len(chunk):
                        break
                    # Also check for lesson section headers
                    for lh_re in _LESSON_SECTION_HEADERS:
                        if lh_re.match(after):
                            best_trim = m.start()
                            break
                    if best_trim != len(chunk):
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

    ads_text, ads_start = find_ads_section(body)

    found = _match_entries_with_strategy(body, entries)
    bounds = _boundaries_from_found(body, found, ads_start=ads_start)

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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/{vol.capitalize()}"
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
        description="Extract Relief Society Magazine Vol 49 into individual entries"
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
    vol49_out = OUTPUT_DIR / "vol49"
    if vol49_out.exists() and not args.dry_run:
        print(f"Cleaning output directory: {vol49_out}")
        shutil.rmtree(vol49_out)

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    volume_json = {}

    for (vol, issue_key), entries in VOL49_TOC.items():
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
