#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 44 (1957).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol44.py
    python extract_vol44.py --dry-run
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

VOL44_TOC = {
    ("Vol44", "No01_January_1957"): [
        {"title": "New Year's Greetings", "author": None, "etype": "article"},
        {"title": "Homemaking, the Ideal Career for Women", "author": "Annie M. Ellsworth", "etype": "article"},
        {"title": "Award Winners -- Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Remembering the Handcarts -- First Prize Poem", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Christmastime -- Second Prize Poem", "author": "Frances Carter Yost", "etype": "poem"},
        {"title": "Benediction to Summer -- Third Prize Poem", "author": "Joanne B. Rose", "etype": "poem"},
        {"title": "Award Winners -- Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Strength for the Way -- First Prize Story", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "The Mexican Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Ancient Cliff Dwellings of Walnut Canyon", "author": "Nell Murbarger", "etype": "article"},
        {"title": "Grocery Hints That Help", "author": None, "etype": "article"},
        {"title": "Modesty Is the Best Policy", "author": None, "etype": "article"},
        {"title": "Polio and the March of Dimes", "author": "Basil O'Connor", "etype": "article"},
        {"title": "Biographical Sketches of Award Winners in the Eliza R. Snow Poem Contest and First Prize Winner in the Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "The Door Opens for Tillie", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Silver Service", "author": "Florence S. Glines", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Let Your Light So Shine", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Relief Society Assigned Evening Meeting of Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Bound Volumes of 1956 Relief Society Magazines", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Sina Bishop Reid Makes Her Own Crochet Designs", "author": None, "etype": "article"},
        {"title": "Recipes From Mexico", "author": "Jennie R. Bowman", "etype": "article"},
        {"title": "Vegetables -- A Different Way Every Day -- Part II", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Theology: A Review of Outstanding Characters of The Book of Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Ye Shall Have Hope Through the Atonement of Christ", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Beverages", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: A Midsummer Night's Dream", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Be Ye Therefore Perfect", "author": "John Farr Larson", "etype": "lesson"},
    ],

    ("Vol44", "No02_February_1957"): [
        {"title": "Muted Hour", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Sustaining the Authorities of the Church", "author": "ElRay L. Christiansen", "etype": "article"},
        {"title": "The Netherlands Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Values Derived From Reading Worthwhile Literature", "author": "Thomas C. Romney", "etype": "article"},
        {"title": "What Makes a Happy Home", "author": "Wilma Boyle Bunker", "etype": "article"},
        {"title": "Great Men Pray", "author": None, "etype": "article"},
        {"title": "A Flag for Utah Statehood", "author": "Margaret G. Derrick", "etype": "article"},
        {"title": "I Explore the Upstairs", "author": "Zipporah Layton Stewart", "etype": "article"},
        {"title": "Mother's Shoes -- Second Prize Story", "author": "Edith Larson", "etype": "fiction"},
        {"title": "Hearts United", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Bitter Medicine -- Part 2", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Step Aside", "author": "June Nielsen", "etype": "editorial"},
        {"title": "Birthday Congratulations to Amy Brown Lyman, Former Relief Society General President", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the Netherlands Mission", "author": "Ada S. Van Dam", "etype": "article"},
        {"title": "Mary E. Jones Dalton Finds Happiness in Her Hobbies", "author": None, "etype": "article"},
        {"title": "Candy for Valentine's Day", "author": "Mary J. Wilson", "etype": "article"},
        {"title": "Theology: A New Witness for Christ", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Ye Would Ask God, the Eternal Father, in the Name of Christ, If These Things Are Not True", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Summary", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Julius Caesar", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Be Ye Therefore Perfect", "author": "John Farr Larson", "etype": "lesson"},
    ],

    ("Vol44", "No03_March_1957"): [
        {"title": "The Vastness of Space", "author": "Katherine P. Walton", "etype": "poem"},
        {"title": "Women Are Worshipers of God", "author": "Levi Edgar Young", "etype": "article"},
        {"title": "Helen Woodruff Anderson Appointed Second Counselor", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Hulda Parker Named General Secretary-Treasurer", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Mary Vogel Cameron Appointed to General Board", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Afton W. Hunt Appointed to General Board", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "Velma N. Simonsen Retires From General Presidency", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Margaret C. Pickering Resigns As General Secretary-Treasurer", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "The New Zealand Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Vera Hinckley Mayhew -- Biographical Sketch", "author": None, "etype": "article"},
        {"title": "Stratford-Upon-Avon and the Shakespeare Memorial Theater", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The American National Red Cross", "author": None, "etype": "article"},
        {"title": "Be a Relief Society Magazine Promoter", "author": "Virginia Glenn", "etype": "article"},
        {"title": "Embellishment", "author": "Clarissa A. Beesley", "etype": "article"},
        {"title": "Buttercups", "author": "Mary C. Martineau", "etype": "article"},
        {"title": "The Slow Hurry -- Third Prize Story", "author": "Vera H. Mayhew", "etype": "fiction"},
        {"title": "The Bright Star, Chapter 1", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "Bitter Medicine -- Part 3 -- Conclusion", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Relief Society Legacy for Young Women", "author": "June Nielsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From New Zealand", "author": "Arta R. Ballif", "etype": "article"},
        {"title": "Sarah Seely Larsen Has Enjoyed a Sewing Hobby for Seventy Years", "author": None, "etype": "article"},
        {"title": "Herbs for Modern Cookery -- Tarragon", "author": "Elizabeth Williamson", "etype": "article"},
    ],

    ("Vol44", "No04_April_1957"): [
        {"title": "Apricot Blossoms", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Family and the Resurrection", "author": "Roy W. Doxey", "etype": "article"},
        {"title": "The Northern Far East Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Successful Selling", "author": None, "etype": "article"},
        {"title": "Fight Cancer With Research, Education, and Service", "author": "Walter J. Kohler", "etype": "article"},
        {"title": "Holly Ann Clem Dye's Hobby Overflows Her Home", "author": None, "etype": "article"},
        {"title": "Over the Back Fence", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "The Bright Star, Chapter 2", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "Something Special", "author": "Frances Carter Yost", "etype": "fiction"},
        {"title": "A Present From Sister Willard", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Blessings Attendant Upon an Office", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Hymn of the Month", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the Northern Far East Mission", "author": "Fern S. Bair", "etype": "article"},
        {"title": "Herbs for Modern Cookery -- Marjoram", "author": "Elizabeth Williamson", "etype": "article"},
    ],

    ("Vol44", "No05_May_1957"): [
        {"title": "Ballerina", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "A Mother's Joy in Her Family", "author": "Vivian R. McConkie", "etype": "article"},
        {"title": "The Norwegian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Contest Announcements -- 1957", "author": None, "etype": "article"},
        {"title": "How to Write a Short Story", "author": "Mary E. Knowles", "etype": "article"},
        {"title": "A Year's Supply", "author": "Mabel L. Anderson", "etype": "article"},
        {"title": "Modesty Is the Best Policy", "author": None, "etype": "article"},
        {"title": "Mrs. Vola Personeni Clo", "author": "Vernessa M. Nagle", "etype": "article"},
        {"title": "Bright Barrier of the Plains", "author": "Nell Murbarger", "etype": "article"},
        {"title": "The Old Tin Trunk", "author": "Grace W. Ball", "etype": "article"},
        {"title": "Be Still and Know That I Am God", "author": "Mabel Law Atkinson", "etype": "article"},
        {"title": "The Blue Calico Dress", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "The Third House Down", "author": "Florence B. Dunford", "etype": "fiction"},
        {"title": "The Bright Star, Chapter 3", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Pen in Hand", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Brigham Young University Leadership Week", "author": None, "etype": "article"},
        {"title": "Magazine Subscriptions for 1956", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Magazine Honor Roll for 1956", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the Norwegian Mission", "author": "Sigrid B. Williams", "etype": "article"},
        {"title": "Herbs for Modern Cookery -- Garlic", "author": "Elizabeth Williamson", "etype": "article"},
    ],

    ("Vol44", "No06_June_1957"): [
        {"title": "They Shall Call Him Blessed", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "Wealtha S. Mendenhall Appointed to General Board", "author": "W. Aird McDonald", "etype": "article"},
        {"title": "The Samoan Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "A Relief Society Gleaning", "author": "Rachel Grant Taylor", "etype": "article"},
        {"title": "Modesty Is the Best Policy", "author": None, "etype": "article"},
        {"title": "Mental Illness -- A National Disaster", "author": "F. Barry Ryan", "etype": "article"},
        {"title": "Help Yourself to Happiness", "author": "Frances C. Yost", "etype": "article"},
        {"title": "A Nursery Will Be Maintained", "author": "Edna H. Day", "etype": "article"},
        {"title": "This Is My Building", "author": "Maud H. Fullmer", "etype": "article"},
        {"title": "Mother Had Seven Girls", "author": "Jennie Brown Rawlins", "etype": "article"},
        {"title": "Slight Hazard", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "The Patchwork Quilt", "author": "Elizabeth Cannon McCrimmon", "etype": "fiction"},
        {"title": "The Bright Star, Chapter 4", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 127th Annual Church Conference", "author": "Helen W. Anderson", "etype": "editorial"},
        {"title": "In Memoriam -- Lucy Jane Brimhall Knight", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Summer Work Meetings", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Recipes From the Samoan Mission", "author": "Irene B. Woodbury", "etype": "article"},
        {"title": "Herbs for Modern Cookery -- Basil", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Teaching Aids for the 1957-58 Lessons", "author": "Mary R. Young", "etype": "lesson"},
        {"title": "Suggestions for Music Leaders", "author": "Florence J. Madsen", "etype": "lesson"},
        {"title": "Theology -- The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Truths To Live By From The Doctrine and Covenants", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting -- Living More Abundantly", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature -- Shakespeare in Our Lives", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science -- Latter-day Saint Family Life", "author": "John Farr Larson", "etype": "lesson"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
    ],

    ("Vol44", "No07_July_1957"): [
        {"title": "Habits of Industry and the Abundant Life for Children and Parents", "author": "Madeline B. Wirthlin", "etype": "article"},
        {"title": "The South African Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Let's Visit a Desert Botanical Garden", "author": "Marijane Morris", "etype": "article"},
        {"title": "The Joy of Remembering", "author": "Naomi M. Manwaring", "etype": "article"},
        {"title": "The Long and Short of Marriage", "author": None, "etype": "article"},
        {"title": "America, Cradle of Liberty", "author": "Elsie Matthews", "etype": "article"},
        {"title": "Handwork Is Fun", "author": "Celia Luce", "etype": "article"},
        {"title": "Your Child Is a Music Lover", "author": "Helen Morris", "etype": "article"},
        {"title": "Life Is a Journey", "author": "Cleopha Jensen", "etype": "article"},
        {"title": "No Hate in Our Hearts", "author": "Wilma Boyle Bunker", "etype": "article"},
        {"title": "Holly, Part I", "author": "Margaret S. Hardy", "etype": "fiction"},
        {"title": "Ice Cream for the Fourth", "author": "Maryhale Woolsey", "etype": "fiction"},
        {"title": "The Bright Star, Chapter 5", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Spirit of the Pioneer", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Buying Textbooks for Relief Society Lessons", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Recipes From the South African Mission", "author": "Nora C. Duncan", "etype": "article"},
        {"title": "Leah A. Hamilton Collects Potted Plants and Does Handwork, Weaving, and Painting", "author": None, "etype": "article"},
        {"title": "When Mother Made Butter", "author": "Mae R. Winters", "etype": "article"},
        {"title": "Theology: The Lord Speaks Through His Prophets", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And the Voice of Warning Shall Be Unto All People", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Spending Money Wisely", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: William Shakespeare, Dramatist", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Family Is Forever", "author": "John Farr Larson", "etype": "lesson"},
    ],

    ("Vol44", "No08_August_1957"): [
        {"title": "Inventory for August", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "The Worth of Souls", "author": "John Longden", "etype": "article"},
        {"title": "The South Australian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "The Long and Short of Marriage -- The Continuity of Companionship", "author": None, "etype": "article"},
        {"title": "A Need for Patience", "author": "Myrtle M. Dean", "etype": "article"},
        {"title": "Annual Report 1956", "author": "Hulda Parker", "etype": "article"},
        {"title": "Society Sonnets", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Holly, Part II", "author": "Margaret S. Hardy", "etype": "fiction"},
        {"title": "All Is Well", "author": "Queenie Jenkins", "etype": "fiction"},
        {"title": "The Bright Star, Chapter 6", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Day of Doing Common Things", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Recipes From the South Australian Mission", "author": "Adelphia D. Bingham", "etype": "article"},
        {"title": "Ella J. Kotter Finds Self-Expression Through Color Combinations and Designs for Quilts", "author": None, "etype": "article"},
        {"title": "Theology: Origin of the Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Inasmuch As They Were Humble", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Increasing Our Standard of Living", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: Othello, The Moor of Venice", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Families Have Problems", "author": "John Farr Larson", "etype": "lesson"},
    ],

    ("Vol44", "No09_September_1957"): [
        {"title": "Country Neighbors", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "My Father -- A True Leader", "author": "Emma Rae McKay Ashton", "etype": "article"},
        {"title": "Free Agency", "author": "Henry D. Moyle", "etype": "article"},
        {"title": "The Southern Far East Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "A Temple Rises in New Zealand", "author": "Wealtha S. Mendenhall", "etype": "article"},
        {"title": "Nothing Worth Having Is Free", "author": None, "etype": "article"},
        {"title": "The Heavenly Bonus", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "For This I Have Yearned", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "The Bright Star, Chapter 7", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Her Choice", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Relief Society Magazine Subscription Price Raised to $2", "author": None, "etype": "article"},
        {"title": "Annual General Relief Society Conference", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Recipes From the Southern Far East Mission", "author": "Luana C. Heaton", "etype": "article"},
        {"title": "Dora Jolley Pursues a Hobby of Handwork", "author": None, "etype": "article"},
        {"title": "Theology: What to Expect From a Study of The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: O Ye That Embark in the Service of God, See That Ye Serve Him With All Your Heart, Might, Mind and Strength", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Living Economically", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: Twelfth Night, or What You Will", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: References for Families Have Problems", "author": None, "etype": "lesson"},
    ],

    ("Vol44", "No10_October_1957"): [
        {"title": "Sparkling Family Hour", "author": "Shirley B. And Monroe J. Paxman", "etype": "article"},
        {"title": "The Southwest Indian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Let's Visit a Desert Sea", "author": "Marijane Morris", "etype": "article"},
        {"title": "Purchasing Soul Growth", "author": None, "etype": "article"},
        {"title": "Compiling a Census of Post-Polio Patients", "author": None, "etype": "article"},
        {"title": "Pine Nutting", "author": "Alice R. Rich", "etype": "article"},
        {"title": "Trespassing Trio", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Miss Pennywell Goes Into Action", "author": "Frances P. Reid", "etype": "fiction"},
        {"title": "Bleak House", "author": "Florence B. Dunford", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Eternal Bonuses", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "In Memoriam -- Priscilla Livingston Evans", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "From Near And Far", "author": None, "etype": "article"},
        {"title": "Recipes From the Southwest Indian Mission", "author": "Lavena L. Rohner", "etype": "article"},
        {"title": "Louise McMurrin Paints Beautiful Pictures", "author": None, "etype": "article"},
        {"title": "Theology: The Prophecy Concerning Elijah the Prophet", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Remember That Without Faith You Can Do Nothing", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Making Money More Valuable", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: The \"Hamlet\" Frame", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Eternal Marriage", "author": "John Farr Larson", "etype": "lesson"},
    ],

    ("Vol44", "No11_November_1957"): [
        {"title": "Cleanliness", "author": "Emma Ray Riggs McKay", "etype": "article"},
        {"title": "Obedience in All Things", "author": "Helen W. Anderson", "etype": "article"},
        {"title": "Love's Equation", "author": "Luett Stantliff", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "The Spanish-American Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Shopping Yesterday", "author": "Elaine Reiser", "etype": "article"},
        {"title": "Live and Learn Forever", "author": None, "etype": "article"},
        {"title": "Bestiary", "author": "Anne S. W. Gould", "etype": "article"},
        {"title": "The National Tuberculosis Association Christmas Seal", "author": "Dorothea M. Lindsey", "etype": "article"},
        {"title": "The Christmas Cards", "author": "Dorothy Boys Kilian", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Gold, and Frankincense, and Myrrh", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Annual General Relief Society Conference Cancelled", "author": None, "etype": "article"},
        {"title": "Pictures of the Relief Society Building Available", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Recipes From the Spanish-American Mission", "author": "Mabel B. Wilson", "etype": "article"},
        {"title": "Noel Bird Goodale Knits Beautiful Garments", "author": None, "etype": "article"},
        {"title": "Theology: Satan's Opposition to the Coming Forth of The Book of Mormon", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Behold, You Should Not Have Feared Man More Than God", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Family and the Home", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: Hamlet, Prisoner in Denmark", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: As the Twig Is Bent", "author": "John Farr Larson", "etype": "lesson"},
    ],

    ("Vol44", "No12_December_1957"): [
        {"title": "Arrival of the Shepherds", "author": None, "etype": "article"},
        {"title": "Jesus Is the Christ", "author": None, "etype": "article"},
        {"title": "Live and Learn Forever", "author": None, "etype": "article"},
        {"title": "Testimony of the Stick of Joseph", "author": "Louise S. Cotterell", "etype": "article"},
        {"title": "Christmas 1884", "author": "Vernessa Miller Nagle", "etype": "article"},
        {"title": "Merry Christmas, Mother!", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "A Grandma for Christmas", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "Something Lacking", "author": "Frances Carter Yost", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Message of Christmas", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "New Serial Elizabeth's Children to Begin in January", "author": "Olive W. Burt", "etype": "article"},
        {"title": "Notes to the Field: Christmas Presents for Relief Society Women", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "From Near And Far", "author": None, "etype": "article"},
        {"title": "Merry, Merry Christmas!", "author": "Helen Spencer Williams", "etype": "article"},
        {"title": "Holiday Guests Deserve the Best", "author": "Helen W. Anderson", "etype": "article"},
        {"title": "Christine H. Jensen Crochets Prize-Winning Rugs", "author": None, "etype": "article"},
        {"title": "Tie Your Ideas With String", "author": None, "etype": "article"},
        {"title": "Theology: Qualifications of Those Who Labor in the Ministry", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Fear Not to Do Good, My Sons, for Whatsoever Ye Sow, That Shall Ye Also Reap", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Importance of Insurance", "author": "William F. Edwards", "etype": "lesson"},
        {"title": "Literature: King Lear", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Families Have Members", "author": "John Farr Larson", "etype": "lesson"},
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
    year = 1957

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol44_{key}.txt"
    ISSUE_FILES[("Vol44", key)] = (fname, month)


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
# Known OCR-mangled section headers for Vol44.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
_BRIGHT_STAR_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)e\s+Bright\s+(?:S|J|\()tar"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|Cuapter)\s+)?"
)
_HOLLY_PART_PAT = (
    r"Holly"
    r".{0,80}?"  # skip author name
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—Characters and Teachings of The Book of Mormon Lesson NN (Jan-Jun)
    # or: Sheology—The Doctrine and Covenants Lesson N / Preview (Jul-Dec)
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
    # OR: Visiting Teacher Messages: ''Ye Shall Have Hope...
    # OCR variants: oVisiting CSeacher ITlessages, Visiting Seacher I lessages, Ussiting elewakion ITlessages
    # Specific severely-mangled entries where remainder can't match:
    "Visiting Teacher Messages: Ye Shall Have Hope Through the Atonement of Christ": (
        r"(?:o|O)?(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:[A-Za-z]{4,12})\s+"
        r"(?:M|IT|I|T|m)(?:\s+)?(?:l)?essages"
        r"[\s\-\u2014\u2013:]*[\"\u201c\u2018\u2019\u0027\|\.]{0,5}\s*"
        r"Ye\s+Shall\s+Have\s+Hope"
    ),
    "Visiting Teacher Messages: Ye Would Ask God, the Eternal Father, in the Name of Christ, If These Things Are Not True": (
        r"(?:o|O)?(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:[A-Za-z]{4,12})\s+"
        r"(?:M|IT|I|T|m)(?:\s+)?(?:l)?essages"
        r"[\s\-\u2014\u2013:]*[\"\u201c\u2018\u2019\u0027\|\.]{0,5}\s*"
    ),
    # General VTM pattern — match header, optionally skip to Message/Lesson N, consume quotes/ellipsis
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
    # Body has: EDITORIAL m9 VOL. 44 MONTH 1957 NO. N Title
    # OCR variants: IITORIAL, DIT ORIAL, EDITORIAL
    "Editorial: Gold, and Frankincense, and Myrrh": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
        r"Gold\s+and\s+Frankincense,?\s+and\s+M[ly]*rrh"
    ),
    "Editorial: Her Choice": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
        r"(?:H|Fl)er\s+Choice"
    ),
    "Editorial: Pen in Hand": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
        r"(?:P|p)en\s+in\s+(?:H|#|Fl)[a-z]*nd"
    ),
    "Editorial: ": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"(?:E?D?I?T?\s*)?(?:EDITORIAL|IITORIAL|DITORIAL|DIT\s*ORIAL)[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Body has: Literature—Shakespeare in Our Lives Lesson NN
    # OCR variants: Ol iterature, eran, tiene, alee oe, Ly pene — word is often unreadable
    # Fallback: match on "Shakespeare in Our Lives" + Lesson N when "iterature" is missing
    "Literature: ": (
        r"(?:"
            r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s\-\u2014\u2013:]*(?:Shakespea[rt]e\s+in\s+Our\s+Lives\s*)?"
        r"|"
            r"[\S\s]{0,30}?Shakespea[rt]e\s+in\s+Our\s+Lives\s*"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Literature -- Shakespeare in Our Lives": (
        r"[\S\s]{0,30}?Shakespea[rt]e\s+in\s+Our\s+Lives"
    ),
    "Literature -- ": (
        r"(?:"
            r"(?:[A-Za-z]{0,6}\s*)?[Ll]?iterature[\s\-\u2014\u2013:]*(?:Shakespea[rt]e\s+in\s+Our\s+Lives\s*)?"
        r"|"
            r"[\S\s]{0,30}?Shakespea[rt]e\s+in\s+Our\s+Lives\s*"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    # Body has: Social Science—Latter-day Saint Family Life Lesson N
    # OCR variants: "enero meen" for Science, "oan! Pieiemce" — word often unreadable
    # Fallback: match on "Latter-day Saint Family Life" when "Science" is garbled
    "Social Science: ": (
        r"(?:"
            r"Social\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"|"
            r"[A-Za-z!\s]{0,25}Latter[\-\s]day\s+Saint\s+Family\s+Life"
        r")"
        r"[\s]*[\-\u2014\u2013:]*[\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3})?"
    ),
    "Social Science -- ": (
        r"(?:"
            r"Social\s+(?:Sc(?:i|t)ence|[A-Za-z!]{3,12})"
        r"|"
            r"[A-Za-z!\s]{0,25}Latter[\-\s]day\s+Saint\s+Family\s+Life"
        r")"
        r"[\s]*[\-\u2014\u2013:]*[\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3})?"
    ),
    # Body has: Work ITleeting—Living More Abundantly ... Lesson N
    # OCR variants: ITlecting (c for e), Discussion N instead of Lesson N (Nov-Dec)
    "Work Meeting: ": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|M)?(?:\s+)?(?:l)?(?:e|E|c)[a-z]?ting"
        r"(?:"
        r"[)\s]*\)\s*"
        r"|"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Work Meeting -- Living More Abundantly": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|M)?(?:\s+)?(?:l)?(?:e|E|c)[a-z]?ting"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"Living\s+More\s+Abundantly"
    ),
    "Work Meeting -- ": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|M)?(?:\s+)?(?:l)?(?:e|E|c)[a-z]?ting"
        r"(?:"
        r"[)\s]*\)\s*"
        r"|"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Discussion\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]{0,3}"
    ),
    "Notes From the Field: Relief Society Activities": (
        r"NOTES\s+FROM\s+THE\s+FIELD\s+\d+"
    ),
    # Severely garbled names/titles
    "Mrs. Vola Personeni Clo": (
        r"(?:M|IT|Il|Nl)l?(?:RS|MS|rs|ms|S)\s*\.?\s*VOLA"
    ),
    "The Joy of Remembering": (
        r"(?:Th|Sh|Ch)e[\s\-\u2014\u2013]+(?:J|Y)oy[\s\-\u2014\u2013]+of[\s\-\u2014\u2013]+Reme"
    ),
    # Prize stories/poems: body doesn't have "First Prize Story" suffix
    "Strength for the Way -- First Prize Story": (
        r"Strength[\s\-\u2014\u2013]+for[\s\-\u2014\u2013]+the[\s\-\u2014\u2013]+Way"
    ),
    "The Slow Hurry -- Third Prize Story": (
        r"(?:Th|Sh|Ch)e[\s\-\u2014\u2013]+Slow[\s\-\u2014\u2013]+(?:H|Fl)urry"
    ),
    "Christmastime -- Second Prize Poem": (
        r"Christmas(?:time|cope|mastime)"
    ),
    "Mother's Shoes -- Second Prize Story": (
        r"(?:M|IT|Nl)l?other[\u2018\u2019\u201c\u201d\u2032\u0027\"]*\s*s[\s\-\u2014\u2013]+Shoes"
    ),
    # Award Winners with OCR-mangled text between "Winners" and contest name
    "Award Winners -- Eliza R. Snow Poem Contest": (
        r"Award[\s\-\u2014\u2013]+Winners[\s\S]{0,30}?(?:E|6)liza[\s\-\u2014\u2013]+R\.?[\s\-\u2014\u2013]+Snow[\s\-\u2014\u2013]+Poem"
    ),
    # Missing initial letter: "agazine" for "Magazine"
    "Magazine Honor Roll for 1956": (
        r"(?:M|IT|Nl)?l?agazine[\s\-\u2014\u2013]+(?:H|Fl)onor[\s\-\u2014\u2013]+Roll"
    ),
    # Tuberculosis with OCR-mangled "Christmas Seal" -> "Christepenns"
    "The National Tuberculosis Association Christmas Seal": (
        r"(?:Th|Sh|Ch)e[\s\-\u2014\u2013]+(?:N|V|T|l)ational[\s\-\u2014\u2013]+"
        r"(?:T|S|J|\()uberculosis[\s\-\u2014\u2013]+Association"
    ),
    # In Memoriam with OCR-mangled names
    "In Memoriam -- Lucy Jane Brimhall Knight": (
        r"(?:I|(?:I|\|)n)\s+(?:M|IT|Nl)l?emoriam[\s\-\u2014\u2013]+Lucy\s+(?:J|Y)ane\s+Brimhall"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    # Serial fiction: The Bright Star
    "The Bright Star, Chapter 1": _BRIGHT_STAR_CHAPTER_PAT + r"(?:1|I)\b",
    "The Bright Star, Chapter 2": _BRIGHT_STAR_CHAPTER_PAT + r"(?:2|II)\b",
    "The Bright Star, Chapter 3": _BRIGHT_STAR_CHAPTER_PAT + r"(?:3|III)\b",
    "The Bright Star, Chapter 4": _BRIGHT_STAR_CHAPTER_PAT + r"(?:4|IV)\b",
    "The Bright Star, Chapter 5": _BRIGHT_STAR_CHAPTER_PAT + r"(?:5|V)\b",
    "The Bright Star, Chapter 6": _BRIGHT_STAR_CHAPTER_PAT + r"(?:6|VI)\b",
    "The Bright Star, Chapter 7": _BRIGHT_STAR_CHAPTER_PAT + r"(?:7|VII)\b",
    # Serial fiction: Holly (2-part, uses "Part I" / "Part II")
    "Holly, Part I": _HOLLY_PART_PAT + r"Part\s+I\b",
    "Holly, Part II": _HOLLY_PART_PAT + r"Part\s+II\b",
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol44"
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
        description="Extract Relief Society Magazine Vol 44 into individual entries"
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

    for (vol, issue_key), entries in VOL44_TOC.items():
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
