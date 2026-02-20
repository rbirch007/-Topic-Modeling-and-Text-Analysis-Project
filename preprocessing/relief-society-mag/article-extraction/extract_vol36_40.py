#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volumes 33-40.

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched using two strategies (strict line-start and loose
anywhere-match) and both results are written as separate text files plus
a per-volume JSON containing full content.  See processed/README.md for
schema documentation.

Usage:
    python extract_vol33-40.py
    python extract_vol33-40.py --dry-run
    python extract_vol33-40.py --volume 33
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
#
# NOTE: This script contains TOC data for Vol33-35 (manually extracted).
#       Vol36-40 TOC data should be added when available.
# ---------------------------------------------------------------------------

TOC = {
    # ===================================================================
    # VOLUME 33 (1946)
    # ===================================================================

    ("Vol33", "No1_January_1946"): [
        {"title": "Frontispiece Picture General Presidency of Relief Society", "author": None, "etype": "front_matter"},
        {"title": "New Year's Greeting", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Margaret Cummock Pickering", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Award Winners Eliza Roxey Snow Memorial Prize Poem Contest", "author": None, "etype": "article"},
        {"title": "Star of Gold", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "I Shall Be Late", "author": "Betty Wall Madsen", "etype": "poem"},
        {"title": "The Good Inheritance", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Award Winners Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Spring Festival", "author": "Mary Ek Knowles", "etype": "fiction"},
        {"title": "Nicholas G. Smith A Tribute", "author": "Marion G. Romney", "etype": "article"},
        {"title": "Drifting Or Sailing to a Charted Course", "author": "Lella Marler Hoggan", "etype": "article"},
        {"title": "Unceasing Crusade National Foundation For Infantile Paralysis", "author": None, "etype": "article"},
        {"title": "Mormonism In the Eyes of the Press: Dolly Madison and the Outcast Mormons", "author": "James R. Clark", "etype": "article"},
        {"title": "Let Us Fittingly Mark the Land", "author": "Howard R. Driggs", "etype": "article"},
        {"title": "And For Eternity Chapter 8", "author": "Olive Woolley Burt", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Be Not Weary in Well Doing", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Howard S. McDonald Inaugurated President of Brigham Young University", "author": None, "etype": "article"},
        {"title": "Blanche B. Stoddard Resigns as General Secretary-Treasurer", "author": None, "etype": "article"},
        {"title": "In Memoriam: Elder Joseph J. Cannon and Elder Burton K. Farnsworth", "author": None, "etype": "article"},
        {"title": "Only One Relief Society Conference to be Held Each Year", "author": None, "etype": "article"},
        {"title": "Only One Relief Society Assigned Evening Meeting Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Suggestions to Contributors", "author": None, "etype": "article"},
        {"title": "Messages From the Missions", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The United Order Begun and Its Establishment in the Settlements", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Charity Hopeth All Things, Charity Endureth All Things", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Some Broader Applications of Social Ethics", "author": "Harold T. Christensen", "etype": "lesson"},
        {"title": "The Infinite Frontispiece", "author": "Grace Sayre", "etype": "poem"},
        {"title": "After A While", "author": "Maude Hatch Benedict", "etype": "poem"},
        {"title": "Elijah", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "The Weight of Your Cloud", "author": "Edna S. Dustin", "etype": "poem"},
        {"title": "Shopping For a Doll", "author": "Hazel Jones Owen", "etype": "poem"},
        {"title": "Promise", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "I Know a Road", "author": "Violet Harris Hendrickson", "etype": "poem"},
        {"title": "Hill Woman", "author": "Maude Blixt Trone", "etype": "poem"},
        {"title": "The Morning Star", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Letter", "author": "Nan S. Richardson", "etype": "poem"},
    ],

    ("Vol33", "No2_February_1946"): [
        {"title": "American Statesmen Their Attitude Toward God", "author": "El Ray L. Christiansen", "etype": "article"},
        {"title": "Mormonism in the Eyes of the Press: The World Receives the News of Joseph Smith's Death", "author": "James R. Clark", "etype": "article"},
        {"title": "Abraham Lincoln Speaks Again", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "What the Children Can Do For Dr.", "author": "M. Lynn Bennion", "etype": "article"},
        {"title": "Fifty and One Ideas For You and For Your Home", "author": "Blanche M. Condie", "etype": "article"},
        {"title": "The Ring of Strength", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Ring Out the Old", "author": "Estelle Webb Thomas", "etype": "fiction"},
        {"title": "And For Eternity Chapter 9", "author": "Olive Woolley Burt", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Star in Her Window", "author": "Ruth H. Chadwick", "etype": "article"},
        {"title": "Study My Word", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Congratulations to Sister Lyman on Her Birthday", "author": None, "etype": "article"},
        {"title": "Messages From the Missions (Continued)", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Music Notes", "author": "Florence J. Madsen", "etype": "article"},
        {"title": "The Building of Temples Continues", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Charity Never Faileth", "author": "Lowell L. Bennion", "etype": "lesson"},
        {"title": "Bible Influence as Revealed in Children's Literature", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Morality and the Church", "author": "Harold T. Christensen", "etype": "lesson"},
        {"title": "The Dream Is Ours Frontispiece", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "To Elizabeth", "author": "Olive M. Nicholes", "etype": "poem"},
        {"title": "The Storm", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "To Marie", "author": "Adeline J. Haws", "etype": "poem"},
        {"title": "My Old Green Chest", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Elsie C. Carroll", "author": None, "etype": "poem"},
        {"title": "Twilight", "author": "Ada N. Jones", "etype": "poem"},
        {"title": "Crucible", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "A Prayer", "author": "Celia Van Cott", "etype": "poem"},
    ],

    ("Vol33", "No3_March_1946"): [
        {"title": "Relief Society and the Future", "author": "Leah D. Widtsoe", "etype": "article"},
        {"title": "A Few of Our Gifted Utah Women Artists Part III", "author": "Alice Merrill Horne", "etype": "article"},
        {"title": "The First Easter A Fantasy", "author": "Evelyn Wooster Viner", "etype": "article"},
        {"title": "An Open Door to Happiness", "author": "Wilford D. Lee", "etype": "article"},
        {"title": "Our Parks and Playgrounds", "author": "Fred Tedesco", "etype": "article"},
        {"title": "The Clouded Window", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Thrift Begins In The Garden", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Lady-in-Waiting", "author": "Marguerite J. Griffin", "etype": "article"},
        {"title": "All Is Known", "author": "Irva Pratt Andrus", "etype": "fiction"},
        {"title": "Prelude to Spring", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        {"title": "Even the Frail", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "The Best Little Shrub", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "And For Eternity Chapter 10 (Conclusion)", "author": "Olive Woolley Burt", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "A Thing of Beauty is a Joy Forever", "author": "Gertrude R. Garff", "etype": "editorial"},
        {"title": "New Serial to Begin in April", "author": None, "etype": "article"},
        {"title": "Messages From the Missions (Continued)", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Save Those Precious Scraps", "author": "Eleanor Welch Schow", "etype": "article"},
        {"title": "Renewal Frontispiece", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Her Staff", "author": "Hortense Spencer Andersen", "etype": "poem"},
        {"title": "The Difference", "author": "Cleo Gordon", "etype": "poem"},
        {"title": "Easter Lily", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Late Motherhood", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Remember Love", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Lesson", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "Lesson", "author": "Roberta Bates", "etype": "poem"},
        {"title": "She Hangs the Linen on the Line", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Call of Spring", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Springtime in the West", "author": "Lydia Hall", "etype": "poem"},
        {"title": "March Promises", "author": "Aileen M. Overfelt", "etype": "poem"},
        {"title": "New Things", "author": "Watson Anderson", "etype": "poem"},
    ],

    ("Vol33", "No4_April_1946"): [
        {"title": "Concerning Jesus of Nazareth", "author": None, "etype": "article"},
        {"title": "An Aria for Easter", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "The Infallible Spirit", "author": "Etta Mai Scott", "etype": "article"},
        {"title": "Our Number One Enemy Cancer", "author": "Utah Division American Cancer Society", "etype": "article"},
        {"title": "Opinions on Alcohol", "author": "Selected", "etype": "article"},
        {"title": "Magic Word", "author": "Mary Ek Knowles", "etype": "article"},
        {"title": "Digging For Happiness", "author": "Elizabeth Adamson", "etype": "article"},
        {"title": "Between the Dusk and the Dark", "author": "Alice Whitson Norton", "etype": "article"},
        {"title": "A Floral Symphony", "author": "Betti Williamson", "etype": "article"},
        {"title": "Faith Is a Heritage Chapter I", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "There Is Truth", "author": "Eva Willes Wangsgaard", "etype": "fiction"},
        {"title": "The White Picket Fence", "author": "Sarah O. Moss", "etype": "fiction"},
        {"title": "Emulating Grandmother", "author": "Linda S. Fletcher", "etype": "fiction"},
        {"title": "The Scar", "author": "Martha Robeson Wright", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Patient in Spirit", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Congratulations to President Smith", "author": None, "etype": "article"},
        {"title": "Relief Society No Longer to Share in Use of the Evenings of the Stake Quarterly Conference Sundays", "author": None, "etype": "article"},
        {"title": "Messages from the Missions (Concluded)", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Let's Make Dickies", "author": "Etta S. Robbins", "etype": "article"},
        {"title": "Before the Dawn", "author": "Olive M. Nicholes", "etype": "poem"},
        {"title": "Echoes of Hope", "author": "Ivy Houtz Woolley", "etype": "poem"},
        {"title": "Springtime Knowledge", "author": "Genevieve Van Wagenen", "etype": "poem"},
        {"title": "Father, Forgive Them", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "When He Returns", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Remembering", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "When the Choir Broadcasts", "author": "Rebecca Merrill Ostler", "etype": "poem"},
        {"title": "Aftermath", "author": "Jo Adelaide Stock", "etype": "poem"},
        {"title": "The Locust Tree", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "Voice of Cumorah", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Offerings", "author": "Marguerite J. Griffin", "etype": "poem"},
        {"title": "Apple Tree in April", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Spring Shadows", "author": "Elsie E. Barrett", "etype": "poem"},
        {"title": "Spring Sonnet", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "God Must Love the Beautiful", "author": "Mabel Jones Gabbott", "etype": "poem"},
    ],

    ("Vol33", "No5_May_1946"): [
        {"title": "Three Miniatures From Life: The Mothers of the First Presidency", "author": "Luacine C. Fox", "etype": "article"},
        {"title": "In Memoriam: President Louise Y. Robison", "author": "Kate M. Barker", "etype": "article"},
        {"title": "Women Poets in Review Part I", "author": "Anna Boss Hart", "etype": "article"},
        {"title": "Are We Protecting Our Children?", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "The Gift of the Genie", "author": "LaRene King Bleecker", "etype": "article"},
        {"title": "Filleth the Hungry Soul", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Faith Is a Heritage Chapter 2", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Return of Spring", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "In Memoriam: Pauline Taggart Pingree", "author": None, "etype": "article"},
        {"title": "The Magazine Honor Roll for 1945", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Notes From the Field", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Comments From Magazine Readers", "author": None, "etype": "article"},
        {"title": "Comments From Magazine Writers", "author": None, "etype": "article"},
        {"title": "Grosgrain Ribbon Handbag", "author": None, "etype": "article"},
        {"title": "To Louise Y. Robison Frontispiece", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "I Heard a Robin Singing", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Mother's Day Greeting", "author": "Robert Layton", "etype": "poem"},
        {"title": "Mother", "author": "Delsa Davis McBride", "etype": "poem"},
        {"title": "The Teacher", "author": "Mary E. Abel", "etype": "poem"},
        {"title": "Sunset Divine", "author": "Pauline Kirkwood", "etype": "poem"},
        {"title": "Mother of Mine", "author": "Vernessa M. Nagle", "etype": "poem"},
        {"title": "Relief Society Magazine May", "author": "Weight Johnson", "etype": "poem"},
        {"title": "Mother's Notebook", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Meadow Lark", "author": "Roberta Bates", "etype": "poem"},
    ],

    ("Vol33", "No6_June_1946"): [
        {"title": "Fasting", "author": "Priscilla L. Evans", "etype": "article"},
        {"title": "Isabel Barton Callister Called to General Board", "author": "Lillie C. Adams", "etype": "article"},
        {"title": "Contest Announcements 1946", "author": None, "etype": "article"},
        {"title": "The Settlement of Sanpete Valley Part I", "author": "Leda Thompson Jensen", "etype": "article"},
        {"title": "Women Poets in Review Part II", "author": "Anna Boss Hart", "etype": "article"},
        {"title": "A Compliment for Mom", "author": "Olive W. Burt", "etype": "article"},
        {"title": "The Price", "author": "Marjorie M. Hamrick", "etype": "article"},
        {"title": "Choosing Roses for the Home Garden", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "The 116th Annual General Conference", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The Bennetts' Bookkeeping System", "author": "Alice Whitson Norton", "etype": "article"},
        {"title": "Only Human", "author": "Marguerite J. Griffin", "etype": "article"},
        {"title": "The Price", "author": "T. A. Hunt", "etype": "article"},
        {"title": "Faith Is a Heritage Chapter 3", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Enlarge Your Souls", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "In Memoriam: Alice Robinson", "author": None, "etype": "article"},
        {"title": "General Activities and Special Programs", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Mesh-Sack Magic", "author": None, "etype": "article"},
        {"title": "For the Coming Night Frontispiece", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Dott J. Sartori", "author": None, "etype": "poem"},
        {"title": "Jewel Box", "author": "Jessie Miller Robinson", "etype": "poem"},
        {"title": "Sanctuary", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "My Father", "author": "Jennie Pyper Johnson", "etype": "poem"},
        {"title": "Peace in a Garden", "author": "Celia A. Van Cott", "etype": "poem"},
        {"title": "Summer Song", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "The Origin of Song", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "Class Reunion", "author": "Olive M. Nicholes", "etype": "poem"},
        {"title": "Little Quails Take to the Woods", "author": "Lydia Hall", "etype": "poem"},
        {"title": "Mothers of This Day", "author": "Gene Romolo", "etype": "poem"},
    ],

    ("Vol33", "No7_July_1946"): [
        {"title": "Opportunities at Brigham Young University for the Youth of the Church", "author": "Howard S. McDonald", "etype": "article"},
        {"title": "John H. Taylor", "author": "Richard L. Evans", "etype": "article"},
        {"title": "Pioneer Stories and Incidents: Indians on the Plains", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "The Settlement of Sanpete Valley Part II", "author": "Leda Thompson Jensen", "etype": "article"},
        {"title": "Summer Blossoms for the Home", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Grain for the Food of Man", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Rag Rug Weaving in Emigration Stake", "author": "Lillie C. Adams", "etype": "article"},
        {"title": "Notes on the Authors of the Lessons", "author": "Leone G. Layton", "etype": "article"},
        {"title": "Faith Is a Heritage Chapter 4", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The Pioneering Heritage", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Congratulations to Sister Augusta Winters Grant", "author": None, "etype": "article"},
        {"title": "The Course of Study for 1946-1947", "author": None, "etype": "article"},
        {"title": "General Activities and Special Programs (Continued)", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Suggestions and Instructions on the Educational Program 1946-1947", "author": None, "etype": "article"},
        {"title": "The Organized Church Moves Forward", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Love of God", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "Choosing the Right Clothes", "author": "Work Meeting Committee", "etype": "article"},
        {"title": "Our American Indians in Literature", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Man's Pre-Earth Life", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "The Family Unit Eternal", "author": "Social Science Committee", "etype": "lesson"},
        {"title": "Only the Believer Frontispiece", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Abundance", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Restoration", "author": "Grace D. Terrell", "etype": "poem"},
        {"title": "One Family", "author": "Maude Blixt Trone", "etype": "poem"},
        {"title": "Acres of Wheat", "author": "Gene Romolo", "etype": "poem"},
        {"title": "These Things I Know", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Please, God", "author": "Edna S. Dustin", "etype": "poem"},
        {"title": "Desert Beauty", "author": "Ethel VanHook Hopper", "etype": "poem"},
        {"title": "To a Great Leader", "author": "Bertha M. Rosevear", "etype": "poem"},
    ],

    ("Vol33", "No8_August_1946"): [
        {"title": "The Importance of Thrift", "author": "Alma Sonne", "etype": "article"},
        {"title": "Cache Valley Part of the Mormon Epic", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Pioneer Stories and Incidents: The Indians of Utah and the Black Hawk War", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Garden Making for Centennial Days", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Photographing Your Children", "author": "Willard Luce", "etype": "article"},
        {"title": "Heavenly Harmony", "author": "Rachel B. Ballantyne", "etype": "article"},
        {"title": "Menus By the Day Or By the Week", "author": "Miranda Snow Walton", "etype": "article"},
        {"title": "Night Callers", "author": "Gladys Dewey", "etype": "fiction"},
        {"title": "Faith Is a Heritage Chapter 5", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Personal Development Through Service", "author": "Gertrude R. Garff", "etype": "editorial"},
        {"title": "May Anderson Friend of the Children", "author": None, "etype": "article"},
        {"title": "Annual Relief Society General Conference", "author": None, "etype": "article"},
        {"title": "General Activities and Special Programs (Continued)", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The Priesthood Quorums Expanded", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Love of Fellow Man", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "The Use of Tools and Treatment of Materials", "author": "Work Meeting Committee", "etype": "article"},
        {"title": "Colonial Founders of America in Literature", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "The Mayflower Compact", "author": None, "etype": "article"},
        {"title": "The General Gospel Plan", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "The Family Organization", "author": "Social Science Committee", "etype": "lesson"},
        {"title": "And What of Time Frontispiece", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Self-Confidence", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "Skull Valley", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Jessie Miller Robinson", "author": None, "etype": "poem"},
        {"title": "The Silent Bard", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "The New Testament", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Irrigation", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Priceless Gem", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "The Poplars", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Iris", "author": "Schow", "etype": "poem"},
        {"title": "Late Traveler", "author": "G. Gwen Kelsey", "etype": "poem"},
    ],

    ("Vol33", "No9_September_1946"): [
        {"title": "A Message to Teachers", "author": "M. Lynn Bennion", "etype": "article"},
        {"title": "Fort Lemhi Mission, Idaho Pioneer in Review", "author": "Dorothy Clapp Robinson", "etype": "article"},
        {"title": "Pioneer Stories and Incidents: The Indians and the Mail Route", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "There Is Still Life", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "A Shelter for Their Hearts", "author": "Maryhale Woolsey", "etype": "fiction"},
        {"title": "Faith Is a Heritage Chapter 6", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Records of Service", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Ward Relief Society Conferences", "author": None, "etype": "article"},
        {"title": "Bound Volumes", "author": None, "etype": "article"},
        {"title": "United States Savings Bonds", "author": "George Albert Smith", "etype": "article"},
        {"title": "Annual Report 1945", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Notes From the Field", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The Relief Society Developed", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Faith", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "Fitting and Adjusting Patterns and Cutting", "author": "Work Meeting Committee", "etype": "article"},
        {"title": "America's First War for Freedom", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Faith", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Autumn Hills Frontispiece", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "Three Precious Things", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Clarence Edwin Flynn", "author": None, "etype": "poem"},
        {"title": "Fall Days Are Closing", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Fulfillment", "author": "Verda P. Bollschweiler", "etype": "poem"},
        {"title": "His Silences", "author": "Blanche Kendall McKey", "etype": "poem"},
        {"title": "Song", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Autumn Has Spoken", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Heart's Reply", "author": "Anna Prince Redd", "etype": "poem"},
    ],

    ("Vol33", "No10_October_1946"): [
        {"title": "Pioneer Preparations for Winter", "author": "Ann P. Nibley", "etype": "article"},
        {"title": "Three Natural Bridges of Utah", "author": "Willard Luce", "etype": "article"},
        {"title": "Pioneer Stories and Incidents: A Stampede on the Plains", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Painted Weeds from Fairyland", "author": "Rosella F. Larkin", "etype": "article"},
        {"title": "Golden Harvest", "author": "Beatrice Rordame Parsons", "etype": "article"},
        {"title": "The Intruder", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Faith Is a Heritage Chapter 7", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sallie-Start-Something", "author": "Alice Whitson Norton", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The Importance of Child Training", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Regulations Governing the Submittal of Material for Notes From the Field", "author": None, "etype": "article"},
        {"title": "The Educational System Developed", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Courage", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "Stitches and Seams", "author": "Work Meeting Committee", "etype": "article"},
        {"title": "Youthtime of a Nation Dedicated to Freedom", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Repentance", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Social Science Committee", "author": None, "etype": "lesson"},
        {"title": "Yesterdays Frontispiece", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "God Speed the Plow", "author": "Anonymous", "etype": "poem"},
        {"title": "The Meaning of Autumn", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Our Goal", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "Nothing Is Lost", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Crocheting", "author": "Merling Dennis Clyde", "etype": "poem"},
        {"title": "Triolet", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Parade", "author": "Courtney Cottam", "etype": "poem"},
    ],

    ("Vol33", "No11_November_1946"): [
        {"title": "Relief Society Women Received at the White House", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Inter-Continental Conference of the National Council of Women", "author": "Priscilla L. Evans", "etype": "article"},
        {"title": "A Visit to Church Shrines", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "Pioneer Stories and Incidents: On the River Platte", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "What Shall I Do Now, Mother?", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "The Luxury of Giving", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "As You Were", "author": "Martha Robeson Wright", "etype": "fiction"},
        {"title": "Faith Is a Heritage Chapter 8", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "She Looketh Well to the Ways of Her Household", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "More Auxiliary Organizations of the Church Started", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Industry", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "Pinning, Basting, Pressing, Edges and Seam Finishings", "author": "Work Meeting Committee", "etype": "article"},
        {"title": "Westward March of America", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Baptism", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Observing a Principle of Prosperity", "author": "Social Science Committee", "etype": "lesson"},
        {"title": "Where Dreams Were Made Frontispiece", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Words", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Keys", "author": "Clarence Edwin Flynn", "etype": "poem"},
        {"title": "Night", "author": "Bernice Hacking Balle", "etype": "poem"},
        {"title": "Autumn Wind", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "The Jungle Tree", "author": "Elsie F. Parton", "etype": "poem"},
        {"title": "Friendly Town", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Consolation", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Hill-Top House", "author": "Beatrice K. Ekman", "etype": "poem"},
    ],

    ("Vol33", "No12_December_1946"): [
        {"title": "Our Wives and Our Mothers in the Eternal Plan", "author": "J. Reuben Clark, Jr.", "etype": "article"},
        {"title": "Hearken to Counsel", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "The Place of Relief Society in the Welfare Plan", "author": "Harold B. Lee", "etype": "article"},
        {"title": "Bruce Redd McConkie Sustained a Member of the First Council of Seventy", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Marvin O. Ashton", "author": "Bryant S. Hinckley", "etype": "article"},
        {"title": "Christmas and the True Christian Spirit", "author": "T. Edgar Lyon", "etype": "article"},
        {"title": "Christmas on the Mantel", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Prelude to Christmas", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Faith Is a Heritage Chapter 9", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "A Tie That Binds", "author": "Gertrude R. Garff", "etype": "editorial"},
        {"title": "In Memoriam: Grace Elizabeth Pack Callis", "author": None, "etype": "article"},
        {"title": "Organizations and Reorganizations of Mission and Stake Relief Societies", "author": None, "etype": "article"},
        {"title": "The Welfare Plan Initiated", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Self Reliance", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "Finishing, Trimming, Collars, Bands, Hems", "author": "Work Meeting Committee", "etype": "article"},
        {"title": "Carrying America Across a Continent", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Laying On of Hands for the Gift of the Holy Ghost", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Importance of Religious Instruction in the Home", "author": "Social Science Committee", "etype": "lesson"},
        {"title": "The Traveler Frontispiece", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "The House of Bread", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "Christmas-Eve Magic", "author": "Mary J. Morris", "etype": "poem"},
        {"title": "The Grandmother", "author": "Theoda Hart Struthers", "etype": "poem"},
        {"title": "Dressing a Doll", "author": "Jeanette P. Parry", "etype": "poem"},
        {"title": "Precious Seasons", "author": "Maud Blixt Trone", "etype": "poem"},
        {"title": "Not Comfortless", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Christmas Cards", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Dorothy J. Roberts", "author": None, "etype": "poem"},
    ],

    # ===================================================================
    # VOLUME 34 (1947)
    # ===================================================================

    ("Vol34", "No1_January_1947"): [
        {"title": "The New Year", "author": "General Presidency of Relief Society", "etype": "article"},
        {"title": "Unveiling of the Portrait of President Belle S. Spafford", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "A New Latter-day Saint Artist", "author": "Gerrit de Jong, Jr.", "etype": "article"},
        {"title": "Award Winners Eliza Roxey Snow Memorial Prize Poem Contest", "author": None, "etype": "article"},
        {"title": "Our Hands in Thine", "author": "Ethel Newman Eccles", "etype": "poem"},
        {"title": "Release From the South Seas", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Centennial Conversation", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "Award Winners Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "The Return", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Mary Jacobs Wilson Called to General Board", "author": "Maurine C. Neilsen", "etype": "article"},
        {"title": "Florence Gay Smith", "author": "Blanche B. Stoddard", "etype": "article"},
        {"title": "Lillie Chipman Adams", "author": "Isabel B. Callister", "etype": "article"},
        {"title": "General Relief Society Conference", "author": None, "etype": "article"},
        {"title": "The Final Year in the Church History Course", "author": "H. Wayne Driggs", "etype": "article"},
        {"title": "The Worth of Testimony Bearing", "author": "Achsa E. Paxman", "etype": "article"},
        {"title": "Our Pioneer Heritage", "author": "Ann P. Nibley", "etype": "article"},
        {"title": "The Sewing Course", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "The Gospel as a Way of Life", "author": "Priscilla L. Evans", "etype": "article"},
        {"title": "Congregational Singing and Song Practice", "author": "Blanche B. Stoddard", "etype": "article"},
        {"title": "The Importance of Music in Relief Society", "author": "Florence J. Madsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Tribute to Sister Louise Y. Robison", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Faith Is a Heritage Chapter 10", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "The New Frontier", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Unsung Heroes in Zion's Cause", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Dependability", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "Buttonholes and Fasteners", "author": "Work Meeting Committee", "etype": "article"},
        {"title": "America Through Testing Years", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "The Abundant Life, Here and Hereafter", "author": "T. Edgar Lyon", "etype": "lesson"},
        {"title": "Constructive Use of Time", "author": "Social Science Committee", "etype": "lesson"},
        {"title": "Tomorrow Frontispiece", "author": "Berta Huish Christensen", "etype": "poem"},
        {"title": "New Year's Resolution", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Pioneer Women", "author": "Olive W. Burt", "etype": "poem"},
        {"title": "Winter Night", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "Birds", "author": "Marguerite Kirkham", "etype": "poem"},
        {"title": "A Mother to Her Babe", "author": "Roxana F. Hase", "etype": "poem"},
        {"title": "Winter Bouquet", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Certitude", "author": "Dorothy J. Roberts", "etype": "poem"},
    ],

    # ===================================================================
    # VOLUME 35 (1948)
    # ===================================================================

    ("Vol35", "No1_January_1948"): [
        {"title": "Happy New Year!", "author": "General Presidency of Relief Society", "etype": "article"},
        {"title": "Pioneer Women", "author": "David O. McKay", "etype": "article"},
        {"title": "Award Winners Eliza Roxey Snow Memorial Prize Poem Contest", "author": None, "etype": "article"},
        {"title": "The Cherry Orchard", "author": "Katherine Fernelius Larsen", "etype": "poem"},
        {"title": "Migrant", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Ruby Baird Andersen", "author": None, "etype": "article"},
        {"title": "Alice Morrey Bailey", "author": None, "etype": "article"},
        {"title": "Dorothy Ducas", "author": None, "etype": "article"},
        {"title": "Bertha Zaugg Perschon", "author": None, "etype": "article"},
        {"title": "Sara Mills", "author": None, "etype": "article"},
        {"title": "Olive W. Burt", "author": None, "etype": "article"},
        {"title": "Anna Prince Redd", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Warm Hearts and Friendly Hands", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Relief Society Assigned Evening Meeting of Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Watch This Month", "author": None, "etype": "article"},
        {"title": "Three-Part Story (Windy Hilltop) to begin in February", "author": None, "etype": "article"},
        {"title": "Relief Society Centennial Programs and Other Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Honored by Strangers, Rejected by His Own; Continuation of Our Lord's Ministry in Galilee", "author": "Don B. Colton", "etype": "lesson"},
        {"title": "Building Activities", "author": "Amy Brown Lyman", "etype": "lesson"},
        {"title": "Fitted Facings", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Doctrinal Exposition of the Doctrine and Covenants", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Social Science", "author": "Joseph Jacobs", "etype": "lesson"},
        {"title": "Snow Upon the Earth Frontispiece", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Earth's Song of the Road", "author": "C. Cameron Johns", "etype": "poem"},
        {"title": "A New Year", "author": "Clarence Edwin Flynn", "etype": "poem"},
        {"title": "Symphony", "author": "Beatrice Rordame Parsons", "etype": "poem"},
        {"title": "Caught Napping", "author": "Grace A. Woodbury", "etype": "poem"},
        {"title": "My Wealth", "author": "Julia Nelson", "etype": "poem"},
        {"title": "Della Adams Leitner", "author": None, "etype": "poem"},
    ],
}
# ---------------------------------------------------------------------------
# Filename mapping: issue key -> (source filename, month name for output)
# ---------------------------------------------------------------------------
ISSUE_FILES = {}

# Volume 33
for no, month, year in [
    ("No1", "January", "1946"), ("No2", "February", "1946"),
    ("No3", "March", "1946"), ("No4", "April", "1946"),
    ("No5", "May", "1946"), ("No6", "June", "1946"),
    ("No7", "July", "1946"), ("No8", "August", "1946"),
    ("No9", "September", "1946"), ("No10", "October", "1946"),
    ("No11", "November", "1946"), ("No12", "December", "1946"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"vol33_{key}.txt"
    ISSUE_FILES[("Vol33", key)] = (fname, month)

# Volume 34
for no, month, year in [
    ("No1", "January", "1947"), ("No2", "February", "1947"),
    ("No3", "March", "1947"), ("No4", "April", "1947"),
    ("No5", "May", "1947"), ("No6", "June", "1947"),
    ("No7", "July", "1947"), ("No8", "August", "1947"),
    ("No9", "September", "1947"), ("No10", "October", "1947"),
    ("No11", "November", "1947"), ("No12", "December", "1947"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"vol34_{key}.txt"
    ISSUE_FILES[("Vol34", key)] = (fname, month)

# Volume 35
for no, month, year in [
    ("No1", "January", "1948"), ("No2", "February", "1948"),
    ("No3", "March", "1948"), ("No4", "April", "1948"),
    ("No5", "May", "1948"), ("No6", "June", "1948"),
    ("No7", "July", "1948"), ("No8", "August", "1948"),
    ("No9", "September", "1948"), ("No10", "October", "1948"),
    ("No11", "November", "1948"), ("No12", "December", "1948"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"vol35_{key}.txt"
    ISSUE_FILES[("Vol35", key)] = (fname, month)

# Volume 36
for no, month, year in [
    ("No01", "January", "1949"), ("No02", "February", "1949"),
    ("No03", "March", "1949"), ("No04", "April", "1949"),
    ("No05", "May", "1949"), ("No06", "June", "1949"),
    ("No07", "July", "1949"), ("No08", "August", "1949"),
    ("No09", "September", "1949"), ("No10", "October", "1949"),
    ("No11", "November", "1949"), ("No12", "December", "1949"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol36_{key}.txt"
    ISSUE_FILES[("Vol36", key)] = (fname, month)

# Volume 37
for no, month, year in [
    ("No01", "January", "1950"), ("No02", "February", "1950"),
    ("No03", "March", "1950"), ("No04", "April", "1950"),
    ("No05", "May", "1950"), ("No06", "June", "1950"),
    ("No07", "July", "1950"), ("No08", "August", "1950"),
    ("No09", "September", "1950"), ("No10", "October", "1950"),
    ("No11", "November", "1950"), ("No12", "December", "1950"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol37_{key}.txt"
    ISSUE_FILES[("Vol37", key)] = (fname, month)

# Volume 38
for no, month, year in [
    ("No01", "January", "1951"), ("No02", "February", "1951"),
    ("No03", "March", "1951"), ("No04", "April", "1951"),
    ("No05", "May", "1951"), ("No06", "June", "1951"),
    ("No07", "July", "1951"), ("No08", "August", "1951"),
    ("No09", "September", "1951"), ("No10", "October", "1951"),
    ("No11", "November", "1951"), ("No12", "December", "1951"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol38_{key}.txt"
    ISSUE_FILES[("Vol38", key)] = (fname, month)

# Volume 39
for no, month, year in [
    ("No01", "January", "1952"), ("No02", "February", "1952"),
    ("No03", "March", "1952"), ("No04", "April", "1952"),
    ("No05", "May", "1952"), ("No06", "June", "1952"),
    ("No07", "July", "1952"), ("No08", "August", "1952"),
    ("No09", "September", "1952"), ("No10", "October", "1952"),
    ("No11", "November", "1952"), ("No12", "December", "1952"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol39_{key}.txt"
    ISSUE_FILES[("Vol39", key)] = (fname, month)

# Volume 40
for no, month, year in [
    ("No01", "January", "1953"), ("No02", "February", "1953"),
    ("No03", "March", "1953"), ("No04", "April", "1953"),
    ("No05", "May", "1953"), ("No06", "June", "1953"),
    ("No07", "July", "1953"), ("No08", "August", "1953"),
    ("No09", "September", "1953"), ("No10", "October", "1953"),
    ("No11", "November", "1953"), ("No12", "December", "1953"),
]:
    key = f"{no}_{month}_{year}"
    fname = f"Vol40_{key}.txt"
    ISSUE_FILES[("Vol40", key)] = (fname, month)


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
    in every issue of Vol 33-40 at the end of the front matter section.

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
        description="Extract Relief Society Magazine Vol 33-40 into individual entries"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without creating files")
    parser.add_argument("--volume", type=int, choices=[33, 34, 35, 36, 37, 38, 39, 40],
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
