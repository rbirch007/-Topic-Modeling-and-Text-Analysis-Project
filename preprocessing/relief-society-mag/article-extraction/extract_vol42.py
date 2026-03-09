#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 42 (1955).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol42.py
    python extract_vol42.py --dry-run
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

VOL42_TOC = {
    ("Vol42", "No01_January_1955"): [
        {"title": "Greetings for the New Year", "author": None, "etype": "article"},
        {"title": "Relief Society Women As Home Missionaries", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Award Winners -- Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Three Scenes in Oil -- First Prize Poem", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "My Case -- Second Prize", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Dedication -- Third Prize", "author": "Hortense Richardson", "etype": "poem"},
        {"title": "Award Winners -- Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Wallflower -- First Prize Story", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Infantile Paralysis and the March of Dimes", "author": "Basil O'Connor", "etype": "article"},
        {"title": "Faith and Prayer", "author": "Maryhale Woolsey", "etype": "fiction"},
        {"title": "Grandma's Responsibility", "author": "LaVina Fugal Martineau", "etype": "fiction"},
        {"title": "Contentment Is a Lovely Thing, Chapter 4", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Morning and the New Year", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Mimosa", "author": None, "etype": "article"},
        {"title": "There Is a Time for Everything", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Bathroom Tricks: Novel Towels", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Her Hobbies Bring Joy to Others", "author": "Mary Elizabeth Jensen", "etype": "article"},
        {"title": "Theology: Helaman, Son of Alma, and His Two Thousand Sons", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For That Which Ye Do Send Out Shall Return Unto You Again, and Be Restored", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Vacuums", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Adam Bede by George Eliot", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States, Articles XI-XV -- Amendment Eleven", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Let Me Then Answer", "author": "Frances C. Yost", "etype": "poem"},
        {"title": "Winter Song", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "Driftwood", "author": "Natalie King", "etype": "poem"},
        {"title": "Before the Storm", "author": "Zara Sabin", "etype": "poem"},
        {"title": "White World", "author": "Gene Romolo", "etype": "poem"},
        {"title": "The Difference", "author": "Inga Smith", "etype": "poem"},
        {"title": "On Measuring", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "New Year's Prayer", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Playtime Is Over", "author": "Ivy Houtz Woolley", "etype": "poem"},
    ],

    ("Vol42", "No02_February_1955"): [
        {"title": "Poet's Mother", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Relief Society and the General Church Welfare Program", "author": "Henry D. Moyle", "etype": "article"},
        {"title": "Mama's Plant", "author": "Klea Evans Worsley", "etype": "article"},
        {"title": "A Home for Holly -- Second Prize Story", "author": "Mabel S. Harmer", "etype": "fiction"},
        {"title": "Green Willows, Chapter 1", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "A Shadowy Form Passed the Window", "author": "Rose A. Openshaw", "etype": "fiction"},
        {"title": "Contentment Is a Lovely Thing, Chapter 5", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Take Time to Safeguard Children", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Birthday Greetings to Former President Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Block and Applique Quilts", "author": "Velma MacKay Paul", "etype": "article"},
        {"title": "Amelia Elizabeth H. Jackson Pieces Quilts and Makes Lampshades", "author": None, "etype": "article"},
        {"title": "Theology: Peace Comes to the Nephites Through Righteousness", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: My Soul Standeth Fast in That Liberty in the Which God Hath Made Us Free", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Utensils for Surface Cookery", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Matthew Arnold", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States, Amendments Sixteen Through Twenty-Two", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Valentines for Mother", "author": "Bernice T. Clayton", "etype": "poem"},
        {"title": "Mountain Peak", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "River of Moses", "author": "Olive Carman", "etype": "poem"},
        {"title": "February Moon", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "The Unanswerable", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Winter Afternoon", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol42", "No03_March_1955"): [
        {"title": "O Be Wise; What Can I Say More?", "author": "Aleine M. Young", "etype": "article"},
        {"title": "Now, in the Twilight of My Life", "author": "Artemesia R. Romney", "etype": "article"},
        {"title": "A Great Tradition -- The American National Red Cross", "author": "Edwin H. Powers", "etype": "article"},
        {"title": "Why Not Be Happy?", "author": "Celia Luce", "etype": "article"},
        {"title": "Nature's Bouquet", "author": "Cecil G. Pugmire", "etype": "article"},
        {"title": "Survival Under Protest -- Third Prize Story", "author": "Leola S. Anderson", "etype": "fiction"},
        {"title": "The Legacy", "author": "Ora Pate Stewart", "etype": "fiction"},
        {"title": "Mother's Baked Apple", "author": "Estelle Webb Thomas", "etype": "fiction"},
        {"title": "Green Willows, Chapter 2", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Eighty-One Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Relief Society for the Perfection of Women", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Perennials Preferred", "author": "Dorthea N. Newbold", "etype": "article"},
        {"title": "And Now It's Spring Again!", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Herbs for Modern Cookery -- Dill", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Of Power and Love", "author": "Genevieve Wyatt", "etype": "poem"},
        {"title": "Let There Be Beauty", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Grandfather's Peppermints", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Let Seasons Linger", "author": "Iris W. Schow", "etype": "poem"},
    ],

    ("Vol42", "No04_April_1955"): [
        {"title": "First to See the Risen Lord", "author": "Vesta P. Crawford", "etype": "poem"},
        {"title": "The Resurrection", "author": "George Q. Morris", "etype": "article"},
        {"title": "Land of the Water Birds", "author": "Willard Luce", "etype": "article"},
        {"title": "We Serve As God's Hands", "author": "Caroline E. Miner", "etype": "article"},
        {"title": "Cancer -- A Quiz That May Save Your Life", "author": "Sandra Munsell", "etype": "article"},
        {"title": "The Lower Hills", "author": "Lucille Waters Mattson", "etype": "article"},
        {"title": "A Handful of Dirt", "author": "Vivian Campbell Work", "etype": "article"},
        {"title": "An Understanding Heart", "author": "Anne S. W. Gould", "etype": "article"},
        {"title": "The Ruby-Throated Hummingbird", "author": "Roy B. McClain", "etype": "article"},
        {"title": "Reap, If You Will", "author": "Elaine J. Wilson", "etype": "fiction"},
        {"title": "The Wall", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "Steak for Thursday", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "Her Own Life", "author": "Ruth Moody Ostegar", "etype": "fiction"},
        {"title": "Green Willows, Chapter 3", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Appreciation of the Gospel", "author": "Velma N. Simonsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Lilies -- 1955", "author": "Dorthea N. Newbold", "etype": "article"},
        {"title": "The Hex Party", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Heirloom Quilt Presented to Missionaries at Carthage Jail", "author": "Josephine Brower", "etype": "article"},
        {"title": "Eggshells for the Garden", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "So Long As Springtime Comes", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "At Easter Dawn", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Blue-Blossomed Jacaranda", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Moment of Music", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Friendship's Garden", "author": "Gene Romolo", "etype": "poem"},
    ],

    ("Vol42", "No05_May_1955"): [
        {"title": "Where Lilacs Grew", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Mother", "author": "Elna P. Haymond", "etype": "article"},
        {"title": "Contest Announcements -- 1955", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "On Writing the Short Story", "author": "Pansye H. Powell", "etype": "article"},
        {"title": "Forever Orchid", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Highly Organized", "author": "Dorothy Boys Kilian", "etype": "fiction"},
        {"title": "Hurrah for Pete!", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Green Willows, Chapter 4", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Word of Appreciation", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Magazine Subscriptions for 1954", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Magazine Honor Roll for 1954", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Designing Original Applique and Block Quilts", "author": "Velma MacKay Paul", "etype": "article"},
        {"title": "Herbs for Modern Cookery -- Chives", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Cement Chimney Blocks as Planting Boxes", "author": "Willard Luce", "etype": "article"},
        {"title": "First Friend", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Lifted Wall", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Between the Bud and the Fruit", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "My Magazine", "author": "Mabel M. Tanner", "etype": "poem"},
        {"title": "On Washdays", "author": "June B. Wunderlich", "etype": "poem"},
        {"title": "Suddenly Butterflies", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Father's Garden", "author": "Bernice T. Clayton", "etype": "poem"},
    ],

    ("Vol42", "No06_June_1955"): [
        {"title": "Eyes of Spring", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "My Daughter Prepares for Marriage", "author": "Harold B. Lee", "etype": "article"},
        {"title": "Annie Merrill Ellsworth", "author": "Zina H. Poole", "etype": "article"},
        {"title": "Mary Ross Young", "author": "Elna P. Haymond", "etype": "article"},
        {"title": "Poetry -- A Rich Heritage", "author": "Christie Lund Coles", "etype": "article"},
        {"title": "Blossoms for the Table", "author": "Willard Luce", "etype": "article"},
        {"title": "How Can It Please the Human Pride?", "author": "Caroline E. Miner", "etype": "article"},
        {"title": "Selling the Relief Society Magazine", "author": "Edith G. Baum", "etype": "article"},
        {"title": "The Morning-Glory Horn", "author": "Nell Murbarger", "etype": "article"},
        {"title": "A Good Day", "author": "Margaret Hardy", "etype": "article"},
        {"title": "A Good Life", "author": "Vera Mayhew", "etype": "fiction"},
        {"title": "First in My Heart", "author": "Maryhale Woolsey", "etype": "fiction"},
        {"title": "Green Willows, Chapter 5", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 125th Annual Church Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "From Sea to Shining Sea", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Let Each Day Be a Pleasant Day for You", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Herbs for Modern Cookery -- Basil", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Teaching and Teaching Aids for the 1955-56 Lessons", "author": "Mildred B. Eyring", "etype": "article"},
        {"title": "It Is a Tragic Thing", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "The Covered Bridge", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "While Summer Sings", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Symphony for America", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Hills", "author": "Francelia Goddard", "etype": "poem"},
        {"title": "The Meadow", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Blue River", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Monday Song", "author": "Faye Gardner", "etype": "poem"},
        {"title": "Weeds", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Return", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Enchanted Moment", "author": "Elizabeth Pew", "etype": "poem"},
        {"title": "Familiar Note", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "It Doesn't Matter", "author": "Josephine H. Beck", "etype": "poem"},
        {"title": "Reward", "author": "Ruth M. Jones", "etype": "poem"},
        {"title": "Nature Song", "author": "Jeanne Wilson", "etype": "poem"},
        {"title": "Courageous Weaver", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Lamp of Faith", "author": "Erma Barney Braack", "etype": "poem"},
        {"title": "Night in the Mountains", "author": "Maude O. Cook", "etype": "poem"},
    ],

    ("Vol42", "No07_July_1955"): [
        {"title": "They Came With Song", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "The Documents of Freedom", "author": "Louis H. Callister", "etype": "article"},
        {"title": "Your Journal Tells Me", "author": "Melba S. Payne", "etype": "article"},
        {"title": "Wealth", "author": "Alma Weixelbaum", "etype": "article"},
        {"title": "Bringing Home the Cows", "author": "Nell Murbarger", "etype": "article"},
        {"title": "Kneel to Your Child", "author": "Christie Lund Coles", "etype": "article"},
        {"title": "Hermanas, Chapter 1", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Strength", "author": "Edith Larson", "etype": "fiction"},
        {"title": "April's Pennies", "author": "Arlene D. Cloward", "etype": "fiction"},
        {"title": "Green Willows, Chapter 6", "author": "Deone R. Sutherland", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Towers of Liberty", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "In Memoriam -- Ida Smoot Dusenberry", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Quilting", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Red Currants Are Ripe!", "author": "Helen S. Williams", "etype": "article"},
        {"title": "The Jump Rug", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Review -- The Book of Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For, If Ye Forgive Men Their Trespasses", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Family Meal Service", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Robert Louis Stevenson, Personality and Poet", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution Established", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Today We Picked the Currant Crop", "author": "Zara Sabin", "etype": "poem"},
        {"title": "A Tribute to the Singing Mothers", "author": "Ida L. Belnap", "etype": "poem"},
        {"title": "Walk Beside Me", "author": "Josephine H. Beck", "etype": "poem"},
        {"title": "Boy Wanted", "author": "Elsie McKinnon Strachan", "etype": "poem"},
    ],

    ("Vol42", "No08_August_1955"): [
        {"title": "The Mountain Climber", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "The Family Hour in Our Home", "author": "LaRue S. Pettit", "etype": "article"},
        {"title": "A Home Where Past and Present Meet", "author": "Dorothy J. Roberts", "etype": "article"},
        {"title": "Tumbleweeds", "author": "Elsie Scott", "etype": "article"},
        {"title": "Chokecherries Meant Adventure", "author": "Nell Murbarger", "etype": "article"},
        {"title": "An Invisible Means of Support", "author": "Caroline E. Miner", "etype": "article"},
        {"title": "Look to the Mountains", "author": "Blanche Johnson", "etype": "article"},
        {"title": "Trouble", "author": "Lucille R. Taylor", "etype": "article"},
        {"title": "A Song of the Heart", "author": "Elsie Sim Hansen", "etype": "article"},
        {"title": "Special Birthday, Part I", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Meet Mother, Jody", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "The Least Clearing", "author": "Florence B. Dunford", "etype": "fiction"},
        {"title": "Hermanas, Chapter 2", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Family Patterns", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Do You Serve Five-Star Meals to Your Family?", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Whole-Wheat Bread", "author": "Jessie Nellis", "etype": "article"},
        {"title": "The Cat Rug", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Watchers", "author": "Kate Richards", "etype": "article"},
        {"title": "Theology: Political and Social Disintegration", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: But Thou, When Thou Prayest", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Meal Planning", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Stevenson's Prose", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Living Constitution", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "The Land Is Still", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Down Summer Lanes", "author": "Gertrude T. Kovan", "etype": "poem"},
        {"title": "Would You Find Peace?", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Three Little Sisters", "author": "Dora Toone Brough", "etype": "poem"},
        {"title": "They Pass in Review", "author": "Eva Cordery", "etype": "poem"},
    ],

    ("Vol42", "No09_September_1955"): [
        {"title": "To the Framers of the Constitution", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "A Tribute to President David O. McKay", "author": "Elizabeth Hill Boswell", "etype": "article"},
        {"title": "The Influence of Relief Society in the Home", "author": "Marion D. Hanks", "etype": "article"},
        {"title": "Poem in the Tetons", "author": "Willard Luce", "etype": "article"},
        {"title": "Harvest Festival", "author": "Nell Murbarger", "etype": "article"},
        {"title": "The First Prayer Made in the Congress of the United States", "author": None, "etype": "article"},
        {"title": "A Table Decoration for Social Science Day", "author": "Inez R. Allen", "etype": "article"},
        {"title": "Annual Report for 1954", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The Bell", "author": "Celia Luce", "etype": "article"},
        {"title": "Apple Polishing", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Special Birthday, Part II", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Hermanas, Chapter 3", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Anxiously Engaged in a Good Cause", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Bulbs for Spring Glory", "author": "Dorthea N. Newbold", "etype": "article"},
        {"title": "There's a Poultry Dish for Every Occasion", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Glamorize Spectacle Cases", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Nephi, Son of Helaman", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: But When Thou Doest Alms", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Poultry Selection and Preparation", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: John Millington Synge and the Irish Theater", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "My Testament", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Homeward Turning", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Pulse of Peace", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Gathering Hour", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol42", "No10_October_1955"): [
        {"title": "Reflections", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Revelation in Our Personal Affairs", "author": "Marion G. Romney", "etype": "article"},
        {"title": "The Great White Sands", "author": "Nell Murbarger", "etype": "article"},
        {"title": "For This Cause", "author": "Emily Wilkerson", "etype": "article"},
        {"title": "Thou Art Thy Mother's Glass", "author": "Helen B. Morris", "etype": "article"},
        {"title": "A Mirror for Virginia", "author": "Helen Hinckley Jones", "etype": "fiction"},
        {"title": "Just Like the Ones I Used to Know", "author": "Pansye H. Powell", "etype": "fiction"},
        {"title": "Hermanas, Chapter 4", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Valiant Womanhood", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "African Violets for Your Home", "author": "Shirley Seegmiller", "etype": "article"},
        {"title": "Make Soup Often", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Wearing a Pretty Face", "author": "Mabel Law Atkinson", "etype": "article"},
        {"title": "A Hooked Rug for Girls or Boys", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Predictions Concerning the Coming of the Messiah", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Therefore, Blessed Are They Who Will Repent and Hearken Unto the Voice of the Lord", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Soups", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Kipling, the Poet of Empire", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution and the Courts", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "October Afternoon", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Old Book", "author": "Gertrude T. Kovan", "etype": "poem"},
        {"title": "How to Make a Dream Come Right", "author": "Ethelyn M. Kincher", "etype": "poem"},
        {"title": "Autumnal Equinox", "author": "Margaret Hyatt", "etype": "poem"},
        {"title": "The Ornament", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "Dreams", "author": "Zara Sabin", "etype": "poem"},
        {"title": "I Found October", "author": "Agnes Just Reid", "etype": "poem"},
        {"title": "Stilled Moment", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "October", "author": "Gene Romolo", "etype": "poem"},
        # "Faith" by Vesta N. Lukei — OCR destroyed title completely (= Fe + + He); unmatchable
        # {"title": "Faith", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Redecorating", "author": "Dorothy O. Rea", "etype": "poem"},
        {"title": "Autumn Soliloquy", "author": "Zera Wilde Earl", "etype": "poem"},
    ],

    ("Vol42", "No11_November_1955"): [
        {"title": "Dedication of the Temple at Bern, Switzerland", "author": None, "etype": "article"},
        {"title": "The Rewards of Service in Relief Society", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Examples of the Believers", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Be Thou Humble; and the Lord Thy God Shall Lead Thee by the Hand", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Covers for Four", "author": "Sarah O. Moss", "etype": "fiction"},
        {"title": "Hermanas, Chapter 5", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Thankfulness for the Gospel Heritage", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Toys for Your Child", "author": "Helen B. Morris", "etype": "article"},
        {"title": "Meet the Bride", "author": "Helen S. Williams", "etype": "article"},
        {"title": "The Watermelon Rug", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Meat for Thrifty Meals", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Making Original Christmas Cards", "author": "Gene Romolo", "etype": "article"},
        {"title": "Theology: Samuel the Lamanite", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Yea, We Can See that the Lord in His Great Infinite Goodness Doth Bless and Prosper Those Who Put Their Trust in Him", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Meat Selection and Preparation", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Rudyard Kipling, the Man Who Was", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Organization and Powers of the Government", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Ye Shall Have Peace", "author": "Gertrude T. Kovan", "etype": "poem"},
        {"title": "Universal Language", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Wagon City", "author": "Roxana Farnsworth Hase", "etype": "poem"},
        {"title": "The Sparrows' Thanksgiving Feast", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "Prayer Preface", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Doll Clothes", "author": "Margaret Hyatt", "etype": "poem"},
        {"title": "Immutable", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Autumn", "author": "Zara Sabin", "etype": "poem"},
        # "Newness" by Elsie McKinnon Strachan — title absent from OCR entirely; unmatchable
        # {"title": "Newness", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Winter Promise", "author": "Zera Wilde Earl", "etype": "poem"},
        {"title": "The Constant Heart", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Fulfillment", "author": "Ethelyn M. Kincher", "etype": "poem"},
        {"title": "Indian Giver", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "A Priceless Collection", "author": "Francelia Goddard", "etype": "poem"},
    ],

    ("Vol42", "No12_December_1955"): [
        {"title": "When Earth Recalls", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Children in the Scriptures", "author": "J. Reuben Clark, Jr.", "etype": "article"},
        {"title": "The Annual General Relief Society Conference -- 1955", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The Joy of Giving", "author": "Elsie Scott", "etype": "article"},
        {"title": "The Scarlet Cloak of Love", "author": "Lane Stanaway Christian", "etype": "fiction"},
        {"title": "Bells of Christmas", "author": "Pearl Montgomery", "etype": "fiction"},
        {"title": "Hermanas, Chapter 6", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The One Hundred Twenty-Sixth Semi-Annual Church Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The Words of Christmas", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Vegetables -- A Different Way Every Day -- Part I", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Quick Little Presents", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Conditions Among the Nephites in the Days of Nephi the Disciple", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Therefore, Whoso Heareth These Sayings of Mine and Doeth Them, I Will Liken Him Unto a Wise Man", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Vegetable Cookery -- Part I", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Post-Victorian Poets", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Citizenship and Suffrage Under the Constitution", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Christmas Hope", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "The Western Sign", "author": "Elsie F. Parton", "etype": "poem"},
        {"title": "Christmas Night", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "His Birthday", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Old Lullabies", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Carol", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Greater Part", "author": "Della Adams Leitner", "etype": "poem"},
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
    year = 1955

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol42_{key}.txt"
    ISSUE_FILES[("Vol42", key)] = (fname, month)


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
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers for Vol42.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
_GREEN_WILLOWS_CHAPTER_PAT = (
    r"Green\s+Willows"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|CuaptTer|Cuapter)\s+)?"
)
_HERMANAS_CHAPTER_PAT = (
    r"Hermanas"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)
_CONTENTMENT_CHAPTER_PAT = (
    r"(?:C|c)ontentment\s+(?:I|i)s\s+a\s+(?:L|l)ovely\s+(?:Th|Sh|Ch)ing"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)
_SPECIAL_BIRTHDAY_PAT = (
    r"(?:S|J|\()pecial\s+Birthday"
    r".{0,80}?"  # skip author
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—Characters and Teachings of The Book of Mormon Lesson NN—SUBTITLE
    # Require em-dash after category to avoid matching casual mentions in Notes from the Field
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Visiting Seacher ITlessages / VUssiting Seacher ITlessages / Ussiting Seacher ITlessages / Visiting Cseacher ITlessages
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:C|c)?(?:T|S)(?:e|l)(?:a|e)cher\s+"
        r"(?:M|IT|T|m)(?:l)?essages"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|]?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+Near\s+and\s+(?:F|S)ar"
    ),
    # Body has: EDITORIAL ... VOL. 42 MONTH 1955 NO. N ... or just EDITORIAL followed by text
    "Editorial: ": (
        r"EDITORIAL"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"EDITORIAL[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Body has: Of iterature—The Literature of England Lesson NN—SUBTITLE
    # or: ere ee Literature of England Lesson NN—SUBTITLE
    "Literature: ": (
        r"(?:(?:Ie\s+eagle\s*[\-\u2014\u2013]*\s*ine\s+)|(?:O|Of?\s+i|of\s+i)?)?"
        r"[Ll]?iterature"
        r"[\s\-\u2014\u2013:]*"
        r"(?:(?:Th|Sh|Ch)e\s+Literature\s+of\s+England)?"
        r"[\s\S]{0,60}?"
    ),
    # Body has: Social Science—The Constitution of the United States Lesson NN—SUBTITLE
    # Require em-dash after category to avoid matching casual mentions in Notes from the Field
    "Social Science: ": (
        r"Social\s+Sc(?:i|t)ence"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has: Vork IT leeting / WVork IT leeting / Work ITleeting
    # Require em-dash after category to avoid matching casual mentions in Notes from the Field
    "Work Meeting: ": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|M)?(?:\s+)?(?:l)?eeting"
        r"[)\s]*[\-\u2014\u2013][\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St)rom\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    # Serial fiction: Green Willows
    "Green Willows, Chapter 1": _GREEN_WILLOWS_CHAPTER_PAT + r"(?:1|I)\b",
    "Green Willows, Chapter 2": _GREEN_WILLOWS_CHAPTER_PAT + r"(?:2|II)\b",
    "Green Willows, Chapter 3": _GREEN_WILLOWS_CHAPTER_PAT + r"(?:3|III)\b",
    "Green Willows, Chapter 4": _GREEN_WILLOWS_CHAPTER_PAT + r"(?:4|IV)\b",
    "Green Willows, Chapter 5": _GREEN_WILLOWS_CHAPTER_PAT + r"(?:5|V)\b",
    "Green Willows, Chapter 6": _GREEN_WILLOWS_CHAPTER_PAT + r"(?:6|VI)\b",
    # Serial fiction: Hermanas
    "Hermanas, Chapter 1": _HERMANAS_CHAPTER_PAT + r"(?:1|I)\b",
    "Hermanas, Chapter 2": _HERMANAS_CHAPTER_PAT + r"(?:2|II)\b",
    "Hermanas, Chapter 3": _HERMANAS_CHAPTER_PAT + r"(?:3|III)\b",
    "Hermanas, Chapter 4": _HERMANAS_CHAPTER_PAT + r"(?:4|IV)\b",
    "Hermanas, Chapter 5": _HERMANAS_CHAPTER_PAT + r"(?:5|V)\b",
    "Hermanas, Chapter 6": _HERMANAS_CHAPTER_PAT + r"(?:6|VI)\b",
    # Serial fiction: Contentment Is a Lovely Thing
    "Contentment Is a Lovely Thing, Chapter 4": _CONTENTMENT_CHAPTER_PAT + r"(?:4|IV)\b",
    "Contentment Is a Lovely Thing, Chapter 5": (
        _CONTENTMENT_CHAPTER_PAT + r"(?:5|V)\b"
    ),
    # Two-part story: Special Birthday
    "Special Birthday, Part I": _SPECIAL_BIRTHDAY_PAT + r"(?:Part\s+)?(?:I|1)\b",
    "Special Birthday, Part II": _SPECIAL_BIRTHDAY_PAT + r"(?:Part\s+)?(?:II|I\]|I\)|2)\b",
    # March uses "Eighty-One Years Ago" instead of "Sixty Years Ago"
    "Eighty-One Years Ago": r"Eighty.One\s+Years\s+Ago",
    # Poem OCR fixes — garbled titles that _build_regex_chars can't handle
    # "The Difference" → "She Dif erence" in body
    "The Difference": r"(?:Th|Sh|Ch)e\s+(?:D|Cl)if\s*e?rence",
    # "Father's Garden" → "Sather $ Garden" in body
    "Father's Garden": r"(?:F|S|St)ather['\u2019s$\s]+\s*Garden",
    # "Return" — common word; require author attribution nearby to disambiguate
    "Return": r"Return\s+Catherine\s+E\.?\s+Berry",
    # "Reflections" — frontispiece poem, require author to disambiguate from prose
    "Reflections": r"Reflections\s+Dorothy\s+J[\].]?\s*Roberts",
    # "Fulfillment" → "Si ulfillment" in body; require author to avoid matching prose "fulfillment"
    "Fulfillment": r"(?:F|S|St|Si\s*)(?:u|U)lfillment\s+Ethelyn\s+M\.?\s+Kincher",
    # "Doll Clothes" — common phrase; require author to disambiguate
    "Doll Clothes": r"Doll\s+Clothes\s+Margaret\s+Hyatt",
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

    # Fallback for Vol42 issues missing "PUBLISHED MONTHLY" (Jan, Jun):
    # Split on "Entered as second-class matter" or subscription info,
    # or on the boundary between short lines and the long body line.
    fallback_markers = [
        r"Payable\s+in\s+advance",
        r"Entered\s+\w+\s+second.class\s+matter",
    ]
    for pat in fallback_markers:
        match = re.search(pat, text, re.IGNORECASE)
        if match:
            # Find the end of this line/section
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol42"
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
        description="Extract Relief Society Magazine Vol 42 into individual entries"
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

    for (vol, issue_key), entries in VOL42_TOC.items():
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
