#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 48 (1961).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol48.py
    python extract_vol48.py --dry-run
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

VOL48_TOC = {
    ("Vol48", "No01_January_1961"): [
        {"title": "Blessings in the New Year", "author": "General Presidency", "etype": "article"},
        {"title": "Feminine Spirituality in the Home", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Song of Three Marys", "author": "Sylvia Probst Young", "etype": "article"},
        {"title": "Joseph: the Prophet", "author": "Genevieve St. Cyr Groen", "etype": "article"},
        {"title": "Pilgrimage to Christmas", "author": "Dorothy J. Roberts", "etype": "article"},
        {"title": "Grafted", "author": "Hope M. Williams", "etype": "fiction"},
        {"title": "Temple Square in Salt Lake City -- Part III", "author": "Preston Nibley", "etype": "article"},
        {"title": "Prevent Crippling Diseases", "author": "Basil O'Connor", "etype": "article"},
        {"title": "Love Is Enough, Chapter 1", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: And Full of Time", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "Singing Mothers to Present Music at Dedication of Hyde Park Chapel in London", "author": None, "etype": "article"},
        {"title": "Afterglow", "author": "Nancy M. Armstrong", "etype": "article"},
        {"title": "Julia Anderson Kirby Specializes in Hardanger Work", "author": None, "etype": "article"},
        {"title": "Stretching", "author": "Celia Larsen Luce", "etype": "article"},
        {"title": "Theology: The Second Coming of Christ", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Thou Shalt Not Speak Evil!", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Feeding the Patient", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: Emerson, the Spokesman for His Age", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Growing Religious Values in the Home", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "The Cup Once Filled", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Thanks for Five Senses", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Hidden Harmonies", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Vigil", "author": "Padda M. Speller", "etype": "poem"},
        {"title": "Have Courage", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Weak Echoes", "author": "Dianne Dibb", "etype": "poem"},
        {"title": "A Child Says Grace", "author": "Ethel Jacobson", "etype": "poem"},
    ],

    ("Vol48", "No02_February_1961"): [
        {"title": "The Rewards of Welfare Service", "author": "Marion G. Romney", "etype": "article"},
        {"title": "Temple Square in Salt Lake City -- Part IV", "author": "Preston Nibley", "etype": "article"},
        {"title": "The Happety Road", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "My Own Stove, My Own Table", "author": "Sarah O. Moss", "etype": "fiction"},
        {"title": "Love Is Enough, Chapter 2", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: ... In Her Tongue Is the Law of Kindness", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "Beauty in the Shade", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "The Old Fireplace", "author": "Bertha M. Walton", "etype": "article"},
        {"title": "Recipes for Winter Evenings", "author": "Emma A. Hanks", "etype": "article"},
        {"title": "Albertha Nielson Hatch Makes Quilts for the Needy", "author": None, "etype": "article"},
        {"title": "Enchantment", "author": "Marion Ellison", "etype": "article"},
        {"title": "New Stockings From Old", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Theology: Gifts of the Holy Ghost", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Unto the Least of These, Ye Do It Unto Me", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Chronically Ill and the Aged", "author": "Maria Johnson", "etype": "lesson"},
        {"title": "Literature: Nathaniel Hawthorne, Haunted Autobiographer", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Abundant Living for Our Day", "author": "Blaine M. Porter", "etype": "lesson"},
        {"title": "To a Tall Pine", "author": "Lela Foster Morris", "etype": "poem"},
        {"title": "Blacksmith", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Homecoming", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Idyll Moment", "author": "Marie Call Webb", "etype": "poem"},
        {"title": "Sunday Street", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Time of Frost", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Note to a Loved One", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Mountain Child", "author": "Shirley N. Howard", "etype": "poem"},
        {"title": "Winter Garden in My Cabin", "author": "Maude Rubin", "etype": "poem"},
    ],

    ("Vol48", "No03_March_1961"): [
        {"title": "Temple Square in Salt Lake City -- Part V", "author": "Preston Nibley", "etype": "article"},
        {"title": "The American Red Cross: Its Function in the Sixties", "author": "Elisha Gray, II", "etype": "article"},
        {"title": "Where Did They Find Their Smiles?", "author": "Olive Sharp", "etype": "article"},
        {"title": "Stranger at the Gate", "author": "Kit J. Poole", "etype": "fiction"},
        {"title": "Close to the Angels", "author": "Norma A. Wrathall", "etype": "fiction"},
        {"title": "Coffin Under the Bed", "author": "Ilene H. King", "etype": "fiction"},
        {"title": "The Silent Sacrifice", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "Love Is Enough, Chapter 3", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Sisters in the Gospel", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "A New Viewpoint", "author": "Celia Larsen Luce", "etype": "article"},
        {"title": "Grandma Had a Parlor", "author": "Helen S. Phillips", "etype": "article"},
        {"title": "It's the Food You Eat That Counts", "author": "Margaret Merkley", "etype": "article"},
        {"title": "Recipes for Pretty Dishes", "author": "Emma H. Hanks", "etype": "article"},
        {"title": "Maren C. Jensen, Expert Quilter and Happy Seamstress", "author": None, "etype": "article"},
        {"title": "Lee Mitten Marvels", "author": "Shirley Thulin", "etype": "article"},
        {"title": "To the Height", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Little Girl Walking", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Mystic Syllables", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "To Robot Task", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "At Quilting", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Humility", "author": "Louise Morris Kelley", "etype": "poem"},
        {"title": "Not a Drum Was Heard", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Dark Come Late", "author": "Maude Rubin", "etype": "poem"},
        {"title": "So You -- With Love", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "My Clinging Hand", "author": "Gladys Hesser Burnham", "etype": "poem"},
        {"title": "After the Silent Year", "author": "Mabel Jones Gabbott", "etype": "poem"},
    ],

    ("Vol48", "No04_April_1961"): [
        {"title": "Search for Knowledge and Understanding", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Cancer Is Everybody's Business", "author": "Wallace W. Tudor", "etype": "article"},
        {"title": "The Locust Tree Shall Bloom Again", "author": "Pauline L. Jensen", "etype": "article"},
        {"title": "Room for Jenny", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "Stranger in Their Midst", "author": "Jeanne J. Larson", "etype": "fiction"},
        {"title": "I'm Sorry for Your Flowers", "author": "Iris W. Schow", "etype": "fiction"},
        {"title": "The Ogre on Alden Street", "author": "Barbara Williams", "etype": "fiction"},
        {"title": "On Second Thought", "author": "Jerry Barlow", "etype": "fiction"},
        {"title": "The Best-Laid Plans", "author": "Maude Proctor", "etype": "fiction"},
        {"title": "Love Is Enough, Chapter 4", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: All Things Shall Be Restored", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "Marie Curtis Richards Released From the General Board", "author": None, "etype": "article"},
        {"title": "National Library Week", "author": None, "etype": "article"},
        {"title": "Kicking the Rock", "author": "Celia Luce", "etype": "article"},
        {"title": "Elvina J. Homer's Hobby Is Family History and Genealogical Work", "author": None, "etype": "article"},
        {"title": "Pioneer Kitchen", "author": "Alice R. Rich", "etype": "article"},
        {"title": "Something Different for Dinner", "author": None, "etype": "article"},
        {"title": "Too Swift the Curve", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Lost Beauty", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Except for the Daisies", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Forever the Fragile Lily", "author": "Blanche Kendall McKey", "etype": "poem"},
        {"title": "Spring Day", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Almond Blossoms", "author": "Annie Atkin Tanner", "etype": "poem"},
        {"title": "Mountain Springtime", "author": "Rowena Jensen Bills", "etype": "poem"},
        {"title": "Tired Warrior", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Follow a Star", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "A Daughter's Prayer", "author": "Billie Sue Nickle Coffin", "etype": "poem"},
        {"title": "Prayer of a Second Wife", "author": "Vesta Nickerson Fairbairn", "etype": "poem"},
        {"title": "The Big and the Little", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Morning Promise", "author": "Leah W. Kimball", "etype": "poem"},
        {"title": "For April's Sake", "author": "Ida Elaine James", "etype": "poem"},
    ],

    ("Vol48", "No05_May_1961"): [
        {"title": "International Singing Mothers Concert Tour", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Magazine Honor Roll for 1960", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Men Are What Their Mothers Make Them", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Lovingly Remembered", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Love Is Enough, Chapter 5", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Train Up a Child As an Individual", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "The Evening Star", "author": "Cleo Jones Johnson", "etype": "article"},
        {"title": "Spring Housecleaning", "author": "Hattie B. Maughan", "etype": "article"},
        {"title": "Buffet to Remember", "author": "Alice Morrey Bailey", "etype": "article"},
        {"title": "Animal Aprons", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Martha Wilcox Hacking, Mistress of Many Hobbies", "author": None, "etype": "article"},
        {"title": "The Recipe", "author": "Marion Ellison", "etype": "article"},
        {"title": "Beauty", "author": "Arlene D. Cloward", "etype": "article"},
        {"title": "The Hard Way", "author": "Celia Luce", "etype": "article"},
        {"title": "To Be a Grandmother", "author": "Harriet De Spain", "etype": "article"},
        {"title": "The Year of the Butterfly", "author": "Rosemond Purviance", "etype": "poem"},
        {"title": "Sunflowers on a Hill", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Set Your Kindred Free", "author": "Clara Lewis Jennings", "etype": "poem"},
        {"title": "Equilibrium", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Inside the Locket", "author": "Lorena A. White", "etype": "poem"},
        {"title": "Earth House in May", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Woman's Choice", "author": "Lula Walker", "etype": "poem"},
        {"title": "Jesus", "author": "Texas A. Gladden", "etype": "poem"},
        {"title": "Six Seas", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Hearts", "author": "Rowena Jensen Bills", "etype": "poem"},
        {"title": "So Beautiful, Beloved", "author": "Grace Barker Wilson", "etype": "poem"},
    ],

    ("Vol48", "No06_June_1961"): [
        {"title": "Let This Be Said -- To Emma Ray Riggs McKay", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "To His Children's Children", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Serendipity", "author": "Albera Baker", "etype": "article"},
        {"title": "Around the World at Eighty", "author": "Etta B. Cowles", "etype": "article"},
        {"title": "Annual Report for 1960", "author": "Hulda Parker", "etype": "article"},
        {"title": "All for the Good of the Family", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "A Feather in Her Hat", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "Truth Is Sublime", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "Love Is Enough, Chapter 6", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 131st Annual Church Conference", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "Mama's Bookshelf", "author": "Helen Hinckley Jones", "etype": "article"},
        {"title": "Cook-Your-Own Barbecue", "author": "Ruby K. Smith", "etype": "article"},
        {"title": "Now Is the Time", "author": "Leona Fetzer Wintch", "etype": "article"},
        {"title": "Whole-Wheat Oatmeal Cookies", "author": "Betty Donelson", "etype": "article"},
        {"title": "Cookie-Jar Dividends", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "Solve a Lengthy Problem", "author": "Shirley Thulin", "etype": "article"},
        {"title": "I Remember Grand", "author": "Donna Mae Bacon", "etype": "article"},
        {"title": "Great Be the Glory of Those Who Do Right", "author": "Celia Luce", "etype": "article"},
        {"title": "Theology: The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Truths to Live By From The Doctrine and Covenants", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Attitudes and Manners", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: America's Literature Comes of Age", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Place of Woman in the Gospel Plan", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Letter From the Sea", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "That Thy Days May Be Long", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Wayside Path", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Great or Small", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Inland Gulls", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Dance Music", "author": "Catherine Bowles", "etype": "poem"},
        {"title": "Earth-Borne", "author": "Marjorie C. Reay", "etype": "poem"},
        {"title": "To a Granddaughter", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol48", "No07_July_1961"): [
        {"title": "Reminiscings", "author": "Camilla Eyring Kimball", "etype": "article"},
        {"title": "The Precious Words", "author": "May C. Hammond", "etype": "article"},
        {"title": "The Missing Ingredient", "author": "Pansye H. Powell", "etype": "fiction"},
        {"title": "It Was Easy, My Child", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "A Very Special Place", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "The Letter", "author": "Patricia Ann Middleton", "etype": "fiction"},
        {"title": "Love Is Enough, Chapter 7", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Ripening of the Wheat", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Sheets Masquerade Well", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Theology: Records Are Important", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: He That Prayeth", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Manners Matter", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: Nathaniel Hawthorne -- The Scarlet Letter", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Scripture and Woman's Place", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "All Must Drink", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Magnolia Bloom", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "High Summer", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Wind-Whispering Wood", "author": "Melba S. Payne", "etype": "poem"},
        {"title": "New Choir Member", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Byways", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "A Time to Dream", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Time Is Now", "author": "Rose Thomas Graham", "etype": "poem"},
        {"title": "Blue Rock", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Forgotten Things", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "The Life Bouquet", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Song for Remembering", "author": "Evelyn Fjeldsted", "etype": "poem"},
    ],

    ("Vol48", "No08_August_1961"): [
        {"title": "Hugh B. Brown -- Of The First Presidency", "author": "Marba C. Josephson", "etype": "article"},
        {"title": "White House Conference on Aging", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Making Good Things Last", "author": "Lydia H. Fielding", "etype": "article"},
        {"title": "The Glory of Light", "author": "Fredrika Clinch", "etype": "article"},
        {"title": "Because of the Word, Chapter 1", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "His Lasting Love", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Love Is Enough, Chapter 8", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Go to the House of Prayer... Upon My Holy Day", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "Plotting Your Perennials", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Grant Them Hilltops", "author": "Pauline L. Jensen", "etype": "article"},
        {"title": "Science and the Jelly Glass", "author": "Alice Morrey Bailey", "etype": "article"},
        {"title": "Make a Play Pal", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Julia Lottie Bach's Hobbies Are Decorative and Useful", "author": None, "etype": "article"},
        {"title": "Sweet Are the Uses of Adversity", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Theology: The Mission to the Shaking Quakers", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: I Will Give Unto You a Pattern in All Things", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Just for Example", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: John Greenleaf Whittier, Commoner", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Full Equality in the Gospel Plan", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Brief Interim", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "Given in Silence", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "The Autumn Day", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "Down the Lanes of August", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "A Glimpse of Grace", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Handcart Boy", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Too Swift the Time", "author": "Pauline M. Bell", "etype": "poem"},
        {"title": "Conversion", "author": "Evelyn Darlington", "etype": "poem"},
        {"title": "Mountain Meditation", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "Hollyhock Dolls", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "First Formal", "author": "Rose Thomas Graham", "etype": "poem"},
        {"title": "Morning Hour", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Tranquility", "author": "Marilyn Young", "etype": "poem"},
    ],

    ("Vol48", "No09_September_1961"): [
        {"title": "President and Prophet", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Shall We Sing?", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "The Key to Compatible Color Schemes Is Careful Selection", "author": "Marian Cornwall", "etype": "article"},
        {"title": "Living Off the Road", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "The Intruders", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "A Parable for Polly", "author": "Maude Proctor", "etype": "fiction"},
        {"title": "A Story to Sell", "author": "Harriet DeSpain", "etype": "fiction"},
        {"title": "Because of the Word, Chapter 2", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Prudent Living", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "Let's Learn to Quilt", "author": "Holly B. Keddington", "etype": "article"},
        {"title": "Theology: Be Not Deceived", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Remember in All Things the Poor and the Needy", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Being a Good Neighbor", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: Whittier, Lover of New England", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "His Is the Glory", "author": "Lela Foster Morris", "etype": "poem"},
        {"title": "Golden Days", "author": "Annie Atkin Tanner", "etype": "poem"},
        {"title": "After the Storm", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "I Heard a Mother Singing", "author": "Illa Mae Richardson", "etype": "poem"},
        {"title": "The Forming Fruit", "author": "Marjorie B. Newton", "etype": "poem"},
        {"title": "Futility", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Copied Handiwork", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Autumn Noon", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Sound in the Valley", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Pathways", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Wind-Lightened Bough", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Nocturne", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "On the Stair", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Autumn", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "Youth Was a Shield", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol48", "No10_October_1961"): [
        {"title": "The Last Days of President Brigham Young", "author": "Preston Nibley", "etype": "article"},
        {"title": "Songs for Singing Mothers", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Commencement for Miss Rowse", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "Aunt Mattie's Retirement List", "author": "Klea Evans Worsley", "etype": "fiction"},
        {"title": "Because of the Word, Chapter 3", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: On Obedience", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},

        {"title": "Lunches That Lure Your Child to Eat", "author": "Hazel Sowards Cannon", "etype": "article"},
        {"title": "Marguerite Wallace Petersen -- Portrait Artist", "author": None, "etype": "article"},
        {"title": "No Trick at All to Make a Costume", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Today", "author": "Pauline Marie Bell", "etype": "article"},
        {"title": "Sunshine Is Where You Find It", "author": "Cleo Jones Johnson", "etype": "article"},
        {"title": "Theology: Teach What the Apostles and Prophets Have Written", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: One Man Shall Not Build Upon Another's Foundation", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Courtesy in Church", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: Henry Wadsworth Longfellow, America's Poet", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Motherhood, the Highest Type of Service", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Adagio", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Barbara", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Listen for Your Word", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Valediction", "author": "Evelyn H. Hughes", "etype": "poem"},
        {"title": "First Steps", "author": "Donna Swain", "etype": "poem"},
        {"title": "Before the Party", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Faith", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "Hope", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "October Day", "author": "Ramona R. Munford", "etype": "poem"},
    ],

    ("Vol48", "No11_November_1961"): [
        {"title": "Home -- The Place of Peace; the Shelter", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Relief Society Today Needs You", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society -- A Bulwark for Women", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "A Christmas to Remember", "author": "Betty Lou Martin", "etype": "fiction"},
        {"title": "Because of the Word, Chapter 4", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Thy Neighbour As Thyself", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Christmas Decorations", "author": "Melba Larson", "etype": "article"},
        {"title": "Christmas Gifts", "author": "Jean Ridges Jennings", "etype": "article"},
        {"title": "Holiday Cookies", "author": "Winnifred C. Jardine", "etype": "article"},
        {"title": "Aprons for Mr. And Mrs. Santa Claus", "author": "Carol Daynes", "etype": "article"},
        {"title": "First Snow in the Mountains", "author": "Cynthia Hepburn Nuffer", "etype": "article"},
        {"title": "Christmas Is Coming", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Bib Made From a Terry Towel", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Theology: Those That Seek Me Early Shall Find Me", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Let Every Man Deal Honestly", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Public Performance", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: The Cosmopolitan Longfellow", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Homemaking, a Creative Calling", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Silver Twilight", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "I Lift My Eyes", "author": "Gladys Hesser Burnham", "etype": "poem"},
        {"title": "Mountain Meadow at Dusk", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "Winter Morning", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Love Lantern in the Night", "author": "Maude Rubin", "etype": "poem"},
        {"title": "The Valley", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "First Snow", "author": "Patricia Robinson King", "etype": "poem"},
        {"title": "On Hilltop Home", "author": "Margaret Evelyn Singleton", "etype": "poem"},
        {"title": "Landscape", "author": "Ida Elaine James", "etype": "poem"},
    ],

    ("Vol48", "No12_December_1961"): [
        {"title": "Henry D. Moyle Appointed First Counselor in the First Presidency", "author": None, "etype": "article"},
        {"title": "Hugh B. Brown Appointed Second Counselor in the First Presidency", "author": None, "etype": "article"},
        {"title": "Gordon Bitner Hinckley Appointed to the Council of the Twelve", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Boyd K. Packer Appointed Assistant to the Council of the Twelve", "author": "Dale T. Tingey", "etype": "article"},
        {"title": "Relief Society -- An Extension of the Home", "author": "Hugh B. Brown", "etype": "article"},
        {"title": "The Relief Society Annual General Conference -- 1961", "author": "Hulda Parker", "etype": "article"},
        {"title": "Protect Your Family Against Tuberculosis", "author": "Franklin K. Brough", "etype": "article"},
        {"title": "Oh, Little Town", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Because of the Word, Chapter 5", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 131st Semi-Annual Church Conference", "author": None, "etype": "editorial"},
        {"title": "What Is Christmas?", "author": "Agetha King", "etype": "article"},
        {"title": "The Gift Tree", "author": "Nancy M. Armstrong", "etype": "article"},
        {"title": "Holiday Table", "author": "LaRue Rosell", "etype": "article"},
        {"title": "Wrapped Up to Please", "author": "June Feulner Krambule", "etype": "article"},
        {"title": "A Heart of Love", "author": "Pauline M. Bell", "etype": "article"},
        {"title": "Theology: Endure Unto the End", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: According to Men's Faith It Shall Be Done Unto Them", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The True Spirit of Hospitality", "author": "Elaine Anderson Cannon", "etype": "lesson"},
        {"title": "Literature: James Russell Lowell", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Homemaking, a Creative Calling (Continued)", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "In Joyful Trust", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Sacred Night", "author": "Lela Foster Morris", "etype": "poem"},
        {"title": "First Fall", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "A Prayer for Christmas", "author": "Margaret B. Shoemaker", "etype": "poem"},
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
    year = 1961

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol48_{key}.txt"
    ISSUE_FILES[("Vol48", key)] = (fname, month)


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
# Known OCR-mangled section headers for Vol48.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns

# Love Is Enough: Chapters 1-8 (Jan-Aug 1961)
_LOVE_IS_ENOUGH_PAT = (
    r"Love\s+Is\s+Enough"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Because of the Word: Chapters 1-5 (Aug-Dec 1961)
_BECAUSE_OF_WORD_PAT = (
    r"Because\s+of\s+(?:th|sh|ch)e\s+Word"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Editorial header prefix (matches OCR variants of "EDITORIAL")
_EDITORIAL_HDR = (
    r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)"
    r"[\s\S]{0,120}?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—The Doctrine and Covenants Lesson NN—SUBTITLE
    # TOC has: Theology: SUBTITLE
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:(?:Lesson|Discussion)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?\s*(?:The\s+|A\s+)?)?"
    ),
    # Body has: Visiting Teacher Message(s)—Truths to Live By ... Message NN—SUBTITLE
    # TOC has: Visiting Teacher Messages: SUBTITLE
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?|Seal!?\s*He)\s*"
        r"(?:M|IT|T|m|He|HeIT)\s*(?:l)?essages?"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:(?:Message|Lesson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|\.…]?\s*(?:The\s+|A\s+)?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+(?:N|Vl)?ear\s+(?:and|Gnd|And|and)\s+(?:F|S|St|t)ar[r]?"
    ),
    # Body has: EDITORIAL ... VOL. 48 MONTH 1961 NO. N <actual title>
    # TOC has: Editorial: <actual title>
    "Editorial: ": (
        r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Body has: Literature—America's Literature Comes of Age Lesson NN—SUBTITLE
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
    # or: Social Science—The Place of Woman in the Gospel Plan ...
    # TOC has: Social Science: SUBTITLE
    "Social Science: ": (
        r"(?:"
            # Standard: "Social Science" (OCR may garble "Science")
            r"Social\s+(?:Sc(?:i|t|l)ence|eicHiee|Stustiee)"
            r"|"
            # Garbled prefix + series title anchor
            r"(?:\S+[\s=_]*)?(?:(?:Th|Sh|Ch|lh|TH)e\s+)?Place\s+of\s+(?:\\?W)*oman"
            r"|"
            # Sub-series header "THE EMINENCE OF WOMAN"
            r"(?:THE\s+)?EMINENCE\s+OF\s+WOMAN"
            r"|"
            # Sub-series header "MOTHERHOOD" or "SERVICE"
            r"(?:SERVICE|MOTHERHOOD)[\s,\-\u2014]+"
        r")"
        r"[\s\S]{0,250}?"
        r"(?:(?:Lesson|Preview)\s+\d+)"
        r"[\s\-\u2014\u2013]*"
        r"[\"\u201c\u2018\u2019\u0027]?"
    ),
    # Body has: Work Meeting—Nursing in the Home ... Lesson N—SUBTITLE
    # or: Work Meeting—What Shall We Do? ... Discussion N—SUBTITLE
    # TOC has: Work Meeting: SUBTITLE
    "Work Meeting: ": (
        r"(?:W|V)ork\s+(?:IT|T|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:(?:Lesson|Discussion)\s+[\d|]+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St|S)rom\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St|S)(?:i|e)?eld"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St|S)(?:i|e)?eld"
        r":?\s*"
    ),
    # Temple Square articles: body has long subtitle between "City" and "Part N"
    "Temple Square in Salt Lake City -- Part III": (
        r"(?:T|S)emple\s+Square\s+in\s+Salt\s+Lake\s+City"
        r"[\s\S]{0,120}?"
        r"Part\s+III"
    ),
    "Temple Square in Salt Lake City -- Part IV": (
        r"(?:T|S)emple\s+Square\s+in\s+Salt\s+Lake\s+City"
        r"[\s\S]{0,120}?"
        r"Part\s+IV"
    ),
    "Temple Square in Salt Lake City -- Part V": (
        r"(?:T|S)emple\s+Square\s+in\s+Salt\s+Lake\s+City"
        r"[\s\S]{0,120}?"
        r"Part\s+V\b"
    ),
    # Sep: body start garbled as "ets rr to Quilt" — anchor on "Quilt" + author
    "Let's Learn to Quilt": r"(?:Let.s\s+Learn\s+to\s+|ets\s+\S+\s+to\s+)Quilt",
    # Jun poem: body has "GarthBorne" (OCR garble of "Earth-Borne") as heading
    "Earth-Borne": r"(?:G|E)arth[\s\-]?Borne",
    # Prize poems: match the actual poem headers, not the award announcement
    # Body has "First Prize Poem Song of Shree Mlarys" (Three→Shree, Marys→Mlarys)
    "Song of Three Marys": (
        r"(?:First|1st)\s+Prize\s+Poem\s+"
        r"Song\s+of\s+(?:Th|Sh|Ch)?ree\s+(?:M|IT|Tl)larys"
    ),
    # Body has "Second Prize Poem GYoseph the Prophet"
    "Joseph: the Prophet": (
        r"(?:Second|2nd)\s+Prize\s+Poem\s+"
        r"(?:G?Y?oseph|Joseph)\s+(?:th|sh|ch)e\s+Prophet"
    ),
    # Body has "Third Prize Poem Pilgrimage to Chasen" (Christmas→Chasen)
    "Pilgrimage to Christmas": (
        r"(?:Third|3rd)\s+Prize\s+Poem\s+"
        r"Pilgrimage\s+to\s+Ch"
    ),
    # Specific editorial overrides for OCR-damaged titles
    # Jan: body has "And Fell of Sime" (Full→Fell, Time→Sime)
    "Editorial: And Full of Time": _EDITORIAL_HDR + r"And\s+F[eu]ll\s+of\s+[ST]ime",
    # Feb: body has "MT ORIAL" + "In Her Jongue Ys the Law of Kindness"
    "Editorial: ... In Her Tongue Is the Law of Kindness": (
        _EDITORIAL_HDR + r"(?:\.{3}|[\"'\u201c])?\s*In\s+Her\s+(?:T|J)ongue\s+"
        r"(?:I|Y)s\s+(?:th|sh|ch)e\s+Law\s+of\s+Kindness"
    ),
    # May: body has "Srain Up a Child As an Yndividual" (I→Y)
    "Editorial: Train Up a Child As an Individual": (
        _EDITORIAL_HDR + r"(?:T|S)rain\s+Up\s+a\s+Child\s+As\s+an\s+(?:I|Y)ndividual"
    ),
    # Jun: body has "She 31st Annual" (131st→31st, The→She)
    "Editorial: The 131st Annual Church Conference": (
        _EDITORIAL_HDR + r"(?:Th|Sh|Ch)e\s+1?31st\s+Annual\s+Church\s+Conference"
    ),
    # Aug: body has "Go to the House of Prayer ae Upon Ty Hele Dax"
    "Editorial: Go to the House of Prayer... Upon My Holy Day": (
        _EDITORIAL_HDR + r"Go\s+to\s+(?:th|sh|ch)e\s+House\s+of\s+Prayer"
        r".{0,20}?"
        r"Upon\s+(?:M|Th|IT|T)y\s+H[eo]l[yei]\s+D[ae][yx]"
    ),
    # Serial fiction: Love Is Enough (Chapters 1-8, Jan-Aug)
    "Love Is Enough, Chapter 1": _LOVE_IS_ENOUGH_PAT + r"(?:1|I)\b",
    "Love Is Enough, Chapter 2": _LOVE_IS_ENOUGH_PAT + r"(?:2|II)\b",
    "Love Is Enough, Chapter 3": _LOVE_IS_ENOUGH_PAT + r"(?:3|III)\b",
    "Love Is Enough, Chapter 4": _LOVE_IS_ENOUGH_PAT + r"(?:4|IV)\b",
    "Love Is Enough, Chapter 5": _LOVE_IS_ENOUGH_PAT + r"(?:5|V)\b",
    "Love Is Enough, Chapter 6": _LOVE_IS_ENOUGH_PAT + r"(?:6|VI)\b",
    "Love Is Enough, Chapter 7": _LOVE_IS_ENOUGH_PAT + r"(?:7|VII)\b",
    "Love Is Enough, Chapter 8": _LOVE_IS_ENOUGH_PAT + r"(?:(?:8|VIII)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: Because of the Word (Chapters 1-5, Aug-Dec)
    "Because of the Word, Chapter 1": _BECAUSE_OF_WORD_PAT + r"(?:1|ONE|I)\b",
    "Because of the Word, Chapter 2": _BECAUSE_OF_WORD_PAT + r"(?:2|TWO|II)\b",
    "Because of the Word, Chapter 3": _BECAUSE_OF_WORD_PAT + r"(?:3|THREE|III)\b",
    "Because of the Word, Chapter 4": _BECAUSE_OF_WORD_PAT + r"(?:4|FOUR|IV)\b",
    "Because of the Word, Chapter 5": _BECAUSE_OF_WORD_PAT + r"(?:(?:5|FIVE|V)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
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
    or 0 if not found (allowing full-body search as fallback).
    """
    m = _LESSON_DEPT_RE.search(body)
    if m:
        return m.start()
    m = _LESSON_DEPT_FALLBACK_RE.search(body)
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
    # "LESSON DEPARTMENT" header (various OCR garbles)
    re.compile(r'(?:LESS.{0,10}N\s+(?:DE.{0,6}\s+)?)?DEPARTMENT[\s|:]+(?:Sh|Th|Ch)eology', re.IGNORECASE),
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol48"
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
        description="Extract Relief Society Magazine Vol 48 into individual entries"
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
    vol48_out = OUTPUT_DIR / "vol48"
    if vol48_out.exists() and not args.dry_run:
        print(f"Cleaning output directory: {vol48_out}")
        shutil.rmtree(vol48_out)

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    volume_json = {}

    for (vol, issue_key), entries in VOL48_TOC.items():
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
