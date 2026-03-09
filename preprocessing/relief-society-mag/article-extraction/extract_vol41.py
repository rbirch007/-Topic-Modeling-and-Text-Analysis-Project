#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 41 (1954).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol41.py
    python extract_vol41.py --dry-run
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

VOL41_TOC = {
    ("Vol41", "No01_January_1954"): [
        {"title": "A New Year's Message", "author": None, "etype": "article"},
        {"title": "Individual Welfare in a Time of Plenty", "author": "Carl W. Buehner", "etype": "article"},
        {"title": "Award Winners Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Wings Over the West", "author": "Lizabeth Wall Madsen", "etype": "poem"},
        {"title": "A Stone in the Wilderness", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "To Shield a King", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Biographical Sketches of Award Winners in the Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Award Winners -- Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "One Wild Rose", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "First Ladies of Our Land -- Part III", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "Support the March of Dimes", "author": "Elsie Sim Hansen", "etype": "article"},
        {"title": "Winter Is for Mothers", "author": "Lucille Waters Mattson", "etype": "article"},
        {"title": "New Year's Choice", "author": "Dorothy Boys Kilian", "etype": "fiction"},
        {"title": "Moon Music", "author": "Louise Morris Kelley", "etype": "fiction"},
        {"title": "The Deeper Melody, Chapter 4", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Happier Life in the New Year", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Relief Society Assigned Evening Meeting of Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Melvina Bennett Clark Makes Braided Rugs", "author": None, "etype": "article"},
        {"title": "Theology: Righteousness and Good Government", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: But There Is a Resurrection, Therefore the Grave Hath No Victory", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Spending Your Home Furnishings Dollar -- Soft Floor Coverings", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Robert Browning, Poet of Personality", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Philadelphia Convention", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Amateur Gardener's Reward", "author": "Sudie Stuart Hager", "etype": "poem"},
        {"title": "Little Girl Before the Piano", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Love's Destiny", "author": "Ada Marie Patten", "etype": "poem"},
        {"title": "Winter Night", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Old Year", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Swift Sketch", "author": "Thelma J. Lund", "etype": "poem"},
    ],

    ("Vol41", "No02_February_1954"): [
        {"title": "Feast Upon the Words of Christ", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "In Memoriam -- Matthew Cowley", "author": "Spencer W. Kimball", "etype": "article"},
        {"title": "Beside the Still Waters", "author": "Mary D. Knowles", "etype": "fiction"},
        {"title": "The Wedding Gift", "author": "Dorothy Cannon", "etype": "fiction"},
        {"title": "The Stranger", "author": "Inez Papinau", "etype": "fiction"},
        {"title": "The Deeper Melody, Chapter 5", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: True Love and Beauty", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Finest Career of All", "author": "Pauline M. Henderson", "etype": "article"},
        {"title": "Theology: Alma, Son of Alma", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Seek Not to Counsel the Lord But to Take Counsel From His Hand", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Spending the Home Furnishings Dollar -- Linens", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Charlotte Bronte", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "George Manwaring", "author": "Lillie C. Adams", "etype": "poem"},
        {"title": "Heart Strings", "author": "Ida Jacobson", "etype": "poem"},
        {"title": "Childhood Memory", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "Trifle", "author": "Leola Woolley Bowen", "etype": "poem"},
        {"title": "Insult", "author": "Vilate R. McAllister", "etype": "poem"},
        {"title": "Compensation", "author": "Luella N. Adams", "etype": "poem"},
        {"title": "Release from Darkness", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Because of Me", "author": "Elaine C. Kloppel", "etype": "poem"},
        {"title": "Candle", "author": "Blanche Kendall McKey", "etype": "poem"},
    ],

    ("Vol41", "No03_March_1954"): [
        {"title": "Relief Society Responsibilities", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "The American Red Cross and Its Program", "author": None, "etype": "article"},
        {"title": "Wilderness Road", "author": "Willard Luce", "etype": "article"},
        {"title": "My Paradise -- Cowslip Hollow", "author": "Emily Wilkerson", "etype": "article"},
        {"title": "Today I Reveal Them", "author": "Rose A. Openshaw", "etype": "article"},
        {"title": "One Sweetly Solemn Thought", "author": "Ruth MacKay", "etype": "fiction"},
        {"title": "Kayo", "author": "Mary C. Martineau", "etype": "fiction"},
        {"title": "A Golden Thread", "author": "Mildred Garrett Enos", "etype": "fiction"},
        {"title": "The Deeper Melody, Chapter 6", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Lifeblood of Relief Society", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Announcing the Special April Short Story Issue", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Organizations and Reorganizations", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Shrubs for Your Garden", "author": "Dorthea Newbold", "etype": "article"},
        {"title": "On the Quiet", "author": "Velma Mackay Paul", "etype": "article"},
        {"title": "Chloe Call Laker Makes Her Own Quilt Designs", "author": None, "etype": "article"},
        {"title": "A Sunken Window Garden", "author": "Celia Luce", "etype": "article"},
        {"title": "Way Down Inside", "author": "Margaret Lundstrom", "etype": "article"},
        {"title": "Bathroom Tricks: Potted Plants in the Bathroom", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Brighten the Corner Where You Are", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "The Relief Society -- A World-Wide Sisterhood", "author": "Phyllis Hodgson Holbrook", "etype": "poem"},
        {"title": "Against the Dark", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Queen of Queens", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Relief Society", "author": "Elsie Scott", "etype": "poem"},
        {"title": "The Way", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Sunrise on Cliff Mountain", "author": "Gertrude T. Kovan", "etype": "poem"},
        {"title": "For Which the First Was Made", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Sweet After Pass", "author": "Rhea M. Carrick", "etype": "poem"},
        {"title": "Hold On", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Honeymoon Garden", "author": "Dora Toone Brough", "etype": "poem"},
        {"title": "Morning Is Her Delight", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Chore", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Our Town", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Spring Fantasy", "author": "Verda Mackay", "etype": "poem"},
        {"title": "Orchard in Bloom", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol41", "No04_April_1954"): [
        {"title": "The Resurrection of Jesus", "author": "Marion G. Romney", "etype": "article"},
        {"title": "Join the Crusade Against Cancer", "author": "Sandra Munsell", "etype": "article"},
        {"title": "Nevada's Valley of Fire", "author": "Willard Luce", "etype": "article"},
        {"title": "Double Beauty", "author": "Lena Woodbury", "etype": "article"},
        {"title": "Participation in Relief Society Can Help Achieve True Happiness", "author": "Edith Kaneko", "etype": "article"},
        {"title": "Within Our Reach", "author": "Donna Day", "etype": "article"},
        {"title": "The Best Years of Her Life", "author": "Pansye H. Powell", "etype": "fiction"},
        {"title": "What It Takes", "author": "Kay Islaub", "etype": "fiction"},
        {"title": "Melissa", "author": "Blanche Sutherland", "etype": "fiction"},
        {"title": "The Part-Time Heart", "author": "Hannah Smith", "etype": "fiction"},
        {"title": "The Deeper Melody, Chapter 7", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: For Liberty", "author": "Velma N. Simonsen", "etype": "editorial"},
        {"title": "Notes to the Field: A Centenary of Relief Society Out of Print", "author": None, "etype": "article"},
        {"title": "Bells", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Summer Fireplace", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Let Your Table Tell a Story", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Gardening for the Home Freezer", "author": "C. W. McCullough", "etype": "article"},
        {"title": "Handwork Hobbies Bring Happiness", "author": None, "etype": "article"},
        {"title": "TV Viewers -- Down in Front", "author": "Eloise Strinz", "etype": "article"},
        {"title": "Even the Moonlight", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Stanzas on Light", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Forever Mine", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "The First Spring Crocus", "author": "Thelma Groneman", "etype": "poem"},
        {"title": "It Happens Every Spring", "author": "Verda Mackay", "etype": "poem"},
        {"title": "Directions for Gardening", "author": "Maude Rubin", "etype": "poem"},
        {"title": "First Bloom", "author": "Sudie Stuart Hager", "etype": "poem"},
        {"title": "Desert Flowers", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Tulips in the Wind", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "It Must Be Spring", "author": "Hilda V. Cameron", "etype": "poem"},
        {"title": "Sunshine and Rain", "author": "Ruth K. Kent", "etype": "poem"},
        {"title": "After Long Years", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Apprehension", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Dogwood Time", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Silent Return", "author": "Blanche Kendall McKey", "etype": "poem"},
    ],

    ("Vol41", "No05_May_1954"): [
        {"title": "Mother's Influence", "author": "Edith Price Backman", "etype": "article"},
        {"title": "A Mother's Prayer for Her Son", "author": "Wilma B. Bunker", "etype": "article"},
        {"title": "The Birth of a Heritage -- The Gospel in England", "author": "Elsie Scott", "etype": "article"},
        {"title": "Coronation", "author": "Roxana Farnsworth Hase", "etype": "article"},
        {"title": "Miracles and Mother", "author": "Eileen Gibbons", "etype": "article"},
        {"title": "Say it with Flowers", "author": "Norma W. South", "etype": "article"},
        {"title": "Anniversary Souvenirs", "author": "Mabel Law Atkinson", "etype": "article"},
        {"title": "With No Regrets", "author": "Myrtle M. Dean", "etype": "article"},
        {"title": "Thou Shalt Never Cease to Grow", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Lest She Forget", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "Things Will Be Different", "author": "Virginia M. Kammeyer", "etype": "fiction"},
        {"title": "The Right Decision!", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "The Deeper Melody, Chapter 8", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Portrait of Mother", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Magazine Subscriptions for 1953", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Preserving Metal Planters", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Launder That White Shirt Yourself", "author": "Ruth K. Kent", "etype": "article"},
        {"title": "Maytime in the Valley", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Faith", "author": "Mary Ellen B. Workman", "etype": "poem"},
        {"title": "Deserted Garden", "author": "Matia McClelland Burk", "etype": "poem"},
        {"title": "To David", "author": "Marjorie Foote", "etype": "poem"},
        {"title": "Morning Glories", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Mother Love", "author": "Hannah C. Ashby", "etype": "poem"},
        {"title": "So Shall We Reap", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Come Gently, Spring", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Fallen Giant", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Motherhood", "author": "Ivinetta R. Oliver", "etype": "poem"},
        {"title": "Vacation Just Beyond", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "For Wood Violets", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Bright Hour", "author": "Grace Sayre", "etype": "poem"},
        {"title": "The Olden Days and the New", "author": "Camilla Alexander", "etype": "poem"},
    ],

    ("Vol41", "No06_June_1954"): [
        {"title": "In Gratitude for Father", "author": "Alice L. Wilkinson", "etype": "article"},
        {"title": "Elder George Q. Morris Fills Vacancy in Quorum of the Twelve", "author": "Alma Sonne", "etype": "article"},
        {"title": "Elder Sterling W. Sill Appointed Assistant to the Council of the Twelve", "author": "David L. McKay", "etype": "article"},
        {"title": "Contest Announcements -- 1954", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Equation for Better Poetry -- Inspiration, Plus", "author": "Maryhale Woolsey", "etype": "article"},
        {"title": "Writing Is Work", "author": "Claire W. Noall", "etype": "article"},
        {"title": "First Ladies of Our Land -- Part IV", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "When Viewed From a Distance", "author": "Nora Yaros", "etype": "article"},
        {"title": "The Falling Shackles, Chapter 1", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "Something Blue", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 124th Annual Church Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Summer Work Meetings", "author": None, "etype": "article"},
        {"title": "Tribute to the Singing Mothers", "author": "David O. McKay", "etype": "article"},
        {"title": "Notes from the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Hulda Peterson, Homemaker and Musician", "author": None, "etype": "article"},
        {"title": "A Different Kind of Bridal Shower", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Housecleaning Nostalgia", "author": "Vernessa M. Nagle", "etype": "article"},
        {"title": "The Scratching Post", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Teaching Aids for the 1954-55 Lessons", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Theology: Characters and Teachings of The Book of Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Book of Mormon Gems of Truth", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Selection, Care, and Use of Household Equipment", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: The Literature of England", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "First Love", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "We Thank Thee O God", "author": "Marjorie Foote", "etype": "poem"},
        {"title": "My Faith", "author": "Jean D. Wright", "etype": "poem"},
        {"title": "Summer Sorcery", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "No Barriers", "author": "Lavina M. Wood", "etype": "poem"},
        {"title": "To a Yellow Rose", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Her Wish Fulfilled", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "June", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "Navajo Cradle Song", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Rain Song", "author": "Verda Mackay", "etype": "poem"},
        {"title": "Neighbors", "author": "Vilate R. McAllister", "etype": "poem"},
        {"title": "Pilgrimage", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Pasture Lot", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Mirror Lake", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Daisies", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol41", "No07_July_1954"): [
        {"title": "The Site of the New Relief Society Building", "author": "Preston Nibley", "etype": "article"},
        {"title": "Tale of the Trail -- A Visit to Phantom Ranch", "author": "C. W. McCullough", "etype": "article"},
        {"title": "First Ladies of Our Land -- Part V -- Conclusion", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "The Birthday", "author": "Helen Hooper", "etype": "fiction"},
        {"title": "The Way", "author": "Ruth Moody Ostegar", "etype": "fiction"},
        {"title": "The Falling Shackles, Chapter 2", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Responsibilities of Freedom", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Program for the November Fast Sunday Evening Meeting", "author": None, "etype": "article"},
        {"title": "Annual Report for 1953", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Needlecraft -- A Useful Hobby", "author": None, "etype": "article"},
        {"title": "Bibs for Babies", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Alma and Amulek", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Now My Brethren, We See That God Is Mindful of Every People", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Home Safety", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Thomas Carlyle", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Fundamental and Basic Principles and the Preamble to the Constitution", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Beautiful As Lace", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Ode to a Deserted Cottage", "author": "Norma Parkinson Post", "etype": "poem"},
        {"title": "Dawn", "author": "Luella N. Adams", "etype": "poem"},
        {"title": "This Be Our Strength", "author": "Alice Briley", "etype": "poem"},
        {"title": "Gateway to Zion", "author": "Elaine A. Walters", "etype": "poem"},
        {"title": "Sioux Chieftain's Prayer", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Frontier Women", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "This Is My Country", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Great Grandad's Day", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "The Lighted Lamp", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "Desert Morning", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Twilight", "author": "Catherine B. Bowles", "etype": "poem"},
    ],

    ("Vol41", "No08_August_1954"): [
        {"title": "Fruits of a Living Faith", "author": "Clifford E. Young", "etype": "article"},
        {"title": "Costumed Models of the First Ladies", "author": "Melba S. Payne", "etype": "article"},
        {"title": "Washington State Wonder -- Soap Lake", "author": "Marijane Morris", "etype": "article"},
        {"title": "Dependable Jane", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "New Light", "author": "Lucile Tournear", "etype": "fiction"},
        {"title": "The Falling Shackles, Chapter 3", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Conversation in the Home", "author": "Velma N. Simonsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Bed Jackets", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Quilts for All the Family", "author": None, "etype": "article"},
        {"title": "Theology: Missionary Activities of Ammon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: But If Ye Will Nourish the Word, Yea, Nourish the Tree as It Beginneth to Grow", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Sewing Machines", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: John Ruskin", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States, Article I", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Now That It Is Summer", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Love of My Savior", "author": "Sarah Carlisle", "etype": "poem"},
        {"title": "The Source", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "God Hears Every Prayer", "author": "Zelda D. Howard", "etype": "poem"},
        {"title": "Jacaranda in Bloom", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Son Flowers", "author": "Louise Morris Kelley", "etype": "poem"},
        {"title": "Manna", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Two Together", "author": "Agda Gronbech Harlow", "etype": "poem"},
        {"title": "In Their Lonely Years", "author": "Ing Smith", "etype": "poem"},
        {"title": "The Seventh Summer", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Heritage", "author": "Vesta N. Lukei", "etype": "poem"},
    ],

    ("Vol41", "No09_September_1954"): [
        {"title": "Identification Key for the Signers of the Constitution", "author": None, "etype": "article"},
        {"title": "The Constitution of the United States -- A Heavenly Banner", "author": "Hugh B. Brown", "etype": "article"},
        {"title": "But Where Are the Nine?", "author": "Sylvia Probst Young", "etype": "article"},
        {"title": "Ralph", "author": "Dorothy A. Johnson", "etype": "article"},
        {"title": "We Don't Do It All Ourselves", "author": "Margaret Allen", "etype": "article"},
        {"title": "When You Reach Bottom", "author": "Evelyn Dorio", "etype": "article"},
        {"title": "One to Tell", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "The Long Night", "author": "Maurine B. Hansen", "etype": "fiction"},
        {"title": "No Secrets Between Them", "author": "Dorothy Boys Kilian", "etype": "fiction"},
        {"title": "The Falling Shackles, Chapter 4", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Congratulations to President David O. McKay on His Eighty-First Birthday", "author": None, "etype": "editorial"},
        {"title": "Editorial: Be Ye Kind", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Annual General Relief Society Conference", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Storing Wheat", "author": None, "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Gramaw", "author": "Kathryn Forbes Clyde", "etype": "article"},
        {"title": "Recipes for Homemade Soap", "author": "Eva Carter", "etype": "article"},
        {"title": "Kitchens for Living", "author": "Celia Luce", "etype": "article"},
        {"title": "Anise -- An Old-Fashioned Herb for Modern Cookery", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Missions of Other Sons of Mosiah", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For Behold, This Life Is the Time for Men to Prepare to Meet God", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Washers and Dryers", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Dante Gabriel and Christina Rossetti", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Fruits of Freedom", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "Unspoken Words", "author": "Emily Carey Alleman", "etype": "poem"},
        {"title": "Red Hills in Utah", "author": "Lydia Hall", "etype": "poem"},
        {"title": "New Loss", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "For an Artist", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "End of Summer", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Wind Fingers", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Traditional", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "Faithful Farmer", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "On Guard", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Colorful Secret", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Provident", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Spendthrift Artistry", "author": "Renie H. Littlewood", "etype": "poem"},
        {"title": "To a Cat, Being Put Out", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Exaltation", "author": "Lydia Bennett Egbert", "etype": "poem"},
        {"title": "Schoolboy", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Each Year I Vow", "author": "Bernice T. Clayton", "etype": "poem"},
    ],

    ("Vol41", "No10_October_1954"): [
        {"title": "Relief Society Responsibilities", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "The International Council of Women Conference at Helsinki, Finland", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Skimping on Spirituality", "author": "Helen A. Nielsen", "etype": "article"},
        {"title": "A Small Trouble", "author": "Celia Luce", "etype": "article"},
        {"title": "Contentment Is a Lovely Thing, Chapter 1", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "To Be or Not to Be", "author": "Mary C. Martineau", "etype": "fiction"},
        {"title": "A Pair of Pants for Benjy", "author": "Pansye H. Powell", "etype": "fiction"},
        {"title": "The Young and the Old", "author": "Caroline C. Lewis", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Heritage of Relief Society", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Mary J. Wilson Resigns From General Board", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Rolls Are Rising!", "author": "Winnifred C. Jardine", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "Let's Get Ready for Christmas", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Have an Apple Party", "author": "Ida M. Pardue", "etype": "article"},
        {"title": "The Golden Cage", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Further Teachings of Alma and Amulek", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For That Same Spirit Which Doth Possess Your Bodies", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Irons and Ironers", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Emily Bronte", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States, Articles II and III", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "I Will Lift Up Mine Eyes", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "End of Summer", "author": "Nell B. Brenchley", "etype": "poem"},
        {"title": "The Rainbow Ending", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Farm Shopping", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "I Must Choose", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Indications", "author": "Ing Smith", "etype": "poem"},
        {"title": "Night", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Last Call to Vacation", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Farewell to the Autumn Leaves", "author": "Lettie B. H. Rich", "etype": "poem"},
        {"title": "Bounty", "author": "Dorothy O. Rea", "etype": "poem"},
        {"title": "Fulfillment", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Visitor", "author": "Genevieve Groen", "etype": "poem"},
        {"title": "Love Grows Fonder", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Night of Magic", "author": "Eunice J. Miles", "etype": "poem"},
        {"title": "Work in Print", "author": "Fanny G. Brunt", "etype": "poem"},
    ],

    ("Vol41", "No11_November_1954"): [
        {"title": "The Greatness of Relief Society", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "But One Thing Is Needful", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Be Ye an Example", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Gift Books for Children", "author": "Abbie H. Wells", "etype": "article"},
        {"title": "A Little More Spirit", "author": "Carol Read Flake", "etype": "fiction"},
        {"title": "A Sure-Enough Seamstress", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "Contentment Is a Lovely Thing, Chapter 2", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Praise and Thanksgiving", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Make Baby Some Rattle Shoes", "author": "Nell Folkman", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "A Different Christmas Wreath", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: Instructions of Alma to His Sons", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For Just As Surely As This Director Did Bring", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Ranges", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Wuthering Heights by Emily Bronte", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States, Articles IV, V, VI, and VII and Bill of Rights, First Four Amendments", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "The Pumpkin Pie Glorified", "author": "Bertha F. Cozzens", "etype": "poem"},
        {"title": "Transition", "author": "Ella J. Skelton", "etype": "poem"},
        {"title": "Hands", "author": "Verda Mackay", "etype": "poem"},
        {"title": "Too Many Memories", "author": "Zera Wilde Earl", "etype": "poem"},
        {"title": "Poetry", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Wherever You Are", "author": "Evelyn L. Daines", "etype": "poem"},
        {"title": "Ultimate Conquest", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Fall Cleaning", "author": "Bernice T. Clayton", "etype": "poem"},
        {"title": "Friendship", "author": "Elsie Sim Hansen", "etype": "poem"},
        {"title": "Family Night", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Little Wise Men", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Ephemeral Moment", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "Autumn Rituals", "author": "Eunice J. Miles", "etype": "poem"},
        {"title": "East Window", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Time By the Heart", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "A Thought", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Reflection", "author": "Pansye H. Powell", "etype": "poem"},
    ],

    ("Vol41", "No12_December_1954"): [
        {"title": "Contributions of the Church to Home and Family", "author": "Stephen L Richards", "etype": "article"},
        {"title": "The Annual General Relief Society Conference -- 1954", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The Relief Society Building Cornerstone-Laying Ceremony", "author": None, "etype": "article"},
        {"title": "Seek for the Gift", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Contentment Is a Lovely Thing, Chapter 3", "author": "Dorothy S. Romney", "etype": "fiction"},
        {"title": "As Little Children", "author": "Virginia M. Kammeyer", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 125th Semi-Annual Church Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Editorial: The Meaning of December for Latter-day Saints", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Right After Christmas", "author": "Ida M. Pardue", "etype": "article"},
        {"title": "Gift Wrapping", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Again It's Christmas!", "author": "Helen S. Williams", "etype": "article"},
        {"title": "Theology: Moroni and the Nephite Armies Serve Their Country and Their Church", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: As Ye Have Begun to Teach the Word Even So I Would That Ye Should Continue to Teach", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Home Freezers and Refrigerators", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Victorian Humorists", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States -- The Bill of Rights -- Amendments Five Through Ten", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "A Sign for Zarahemla", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Winter Heart", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Country Houses", "author": "Margaret Evelyn Singleton", "etype": "poem"},
        {"title": "The Paper Santa Claus", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Through Your Eyes", "author": "Bernice T. Clayton", "etype": "poem"},
        {"title": "One Story", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Turntable", "author": "Maud Rubin", "etype": "poem"},
        {"title": "From an Old Trunk", "author": "Angelyn W. Wadley", "etype": "poem"},
        {"title": "There Will Always Be a Christmas", "author": "Adelia M. Pierce", "etype": "poem"},
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
    year = 1954

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol41_{key}.txt"
    ISSUE_FILES[("Vol41", key)] = (fname, month)


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
    "W": r"(?:W|V)",
    "w": r"(?:w|v)",
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers for Vol41.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
_DEEPER_MELODY_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)e\s+Deeper\s+(?:M|IT|Nl|Vl)elody"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)
_FALLING_SHACKLES_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)e\s+(?:F|St)alling\s+(?:Sh|Ch|Th)ackles"
    r".{0,80}?"  # skip author name / synopsis
    r"(?:(?:CHAPTER|Chapter|Carter)\s+)?"  # "Carter" is OCR for "Chapter"
)
_CONTENTMENT_CHAPTER_PAT = (
    r"Contentment\s+(?:I|Y|1)s\s+a\s+(?:L|1)ovely\s+(?:Th|Sh|Ch|S)(?:i|t)ng"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # Theology: body has Sheology—Characters and Teachings of The Book of Mormon Lesson NN—SUBTITLE
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Visiting Teacher Messages: OCR variants include:
    #   "Visiting Seacher ITlessages", "Seacher ITlessages" (no Visiting),
    #   "Visiting Seacher IT lessages", "Uisiting Seacher ITlessages",
    #   or sometimes just "Gems of Truth Lesson NN" with no header prefix.
    # Make prefix fully optional, anchor on "Gems of Truth" for reliability.
    "Visiting Teacher Messages: Book of Mormon Gems of Truth": (
        r"(?:(?:V|U|\()?(?:i|s|u)?s?iting\s+)?"
        r"(?:S|T|J)(?:e|l)(?:a|e)ch(?:e|o)r\s+"
        r"(?:M|IT|7T|T|m)(?:\s*l)?ess(?:a|i)?ges"
        r"[\s\-\u2014\u2013:]*"
        r"Book\s+of\s+(?:M|N|IT)ormon\s+Gems\s+of\s+(?:Th|Sh|Ch|T)r(?:u|o)th"
    ),
    "Visiting Teacher Messages: ": (
        r"(?:(?:V|U|\()?(?:i|s|u)?s?iting\s+)?"
        r"(?:S|T|J)(?:e|l)(?:a|e)ch(?:e|o)r\s+"
        r"(?:M|IT|7T|T|m)(?:\s*l)?ess(?:a|i)?ges"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|]?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+Near\s+(?:a|A)nd\s+(?:F|S)ar"
    ),
    # Editorial: body has EDITORIAL noise VOL. 41 MONTH 1954 <actual title>
    # Many subtitle texts differ from TOC titles, so use month name as anchor.
    # Specific entries for each month to avoid subtitle mismatch issues.
    "Editorial: A Happier Life in the New Year": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?JANUARY"
    ),
    "Editorial: True Love and Beauty": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?FEBRUARY"
    ),
    "Editorial: The Lifeblood of Relief Society": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?MARCH"
    ),
    "Editorial: For Liberty": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?APRIL"
    ),
    "Editorial: Portrait of Mother": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?MAY"
    ),
    "Editorial: The 124th Annual Church Conference": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?JUNE"
    ),
    "Editorial: The Responsibilities of Freedom": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?JULY"
    ),
    "Editorial: Conversation in the Home": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?AUGUST"
    ),
    # September has 2 editorials - first one has EDITORIAL VOL. 41 SEPTEMBER header
    "Editorial: Congratulations to President David O. McKay on His Eighty-First Birthday": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?SEPTEMBER"
    ),
    # Second September editorial follows later without its own VOL. header
    "Editorial: Be Ye Kind": (
        r"EDITORIAL\s+ness\s+of\s+the\s+wit"
    ),
    "Editorial: The Heritage of Relief Society": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?OCTOBER"
    ),
    "Editorial: Praise and Thanksgiving": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?NOVEMBER"
    ),
    "Editorial: The 125th Semi-Annual Church Conference": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL[\s\S]{0,20}?VOL\.\s*41[\s\S]{0,30}?DECEMBER"
    ),
    # Second December editorial - follows later in the text
    "Editorial: The Meaning of December for Latter-day Saints": (
        r"EDITORIAL\s+(?:T|S)eaching\s+with\s+testimony"
    ),
    # Literature: body has "Literature of England Lesson NN—SUBTITLE"
    "Literature: The Literature of England": (
        r"(?:[A-Za-z\s\-\u2014\u2013:=]{0,30})?"
        r"(?:(?:Th|Sh|Ch)?e\s+)?Literature\s+of\s+England"
    ),
    "Literature: ": (
        r"(?:[A-Za-z\s\-\u2014\u2013:=]{0,30})?"
        r"(?:(?:Th|Sh|Ch)?e\s+)?Literature\s+of\s+England"
        r"[\s\S]{0,80}?"
        r"Lesson\s+\d+"
        r"[\s\-\u2014\u2013=]*"
    ),
    # Social Science: wildly variable OCR - "Social Serenco—", "Slocant Gecence—", "Socal Sciehee—",
    #   "Sepeall Neiedeco—" (Oct). Second word can start with S or N.
    #   Constitution OCR: "Constinien" (Dec), "Constitution" (clean), etc.
    "Social Science: The Constitution of the United States": (
        r"(?:S\w{2,6}l?)\s+(?:S\w{3,8}|N\w{3,8}|Gecence|Serenco|Sciehee|Neiedeco)"
        r"[\s\-\u2014\u2013:]*"
        r"(?:(?:Th|Sh|Ch|Tke|Ske)?e?\s+)?Const(?:i|t)(?:tu|iu|ni)(?:tion|tien|en|ion|ien)\s+of\s+(?:th|sh|ch)e\s+United\s+States"
    ),
    "Social Science: ": (
        r"(?:S\w{2,6}l?)\s+(?:S\w{3,8}|N\w{3,8}|Gecence|Serenco|Sciehee|Neiedeco)"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Work Meeting: OCR variants "Work Ileeting", "Work IT leeting", "Vork IT leeting"
    # Jan-May: "Family Money Management" series; Jul+: "Selection, Care, and Use of Household Equipment"
    # Some months have "Work Meeting—Selection, Care..." header; others just "Selection, Care..."
    "Work Meeting: Selection, Care, and Use of Household Equipment": (
        r"(?:[\(]?(?:W|V)ork\s+(?:IT\s*|I|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*)?"
        r"(?:S|J)election,?\s+Care"
    ),
    # Oct specific: "Irons and Ironers" (subtitle only, no Work Meeting prefix)
    "Work Meeting: Irons and Ironers": (
        r"(?:[\(]?(?:W|V)ork\s+(?:IT\s*|I|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*)?"
        r"(?:S|J)election,?\s+Care,?\s+and\s+Use\s+of\s+.{0,20}?(?:H|F)ousehold\s+Equipment"
    ),
    "Work Meeting: Family Money Management": (
        r"[\(]?(?:W|V)ork\s+(?:IT\s*|I|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:F|St)amily\s+(?:M|IT|Nl)oney\s+(?:M|IT|Nl)anagement"
    ),
    # Jan/Feb Work Meeting: body says "Family Money Management" but TOC has specific lesson titles
    "Work Meeting: Spending Your Home Furnishings Dollar -- Soft Floor Coverings": (
        r"[\(]?(?:W|V)ork\s+(?:IT\s*|I|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:F|St)amily\s+(?:M|IT|Nl)oney\s+(?:M|IT|Nl)anagement"
    ),
    "Work Meeting: Spending the Home Furnishings Dollar -- Linens": (
        r"[\(]?(?:W|V)ork\s+(?:IT\s*|I|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:F|St)amily\s+(?:M|IT|Nl)oney\s+(?:M|IT|Nl)anagement"
    ),
    "Work Meeting: ": (
        r"[\(]?(?:W|V)ork\s+(?:IT\s*|I|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St)rom\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    "Notes from the Field: ": (
        r"(?:Notes?\s+)?(?:F|St|f|st)rom\s+(?:Th|Sh|Ch|th|sh|ch)e\s+(?:F|St|f|st)ield"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"(?:(?:N|W|V)ot(?:e|ed)?s?\.?\s+)?(?:T|S|J|\(|1)(?:O|o|0)\s+(?:TH|Th|Sh|Ch|T)(?:H|h)?(?:E|e)\s+(?:F|St)(?:I|i|l)(?:E|e)(?:L|l)(?:D|d)"
        r":?\s*"
    ),
    # Serial fiction: The Deeper Melody (Chapters 4-8, continuing from Vol40)
    "The Deeper Melody, Chapter 8": _DEEPER_MELODY_CHAPTER_PAT + r"(?:8|VIII)\b",
    "The Deeper Melody, Chapter 7": _DEEPER_MELODY_CHAPTER_PAT + r"(?:7|VII)\b",
    "The Deeper Melody, Chapter 6": _DEEPER_MELODY_CHAPTER_PAT + r"(?:6|VI)\b",
    "The Deeper Melody, Chapter 5": _DEEPER_MELODY_CHAPTER_PAT + r"(?:5|V)\b",
    "The Deeper Melody, Chapter 4": _DEEPER_MELODY_CHAPTER_PAT + r"(?:4|IV)\b",
    # Serial fiction: The Falling Shackles (Chapters 1-4)
    "The Falling Shackles, Chapter 4": _FALLING_SHACKLES_CHAPTER_PAT + r"(?:4|IV)\b",
    "The Falling Shackles, Chapter 3": _FALLING_SHACKLES_CHAPTER_PAT + r"(?:3|III)\b",
    "The Falling Shackles, Chapter 2": _FALLING_SHACKLES_CHAPTER_PAT + r"(?:2|II)\b",
    "The Falling Shackles, Chapter 1": _FALLING_SHACKLES_CHAPTER_PAT + r"(?:1|I)\b",
    # Serial fiction: Contentment Is a Lovely Thing (Chapters 1-3)
    "Contentment Is a Lovely Thing, Chapter 3": _CONTENTMENT_CHAPTER_PAT + r"(?:3|III)\b",
    "Contentment Is a Lovely Thing, Chapter 2": _CONTENTMENT_CHAPTER_PAT + r"(?:2|II)\b",
    "Contentment Is a Lovely Thing, Chapter 1": _CONTENTMENT_CHAPTER_PAT + r"(?:1|I)\b",
    # In Memoriam entries
    "In Memoriam -- Matthew Cowley": (
        r"(?:I|Y|G)n\s+(?:M|N|IT)(?:l)?emor(?:i|t)(?:a|t)(?:m|am)"
        r"[\s\-\u2014\u2013~=]*"
        r"(?:M|N|IT)atthew\s+Cowley"
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol41"
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
        description="Extract Relief Society Magazine Vol 41 into individual entries"
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

    for (vol, issue_key), entries in VOL41_TOC.items():
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
