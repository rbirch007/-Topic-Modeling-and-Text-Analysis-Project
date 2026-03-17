#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 29 (1942).

The centennial year of Relief Society. This volume features the serial
autobiography "In Retrospect" by President Amy Brown Lyman (Chapters I-XII),
centennial celebrations, and wartime content.

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Usage:
    python extract_vol29.py
    python extract_vol29.py --dry-run
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

VOL29_TOC = {
    ("Vol29", "No01_January_1942"): [
        {"title": "A Tribute to the Relief Society", "author": "Rudger Clawson", "etype": "article"},
        {"title": "In Retrospect Chapter I", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "A Monument With a Message", "author": "Mary Grant Judd", "etype": "article"},
        {"title": "Prize-Winning Poems Eliza Roxey Snow Memorial Poem Contest", "author": None, "etype": "article"},
        {"title": "Prize-Winning Story Relief Society Centennial Short Story Contest", "author": None, "etype": "article"},
        {"title": "Women and the Church", "author": "Stephen L Richards", "etype": "article"},
        {"title": "The Great Fig Tree", "author": "Joseph J. Cannon", "etype": "article"},
        {"title": "Children's Books", "author": None, "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Fashions of Yesterday", "author": None, "etype": "article"},
        {"title": "Participation of Relief Society in Red Cross Work", "author": None, "etype": "article"},
        {"title": "The Wilderness", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "There's Always a New Beginning", "author": "Mary E Knowles", "etype": "fiction"},
        {"title": "The Merry-Go-Round Chapter Ten", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Our Centennial", "author": None, "etype": "editorial"},
        {"title": "Theology and Testimony Articles of Faith, by James E. Talmage, Chapter 7", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Sabbath Day Observance as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business Children's Clothing", "author": None, "etype": "lesson"},
        {"title": "Literature Humor About and For Children", "author": None, "etype": "lesson"},
        {"title": "Social Science Economic Aspects of Family Life", "author": None, "etype": "lesson"},
        {"title": "To a Pioneer Lady", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "Sanctuary", "author": "Alice L. Eddy", "etype": "poem"},
        {"title": "Snow", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Brave Privileged Feet", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Spacious Century 1842-1942", "author": "Vesta Pierce Crawford", "etype": "poem"},
        {"title": "Centennial", "author": "Zella A. Johnson", "etype": "poem"},
        {"title": "The Second Mile", "author": "Gertrude Perry Stanton", "etype": "poem"},
        {"title": "And Tell of Time", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Pioneer of Women", "author": "Elsie F. Parton", "etype": "poem"},
    ],

    ("Vol29", "No02_February_1942"): [
        {"title": "This Happened to Me", "author": "Rae B. Barker", "etype": "article"},
        {"title": "In Retrospect Chapter II", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Ann Parkinson Nibley", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Excerpts from Woman's Exponent, 1872-1877", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Lovest Thou Me More Than These?", "author": "O. F. Ursenbach", "etype": "article"},
        {"title": "Little Habits That Grow Into Ruling Forces", "author": "Lella Marler Hoggan", "etype": "article"},
        {"title": "Vegetables for Vitality for Victory", "author": "James M. Kirkham", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Inheritance", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "The Merry-Go-Round (concluded)", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Sources of Spiritual Refreshment", "author": None, "etype": "editorial"},
        {"title": "Theology and Testimony Articles of Faith, by James E. Talmage, Chapter 8", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Obeying the Commandments of God as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business Care of Clothing", "author": None, "etype": "lesson"},
        {"title": "Literature Importance of a Sense of Humor", "author": None, "etype": "lesson"},
        {"title": "Social Science Future of Marriage and Family Life", "author": None, "etype": "lesson"},
        {"title": "High Dreams", "author": "Linnie Fisher Robinson", "etype": "poem"},
        {"title": "Valentines", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Two Of Me", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Art Of Living", "author": "Gertrude Perry Stanton", "etype": "poem"},
        {"title": "To My Father", "author": "Beatrice Rordame Parsons", "etype": "poem"},
        {"title": "Songs of the Pioneers", "author": "Leslie Derbyshire", "etype": "poem"},
    ],

    ("Vol29", "No03_March_1942"): [
        {"title": "The Female Relief Society of Nauvoo", "author": "Eliza R. Snow", "etype": "article"},
        {"title": "In Retrospect Chapter III", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "The Promise of the Gospel", "author": "David O. McKay", "etype": "article"},
        {"title": "Relief Society General Presidents", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "The Rise of the Women's Auxiliaries", "author": "Clarissa A. Beesley", "etype": "article"},
        {"title": "The Joy of Progression", "author": "Leah D. Widtsoe", "etype": "article"},
        {"title": "The Relief Society Magazine Honor Roll", "author": None, "etype": "article"},
        {"title": "Relief Society Passes Membership Goal", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Men Must Work", "author": "Vera Hinckley Mayhew", "etype": "fiction"},
        {"title": "Anniversary Thoughts", "author": "Amy Brown Lyman", "etype": "editorial"},
        {"title": "Morning Prayer", "author": "Norma Wrathall", "etype": "poem"},
        {"title": "Mary-Martha Am I", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Symbol", "author": "William A. Forsyth", "etype": "poem"},
    ],

    ("Vol29", "No04_April_1942"): [
        {"title": "Relief Society Celebrates its Centennial", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "We Are At War How Shall We Meet It?", "author": "Bryant S. Hinckley", "etype": "article"},
        {"title": "Family Prayer", "author": "Ray R. McKay", "etype": "article"},
        {"title": "Personal Attitudes Toward Tithing", "author": "T. Edgar Lyon", "etype": "article"},
        {"title": "Morale: What Does It Mean To Me?", "author": "Mark K. Allen", "etype": "article"},
        {"title": "In Retrospect Chapter IV", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "What Fiction Shall I Read?", "author": "Harriet P. Johnson", "etype": "article"},
        {"title": "The Way to Peace", "author": "Alice B. Castleton", "etype": "article"},
        {"title": "Little Dreams That Grow Into Great Achievements", "author": "Lella Marler Hoggan", "etype": "article"},
        {"title": "The Luxuriant Home", "author": "Ruth Moody Ostegar", "etype": "fiction"},
        {"title": "The Significance of Centennial", "author": None, "etype": "editorial"},
        {"title": "Mother's Day Sonnet", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "The Old House", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Wanderlust", "author": "Sylvia Probst", "etype": "poem"},
        {"title": "This Then Is Easter", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Centennial Poem", "author": "Ruth May Fox", "etype": "poem"},
    ],

    ("Vol29", "No05_May_1942"): [
        {"title": "Lucy Mack Smith", "author": "Donnette S. Kesler", "etype": "article"},
        {"title": "A Mission to End War", "author": "Albert R. Lyman", "etype": "article"},
        {"title": "In Retrospect Chapter V", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Relief Society Membership Goal Achieved", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Centennial Music", "author": "Florence J. Madsen", "etype": "article"},
        {"title": "Trends in Textiles", "author": "Margaret Olsen", "etype": "article"},
        {"title": "Weapons Women May Use", "author": "James M. Kirkham", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes From the Field", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Mothers in War", "author": "Donna D. Sorensen", "etype": "editorial"},
        {"title": "Night Rain", "author": "Alice L. Eddy", "etype": "poem"},
    ],

    ("Vol29", "No06_June_1942"): [
        {"title": "Recreation in Pioneer Days", "author": "Preston Nibley", "etype": "article"},
        {"title": "Preserve the Child's Family Life in Wartime", "author": "Hazel M. Peterson", "etype": "article"},
        {"title": "Children in Wartime", "author": "Rulon W. Clark", "etype": "article"},
        {"title": "In Retrospect Chapter VI", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "America Needs You Strong", "author": "Rose H. Widtsoe", "etype": "article"},
        {"title": "Little Paths of Peace to Follow", "author": "Lella Marler Hoggan", "etype": "article"},
        {"title": "Our Need of Rhythm", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Lesson Previews 1942-43", "author": None, "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "It's Gotta Be Cash", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Sacrament Meetings", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Portrait of a Pioneer Man", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "To Warm the Heart", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Pioneer", "author": "Inez B. Allred", "etype": "poem"},
    ],

    ("Vol29", "No07_July_1942"): [
        {"title": "Education in Early Utah", "author": "L. H. Creer", "etype": "article"},
        {"title": "Long May It Wave", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Preserving Our Homes in Wartime", "author": "Mark K. Allen", "etype": "article"},
        {"title": "In Retrospect Chapter VII", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Reading for Understanding", "author": "Harriet P. Johnson", "etype": "article"},
        {"title": "Grandmother Found a Way", "author": "Mabel Harmer", "etype": "article"},
        {"title": "The Lure of Melody", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "The Century of Light", "author": "Dorothy Oakley Rea", "etype": "fiction"},
        {"title": "All of Hers and Half of His", "author": "Caroline Eyring Miner", "etype": "fiction"},
        {"title": "Wherefore Have We Fasted?", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Theology and Testimony Articles of Faith, By James E. Talmage, Chapter 9", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Scripture Reading as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Literature Why the Bible Should be Studied as Literature", "author": None, "etype": "lesson"},
        {"title": "Social Science America, The Land of Promise", "author": None, "etype": "lesson"},
        {"title": "Miracle", "author": "Belle Watson Anderson", "etype": "poem"},
        {"title": "Told At Sunset", "author": "Grace Zenor Pratt", "etype": "poem"},
        {"title": "A Shirt Tale", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Nocturnal Visitors", "author": "Olive C. Wehr", "etype": "poem"},
        {"title": "Worthy To Be Called Friend", "author": "Gertrude Perry Stanton", "etype": "poem"},
        {"title": "To The Nauvoo Bell", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "My Son Passes", "author": "Zina Card Brown", "etype": "poem"},
    ],

    ("Vol29", "No08_August_1942"): [
        {"title": "Under Divine Charter", "author": "Leo J. Muir", "etype": "article"},
        {"title": "The Social Effects of Alcohol", "author": "Harold T Christensen", "etype": "article"},
        {"title": "How Shall We Preserve the Mental Health of Our Children in Wartime?", "author": "Mark K. Allen", "etype": "article"},
        {"title": "In Retrospect Chapter VIII", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Three Styles or Textures of Musical Composition", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Bend in the River", "author": "Iris Schow", "etype": "fiction"},
        {"title": "Whither Thou Goest", "author": "LaRene King Bleecker", "etype": "fiction"},
        {"title": "Let Your Light So Shine", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Theology and Testimony Articles of Faith, By James E. Talmage, Chapter 10", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Reverence as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business Peace vs. Wartime Habits", "author": None, "etype": "lesson"},
        {"title": "Literature The Background of the Bible", "author": None, "etype": "lesson"},
        {"title": "Social Science Our Heritage", "author": None, "etype": "lesson"},
        {"title": "Trees", "author": "Elsie E. Barrett", "etype": "poem"},
        {"title": "Give us This Day", "author": "Vesta P. Crawford", "etype": "poem"},
    ],

    ("Vol29", "No09_September_1942"): [
        {"title": "Added Beauty and Utility for Tabernacle", "author": "Howard Barker", "etype": "article"},
        {"title": "First Curtain Made for Tabernacle", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "The Great Certainties", "author": "Levi Edgar Young", "etype": "article"},
        {"title": "Woman's Place in American Radio", "author": "Dorothy Lewis", "etype": "article"},
        {"title": "Parents, What of Youth in a War-Torn World?", "author": "Mark K. Allen", "etype": "article"},
        {"title": "In Retrospect Chapter IX", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "The 'Work' of Work-and-Business Day", "author": "Priscilla L. Evans", "etype": "article"},
        {"title": "Handwork of Pioneer Days", "author": "Emeline Winsor McBride", "etype": "article"},
        {"title": "What Wartime Price Control Means", "author": "H. Grant Ivins", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Helpin' Out", "author": "Ruth Savage Hilton", "etype": "fiction"},
        {"title": "Grains of Gold", "author": "Ora Pate Stewart", "etype": "fiction"},
        {"title": "The Relief Society Educational Season 1942-43", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Theology and Testimony The Church and its Plan of Organization", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Cultivating the Mind as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business Thrift in Food Buying", "author": None, "etype": "lesson"},
        {"title": "Literature What the Bible Is", "author": None, "etype": "lesson"},
        {"title": "Calm", "author": "Rose Thomas Graham", "etype": "poem"},
    ],

    ("Vol29", "No10_October_1942"): [
        {"title": "Reaping of a Century", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "The First Line of Defense The Home", "author": "Rufus K. Hardy", "etype": "article"},
        {"title": "In Retrospect Chapter X", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Little Memories That Sweeten Life", "author": "Nellie O. Parker", "etype": "article"},
        {"title": "Give Thanks For What", "author": "Justin C. Stewart", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "The Development of New Leaders", "author": None, "etype": "editorial"},
        {"title": "Theology and Testimony Spiritual Gifts", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Interest in Missionary work as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business Eliminate Household Wastes", "author": None, "etype": "lesson"},
        {"title": "Literature Types of Literature in the Bible Prose", "author": None, "etype": "lesson"},
        {"title": "Social Science The Declaration of Independence", "author": None, "etype": "lesson"},
        {"title": "Dreams", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "A Woman's Awakening", "author": "Ruby Johnson", "etype": "poem"},
    ],

    ("Vol29", "No11_November_1942"): [
        {"title": "The 113th Semi-annual Conference of Church", "author": "John A. Widtsoe", "etype": "article"},
        {"title": "Joseph F. Smith Patriarch to the Church", "author": "Richard L. Evans", "etype": "article"},
        {"title": "Give Thanks", "author": "Alice Morrey Bailey", "etype": "article"},
        {"title": "Shall the Youth of Zion Falter?", "author": "Rulon W. Clark", "etype": "article"},
        {"title": "In Retrospect Chapter XI", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Mental Health Through Recreation", "author": "Mark K. Allen", "etype": "article"},
        {"title": "Happenings", "author": "Annie Wells Cannon", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Christmas at Aunt Sally's", "author": "Jeanette McKay Morrell", "etype": "fiction"},
        {"title": "President Heber J. Grant", "author": None, "etype": "editorial"},
        {"title": "Theology and Testimony The Holy Bible", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Obedience to Civil Law as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business Conserve Heat and Light", "author": None, "etype": "lesson"},
        {"title": "Literature Prose Types in the Bible", "author": None, "etype": "lesson"},
        {"title": "Social Science Constitution of the United States", "author": None, "etype": "lesson"},
        {"title": "Overtones", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Mothers' Prayer", "author": "Dorothy Clapp Robinson", "etype": "poem"},
    ],

    ("Vol29", "No12_December_1942"): [
        {"title": "Toward a Lasting Victory", "author": "Carlton Culmsee", "etype": "article"},
        {"title": "In Retrospect Chapter XII", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Mothers' Prayers in a World of War", "author": "Roy A. Welker", "etype": "article"},
        {"title": "Government Specifications on Bed Linen", "author": "Justin C. Stewart", "etype": "article"},
        {"title": "Suggestions for Music Department", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Notes from the Field", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Christmas is Just Another Day", "author": "Beatrice Rordame Parsons", "etype": "fiction"},
        {"title": "Christmas in 1942", "author": None, "etype": "editorial"},
        {"title": "Theology and Testimony The Book of Mormon", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers Seeking the Lovely in Life as an Expression of Faith", "author": None, "etype": "lesson"},
        {"title": "Work-and-Business Care of Household Equipment", "author": None, "etype": "lesson"},
        {"title": "Literature Poetry of the Bible", "author": None, "etype": "lesson"},
        {"title": "Social Science Our Civic Rights and Duties", "author": None, "etype": "lesson"},
        {"title": "A Christmas Tree", "author": "Geneva I. Oldroyd", "etype": "poem"},
        {"title": "Christmas Eve", "author": "Robert Layton", "etype": "poem"},
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
    year = 1942
    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol29_No{no}_{month}_{year}.txt"
    ISSUE_FILES[("Vol29", key)] = (fname, month)


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
    # "I" may OCR as "|" or "l" (pipe/lowercase-L)
    "I": r"(?:I|\||l)",
}

# ---------------------------------------------------------------------------
# Known OCR-mangled section headers for Vol29.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns

# Serial: In Retrospect by Amy Brown Lyman (Chapters I-XII, Jan-Dec 1942)
_IN_RETROSPECT_PAT = (
    r"(?:I|l)n\s+Retrospect"
    r".{0,80}?"
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # --- Lesson patterns (Vol29 uses older-style lesson names) ---
    # The body text format is: "Section Header [series title] Lesson NN Subtitle"
    # The TOC title is:        "Section Header Subtitle"
    # So the known-header pattern must skip the series+lesson prefix to reach
    # the subtitle from the TOC.
    "Theology and Testimony ": (
        r"(?:Th|Sh|Ch)eology\s+(?:and|Gnd)\s+(?:T|S)(?:e|l)stimony"
        r"[\s\S]{0,300}?"  # skip series title + lesson number
    ),
    "Visiting Teachers ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:(?:T|S|C|l)(?:e|l|a)?(?:a|e|c)?(?:ch|h)?(?:er)?s?|Seachers?)\s*"
        r"(?:'?\s*)?"
        r"(?:(?:M|IT|T|m)essages?\s*(?:(?:T|S)o\s+(?:Th|Sh|Ch)e\s+Home\s*)?)?"
        r"[\s\S]{0,300}?"  # skip series title + lesson number
    ),
    "Work-and-Business ": (
        r"Work[\s\-]*and[\s\-]*(?:B|G)us(?:i|t)n(?:e|a)ss"
        r"[\s\S]{0,300}?"  # skip series title + lesson number
    ),
    "Literature ": (
        r"(?:L|l)iterature"
        r"[\s\S]{0,300}?"  # skip series title + lesson number
    ),
    "Social Science ": (
        r"Social\s+Science"
        r"[\s\S]{0,300}?"  # skip series title + lesson number
    ),
    # EDITORIAL pattern for Vol29
    "EDITORIAL": (
        r"EDITORIAL"
        r"[\s\S]{0,50}?"
        r"Vol\.\s*29"
    ),
    # Serial: In Retrospect chapter patterns
    "In Retrospect Chapter I": _IN_RETROSPECT_PAT + r"(?:I(?![IVX]))\b",
    "In Retrospect Chapter II": _IN_RETROSPECT_PAT + r"(?:II(?!I))\b",
    "In Retrospect Chapter III": _IN_RETROSPECT_PAT + r"III\b",
    "In Retrospect Chapter IV": _IN_RETROSPECT_PAT + r"IV\b",
    "In Retrospect Chapter V": _IN_RETROSPECT_PAT + r"(?:V(?![I]))\b",
    "In Retrospect Chapter VI": _IN_RETROSPECT_PAT + r"VI(?!I)\b",
    "In Retrospect Chapter VII": _IN_RETROSPECT_PAT + r"VII(?!I)\b",
    "In Retrospect Chapter VIII": _IN_RETROSPECT_PAT + r"VIII\b",
    "In Retrospect Chapter IX": _IN_RETROSPECT_PAT + r"IX\b",
    "In Retrospect Chapter X": _IN_RETROSPECT_PAT + r"(?:X(?![I]))\b",
    "In Retrospect Chapter XI": _IN_RETROSPECT_PAT + r"XI(?!I)\b",
    "In Retrospect Chapter XII": _IN_RETROSPECT_PAT + r"XII\b",
    # Other recurring section headers
    "Notes from the Field": (
        r"Notes?\s+(?:F|St)rom\s+(?:the|(?:Th|Sh|Ch)e)\s+(?:F|St)ield"
    ),
    "Notes to the Field": (
        r"Notes?\s+(?:T|S)o\s+(?:the|(?:Th|Sh|Ch)e)\s+(?:F|St)ield"
    ),
    "Happenings": (
        r"(?:HAPPENINGS|Happenings)"
    ),
    "LESSON DEPARTMENT": (
        r"LESSON\s+DEPARTMENT"
    ),
    "Merry-Go-Round": (
        r"(?:Th|Sh|Ch)e\s+(?:M|IT)erry[\s\-]*(?:G|C)o[\s\-]*(?:R|r)ound"
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
        "Published by the General Board",
        "Owned and Published by the General Board",
        "Organ of the Relief Society",
        "ORGAN OF THE RELIEF SOCIETY",
        "General Board Association",
        "General Manager",
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

            # If the body still starts with TOC content (e.g. when the
            # marker is at the very start of the file and the TOC follows
            # the copyright boilerplate), skip past the TOC.
            toc_in_body = re.search(r'\nContent', body[:2000], re.IGNORECASE)
            if toc_in_body:
                # Find the end of the TOC: poetry listing ends with "NNN."
                # followed by newlines, or double newline after last entry
                toc_search = body[toc_in_body.start():toc_in_body.start() + 5000]
                poetry_end = re.search(
                    r'\d{3}\.\s*\n', toc_search
                )
                if poetry_end:
                    toc_end_pos = toc_in_body.start() + poetry_end.end()
                    front_matter = front_matter + body[:toc_end_pos]
                    body = body[toc_end_pos:]
                else:
                    # Fallback: find the Poetry section header and then the
                    # next double-newline after it (end of poetry listing).
                    poetry_hdr = re.search(r'(?:\n|\s)Poetry\s', toc_search, re.IGNORECASE)
                    if poetry_hdr:
                        after_poetry = toc_search[poetry_hdr.end():]
                        # Find end of poetry listing: double newline, or
                        # transition to prose (sentence with 50+ chars)
                        dbl_nl = re.search(r'\n\n', after_poetry)
                        # Also look for a 3-digit page number followed by
                        # a space and then a long stretch without more
                        # page numbers (indicates body text has started)
                        if dbl_nl:
                            toc_end_pos = toc_in_body.start() + poetry_hdr.end() + dbl_nl.end()
                            front_matter = front_matter + body[:toc_end_pos]
                            body = body[toc_end_pos:]
                    else:
                        # Last resort: find "Lesson Department" or last
                        # 3-digit number in the TOC and skip past it
                        lesson_dept = re.search(r'Lesson\s+Department', toc_search, re.IGNORECASE)
                        if lesson_dept:
                            # Find the next double-newline or long text after
                            after_ld = toc_search[lesson_dept.start():]
                            dbl_nl = re.search(r'\n\n', after_ld)
                            if dbl_nl:
                                toc_end_pos = toc_in_body.start() + lesson_dept.start() + dbl_nl.end()
                                front_matter = front_matter + body[:toc_end_pos]
                                body = body[toc_end_pos:]

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
    r'where|which|while|after|other|what|your|before'
    r"|new|season's|year's|today's|church's|society's"
    r')\s*$',
    re.IGNORECASE
)


# Lesson entry types that should only match within the Lesson Department section
_LESSON_TITLE_PREFIXES = {
    # New-style (Vol57 1970)
    "Spiritual Living", "Homemaking", "Homemaking Meeting",
    "Homemaking Meeting\u2014", "Homemaking Discussion",
    "Social Relations", "Cultural Refinement",
    "Visiting Teacher Message", "Visiting Teacher Messages",
    "Summer Months Sewing Course",
    # Nov/Dec multi-lesson format prefixes
    "Spiritual Living Lesson", "Visiting Teacher Messages Message",
    "Homemaking Discussion", "Social Relations Lesson",
    "Cultural Refinement Lesson",
    # Sep lesson art references
    "\u201cLa Bonne Mere\u201d", "\u201cLittle White Girl\u201d",
    # Jun lesson art references
    "Sunflowers by Claude Monet", "Temple Site by Dale T. Fletcher",
}

# Pattern to find the "LESSON DEPARTMENT" section header in the body.
# The OCR garbles this header in many ways:
#   "LESSON DEPARTMENT", "LESS N DEPARTMENT", "LESSON DE N DEPARTMENT",
#   "LESSON DEP. DEPARTMENT", "Lnsson DEPARTMENT", "SON DEPARTMENT"
# The most reliable anchor is DEPARTMENT immediately followed by Sheology/Theology.
_LESSON_DEPT_RE = re.compile(
    r'(?:LESS.{0,10}N\s+(?:DE.{0,6}\s+)?)?DEPARTMENT[\s|:]+(?:Sh|Th|Ch|Sp)(?:eology|iritual)',
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

            # Signal: match is inside parentheses — e.g. "(Sugar Cookies)"
            # is a parenthetical subtitle, not a standalone heading.
            # Check for opening paren before and closing paren shortly after.
            paren_before = (pos > 0 and body[pos - 1] == '(') or \
                           (pos > 1 and body[pos - 2:pos] in ('( ', '("'))
            if paren_before:
                # Check for closing paren within 80 chars after match
                after_match = body[m.end():m.end() + 80]
                if ')' in after_match:
                    score -= 200  # definitely inside parens
                else:
                    score -= 60

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

            # Inside parentheses — parenthetical subtitle, not heading
            if pos > 0 and body[pos - 1] == '(':
                score -= 60
            elif pos > 1 and body[pos - 2:pos] in ('( ', '("'):
                score -= 60

            if score > best_score:
                best_score = score
                best_pos = pos

        # For short single-word titles, require author proximity even
        # in the second pass (sequential ordering alone is not enough)
        title_words = entry["title"].split()
        is_short_common = len(title_words) <= 2 and len(entry["title"]) < 10
        is_poem_with_author = entry.get("etype") == "poem" and entry.get("author")
        if is_short_common and is_poem_with_author and best_score < 50:
            # Don't update — keep the first pass position or skip
            pass
        elif is_short_common and not is_poem_with_author and best_score < 30:
            pass
        else:
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

            # Reject if preceded by a name-list pattern (consecutive proper
            # names separated by whitespace, typical of board member lists)
            if re.search(
                r'[A-Z][a-z]+\s+[A-Z]\.?\s+[A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z]\.?\s+[A-Z][a-z]+\s*$',
                before_stripped
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
    re.compile(r'(?:LESS.{0,10}N\s+(?:DE.{0,6}\s+)?)?DEPARTMENT[\s|:]+(?:Sh|Th|Ch|Sp)(?:eology|iritual)', re.IGNORECASE),
    # "Woman's Sphere" (recurring section)
    re.compile(r"(?:W|V)oman.s\s+(?:Sp|S)here", re.IGNORECASE),
]

# Section-header patterns for detecting lesson boundaries within chunks.
# These match the OCR-garbled headers without requiring the specific lesson title.
_LESSON_SECTION_HEADERS = [
    # Visiting Teacher Messages
    re.compile(
        r'(?:V|U|\()?(?:i|s|u)?s?iting\s+'
        r'(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?|Seal!?\s*He)\s*'
        r'(?:M|IT|T|m|He|HeIT)\s*(?:l)?essages?',
        re.IGNORECASE
    ),
    # Spiritual Living
    re.compile(
        r'(?:Sp|S)iritual\s+(?:L|l)iving[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Homemaking
    re.compile(
        r'(?:H|(?:IT|Tl))omemaking[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Social Relations
    re.compile(
        r'Social\s+(?:R|r)elations[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Cultural Refinement
    re.compile(
        r'Cultural\s+(?:R|r)efinement[\s\u2014\u2013\-:]+',
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

    # Build a set of all author names from TOC entries for detection.
    # Only include authors whose entries were actually matched — an
    # unmatched entry's author appearing in the text is not a reliable
    # boundary signal (e.g. "Snickerdoodles (Sugar Cookies) Myrtle E.
    # Henderson" when "Sugar Cookies" wasn't matched as its own entry).
    _all_author_names = set()
    _all_author_res = {}
    for e in entries:
        if e.get("author") and e["title"] in matched_titles:
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

                # Check if match is inside parentheses — subtitle, not heading
                paren_before = (pos > 0 and chunk[pos - 1] == '(') or \
                               (pos > 1 and chunk[pos - 2:pos] in ('( ', '("'))
                if paren_before:
                    after_match = chunk[m.end():m.end() + 80]
                    if ')' in after_match:
                        score -= 200

                # For matched entries appearing in another chunk, require
                # slightly less confidence (they're known real entries).
                # Short single-word titles (like "Why?", "Wind") need
                # stronger evidence to avoid false matches in dialogue.
                oe_title_words = oe_entry["title"].split()
                is_short_title = len(oe_title_words) <= 2 and len(oe_entry["title"]) < 10
                if is_short_title:
                    threshold = 80  # require author + structural break
                else:
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
        # Skip entirely for "Report and Official Instructions" entries
        # which list lesson course names in their body text.
        current_title = entry.get("title", "")
        own_prefix = None
        _skip_lesson_headers = "Report" in current_title and "Instructions" in current_title
        if _is_lesson_entry(entry):
            for prefix in _LESSON_TITLE_PREFIXES:
                if current_title.startswith(prefix):
                    own_prefix = prefix.rstrip(": ")
                    break

        for header_re in (_LESSON_SECTION_HEADERS if not _skip_lesson_headers else []):
            for m in header_re.finditer(chunk, search_offset):
                # Skip if this matches our own section header
                matched_text = m.group()
                if own_prefix and own_prefix.lower().replace(" ", "") in \
                   matched_text.lower().replace(" ", ""):
                    continue

                pos = m.start()
                # Lesson headers in the Lesson Department always follow
                # a page number or are at a structural boundary.
                # Require BOTH: no mid-sentence words AND a structural
                # break or sentence-ending punctuation before the match.
                # This prevents false matches on words like "theology"
                # appearing in running prose (e.g. "the new season's
                # theology course").
                before = chunk[max(0, pos - 80):pos]
                if _MID_SENTENCE_WORDS.search(before):
                    continue  # embedded in prose, not a real header

                # Require structural context — lesson headers always
                # appear at page/section boundaries, not mid-paragraph
                before_stripped = before.rstrip()
                has_structural_break = bool(
                    _STRUCTURAL_BREAK_RE.search(before)
                )
                has_sentence_end = bool(
                    before_stripped and
                    before_stripped[-1] in '.!?""\u201d\u2019'
                )
                # Also accept if preceded by a lowercase word ending
                # in possessive ('s) or other non-guard-list words —
                # these are mid-prose contexts that should be rejected
                # Accept if the matched text is ALL-CAPS — that's a strong
                # structural signal even if preceded by OCR garbage
                # (e.g. "Ee l VISITING TEACHER MESSAGE")
                is_allcaps_header = (
                    matched_text.strip()[:20].isupper() and
                    len(matched_text.strip()) > 5
                )
                has_lowercase_before = bool(
                    before_stripped and
                    before_stripped[-1].isalpha() and
                    before_stripped[-1].islower()
                )
                if has_lowercase_before and not is_allcaps_header:
                    continue  # mid-prose, not a real header
                if not has_structural_break and not has_sentence_end and not is_allcaps_header:
                    continue  # no structural context

                # Reject if the text AFTER the match starts with a verb —
                # indicates prose ("Social science will be presented...")
                # rather than a lesson header ("Social Science — The Dignity...")
                after_header = chunk[m.end():m.end() + 30].lstrip()
                if re.match(
                    r'(?:will|was|is|are|has|had|can|could|should|'
                    r'and|or|at|in|for|the|to)\b',
                    after_header, re.IGNORECASE
                ):
                    continue  # prose mention, not a section header

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
            MAX_POEM_CHARS = 800
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
            r'\(\s*(?:T|S|J)o\s+be\s+conc?(?:luded|tinued)\s*\)',
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
                    # Reject if preceded by a biographical/honorific
                    # context word — indicates the name is mentioned
                    # in prose, not a new article byline
                    before_stripped = before.rstrip()
                    if re.search(
                        r'\b(?:President|Elder|Bishop|Dr|Mrs?|Mr|'
                        r'Sister|Brother|Professor|Apostle|'
                        r'husband|wife|daughter|son|cousin|uncle|'
                        r'aunt|nephew|niece|friend|neighbor|'
                        r'mother|father|children|parents|'
                        r'said|wrote|by|speaker|principal)\s*$',
                        before_stripped, re.IGNORECASE
                    ):
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
                # Reject if followed by comma — indicates an appositive
                # or inline mention (e.g. "Margaret Jones Stephens,
                # tracing the genealogy..."), not a title+author block
                after_raw = chunk[m.end():m.end() + 5]
                if after_raw.startswith(',') or after_raw.startswith(', '):
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
                        r'\b(?:Dr|Mrs?|Mr|husband|wife|daughter|son|'
                        r'cousin|uncle|aunt|nephew|niece|friend|'
                        r'neighbor|brother|sister|mother|father|'
                        r'President|Bishop|Elder|Professor|Apostle|'
                        r'children|parents|speaker|principal|'
                        r'said|wrote|by)'
                        r'[.,;\s]*$',
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
        description="Extract Relief Society Magazine Vol 29 into individual entries"
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
    vol29_out = OUTPUT_DIR / "vol29"
    if vol29_out.exists() and not args.dry_run:
        print(f"Cleaning output directory: {vol29_out}")
        shutil.rmtree(vol29_out)

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    volume_json = {}

    for (vol, issue_key), entries in VOL29_TOC.items():
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
                    # Flag 1: title not found at start of content
                    title_missing = not title_pat.search(head)

                    # Flag 2: suspiciously short content for the entry type
                    # (may indicate truncation)
                    etype = entry_json["etype"]
                    content_len = len(content) if content else 0
                    _MIN_LENGTHS = {
                        "article": 800,
                        "fiction": 1000,
                        "lesson": 1500,
                        "editorial": 600,
                    }
                    min_len = _MIN_LENGTHS.get(etype, 0)
                    suspiciously_short = content_len < min_len if min_len else False

                    if title_missing or suspiciously_short:
                        flag_entry = {
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
                        }
                        if title_missing:
                            flag_entry["title_not_at_start"] = True
                        if suspiciously_short:
                            flag_entry["suspiciously_short"] = True
                            flag_entry["content_length"] = content_len
                            flag_entry["expected_min"] = min_len
                        flagged.append(flag_entry)

            if flagged:
                flagged_path = vol_dir / "flagged_for_review.json"
                with open(flagged_path, "w", encoding="utf-8") as f:
                    json.dump(flagged, f, indent=2, ensure_ascii=False, default=str)
                n_title = sum(1 for f in flagged if f.get("title_not_at_start"))
                n_short = sum(1 for f in flagged if f.get("suspiciously_short"))
                print(f"Flagged for review: {flagged_path} "
                      f"({len(flagged)} entries: "
                      f"{n_title} title-not-at-start, "
                      f"{n_short} suspiciously short)")

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
