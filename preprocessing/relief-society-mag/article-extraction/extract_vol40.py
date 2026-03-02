#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 40 (1953).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol40.py
    python extract_vol40.py --dry-run
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

VOL40_TOC = {
    ("Vol40", "No01_January_1953"): [
        {"title": "New Year's Greetings", "author": None, "etype": "article"},
        {"title": "Testimony Through Reading The Book of Mormon", "author": "Matthew Cowley", "etype": "article"},
        {"title": "Elder John A. Widtsoe", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Award Winners: Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "No Barren Bough", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Goodbye to Her Son", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "The Greater Love", "author": "Eleanor W. Schow", "etype": "poem"},
        {"title": "Biographical Sketches of Award Winners", "author": None, "etype": "article"},
        {"title": "Award Winners: Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Forever After", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "We Must Unite to Conquer Polio", "author": "Basil O'Connor", "etype": "article"},
        {"title": "The Value and Use of Audio-Visual Teaching Aids in Relief Society", "author": "Leone G. Layton", "etype": "article"},
        {"title": "The Cleruet", "author": "Mary C. Martineau", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 7", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Gifts of Prayer", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Elder Ezra Taft Benson Appointed Secretary of Agriculture", "author": None, "etype": "article"},
        {"title": "In Memoriam -- Isabel Barton Callister", "author": None, "etype": "article"},
        {"title": "Relief Society Monument at Nauvoo Moved", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Relief Society Assigned Evening of Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Irene Burch Sutton", "author": None, "etype": "article"},
        {"title": "Theology: Nephi's Predictions, Promises, and Instructions", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For They Who Are Not for Me Are Against Me", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Kinds of Income", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Jane Austen", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Kingdom of God", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "New Snow", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Christmas", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Beyond Confining Bounds", "author": "Elena Hassell Stanley", "etype": "poem"},
        {"title": "Reflection", "author": "Mirla Greenwood Thayne", "etype": "poem"},
    ],

    ("Vol40", "No02_February_1953"): [
        {"title": "The Fluorescence of the Lamanites", "author": "Spencer W. Kimball", "etype": "article"},
        {"title": "Wings of the Morning", "author": "Ruth Young", "etype": "article"},
        {"title": "Morning's at Seven", "author": "Mabel S. Harmer", "etype": "fiction"},
        {"title": "Charlie's Wife", "author": "Shirley Sargent", "etype": "fiction"},
        {"title": "Trite, But True", "author": "Iris W. Schow", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 8", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Whosoever Will Be Great Among You", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Birthday Greetings to President Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "A Party That's Different", "author": "Yvonne Child Hiss", "etype": "article"},
        {"title": "Adventure in Glass -- VII. Old Glass in New Houses", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Theology: The Words of Jacob Recorded by Nephi", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And If It So Be That the Children of Men Keep the Commandments", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Use and Distribution of Income", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Readings in Romanticism -- A Review", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Destiny of the Earth and Man", "author": "Archibald F. Bennett", "etype": "lesson"},
        {"title": "Lehi Wakes Again", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Dispossessed", "author": "Zera Wilde Earl", "etype": "poem"},
        {"title": "My Hearth", "author": "Ethel Hopper", "etype": "poem"},
        {"title": "The Familiar Dark", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Night", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Afterthought", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "George Washington", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Old Couple", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Testimony", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Forever Summer", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol40", "No03_March_1953"): [
        {"title": "A Visit to Relief Society Headquarters", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "Chasing the Rainbow", "author": "C. W. McCullough", "etype": "article"},
        {"title": "Answer the Call of the American Red Cross", "author": "Louis C. Boochiver", "etype": "article"},
        {"title": "We Found Spring", "author": "Frances P. Reid", "etype": "article"},
        {"title": "Travel Highlights -- Family Outings in Switzerland", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "On Trains", "author": "Matia McClelland Burk", "etype": "article"},
        {"title": "Marcie's Pink Taffeta", "author": "Norma Wrathall", "etype": "fiction"},
        {"title": "Deflated Dollar", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "The Family Reunion", "author": "Yvonne Child Hiss", "etype": "fiction"},
        {"title": "Celebration", "author": "Florence B. Dunford", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 9", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Dwight D. Eisenhower Inaugurated President of the United States", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Organizations and Reorganizations", "author": None, "etype": "article"},
        {"title": "Official Costume for Singing Mothers", "author": None, "etype": "article"},
        {"title": "Announcing the Special April Short Story Issue", "author": None, "etype": "article"},
        {"title": "By Early Candlelight -- 2. Building a Home in the Valley", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Arts and Crafts Appeal to a Young Mother", "author": None, "etype": "article"},
        {"title": "Today's Dinner", "author": "Loretta P. Epperson", "etype": "article"},
        {"title": "Health to the Bones", "author": "Maryann M. Ukropina", "etype": "article"},
        {"title": "Tricks With Bed Linen", "author": "Leona Bammes Gardner", "etype": "article"},
        {"title": "A Tribute to Visiting Teachers", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "Midnight Moon", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Take-Off", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "Impressive Entrance", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Salute", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "How Shall I Say Your Name?", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Crocus", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Yellow Violets", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Supplication to Spring", "author": "Ina Smith", "etype": "poem"},
        {"title": "Romance", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Personnel for Spring", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Fantasy", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Resonance", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol40", "No04_April_1953"): [
        {"title": "Christ's Visit to the Western Hemisphere Following His Resurrection", "author": "Delbert L. Stapley", "etype": "article"},
        {"title": "The National Council of Women", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Springville -- Utah's Art City", "author": "Willard Luce", "etype": "article"},
        {"title": "Delay Is Dangerous", "author": None, "etype": "article"},
        {"title": "Our April Short Story Writers", "author": None, "etype": "article"},
        {"title": "The Bitter Cup", "author": "Angelyn W. Wadley", "etype": "fiction"},
        {"title": "Old Bias", "author": "Ruth Moody Ostegar", "etype": "fiction"},
        {"title": "No Need to Worry", "author": "Velma D. Cloward", "etype": "fiction"},
        {"title": "The Lasting Joys", "author": "Sylvia Probst Young", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 10", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Spiritualizing Our Homes", "author": "Velma N. Simonsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Martha Ann Walker Has Made Six Hundred Quilts", "author": None, "etype": "article"},
        {"title": "Easter Bunny Review", "author": None, "etype": "article"},
        {"title": "Gardening at Seven Thousand Feet", "author": "C. W. McCullough", "etype": "article"},
        {"title": "By Early Candlelight -- III. A Pioneer Kitchen", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Your Outdoor Living Room", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Recipe for Chocolate Fudge", "author": "Eva Carter", "etype": "article"},
        {"title": "Those Yesterdays", "author": "Grace M. Candland", "etype": "article"},
        {"title": "Bathroom Tricks", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Blind Woman", "author": "Gene Romolo", "etype": "poem"},
        {"title": "If You Would Remember", "author": "Alberta H. Christensen", "etype": "poem"},
        {"title": "April Aspens", "author": "Lizabeth Wall", "etype": "poem"},
        {"title": "Small Girl With a Flower", "author": "Genevieve Groen", "etype": "poem"},
        {"title": "Frock of April", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "Awakening", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Wings of Time", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "April Returns", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Message", "author": "Zera Wilde Earl", "etype": "poem"},
        {"title": "Our Thanks", "author": "Isadora Price", "etype": "poem"},
        {"title": "For David at Easter", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Through a Window", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Spring Riddle", "author": "Lydia Bennett Egbert", "etype": "poem"},
        {"title": "Green Interim", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Communique", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Night Fashions", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Plea", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Day Fades", "author": "Olive McHugh", "etype": "poem"},
    ],

    ("Vol40", "No05_May_1953"): [
        {"title": "Ten Conditions Which Contribute to a Happy Home", "author": "David O. McKay", "etype": "article"},
        {"title": "The Purposefulness of Life", "author": "W. Cleon Skousen", "etype": "article"},
        {"title": "The Coronation of a Beloved Queen", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Travel Highlights -- Highway in Venezuela", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Sweetpeas", "author": "Lois E. Forkner", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 11", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Lucky Two Shoes", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "Karren", "author": "Blanche M. Hollingsworth", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Song Of the Righteous", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "In Memoriam -- Emma Adams Empey", "author": None, "etype": "article"},
        {"title": "Magazine Subscriptions for 1952", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Magazine Honor Roll for 1952", "author": None, "etype": "article"},
        {"title": "The Stranger in My Garden", "author": "Alice Whitson Norton", "etype": "article"},
        {"title": "By Early Candlelight -- IV. A Pioneer Bedroom", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Tricks With Flour Sacks", "author": "Leona Bammes Gardner", "etype": "article"},
        {"title": "A Place Apart", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "After the Feast", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "My Mother", "author": "Marjorie L. Hafen", "etype": "poem"},
        {"title": "The Pepper Tree", "author": "Mary Hess Hackney", "etype": "poem"},
        {"title": "My Gift", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "One Shining Day", "author": "Josephine J. Harvey", "etype": "poem"},
        {"title": "A Mother's Debt", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Little Gray Horse", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Web of Words", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "My Wish for You", "author": "Bernice T. Clayton", "etype": "poem"},
        {"title": "Mother", "author": "Emily Carey Alleman", "etype": "poem"},
        {"title": "About Flowers", "author": "Elaine Swain", "etype": "poem"},
        {"title": "A Humming Bird", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Sea Call", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Silenced", "author": "Hannah C. Ashby", "etype": "poem"},
        {"title": "Spring Bride", "author": "Maxine H. Holbrook", "etype": "poem"},
        {"title": "Gift Divine", "author": None, "etype": "poem"},
    ],

    ("Vol40", "No06_June_1953"): [
        {"title": "No Wonder We Love Him!", "author": "Edith Smith Elliott", "etype": "article"},
        {"title": "Adam S. Bennion Appointed to the Council of the Twelve", "author": "Henry D. Moyle", "etype": "article"},
        {"title": "Five New Members Appointed to the Relief Society General Board", "author": "Leone G. Layton", "etype": "article"},
        {"title": "Charlotte Anderson Larsen", "author": "Bernice L. Robbins", "etype": "article"},
        {"title": "Anna Ballantyne Hart", "author": "Blanche B. Stoddard", "etype": "article"},
        {"title": "Winniefred Spafford Manwaring", "author": "Lillie C. Adams", "etype": "article"},
        {"title": "Florence Jepperson Madsen", "author": "Aleine M. Young", "etype": "article"},
        {"title": "Contest Announcements -- 1953", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Why Study Literature", "author": "Blanche Kendall McKey", "etype": "article"},
        {"title": "Summer Landscape", "author": "Willard Luce", "etype": "article"},
        {"title": "Be Amy", "author": "Amy Viau", "etype": "fiction"},
        {"title": "A Time to Forget, Chapter 12", "author": "Fay Tarlock", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: June Brightness", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Relief Society Summer Schools", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "By Early Candlelight -- V. A Parlor of the Pioneer Home", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Anyone Can Make a Feather Quilt", "author": "Marijane Morris", "etype": "article"},
        {"title": "Fun for Children on the Trip", "author": "Ruth K. Kent", "etype": "article"},
        {"title": "Ham-Pineapple Brochettes", "author": None, "etype": "article"},
        {"title": "Theology: Characters and Teachings of The Book of Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Book of Mormon Gems of Truth", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Family Money Management", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: The Literature of England", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution of the United States", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Suggestions on Teaching Aids for 1953-54 Lessons", "author": "Leone G. Layton", "etype": "lesson"},
        {"title": "Notes on the Author of the Lessons", "author": None, "etype": "lesson"},
        {"title": "The Father in His Field", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Summer Morning", "author": "Luella N. Adams", "etype": "poem"},
        {"title": "June Lure", "author": "Leone O. McCune", "etype": "poem"},
        {"title": "Beyond the Strength of Rock", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "To My Children", "author": "Louise Nicholes", "etype": "poem"},
        {"title": "The Token", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Beauty", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "A Mother's Prayer", "author": "Elsie Scott", "etype": "poem"},
        {"title": "Let Me Take Time for These", "author": "Vilate R. Womack", "etype": "poem"},
        {"title": "Oasis of Home", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Forest Foxgloves", "author": "Vesta Nickerson Lukei", "etype": "poem"},
        {"title": "Long Ago", "author": "Wanda G. Nielson", "etype": "poem"},
        {"title": "Crows", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "With a Page of Poetry", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "To My Daughter", "author": "Grace B. Wilson", "etype": "poem"},
        {"title": "Meditation", "author": "Emily Carey Alleman", "etype": "poem"},
    ],

    ("Vol40", "No07_July_1953"): [
        {"title": "First Ladies of Our Land -- Part I", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "The Road of Memories", "author": "Fredrika Clinch", "etype": "article"},
        {"title": "Special for Redheads, Part I", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "Big Beredy's Skirt", "author": "Blanche Kendall McKey", "etype": "fiction"},
        {"title": "The Fried Shirt", "author": "Mary C. Martineau", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Work -- Not Drudgery", "author": "Velma N. Simonsen", "etype": "editorial"},
        {"title": "Notes to the Field: Program for the November Fast Sunday Evening Meeting", "author": None, "etype": "article"},
        {"title": "Notes from the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "By Early Candlelight -- VI. Industry and Manufacturing in the Pioneer Home", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Hobbies Keep Her Young and Happy", "author": None, "etype": "article"},
        {"title": "Speedy Chow Spaghetti", "author": None, "etype": "article"},
        {"title": "Children's Clothes", "author": "Winona S. Jensen", "etype": "article"},
        {"title": "Theology: Jacob and His Teachings", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: But Before Ye Seek for Riches", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Children's Spending", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: The Maturing Tennyson", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Man's Struggle for Freedom", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Faith of Our Fathers", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "On Wings of Prayer", "author": "Elena Hassell Stanley", "etype": "poem"},
        {"title": "But This He Left", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Dancing by Campfire", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Eyes to See", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Grandmother's Album", "author": "Amy Viau", "etype": "poem"},
        {"title": "Acknowledgment", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Summer Lullabye", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Housewife's Song", "author": "Genevieve Groen", "etype": "poem"},
        {"title": "Song to My Love", "author": "Grace Sayre", "etype": "poem"},
        {"title": "Pioneers", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Prayer", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Release From Fear", "author": "Eunice J. Miles", "etype": "poem"},
    ],

    ("Vol40", "No08_August_1953"): [
        {"title": "Living More Abundantly on Your Income", "author": "William F. Edwards", "etype": "article"},
        {"title": "Family Home Hour -- Maker of Happy Memories", "author": "LaRue C. Longden", "etype": "article"},
        {"title": "In Memoriam -- Elder Stayner Richards", "author": "LeGrand Richards", "etype": "article"},
        {"title": "Gratitude", "author": "Evelyn Dorio", "etype": "article"},
        {"title": "Lisa", "author": "Margery S. Stewart", "etype": "fiction"},
        {"title": "The End of a Season", "author": "Zera Wilde Earle", "etype": "fiction"},
        {"title": "Special for Redheads, Part II", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Riches of Summer", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Achsa E. Paxman and Lilie C. Adams Released From General Board", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Cost of Binding The Relief Society Magazine Increased", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Mime", "author": "Llayne Larsen", "etype": "article"},
        {"title": "A Letter From Mother", "author": "Clara Horne Park", "etype": "article"},
        {"title": "By Early Candlelight -- VII. Arts and Crafts in a Pioneer Home", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Do It Willingly", "author": "Annie S. W. Gould", "etype": "article"},
        {"title": "Pioneer Hobbies Bring Happiness", "author": None, "etype": "article"},
        {"title": "Tenderness", "author": "Kate Richards", "etype": "article"},
        {"title": "Bathroom Tricks -- Make a Towel Tree", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Try Touring Along", "author": "Esther Warburton", "etype": "article"},
        {"title": "Theology: The Completion of the Small Plates of Nephi", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And See That Ye Have Faith, Hope, and Charity", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Your Shopping Dollar", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Alfred Lord Tennyson, Representative Victorian", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Latter-day Saint View of the Constitution of the United States", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Books Dealing With American History Circa 1760-1790", "author": "Helen P. Gibson", "etype": "lesson"},
        {"title": "Western Midsummer Landscape", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Pioneer Woman", "author": "Vilate R. McAllister", "etype": "poem"},
        {"title": "Loneliness", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Lines for a Yearbook", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Evolution", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Instinct", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "These Remain", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Mother's Thimble", "author": "Elsie C. Carroll", "etype": "poem"},
        {"title": "Blue Preferred", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Three Years Old in August", "author": "Emily Carey Alleman", "etype": "poem"},
        {"title": "Long Ago", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "Dusk in Bouquet Canyon", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "The Closed Door", "author": "Carrie Lee Franklin", "etype": "poem"},
        {"title": "Heart's Home", "author": "Bernice T. Clayton", "etype": "poem"},
        {"title": "Whispered Wisdom", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],

    ("Vol40", "No09_September_1953"): [
        {"title": "Ancestry of President David O. McKay", "author": "Jeanette McKay Morrell", "etype": "article"},
        {"title": "In Memoriam -- Elder Albert E. Bowen", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "First Ladies of Our Land -- Part II", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "Travel Highlights -- Farm Scenes Near Lake Geneva", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Elizabeth", "author": "Florence B. Dunford", "etype": "fiction"},
        {"title": "Special for Redheads, Part III, Conclusion", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Congratulations to President David O. McKay on His Eightieth Birthday", "author": None, "etype": "article"},
        {"title": "Editorial: Never Give a Cross... Word", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "New Serial The Deeper Melody to Begin in October", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Annual General Relief Society Conference", "author": None, "etype": "article"},
        {"title": "Annual Report 1952", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Plant Bulbs for Spring Beauty", "author": "Dorthea N. Newbold", "etype": "article"},
        {"title": "By Early Candlelight -- VIII. Music in the Pioneer Home", "author": "Bonnie A. Kesler", "etype": "article"},
        {"title": "Theology: Teachings of King Benjamin", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Behold, He Sendeth an Invitation Unto All Men", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Spending the Food Dollar", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Charles Dickens", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Heritage of Earth", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Six Years, Passed", "author": "Blanche Kendall McKey", "etype": "poem"},
        {"title": "Who Once Has Climbed a Mountain", "author": "Pansye H. Powell", "etype": "poem"},
        {"title": "I Serve Eternity", "author": "Amy Viau", "etype": "poem"},
        {"title": "I Can See America", "author": "Marian Driggs", "etype": "poem"},
        {"title": "September Snow", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Pine Country Autumn", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "Sunflowers", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Memory's Magic Mile", "author": "Francelia Goddard", "etype": "poem"},
        {"title": "Rendezvous", "author": "Genevieve Groen", "etype": "poem"},
        {"title": "Song for a New School Year", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "September", "author": "Beatrice R. Parsons", "etype": "poem"},
    ],

    ("Vol40", "No10_October_1953"): [
        {"title": "The Framers of the Constitution and Their Work", "author": "G. Homer Durham", "etype": "article"},
        {"title": "Portraits of the Signers of the Constitution of the United States", "author": None, "etype": "article"},
        {"title": "Boyhood of President David O. McKay", "author": "Jeanette McKay Morrell", "etype": "article"},
        {"title": "The Lehman Caves of Nevada", "author": "Willard Luce", "etype": "article"},
        {"title": "The Deeper Melody, Chapter 1", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "Pheasants on the Lawn", "author": "Virginia M. Kammeyer", "etype": "fiction"},
        {"title": "Grandpa As a Magician", "author": "Mabel Law Atkinson", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Their Temples and Their Sanctuaries", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Theology: Ammon and the People of Limhi", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Ye Will Not Have a Mind to Injure One Another", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Spending the Clothing Dollar", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Charles Dickens (Continued)", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Place of the Constitution in History", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "The Salt Lake Temple", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "October Twilight", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "Autumn Sunrise", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Homeward", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "October", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Interim", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Remembrance", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Women at Conference", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Autumn Leaves", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Summer's Last Bloom", "author": "Lucy Woolley Brown", "etype": "poem"},
        {"title": "Canning Magic", "author": "Lydia Bennett Egbert", "etype": "poem"},
    ],

    ("Vol40", "No11_November_1953"): [
        {"title": "Testimony, the First Responsibility of Relief Society", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "For Every Man Receiveth Wages", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Charity -- A Gem of Many Facets", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Life of President David O. McKay -- A Few Highlights of a Busy Life", "author": "Jeanette McKay Morrell", "etype": "article"},
        {"title": "Just for the Season", "author": "P. Molly Johnson", "etype": "fiction"},
        {"title": "The Deeper Melody, Chapter 2", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: After Much Tribulation Come the Blessings", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The Renovating and Dressing of Dolls", "author": "Thelma Standering", "etype": "article"},
        {"title": "Thanksgiving Dinner Suggestions", "author": "Ethel C. Smith", "etype": "article"},
        {"title": "Theology: The Record of Zeniff", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Let Not This Pride of Your Hearts Destroy Your Souls", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Spending the Furniture Dollar", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Thomas Babington Macaulay", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Forerunners of the Constitution", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Autumn Lens", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Toward Remembering", "author": "Renie H. Littlewood", "etype": "poem"},
        {"title": "My Father's Sky", "author": "Norma Vance", "etype": "poem"},
        {"title": "Perception", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Homemaking in Action", "author": "Ouida Johns Pedersen", "etype": "poem"},
        {"title": "Fireplace", "author": "Cynthia A. Scott", "etype": "poem"},
        {"title": "Woman-Power", "author": "Emily Carey Alleman", "etype": "poem"},
        {"title": "Rest Period", "author": "Eleanor W. Schow", "etype": "poem"},
        {"title": "Pilgrim Lure", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Autumn Is So Transient", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "November", "author": "Mary Gustafson", "etype": "poem"},
        {"title": "Flame", "author": "Ivy Houtz Woolley", "etype": "poem"},
        {"title": "We Will Remember", "author": "Agnes Just Reid", "etype": "poem"},
    ],

    ("Vol40", "No12_December_1953"): [
        {"title": "Woman's Influence", "author": "David O. McKay", "etype": "article"},
        {"title": "The Annual General Relief Society Conference -- 1953", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "The Relief Society Building Ground-Breaking Ceremony", "author": None, "etype": "article"},
        {"title": "Elder Richard L. Evans Appointed a Member of the Council of the Twelve", "author": "Antoine R. Ivins", "etype": "article"},
        {"title": "Elder Hugh B. Brown Appointed Assistant to the Council of the Twelve", "author": "George Q. Morris", "etype": "article"},
        {"title": "Marion Duff Hanks, New Member of the First Council of the Seventy", "author": "Richard L. Evans", "etype": "article"},
        {"title": "Life of President David O. McKay -- Part IV", "author": "Jeanette McKay Morrell", "etype": "article"},
        {"title": "Good Will Toward Men", "author": "Helen Hinckley Jones", "etype": "article"},
        {"title": "Journey for Christmas", "author": "Katherine F. Larsen", "etype": "fiction"},
        {"title": "The Deeper Melody, Chapter 3", "author": "Alice Morrey Bailey", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 124th Semi-Annual Church Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Editorial: The Precious Things of Earth", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Make Interesting Blocks for Children", "author": "Thalia Black", "etype": "article"},
        {"title": "Making Cloth Books", "author": "Elaine C. Nichols", "etype": "article"},
        {"title": "Creative Courtesy", "author": "Luella N. Adams", "etype": "article"},
        {"title": "A Spice Cake for Special Occasions", "author": "Rose A. Openshaw", "etype": "article"},
        {"title": "Gifts for Yourself and Others", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Party Ideas", "author": "Rose A. Openshaw", "etype": "article"},
        {"title": "Theology: Alma the Elder", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Ye Have Not Applied Your Hearts to Understanding", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Spending the Home Furnishings Dollar -- Curtains and Drapery", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Robert Browning", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Articles of Confederation", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "The Broken Caravan", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Christmas", "author": "Marilyn Odell", "etype": "poem"},
        {"title": "First Christmas", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Old Home", "author": "Jeanette McKay Morrell", "etype": "poem"},
        {"title": "Fashion Note", "author": "Marian Schroder Crothers", "etype": "poem"},
        {"title": "Snowfall", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Renumeration", "author": "Lydia Bennett Egbert", "etype": "poem"},
        {"title": "The Gift She Liked the Best", "author": "Frances C. Yost", "etype": "poem"},
        {"title": "Consolation", "author": "Catherine B. Bowles", "etype": "poem"},
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
    year = 1953

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol40_{key}.txt"
    ISSUE_FILES[("Vol40", key)] = (fname, month)


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
# Known OCR-mangled section headers for Vol40.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns
_TIME_TO_FORGET_CHAPTER_PAT = (
    r"A\s+(?:T|S|J|\()ime\s+(?:t|s|j)o\s+(?:F|St)orget"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)
_SPECIAL_REDHEADS_PART_PAT = (
    r"Special\s+(?:f|st)?or\s+Redheads"
    r".{0,80}?"  # skip author name
    r"(?:(?:PART|Part)\s+)?"
)
_DEEPER_MELODY_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)e\s+Deeper\s+(?:M|IT|Nl|Vl)elody"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # Theology: body has Sheology—Characters and Teachings of The Book of Mormon Lesson NN—"SUBTITLE"
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Visiting Teacher Messages: body has various OCR corruptions
    "Visiting Teacher Messages: Book of Mormon Gems of Truth": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:T|S)(?:e|l)(?:a|e)cher\s+"
        r"(?:M|IT|7T|T|m)(?:l)?ess(?:a|i)?ges"
        r"[\s\-\u2014\u2013:]*"
        r"Book\s+of\s+Mormon\s+Gems\s+of\s+(?:Th|Sh|Ch)ruth"
    ),
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:T|S)(?:e|l)(?:a|e)cher\s+"
        r"(?:M|IT|7T|T|m)(?:l)?ess(?:a|i)?ges"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|]?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+Near\s+(?:a|A)nd\s+(?:F|S)ar"
    ),
    # Editorial: body has EDITORIAL noise VOL. 40 MONTH 1953 <actual title>
    "Editorial: ": (
        r"(?:ED\s*)?IT\s*O?\s*RIAL"
        r"[\s\S]{0,100}?"
    ),
    # Literature: body has garbled prefix then "Literature of England Lesson NN—SUBTITLE"
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
    # Social Science: handles both "Progress of Man" (Jan-May) and "Constitution" (Jul-Dec) series
    "Social Science: The Constitution of the United States": (
        r"Social\s+Sc(?:i|t)ence"
        r"[\s\-\u2014\u2013:]*"
        r"(?:(?:Th|Sh|Ch)?e\s+)?Constitution\s+of\s+(?:th|sh|ch)e\s+United\s+States"
    ),
    "Social Science: ": (
        r"Social\s+Sc(?:i|t)ence"
        r"[\s\-\u2014\u2013:]*"
        r"(?:.*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Work Meeting: handles both "Home Management" (Jan) and "Family Money Management" (Jul+) series
    "Work Meeting: Family Money Management": (
        r"[\(]?(?:W|V)ork\s+(?:IT|T|N|M)?(?:l)?eeting"
        r"[)\s\-\u2014\u2013:]*"
        r"(?:F|St)amily\s+(?:M|IT|Nl)oney\s+(?:M|IT|Nl)anagement"
    ),
    "Work Meeting: ": (
        r"[\(]?(?:W|V)ork\s+(?:IT|T|N|M)?(?:l)?eeting"
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
    # Serial fiction: A Time to Forget (Chapters 7-12, continuing from Vol39)
    "A Time to Forget, Chapter 12": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:12|XII)\b",
    "A Time to Forget, Chapter 11": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:11|XI)\b",
    "A Time to Forget, Chapter 10": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:10|X)\b",
    "A Time to Forget, Chapter 9": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:9|IX)\b",
    "A Time to Forget, Chapter 8": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:8|VIII)\b",
    "A Time to Forget, Chapter 7": _TIME_TO_FORGET_CHAPTER_PAT + r"(?:7|VII)\b",
    # Serial fiction: Special for Redheads (Parts I-III)
    # Longer matches first to avoid prefix-matching bug
    "Special for Redheads, Part III, Conclusion": _SPECIAL_REDHEADS_PART_PAT + r"(?:III|3)\b",
    "Special for Redheads, Part II": _SPECIAL_REDHEADS_PART_PAT + r"(?:II|2)\b",
    "Special for Redheads, Part I": _SPECIAL_REDHEADS_PART_PAT + r"(?:I|1)\b",
    # Serial fiction: The Deeper Melody (Chapters 1-3)
    "The Deeper Melody, Chapter 3": _DEEPER_MELODY_CHAPTER_PAT + r"(?:3|III)\b",
    "The Deeper Melody, Chapter 2": _DEEPER_MELODY_CHAPTER_PAT + r"(?:2|II)\b",
    "The Deeper Melody, Chapter 1": _DEEPER_MELODY_CHAPTER_PAT + r"(?:1|I)\b",
    # In Memoriam entries
    "In Memoriam -- Isabel Barton Callister": (
        r"(?:I|Y)n\s+(?:M|N)(?:l)?emor(?:i|t)(?:a|t)(?:m|am)"
        r"[\s\-\u2014\u2013~]*"
        r"(?:I|Y)sabel"
    ),
    "In Memoriam -- Emma Adams Empey": (
        r"(?:I|Y)n\s+(?:M|N)(?:l)?emor(?:i|t)(?:a|t)(?:m|am)"
        r"[\s\-\u2014\u2013~]*"
        r"Emma"
    ),
    "In Memoriam -- Elder Stayner Richards": (
        r"(?:I|Y)n\s+(?:M|N)(?:l)?emor(?:i|t)(?:a|t)(?:m|am)"
        r"[\s\-\u2014\u2013~]*"
        r"Elder\s+(?:S|J)tayner"
    ),
    "In Memoriam -- Elder Albert E. Bowen": (
        r"(?:I|Y)n\s+(?:M|N)(?:l)?emor(?:i|t)(?:a|t)(?:m|am)"
        r"[\s\-\u2014\u2013~]*"
        r"Elder\s+Albert"
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol40"
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
        description="Extract Relief Society Magazine Vol 40 into individual entries"
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

    for (vol, issue_key), entries in VOL40_TOC.items():
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
