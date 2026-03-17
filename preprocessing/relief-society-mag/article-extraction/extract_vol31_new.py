#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 31 (1944).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol31_new.py
    python extract_vol31_new.py --dry-run
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

VOL31_TOC = {
    ("Vol31", "No1_January_1944"): [
        {"title": "Message", "author": "The General Board of Relief Society", "etype": "article"},
        {"title": "Herod\u2019s Temple", "author": "Evan Jensen", "etype": "article"},
        {"title": "\u201cWist Ye Not That I Must Be About My Father\u2019s Business?\u201d Part II", "author": "J. Reuben Clark Jr.", "etype": "article"},
        {"title": "Award Winners Eliza Roxey Snow Memorial Prize Poem Contest", "author": None, "etype": "article"},
        {"title": "President Grant Acknowledges Birthday Remembrances", "author": None, "etype": "article"},
        {"title": "Award Winners Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Woman\u2019s World", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field: Suggestive Program for Sunday Evening, March 5, 1944", "author": None, "etype": "article"},
        {"title": "General Board Bulletins to Be Studied", "author": None, "etype": "article"},
        {"title": "Bound Volumes", "author": None, "etype": "article"},
        {"title": "Notes from the Field: Interesting Local Relief Societies", "author": None, "etype": "article"},
        {"title": "General Secretary Treasurer", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Editorial: Honour Thy Father and Thy Mother", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Undeclared Dividends", "author": "Vesta P. Crawford", "etype": "fiction"},
        {"title": "The Least of the Flock", "author": "M. Lynn Bennion", "etype": "fiction"},
        {"title": "Tomorrow\u2019s Cup Chapter X Conclusion", "author": "Anna Prince Redd", "etype": "fiction"},
        {"title": "In Times of War", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Night Shadows", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "Benison", "author": "Linda S. Fletcher", "etype": "poem"},
        {"title": "Given", "author": "Olive C. Wehr", "etype": "poem"},
        {"title": "The Chimes", "author": "Pliny A. Wiley", "etype": "poem"},
        {"title": "To A New House", "author": "Etta A. Christensen", "etype": "poem"},
        {"title": "Do Not Despair", "author": "Winnifred M. Tibbs", "etype": "poem"},
        {"title": "Messages to the Home\u2014Loyalty", "author": None, "etype": "lesson"},
        {"title": "Home Topics\u2014Care of Floors and Furnishings", "author": None, "etype": "lesson"},
        {"title": "Social Science\u2014What \u201cAmerica\u201d Means to Me", "author": None, "etype": "lesson"},
        {"title": "Stories from the Book of Mormon\u2014Conversion of Lamanites by Ammon and His Brethren", "author": None, "etype": "lesson"},
        {"title": "Theology\u2014Submission to Secular Authority", "author": None, "etype": "lesson"},
        {"title": "Literature\u2014Bible Oratory", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No2_February_1944"): [
        {"title": "Abraham Lincoln", "author": "Bryant S. Hinckley", "etype": "article"},
        {"title": "Gettysburg Address", "author": "Abraham Lincoln", "etype": "article"},
        {"title": "\u201cWist Ye Not That I Must Be About My Father\u2019s Business?\u201d Part III", "author": "J. Reuben Clark Jr.", "etype": "article"},
        {"title": "The Poet Has a Birthday", "author": "Anna B. Hart", "etype": "article"},
        {"title": "A Bride Goes West", "author": "Julia Alleman Caine", "etype": "article"},
        {"title": "Food Fights For Freedom", "author": "War Food Administration", "etype": "article"},
        {"title": "My Window Garden", "author": "Florence Grant Smith", "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field: Relief Society Women Urged to Take Advantage of Red Cross Classes", "author": None, "etype": "article"},
        {"title": "Primary Association to Promote Creative Activities", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Special Relief Society Programs", "author": None, "etype": "article"},
        {"title": "General Secretary-Treasurer", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Editorial: The Exercise of Religious Liberty", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "We\u2019re Not So Different", "author": "Christie Lund Coles", "etype": "fiction"},
        {"title": "The Heart Must Kneel", "author": "Anna Prince Redd", "etype": "poem"},
        {"title": "The Sabbath", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Winter in the Canyon", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Winter Sleep", "author": "Clara Tanner", "etype": "poem"},
        {"title": "Rebinding", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Theology\u2014Practical Religion", "author": None, "etype": "lesson"},
        {"title": "Messages to the Home\u2014Love", "author": None, "etype": "lesson"},
        {"title": "Home Topics\u2014Is Your Home Convenient", "author": None, "etype": "lesson"},
        {"title": "Literature\u2014Bible Prophecy", "author": None, "etype": "lesson"},
        {"title": "Social Science\u2014The Need of the Hour", "author": None, "etype": "lesson"},
        {"title": "Stories from the Book of Mormon\u2014Christ\u2019s Visit to the Nephites", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No3_March_1944"): [
        {"title": "Sketch Of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Relief Society In Action", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "\u201cWist Ye Not That I Must Be About My Father\u2019s Business?\u201d Part IV", "author": "J. Reuben Clark Jr.", "etype": "article"},
        {"title": "Glimpses of an Old Town", "author": "Katherine Palmer Macfarlane", "etype": "article"},
        {"title": "Lula Greene Richards to Observe Ninety-fifth Birthday", "author": "Louisa Wells Luke", "etype": "article"},
        {"title": "Soil Improvement in the Home Garden", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "A Brief History of the Spring City Relief Society", "author": "Bergetta Jensen", "etype": "article"},
        {"title": "How I Help Ward Literature Leaders", "author": "Fae J. Nielsen", "etype": "article"},
        {"title": "The Soldier and His Health Overseas", "author": "War Department", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Notes To The Field: Magazine Honor Roll In May", "author": None, "etype": "article"},
        {"title": "Binding Material For Magazine", "author": None, "etype": "article"},
        {"title": "Editorial: Gratitude to Our Leaders", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The Pink Angel", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "The Rock and the River Part I", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "Snow In The Orchard", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Our Relief Society President", "author": "Marguerite Burnhope Harris", "etype": "poem"},
        {"title": "China Sky", "author": "Thelma Ireland", "etype": "poem"},
        {"title": "Early Spring", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Forever", "author": "Jeanette P. Parry", "etype": "poem"},
    ],

    ("Vol31", "No4_April_1944"): [
        {"title": "Easter", "author": "Stephen L Richards", "etype": "article"},
        {"title": "\u201cWist Ye Not That I Must Be About My Father\u2019s Business?\u201d Part V", "author": "J. Reuben Clark Jr.", "etype": "article"},
        {"title": "Sketch of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "The Child\u2019s Voice and His Early Musical Experiences", "author": "Florence Jepperson Madsen", "etype": "article"},
        {"title": "Rose Growing in the Home Garden", "author": "Hazel D. Moyle", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field: Relief Society-Stake Quarterly Conference", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Messages From the Missions", "author": None, "etype": "article"},
        {"title": "General Secretary-Treasurer", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Editorial: Salt Lake Region Welfare Meeting", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "The Rock and the River Part II", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "Spring Silhouette", "author": "Agnes K. Morgan", "etype": "poem"},
        {"title": "When We Must Say Goodby", "author": "Grace Zenor Pratt", "etype": "poem"},
    ],

    ("Vol31", "No5_May_1944"): [
        {"title": "This I Know To Be True", "author": "Leone G. Layton", "etype": "article"},
        {"title": "Sketch Of My Life", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "\u201cSisters, Let Your Own Hands Adorn Your Homes\u201d", "author": "William R. Palmer", "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field: Publication of Magazine Honor Roll Delayed", "author": None, "etype": "article"},
        {"title": "Change In Price of Relief Society Pins", "author": None, "etype": "article"},
        {"title": "Editorial: Women Courageous", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Lissa\u2019s Song", "author": "Mary Ek Knowles", "etype": "fiction"},
        {"title": "The Rock and the River Part III", "author": "Ezra J. Poulsen", "etype": "fiction"},
        {"title": "Your Spring and Mine", "author": "Olive Maiben Nicholes", "etype": "poem"},
        {"title": "Passover", "author": "Weneta Leatham Nielsen", "etype": "poem"},
        {"title": "The Faith of Motherhood", "author": "Ann Young Hughes", "etype": "poem"},
        {"title": "So Much To Do", "author": "Grace M. Candland", "etype": "poem"},
        {"title": "The First Born", "author": "Abbie R. Madsen", "etype": "poem"},
        {"title": "Love\u2019s the Bond", "author": "Merling Dennis Clyde", "etype": "poem"},
    ],

    ("Vol31", "No6_June_1944"): [
        {"title": "The Friendship Between the Prophets Joseph Smith and Brigham Young", "author": "Preston Nibley", "etype": "article"},
        {"title": "Report on the 114th Annual Conference", "author": "Antoine R. Ivins", "etype": "article"},
        {"title": "Mark E. Petersen Named Apostle", "author": "Henry A. Smith", "etype": "article"},
        {"title": "Contest Announcements\u20141944: Eliza R. Snow Memorial Prize Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "The Republic of Czechoslovakia", "author": "Leland H. Creer", "etype": "article"},
        {"title": "Sketch of My Life\u2014Chapter IV", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Table and Pictographs on Magazine Subscriptions", "author": None, "etype": "article"},
        {"title": "Magazine Honor Roll", "author": None, "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes From The Field: Relief Society Magazine Campaigns", "author": None, "etype": "article"},
        {"title": "General Secretary-Treasurer", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Editorial: Adapted to the Weakest of All Saints", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Offering", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Convoy", "author": "Grace A. Cooper", "etype": "poem"},
        {"title": "Living Magic", "author": "Ora Lee Parthesius", "etype": "poem"},
    ],

    ("Vol31", "No7_July_1944"): [
        {"title": "Freedom of Choice", "author": "David O. McKay", "etype": "article"},
        {"title": "Yugoslavia: Kingdom of the Serbs, Croats, and Slovenes", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "New Honor Comes to Tabernacle Choir", "author": "J. Spencer Cornwall", "etype": "article"},
        {"title": "Annual Report", "author": "Vera White Pohlman", "etype": "article"},
        {"title": "Time, Energy, and Fatigue Costs of Housekeeping Activities", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Map of the United States Showing Pioneer Lines of Travel", "author": None, "etype": "article"},
        {"title": "Sketch of My Life Chapter V", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field: Modifications in Work Meeting Program", "author": None, "etype": "article"},
        {"title": "Editorial: Appropriate Relief Society Programs", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "So Let It Ring", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "I-30", "author": "Dott J. Sartori", "etype": "poem"},
        {"title": "Suggestions and Instructions on the Educational Program 1944-45", "author": None, "etype": "lesson"},
        {"title": "Theology: Preview; Joseph Smith\u2019s First Vision", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers\u2019 Messages: Preview; Blessed Are the Poor in Spirit", "author": None, "etype": "lesson"},
        {"title": "Literature: Preview; Historical Literature of the New Testament", "author": None, "etype": "lesson"},
        {"title": "Social Science: Preview; Morality in Our Day", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No8_August_1944"): [
        {"title": "Mormon Pioneer Culture", "author": "Alice Merrill Horne", "etype": "article"},
        {"title": "Leone Gedge Layton", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Pioneering in Southern Alberta, Canada", "author": "Jennie B. Knight", "etype": "article"},
        {"title": "The Three Witnesses of the Book of Mormon", "author": "Preston Nibley", "etype": "article"},
        {"title": "Rumania Part I", "author": "Leland H. Creer", "etype": "article"},
        {"title": "Sketch of My Life Chapter VI", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Income Management in Wartime", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes From the Field: Relief Society War and Welfare Activities", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Editorial: Honest and Wise Men Should Be Sought For", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Forward, Without Fear Chapter I", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Declare the Glory", "author": "Vesta P. Crawford", "etype": "poem"},
        {"title": "Summer Rain", "author": "Louise Larson Cornish", "etype": "poem"},
        {"title": "Fruition", "author": "Leone G. Layton", "etype": "poem"},
        {"title": "Theology: The Book of Mormon Revealed", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers\u2019 Messages: Blessed Are They That Mourn", "author": None, "etype": "lesson"},
        {"title": "Literature: Story Telling in the New Testament", "author": None, "etype": "lesson"},
        {"title": "Social Science: \u201cMen Are, That They Might Have Joy\u201d", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No9_September_1944"): [
        {"title": "Types of New Testament Literature", "author": "Russel B. Swensen", "etype": "article"},
        {"title": "Luacine Savage", "author": "Amy Brown Lyman", "etype": "article"},
        {"title": "Rumania-Part II", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "Food Fights For Freedom", "author": "Rose H. Widtsoe", "etype": "article"},
        {"title": "Sketch of My Life Chapter VII", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "Home Safety", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Editorials: Back to School", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Editorials: The Framers of the Constitution", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Help Wanted", "author": "Nora Wayne", "etype": "fiction"},
        {"title": "Forward, Without Fear Chapter II", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "I Will Look Unto the Hills", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "Fall", "author": "Lottie Hammer Singley", "etype": "poem"},
        {"title": "Hrumph!", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "Theology: The Priesthood of God Restored", "author": None, "etype": "lesson"},
        {"title": "Visiting Teachers\u2019 Messages: Blessed Are the Meek", "author": None, "etype": "lesson"},
        {"title": "Literature: New Testament Parables", "author": None, "etype": "lesson"},
    ],

    ("Vol31", "No10_October_1944"): [
        {"title": "Snow in the Rockies, Montana", "author": "Glen Perrin", "etype": "article"},
        {"title": "Death and Sin", "author": "Albert E. Bowen", "etype": "article"},
        {"title": "Toward Richer Living", "author": "Franklin S. Harris", "etype": "article"},
        {"title": "Rheumatic Heart Disease", "author": "Frederic M. Kriete", "etype": "article"},
        {"title": "The New Turkey", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "Sketch of My Life Chapter VIII", "author": "Eliza R. Snow Smith", "etype": "article"},
        {"title": "As a Grain of Mustard Seed", "author": "Leslie L. Sudweeks", "etype": "article"},
        {"title": "Let Us Take the Time", "author": "Margaret E. Maslin", "etype": "article"},
        {"title": "Laundering", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes from the Field: Relief Society War and Welfare Services in Missions", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Editorial: Home After the War", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The Captain\u2019s Biggest Battle", "author": "Sadie Willis Adamson", "etype": "fiction"},
        {"title": "Forward, Without Fear Chapter III", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Song of America", "author": "Elizabeth Burningham", "etype": "poem"},
        {"title": "Magic in Memory", "author": "Courtney E. Cottam", "etype": "poem"},
        {"title": "October", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Depot", "author": "Ora Lee Parthesius", "etype": "poem"},
        {"title": "She Steps Next Door", "author": "Edna S. Dustin", "etype": "poem"},
        {"title": "They Also Serve", "author": "Alice Eddy LeCornu", "etype": "poem"},
        {"title": "Yes, I Would Be A Saint", "author": "Eliza R. Snow Smith", "etype": "poem"},
        {"title": "Bury Me Quietly When I Die", "author": "Eliza R. Snow Smith", "etype": "poem"},
        {"title": "Theology: The Church of Jesus Christ Organized", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers\u2019 Messages: Blessed Are They Which Do Hunger and Thirst After Righteousness", "author": "Bryant S. Hinckley", "etype": "lesson"},
        {"title": "Literature: Wisdom Literature of the New Testament", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: The Road to Righteousness", "author": "Harold T. Christensen", "etype": "lesson"},
    ],

    ("Vol31", "No11_November_1944"): [
        {"title": "The Pearl of Great Price", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Why a Singer Should Keep the Word of Wisdom", "author": "Emma Lucy Gates Bowen", "etype": "article"},
        {"title": "For Thy Bounties", "author": "Marguerite J. Griffin", "etype": "article"},
        {"title": "Money Isn\u2019t Everything", "author": "Olive C. Wehr", "etype": "article"},
        {"title": "Finland", "author": "Leland Hargrave Creer", "etype": "article"},
        {"title": "What an Old Trunk May Bring Forth", "author": "William R. Palmer", "etype": "article"},
        {"title": "Ironing Procedures", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes from the Field: Relief Society War and Welfare Services in Missions (Continued)", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Editorials: Birthday Greetings to President Heber J. Grant", "author": "Amy Brown Lyman", "etype": "editorial"},
        {"title": "Editorials: Wise Spending", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "Forward, Without Fear Chapter IV", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "Thankful Thoughts", "author": "Gertrude Perry Stanton", "etype": "poem"},
        {"title": "In November", "author": "Beatrice Knowlton Ekman", "etype": "poem"},
        {"title": "All These Remain", "author": "Betty Wall Madsen", "etype": "poem"},
        {"title": "Superscription", "author": "Jessie Miller Robinson", "etype": "poem"},
        {"title": "God\u2019s Garden", "author": "Daphne Jemmett", "etype": "poem"},
        {"title": "Theology: The Kirtland Period", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers\u2019 Messages: Blessed Are The Merciful", "author": "Bryant S. Hinckley", "etype": "lesson"},
        {"title": "Literature: Gospel Literature of St. John", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Freedom and Responsibility", "author": "Harold T. Christensen", "etype": "lesson"},
    ],

    ("Vol31", "No12_December_1944"): [
        {"title": "The Spirit of Christmas", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "A Few of Our Gifted Women Artists", "author": "Alice Merrill Horne", "etype": "article"},
        {"title": "Palestine\u2014Part I", "author": "Leland H. Creer", "etype": "article"},
        {"title": "Be Strong, My Heart", "author": "Marguerite J. Griffin", "etype": "article"},
        {"title": "115th Semi-Annual Conference", "author": "Joseph J. Cannon", "etype": "article"},
        {"title": "Home Lighting", "author": "Claire P. Dorius", "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman\u2019s Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes to the Field", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Parties and Special Programs", "author": "Vera W. Pohlman", "etype": "article"},
        {"title": "Editorial: A Cry of Repentance", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "It Isn\u2019t the Gifts", "author": "Olive W. Burt", "etype": "fiction"},
        {"title": "Forward, Without Fear\u2014Chapter 5", "author": "Dorothy Clapp Robinson", "etype": "fiction"},
        {"title": "She Trims Her Tree", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Call to Service", "author": "Gertrude Perry Stanton", "etype": "poem"},
        {"title": "The Lost Child", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Bethlehem the Blessed", "author": "Alice Whitson Norton", "etype": "poem"},
        {"title": "Theology: The Missouri Period", "author": "H. Wayne Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers\u2019 Messages: Blessed Are The Pure in Heart", "author": "Bryant S. Hinckley", "etype": "lesson"},
        {"title": "Literature: The Acts of the Apostles", "author": "Howard R. Driggs", "etype": "lesson"},
        {"title": "Social Science: Truth and Its Perversions", "author": "Harold T. Christensen", "etype": "lesson"},
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
    year = 1944

    key = f"No{no}_{month}_{year}"  # No zero-padding!
    fname = f"Vol31_{key}.txt"
    ISSUE_FILES[("Vol31", key)] = (fname, month)


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
# Known OCR-mangled section headers for Vol31.
# ---------------------------------------------------------------------------

# Editorial header prefix (matches OCR variants of "EDITORIAL")
_EDITORIAL_HDR = (
    r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)"
    r"[\s\S]{0,120}?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—Lesson NN—SUBTITLE or THEOLOGY ... Lesson NN—SUBTITLE
    # TOC has: Theology: SUBTITLE
    "Theology: ": (
        r"(?:Th|Sh|Ch|THEOL)eology"
        r"[\s\-\u2014\u2013:e¢]*"
        r"(?:[\s\S]{0,250}?(?:(?:Lesson|Discussion|esson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?\s*(?:The\s+|A\s+)?)?"
    ),
    # Visiting Teacher Messages (no apostrophe variant)
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?|Seal!?\s*He)\s*"
        r"(?:M|N|IT|T|m|He|HeIT)\s*(?:l)?essages?"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Message|Lesson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|\.…]?\s*(?:The\s+|A\s+)?)?"
    ),
    # Visiting Teachers' Messages (with apostrophe)
    "Visiting Teachers\u2019 Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?s?|Seal!?\s*Hes?)\s*"
        r"['\u2019]?\s*"
        r"(?:M|N|IT|T|m|He|HeIT)\s*(?:l)?essages?"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Message|Lesson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|\.…]?\s*(?:The\s+|A\s+)?)?"
    ),
    # Visiting Teachers Messages (no apostrophe variant)
    "Visiting Teachers Messages: ": (
        r"(?:V|U|\()?(?:i|s|u)?s?iting\s+"
        r"(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?s?|Seal!?\s*Hes?)\s*"
        r"['\u2019]?\s*"
        r"(?:M|N|IT|T|m|He|HeIT)\s*(?:l)?essages?"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,250}?(?:(?:Message|Lesson)\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|\.…]?\s*(?:The\s+|A\s+)?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+(?:N|Vl)?ear\s+(?:and|Gnd|And|and)\s+(?:F|S|St|t)ar[r]?"
    ),
    # Body has: EDITORIAL ... VOL. 31 MONTH 1944 NO. N <actual title>
    # TOC has: Editorial: <actual title>
    "Editorial: ": (
        r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"(?:EDITORIAL|E?\s*JIT\s*ORIAL|[A-Z]{0,4}\s*ORIAL)[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Literature lessons
    "Literature: ": (
        r"(?:(?:Fo\s+i|S\s+i|O|Of?\s+i|of\s+i)?)?"
        r"[Ll]?iterature"
        r"[\s\-\u2014\u2013:e@]*"
        r"(?:America'?s?\s+Literature)?"
        r"[\s\S]{0,150}?"
        r"(?:(?:Lesson|Preview)\s+\d*)?"
        r"[\s\-\u2014\u2013]*"
    ),
    # Social Science lessons
    "Social Science: ": (
        r"(?:Social\s+(?:Sc(?:i|t|l)ence|eicHiee|Stustiee|SCIENCE|ae|[A-Za-z]e))"
        r"[\s\-\u2014\u2013:@e]*"
        r"(?:Divine\s+Law\s+and\s+Church\s+Government)?"
        r"[\s\S]{0,250}?"
        r"(?:(?:Lesson|Preview)\s+\d+)"
        r"[\s\-\u2014\u2013]*"
        r"[\"\u201c\u2018\u2019\u0027]?"
    ),
    # Messages to the Home (vol31 Jan-Feb lesson type)
    "Messages to the Home": (
        r"(?:M|IT|Tl)essages?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch|th|t)e\s+(?:H|h)ome"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,150}?(?:(?:Lesson|Message)\s+\d+)[\s\-\u2014\u2013]*)?"
    ),
    # Home Topics (vol31 Jan-Feb lesson type)
    "Home Topics": (
        r"(?:H|h)ome\s+(?:T|S|J)opics?"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,150}?(?:(?:Lesson)\s+\d+)[\s\-\u2014\u2013]*)?"
    ),
    # Stories from the Book of Mormon (vol31 Jan-Feb lesson type)
    "Stories from the Book of Mormon": (
        r"(?:S|St)tories?\s+(?:f|F|St|S)rom\s+(?:Th|Sh|Ch|th|t)e\s+"
        r"(?:B|b)ook\s+of\s+(?:M|IT|Tl)ormon"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]{0,150}?(?:(?:Lesson)\s+\d+)[\s\-\u2014\u2013]*)?"
    ),
    # Woman's World (vol31 January only — variant of Woman's Sphere)
    "Woman\u2019s World": (
        r"(?:W|V)oman.s\s+(?:W|V)orld"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St|S)rom\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St|S)(?:i|e)?eld"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St|S)(?:i|e)?eld"
        r":?\s*"
    ),
    "Notes from the Field: ": (
        r"(?:Notes?\s+)?(?:f|F|St|S)rom\s+(?:Th|Sh|Ch|th|t)e\s+(?:F|St|S)(?:i|e)?eld"
        r":?\s*"
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
        "RELIEF SOCIETY GENERAL BOARD",
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
                poetry_end = re.search(
                    r'\d{3}\.\s*\n', body[toc_in_body.start():toc_in_body.start() + 5000]
                )
                if poetry_end:
                    toc_end_pos = toc_in_body.start() + poetry_end.end()
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
    "Theology: ", "Theology\u2014", "Visiting Teacher Messages: ", "Work Meeting: ",
    "Literature: ", "Literature\u2014", "Social Science: ", "Social Science\u2014",
    "Visiting Teachers Messages: ", "Visiting Teachers\u2019 Messages: ",
    "Work Meeting Sewing: ", "Work Meeting Sewing",
    "Messages to the Home", "Home Topics",
    "Stories from the Book of Mormon",
    "Suggestions and Instructions on the Educational Program",
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
    re.compile(r'(?:LESS.{0,10}N\s+(?:DE.{0,6}\s+)?)?DEPARTMENT[\s|:]+(?:Sh|Th|Ch)eology', re.IGNORECASE),
    # "Woman's Sphere" / "Woman's World" (recurring section)
    re.compile(r"(?:W|V)oman.s\s+(?:Sp|S)here", re.IGNORECASE),
    re.compile(r"(?:W|V)oman.s\s+(?:W|V)orld", re.IGNORECASE),
]

# Section-header patterns for detecting lesson boundaries within chunks.
# These match the OCR-garbled headers without requiring the specific lesson title.
_LESSON_SECTION_HEADERS = [
    # Theology
    re.compile(
        r'(?:Th|Sh|Ch)eology[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Visiting Teacher(s) Messages (with or without apostrophe/s)
    re.compile(
        r'(?:V|U|\()?(?:i|s|u)?s?iting\s+'
        r'(?:(?:T|S|C|l|SJ)(?:e|l|a|J)?(?:a|e|c)?(?:ch|l!|h)?(?:er)?s?|Seal!?\s*Hes?)\s*'
        r"['\u2019]?\s*"
        r'(?:M|IT|T|m|He|HeIT)\s*(?:l)?essages?',
        re.IGNORECASE
    ),
    # Work Meeting (including Work Meeting Sewing)
    re.compile(
        r'(?:W|V)ork\s+(?:IT|T|M)?(?:l)?eeting[)\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Literature
    re.compile(
        r'(?:(?:Fo\s+i|S\s+i|O|Of?\s+i|of\s+i)?)?'
        r'[Ll]?iterature[\s\u2014\u2013\-:]+'
        r'(?:America|Come|Expand|Lesson|Cosmopolitan|Doctrinal|Appreciation|Highlights|Gospel|Words|Literature|Bible|Story|Historical|New|Wisdom|Acts|Preview)',
        re.IGNORECASE
    ),
    # Social Science (OCR garbles "Science" as "ae", "eicHiee", etc.)
    re.compile(
        r'Social\s+(?:Sc(?:i|t|l)ence|eicHiee|Stustiee|ae|[A-Za-z]e)[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Messages to the Home (vol31 lesson type)
    re.compile(
        r'(?:M|IT|Tl)essages?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch|th|t)e\s+(?:H|h)ome[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Home Topics (vol31 lesson type)
    re.compile(
        r'(?:H|h)ome\s+(?:T|S|J)opics?[\s\u2014\u2013\-:]+',
        re.IGNORECASE
    ),
    # Stories from the Book of Mormon (vol31 lesson type)
    re.compile(
        r'(?:S|St)tories?\s+(?:f|F|St|S)rom\s+(?:Th|Sh|Ch|th|t)e\s+(?:B|b)ook\s+of\s+(?:M|IT|Tl)ormon[\s\u2014\u2013\-:]+',
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
        description="Extract Relief Society Magazine Vol 31 into individual entries"
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
    vol31_out = OUTPUT_DIR / "vol31"
    if vol31_out.exists() and not args.dry_run:
        print(f"Cleaning output directory: {vol31_out}")
        shutil.rmtree(vol31_out)

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    volume_json = {}

    for (vol, issue_key), entries in VOL31_TOC.items():
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
