#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 43 (1956).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol43.py
    python extract_vol43.py --dry-run
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

VOL43_TOC = {
    ("Vol43", "No01_January_1956"): [
        {"title": "New Year's Greetings", "author": None, "etype": "article"},
        {"title": "The Objectives of Relief Society", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Award Winners -- Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "My Rose of Deseret -- First Prize Poem", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Enduring Memories -- Second Prize Poem", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "Be Still, My Heart -- Third Prize Poem", "author": "Ruth C. Langlois", "etype": "poem"},
        {"title": "Award Winners -- Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Now Is a Man Grown -- First Prize Story", "author": "Maryhale Woolsey", "etype": "story"},
        {"title": "The Argentine Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Swiss Temple Table Arrangement", "author": "Inez R. Allen", "etype": "article"},
        {"title": "How We Sell The Relief Society Magazine", "author": "Dr. Royal L. Garff", "etype": "article"},
        {"title": "Polio Isn't Licked Yet", "author": "Basil O'Connor", "etype": "article"},
        {"title": "Biographical Sketches of Award Winners in the Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "The Living Gifts", "author": "Dorothy Boys Kilian", "etype": "story"},
        {"title": "The Closed Circle", "author": "Beatrice R. Parsons", "etype": "story"},
        {"title": "Hermanas, Chapter 7", "author": "Fay Tarlock", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Greetings for the New Year", "author": "Velma N. Simonsen", "etype": "editorial"},
        {"title": "In Memoriam -- Emeline Young Nebeker", "author": None, "etype": "article"},
        {"title": "Covers Will Feature Missions Outside Continental United States", "author": None, "etype": "article"},
        {"title": "New Serial 'There Is Still Time' to Begin in February", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Relief Society Assigned Evening Meeting of Fast Sunday in March", "author": None, "etype": "article"},
        {"title": "Lesson Work for Spanish-Speaking Relief Societies and Other Minority Groups in Stakes", "author": None, "etype": "article"},
        {"title": "Recipes from Argentina", "author": "Keith F. Thompson", "etype": "article"},
        {"title": "Vegetables -- A Different Way Every Day -- Part I", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Theology: Signs of the Crucifixion; the Voice of Jesus Christ Is Heard", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: 'But Behold, the Resurrection of Christ Redeemeth Mankind'", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Vegetable Cookery (Continued)", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Thomas Hardy 'The Return of the Native'", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Constitution and World Affairs", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Today", "author": "Etta Robbins", "etype": "poem"},
        {"title": "Winter Memory", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Threshold", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Weaving", "author": "Miriam W. Wright", "etype": "poem"},
        {"title": "New Year's Day", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "At Midnight, December Thirty-first", "author": "Katherine F. Larsen", "etype": "poem"},
        {"title": "Time Eternal", "author": "Vesta N. Lukei", "etype": "poem"},
    ],
    ("Vol43", "No02_February_1956"): [
        {"title": "Leadership", "author": "Adam S. Bennion", "etype": "article"},
        {"title": "The Australian Mission", "author": "Preston Nibley", "etype": "article"},
        {"title": "Tribute to the Visiting Teachers", "author": "Wanda Pexton", "etype": "article"},
        {"title": "The Majestic View", "author": "Ruth Wilson", "etype": "article"},
        {"title": "World of Three", "author": "Nell Murbarger", "etype": "article"},
        {"title": "Table Decorations for Anniversary Day", "author": "Inez R. Allen", "etype": "article"},
        {"title": "Keep Me Forever -- Second Prize Story", "author": "Margaret Hardy", "etype": "story"},
        {"title": "There Is Still Time, Chapter 1", "author": "Margery S. Stewart", "etype": "serial_fiction"},
        {"title": "Hermanas, Chapter 8", "author": "Fay Tarlock", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Anniversary Day", "author": "Belle S. Spafford", "etype": "editorial"},
        {"title": "In Memoriam -- Mary Grant Judd", "author": None, "etype": "article"},
        {"title": "Birthday Congratulations to Former President Amy Brown Lyman", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Notes From the Field Should Be Submitted", "author": None, "etype": "article"},
        {"title": "Programs for Anniversary Day", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Recipes From Australia", "author": "Irene T. Erekson", "etype": "article"},
        {"title": "Salads for Health and Beauty", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Mary Blanchard Williams Decorates Cakes", "author": None, "etype": "article"},
        {"title": "Theology: Christ Among the Nephites", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: '... For Behold, Ye Are Free; Ye Are Permitted to Act For Yourselves'", "author": "Edith S. Elliott", "etype": "lesson"},
        {"title": "Work Meeting: Salads", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Review of English Literature", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Fruits of Freedom", "author": "Albert R. Bowen", "etype": "lesson"},
        {"title": "Still, White Hour", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Snowscape", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Snow", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Compensation", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Time", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Renewal", "author": "Anna Rice", "etype": "poem"},
        {"title": "My Valentine", "author": "Pearl D. Bringhurst", "etype": "poem"},
        {"title": "Rocketeer", "author": "Maude Rubin", "etype": "poem"},
    ],
    ("Vol43", "No03_March_1956"): [
        {"title": "Words to Live By", "author": "Josie B. Bay", "etype": "article"},
        {"title": "The Brazilian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "How We Conduct the Family Hour in Our Home", "author": "Helen S. Gardner", "etype": "article"},
        {"title": "Getting New Subscriptions and Renewals for The Relief Society Magazine", "author": "Lucy Horman", "etype": "article"},
        {"title": "Sierra Ghost Towns", "author": "Nell Murbarger", "etype": "article"},
        {"title": "The Preparation and Serving of Food for Large Gatherings", "author": "Frank D. Arnold", "etype": "article"},
        {"title": "Put on Your Gay Sunbonnet", "author": "Jennie E. Graham", "etype": "article"},
        {"title": "The Home on the Hill", "author": "Alyce O. Nelson", "etype": "article"},
        {"title": "Room for Nancy -- Third Prize Story", "author": "Edith Larson", "etype": "story"},
        {"title": "The Ice-Cream Pie", "author": "Florence B. Dunford", "etype": "story"},
        {"title": "There Is Still Time, Chapter 2", "author": "Margery S. Stewart", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Constitution of Relief Society", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "In Memoriam -- Charlotte Owens Sackett", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Organizations and Reorganizations of Stake and Mission Relief Societies for 1955", "author": None, "etype": "article"},
        {"title": "Index for 1955 Relief Society Magazine", "author": None, "etype": "article"},
        {"title": "Announcing the Special April Short Story Issue", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Sometime Table", "author": "Helen B. Morris", "etype": "article"},
        {"title": "Recipes From Brazil", "author": "Asael T. Sorensen", "etype": "article"},
        {"title": "Life Is Like a Pattern", "author": "Annie S. W. Gould", "etype": "article"},
        {"title": "Let's Have Fish", "author": "Winnifred C. Jardine", "etype": "article"},
        {"title": "The Rugged Rug Hookers", "author": "Geneve Hourihan", "etype": "article"},
        {"title": "Garden Accents", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Multiple Hobbies Help Mary Hilda Smith to Make Others Happy", "author": None, "etype": "article"},
        {"title": "Kindness", "author": "Jane B. Wunderlich", "etype": "poem"},
        {"title": "Competitor", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Cryptic Tokens", "author": "Eva W. Wangsgaard", "etype": "poem"},
        {"title": "Desert Yearnings", "author": "Annie Atkin Tanner", "etype": "poem"},
        {"title": "Temple Square", "author": "Leone E. McCune", "etype": "poem"},
        {"title": "Spring Cleaning", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Honeymoon Salad", "author": "Francelia Goddard", "etype": "poem"},
        {"title": "Where Sweets Are", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "March", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Timber Line", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Perennials", "author": "Della Adams Leitner", "etype": "poem"},
    ],
    ("Vol43", "No04_April_1956"): [
        {"title": "The Resurrection", "author": "LeGrand Richards", "etype": "article"},
        {"title": "The British Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Relief Society Assists in Welfare Program for Marysville-Yuba City Flood", "author": "Alice I. Ferrin", "etype": "article"},
        {"title": "A Temple Will Be Built", "author": "Mabel L. Anderson", "etype": "article"},
        {"title": "Cancer -- A Check-Up and a Check", "author": "Sandra Munsell", "etype": "article"},
        {"title": "The Relief Society Magazine -- 'A Messenger'", "author": "Emily C. Pollei", "etype": "article"},
        {"title": "Trouble", "author": "Celia Luce", "etype": "article"},
        {"title": "Fear Is a Habit", "author": "Anne S. W. Gould", "etype": "article"},
        {"title": "Alternate 89", "author": "Willard Luce", "etype": "article"},
        {"title": "Happiness Now", "author": "Wilma Boyle Bunker", "etype": "article"},
        {"title": "The Aspen Grove", "author": "Vernessa Miller Nagle", "etype": "article"},
        {"title": "New Vistas", "author": "Jennie Brown Rawlins", "etype": "article"},
        {"title": "A Full Hive", "author": "Dorothy Clapp Robinson", "etype": "story"},
        {"title": "To You, Beloved", "author": "Lois E. Fockner", "etype": "story"},
        {"title": "Lesson From Letty", "author": "Arlene D. Cloward", "etype": "story"},
        {"title": "The Day Before the Wedding", "author": "Dorothy Boys Kilian", "etype": "story"},
        {"title": "There Is Still Time, Chapter 3", "author": "Margery S. Stewart", "etype": "serial_fiction"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Woman and Her Garden", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Leone O. Jacobs Resigns From the General Board", "author": None, "etype": "article"},
        {"title": "Notes to the Field: Brigham Young University Leadership Week", "author": None, "etype": "article"},
        {"title": "Book of Mormon Reading Project", "author": None, "etype": "article"},
        {"title": "Hymn of the Month", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Typical British Recipes", "author": "Elaine Reiser", "etype": "article"},
        {"title": "Old-Fashioned Flowers in Modern Gardens", "author": "Dorthea N. Newbold", "etype": "article"},
        {"title": "Ella Randall Lewis Pieces Quilts for Happiness", "author": None, "etype": "article"},
        {"title": "A Use for Old Screens", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "To a Child Gardening", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "Before Your Beauty", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Blessed Easter", "author": "Remelda Nielsen Gibson", "etype": "poem"},
        {"title": "Lift Your Eyes", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Springtime Finds the Canyon", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Now Spring", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Before Night-Shadows Fall", "author": "Maude O. Cook", "etype": "poem"},
        {"title": "Heartsease", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "Remember Today", "author": "Daphne Jemmett", "etype": "poem"},
        {"title": "Her Gift", "author": "Della Adams Leitner", "etype": "poem"},
        {"title": "The Kingbird", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Sacred Ground", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "April-Fingered", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Palomino", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Circle", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "To the Relief Society Visiting Teachers", "author": "Hazel Jones Owen", "etype": "poem"},
    ],
    ("Vol43", "No05_May_1956"): [
        {"title": "Dedication of the Los Angeles Temple", "author": None, "etype": "article"},
        {"title": "Rewards for Activity in the Church", "author": "Thorpe B. Isaacson", "etype": "article"},
        {"title": "The Canadian Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Contest Announcements -- 1956", "author": None, "etype": "article"},
        {"title": "Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Poetry Is for People", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Cheerfulness Is Always Right", "author": "Annie S. W. Gould", "etype": "article"},
        {"title": "So You Want to Write a Story!", "author": "Frances C. Yost", "etype": "article"},
        {"title": "My Legacy", "author": "Margaret S. Fife", "etype": "article"},
        {"title": "Repentance", "author": "Kate Richards", "etype": "article"},
        {"title": "Silver Spoons", "author": "Marilla C. Cook", "etype": "article"},
        {"title": "Who Sings the Lullaby", "author": "Rosa Lee Lloyd", "etype": "story"},
        {"title": "The Perfect Gift", "author": "Mabel Law Atkinson", "etype": "story"},
        {"title": "There Is Still Time, Chapter 4", "author": "Margery S. Stewart", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Gift for Mother's Day", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Program for the November Fast Sunday Evening Meeting", "author": None, "etype": "article"},
        {"title": "A Centenary of Relief Society Out of Print", "author": None, "etype": "article"},
        {"title": "Review Outline for May 1956 Literature Lesson", "author": None, "etype": "article"},
        {"title": "New Serial 'Heart's Bounty' to Begin in June", "author": None, "etype": "article"},
        {"title": "Magazine Subscriptions for 1955", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "The Magazine Honor Roll for 1955", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Recipes From the Canadian Mission", "author": "Leah H. Lewis", "etype": "article"},
        {"title": "Mary Ann Hyde Mortenson Makes Wedding Cakes of Intricate Design", "author": None, "etype": "article"},
        {"title": "A Rug in Dutch Butter Mold Design", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "The Things in You I See", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Gift of Song", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Song From the Rim of Silence", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "The Fern", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Hidden Valley", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Never in Triteness", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "For My Mother", "author": "Christie Lund Coles", "etype": "poem"},
    ],
    ("Vol43", "No06_June_1956"): [
        {"title": "'A Thing of Beauty' -- Restoration of the Heber C. Kimball Home", "author": "Christine H. Robinson", "etype": "article"},
        {"title": "The Central American Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Our Homes and the Flag", "author": "Blanche B. Stoddard", "etype": "article"},
        {"title": "Appreciation for the Singing Mothers", "author": "David O. McKay", "etype": "article"},
        {"title": "Virtue Is Its Own Reward", "author": None, "etype": "article"},
        {"title": "Diamonds and People", "author": "Celia Luce", "etype": "article"},
        {"title": "Cliff Homes of the Ancients", "author": "Nell Murbarger", "etype": "article"},
        {"title": "The Testimony Plant", "author": "Nancy M. Armstrong", "etype": "article"},
        {"title": "Heart's Bounty, Chapter 1", "author": "Deone R. Sutherland", "etype": "serial_fiction"},
        {"title": "When Mother's Reputation Was at Stake", "author": "Nedra Stone Nickell", "etype": "story"},
        {"title": "There Is Still Time, Chapter 5", "author": "Margery S. Stewart", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The 126th Annual Church Conference", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Summer Work Meetings", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Recipes From Central America", "author": "Gladys K. Wagner", "etype": "article"},
        {"title": "Make Children's Clothes Economically and Professionally", "author": "Ivie Huish Jones", "etype": "article"},
        {"title": "Emily Chadwick Zaugg Makes Many Quilts", "author": None, "etype": "article"},
        {"title": "Early American Hooked Rug Design", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology -- Characters and Teachings of The Book of Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages -- Book of Mormon Gems of Truth", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting -- Food Preparation and Service", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature -- Shakespeare in Our Lives", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Music for Lessons on Shakespeare", "author": None, "etype": "lesson"},
        {"title": "Social Science -- Latter-day Saint Family Life", "author": "John Farr Larson", "etype": "lesson"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
        {"title": "I Need a Tranquil Hour", "author": "Wanda G. Nielson", "etype": "poem"},
        {"title": "The Rod", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Dream Mirage", "author": "Grace Wilson", "etype": "poem"},
        {"title": "My Faith in God", "author": "Alice Lyman Welling", "etype": "poem"},
        {"title": "Long-Lost Friends", "author": "Maude Rubin", "etype": "poem"},
        {"title": "My Garden", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Mortar for the Miles", "author": "Verla R. Hull", "etype": "poem"},
        {"title": "On a Sleepless Summer Night", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "To a Friend", "author": "Marvel Crookston", "etype": "poem"},
        {"title": "The Manti Temple", "author": "Gertrude Kovan", "etype": "poem"},
        {"title": "Sleeping Love", "author": "Gertrude Kovan", "etype": "poem"},
        {"title": "An Orchard", "author": "Christie Lund Coles", "etype": "poem"},
    ],
    ("Vol43", "No07_July_1956"): [
        {"title": "Outings and Family Solidarity", "author": "LaVern W. Parmley", "etype": "article"},
        {"title": "The Danish Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Grand Canyon", "author": "Willard Luce", "etype": "article"},
        {"title": "Annual Report 1955", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Pretty As a Pansy", "author": "Frances C. Yost", "etype": "story"},
        {"title": "Cathy and the Onions", "author": "Florence B. Dunford", "etype": "story"},
        {"title": "Heart's Bounty, Chapter 2", "author": "Deone R. Sutherland", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Frontiers", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Buying Textbooks for Relief Society Lessons", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Lily N. Hortnagl Makes Handicraft Gifts for Many Occasions", "author": None, "etype": "article"},
        {"title": "Recipes From the Danish Mission", "author": "Mary Kaiser", "etype": "article"},
        {"title": "Sew a Sunbonnet", "author": "Marijane Morris", "etype": "article"},
        {"title": "Sandwiches", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Herbs for Modern Cookery", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Audio Visual Aids for 1956-57", "author": "Mildred B. Eyring", "etype": "lesson"},
        {"title": "Theology: Jesus Instructs the Nephites", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: 'Yea, Wo Unto Him That Shall Deny the Revelations of the Lord'", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Lunches and Sandwiches", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Why Shakespeare?", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Joseph Smith's Early Home Life", "author": "John Farr Larson", "etype": "lesson"},
        {"title": "Navajo Flute", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Mountain Vigil", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Her Hands", "author": "Luella N. Adams", "etype": "poem"},
        {"title": "Wega", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Golden Windows", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Grandfather's Cello", "author": "Dora Toone Brough", "etype": "poem"},
        {"title": "Detour", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Each Day Is Mine", "author": "Mary Hess Hackney", "etype": "poem"},
        {"title": "In the Quiet Harbor", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "My Prayer", "author": "Irene E. Jones", "etype": "poem"},
    ],
    ("Vol43", "No08_August_1956"): [
        {"title": "Messages From the Wives of the General Authorities of The Church of Jesus Christ of Latter-day Saints", "author": None, "etype": "article"},
        {"title": "'And I Now Turn the Key in Your Behalf'", "author": None, "etype": "article"},
        {"title": "Relief Society General Presidency With Gifts for the Relief Society Building", "author": None, "etype": "article"},
        {"title": "The East German Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Vacation Mecca on the Rio Grande", "author": "Nell Murbarger", "etype": "article"},
        {"title": "The Art of Worthwhile Waiting", "author": "Pauline Henderson", "etype": "article"},
        {"title": "A Tribute to the Singing Mothers", "author": "Myree Daugherty", "etype": "article"},
        {"title": "Trimming", "author": "Celia Luce", "etype": "article"},
        {"title": "The Gift of Love", "author": "Rosa Lee Lloyd", "etype": "story"},
        {"title": "A Rose Without a Thorn", "author": "Irene McCullough", "etype": "story"},
        {"title": "Heart's Bounty, Chapter 3", "author": "Deone R. Sutherland", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Priceless Gift of Peace", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Recipes From the East German Mission", "author": "Mary Ethel E. Gregory", "etype": "article"},
        {"title": "Casserole Recipes", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Josephine Johnson Laker Specializes in Useful Hobbies", "author": None, "etype": "article"},
        {"title": "Theology: Christ's Ministry Continued", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: 'Pray in Your Families Unto the Father'", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Casseroles and Leftovers", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Shakespeare's World", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: A New Day for Women", "author": "John Farr Larsen", "etype": "lesson"},
        {"title": "Home", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Written at Sunset", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Grandfather's Wagon", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Quilt Pattern: Star of Faith", "author": "Maude Rubin", "etype": "poem"},
        {"title": "Core of Summer", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Moonrise", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Melody", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Woodland Wonder", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Summer Tide", "author": "Beatrice K. Ekman", "etype": "poem"},
        {"title": "I Need the Stars", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Fences", "author": "Thelma Ireland", "etype": "poem"},
    ],
    ("Vol43", "No09_September_1956"): [
        {"title": "A Happy Birthday to President David O. McKay", "author": "Mildred C. McKay", "etype": "article"},
        {"title": "Fueling the Flame of Testimony", "author": "Bertha S. Reeder", "etype": "article"},
        {"title": "The Finnish Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Healthy, Happy, Lucky You!", "author": None, "etype": "article"},
        {"title": "Promised Blessings to Word of Wisdom Observers", "author": None, "etype": "article"},
        {"title": "Helps for Accompanists", "author": "Roy M. Darley", "etype": "article"},
        {"title": "The Chocolate Box", "author": "Dorothy Oakley Rea", "etype": "article"},
        {"title": "Little Boy Reading a Book", "author": "Evelyn Dorio", "etype": "article"},
        {"title": "Inventions and Ambitions", "author": "Celia Luce", "etype": "article"},
        {"title": "Meeting Objections in Selling The Relief Society Magazine", "author": "Clara Love", "etype": "article"},
        {"title": "The Joy of Early Rising", "author": "Mildred Higgins", "etype": "article"},
        {"title": "Something Artistic", "author": "Frances C. Yost", "etype": "story"},
        {"title": "Heart's Bounty, Chapter 4", "author": "Deone R. Sutherland", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: 'And They Shall Teach Their Children'", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Annual General Relief Society Conference and Dedicatory Service of Relief Society Building", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Bulbs for Winter Blossoming", "author": "Dorthea N. Newbold", "etype": "article"},
        {"title": "Recipes From the Finnish Mission", "author": "Hortense B. Robinson", "etype": "article"},
        {"title": "Sarah M. Brown Makes Braided Rugs for Home Beautification", "author": None, "etype": "article"},
        {"title": "Refreshments for Entertaining", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Theology: The Nephites and Lamanites Become a United People", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: 'But Charity Is the Pure Love of Christ, and It Endureth Forever'", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Party Refreshments", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Shakespeare's Poetic Power", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Theirs to Hold", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Mother-Sculptor", "author": "Mabel Law Atkinson", "etype": "poem"},
        {"title": "Bright Things", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "School Again", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Mood", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "This Is September", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Rememberings", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The First Plum", "author": "Kathryn Tanner", "etype": "poem"},
        {"title": "Flower of Light", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],
    ("Vol43", "No10_October_1956"): [
        {"title": "Charity Never Faileth", "author": "George H. Mortimer", "etype": "article"},
        {"title": "The French Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Pumpkins", "author": "Celia Luce", "etype": "article"},
        {"title": "Brave Men and Worthy Patriots", "author": "Samuel Chandler", "etype": "article"},
        {"title": "Premiums for Magazine Subscriptions", "author": None, "etype": "article"},
        {"title": "Our Cabin Had a Porch", "author": "Nell Murbarger", "etype": "article"},
        {"title": "My Guiding Light", "author": "Lucile Tournear", "etype": "article"},
        {"title": "So Dear to My Heart", "author": "Ruth Young", "etype": "article"},
        {"title": "The Lord's Unit", "author": "Janice Stock", "etype": "article"},
        {"title": "Last Summer", "author": "Christie L. Coles and Beth C. Johnson", "etype": "story"},
        {"title": "Early Present", "author": "Shirley Sargent", "etype": "story"},
        {"title": "Helping Hands", "author": "Flo Whittemore", "etype": "story"},
        {"title": "Heart's Bounty, Chapter 5", "author": "Deone R. Sutherland", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Study Courses Availeth Much", "author": "June Nielsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Recipes From the French Mission", "author": "Rachael L. Lee", "etype": "article"},
        {"title": "Recipes for Bread and Rolls", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Harriet McNeil Bradshaw Enjoys a Life of Creativity Through Hobbies", "author": None, "etype": "article"},
        {"title": "Winter Table Arrangements", "author": "Elizabeth Williamson", "etype": "article"},
        {"title": "Theology: The Fall of the Nephite Nation As Recorded by Mormon", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: 'And Wo Be Unto Him That Will Not Hearken Unto the Words of Jesus, and Also to Them Whom He Hath Chosen'", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Breads", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: The Moral Shakespeare", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Key Is Turned", "author": "John Farr Larson", "etype": "lesson"},
        {"title": "Magic Call", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Prophecy and Fulfillment", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Paradox", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "The Leaf", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "To a Pioneer Grandmother", "author": "Elsie McKinnon Strachan", "etype": "poem"},
        {"title": "Night Incident", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Words", "author": "Ida Isaacson", "etype": "poem"},
        {"title": "Prelude to Winter", "author": "Miranda Snow Walton", "etype": "poem"},
        {"title": "I Thank Thee", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Reverie", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Sunflowers", "author": "Luella N. Adams", "etype": "poem"},
    ],
    ("Vol43", "No11_November_1956"): [
        {"title": "Developing Safeguards for Youth", "author": "Delbert L. Stapley", "etype": "article"},
        {"title": "The Hawaii Mission", "author": "Preston R. Nibley", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Beauty Is More Than Skin Deep", "author": None, "etype": "article"},
        {"title": "One Bright Star", "author": "Myrtle M. Dean", "etype": "story"},
        {"title": "Heart's Bounty, Chapter 6", "author": "Deone R. Sutherland", "etype": "serial_fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Sixty Years Ago", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: A Home for Relief Society", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: New Address for Relief Society", "author": None, "etype": "article"},
        {"title": "Dedication of The Relief Society Building to Be Featured in December Magazine", "author": None, "etype": "article"},
        {"title": "Relief Society Souvenir Plate Available", "author": None, "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Rozella Stringham Grant Makes Rugs of Intricate Designs", "author": None, "etype": "article"},
        {"title": "Recipes From the Hawaii Mission", "author": "Maurine M. Haycock", "etype": "article"},
        {"title": "Candy Recipes", "author": "Mary J. Wilson", "etype": "article"},
        {"title": "Recipes for Cheese Dishes", "author": "Rhea H. Gardner", "etype": "article"},
        {"title": "Theology: Moroni Explains Principles and Ordinances of the Gospel", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: 'And It Came to Pass That There Was No Contention in the Land Because of the Love of God Which Did Dwell in the Hearts of the People'", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Cheese", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: The Merchant of Venice", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Women and the Priesthood", "author": "John Farr Larson", "etype": "lesson"},
        {"title": "November Afternoon", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Mountain Range", "author": "Leslie Savage Clark", "etype": "poem"},
        {"title": "Anniversary in Chrysanthemums", "author": "Maryhale Woolsey", "etype": "poem"},
        {"title": "The Tatted Lace", "author": "Norma W. Wrathall", "etype": "poem"},
        {"title": "Not Just the Heart", "author": "Catherine E. Berry", "etype": "poem"},
        {"title": "Autumn", "author": "Gene Romolo", "etype": "poem"},
        {"title": "Far Are These Ways", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Sketch for November", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Almighty", "author": "Jessie Dagsland Johannessen", "etype": "poem"},
        {"title": "Autumn Mood", "author": "Vesta N. Lukei", "etype": "poem"},
        {"title": "Perennial", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Reluctant Season", "author": "Eva Willes Wangsgaard", "etype": "poem"},
    ],
    ("Vol43", "No12_December_1956"): [
        {"title": "Dedicatory Service", "author": None, "etype": "article"},
        {"title": "Dedicatory Anthem: 'Thy Blessing on This House, Dear Lord'", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Dedicatory Prayer", "author": "David O. McKay", "etype": "article"},
        {"title": "Introductory Remarks", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Invocation", "author": "Mark E. Petersen", "etype": "article"},
        {"title": "Report of Relief Society Building Activities", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "We Built As One", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Address", "author": "J. Reuben Clark, Jr.", "etype": "article"},
        {"title": "An Enduring Tribute to Visiting Teachers", "author": "Stephen L Richards", "etype": "article"},
        {"title": "Women's Influence and Responsibility", "author": "David O. McKay", "etype": "article"},
        {"title": "Benediction", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "Annual General Relief Society Conference -- 1956", "author": "Margaret C. Pickering", "etype": "article"},
        {"title": "Loving, United Effort Availeth Much", "author": "Velma N. Simonsen", "etype": "article"},
        {"title": "Dedication Hymn: 'Lord, We Dedicate This House to Thee'", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Beauty Is More Than Skin Deep -- Beauty That Lasts Forever", "author": None, "etype": "article"},
        {"title": "The Loan", "author": "Hazel K. Todd", "etype": "story"},
        {"title": "Editorial: A Dedicated Home for Relief Society", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The One Hundred Twenty-Seventh Semi-Annual Church Conference", "author": "Vesta P. Crawford", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "A Christmas Decoration for Relief Society Social or Program", "author": "Inez R. Allen", "etype": "article"},
        {"title": "Christmas Dinner Decoration at Home", "author": "Inez R. Allen", "etype": "article"},
        {"title": "Theology: Prophecies Concerning the Jews, Gentiles, and Lamanites in the Latter Days", "author": "Leland H. Monson", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: 'We Have a Labor to Perform Whilst in This Tabernacle of Clay'", "author": "Leone O. Jacobs", "etype": "lesson"},
        {"title": "Work Meeting: Desserts", "author": "Rhea H. Gardner", "etype": "lesson"},
        {"title": "Literature: Romeo and Juliet", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: 'Unto the Least of These'", "author": "John Farr Larson", "etype": "lesson"},
        {"title": "Octave", "author": "Dorothy J. Roberts", "etype": "poem"},
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
    year = 1956

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol43_{key}.txt"
    ISSUE_FILES[("Vol43", key)] = (fname, month)


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
# Known OCR-mangled section headers for Vol43.
# ---------------------------------------------------------------------------

# Serial fiction chapter patterns
_HERMANAS_CHAPTER_PAT = (
    r"Hermanas"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|Cuapter)\s+)?"
)
_THERE_IS_STILL_TIME_CHAPTER_PAT = (
    r"(?:Th|Sh|Ch)ere\s+(?:I|i)s\s+(?:S|J|\()till\s+"
    r"(?:'|[\u2018\u2019])?"
    r"(?:T|S|J|\()ime"
    r".{0,80}?"  # skip author name / synopsis marker
    r"(?:(?:CHAPTER|Chapter|Cuapter|CUAPTER)\s+)?"
)
_HEARTS_BOUNTY_CHAPTER_PAT = (
    r"Heart(?:'|[\u2018\u2019])s\s+Bounty"
    r".{0,80}?"  # skip author name
    r"(?:(?:CHAPTER|Chapter|Cuapter|CUAPTER)\s+)?"
)

_KNOWN_HEADER_PATTERNS = {
    # Body has: Sheology—Characters and Teachings of The Book of Mormon Lesson NN
    # Require em-dash followed by a letter to avoid matching stats tables ("Theology -..---")
    # and casual mentions in Notes from the Field
    "Theology: ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?"
    ),
    "Theology -- ": (
        r"(?:Th|Sh|Ch)eology"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?"
    ),
    # Body has: Ussiting Sheacher ITlessages / (Uisiting Seacher ITlessages / Visiting Ceacher ITlessages / Visiting eaeher IT lessages
    "Visiting Teacher Messages: ": (
        r"(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:C|c)?(?:T|S|e)?(?:e|l)?(?:a|e)?(?:c|e)?h?er\s+"
        r"(?:M|IT|T|m)(?:\s+)?(?:l)?essages"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|]?)?"
    ),
    "Visiting Teacher Messages -- ": (
        r"(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:C|c)?(?:T|S|e)?(?:e|l)?(?:a|e)?(?:c|e)?h?er\s+"
        r"(?:M|IT|T|m)(?:\s+)?(?:l)?essages"
        r"[\s\-\u2014\u2013:]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027\|]?)?"
    ),
    "From Near and Far": (
        r"(?:F|St|S)rom\s+(?:N|l)ear\s+and\s+(?:F|S)ar"
    ),
    # Body has: EDITORIAL ... VOL. 43 MONTH 1956 NO. N
    "Editorial: ": (
        r"EDITORIAL"
        r"[\s\S]{0,120}?"
    ),
    "Editorials: ": (
        r"EDITORIAL[S]?"
        r"[\s\S]{0,120}?"
    ),
    # Body has: OLiterature / oLiterature / ol iterature / ob iterature / Literature (clean)
    "Literature: ": (
        r"(?:[Oo][Llb]?\s*)?"
        r"[Ll]?iterature"
        r"[\s\-\u2014\u2013:]*"
        r"(?:(?:Sh|Th|Ch)e\s+Literature\s+of\s+England|Shakespea[rt]e\s+in\s+Our\s+Lives)?"
        r"[\s\S]{0,60}?"
    ),
    "Literature -- ": (
        r"(?:[Oo][Llb]?\s*)?"
        r"[Ll]?iterature"
        r"[\s\-\u2014\u2013:]*"
        r"(?:(?:Sh|Th|Ch)e\s+Literature\s+of\s+England|Shakespea[rt]e\s+in\s+Our\s+Lives)?"
        r"[\s\S]{0,60}?"
    ),
    # Body has: Social Science—Latter-day Saint Family Life (clean)
    # Require em-dash after category to avoid matching casual mentions
    "Social Science: ": (
        r"Social\s+Sc(?:i|t)ence"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    "Social Science -- ": (
        r"Social\s+Sc(?:i|t)ence"
        r"[\s]*[\-\u2014\u2013][\s]*"
        r"(?:[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?)?"
    ),
    # Body has two formats:
    #   "...at Work Meeting) Lesson N—Title"  (regular months)
    #   "Work Meeting—Food Preparation..."    (preview months like June)
    # Require either ) or em-dash+letter after "Meeting" to avoid casual mentions
    "Work Meeting: ": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|M)?(?:\s+)?(?:l)?(?:e|E)eting"
        r"(?:"
        r"[)\s]*\)\s*"                          # ...at Work Meeting) ...
        r"|"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"  # Work Meeting—Food ...
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?"
    ),
    "Work Meeting -- ": (
        r"(?:W)?(?:V|W)ork\s+(?:IT|T|M)?(?:\s+)?(?:l)?(?:e|E)eting"
        r"(?:"
        r"[)\s]*\)\s*"
        r"|"
        r"[\s]*[\-\u2014\u2013][\s]*[A-Za-z]"
        r")"
        r"[\s\S]*?(?:Lesson\s+\d+|Preview)[\s\-\u2014\u2013]*[\"\u201c\u2018\u2019\u0027]?"
    ),
    "Notes From the Field: ": (
        r"(?:Notes?\s+)?(?:F|St)rom\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    "Notes to the Field: ": (
        r"Notes?\s+(?:T|S|J|\()o\s+(?:Th|Sh|Ch)e\s+(?:F|St)ield"
        r":?\s*"
    ),
    # December VTM: the quote "We Have a Labor..." appears well after the Lesson header
    "Visiting Teacher Messages: 'We Have a Labor to Perform Whilst in This Tabernacle of Clay'": (
        r"(?:V|U|\()?(?:U|u)?(?:i|s|u)?s?iting\s+"
        r"(?:C|c)?(?:T|S|e)?(?:e|l)?(?:a|e)?(?:c|e)?h?er\s+"
        r"(?:M|IT|T|m)(?:\s+)?(?:l)?essages"
        r"[\s\S]{0,300}?"
        r"(?:W|V)e\s+Have\s+a\s+Labor"
    ),
    # June: OCR-corrupted titles
    # "The Testimony Plant" -> "She Sestimon y Plant"
    "The Testimony Plant": r"(?:Th|Sh|Ch)e\s+(?:T|S)estimon\s*y\s+Plant",
    # "Emily Chadwick Zaugg Makes Many Quilts" -> "&mily Chadwick Laugg Takes Many Quilts"
    "Emily Chadwick Zaugg Makes Many Quilts": r"[&E]mily\s+Chadwick\s+[LZ]augg\s+[TM](?:a|ak)kes?\s+Many\s+Quilts",
    # "Early American Hooked Rug Design" -> "&arly American Flooked Rug Design"
    "Early American Hooked Rug Design": r"[&E]arly\s+American\s+(?:Fl|H)ooked\s+Rug\s+Design",
    # "Appreciation for the Singing Mothers" -> "Appreciation for the Singing Mlothers"
    "Appreciation for the Singing Mothers": r"Appreciation\s+for\s+the\s+Singing\s+(?:M|Ml)others",
    # "'A Thing of Beauty'" - handle various quote styles at position 0
    "'A Thing of Beauty' -- Restoration of the Heber C. Kimball Home": (
        r'["\u201c\u201d\u2018\u2019\u0027]?A\s+(?:Th|Sh|Ch)ing\s+of\s+Beauty["\u201c\u201d\u2018\u2019\u0027]?'
    ),
    # "The Manti Temple" -> "She Mlanti Semple"
    "The Manti Temple": r"(?:Th|Sh|Ch)e\s+(?:M|Ml)anti\s+(?:T|S)emple",
    # Poem OCR fixes — garbled or common-word titles needing disambiguation
    # "Today" (Jan) → "Socket Etta Robbins Today" - author precedes title in OCR
    "Today": r"(?:Socket\s+)?Etta\s+Robbins\s+Today",
    # "Snow" (Feb) → "Snow Christie Lund Coles" - "Eliza R. Snow" matches first otherwise
    "Snow": r"Snow\s+Christie\s+Lund\s+Coles",
    # "Home" (Aug) → "Flome Catherine E.. Berry" - garbled frontispiece
    "Home": r"(?:F|Fl|H)ome\s+Catherine\s+E\.+\s+Berry",
    # "Mood" (Sep) → "Ilood Vesta N. Lukei" - garbled
    "Mood": r"(?:M|Il|I)ood\s+Vesta\s+N\.?\s+Lukei",
    # "Flower of Light" (Sep) → "Si lower of Light Eva Willes Wangsgaard" - garbled
    "Flower of Light": r"(?:F|S|Si\s*)(?:l|f)ower\s+of\s+Light\s+Eva\s+Willes",
    # "Mountain Vigil" (Jul) → "Nountain Vigil Ethel Jacobson" - garbled
    "Mountain Vigil": r"(?:M|N)ountain\s+Vigil",
    # "Her Hands" (Jul) → body heading
    "Her Hands": r"Her\s+Hands\s+Luella\s+N\.?\s+Adams",
    # "Wega" (Jul) → body heading
    "Wega": r"Wega\s+Grace\s+Barker\s+Wilson",
    # "My Prayer" (Jul) → "INy Prayer Irene E. Jones" - garbled
    "My Prayer": r"(?:M|IN|I)y\s+Prayer\s+Irene\s+E\.?\s+Jones",
    # "Night Incident" (Oct) → "Night Gneident Lael W. Hill" - garbled
    "Night Incident": r"Night\s+(?:I|G)n(?:c|e)ident\s+Lael\s+W\.?\s+Hill",
    # "I Thank Thee" (Oct) → "L Shank Shee Caroline Eyring Miner" - garbled
    "I Thank Thee": r"(?:I|L)\s*(?:Th|Sh|Ch)ank\s+(?:Th|Sh|Ch)ee\s+Caroline",
    # "Octave" (Dec) → "Octa ve Dorothy ]. Roberts" - garbled
    "Octave": r"Octa\s*ve\s+Dorothy\s+[\].]?\s*Roberts",
    # Serial fiction: Hermanas (continuation from Vol42)
    "Hermanas, Chapter 7": _HERMANAS_CHAPTER_PAT + r"(?:7|VII)\b",
    "Hermanas, Chapter 8": _HERMANAS_CHAPTER_PAT + r"(?:8|VIII)\b",
    # Serial fiction: There Is Still Time
    "There Is Still Time, Chapter 1": _THERE_IS_STILL_TIME_CHAPTER_PAT + r"(?:1|I)\b",
    "There Is Still Time, Chapter 2": _THERE_IS_STILL_TIME_CHAPTER_PAT + r"(?:2|II)\b",
    "There Is Still Time, Chapter 3": _THERE_IS_STILL_TIME_CHAPTER_PAT + r"(?:3|III)\b",
    "There Is Still Time, Chapter 4": _THERE_IS_STILL_TIME_CHAPTER_PAT + r"(?:4|IV)\b",
    "There Is Still Time, Chapter 5": _THERE_IS_STILL_TIME_CHAPTER_PAT + r"(?:5|V)\b",
    # Serial fiction: Heart's Bounty
    "Heart's Bounty, Chapter 1": _HEARTS_BOUNTY_CHAPTER_PAT + r"(?:1|I)\b",
    "Heart's Bounty, Chapter 2": _HEARTS_BOUNTY_CHAPTER_PAT + r"(?:2|II)\b",
    "Heart's Bounty, Chapter 3": _HEARTS_BOUNTY_CHAPTER_PAT + r"(?:3|III)\b",
    "Heart's Bounty, Chapter 4": _HEARTS_BOUNTY_CHAPTER_PAT + r"(?:4|IV)\b",
    "Heart's Bounty, Chapter 5": _HEARTS_BOUNTY_CHAPTER_PAT + r"(?:5|V)\b",
    "Heart's Bounty, Chapter 6": _HEARTS_BOUNTY_CHAPTER_PAT + r"(?:6|VI)\b",
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

    source_rel_path = f"cleaned-data/relief-society/txtvolumesbymonth/Vol43"
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
        description="Extract Relief Society Magazine Vol 43 into individual entries"
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

    for (vol, issue_key), entries in VOL43_TOC.items():
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
