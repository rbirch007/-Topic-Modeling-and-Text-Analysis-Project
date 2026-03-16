#!/usr/bin/env python3
"""
Extraction script for Relief Society Magazine Volume 51 (1964).

Reads cleaned monthly issue files from cleaned-data/ and extracts them into
individual entries (articles, poems, editorials, fiction, lessons, etc.).

Each entry is matched by searching for its title in the body text (after
splitting off front matter).  Results are written as separate text files
plus a per-volume JSON containing full content.

Usage:
    python extract_vol51.py
    python extract_vol51.py --dry-run
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

VOL51_TOC = {
    ("Vol51", "No01_January_1964"): [
        {"title": "Happy New Year!", "author": "General Presidency", "etype": "article"},
        {"title": "Purpose of the Relief Society", "author": "Joseph Fielding Smith", "etype": "article"},
        {"title": "John Fitzgerald Kennedy", "author": None, "etype": "article"},
        {"title": "Award Winners \u2014 Eliza R. Snow Poem Contest", "author": None, "etype": "article"},
        {"title": "The First to Go", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Verdure", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "Quo Vadis?", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Award Winners \u2014 Annual Relief Society Short Story Contest", "author": None, "etype": "article"},
        {"title": "Moment of Trust", "author": "Mary Ek Knowles", "etype": "fiction"},
        {"title": "The New March of Dimes", "author": None, "etype": "article"},
        {"title": "What Is a Work Meeting Leader?", "author": "Sylvia Lundgren", "etype": "article"},
        {"title": "Carol's Christmas", "author": "Adelle Ashby", "etype": "fiction"},
        {"title": "The Lost Star", "author": "Hazel K. Todd", "etype": "fiction"},
        {"title": "Kiss of the Wind, Chapter 7", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Day of the Lamanite", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Bound Volumes of the 1963 Magazines", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Playtime Recipes", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Ear Huggers Are Nice", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Stretching the Food Budget \u2014 Part IV \u2014 Dried Beans", "author": "Marion Bennion", "etype": "article"},
        {"title": "Della Gleed's Hobby Is Making Gifts", "author": None, "etype": "article"},
        {"title": "Theology: Missionary Service", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Wherefore, Be Not Weary in Well-Doing", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Planning the Family Wardrobe", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: Sinclair Lewis, American Self-Satirist", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: The Opportunity and Responsibility of a Calling in Church Government", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Give Me These", "author": "Elsie F. Parton", "etype": "poem"},
        {"title": "Exile", "author": "Gilean Douglas", "etype": "poem"},
        {"title": "First Heartaches", "author": "Gladys Hesser Burnham", "etype": "poem"},
        {"title": "Camoes", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Precious Moment", "author": "Verda P. Bollschweiler", "etype": "poem"},
    ],

    ("Vol51", "No02_February_1964"): [
        {"title": "The Influence and Responsibility of Women", "author": "Harold B. Lee", "etype": "article"},
        {"title": "In Memoriam \u2014 President Levi Edgar Young", "author": None, "etype": "article"},
        {"title": "Lenore C. Gundersen Appointed to the General Board", "author": "Fawn H. Sharp", "etype": "article"},
        {"title": "Heart Fund Dollars Buy Life and Hope", "author": None, "etype": "article"},
        {"title": "National Children's Dental Health Week", "author": None, "etype": "article"},
        {"title": "Mama Lives in the Kitchen", "author": "Lael J. Littke", "etype": "fiction"},
        {"title": "Your Heart to Understanding, Chapter 1", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "Kiss of the Wind, Chapter 8", "author": "Rosa Lee Lloyd", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Words That Women Write", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes to the Field: Index for 1963 Relief Society Magazine Available", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "A Valentine for My Husband", "author": "Helen Hinckley Jones", "etype": "article"},
        {"title": "Easy Valentine Cake", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "Stretching the Food Budget \u2014 Part V \u2014 Wonders With Wheat", "author": "Marion Bennion", "etype": "article"},
        {"title": "Letty S. Mickelson Makes Appliqued Quilts for Relief Society Bazaars", "author": None, "etype": "article"},
        {"title": "Theology: The Book of Revelation", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And Ye Shall Bear Record of Me", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Planning Family Recreation", "author": "Virginia F. Cutler", "etype": "lesson"},
        {"title": "Literature: Carl Sandburg, American Folk Singer", "author": "Briant S. Jacobs", "etype": "lesson"},
        {"title": "Social Science: Summary of Organization and Structure of the Church", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Sonnet to the Sea", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "White Miracle", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Memories on a Winter Night", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Book Interest", "author": "Pearle M. Olsen", "etype": "poem"},
        {"title": "Womanhood", "author": "Mary Brown", "etype": "poem"},
        {"title": "Did the Groundhog See His Shadow?", "author": "Evalyn M. Sandberg", "etype": "poem"},
        {"title": "Remembering", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Frozen Splendor", "author": "Gladys Hesser Burnham", "etype": "poem"},
    ],

    ("Vol51", "No03_March_1964"): [
        {"title": "Train Up a Child", "author": "Marion G. Romney", "etype": "article"},
        {"title": "Charity Never Faileth", "author": "Edith P. Backman", "etype": "article"},
        {"title": "The American Red Cross and Its Blood Donor Program", "author": "Perkins McGuire", "etype": "article"},
        {"title": "What Does Your Speech Reveal?", "author": "Myrtle Henderson", "etype": "article"},
        {"title": "My Father's Violin", "author": "Afton Brown", "etype": "article"},
        {"title": "Someone to Cheer for Johnny", "author": "Myrtle M. Dean", "etype": "fiction"},
        {"title": "More Value Than Many Sparrows", "author": "Margaret Woods", "etype": "fiction"},
        {"title": "Second Chance", "author": "Blanche M. Hollingsworth", "etype": "fiction"},
        {"title": "Your Heart to Understanding, Chapter 2", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: We Are the Beneficiaries", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Don't You Love Your Sisters!", "author": "Margaret Russell", "etype": "article"},
        {"title": "The World's Favorite Roses", "author": "Dorothy J. Roberts", "etype": "article"},
        {"title": "Painting With Glass", "author": "Florian H. Thayn", "etype": "article"},
        {"title": "Parleys Sixth Ward Country Fair Bazaar", "author": "VaLora M. Anderson", "etype": "article"},
        {"title": "Planning a Successful Bazaar", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "Recipes for Springtime Charlotte Russe", "author": "Ingrid W. Olsen", "etype": "article"},
        {"title": "Desserts That Please the Family", "author": "Florence K. Gates", "etype": "article"},
        {"title": "Cast Your Bread Upon the Waters", "author": "Olive Sharp", "etype": "article"},
        {"title": "Charlotte H. Singley \u2014 Landscape Artist", "author": None, "etype": "article"},
        {"title": "The Heart Looks Up", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "On Spring and Love", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Spring", "author": "Rowena Jensen Bills", "etype": "poem"},
        {"title": "Who Plants a Garden", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Curled in Seed", "author": "Alice Morrey Bailey", "etype": "poem"},
        {"title": "Fawn at the Pool", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "Our Chapel", "author": "Margaret B. Shomaker", "etype": "poem"},
        {"title": "Feather Grief", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Pathway", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Thanks for These", "author": "Caroline Eyring Miner", "etype": "poem"},
    ],

    ("Vol51", "No04_April_1964"): [
        {"title": "Exalting the Poor", "author": "John H. Vandenberg", "etype": "article"},
        {"title": "Marjorie C. Pingree Appointed to the General Board", "author": "Elizabeth B. Winters", "etype": "article"},
        {"title": "Darlene C. Dedekind Appointed to the General Board", "author": "Irene W. Buehner", "etype": "article"},
        {"title": "Cleone R. Eccles Appointed to the General Board", "author": "Jeanette M. Morrell", "etype": "article"},
        {"title": "The Worth of The Relief Society Magazine", "author": "T. Bowring Woodbury", "etype": "article"},
        {"title": "Springtime Thoughts of a Happy Woman", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Cancer's Two Deadly Gaps", "author": None, "etype": "article"},
        {"title": "The Storm", "author": "Lila Spencer", "etype": "fiction"},
        {"title": "More Blessed to Receive", "author": "Nita Ellis", "etype": "fiction"},
        {"title": "Please, Not This One", "author": "Merle E. Feriante", "etype": "fiction"},
        {"title": "Be Yourself", "author": "Betty G. Spencer", "etype": "fiction"},
        {"title": "Your Heart to Understanding, Chapter 3", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Relief Society Time at the New York World's Fair", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Lesson Previews to Appear in the June Issue", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Application", "author": "Louise Murray", "etype": "article"},
        {"title": "Stretching the Food Budget Part VI \u2014 Make the Most of Economical Cuts of Meat", "author": "Sadie O. Morris", "etype": "article"},
        {"title": "The Other Day", "author": "Christie Lund Coles", "etype": "article"},
        {"title": "How About a Get It Done Day?", "author": "Vera Stocker", "etype": "article"},
        {"title": "It Was Springtime", "author": "Evelyn Fjeldsted", "etype": "article"},
        {"title": "Round-Table Togetherness", "author": "Violet Nimmo", "etype": "article"},
        {"title": "Cover-Ups for Older Tots", "author": "Adelle Ashby", "etype": "article"},
        {"title": "Sara C. Bowles Makes Many Quilts and Quilted Bedspreads", "author": None, "etype": "article"},
        {"title": "Quest", "author": "Lucille Rampton Perry", "etype": "poem"},
        {"title": "Neighbor Child", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Enchanted Valley", "author": "Sylvia Probst Young", "etype": "poem"},
        {"title": "Spring's Impulse", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "Quintet for Spring", "author": "Hazel Loomis", "etype": "poem"},
        {"title": "The Warblers", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Earth Renewed", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Desert in Springtime", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "His Forgiving Way", "author": "Viola Ashton Candland", "etype": "poem"},
        {"title": "Gift Apron", "author": "Beulah Huish Sadleir", "etype": "poem"},
        {"title": "The Heavens Proclaim", "author": "Veda G. Linford", "etype": "poem"},
        {"title": "Spring Fashion", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "Stimulus", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Empty Nest", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "An Evening Thought", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Halfway", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Deserted Farm House", "author": "Annie Atkin Tanner", "etype": "poem"},
        {"title": "Sleep", "author": "Iris W. Schow", "etype": "poem"},
        {"title": "Thank You", "author": "Lorretta Hanson", "etype": "poem"},
        {"title": "Zaccheus", "author": "Linnie Fisher Robinson", "etype": "poem"},
    ],

    ("Vol51", "No05_May_1964"): [
        {"title": "The Family Goes Back to School", "author": "Elaine Reiser Alder", "etype": "article"},
        {"title": "Literary Contest Announcements 1964", "author": None, "etype": "article"},
        {"title": "Your Poem and You", "author": "Eva Willes Wangsgaard", "etype": "article"},
        {"title": "Art As a Hobby", "author": "Myrtle E. Henderson", "etype": "article"},
        {"title": "Magazine Honor Roll for 1963", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Adjustment", "author": "Alice Gubler Sabin", "etype": "fiction"},
        {"title": "Gramps and Beanie", "author": "Shirley Thulin", "etype": "fiction"},
        {"title": "Your Heart to Understanding, Chapter 4", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Lighted Window", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "What Is a Mother?", "author": "Margaret Richards", "etype": "article"},
        {"title": "Mother \u2014 Pioneer of the Present", "author": "Hazel Sowards Cannon", "etype": "article"},
        {"title": "Thanks, Mom, for Everything", "author": "Sara O. Moss", "etype": "article"},
        {"title": "Verdi on the Farm", "author": "Winona F. Thomas", "etype": "article"},
        {"title": "Ask Mrs. Braithwaite", "author": "Janice Dixon", "etype": "article"},
        {"title": "A Letter From Grandma", "author": "Violet Nimmo", "etype": "article"},
        {"title": "A Call in the Night", "author": "Mary E. Gallamore", "etype": "article"},
        {"title": "Cuddly Crib Quilts", "author": "Adelle Ashby", "etype": "article"},
        {"title": "Sewing Time", "author": "Dorothy C. Little", "etype": "article"},
        {"title": "Potpourri of Handy Hints", "author": "Jo M. Stock", "etype": "article"},
        {"title": "Recipes From the Southwest", "author": "Romaine R. Cooksey", "etype": "article"},
        {"title": "Appetite Teasers for Preschoolers", "author": "Margaret Maxwell", "etype": "article"},
        {"title": "Oatmeal Date Bread", "author": "Anne Marie Astle", "etype": "article"},
        {"title": "Select Your Own Dessert", "author": "Patricia M. Faas", "etype": "article"},
        {"title": "Transfers for Children's Clothing", "author": "Janet W. Breeze", "etype": "article"},
        {"title": "From the Dear Windows of Home", "author": "Leona Fetzer Wintch", "etype": "article"},
        {"title": "Alma A. Fernelius \u2014 Lady of Lovely Crocheting", "author": None, "etype": "article"},
        {"title": "The Right Key", "author": "Luella Foster", "etype": "article"},
        {"title": "Whom Should I Seek?", "author": "Blanche Briggs", "etype": "article"},
        {"title": "Joy", "author": "Olive Sharp", "etype": "article"},
        {"title": "Double Your Pleasure \u2014 Double Your Subscription", "author": "Ouida Johns Pedersen", "etype": "article"},
        {"title": "Life Is Beautiful", "author": "Ida G. Hepworth", "etype": "article"},
        {"title": "I Am Crowded Within", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Secret", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Spring Lambs", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "My Neighbor", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Valley Sky at Sunrise", "author": "Pearle M. Olsen", "etype": "poem"},
        {"title": "Renewal", "author": "Caroline Eyring Miner", "etype": "poem"},
        {"title": "Prayer for My Daughter", "author": "Carolle Denton", "etype": "poem"},
        {"title": "Prayer for People", "author": "Gilean Douglas", "etype": "poem"},
        {"title": "Memory", "author": "Grace Barker Wilson", "etype": "poem"},
        {"title": "Summer Song", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "We Pass But Once", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Unsaid Words", "author": "Zara Sabin", "etype": "poem"},
        {"title": "Shape of Time", "author": "Thelma J. Lund", "etype": "poem"},
        {"title": "My Mother", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol51", "No06_June_1964"): [
        {"title": "Birthday Congratulations to Emma Ray Riggs McKay", "author": None, "etype": "article"},
        {"title": "Paul H. Dunn Appointed to the First Council of Seventy", "author": None, "etype": "article"},
        {"title": "You, Dear Friends", "author": "Leone O. Jacobs", "etype": "article"},
        {"title": "My English Textbook", "author": "Martha Weyland Paulsen", "etype": "article"},
        {"title": "Emily Dickinson \u2014 Occupation \u2014 Keeps House", "author": "Laurel Ulrich", "etype": "article"},
        {"title": "Annual Report for 1963", "author": "Hulda Parker", "etype": "article"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Editorial: The 134th Annual General Church Conference", "author": None, "etype": "editorial"},
        {"title": "In Memoriam \u2014 Gladys P. Young", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes From the Field", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Gretchen Stratton Makes Prize-Winning Pillows", "author": None, "etype": "article"},
        {"title": "Anna Marie Richins \u2014 Expert in Handicraft", "author": None, "etype": "article"},
        {"title": "Happiness in Flower Arranging", "author": "Maude W. Howard", "etype": "article"},
        {"title": "Parchment Correspondence Cards", "author": "Peggy Tangren", "etype": "article"},
        {"title": "Your Heart to Understanding, Chapter 5", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "Theology: The Doctrine and Covenants", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "The Shadowfall", "author": "Shirley M. Howard", "etype": "poem"},
        {"title": "Always the Moment", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Tide Turn", "author": "Marjorie Newton", "etype": "poem"},
        {"title": "Airborne Heritage", "author": "Viola Ashton Candland", "etype": "poem"},
        {"title": "Night", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "Bird Song", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "One Year Old", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "The Secret", "author": "Eleanor W. Schow", "etype": "poem"},
        {"title": "Enchanted", "author": "Vesta N. Fairbairn", "etype": "poem"},
    ],

    ("Vol51", "No07_July_1964"): [
        {"title": "Victory Over Wilderness", "author": "Alberta H. Christensen", "etype": "article"},
        {"title": "Disciplining Children in the Latter-day Saint Home", "author": "Nell Folkman", "etype": "article"},
        {"title": "A Tribute to Relief Society Presidents", "author": "Lois Tanner", "etype": "article"},
        {"title": "The Young Go Visiting Teaching", "author": "Ella K. Hundsman", "etype": "article"},
        {"title": "Don't Leave Me", "author": "Helen B. Gibbons", "etype": "fiction"},
        {"title": "Momentous Decision", "author": "Beatrice R. Parsons", "etype": "fiction"},
        {"title": "Your Heart to Understanding, Chapter 6", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: The Intent of The Heart", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Thoughts for a Summer Day", "author": "Caroline Eyring Miner", "etype": "article"},
        {"title": "Diet As a Way of Life", "author": "Margaret F. Maxwell", "etype": "article"},
        {"title": "How to Remodel a Fitted Suit", "author": "Bonnie S. Hansen", "etype": "article"},
        {"title": "Train Up a Child", "author": "Dorothy Hicks", "etype": "article"},
        {"title": "Delilah Davis Pike \u2014 Her Hobbies Bring Happiness", "author": None, "etype": "article"},
        {"title": "The Sego Lily \u2014 Flower of the Pioneers", "author": "Celia Luce", "etype": "article"},
        {"title": "I'm Writing Five Books", "author": "Janice Dixon", "etype": "article"},
        {"title": "Acceptance", "author": "Irene T. Irving", "etype": "article"},
        {"title": "Growing Old", "author": "Lizzie O. B. White", "etype": "article"},
        {"title": "Violets Are for Remembrance", "author": "Carol Mattson", "etype": "article"},
        {"title": "Humble Values", "author": "Alice R. Rich", "etype": "article"},
        {"title": "Theology: The Vision", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Leave Judgment Alone With Me", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Introduction", "author": "Winnifred C. Jardine", "etype": "lesson"},
        {"title": "Literature: The Appreciation and Criticism of Literature", "author": "Robert K. Thomas", "etype": "lesson"},
        {"title": "Social Science: The Application of Divine Law to Life", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Notes on the Authors of the Lessons", "author": None, "etype": "article"},
        {"title": "For a Grandchild", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Our Pioneer Centenarians", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Let Me Remember", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Faith", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Night is My Friend", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "Alchemy", "author": "Vesta N. Fairbairn", "etype": "poem"},
        {"title": "Her Letter", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Grandmother's Chair", "author": "Carolle Denton", "etype": "poem"},
        {"title": "Prayer", "author": "Nellie B. Schmidt", "etype": "poem"},
        {"title": "To My Dear Sisters in Relief Society", "author": "Marion Mallorie", "etype": "poem"},
        {"title": "Come and See", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Potential", "author": "Pearle M. Olsen", "etype": "poem"},
        {"title": "Faith's Magic Wand", "author": "Ida Elaine James", "etype": "poem"},
    ],

    ("Vol51", "No08_August_1964"): [
        {"title": "For the Blessing of the Lamanites", "author": "Boyd K. Packer", "etype": "article"},
        {"title": "Singing Mothers Present Music at the American Mother Rainbow Awards Luncheon", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Our Board Goes to Conference", "author": "Marjorie M. Reeve", "etype": "article"},
        {"title": "Promotion Posters for The Relief Society Magazine", "author": "Gertrude Keeler", "etype": "article"},
        {"title": "Mr. Lewis and the Ravens \u2014 Part I", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "The Someday Vacation", "author": "Alice Gubler Sabin", "etype": "fiction"},
        {"title": "Your Heart to Understanding, Chapter 7", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Compassion Becometh a Woman", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "In Memoriam \u2014 Alice Bitner Castleton", "author": None, "etype": "article"},
        {"title": "Notes to the Field: The Relief Society Annual General Conference", "author": None, "etype": "article"},
        {"title": "Notes From the Field \u2014 Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Raw Apple Cookies", "author": "Florence L. Mecham", "etype": "article"},
        {"title": "Butter Mints", "author": "Dorothy Scott", "etype": "article"},
        {"title": "Pretty Pillows, Useful, Too", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Gleanings From the Summer", "author": "Nancy M. Armstrong", "etype": "article"},
        {"title": "Pauline B. Wale's Hobby \u2014 Seventy Years of Quiltmaking", "author": None, "etype": "article"},
        {"title": "Theology: The Vision (Continued)", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: And He Who Receiveth All Things With Thankfulness", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Health and Its Influence on Happiness", "author": "Winnifred C. Jardine", "etype": "lesson"},
        {"title": "Literature: Literature Tasted...Swallowed...Digested", "author": "Robert K. Thomas", "etype": "lesson"},
        {"title": "Social Science: The Function of Divine Law in Family Relations", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "House of Earth", "author": "Carlton Culmsee", "etype": "poem"},
        {"title": "Maria's Return", "author": "Lottie H. Singley", "etype": "poem"},
        {"title": "Nijoni", "author": "Helen Fletcher Collins", "etype": "poem"},
        {"title": "The Evening and Morning of the First Day", "author": "Carolle Denton", "etype": "poem"},
        {"title": "Sequel", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "The Happy Children", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "A Plea for Loveliness", "author": "Alice Lowe Corbett", "etype": "poem"},
        {"title": "I Stood on a Hilltop", "author": "Vilate R. McAllister", "etype": "poem"},
        {"title": "Lonely", "author": "Catherine B. Bowles", "etype": "poem"},
        {"title": "Golden Wedding Anniversary", "author": "Ida Elaine James", "etype": "poem"},
        {"title": "The Goodness of a Child", "author": "Christie Lund Coles", "etype": "poem"},
    ],

    ("Vol51", "No09_September_1964"): [
        {"title": "The Presence of a Prophet", "author": None, "etype": "article"},
        {"title": "Nursing and Nurse Training in Relief Society", "author": "Irene B. Woodford", "etype": "article"},
        {"title": "Relief Society Magazine Receives Outstanding Award", "author": None, "etype": "article"},
        {"title": "Singing Mothers Concerts at the New York World's Fair", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Relief Society History and Activities", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Your Heart to Understanding, Chapter 8", "author": "Hazel M. Thomson", "etype": "fiction"},
        {"title": "Mr. Lewis and the Ravens \u2014 Part II", "author": "Ilene H. Kingsbury", "etype": "fiction"},
        {"title": "Snakes, Snails, and Puppy Dog Tails", "author": "Janice Dixon", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Placing The Relief Society Magazine in Public Libraries", "author": "Ferrel Christensen", "etype": "article"},
        {"title": "Editorial: The Standard of Liberty", "author": "Vesta P. Crawford", "etype": "editorial"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Hannah Nyquist Specializes in Making Afghans and Rugs", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Theology: The Church Independent", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Every Man Seeking the Interest of His Neighbor", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Personal Attractiveness, a Factor in Happiness", "author": "Winnifred C. Jardine", "etype": "lesson"},
        {"title": "Literature: Poetry of Faith in God and Man Part I", "author": "Bruce B. Clark", "etype": "lesson"},
        {"title": "How Good the Sun", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Wide Autumn", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Old Wagon Wheel", "author": "Jeanette Swanson", "etype": "poem"},
        {"title": "On Seeing a Son off to School", "author": "Evalyn M. Sandberg", "etype": "poem"},
        {"title": "Autumn Raiment", "author": "Ethel Jacobson", "etype": "poem"},
    ],

    ("Vol51", "No10_October_1964"): [
        {"title": "So Grateful That You Taught Me", "author": "Edith S. Elliott", "etype": "article"},
        {"title": "A Scenic View of the Literature Lessons", "author": "Vilate R. McAllister", "etype": "article"},
        {"title": "They Joined the Church Because of Relief Society", "author": "Maxine T. Grimm", "etype": "article"},
        {"title": "Three of a Family", "author": "Marvel M. Young", "etype": "article"},
        {"title": "Point of View Is Important", "author": "Leona Fetzer Wintch", "etype": "article"},
        {"title": "Gleanings for the Harvest Season", "author": "Celia Luce", "etype": "article"},
        {"title": "The Turn of the Wheel, Chapter 1", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "The Good Fountain", "author": "Joyce B. Bailey", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Editorial: Refinement in Relief Society Activities", "author": "Louise W. Madsen", "etype": "editorial"},
        {"title": "Retirements From the General Board", "author": None, "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": "Hulda Parker", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Blessed Autumn", "author": "Dorothy J. Smith", "etype": "article"},
        {"title": "Tiny Sewing Kit", "author": "Shirley Thulin", "etype": "article"},
        {"title": "Celia Willden Jensen \u2014 Artist of Rugs and Quilts", "author": None, "etype": "article"},
        {"title": "The Cents-less Ghost Has Gone", "author": "Genevieve Van Wagenen", "etype": "article"},
        {"title": "The Day After Halloween", "author": "Janice T. Dixon", "etype": "article"},
        {"title": "Comparative British and American Measures", "author": "Sandra M. Covey", "etype": "article"},
        {"title": "Recipes From Scotland", "author": "Kathleen S. Farnsworth", "etype": "article"},
        {"title": "Theology: Individual Responsibility as a Member and Missionary", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: That Every Man May Improve Upon His Talent", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Literature: Poetry of Faith in God and Man \u2014 Part II", "author": "Bruce B. Clark", "etype": "lesson"},
        {"title": "Social Science: The Function of Divine Law in Family Relations (Continued)", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Leaf By Leaf", "author": "Alberta Christensen", "etype": "poem"},
        {"title": "Why?", "author": "Linnie M. Findlay", "etype": "poem"},
        {"title": "Beauty", "author": "Rowena Jensen Bills", "etype": "poem"},
        {"title": "Wind", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Day in Late October", "author": "Lael W. Hill", "etype": "poem"},
        {"title": "The Golden, Golden Time", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Dew on a Peace Rose", "author": "Judy Paget", "etype": "poem"},
        {"title": "Requited", "author": "Bertha A. Kleinman", "etype": "poem"},
        {"title": "Autumn Color After the Rains", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Inspiration From Zion National Park", "author": "Vesta Nickerson Fairbairn", "etype": "poem"},
        {"title": "Wood Duck on Water", "author": "Ethel Jacobson", "etype": "poem"},
    ],

    ("Vol51", "No11_November_1964"): [
        {"title": "Home, the Child Training Center", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Take My Yoke Upon You", "author": "Marianne C. Sharp", "etype": "article"},
        {"title": "Happiness", "author": "Louise W. Madsen", "etype": "article"},
        {"title": "Report and Official Instructions", "author": "Belle S. Spafford", "etype": "article"},
        {"title": "Our Roots Go Deep", "author": "Frances C. Yost", "etype": "fiction"},
        {"title": "The Turn of the Wheel, Chapter 2", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Editorial: Gratitude", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "Notes to the Field: Relief Society Lessons on Talking Book Records Again Available for the Sightless", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Notes From the Field: Relief Society Activities", "author": None, "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Special Rolls for Christmas", "author": "Melba Christensen", "etype": "article"},
        {"title": "A Welcome Christmas Gift", "author": "Vilate R. McAllister", "etype": "article"},
        {"title": "Colorful Felt Pictures", "author": "June F. Krambule", "etype": "article"},
        {"title": "Little Acts of Kindness and of Love", "author": "Elsie C. Carroll", "etype": "article"},
        {"title": "Crocheting Is a Fine Art for Callie Haggard", "author": None, "etype": "article"},
        {"title": "Theology: The Revelation on Priesthood", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: Watch, for the Adversary Spreadeth His Dominions", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: Teaching the Gospel in the Home \u2014 Introduction", "author": "Alberta H. Christensen", "etype": "lesson"},
        {"title": "Literature: Exploring Right and Wrong Attitudes Through Literature \u2014 Part I", "author": "Robert K. Thomas", "etype": "lesson"},
        {"title": "Social Science: Priesthood Function and the Community", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Sound of Storm and Praise", "author": "Margery S. Stewart", "etype": "poem"},
        {"title": "Snow to Snow", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Signature of Faith", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "Grace for God's Bounty", "author": "Ethel Jacobson", "etype": "poem"},
        {"title": "Time", "author": "Evelyn Fjeldsted", "etype": "poem"},
        {"title": "Undefeated", "author": "Ruth H. Chadwick", "etype": "poem"},
        {"title": "Our Garden Gate", "author": "Rowena Jensen Bills", "etype": "poem"},
        {"title": "First Grand Addition", "author": "Leora Larsen", "etype": "poem"},
        {"title": "Hold Back This Autumn", "author": "Linnie F. Robinson", "etype": "poem"},
        {"title": "Brief Hour", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "Another Season", "author": "Christie Lund Coles", "etype": "poem"},
        {"title": "Design", "author": "Vesta N. Fairbairn", "etype": "poem"},
    ],

    ("Vol51", "No12_December_1964"): [
        {"title": "Effects of Home Influence", "author": "N. Eldon Tanner", "etype": "article"},
        {"title": "Edythe Kimball Watson Appointed to the General Board", "author": "Ellen Neilson Barnes", "etype": "article"},
        {"title": "Kathryn Smith Gilbert Appointed to the General Board", "author": "Verda Fox Burton", "etype": "article"},
        {"title": "Myrtle Robison Olson Appointed to the General Board", "author": "Alice Colton Smith", "etype": "article"},
        {"title": "The 1964 Relief Society Annual General Conference", "author": "Hulda Parker", "etype": "article"},
        {"title": "Christmas Seals \u2014 A Needed Gift", "author": "James E. Perkins", "etype": "article"},
        {"title": "Turn of the Wheel, Chapter 3", "author": "Mabel Harmer", "etype": "fiction"},
        {"title": "The Christmas Gift", "author": "Kathryn E. Franks", "etype": "fiction"},
        {"title": "From Near and Far", "author": None, "etype": "article"},
        {"title": "Editorial: Who's on The Lord's Side, Who?", "author": "Marianne C. Sharp", "etype": "editorial"},
        {"title": "The 134th Semi-Annual Church Conference", "author": None, "etype": "article"},
        {"title": "Woman's Sphere", "author": "Ramona W. Cannon", "etype": "article"},
        {"title": "Birthday Congratulations", "author": None, "etype": "article"},
        {"title": "Christmas in Gold and Green", "author": None, "etype": "article"},
        {"title": "Strawberries Decorate a Holiday Table", "author": None, "etype": "article"},
        {"title": "Golden Pictures Made From Keepsakes", "author": "Beatrice S. Poelman", "etype": "article"},
        {"title": "Christmas Comes in Color", "author": "Florence S. Jacobsen", "etype": "article"},
        {"title": "The Christmas Tree Wears Roses", "author": None, "etype": "article"},
        {"title": "Old-Fashioned Hardtack Candy", "author": "Fran Dunford", "etype": "article"},
        {"title": "A Table Tells the Christmas Story", "author": None, "etype": "article"},
        {"title": "Grand Junction Stake Gift Bazaar", "author": None, "etype": "article"},
        {"title": "A Candy House for Christmas", "author": None, "etype": "article"},
        {"title": "Lucy Andrews Lives in a World of Handiwork", "author": None, "etype": "article"},
        {"title": "Theology: The Revelation on Priesthood (Continued)", "author": "Roy W. Doxey", "etype": "lesson"},
        {"title": "Visiting Teacher Messages: For the Word of the Lord Is Truth", "author": "Christine H. Robinson", "etype": "lesson"},
        {"title": "Work Meeting: The Eternal Family", "author": "Alberta H. Christensen", "etype": "lesson"},
        {"title": "Literature: Exploring Right and Wrong Attitudes through Literature Part II", "author": "Robert K. Thomas", "etype": "lesson"},
        {"title": "Social Science: The Value of Church Activity", "author": "Ariel S. Ballif", "etype": "lesson"},
        {"title": "Three Shepherds", "author": "Eva Willes Wangsgaard", "etype": "poem"},
        {"title": "Christmas 1897", "author": "Enola Chamberlin", "etype": "poem"},
        {"title": "The Shepherds", "author": "Lela F. Morris", "etype": "poem"},
        {"title": "May There Always Be a Christmas", "author": "Dorothy J. Roberts", "etype": "poem"},
        {"title": "A Word", "author": "Mabel Jones Gabbott", "etype": "poem"},
        {"title": "Again Tonight", "author": "Christie Lund Coles", "etype": "poem"},
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
    year = 1964

    key = f"No{no:02d}_{month}_{year}"
    fname = f"Vol51_{key}.txt"
    ISSUE_FILES[("Vol51", key)] = (fname, month)


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
# Known OCR-mangled section headers for Vol51.
# ---------------------------------------------------------------------------
# Serial fiction chapter patterns

# Serial fiction: Kiss of the Wind (Chapters 7-8, Jan-Feb 1964, continued from vol50)
_KISS_OF_WIND_PAT = (
    r"Kiss\s+of\s+(?:th|sh|ch)e\s+Wind"
    r".{0,80}?"
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Serial fiction: Your Heart to Understanding (Chapters 1-8, Feb-Sep 1964)
# OCR sometimes splits "Understanding" as "Under Standing" or "Under standing"
_YOUR_HEART_PAT = (
    r"Your\s+Heart\s+(?:th|sh|ch|t)o\s+Under\s*(?:s|S)?tanding"
    r".{0,80}?"
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
)

# Serial fiction: Mr. Lewis and the Ravens (Parts I-II, Aug-Sep 1964)
# OCR may render "Mr." as "Mister" and drop "and the"; "Part II" as "Part IT"
_MR_LEWIS_PAT = (
    r"(?:(?:M|IT|Tl)r\.?|Mister)\s+Lewis"
    r"(?:\s+and\s+(?:th|sh|ch)e)?\s+Ravens"
    r".{0,80}?"
    r"(?:Part\s+)?"
)

# Serial fiction: The Turn of the Wheel (Chapters 1-3, Oct-Dec 1964)
# OCR may garble as "The 4 Turn, of the \WWheel" — allow stray digits,
# commas, backslashes, and doubled letters
_TURN_OF_WHEEL_PAT = (
    r"(?:Th|Sh|Ch)e?\s*\d?\s*(?:T|S|J)urn,?\s+of\s+(?:th|sh|ch)e\s+\\?W+heel"
    r".{0,80}?"
    r"(?:(?:CHAPTER|Chapter|CuapTer)\s+)?"
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
    # Body has: EDITORIAL ... VOL. 51 MONTH 1964 NO. N <actual title>
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
    # Prize poems: match actual poem headers, not the award announcement
    "The First to Go": (
        r"(?:First|1st)\s+Prize\s+(?:Poem|Winn)"
        r"[\s\S]{0,50}?"
        r"(?:Th|Sh|Ch)e\s+(?:F|St|S)irst\s+(?:T|S|J)o\s+Go"
    ),
    "Verdure": (
        r"(?:Second|2nd)\s+Prize\s+Poem"
        r"[\s\S]{0,50}?"
        r"Verdure"
    ),
    "Quo Vadis?": (
        r"(?:Third|3rd)\s+Prize\s+(?:Poem|Winn)"
        r"[\s\S]{0,50}?"
        r"Quo\s+Vadis"
    ),
    # First-prize-winning story: match the story heading, not the award announcement
    "Moment of Trust": (
        r"(?:(?:F|St|S)irst\s+)?(?:Prize|Winn)"
        r"[\s\-\u2014\u2013]*(?:ing)?\s*(?:S|J)(?:\s*t)?ory"
        r"[\s\S]{0,120}?"
        r"(?:M|IT|Tl)oment\s+of\s+(?:T|S|J)rust"
    ),
    # Oct "Why?" poem: common word "why" appears many times in fiction.
    # Match specifically after the author name following "(To be continued)"
    "Why?": (
        r"Linnie\s+(?:M|IT)\.?\s+Findlay"
        r"[\s\S]{0,30}?"
        r"Earth.s\s+gift"
    ),
    # Oct "So Grateful That You Taught Me": no author byline in body,
    # match the ALL-CAPS heading
    "So Grateful That You Taught Me": (
        r"SO\s+GRATEFUL\s+THAT\s+YOU\s+TAUGHT\s+ME"
    ),
    # Oct "The Good Fountain": author name reversed by OCR as "Bailey Joyce B."
    "The Good Fountain": (
        r"(?:Th|Sh|Ch)e\s+Good\s+(?:F|St|S)ountain"
        r"[\s\S]{0,30}?"
        r"(?:Bailey|Joyce)"
    ),
    # Nov "Report and Official Instructions": title split by photo caption.
    # Match the opening text after the garbled header.
    "Report and Official Instructions": (
        r"Report\s+and\s+Official"
        r"[\s\S]{0,300}?"
        r"(?:Officers\s+Meeting|regular\s+feature|status\s+of)"
    ),
    # Dec "The Shepherds" poem: appears inline after conference report.
    # Must match on the author name to find it as a standalone piece.
    "The Shepherds": (
        r"(?:Th|Sh|Ch)e\s+Shepherds\s+"
        r"Lela\s+F\.?\s+Morris"
    ),
    # Aug "Our Board Goes to Conference": no title heading in body,
    # match by author and opening text after "(To be concluded)"
    "Our Board Goes to Conference": (
        r"(?:Marjorie|(?:M|IT)arjorie)\s+(?:M|IT)\.?\s+Reeve"
        r"|"
        r"(?:A|AA)(?:M|m)o(?:N|n)(?:G|g)\s+many\s+of\s+the\s+blessings"
    ),
    # May sub-articles: lack headings in body text, match by author byline.
    # These flow continuously in the magazine's "Home" section.
    "Mother \u2014 Pioneer of the Present": (
        r"Hazel\s+Sowards\s+Cannon"
        r"[\s\S]{0,80}?"
        r"(?:(?:M|IT|Tl)other|(?:M|IT|Tl)OTHER|life\s+epitomized)"
    ),
    "Verdi on the Farm": (
        r"Winona\s+F\.?\s+Thomas"
        r"[\s\S]{0,80}?"
        r"(?:Big\s+Jim|Verdi|farm)"
    ),
    # June "You, Dear Friends": title is inline, match by author byline
    "You, Dear Friends": (
        r"Leone\s+O\.?\s+Jacobs"
        r"[\s\S]{0,200}?"
        r"(?:friend|Friend|happy)"
    ),
    # Serial fiction: Kiss of the Wind (Chapters 7-8, continued from vol50)
    "Kiss of the Wind, Chapter 7": _KISS_OF_WIND_PAT + r"(?:7|VII)\b",
    "Kiss of the Wind, Chapter 8": _KISS_OF_WIND_PAT + r"(?:(?:8|VIII)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: Your Heart to Understanding (Chapters 1-8)
    "Your Heart to Understanding, Chapter 1": _YOUR_HEART_PAT + r"(?:1|I)\b",
    "Your Heart to Understanding, Chapter 2": _YOUR_HEART_PAT + r"(?:2|II)\b",
    "Your Heart to Understanding, Chapter 3": _YOUR_HEART_PAT + r"(?:3|III)\b",
    "Your Heart to Understanding, Chapter 4": _YOUR_HEART_PAT + r"(?:4|IV)\b",
    "Your Heart to Understanding, Chapter 5": _YOUR_HEART_PAT + r"(?:5|V)\b",
    "Your Heart to Understanding, Chapter 6": _YOUR_HEART_PAT + r"(?:6|VI)\b",
    "Your Heart to Understanding, Chapter 7": _YOUR_HEART_PAT + r"(?:7|VII)\b",
    "Your Heart to Understanding, Chapter 8": _YOUR_HEART_PAT + r"(?:(?:8|VIII)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
    # Serial fiction: Mr. Lewis and the Ravens (Parts I-II)
    "Mr. Lewis and the Ravens \u2014 Part I": _MR_LEWIS_PAT + r"(?:I\b|1\b)",
    "Mr. Lewis and the Ravens \u2014 Part II": _MR_LEWIS_PAT + r"(?:II\b|IT\b|2\b)",
    # Serial fiction: The Turn of the Wheel (Chapters 1-3)
    # Ch 1 heading is OCR-destroyed; match via CHAPTER 1 near story opening
    "The Turn of the Wheel, Chapter 1": (
        r"(?:"
        + _TURN_OF_WHEEL_PAT + r"(?:1|I)\b"
        + r"|"
        # Fallback: garbled heading then CHAPTER 1 followed by story opening
        + r"(?:CHAPTER|Chapter|CuapTer)\s+1\s+Sprin"
        + r")"
    ),
    "The Turn of the Wheel, Chapter 2": _TURN_OF_WHEEL_PAT + r"(?:2|II)\b",
    "The Turn of the Wheel, Chapter 3": _TURN_OF_WHEEL_PAT + r"(?:(?:3|III)\b|\(?\s*(?:C|c)onclusion\s*\)?)",
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
        "General Board Association",
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
                has_lowercase_before = bool(
                    before_stripped and
                    before_stripped[-1].isalpha() and
                    before_stripped[-1].islower()
                )
                if has_lowercase_before:
                    continue  # mid-prose, not a real header
                # Accept if the matched text is ALL-CAPS — that's a strong
                # structural signal even without preceding punctuation
                # (e.g. "Paul RE VISITING TEACHER MESSAGE")
                is_allcaps_header = (
                    matched_text.strip()[:20].isupper() and
                    len(matched_text.strip()) > 5
                )
                if not has_structural_break and not has_sentence_end and not is_allcaps_header:
                    continue  # no structural context

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
        description="Extract Relief Society Magazine Vol 51 into individual entries"
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
    vol51_out = OUTPUT_DIR / "vol51"
    if vol51_out.exists() and not args.dry_run:
        print(f"Cleaning output directory: {vol51_out}")
        shutil.rmtree(vol51_out)

    total_matched = 0
    total_misc = 0
    total_bytes = 0
    issues_processed = 0
    all_manifest_rows = []

    volume_json = {}

    for (vol, issue_key), entries in VOL51_TOC.items():
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
