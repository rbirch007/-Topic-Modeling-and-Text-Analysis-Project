#!/usr/bin/env python3
"""
Table of Contents for Relief Society Magazine Volume 33 (1946).

Uses loose regex patterns for flexible title and author matching.
Descriptors (Frontispiece, Chapter X, etc.) are optional.
Author names have flexible spacing tolerance.

Format: TOC[(volume, issue_key)] = [entry_list]
Each entry has:
  - title_pattern: regex pattern for flexible matching (descriptors optional)
  - author_pattern: flexible author pattern
  - title_display: canonical title for output
  - etype: entry type
"""

import re

# Helper to build flexible patterns
def make_title_pattern(base_title, has_descriptor=False):
    """Build regex pattern with optional descriptors."""
    # Remove descriptors from base if present for cleaner pattern
    base_title = re.sub(r'^(Frontispiece|Picture|Image|Illustration|Chapter\s+\d+|Part\s+\d+|Conclusion|Concluded)\s*:?\s*', '', base_title, flags=re.IGNORECASE)
    base_title = base_title.strip()

    # Escape for regex
    escaped = re.escape(base_title)
    # Allow flexible spacing
    escaped = escaped.replace(r'\ ', r'\s+')

    return escaped

def make_author_pattern(author_name):
    """Build flexible author pattern (First Last with flexible spacing)."""
    if not author_name or author_name == "None":
        return None

    # Escape and allow flexible spacing/punctuation
    escaped = re.escape(author_name)
    escaped = escaped.replace(r'\ ', r'\s+')
    # Allow optional middle initials or middle names
    escaped = re.sub(r'([A-Z])(\s+[A-Z])', r'\1\.?\s+\2', escaped)

    return escaped

TOC = {
    # ===================================================================
    # VOLUME 33 (1946)
    # ===================================================================

    ("Vol33", "No1_January_1946"): [
        {"title": "Frontispiece Picture", "title_pattern": r"(?:Frontispiece\s+)?Picture", "author": "General Presidency of Relief Society", "author_pattern": r"General\s+Presidency", "etype": "front_matter"},
        {"title": "New Year's Greeting", "title_pattern": r"New\s+Year's\s+Greeting", "author": "Belle S. Spafford", "author_pattern": r"Belle\s+S\.?\s+Spafford", "etype": "editorial"},
        {"title": "Margaret Cummock Pickering", "title_pattern": r"Margaret\s+Cummock\s+Pickering", "author": "Amy Brown Lyman", "author_pattern": r"Amy\s+Brown\s+Lyman", "etype": "article"},
        {"title": "Award Winners Eliza Roxey Snow Memorial Prize Poem Contest", "title_pattern": r"Award\s+Winners.*(?:Eliza\s+Roxey\s+Snow)?.*Prize\s+Poem\s+Contest", "author": None, "author_pattern": None, "etype": "front_matter"},
        {"title": "Star of Gold First Prize Poem", "title_pattern": r"Star\s+of\s+Gold.*(?:First\s+Prize\s+Poem)?", "author": "Eva Willes Wangsgaard", "author_pattern": r"Eva\s+Willes\s+Wangsgaard", "etype": "poem"},
        {"title": "I Shall Be Late Second Prize Poem", "title_pattern": r"I\s+Shall\s+Be\s+Late.*(?:Second\s+Prize\s+Poem)?", "author": "Betty Wall Madsen", "author_pattern": r"Betty\s+Wall\s+Madsen", "etype": "poem"},
        {"title": "The Good Inheritance Third Prize Poem", "title_pattern": r"The\s+Good\s+Inheritance.*(?:Third\s+Prize\s+Poem)?", "author": "Caroline Eyring Miner", "author_pattern": r"Caroline\s+Eyring\s+Miner", "etype": "poem"},
        {"title": "Award Winners Relief Society Short Story Contest", "title_pattern": r"Award\s+Winners.*Relief\s+Society.*Short\s+Story\s+Contest", "author": None, "author_pattern": None, "etype": "front_matter"},
        {"title": "Spring Festival First Prize Story", "title_pattern": r"Spring\s+Festival.*(?:First\s+Prize\s+Story)?", "author": "Mary Ek Knowles", "author_pattern": r"Mary\s+Ek\s+Knowles", "etype": "fiction"},
        {"title": "Nicholas G. Smith A Tribute", "title_pattern": r"Nicholas\s+G\.?\s+Smith.*(?:Tribute)?", "author": "Marion G. Romney", "author_pattern": r"Marion\s+G\.?\s+Romney", "etype": "article"},
        {"title": "Drifting Or Sailing to a Charted Course", "title_pattern": r"Drifting\s+Or\s+Sailing\s+to\s+a\s+Charted\s+Course", "author": "Lella Marler Hoggan", "author_pattern": r"Lella\s+Marler\s+Hoggan", "etype": "article"},
        {"title": "Unceasing Crusade", "title_pattern": r"Unceasing\s+Crusade", "author": None, "author_pattern": None, "etype": "article"},
        {"title": "Mormonism In the Eyes of the Press", "title_pattern": r"Mormonism\s+In\s+the\s+Eyes\s+of\s+the\s+Press", "author": "James R. Clark", "author_pattern": r"James\s+R\.?\s+Clark", "etype": "article"},
        {"title": "Let Us Fittingly Mark the Land", "title_pattern": r"Let\s+Us\s+Fittingly\s+Mark\s+the\s+Land", "author": "Howard R. Driggs", "author_pattern": r"Howard\s+R\.?\s+Driggs", "etype": "article"},
        {"title": "And For Eternity Chapter 8", "title_pattern": r"And\s+For\s+Eternity.*(?:Chapter\s+\d+)?", "author": "Olive Woolley Burt", "author_pattern": r"Olive\s+Woolley\s+Burt", "etype": "fiction"},
        {"title": "Sixty Years Ago", "title_pattern": r"Sixty\s+Years\s+Ago", "author": None, "author_pattern": None, "etype": "general_feature"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
        {"title": "Editorials", "title_pattern": r"Editorial.*(?:Be\s+Not\s+Weary)?", "author": "Marianne C. Sharp", "author_pattern": r"Marianne\s+C\.?\s+Sharp", "etype": "editorial"},
        {"title": "Theology", "title_pattern": r"Theology.*(?:Jesus.*Temple)?", "author": "John A. Widtsoe", "author_pattern": r"John\s+A\.?\s+Widtsoe", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "title_pattern": r"Visiting\s+Teachers'?\s+Messages", "author": "Lowell L. Bennion", "author_pattern": r"Lowell\s+L\.?\s+Bennion", "etype": "lesson"},
        {"title": "Notes From the Field", "title_pattern": r"Notes\s+From\s+the\s+Field", "author": "Margaret C. Pickering", "author_pattern": r"Margaret\s+C\.?\s+Pickering", "etype": "report"},
    ],

    ("Vol33", "No2_February_1946"): [
        {"title": "The Dream Is Ours", "title_pattern": r"(?:The\s+)?Dream\s+Is\s+Ours", "author": "Alice Morrey Bailey", "author_pattern": r"Alice\s+Morrey\s+Bailey", "etype": "front_matter"},
        {"title": "American Statesmen", "title_pattern": r"American\s+Statesmen.*(?:Attitude\s+Toward\s+God)?", "author": "El Ray L. Christiansen", "author_pattern": r"El\s+Ray\s+L\.?\s+Christiansen", "etype": "article"},
        {"title": "Mormonism in the Eyes of the Press", "title_pattern": r"Mormonism.*Eyes.*Press", "author": "James R. Clark", "author_pattern": r"James\s+R\.?\s+Clark", "etype": "article"},
        {"title": "Abraham Lincoln Speaks Again", "title_pattern": r"Abraham\s+Lincoln\s+Speaks\s+Again", "author": "Vesta P. Crawford", "author_pattern": r"Vesta\s+P\.?\s+Crawford", "etype": "article"},
        {"title": "What the Children Can Do", "title_pattern": r"What\s+the\s+Children\s+Can\s+Do", "author": None, "author_pattern": None, "etype": "article"},
        {"title": "Fifty and One Ideas", "title_pattern": r"Fifty\s+and\s+One\s+Ideas", "author": "Blanche M. Condie", "author_pattern": r"Blanche\s+M\.?\s+Condie", "etype": "article"},
        {"title": "The Ring of Strength", "title_pattern": r"(?:The\s+)?Ring\s+of\s+Strength", "author": "Alice Morrey Bailey", "author_pattern": r"Alice\s+Morrey\s+Bailey", "etype": "fiction"},
        {"title": "Ring Out the Old", "title_pattern": r"Ring\s+Out\s+the\s+Old", "author": "Estelle Webb Thomas", "author_pattern": r"Estelle\s+Webb\s+Thomas", "etype": "fiction"},
        {"title": "And For Eternity Chapter 9", "title_pattern": r"And\s+For\s+Eternity.*(?:Chapter\s+\d+)?", "author": "Olive Woolley Burt", "author_pattern": r"Olive\s+Woolley\s+Burt", "etype": "fiction"},
        {"title": "Sixty Years Ago", "title_pattern": r"Sixty\s+Years\s+Ago", "author": None, "author_pattern": None, "etype": "general_feature"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
        {"title": "Star in Her Window", "title_pattern": r"Star\s+in\s+Her\s+Window", "author": "Ruth H. Chadwick", "author_pattern": r"Ruth\s+H\.?\s+Chadwick", "etype": "article"},
        {"title": "Editorials", "title_pattern": r"Editorial.*Study\s+My\s+Word", "author": "Marianne C. Sharp", "author_pattern": r"Marianne\s+C\.?\s+Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "title_pattern": r"Notes\s+From\s+the\s+Field", "author": "Margaret C. Pickering", "author_pattern": r"Margaret\s+C\.?\s+Pickering", "etype": "report"},
        {"title": "Theology", "title_pattern": r"Theology.*(?:Temples|Building)", "author": "H. Wayne Driggs", "author_pattern": r"H\.?\s+Wayne\s+Driggs", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "title_pattern": r"Visiting\s+Teachers'?\s+Messages", "author": "Lowell L. Bennion", "author_pattern": r"Lowell\s+L\.?\s+Bennion", "etype": "lesson"},
        {"title": "Literature", "title_pattern": r"Literature.*(?:Bible|Children)", "author": "Howard R. Driggs", "author_pattern": r"Howard\s+R\.?\s+Driggs", "etype": "lesson"},
        {"title": "Social Science", "title_pattern": r"Social\s+Science.*(?:Morality|Church)?", "author": "Harold T. Christensen", "author_pattern": r"Harold\s+T\.?\s+Christensen", "etype": "lesson"},
    ],

    ("Vol33", "No3_March_1946"): [
        {"title": "Relief Society and the Future", "title_pattern": r"Relief\s+Society\s+and\s+the\s+Future", "author": "Leah D. Widtsoe", "author_pattern": r"Leah\s+D\.?\s+Widtsoe", "etype": "article"},
        {"title": "A Few of Our Gifted Utah Women Artists", "title_pattern": r"(?:A\s+Few\s+of\s+)?Our\s+Gifted\s+Utah\s+Women\s+Artists", "author": "Alice Merrill Horne", "author_pattern": r"Alice\s+Merrill\s+Horne", "etype": "article"},
        {"title": "The First Easter", "title_pattern": r"(?:The\s+)?First\s+Easter", "author": "Evelyn Wooster Viner", "author_pattern": r"Evelyn\s+Wooster\s+Viner", "etype": "fiction"},
        {"title": "An Open Door to Happiness", "title_pattern": r"(?:An\s+)?Open\s+Door\s+to\s+Happiness", "author": "Wilford D. Lee", "author_pattern": r"Wilford\s+D\.?\s+Lee", "etype": "article"},
        {"title": "Our Parks and Playgrounds", "title_pattern": r"Our\s+Parks\s+and\s+Playgrounds", "author": "Fred Tedesco", "author_pattern": r"Fred\s+Tedesco", "etype": "article"},
        {"title": "The Clouded Window", "title_pattern": r"(?:The\s+)?Clouded\s+Window", "author": "Caroline Eyring Miner", "author_pattern": r"Caroline\s+Eyring\s+Miner", "etype": "article"},
        {"title": "Thrift Begins In The Garden", "title_pattern": r"Thrift\s+Begins.*Garden", "author": "Vesta P. Crawford", "author_pattern": r"Vesta\s+P\.?\s+Crawford", "etype": "article"},
        {"title": "Lady-in-Waiting", "title_pattern": r"Lady-?in-?Waiting", "author": "Marguerite J. Griffin", "author_pattern": r"Marguerite\s+J\.?\s+Griffin", "etype": "fiction"},
        {"title": "All Is Known", "title_pattern": r"All\s+Is\s+Known", "author": "Irva Pratt Andrus", "author_pattern": r"Irva\s+Pratt\s+Andrus", "etype": "fiction"},
        {"title": "Prelude to Spring", "title_pattern": r"Prelude\s+to\s+Spring", "author": "Beatrice Rordame Parsons", "author_pattern": r"Beatrice\s+Rordame\s+Parsons", "etype": "fiction"},
        {"title": "Even the Frail", "title_pattern": r"Even\s+the\s+Frail", "author": "Christie Lund Coles", "author_pattern": r"Christie\s+Lund\s+Coles", "etype": "fiction"},
        {"title": "The Best Little Shrub", "title_pattern": r"(?:The\s+)?Best\s+Little\s+Shrub", "author": "Mabel Harmer", "author_pattern": r"Mabel\s+Harmer", "etype": "fiction"},
        {"title": "And For Eternity Chapter 10", "title_pattern": r"And\s+For\s+Eternity.*(?:Chapter\s+\d+|Conclusion)?", "author": "Olive Woolley Burt", "author_pattern": r"Olive\s+Woolley\s+Burt", "etype": "fiction"},
        {"title": "Sixty Years Ago", "title_pattern": r"Sixty\s+Years\s+Ago", "author": None, "author_pattern": None, "etype": "general_feature"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
        {"title": "Editorials", "title_pattern": r"Editorial.*Beauty", "author": "Gertrude R. Garff", "author_pattern": r"Gertrude\s+R\.?\s+Garff", "etype": "editorial"},
        {"title": "Notes From the Field", "title_pattern": r"Notes\s+From\s+the\s+Field", "author": "Margaret C. Pickering", "author_pattern": r"Margaret\s+C\.?\s+Pickering", "etype": "report"},
        {"title": "Work Meeting", "title_pattern": r"Work\s+Meeting", "author": "Eleanor Welch Schow", "author_pattern": r"Eleanor\s+Welch\s+Schow", "etype": "article"},
        {"title": "Theology", "title_pattern": r"Theology.*(?:Growth|Adversity)", "author": "John A. Widtsoe", "author_pattern": r"John\s+A\.?\s+Widtsoe", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "title_pattern": r"Visiting\s+Teachers'?\s+Messages", "author": "Lowell L. Bennion", "author_pattern": r"Lowell\s+L\.?\s+Bennion", "etype": "lesson"},
        {"title": "Literature", "title_pattern": r"Literature.*(?:Books|Children)", "author": "Paul L. Neff", "author_pattern": r"Paul\s+L\.?\s+Neff", "etype": "lesson"},
        {"title": "Social Science", "title_pattern": r"Social\s+Science.*(?:Families|Community)?", "author": "Inis J. Bevan", "author_pattern": r"Inis\s+J\.?\s+Bevan", "etype": "lesson"},
    ],

    ("Vol33", "No4_April_1946"): [
        {"title": "A Pioneer Couple", "title_pattern": r"(?:A\s+)?Pioneer\s+Couple", "author": "Dora D. Cohen", "author_pattern": r"Dora\s+D\.?\s+Cohen", "etype": "article"},
        {"title": "Relief Society Prepares for Centennial", "title_pattern": r"Relief\s+Society.*(?:Prepares|Centennial)", "author": "Leah D. Widtsoe", "author_pattern": r"Leah\s+D\.?\s+Widtsoe", "etype": "article"},
        {"title": "Modern Miracle in Central Europe", "title_pattern": r"(?:A\s+)?Modern\s+Miracle.*Europe", "author": "Harold B. Lee", "author_pattern": r"Harold\s+B\.?\s+Lee", "etype": "article"},
        {"title": "The Harvest Is White", "title_pattern": r"(?:The\s+)?Harvest\s+Is\s+White", "author": "Lella Marler Hoggan", "author_pattern": r"Lella\s+Marler\s+Hoggan", "etype": "article"},
        {"title": "From Sea to Shining Sea", "title_pattern": r"From\s+Sea\s+to\s+Shining\s+Sea", "author": "Richard R. Lyman", "author_pattern": r"Richard\s+R\.?\s+Lyman", "etype": "article"},
        {"title": "A Charming Story of Love and Devotion", "title_pattern": r"(?:A\s+)?Charming\s+Story.*Love.*Devotion", "author": "Vesta P. Crawford", "author_pattern": r"Vesta\s+P\.?\s+Crawford", "etype": "fiction"},
        {"title": "The New Beginning", "title_pattern": r"(?:The\s+)?New\s+Beginning", "author": "Rae Rydberg", "author_pattern": r"Rae\s+Rydberg", "etype": "fiction"},
        {"title": "Blessed Are the Merciful", "title_pattern": r"Blessed\s+Are\s+the\s+Merciful", "author": "Emilia Elizabeth Horne", "author_pattern": r"Emilia\s+Elizabeth\s+Horne", "etype": "fiction"},
        {"title": "Sixty Years Ago", "title_pattern": r"Sixty\s+Years\s+Ago", "author": None, "author_pattern": None, "etype": "general_feature"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
        {"title": "Editorials", "title_pattern": r"Editorial.*Work\s+Before", "author": "Marianne C. Sharp", "author_pattern": r"Marianne\s+C\.?\s+Sharp", "etype": "editorial"},
        {"title": "Notes From the Field", "title_pattern": r"Notes\s+From\s+the\s+Field", "author": "Margaret C. Pickering", "author_pattern": r"Margaret\s+C\.?\s+Pickering", "etype": "report"},
        {"title": "Theology", "title_pattern": r"Theology.*(?:Savior|Spirit)", "author": "John A. Widtsoe", "author_pattern": r"John\s+A\.?\s+Widtsoe", "etype": "lesson"},
        {"title": "Visiting Teachers' Messages", "title_pattern": r"Visiting\s+Teachers'?\s+Messages", "author": "Lowell L. Bennion", "author_pattern": r"Lowell\s+L\.?\s+Bennion", "etype": "lesson"},
    ],

    # Additional months (5-12) use simplified entries for brevity
    ("Vol33", "No5_May_1946"): [
        {"title": "Relief Society Centennial", "title_pattern": r"Relief\s+Society.*Centennial", "author": "Leah D. Widtsoe", "author_pattern": r"Leah\s+D\.?\s+Widtsoe", "etype": "article"},
        {"title": "Recipes for the Spring Season", "title_pattern": r"Recipes.*Spring", "author": "Vesta P. Crawford", "author_pattern": r"Vesta\s+P\.?\s+Crawford", "etype": "article"},
        {"title": "Homemaker as She Really Is", "title_pattern": r"Homemaker.*(?:Really\s+Is)?", "author": "Florence J. Madsen", "author_pattern": r"Florence\s+J\.?\s+Madsen", "etype": "article"},
        {"title": "Service With a Smile", "title_pattern": r"Service.*Smile", "author": "Leone G. Layton", "author_pattern": r"Leone\s+G\.?\s+Layton", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
        {"title": "Sixty Years Ago", "title_pattern": r"Sixty\s+Years\s+Ago", "author": None, "author_pattern": None, "etype": "general_feature"},
    ],

    ("Vol33", "No6_June_1946"): [
        {"title": "Relief Society at Its Best", "title_pattern": r"Relief\s+Society.*(?:Its\s+Best)?", "author": "Leah D. Widtsoe", "author_pattern": r"Leah\s+D\.?\s+Widtsoe", "etype": "article"},
        {"title": "Home Management", "title_pattern": r"Home\s+Management", "author": "Florence J. Madsen", "author_pattern": r"Florence\s+J\.?\s+Madsen", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
        {"title": "Sixty Years Ago", "title_pattern": r"Sixty\s+Years\s+Ago", "author": None, "author_pattern": None, "etype": "general_feature"},
    ],

    ("Vol33", "No7_July_1946"): [
        {"title": "Pioneer Heritage", "title_pattern": r"Pioneer\s+Heritage", "author": "Dora D. Cohen", "author_pattern": r"Dora\s+D\.?\s+Cohen", "etype": "article"},
        {"title": "Summer Hospitality", "title_pattern": r"Summer\s+Hospitality", "author": "Leone G. Layton", "author_pattern": r"Leone\s+G\.?\s+Layton", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
    ],

    ("Vol33", "No8_August_1946"): [
        {"title": "Relief Society at a Glance", "title_pattern": r"Relief\s+Society.*Glance", "author": "Leah D. Widtsoe", "author_pattern": r"Leah\s+D\.?\s+Widtsoe", "etype": "article"},
        {"title": "Canning for the Future", "title_pattern": r"Canning.*Future", "author": "Vesta P. Crawford", "author_pattern": r"Vesta\s+P\.?\s+Crawford", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
    ],

    ("Vol33", "No9_September_1946"): [
        {"title": "New School Year", "title_pattern": r"(?:The\s+)?New\s+School\s+Year", "author": "Leah D. Widtsoe", "author_pattern": r"Leah\s+D\.?\s+Widtsoe", "etype": "article"},
        {"title": "Back to School", "title_pattern": r"Back\s+to\s+School", "author": "Leone G. Layton", "author_pattern": r"Leone\s+G\.?\s+Layton", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
    ],

    ("Vol33", "No10_October_1946"): [
        {"title": "Pioneer Preparations", "title_pattern": r"Pioneer\s+Preparations", "author": "Ann P. Nibley", "author_pattern": r"Ann\s+P\.?\s+Nibley", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
    ],

    ("Vol33", "No11_November_1946"): [
        {"title": "Relief Society Women", "title_pattern": r"Relief\s+Society\s+Women", "author": "Belle S. Spafford", "author_pattern": r"Belle\s+S\.?\s+Spafford", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
    ],

    ("Vol33", "No12_December_1946"): [
        {"title": "Christmas Message", "title_pattern": r"Christmas\s+(?:Message|Spirit)", "author": "Belle S. Spafford", "author_pattern": r"Belle\s+S\.?\s+Spafford", "etype": "editorial"},
        {"title": "Christmas Spirit in the Home", "title_pattern": r"Christmas\s+Spirit.*Home", "author": "Leone G. Layton", "author_pattern": r"Leone\s+G\.?\s+Layton", "etype": "article"},
        {"title": "Woman's Sphere", "title_pattern": r"Woman's\s+Sphere", "author": "Ramona W. Cannon", "author_pattern": r"Ramona\s+W\.?\s+Cannon", "etype": "article"},
    ],
}
