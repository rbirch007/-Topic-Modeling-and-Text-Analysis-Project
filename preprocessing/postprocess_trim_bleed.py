#!/usr/bin/env python3
"""Post-processing pass to trim boundary bleed from extracted article files.

Scans all article .txt files in processed/regex_and_llm/ and
processed/R_RS_article_extracted/ and trims content that belongs to a
subsequent article/section.  Works without TOC context by detecting
structural signals in the output files themselves.

Run with --dry-run to preview changes without writing.
"""

import argparse
import re
import sys
from pathlib import Path

# ── Patterns that signal the start of a NEW article/section ──────────────

# Known recurring section headers (OCR-tolerant)
_SECTION_HEADERS = [
    re.compile(r'Notes?\s+(?:TO|FROM|(?:F|St)rom)\s+(?:THE|(?:Th|Sh|Ch)e)\s+(?:F|St)(?:i|e)?eld', re.IGNORECASE),
    re.compile(r'(?:F|St)rom\s+Near\s+(?:and|Gnd|And)\s+(?:F|S|t)ar', re.IGNORECASE),
    re.compile(r'Sixty\s+Years\s+Ago', re.IGNORECASE),
    re.compile(r"(?:W|V)oman.s\s+(?:Sp|S)here", re.IGNORECASE),
    re.compile(r'Birthday\s+Congratulations', re.IGNORECASE),
    re.compile(r'Excerpts?\s+(?:F|St)rom\s+(?:the\s+)?Woman.s\s+Exponent', re.IGNORECASE),
]

# Lesson department headers
_LESSON_HEADERS = [
    re.compile(r'(?:V|U|\()?(?:i|s|u)?s?iting\s+(?:T|S|C)(?:e|l|a)?(?:a|e|c)?(?:ch|h)?er\s*(?:M|IT|T|m)essages?', re.IGNORECASE),
    re.compile(r'(?:Sp|S)iritual\s+(?:L|l)iving[\s\u2014\u2013\-:]', re.IGNORECASE),
    re.compile(r'(?:H|IT)omemaking[\s\u2014\u2013\-:]', re.IGNORECASE),
    re.compile(r'Social\s+(?:R|r)elations[\s\u2014\u2013\-:]', re.IGNORECASE),
    re.compile(r'Cultural\s+(?:R|r)efinement[\s\u2014\u2013\-:]', re.IGNORECASE),
    re.compile(r'LESS.{0,10}N\s+(?:DE.{0,6}\s+)?DEPARTMENT', re.IGNORECASE),
    # Early-volume lesson headers (Vols 1-29)
    re.compile(r'(?:Th|Sh|Ch)eology\s+(?:and|Gnd)\s+(?:T|S)estimony[\s\u2014\u2013\-:]', re.IGNORECASE),
    re.compile(r'Work[\s\-]*and[\s\-]*(?:B|G)usiness[\s\u2014\u2013\-:]', re.IGNORECASE),
    re.compile(r'(?:Th|Sh|Ch)e\s+Mission\s+Lessons?\b', re.IGNORECASE),
    re.compile(r'Mission\s+Lessons?\s+Latter[\s\-]day\s+Saint\s+Hymns', re.IGNORECASE),
]

# Structural break: page numbers, magazine headers, separators
_STRUCTURAL_BREAK = re.compile(
    r'(?:'
    r'Page\s+\d{1,4}'
    r'|RELIEF\s+SOCIETY\s+MAGAZINE'
    r'|\d{1,4}\s+RELIEF\s+SOCIETY\s+MAGAZINE'
    r'|[_=\-\u2014\u2013]{5,}'
    r'|\f'
    r')',
    re.IGNORECASE
)

# Mid-sentence words that indicate a match is embedded in prose
_MID_SENTENCE = re.compile(
    r'\b(?:the|a|an|and|or|but|for|with|from|into|upon|'
    r'that|this|their|these|those|his|her|its|our|your|'
    r'was|were|is|are|has|had|have|been|will|would|could|'
    r'should|shall|may|might|can|said|wrote|about|'
    r'to|of|in|on|at|by)\s*$',
    re.IGNORECASE
)

# ALL-CAPS heading: 3+ uppercase words in a row (likely a new section title)
_ALL_CAPS_HEADING = re.compile(
    r'^([A-Z][A-Z\s,\'\u2019\-]{8,})$',
    re.MULTILINE
)

# Title + "By Author" pattern (explicit byline)
# e.g. "Spring By Lulu W. Nelson" or "The Welfare Plan by Ezra C. Knowlton"
_TITLE_BY_AUTHOR = re.compile(
    r'(?:^|\n)\s*'
    r'([A-Z][a-z]+(?:[\s,\-]+(?:the|a|an|and|or|of|in|to|for|on|at|with|is|was|[A-Z][a-z]+))*)'  # Title
    r'\s+[Bb]y\s+'
    r'([A-Z][a-z]+\s+'          # first name
    r'(?:[A-Z]\.?\s+)?'         # optional middle initial
    r'[A-Z][a-z]{2,})',         # last name
)

# Title on one line, author name on a separate line
# e.g. "Farewell to Summer\nMabel Jones Gabbott"
_TITLE_NEWLINE_AUTHOR = re.compile(
    r'(?:^|\n)\s*'
    r'([A-Z][a-z]+(?:[\s,\-]+(?:the|a|an|and|or|of|in|to|for|on|at|with|is|was|[A-Z][a-z]+))*)'  # Title
    r'\s*\n\s*'                 # newline separating title from author
    r'([A-Z][a-z]+\s+'          # first name
    r'(?:[A-Z]\.?\s+)?'         # optional middle initial
    r'[A-Z][a-z]{2,})',         # last name
)

# Biographical intro pattern: full name followed by biographical info
_BIO_INTRO = re.compile(
    r'(?:^|\n\s*)'
    r'(?:MRS?\.|MISS|DR\.?|PRESIDENT|ELDER|SISTER)\s+'
    r'[A-Z][A-Z\s\.]+[A-Z]'   # name in caps
    r',\s+'                     # comma
    r'(?:one|two|three|four|five|six|seven|eight|nine|ten|'
    r'a\s+well|the\s+|who\s+|is\s+|was\s+|has\s+been)',
    re.IGNORECASE
)

# "List of..." pattern that signals a new section
_LIST_PATTERN = re.compile(
    r'(?:^|\n)\s*List\s+of\s+',
    re.IGNORECASE
)

# Ad/commercial content markers
_AD_MARKERS = re.compile(
    r'(?:BENEFICIAL\s+LIFE|L\.\s*D\.\s*S\.\s+(?:TRAINING|Business\s+College)|'
    r'NEPTUNITE|Temple\s+and\s+Burial\s+Clothes|'
    r'ZCMI\s+|DOWNSTAIRS\s+STORE|'
    r'LOWE\s+BROTHERS|Cable.Nelson|'
    r'UTAH.IDAHO\s+SUGAR|'
    r'Margaret\s+Lund\s+Tours|'
    r'TOURS\s+FOR\s+\d{4}|'
    r'HANDY\s+.{0,5}\s+DURABLE)',
    re.IGNORECASE
)


def _before_context(text, pos, chars=80):
    """Return up to `chars` characters before `pos`."""
    return text[max(0, pos - chars):pos]


def _is_prose_context(text, pos):
    """Check if position appears to be mid-prose (not a real heading)."""
    before = _before_context(text, pos)
    return bool(_MID_SENTENCE.search(before))


def _extract_article_title(filename):
    """Extract the article title from the filename for self-reference checks.

    Filenames look like '05_Symphony_of_the_Snow.txt' or
    'January_Vol22_No1_04_Article_Christ_and_the_Present_Crisis.txt'
    Returns a lowercase set of significant words from the title.
    """
    stem = Path(filename).stem
    # Strip leading number prefix (e.g. "05_")
    stem = re.sub(r'^\d+_', '', stem)
    # Strip R_RS prefixes like "January_Vol22_No1_04_Article_"
    stem = re.sub(r'^[A-Z][a-z]+_Vol\d+_No\d+_\d+_(?:Article_)?', '', stem)
    # Convert underscores to spaces
    title = stem.replace('_', ' ')
    # Return significant words (skip short stopwords)
    stopwords = {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'to', 'for',
                 'on', 'at', 'with', 'is', 'was', 'by', 'my', 'our', 'his',
                 'her', 'its', 'not'}
    words = {w.lower() for w in title.split() if len(w) > 2 and w.lower() not in stopwords}
    return words


def _matches_article_title(candidate_text, title_words):
    """Check if candidate text substantially overlaps with the article's own title."""
    if not title_words:
        return False
    candidate_words = {w.lower() for w in candidate_text.split()
                       if len(w) > 2 and w.lower() not in
                       {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'to', 'for',
                        'on', 'at', 'with', 'is', 'was', 'by'}}
    if not candidate_words:
        return False
    overlap = title_words & candidate_words
    # If more than half the candidate words match the title, it's self-reference
    return len(overlap) >= max(2, len(candidate_words) * 0.5)


def _text_continues_after(text, pos):
    """Check if the text after pos continues the same narrative flow.

    Looks for signs that content after the match is a continuation of the
    same article rather than a new section (e.g. sentence continues across
    a page break with an embedded photo caption).
    """
    after = text[pos:pos + 300].strip()
    # If the text after starts with a lowercase word, it's likely a
    # continuation of a sentence broken by a page header/caption
    if after and after[0].islower():
        return True
    # Check for sentence fragments that got split
    before = text[max(0, pos - 200):pos].rstrip()
    if before and before[-1] not in '.!?"\u201d\u2019':
        # Sentence didn't end before the match — likely a caption interruption
        return True
    return False


def find_bleed_point(text, filename):
    """Find the position where boundary bleed begins.

    Returns (trim_pos, reason) or (None, None) if no bleed detected.
    """
    if len(text) < 200:
        return None, None

    # Extract title words from filename for self-reference checks
    title_words = _extract_article_title(filename)

    # Skip the first portion (the article's own title/header region)
    skip = min(250, len(text) // 4)
    best_pos = None
    best_reason = None

    def update(pos, reason):
        nonlocal best_pos, best_reason
        if pos is not None and pos > skip and (best_pos is None or pos < best_pos):
            best_pos = pos
            best_reason = reason

    # ── Strategy 1: Known section headers appearing mid-article ──
    for pat in _SECTION_HEADERS + _LESSON_HEADERS:
        for m in pat.finditer(text, skip):
            if not _is_prose_context(text, m.start()):
                # Don't trim if the article IS about this section
                # (e.g. "Notes from the Field" article matching its own header)
                if _matches_article_title(m.group(), title_words):
                    continue
                update(m.start(), f"section header: {m.group()[:50]}")
                break

    # ── Strategy 2: ALL-CAPS heading lines (multi-word, not the article title) ──
    for m in _ALL_CAPS_HEADING.finditer(text, skip):
        heading = m.group(1).strip()
        # Must be substantial (not just "THE" or page noise)
        words = heading.split()
        if len(words) < 2 or len(heading) < 10:
            continue
        # Skip if it looks like the article's own emphasis or a quote
        if _is_prose_context(text, m.start()):
            continue
        # Skip common non-title caps (magazine headers, noise)
        if re.match(r'RELIEF\s+SOCIETY|VOL\s+\d|CONTENTS|PUBLISHED|GENERAL\s+BOARD', heading):
            continue
        # Skip if it matches the article's own title (repeated page header)
        if _matches_article_title(heading, title_words):
            continue
        # Skip photo/illustration captions — these are typically short
        # descriptive phrases and the text continues narratively after them
        if _text_continues_after(text, m.end()):
            continue
        # Must be preceded by structural break or paragraph end
        before = _before_context(text, m.start(), 120)
        has_break = bool(_STRUCTURAL_BREAK.search(before))
        has_para_end = bool(before.rstrip() and before.rstrip()[-1] in '.!?"\u201d\u2019\n')
        if has_break or has_para_end:
            update(m.start(), f"ALL-CAPS heading: {heading[:50]}")
            break

    # ── Strategy 3: Title+Author block (new poem/article starting) ──
    # Search the latter half of the text using two patterns:
    # (a) "Title By Author" with explicit byline keyword
    # (b) "Title\nAuthor" with newline separation
    late_start = max(skip, len(text) // 2)

    def _check_title_author(m, title_candidate, author_candidate):
        """Validate a title+author match. Returns True if it should be used."""
        if len(title_candidate) < 5:
            return False
        if _matches_article_title(title_candidate, title_words):
            return False
        if title_candidate.lower() in {
            'that', 'this', 'there', 'these', 'those', 'their',
            'after', 'before', 'would', 'could', 'should',
            'about', 'again', 'still', 'where', 'which', 'while',
            'some', 'many', 'most', 'much', 'every', 'other',
        }:
            return False
        if re.match(
            r'(?:Although|Because|However|Moreover|Furthermore|'
            r'Meanwhile|Nevertheless|Therefore|Whereas|While)\b',
            title_candidate
        ):
            return False
        if _is_prose_context(text, m.start()):
            return False
        before = _before_context(text, m.start(), 120)
        has_break = bool(_STRUCTURAL_BREAK.search(before))
        before_stripped = before.rstrip()
        has_sent_end = bool(before_stripped and before_stripped[-1] in '.!?"\u201d\u2019')
        if not (has_break or has_sent_end):
            return False
        after = text[m.end():m.end() + 100].strip()
        if after and after[0].islower():
            return False
        return True

    for pat in [_TITLE_BY_AUTHOR, _TITLE_NEWLINE_AUTHOR]:
        for m in pat.finditer(text, late_start):
            title_candidate = m.group(1).strip()
            author_candidate = m.group(2).strip()
            # Only accept matches where the "author" looks like a real
            # person name — reject common nouns, place names, etc.
            # Require the author last name to be 3-12 chars (typical name length)
            author_words = author_candidate.split()
            last_word = author_words[-1] if author_words else ''
            if len(last_word) < 3 or len(last_word) > 15:
                continue
            # Reject author names that are clearly not people
            if re.search(
                r'\b(?:Ward|Stake|Branch|Mission|District|'
                r'Store|Books?|House|Home|Church|Temple|'
                r'Society|Magazine|Press|Company|School|'
                r'Club|League|Council|Board|Bureau|Office|'
                r'States?|City|Valley|Mountain|Lake|River|'
                r'Street|Avenue|Building|Center|Park|'
                r'North|South|East|West|County|'
                r'January|February|March|April|May|June|'
                r'July|August|September|October|November|December|'
                r'Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|'
                r'Buying|Selling|Making|Getting|Having|Being|'
                r'Seals|Christmas|Easter|Thanksgiving|'
                r'General|Sweethearts?|Uintah|Photo|Muench)\b',
                author_candidate
            ):
                continue
            if _check_title_author(m, title_candidate, author_candidate):
                update(m.start(), f"title+author: {title_candidate} / {author_candidate}")
                break

    # ── Strategy 4: Biographical intro after article/poem end ──
    # Only match if the bio subject is NOT the article's subject
    for m in _BIO_INTRO.finditer(text, skip):
        if not _is_prose_context(text, m.start()):
            # Extract the name from the bio intro and check against title
            bio_name = re.search(r'(?:MRS?\.?|MISS|DR\.?|PRESIDENT|ELDER|SISTER)\s+([A-Z][A-Z\s\.]+[A-Z])', m.group(), re.IGNORECASE)
            if bio_name:
                name_words = {w.lower() for w in bio_name.group(1).split() if len(w) > 1}
                # Skip if any name word appears in the article title
                if name_words & title_words:
                    continue
            # Also skip if the text continues narratively (character in a story)
            if _text_continues_after(text, m.end()):
                continue
            update(m.start(), f"bio intro: {m.group()[:60]}")
            break

    # ── Strategy 5: "List of..." sections ──
    for m in _LIST_PATTERN.finditer(text, skip):
        if not _is_prose_context(text, m.start()):
            update(m.start(), f"list section: {m.group()[:40]}")
            break

    # ── Strategy 6: Ad/commercial content ──
    m = _AD_MARKERS.search(text, skip)
    if m:
        # Trim at the last sentence boundary before the ad
        before_ad = text[:m.start()].rstrip()
        last_sent = max(
            before_ad.rfind('.'),
            before_ad.rfind('!'),
            before_ad.rfind('?'),
            before_ad.rfind('\u201d'),
        )
        if last_sent > skip:
            update(last_sent + 1, f"ad content: {m.group()[:40]}")
        else:
            update(m.start(), f"ad content: {m.group()[:40]}")

    # ── Strategy 7: Detect abrupt topic shift via biographical name blocks ──
    # Pattern A: "FIRSTNAME LASTNAME is/was..." or "FIRSTNAME LASTNAME, one/a/the..."
    # at start of line
    _name_bio = re.compile(
        r'(?:^|\n\s*)'
        r'([A-Z][A-Z]+(?:\s+[A-Z]\.?)?\s+[A-Z][A-Z]+)'  # NAME IN CAPS
        r'(?:\s+is\s+|\s+was\s+|,\s+(?:one|a|the|who)\s+)',
        re.MULTILINE
    )
    # Pattern B: Same but also matches mid-line with stronger signal
    # (3-word ALL-CAPS name + comma + biographical opening)
    _name_bio_midline = re.compile(
        r'([A-Z][A-Z]+\s+[A-Z][A-Z]+(?:\s+[A-Z][A-Z]+)+)'  # 3+ word ALL-CAPS name
        r',\s+'
        r'(?:one\s+hundred|a\s+well|the\s+(?:only|first|last)|who\s+)',
    )
    for pattern in [_name_bio, _name_bio_midline]:
        for m in pattern.finditer(text, max(skip, len(text) // 3)):
            if _is_prose_context(text, m.start()):
                continue
            # Skip if the name matches the article's subject
            name_words = {w.lower() for w in m.group(1).split() if len(w) > 1}
            if name_words & title_words:
                continue
            # Skip if the "name" contains common non-person words
            non_person = {'relief', 'society', 'church', 'electric', 'cooking',
                          'temple', 'magazine', 'general', 'board', 'department',
                          'lesson', 'notes', 'field', 'national', 'american',
                          'visiting', 'teacher', 'spiritual', 'cultural'}
            if name_words & non_person:
                continue
            # Skip if text continues narratively after (fictional character).
            # For bio patterns, look further ahead since the bio opener
            # itself may have lowercase continuations ("one hundred and...")
            lookahead_start = m.end()
            # Find end of the current sentence to check what comes next
            next_sent_end = re.search(r'[.!?]\s', text[lookahead_start:lookahead_start + 300])
            if next_sent_end:
                check_pos = lookahead_start + next_sent_end.end()
            else:
                check_pos = m.end()
            after = text[check_pos:check_pos + 100].strip()
            # Only reject if the NEXT sentence starts with lowercase
            # (indicating continuous narrative rather than a separate bio blurb)
            if after and after[0].islower() and not re.match(r'[a-z]+\s+[A-Z]', after):
                continue
            before = _before_context(text, m.start(), 120)
            before_stripped = before.rstrip()
            # Only trim if preceded by structural break or sentence end
            if before_stripped and before_stripped[-1] in '.!?"\u201d\u2019':
                # Trim at the newline before the name, or at last sentence end
                nl_pos = text.rfind('\n', max(0, m.start() - 5), m.start() + 1)
                if nl_pos > skip:
                    update(nl_pos, f"name+bio block: {m.group(1)[:30]}")
                else:
                    # No newline — find last sentence boundary before the name
                    pre = text[:m.start()].rstrip()
                    last_sent = max(
                        pre.rfind('.'),
                        pre.rfind('!'),
                        pre.rfind('?'),
                        pre.rfind('\u201d'),
                    )
                    if last_sent > skip:
                        update(last_sent + 1, f"name+bio block: {m.group(1)[:30]}")
                    else:
                        update(m.start(), f"name+bio block: {m.group(1)[:30]}")
                break

    return best_pos, best_reason


def trim_to_sentence_boundary(text, pos):
    """Adjust trim position to the nearest preceding sentence boundary."""
    # Look back up to 200 chars for a clean sentence end
    search_region = text[max(0, pos - 200):pos]
    for end_char in ['.', '!', '?', '\u201d', '"']:
        idx = search_region.rfind(end_char)
        if idx != -1:
            candidate = max(0, pos - 200) + idx + 1
            # Make sure we're not trimming too much (at least 60% of bleed point)
            if candidate > pos * 0.5:
                return candidate
    # Fall back to trimming at a newline
    nl = text.rfind('\n', max(0, pos - 100), pos)
    if nl > 0:
        return nl
    return pos


def process_file(filepath, dry_run=False):
    """Process a single article file. Returns (changed, reason, chars_removed) or (False, None, 0)."""
    text = filepath.read_text(encoding='utf-8', errors='replace')

    trim_pos, reason = find_bleed_point(text, filepath.name)
    if trim_pos is None:
        return False, None, 0

    # Adjust to sentence boundary
    trim_pos = trim_to_sentence_boundary(text, trim_pos)

    trimmed = text[:trim_pos].rstrip()
    chars_removed = len(text) - len(trimmed)

    # Sanity check: don't trim more than 90% of the file
    if len(trimmed) < len(text) * 0.1:
        return False, f"SKIP (would remove {chars_removed} of {len(text)} chars): {reason}", 0

    # Sanity check: don't trim less than 20 chars (not worth it)
    if chars_removed < 20:
        return False, None, 0

    if not dry_run:
        filepath.write_text(trimmed + '\n', encoding='utf-8')

    return True, reason, chars_removed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without writing files')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show all detections including skips')
    parser.add_argument('--dirs', nargs='+',
                        default=['processed/regex_and_llm',
                                 'processed/R_RS_article_extracted'],
                        help='Directories to scan')
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent

    skip_names = {'TOC.txt', 'ADS.txt', 'MISC.txt', 'CONTENTS.txt',
                  'BOARD.txt', 'Misc.txt'}

    total_files = 0
    trimmed_files = 0
    skipped_files = 0
    total_chars_removed = 0
    changes = []

    for dir_name in args.dirs:
        scan_dir = project_root / dir_name
        if not scan_dir.exists():
            print(f"WARNING: {scan_dir} does not exist, skipping")
            continue

        for txt_file in sorted(scan_dir.rglob('*.txt')):
            if txt_file.name in skip_names:
                continue
            # Skip front-matter-only files
            if txt_file.name.startswith('00_') or '_front_matter' in txt_file.name:
                continue

            total_files += 1
            changed, reason, chars_removed = process_file(txt_file, dry_run=args.dry_run)

            if changed:
                trimmed_files += 1
                total_chars_removed += chars_removed
                rel_path = txt_file.relative_to(project_root)
                changes.append((rel_path, reason, chars_removed))
                action = "WOULD TRIM" if args.dry_run else "TRIMMED"
                print(f"  {action}: {rel_path}")
                print(f"    reason: {reason}")
                print(f"    removed: {chars_removed} chars")
            elif reason and args.verbose:
                skipped_files += 1
                rel_path = txt_file.relative_to(project_root)
                print(f"  {reason}: {rel_path}")

    print(f"\n{'=' * 60}")
    print(f"Summary:")
    print(f"  Files scanned:  {total_files}")
    print(f"  Files trimmed:  {trimmed_files}")
    if args.verbose:
        print(f"  Files skipped:  {skipped_files}")
    print(f"  Chars removed:  {total_chars_removed:,}")
    if args.dry_run:
        print(f"\n  (dry run — no files were modified)")

    return 0 if trimmed_files >= 0 else 1


if __name__ == '__main__':
    sys.exit(main())
