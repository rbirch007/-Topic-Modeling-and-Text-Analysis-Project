#!/usr/bin/env python3
"""
Vol35 Relief Society Magazine Article Extraction Script (Rewritten)

This script extracts articles from the 1948 Relief Society Magazine (Volume 35)
using the structured Table of Contents as a guide and page numbers as anchors.

EXTRACTION STRATEGY:
1. Extract and parse the Contents section to get structured TOC with section types
2. Use page numbers as primary anchors to locate articles in body text
3. Identify title vs author from "Title Author PageNumber" format using heuristics
4. Extract content between page boundaries
5. Retain section type (SPECIAL FEATURES, POETRY, etc.) in output

USAGE:
    python extract_vol35.py                 # Extract all months
    python extract_vol35.py --dry-run       # Preview without writing
    python extract_vol35.py --month April   # Extract one month only
    python extract_vol35.py --verbose       # Show detailed extraction info
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from collections import defaultdict

# Data directory paths
RAW_DATA_DIR = Path("/Users/chris/devel/rbirch/cleaned-data/relief-society/txtvolumesbymonth/Vol35")
OUTPUT_DIR = Path("/Users/chris/devel/rbirch/processed/vol35")

MONTHS = ["January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December"]


@dataclass
class TOCEntry:
    """Single table of contents entry from Contents section."""
    month: str
    section: str  # SPECIAL FEATURES, POETRY, GENERAL FEATURES, etc.
    title: str
    author: Optional[str]
    page: int
    toc_index: int


@dataclass
class ExtractedArticle:
    """Successfully extracted article with metadata."""
    month: str
    section: str
    title: str
    author: Optional[str]
    page: int
    content: str
    filename: str
    toc_index: int


class Vol35Extractor:
    """Extract articles from Vol35 using page-based anchors."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.extracted_articles: List[ExtractedArticle] = []
        self.unmatched_entries: List[Tuple[str, TOCEntry, str]] = []

    def extract_all_months(self, dry_run: bool = False) -> None:
        """Extract all 12 months of Vol35."""
        for month in MONTHS:
            self.extract_month(month, dry_run)

    def extract_month(self, month: str, dry_run: bool = False) -> None:
        """Extract a single month."""
        filepath = RAW_DATA_DIR / f"vol35_No{self._month_to_number(month):.0f}_{month}_1948.txt"

        if not filepath.exists():
            print(f"ERROR: File not found: {filepath}")
            return

        if self.verbose:
            print(f"\n{'='*80}")
            print(f"EXTRACTING: {month} 1948")
            print('='*80)

        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            raw_text = f.read()

        # Extract Contents section to get structured TOC
        toc_entries = self._parse_contents_section(raw_text, month)
        if not toc_entries:
            print(f"WARNING: No Contents section found for {month}")
            return

        if self.verbose:
            print(f"TOC Entries: {len(toc_entries)}")

        # Split raw text into front matter and body
        body_text = self._extract_body_text(raw_text)
        if not body_text:
            print(f"ERROR: Could not extract body text for {month}")
            return

        # Extract each TOC entry from body using page numbers
        for toc_entry in toc_entries:
            article = self._extract_article_by_page(month, body_text, toc_entry)
            if article:
                self.extracted_articles.append(article)
                if self.verbose:
                    print(f"  ✓ {article.title[:50]:<50} (p.{article.page})")
            else:
                self.unmatched_entries.append((month, toc_entry, "Not found in body"))
                if self.verbose:
                    print(f"  ✗ {toc_entry.title[:50]:<50} (p.{toc_entry.page})")

        # Write output if not dry-run
        if not dry_run:
            self._write_output_files(month)

        # Print summary
        self._print_summary()

    def _month_to_number(self, month: str) -> int:
        """Convert month name to 1-based month number."""
        return MONTHS.index(month) + 1

    def _parse_contents_section(self, raw_text: str, month: str) -> List[TOCEntry]:
        """Extract and parse the Contents section to build TOC."""
        # Find Contents section (usually near start, before PUBLISHED MONTHLY)
        contents_start = raw_text.find("Contents")
        if contents_start == -1:
            contents_start = raw_text.find("CONTENTS")
        if contents_start == -1:
            return []

        # Find end of Contents (PUBLISHED MONTHLY marker)
        body_marker = "PUBLISHED MONTHLY BY THE GENERAL BOARD OF RELIEF SOCIETY"
        contents_end = raw_text.find(body_marker, contents_start)
        if contents_end == -1:
            contents_end = len(raw_text)

        contents_text = raw_text[contents_start:contents_end]

        # Parse the Contents section
        toc_entries = []
        current_section = None
        toc_index = 0

        for line in contents_text.split('\n'):
            line = line.strip()
            if not line:
                continue

            # Check if this is a section header (all caps, known section names)
            if self._is_section_header(line):
                current_section = line
                continue

            # Parse TOC entry line: "Title Author PageNumber"
            entry = self._parse_toc_line(line, month, current_section, toc_index)
            if entry:
                toc_entries.append(entry)
                toc_index += 1

        return toc_entries

    def _is_section_header(self, line: str) -> bool:
        """Check if line is a section header."""
        headers = [
            "SPECIAL FEATURES",
            "GENERAL FEATURES",
            "FEATURES FOR THE HOME",
            "POETRY",
            "FICTION",
            "SERIALS",
            "SPECIAL SHORT STORIES",
            "SHORT STORIES",
            "LESSON DEPARTMENT",
            "LESSONS AND PREVIEWS",
            "LESSONS FOR",
            "VISITING TEACHERS",
            "WORK MEETING",
        ]
        return any(header in line.upper() for header in headers)

    def _parse_toc_line(self, line: str, month: str, section: Optional[str],
                       index: int) -> Optional[TOCEntry]:
        """Parse a single TOC line: 'Title Author PageNumber'."""
        if not line or not section:
            return None

        # Extract page number (always last token, numeric)
        tokens = line.split()
        if not tokens or not tokens[-1].isdigit():
            return None

        page = int(tokens[-1])
        line_without_page = ' '.join(tokens[:-1]).strip()

        if not line_without_page:
            return None

        # Identify title vs author
        title, author = self._split_title_and_author(line_without_page)

        if not title:
            return None

        return TOCEntry(
            month=month,
            section=section,
            title=title,
            author=author,
            page=page,
            toc_index=index
        )

    def _split_title_and_author(self, text: str) -> Tuple[str, Optional[str]]:
        """Split 'Title Author' into components using heuristics."""
        # Remove type descriptors that appear between title and author
        # Pattern: "Title [Type Descriptor] Author"
        type_descriptors = [
            " First Prize Poem",
            " Second Prize Poem",
            " Third Prize Poem",
            " Frontispiece",
            " (Conclusion)",
            " Conclusion",
        ]

        for descriptor in type_descriptors:
            if descriptor.lower() in text.lower():
                # Extract text before and after descriptor
                idx = text.lower().find(descriptor.lower())
                before = text[:idx].strip()
                after = text[idx + len(descriptor):].strip()

                # Check if 'after' looks like an author name
                if after and len(after.split()) <= 4:  # Author names are typically <= 4 words
                    # This is likely: Title [Descriptor] Author
                    text = before + " " + after

        # Check for role prefix indicators first (most reliable)
        role_prefixes = [
            "Elder ", "President ", "Sister ", "Bishop ", "Counselor ",
            "General Presidency of ", "General Secretary-Treasurer, ", "Member, Relief Society",
            "First Counselor ", "Second Counselor ", "Secretary-Treasurer ",
            "General Manager ", "Editor ", "Associate Editor ",
            "Called to ", "Issued by ",
            "General Board ", "Relief Society ",
        ]

        for prefix in role_prefixes:
            if prefix.lower() in text.lower():
                # Find where the prefix starts (case-insensitive)
                idx = text.lower().find(prefix.lower())
                if idx > 0:
                    title = text[:idx].strip()
                    author = text[idx:].strip()
                    return (title, author) if title else (author, None)

        # Check if text ends with a name pattern (author at end)
        # Try longer patterns first (3 words, then 2, then 1)
        tokens = text.split()

        # Try 3-word author patterns first
        if len(tokens) >= 4:  # Need at least 1 word for title + 3 for author
            potential_author = ' '.join(tokens[-3:])
            if self._looks_like_author_name(potential_author, 3, len(tokens)):
                title = ' '.join(tokens[:-3]).strip()
                return (title, potential_author) if title else (potential_author, None)

        # Try 2-word author patterns
        if len(tokens) >= 3:  # Need at least 1 word for title + 2 for author
            potential_author = ' '.join(tokens[-2:])
            if self._looks_like_author_name(potential_author, 2, len(tokens)):
                title = ' '.join(tokens[:-2]).strip()
                return (title, potential_author) if title else (potential_author, None)

        # Try 1-word author patterns (least likely)
        if len(tokens) >= 2:  # Need at least 1 word for title + 1 for author
            potential_author = tokens[-1]
            if self._looks_like_author_name(potential_author, 1, len(tokens)):
                title = ' '.join(tokens[:-1]).strip()
                return (title, potential_author) if title else (potential_author, None)

        # No author found, entire text is title
        return (text, None)

    def _looks_like_author_name(self, text: str, word_count: int, total_tokens: int) -> bool:
        """Heuristic: check if text looks like an author name.

        Args:
            text: Potential author name text
            word_count: Expected number of words (1, 2, or 3)
            total_tokens: Total words in original line
        """
        if not text or len(text) > 80:
            return False

        actual_word_count = len(text.split())
        if actual_word_count != word_count:
            return False

        # Author names are typically 1-3 words
        if word_count > 3:
            return False

        # Common non-author words that appear at end of titles
        common_non_names = [
            "The", "A", "Of", "And", "Or", "In", "For", "On", "At",
            "By", "From", "With", "About", "Into", "Through", "During",
            "News", "Ago", "Day", "Time", "Story", "Ways", "Place", "Things",
            "Work", "More", "Love", "Change", "Gift", "House", "Home",
            "New", "Old", "Young", "Good", "True", "Life", "World",
            "Mother", "Father", "Girl", "Boy", "Woman", "Man", "Friend",
            "Coat", "Pie", "Picture", "Letter", "Message", "Call", "Answer"
        ]

        words = text.split()

        # Single word - very restrictive for single words
        if word_count == 1:
            word = words[0]
            if word in common_non_names or not word[0].isupper():
                return False
            # Must have at least 4 chars to be a surname
            if len(word) < 4:
                return False
            return True

        # Two words - check for "FirstName LastName" pattern
        if word_count == 2:
            w1, w2 = words
            # Both should start with capital
            if not (w1[0].isupper() and w2[0].isupper()):
                return False
            # Second word shouldn't be a common non-name
            if w2 in common_non_names:
                return False
            # Should have reasonable lengths
            if len(w1) < 2 or len(w2) < 3:
                return False
            return True

        # Three words - check for "FirstName Middle LastName" or "FirstName M. LastName"
        if word_count == 3:
            # Common pattern: "FirstName MiddleInit LastName" e.g., "Belle Watson Anderson"
            w1, w2, w3 = words

            # All should start with capital
            if not all(w[0].isupper() for w in words):
                return False

            # First or last word shouldn't be a common non-name
            if w1 in common_non_names or w3 in common_non_names:
                return False

            # Check for "X. Y Z" pattern (initial in middle)
            if len(w2) <= 2 or (len(w2) == 2 and w2[1] == '.'):
                # "FirstName I. LastName" - likely a name
                return True

            # All three are normal words - check they look like names
            # But be more restrictive: first word should not be a title word
            if len(w1) >= 3 and len(w2) >= 2 and len(w3) >= 3:
                # Exclude if first word matches common title patterns
                if w1.lower() not in ["the", "a", "for", "and", "or"]:
                    return True

        return False

    def _extract_body_text(self, raw_text: str) -> str:
        """Extract body text (everything after PUBLISHED MONTHLY marker)."""
        marker = "PUBLISHED MONTHLY BY THE GENERAL BOARD OF RELIEF SOCIETY"
        idx = raw_text.find(marker)
        if idx == -1:
            return ""

        # Start body after the marker
        return raw_text[idx + len(marker):]

    def _extract_article_by_page(self, month: str, body_text: str,
                                 toc_entry: TOCEntry) -> Optional[ExtractedArticle]:
        """Extract article content by searching for title in body text."""

        body_lower = body_text.lower()
        title_pos = -1
        title_search = toc_entry.title.lower()

        # Remove common prefixes that might appear differently in body
        common_prefixes = [
            "editorial: ",
            "notes to the field: ",
            "notes from the field: ",
            "theology: ",
            "literature: ",
            "social science: ",
            "visiting teachers ",
            "work meeting ",
        ]

        title_for_search = title_search
        for prefix in common_prefixes:
            if title_for_search.startswith(prefix):
                title_for_search = title_for_search[len(prefix):].strip()
                break

        # Try multiple search strategies
        # 1. Exact title match
        title_pos = body_lower.find(title_search)

        # 2. Title without prefix
        if title_pos == -1 and title_for_search != title_search:
            title_pos = body_lower.find(title_for_search)

        # 3. Title without quotes
        if title_pos == -1:
            title_no_quotes = title_for_search.replace('"', '').replace("'", '')
            if title_no_quotes != title_for_search:
                title_pos = body_lower.find(title_no_quotes)

        # 4. Title with simplified punctuation
        if title_pos == -1:
            title_simple = title_for_search.replace(';', ':').replace('"', '')
            if title_simple != title_for_search:
                title_pos = body_lower.find(title_simple)

        # 5. First words of title (if title is long)
        if title_pos == -1 and len(title_for_search.split()) > 3:
            first_words = ' '.join(title_for_search.split()[:4])
            title_pos = body_lower.find(first_words)

        if title_pos == -1:
            return None

        # Start extraction from the title
        start_pos = title_pos

        # Find the end of this article:
        # Look for section headers, next article, or end of text
        end_pos = len(body_text)

        # Try to find section headers or other article boundaries
        section_pattern = r"\n(?:POETRY|FICTION|GENERAL FEATURES|SPECIAL FEATURES|LESSONS|SERIALS|STORIES|VISITING TEACHERS|WORK MEETING)\s*\n"
        section_match = re.search(section_pattern, body_text[start_pos + len(toc_entry.title):],
                                 re.MULTILINE | re.IGNORECASE)
        if section_match:
            end_pos = start_pos + len(toc_entry.title) + section_match.start()

        # Extract content from title to boundary
        content = body_text[start_pos:end_pos].strip()

        # Remove the title itself from the beginning (keep just the content)
        # Try to remove title and author
        content = re.sub(rf"^{re.escape(toc_entry.title)}\s*\n", "", content, flags=re.IGNORECASE)
        if toc_entry.author:
            content = re.sub(rf"^{re.escape(toc_entry.author)}\s*\n", "", content, flags=re.IGNORECASE)
        content = content.strip()

        # Validate content
        if len(content) < 50:  # Minimum content length
            return None

        return ExtractedArticle(
            month=month,
            section=toc_entry.section,
            title=toc_entry.title,
            author=toc_entry.author,
            page=toc_entry.page,
            content=content,
            filename=self._generate_filename(toc_entry),
            toc_index=toc_entry.toc_index
        )

    def _generate_filename(self, entry: TOCEntry) -> str:
        """Generate output filename for article."""
        safe_title = re.sub(r'[^a-zA-Z0-9\-_]', '_', entry.title)[:50]
        return f"{entry.month}_{entry.page:03d}_{safe_title}.txt"

    def _write_output_files(self, month: str) -> None:
        """Write extracted articles to files."""
        month_dir = OUTPUT_DIR / month
        month_dir.mkdir(parents=True, exist_ok=True)

        # Write individual files
        for article in self.extracted_articles:
            if article.month != month:
                continue

            filepath = month_dir / article.filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"Title: {article.title}\n")
                f.write(f"Author: {article.author or '(no author)'}\n")
                f.write(f"Page: {article.page}\n")
                f.write(f"Section: {article.section}\n")
                f.write(f"\n{article.content}\n")

        # Write JSON manifest
        month_articles = [a for a in self.extracted_articles if a.month == month]
        manifest = {
            'month': month,
            'year': 1948,
            'total_extracted': len(month_articles),
            'articles': [
                {
                    'title': a.title,
                    'author': a.author,
                    'page': a.page,
                    'section': a.section,
                    'filename': a.filename,
                }
                for a in month_articles
            ]
        }

        manifest_path = month_dir / "manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)

        if self.verbose:
            print(f"\nWrote {len(month_articles)} articles to {month_dir}")

    def _print_summary(self) -> None:
        """Print extraction summary."""
        total_toc = len(self.extracted_articles) + len(self.unmatched_entries)
        extracted_count = len(self.extracted_articles)

        if total_toc == 0:
            return

        match_rate = (extracted_count / total_toc * 100) if total_toc > 0 else 0

        print("\n" + "="*80)
        print("EXTRACTION SUMMARY")
        print("="*80)
        print(f"Total TOC entries:     {total_toc}")
        print(f"Successfully matched:  {extracted_count}")
        print(f"Unmatched entries:     {len(self.unmatched_entries)}")
        print(f"Match rate:            {match_rate:.1f}%")

        if self.unmatched_entries:
            print("\nUnmatched entries (sample):")
            for month, entry, reason in self.unmatched_entries[:10]:
                print(f"  - {month}: {entry.title} (p.{entry.page}) - {reason}")


def main():
    parser = argparse.ArgumentParser(description="Extract articles from Vol35 Relief Society Magazine")
    parser.add_argument('--month', help='Extract single month only', choices=MONTHS)
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing files')
    parser.add_argument('--verbose', action='store_true', help='Show detailed extraction info')

    args = parser.parse_args()

    extractor = Vol35Extractor(verbose=args.verbose)

    if args.month:
        extractor.extract_month(args.month, dry_run=args.dry_run)
    else:
        extractor.extract_all_months(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
