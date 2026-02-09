#!/usr/bin/env python3
"""
OCR Cleanup Pipeline for Relief Society Magazine text files.

Sends OCR'd text through GPT-4o to fix character substitution errors
(e.g. ¥, é, ©, ®, £, garbled sequences) while preserving original
formatting and meaning.

Usage:
    python ocr_cleanup.py                  # process all 684 files
    python ocr_cleanup.py --limit 5        # process only 5 files (for testing)
    python ocr_cleanup.py --volume vol45   # process only one volume folder
    python ocr_cleanup.py --dry-run        # show what would be processed, no API calls
"""

import argparse
import collections
import json
import os
import re
import sys
import time
from pathlib import Path

try:
    from openai import OpenAI, RateLimitError, APIError
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

# ---------------------------------------------------------------------------
# Paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # up from ocr-cleanup → relief-society-mag → preprocessing → root
RAW_DIR = PROJECT_ROOT / "raw-data" / "relief-society" / "txtvolumesbymonth"
CLEAN_DIR = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"
PROGRESS_FILE = Path(__file__).resolve().parent / "progress.json"

# ---------------------------------------------------------------------------
# LLM settings
# ---------------------------------------------------------------------------
MODEL = "gpt-4o"
CHUNK_TARGET_WORDS = 600  # approximate words per chunk (kept under TPM_LIMIT with margin)
MAX_CHUNK_WORDS = 800  # hard limit - split paragraphs if needed
MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # seconds
TPM_LIMIT = 30_000  # tokens per minute budget
MAX_TOKENS_PER_REQUEST = 12_000  # max tokens for a single request (input + output)

SYSTEM_PROMPT = """\
You are an OCR post-processing assistant. You will receive a passage of text \
that was produced by OCR (optical character recognition) from a scanned \
magazine page. The text contains character substitution errors introduced by \
the OCR process — for example, ¥ instead of Y, é instead of e, © instead of \
C or O, ® instead of R, £ instead of L, « » instead of quotation marks, \
garbled sequences of special characters, etc.

Your task:
1. Fix OCR character substitution errors so the text reads as the original \
   English text would have.
2. Preserve the original line breaks, paragraph breaks, and formatting exactly.
3. Do NOT rewrite, summarize, paraphrase, or alter the meaning of the text.
4. Do NOT add any commentary, headers, or explanation — return ONLY the \
   corrected text.
5. If a passage appears to be a garbled table of contents with dot leaders \
   and page numbers that are unrecoverable, do your best to reconstruct \
   readable entries but do not invent content.
"""


def load_progress() -> dict:
    """Load the progress tracking file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed": [], "failed": {}}


def save_progress(progress: dict):
    """Save progress tracking file."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def has_garbled_line0(first_line: str) -> bool:
    """
    Detect if the first line is garbled OCR junk (high density of special chars).

    Heuristic: if the first line has a high ratio of non-ASCII or known OCR-junk
    characters relative to its length, it's likely garbled metadata.
    """
    if not first_line.strip():
        return False

    junk_chars = set("«»¥£©®™°±²³µ¶·¹º¼½¾¿×÷")
    # Count characters that are non-ASCII or in the junk set
    special_count = sum(1 for c in first_line if ord(c) > 127 or c in junk_chars)

    # Also check for patterns like random short words with special chars interspersed
    # e.g. "he My « ' 4 \"ae NUARY' 1963 e weit » wea q i."
    if len(first_line) < 20:
        return False

    ratio = special_count / len(first_line)

    # A line with >5% special characters in the first line is suspicious,
    # but we also check for the fragmented-words pattern
    if ratio > 0.05:
        return True

    # Check for the fragmented single-letter words pattern
    # (many 1-2 char "words" interspersed with special chars)
    words = first_line.split()
    if len(words) > 10:
        short_words = sum(1 for w in words[:20] if len(w) <= 2)
        if short_words / min(len(words), 20) > 0.4:
            return True

    return False


def strip_garbled_line0(text: str) -> tuple[str, bool]:
    """
    Remove garbled first line if detected.
    Returns (cleaned_text, was_stripped).
    """
    lines = text.split("\n")
    if lines and has_garbled_line0(lines[0]):
        # Remove the garbled line and any immediately following blank lines
        idx = 1
        while idx < len(lines) and not lines[idx].strip():
            idx += 1
        return "\n".join(lines[idx:]), True
    return text, False


def chunk_text(text: str, target_words: int = CHUNK_TARGET_WORDS) -> list[str]:
    """
    Split text into chunks at paragraph boundaries (~target_words each).

    Splits on double-newlines (paragraph breaks). If a single paragraph
    exceeds MAX_CHUNK_WORDS, splits it further at sentence boundaries or
    by word count to ensure no chunk is too large.
    """
    paragraphs = re.split(r"(\n\s*\n)", text)

    chunks = []
    current_chunk = ""
    current_words = 0

    for part in paragraphs:
        part_words = len(part.split())

        # If this paragraph alone exceeds the max, split it
        if part_words > MAX_CHUNK_WORDS:
            # Save current chunk if any
            if current_chunk.strip():
                chunks.append(current_chunk)
                current_chunk = ""
                current_words = 0

            # Split the large paragraph into smaller pieces
            sub_chunks = split_large_paragraph(part, MAX_CHUNK_WORDS)
            chunks.extend(sub_chunks)
        elif current_words + part_words > target_words and current_chunk.strip():
            chunks.append(current_chunk)
            current_chunk = part
            current_words = part_words
        else:
            current_chunk += part
            current_words += part_words

    if current_chunk.strip():
        chunks.append(current_chunk)

    return chunks if chunks else [text]


def split_large_paragraph(paragraph: str, max_words: int) -> list[str]:
    """
    Split a large paragraph into smaller chunks at sentence boundaries.

    If sentences themselves are too large, split by word count.
    """
    # Try to split by sentences first
    sentences = re.split(r'([.!?]+\s+)', paragraph)

    chunks = []
    current_chunk = ""
    current_words = 0

    for i in range(0, len(sentences), 2):
        sentence = sentences[i]
        punctuation = sentences[i + 1] if i + 1 < len(sentences) else ""
        full_sentence = sentence + punctuation
        sentence_words = len(full_sentence.split())

        # If a single sentence exceeds max_words, split it by word count
        if sentence_words > max_words:
            if current_chunk.strip():
                chunks.append(current_chunk)
                current_chunk = ""
                current_words = 0

            # Split by word count
            words = full_sentence.split()
            for j in range(0, len(words), max_words):
                chunk_words = words[j:j + max_words]
                chunks.append(" ".join(chunk_words))
        elif current_words + sentence_words > max_words and current_chunk.strip():
            chunks.append(current_chunk)
            current_chunk = full_sentence
            current_words = sentence_words
        else:
            current_chunk += full_sentence
            current_words += sentence_words

    if current_chunk.strip():
        chunks.append(current_chunk)

    return chunks if chunks else [paragraph]


class TokenRateLimiter:
    """Sliding-window rate limiter that enforces a tokens-per-minute budget."""

    def __init__(self, tpm_limit: int = TPM_LIMIT):
        self.tpm_limit = tpm_limit
        self._window: collections.deque[tuple[float, int]] = collections.deque()

    def _expire(self):
        """Remove entries older than 60 seconds."""
        cutoff = time.monotonic() - 60
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()

    def _tokens_used(self) -> int:
        self._expire()
        return sum(t for _, t in self._window)

    def wait_if_needed(self, estimated_tokens: int):
        """Block until there is room in the budget for *estimated_tokens*."""
        # First check if this single request would exceed the per-request limit
        if estimated_tokens > MAX_TOKENS_PER_REQUEST:
            raise ValueError(
                f"Single request ({estimated_tokens} tokens) exceeds max tokens per request "
                f"({MAX_TOKENS_PER_REQUEST})"
            )

        while True:
            self._expire()
            used = self._tokens_used()
            if used + estimated_tokens <= self.tpm_limit:
                return
            # If the window is empty, this single request exceeds the per-minute budget
            # Wait for the window to reset
            if not self._window:
                print(f"    Warning: single request (~{estimated_tokens} tokens) exceeds TPM budget of {self.tpm_limit}")
                print(f"    Waiting 60s for rate limit window to reset...")
                time.sleep(60)
                continue
            # Wait until the oldest entry expires out of the window
            oldest_ts = self._window[0][0]
            wait = (oldest_ts + 60) - time.monotonic() + 0.1
            if wait > 0:
                print(f"    Throttling: {used} tokens used in last 60s, waiting {wait:.1f}s...")
                time.sleep(wait)

    def record(self, total_tokens: int):
        """Record actual token usage returned by the API."""
        self._window.append((time.monotonic(), total_tokens))


def estimate_tokens(text: str) -> int:
    """
    Rough token estimate: ~1.3 tokens per word for English text.

    This is conservative to ensure we don't underestimate.
    """
    words = len(text.split())
    return int(words * 1.5)  # Conservative estimate with buffer


def call_gpt(client: OpenAI, text_chunk: str, limiter: TokenRateLimiter) -> str:
    """
    Send a text chunk to GPT-4o for OCR cleanup with proactive rate
    limiting and exponential backoff.
    """
    # Estimate tokens for this request (input + expected output)
    input_tokens = estimate_tokens(SYSTEM_PROMPT) + estimate_tokens(text_chunk)
    # Assume output will be similar length to input
    estimated_total = input_tokens * 2

    # Safety check: reject requests that would exceed the per-request limit
    if estimated_total > MAX_TOKENS_PER_REQUEST:
        raise ValueError(
            f"Chunk too large: estimated {estimated_total} tokens, "
            f"max is {MAX_TOKENS_PER_REQUEST}. Chunk has {len(text_chunk.split())} words."
        )

    # Wait until the TPM budget has room for this request
    limiter.wait_if_needed(estimated_total)

    backoff = INITIAL_BACKOFF

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": text_chunk},
                ],
                temperature=0.0,
            )
            # Record actual token usage
            if response.usage:
                limiter.record(response.usage.total_tokens)
            return response.choices[0].message.content
        except RateLimitError as e:
            # Check if this is a "request too large" error
            error_str = str(e)
            if "Request too large" in error_str or "Requested" in error_str:
                # This is a per-request size limit, not a rate limit - don't retry
                raise ValueError(
                    f"Request exceeds OpenAI size limit. Chunk has {len(text_chunk.split())} words. "
                    f"Error: {error_str}"
                ) from e
            # Regular rate limit - retry with backoff
            if attempt < MAX_RETRIES - 1:
                print(f"    Rate limited, waiting {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise
        except APIError as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    API error ({e}), retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise


def process_file(client: OpenAI, limiter: TokenRateLimiter, raw_path: Path, clean_path: Path, file_num: int, total: int) -> bool:
    """
    Process a single file: strip garbled line 0, chunk, send to GPT-4o, reassemble.
    Returns True on success, False on failure.
    """
    rel = raw_path.relative_to(RAW_DIR)
    print(f"[{file_num}/{total}] Processing {rel}")

    text = raw_path.read_text(encoding="utf-8", errors="replace")

    # Step 1: Strip garbled line 0
    text, was_stripped = strip_garbled_line0(text)
    if was_stripped:
        print(f"    Stripped garbled line 0")

    # Step 2: Chunk the text
    chunks = chunk_text(text)
    print(f"    {len(chunks)} chunk(s), ~{len(text.split())} words total")

    # Step 3: Process each chunk through GPT-4o
    cleaned_chunks = []
    for i, chunk in enumerate(chunks):
        if len(chunks) > 1:
            print(f"    Chunk {i+1}/{len(chunks)}...")
        cleaned = call_gpt(client, chunk, limiter)
        cleaned_chunks.append(cleaned)

    # Step 4: Reassemble and write
    cleaned_text = "\n\n".join(cleaned_chunks)

    clean_path.parent.mkdir(parents=True, exist_ok=True)
    clean_path.write_text(cleaned_text, encoding="utf-8")

    print(f"    Done → {clean_path.relative_to(PROJECT_ROOT)}")
    return True


def gather_files(volume_filter: str | None = None) -> list[tuple[Path, Path]]:
    """
    Gather all (raw_path, clean_path) pairs, optionally filtered by volume.
    """
    pairs = []

    for vol_dir in sorted(RAW_DIR.iterdir()):
        if not vol_dir.is_dir():
            continue
        if volume_filter and vol_dir.name != volume_filter:
            continue

        for txt_file in sorted(vol_dir.glob("*.txt")):
            clean_path = CLEAN_DIR / vol_dir.name / txt_file.name
            pairs.append((txt_file, clean_path))

    return pairs


def main():
    parser = argparse.ArgumentParser(description="OCR cleanup pipeline for Relief Society Magazine")
    parser.add_argument("--limit", type=int, default=None, help="Process only N files (for testing)")
    parser.add_argument("--volume", type=str, default=None, help="Process only a specific volume folder (e.g. vol45)")
    parser.add_argument("--dry-run", action="store_true", help="Show files that would be processed without making API calls")
    parser.add_argument("--reset-progress", action="store_true", help="Clear the progress file and start fresh")
    args = parser.parse_args()

    # Check dependencies for non-dry-run mode
    if not args.dry_run:
        if not HAS_OPENAI:
            print("Error: openai package not installed. Run: pip install openai", file=sys.stderr)
            sys.exit(1)
        if not os.environ.get("OPENAI_API_KEY"):
            print("Error: OPENAI_API_KEY environment variable not set.", file=sys.stderr)
            sys.exit(1)

    # Reset progress if requested
    if args.reset_progress and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        print("Progress file cleared.")

    # Load progress
    progress = load_progress()
    completed_set = set(progress["completed"])

    # Gather files
    all_pairs = gather_files(volume_filter=args.volume)

    if not all_pairs:
        print(f"No files found in {RAW_DIR}")
        if args.volume:
            print(f"  (filtered to volume: {args.volume})")
        sys.exit(1)

    # Filter out already-completed files
    pairs = [(r, c) for r, c in all_pairs if str(r.relative_to(RAW_DIR)) not in completed_set]

    if args.limit:
        pairs = pairs[:args.limit]

    already_done = len([p for p in all_pairs if str(p[0].relative_to(RAW_DIR)) in completed_set])

    print(f"Total files found: {len(all_pairs)}")
    print(f"Already completed: {already_done}")
    print(f"To process: {len(pairs)}")
    print(f"Failed previously: {len(progress['failed'])}")
    print()

    if args.dry_run:
        for raw_path, clean_path in pairs:
            print(f"  {raw_path.relative_to(RAW_DIR)} → {clean_path.relative_to(PROJECT_ROOT)}")
        return

    # Initialize OpenAI client and rate limiter
    client = OpenAI()
    limiter = TokenRateLimiter(TPM_LIMIT)

    succeeded = 0
    failed = 0

    for i, (raw_path, clean_path) in enumerate(pairs, 1):
        rel_key = str(raw_path.relative_to(RAW_DIR))
        try:
            ok = process_file(client, limiter, raw_path, clean_path, i, len(pairs))
            if ok:
                progress["completed"].append(rel_key)
                # Remove from failed if it was there
                progress["failed"].pop(rel_key, None)
                succeeded += 1
        except Exception as e:
            print(f"    FAILED: {e}", file=sys.stderr)
            progress["failed"][rel_key] = str(e)
            failed += 1

        # Save progress after each file
        save_progress(progress)

    print()
    print(f"Complete. Succeeded: {succeeded}, Failed: {failed}")
    print(f"Total completed (all runs): {len(progress['completed'])}")
    if progress["failed"]:
        print(f"Total failed: {len(progress['failed'])}")
        for f, err in progress["failed"].items():
            print(f"  {f}: {err}")


if __name__ == "__main__":
    main()
