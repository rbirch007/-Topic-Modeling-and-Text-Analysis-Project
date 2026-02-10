#!/usr/bin/env python3
"""
OCR Cleanup Pipeline for Relief Society Magazine text files - LOCAL LLM VERSION.

Uses Ollama (or LiteLLM) to run OCR cleanup with a local model instead of OpenAI.
Designed for machines with 8GB GPU (NVIDIA 4060 or similar).

Recommended models for 8GB GPU:
  - llama3.1:8b (best balance of quality and speed)
  - qwen2.5:7b (excellent instruction following)
  - mistral:7b-v0.3 (fast and efficient)
  - phi3:14b-medium-4k-instruct-q4_0 (14B quantized to fit 8GB)

Setup:
  1. Install Ollama: https://ollama.ai/
  2. Pull a model: ollama pull llama3.1:8b
  3. Install Python package: pip install ollama

  Alternative - Use LiteLLM for unified API:
    pip install litellm
    # Works with Ollama, vLLM, LocalAI, and more

Usage:
    python ocr_cleanup_local.py                  # process all 684 files
    python ocr_cleanup_local.py --limit 5        # process only 5 files (for testing)
    python ocr_cleanup_local.py --volume Vol45   # process only one volume folder
    python ocr_cleanup_local.py --dry-run        # show what would be processed, no API calls
    python ocr_cleanup_local.py --model qwen2.5:7b  # use a different model
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

try:
    import ollama
    HAS_OLLAMA = True
except ImportError:
    HAS_OLLAMA = False

try:
    import litellm
    HAS_LITELLM = True
except ImportError:
    HAS_LITELLM = False

# ---------------------------------------------------------------------------
# Paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "raw-data" / "relief-society" / "txtvolumesbymonth"
CLEAN_DIR = PROJECT_ROOT / "cleaned-data" / "relief-society" / "txtvolumesbymonth"
PROGRESS_FILE = Path(__file__).resolve().parent / "progress.json"  # Shared with OpenAI version

# ---------------------------------------------------------------------------
# LLM settings
# ---------------------------------------------------------------------------
DEFAULT_MODEL = "qwen-ocr"  # Custom model created from Modelfile (see setup_local_model.sh)
USE_LITELLM = False  # Set to True to use LiteLLM instead of Ollama directly

# Chunking settings - optimized for Qwen 2.5 7B Q5 on 8GB GPU
# Larger chunks = better context for OCR correction = higher quality
CHUNK_TARGET_WORDS = 800  # Target ~1200 tokens per chunk
MAX_CHUNK_WORDS = 1200  # Hard limit ~1800 tokens
MAX_RETRIES = 3
INITIAL_BACKOFF = 1  # seconds

# Local models don't have rate limits, but we can add a delay to prevent overheating
DELAY_BETWEEN_REQUESTS = 0.0  # seconds (increase if GPU gets too hot)

# Progress verbosity
SHOW_CHUNK_DETAILS = True  # Show word counts and timing per chunk
SHOW_TOKEN_ESTIMATES = True  # Show estimated token counts

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

Return ONLY the corrected text with no additional commentary.
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
    """
    if not first_line.strip():
        return False

    junk_chars = set("«»¥£©®™°±²³µ¶·¹º¼½¾¿×÷")
    special_count = sum(1 for c in first_line if ord(c) > 127 or c in junk_chars)

    if len(first_line) < 20:
        return False

    ratio = special_count / len(first_line)

    if ratio > 0.05:
        return True

    # Check for fragmented single-letter words pattern
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
        idx = 1
        while idx < len(lines) and not lines[idx].strip():
            idx += 1
        return "\n".join(lines[idx:]), True
    return text, False


def chunk_text(text: str, target_words: int = CHUNK_TARGET_WORDS) -> list[str]:
    """
    Split text into chunks at paragraph boundaries (~target_words each).
    """
    paragraphs = re.split(r"(\n\s*\n)", text)

    chunks = []
    current_chunk = ""
    current_words = 0

    for part in paragraphs:
        part_words = len(part.split())

        if part_words > MAX_CHUNK_WORDS:
            if current_chunk.strip():
                chunks.append(current_chunk)
                current_chunk = ""
                current_words = 0

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
    """
    sentences = re.split(r'([.!?]+\s+)', paragraph)

    chunks = []
    current_chunk = ""
    current_words = 0

    for i in range(0, len(sentences), 2):
        sentence = sentences[i]
        punctuation = sentences[i + 1] if i + 1 < len(sentences) else ""
        full_sentence = sentence + punctuation
        sentence_words = len(full_sentence.split())

        if sentence_words > max_words:
            if current_chunk.strip():
                chunks.append(current_chunk)
                current_chunk = ""
                current_words = 0

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


def estimate_tokens(text: str) -> int:
    """Estimate token count: ~1.5 tokens per word for English text."""
    return int(len(text.split()) * 1.5)


def call_ollama_direct(model: str, text_chunk: str) -> tuple[str, dict]:
    """
    Call Ollama directly using the ollama Python package.
    Returns (response_text, stats_dict).
    """
    if DELAY_BETWEEN_REQUESTS > 0:
        time.sleep(DELAY_BETWEEN_REQUESTS)

    backoff = INITIAL_BACKOFF
    start_time = time.time()

    for attempt in range(MAX_RETRIES):
        try:
            response = ollama.chat(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": text_chunk},
                ],
                # Options configured server-side in Modelfile (temperature=0.0, num_ctx=8192, etc.)
            )
            elapsed = time.time() - start_time

            # Extract stats from response
            stats = {
                "duration_sec": elapsed,
                "prompt_tokens": response.get("prompt_eval_count", 0),
                "completion_tokens": response.get("eval_count", 0),
                "total_tokens": response.get("prompt_eval_count", 0) + response.get("eval_count", 0),
            }

            return response['message']['content'], stats
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"      ⚠ Error ({e}), retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise


def call_litellm(model: str, text_chunk: str) -> tuple[str, dict]:
    """
    Call LiteLLM with Ollama backend.

    LiteLLM provides a unified interface for multiple providers.
    For Ollama, use model format: "ollama/llama3.1:8b"
    Returns (response_text, stats_dict).
    """
    if DELAY_BETWEEN_REQUESTS > 0:
        time.sleep(DELAY_BETWEEN_REQUESTS)

    backoff = INITIAL_BACKOFF
    start_time = time.time()

    # Ensure model has ollama prefix for LiteLLM
    if not model.startswith("ollama/"):
        model = f"ollama/{model}"

    for attempt in range(MAX_RETRIES):
        try:
            response = litellm.completion(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": text_chunk},
                ],
                temperature=0.0,
                api_base="http://localhost:11434",  # Default Ollama port
            )
            elapsed = time.time() - start_time

            # Extract stats
            usage = response.get("usage", {})
            stats = {
                "duration_sec": elapsed,
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
                "total_tokens": usage.get("total_tokens", 0),
            }

            return response.choices[0].message.content, stats
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"      ⚠ Error ({e}), retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise


def call_local_llm(model: str, text_chunk: str) -> tuple[str, dict]:
    """
    Call local LLM using either Ollama or LiteLLM.
    Returns (response_text, stats_dict).
    """
    if USE_LITELLM and HAS_LITELLM:
        return call_litellm(model, text_chunk)
    elif HAS_OLLAMA:
        return call_ollama_direct(model, text_chunk)
    else:
        raise RuntimeError("Neither ollama nor litellm is available")


def process_file(model: str, raw_path: Path, clean_path: Path, file_num: int, total: int) -> tuple[bool, dict]:
    """
    Process a single file: strip garbled line 0, chunk, send to local LLM, reassemble.
    Returns (success: bool, stats: dict).
    """
    rel = raw_path.relative_to(RAW_DIR)
    file_start_time = time.time()

    print(f"\n{'='*80}")
    print(f"[{file_num}/{total}] {rel}")
    print(f"{'='*80}")

    text = raw_path.read_text(encoding="utf-8", errors="replace")
    total_words = len(text.split())

    # Step 1: Strip garbled line 0
    text, was_stripped = strip_garbled_line0(text)
    if was_stripped:
        print(f"  ✓ Stripped garbled line 0")

    # Step 2: Chunk the text
    chunks = chunk_text(text)
    print(f"  ℹ Text: {total_words} words → {len(chunks)} chunk(s)")

    if SHOW_TOKEN_ESTIMATES:
        est_tokens = estimate_tokens(text)
        print(f"  ℹ Estimated tokens: ~{est_tokens} total, ~{est_tokens//len(chunks)} per chunk")

    # Step 3: Process each chunk through local LLM
    cleaned_chunks = []
    chunk_stats = []

    for i, chunk in enumerate(chunks):
        chunk_words = len(chunk.split())
        chunk_tokens = estimate_tokens(chunk)

        if SHOW_CHUNK_DETAILS:
            print(f"\n  Chunk {i+1}/{len(chunks)}: {chunk_words} words (~{chunk_tokens} tokens)")

        chunk_start = time.time()
        cleaned, stats = call_local_llm(model, chunk)
        chunk_elapsed = time.time() - chunk_start

        if SHOW_CHUNK_DETAILS:
            actual_tokens = stats.get("total_tokens", 0)
            tokens_per_sec = actual_tokens / chunk_elapsed if chunk_elapsed > 0 else 0
            print(f"    ✓ Completed in {chunk_elapsed:.1f}s", end="")
            if actual_tokens > 0:
                print(f" ({actual_tokens} tokens, {tokens_per_sec:.0f} tok/s)", end="")
            print()

        cleaned_chunks.append(cleaned)
        chunk_stats.append(stats)

    # Step 4: Reassemble and write
    cleaned_text = "\n\n".join(cleaned_chunks)

    clean_path.parent.mkdir(parents=True, exist_ok=True)
    clean_path.write_text(cleaned_text, encoding="utf-8")

    file_elapsed = time.time() - file_start_time

    # Aggregate stats
    total_prompt_tokens = sum(s.get("prompt_tokens", 0) for s in chunk_stats)
    total_completion_tokens = sum(s.get("completion_tokens", 0) for s in chunk_stats)
    total_tokens = sum(s.get("total_tokens", 0) for s in chunk_stats)

    print(f"\n  ✓ File completed in {file_elapsed:.1f}s")
    if total_tokens > 0:
        print(f"    Tokens: {total_prompt_tokens} prompt + {total_completion_tokens} completion = {total_tokens} total")
        print(f"    Throughput: {total_tokens/file_elapsed:.0f} tokens/sec")
    print(f"    Output: {clean_path.relative_to(PROJECT_ROOT)}")

    file_stats = {
        "duration_sec": file_elapsed,
        "chunks": len(chunks),
        "words": total_words,
        "total_tokens": total_tokens,
        "prompt_tokens": total_prompt_tokens,
        "completion_tokens": total_completion_tokens,
    }

    return True, file_stats


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


def check_model_availability(model: str) -> bool:
    """
    Check if the specified model is available in Ollama.
    """
    try:
        models = ollama.list()
        available = [m['name'] for m in models.get('models', [])]

        # Check for exact match or partial match (e.g., llama3.1:8b matches llama3.1)
        for available_model in available:
            if model in available_model or available_model in model:
                return True

        print(f"Warning: Model '{model}' not found in Ollama.")
        print(f"Available models: {', '.join(available)}")
        print(f"\nTo pull the model, run: ollama pull {model}")
        return False
    except Exception as e:
        print(f"Warning: Could not check Ollama models: {e}")
        return True  # Proceed anyway


def main():
    parser = argparse.ArgumentParser(
        description="OCR cleanup pipeline using local LLM (Ollama)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Recommended models for 8GB GPU (NVIDIA 4060):
  llama3.1:8b          - Best balance of quality and speed (recommended)
  qwen2.5:7b           - Excellent instruction following
  mistral:7b-v0.3      - Fast and efficient
  phi3:14b-medium-4k-instruct-q4_0 - 14B quantized to fit 8GB

Setup:
  1. Install Ollama: https://ollama.ai/
  2. Pull a model: ollama pull llama3.1:8b
  3. Install: pip install ollama

Alternative - Use LiteLLM:
  pip install litellm
  # Edit USE_LITELLM = True in this script
        """
    )
    parser.add_argument("--limit", type=int, default=None, help="Process only N files (for testing)")
    parser.add_argument("--volume", type=str, default=None, help="Process only a specific volume folder (e.g. Vol45)")
    parser.add_argument("--dry-run", action="store_true", help="Show files that would be processed without making API calls")
    parser.add_argument("--reset-progress", action="store_true", help="Clear the progress file and start fresh")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL, help=f"Model to use (default: {DEFAULT_MODEL})")
    parser.add_argument("--use-litellm", action="store_true", help="Use LiteLLM instead of Ollama directly")
    args = parser.parse_args()

    # Set LiteLLM flag if requested
    global USE_LITELLM
    if args.use_litellm:
        USE_LITELLM = True

    # Check dependencies for non-dry-run mode
    if not args.dry_run:
        if USE_LITELLM:
            if not HAS_LITELLM:
                print("Error: litellm package not installed. Run: pip install litellm", file=sys.stderr)
                sys.exit(1)
        else:
            if not HAS_OLLAMA:
                print("Error: ollama package not installed. Run: pip install ollama", file=sys.stderr)
                sys.exit(1)

            # Check if model is available
            check_model_availability(args.model)

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

    print(f"Model: {args.model}")
    print(f"Backend: {'LiteLLM' if USE_LITELLM else 'Ollama'}")
    print(f"Total files found: {len(all_pairs)}")
    print(f"Already completed: {already_done}")
    print(f"To process: {len(pairs)}")
    print(f"Failed previously: {len(progress['failed'])}")
    print()

    if args.dry_run:
        for raw_path, clean_path in pairs:
            print(f"  {raw_path.relative_to(RAW_DIR)} → {clean_path.relative_to(PROJECT_ROOT)}")
        return

    # Check if Ollama is running before starting
    if not args.dry_run and not USE_LITELLM:
        try:
            print("Checking Ollama connection...")
            ollama.list()
            print("✓ Ollama is running\n")
        except Exception as e:
            print(f"✗ Error: Cannot connect to Ollama. Is it running?", file=sys.stderr)
            print(f"  Start Ollama with: ollama serve", file=sys.stderr)
            sys.exit(1)

    succeeded = 0
    failed = 0
    start_time = time.time()
    all_stats = []

    for i, (raw_path, clean_path) in enumerate(pairs, 1):
        rel_key = str(raw_path.relative_to(RAW_DIR))
        try:
            ok, file_stats = process_file(args.model, raw_path, clean_path, i, len(pairs))
            if ok:
                progress["completed"].append(rel_key)
                progress["failed"].pop(rel_key, None)
                succeeded += 1
                all_stats.append(file_stats)

                # Show cumulative progress
                elapsed = time.time() - start_time
                avg_time = elapsed / succeeded
                remaining = len(pairs) - i
                eta_sec = avg_time * remaining
                eta_min = eta_sec / 60

                print(f"\n  Progress: {succeeded}/{len(pairs)} files ({succeeded/len(pairs)*100:.1f}%)")
                print(f"  Elapsed: {elapsed/60:.1f} min | ETA: {eta_min:.1f} min")

        except Exception as e:
            print(f"\n  ✗ FAILED: {e}", file=sys.stderr)
            progress["failed"][rel_key] = str(e)
            failed += 1

        # Save progress after each file
        save_progress(progress)

    elapsed = time.time() - start_time

    # Print summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Status: {succeeded} succeeded, {failed} failed")
    print(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")

    if succeeded > 0:
        avg_time = elapsed / succeeded
        total_tokens = sum(s.get("total_tokens", 0) for s in all_stats)
        total_words = sum(s.get("words", 0) for s in all_stats)
        avg_tokens_per_sec = total_tokens / elapsed if elapsed > 0 else 0

        print(f"Average time per file: {avg_time:.1f}s")
        print(f"Total words processed: {total_words:,}")
        print(f"Total tokens processed: {total_tokens:,}")
        print(f"Average throughput: {avg_tokens_per_sec:.0f} tokens/sec")

        # Estimate remaining work
        remaining_files = 684 - len(progress['completed'])
        if remaining_files > 0:
            estimated_remaining_time = (remaining_files * avg_time) / 60
            print(f"\nEstimated time to complete all {remaining_files} remaining files: {estimated_remaining_time:.1f} minutes")

    print(f"\nTotal completed (all runs): {len(progress['completed'])}/684 ({len(progress['completed'])/684*100:.1f}%)")

    if progress["failed"]:
        print(f"\nFailed files: {len(progress['failed'])}")
        for f, err in progress["failed"].items():
            print(f"  - {f}: {err}")


if __name__ == "__main__":
    main()
