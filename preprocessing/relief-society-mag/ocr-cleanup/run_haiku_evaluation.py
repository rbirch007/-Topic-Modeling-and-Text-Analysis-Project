#!/usr/bin/env python3
"""
Run Haiku-based quality assessment of OCR cleanup samples.
Processes all batch files and generates detailed evaluation results.
"""

import json
import anthropic
import time
from pathlib import Path

# Initialize Anthropic client
client = anthropic.Anthropic()

# Paths
qa_dir = Path("preprocessing/relief-society-mag/ocr-cleanup/qa_results")
batch_files = sorted(qa_dir.glob("batch_*_input.json"))

print(f"Found {len(batch_files)} batch files to process")

all_results = []
total_samples = 0
start_time = time.time()

for batch_file in batch_files:
    print(f"\nProcessing {batch_file.name}...")

    with open(batch_file, 'r', encoding='utf-8') as f:
        batch_data = json.load(f)

    batch_num = batch_data['batch_num']
    samples = batch_data['samples']
    prompt_template = batch_data['prompt_template']

    print(f"  Batch {batch_num}: {len(samples)} samples")

    batch_results = {
        'batch_num': batch_num,
        'sample_evaluations': []
    }

    for sample in samples:
        sample_id = sample['id']
        volume = sample['volume']
        filename = sample['filename']
        raw_text = sample['raw_sample']
        clean_text = sample['clean_sample']

        print(f"    Evaluating sample {sample_id}: {filename}...", end='', flush=True)

        # Format the prompt
        prompt = prompt_template.format(raw_text=raw_text, clean_text=clean_text)

        try:
            # Call Haiku
            message = client.messages.create(
                model="claude-haiku-4.5-20251001",
                max_tokens=1024,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # Parse response
            response_text = message.content[0].text

            # Try to extract JSON from response
            # Sometimes the model includes text before/after JSON
            if '{' in response_text and '}' in response_text:
                json_start = response_text.index('{')
                json_end = response_text.rindex('}') + 1
                json_str = response_text[json_start:json_end]
                evaluation = json.loads(json_str)
            else:
                evaluation = {"error": "No JSON found in response", "raw_response": response_text}

            # Add metadata
            evaluation['sample_id'] = sample_id
            evaluation['volume'] = volume
            evaluation['filename'] = filename
            evaluation['sample_words'] = sample['sample_words']

            batch_results['sample_evaluations'].append(evaluation)

            print(f" ✓ (score: {evaluation.get('quality_score', 'N/A')}, errors: {evaluation.get('total_errors', 'N/A')})")
            total_samples += 1

            # Small delay to avoid rate limiting
            time.sleep(0.5)

        except Exception as e:
            print(f" ✗ Error: {e}")
            batch_results['sample_evaluations'].append({
                'sample_id': sample_id,
                'volume': volume,
                'filename': filename,
                'error': str(e)
            })

    all_results.append(batch_results)

    # Save intermediate results after each batch
    output_file = qa_dir / f"batch_{batch_num:02d}_evaluation.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, indent=2)
    print(f"  Saved results to {output_file.name}")

# Save combined results
combined_file = qa_dir / "all_evaluations.json"
with open(combined_file, 'w', encoding='utf-8') as f:
    json.dump(all_results, f, indent=2)

elapsed = time.time() - start_time
print(f"\n{'='*60}")
print(f"Completed evaluation of {total_samples} samples in {elapsed:.1f} seconds")
print(f"Results saved to {combined_file}")

# Generate summary statistics
print(f"\n{'='*60}")
print("SUMMARY STATISTICS")
print(f"{'='*60}")

total_errors = 0
total_quality_score = 0
valid_samples = 0
error_breakdown = {
    'character_substitutions': 0,
    'word_level_errors': 0,
    'proper_noun_errors': 0,
    'complex_garbles': 0,
    'punctuation_errors': 0,
    'context_ambiguities': 0,
    'severity_critical': 0,
    'severity_minor': 0
}

for batch in all_results:
    for eval_result in batch['sample_evaluations']:
        if 'error' not in eval_result and 'total_errors' in eval_result:
            valid_samples += 1
            total_errors += eval_result.get('total_errors', 0)
            total_quality_score += eval_result.get('quality_score', 0)

            for key in error_breakdown.keys():
                error_breakdown[key] += eval_result.get(key, 0)

if valid_samples > 0:
    avg_errors = total_errors / valid_samples
    avg_quality = total_quality_score / valid_samples

    print(f"\nSamples evaluated: {valid_samples}/{total_samples}")
    print(f"Average quality score: {avg_quality:.1f}/100")
    print(f"Average errors per sample: {avg_errors:.1f}")
    print(f"\nError breakdown:")
    for error_type, count in error_breakdown.items():
        print(f"  {error_type}: {count} ({count/valid_samples:.1f} avg per sample)")

    # Save summary
    summary = {
        'evaluation_date': time.strftime('%Y-%m-%d %H:%M:%S'),
        'total_samples': total_samples,
        'valid_samples': valid_samples,
        'average_quality_score': round(avg_quality, 2),
        'average_errors_per_sample': round(avg_errors, 2),
        'total_errors': total_errors,
        'error_breakdown': error_breakdown,
        'error_breakdown_averages': {k: round(v/valid_samples, 2) for k, v in error_breakdown.items()}
    }

    summary_file = qa_dir / "evaluation_summary.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary saved to {summary_file.name}")
else:
    print("\nNo valid samples evaluated!")

print(f"{'='*60}")
