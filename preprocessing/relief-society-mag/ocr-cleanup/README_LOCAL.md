# OCR Cleanup with Local LLM (Ollama)

Run OCR cleanup locally using Qwen 2.5 7B instead of OpenAI's API.

## Hardware Requirements

- **8GB NVIDIA GPU** (4060 or similar)
- **16GB+ RAM** recommended
- **~5GB disk space** for model

## One-Time Setup

### 1. Install Ollama

```bash
curl -fsSL https://ollama.ai/install.sh | sh
```

### 2. Start Ollama Server

```bash
ollama serve
```

Keep this running in a terminal (or run as a service).

### 3. Create Optimized Model

```bash
cd preprocessing/relief-society-mag/ocr-cleanup/
./setup_local_model.sh
```

This will:
- Pull `qwen2.5:7b` (~4.7GB download)
- Create `qwen-ocr` model with optimized settings:
  - `temperature=0.0` (deterministic output)
  - `num_ctx=8192` (8K context window)
  - `num_predict=-1` (no output limit)
  - All parameters configured server-side

### 4. Install Python Package

```bash
pip install ollama
```

## Usage

### Test with One File

```bash
python ocr_cleanup_local.py --limit 1
```

### Process One Volume

```bash
python ocr_cleanup_local.py --volume Vol07
```

### Process All Remaining Files

```bash
python ocr_cleanup_local.py
```

### Dry Run (Preview)

```bash
python ocr_cleanup_local.py --dry-run --volume Vol07
```

## Configuration

All model parameters are configured **server-side** in the `Modelfile`:

```dockerfile
FROM qwen2.5:7b
PARAMETER temperature 0.0      # Deterministic output
PARAMETER num_ctx 8192         # 8K context (safe for 8GB GPU)
PARAMETER num_predict -1       # No output limit
PARAMETER repeat_penalty 1.1   # Reduce repetition
PARAMETER num_gpu 99           # Use all GPU layers
```

No per-request configuration needed - just call the model!

## Performance Expectations

With Qwen 2.5 7B Q5 on 8GB 4060:

- **Speed**: 20-40 tokens/second
- **Time per file**: 30-90 seconds (depends on file size)
- **Total time**: ~9-18 hours for all 611 remaining files
- **Cost**: $0 (free, running locally)

## Troubleshooting

### "Cannot connect to Ollama"
```bash
# Check if Ollama is running
curl http://localhost:11434/api/tags

# If not, start it
ollama serve
```

### "Model not found"
```bash
# Recreate the model
./setup_local_model.sh
```

### "Out of memory"
If you run out of VRAM, reduce context window:
```bash
# Edit Modelfile
PARAMETER num_ctx 4096  # Reduce from 8192

# Recreate model
ollama create qwen-ocr -f Modelfile
```

### GPU not being used
```bash
# Check NVIDIA drivers
nvidia-smi

# Verify Ollama sees GPU
ollama show qwen-ocr
```

## Comparison to OpenAI Version

| Aspect | OpenAI (gpt-4o) | Local (qwen-ocr) |
|--------|----------------|------------------|
| Speed | Faster (~100 tok/s) | Slower (~30 tok/s) |
| Cost | ~$0.27/file | Free |
| Quality | Excellent | Very Good |
| Privacy | Data sent to OpenAI | Fully local |
| Total Cost | ~$187 for all files | $0 |
| Total Time | ~2-4 hours | ~9-18 hours |

## Files

- `ocr_cleanup_local.py` - Main script for local processing
- `Modelfile` - Server-side configuration for qwen-ocr model
- `setup_local_model.sh` - One-time setup script
- `progress.json` - Tracks completed files (shared with OpenAI version)

## Advanced: Custom Models

Want to try a different model? Edit the `Modelfile`:

```dockerfile
# Try Llama 3.1 8B instead
FROM llama3.1:8b
PARAMETER temperature 0.0
PARAMETER num_ctx 8192
```

Then recreate:
```bash
ollama create qwen-ocr -f Modelfile  # Reuses same name
```
