#!/bin/bash
# Setup script for creating the optimized OCR cleanup model

set -e  # Exit on error

echo "================================================"
echo "Setting up Qwen 2.5 7B for OCR Cleanup"
echo "================================================"
echo

# Check if ollama is installed
if ! command -v ollama &> /dev/null; then
    echo "Error: Ollama is not installed."
    echo "Install from: https://ollama.ai/"
    exit 1
fi

# Check if ollama is running
if ! curl -s http://localhost:11434/api/tags &> /dev/null; then
    echo "Error: Ollama is not running."
    echo "Start it with: ollama serve"
    exit 1
fi

echo "✓ Ollama is installed and running"
echo

# Pull base model if not present
echo "Checking for qwen2.5:7b..."
if ! ollama list | grep -q "qwen2.5:7b"; then
    echo "Pulling qwen2.5:7b (this may take a few minutes)..."
    ollama pull qwen2.5:7b
else
    echo "✓ qwen2.5:7b already available"
fi
echo

# Create optimized model
echo "Creating optimized model 'qwen-ocr' from Modelfile..."
ollama create qwen-ocr -f Modelfile

echo
echo "✓ Model 'qwen-ocr' created successfully!"
echo

# Show model info
echo "Model details:"
ollama show qwen-ocr --modelfile
echo

# Test the model
echo "Testing model with a simple prompt..."
echo "Prompt: 'Fix OCR errors: The qu¥ck brown fox jumps over the l©zy dog.'"
echo
echo "Response:"
echo "Fix OCR errors: The qu¥ck brown fox jumps over the l©zy dog." | ollama run qwen-ocr "You are an OCR cleanup assistant. Fix character substitution errors in this text and return ONLY the corrected text:"
echo

echo "================================================"
echo "Setup complete!"
echo "================================================"
echo
echo "Usage:"
echo "  python ocr_cleanup_local.py --model qwen-ocr --limit 1"
echo
