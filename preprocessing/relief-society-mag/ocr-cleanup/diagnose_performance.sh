#!/bin/bash
# Performance Diagnostic Script for OCR Cleanup

echo "========================================="
echo "OCR Cleanup Performance Diagnostics"
echo "========================================="
echo

# 1. Check GPU availability
echo "1. GPU Status:"
echo "-------------------------------------------"
if command -v nvidia-smi &> /dev/null; then
    nvidia-smi --query-gpu=name,memory.total,memory.used,memory.free,utilization.gpu,utilization.memory,temperature.gpu --format=csv
    echo
    nvidia-smi
else
    echo "⚠ WARNING: nvidia-smi not found. GPU may not be available!"
fi
echo

# 2. Check which model is loaded
echo "2. Model Information:"
echo "-------------------------------------------"
if command -v ollama &> /dev/null; then
    echo "Installed models:"
    ollama list
    echo
    if ollama list | grep -q "qwen-ocr"; then
        echo "qwen-ocr model details:"
        ollama show qwen-ocr
    fi
    if ollama list | grep -q "qwen2.5:7b"; then
        echo "qwen2.5:7b model details:"
        ollama show qwen2.5:7b | head -20
    fi
else
    echo "⚠ ERROR: ollama command not found"
fi
echo

# 3. Check Ollama server status
echo "3. Ollama Server Status:"
echo "-------------------------------------------"
if curl -s http://localhost:11434/api/tags &> /dev/null; then
    echo "✓ Ollama server is running on localhost:11434"

    # Check if a model is currently loaded
    echo
    echo "Checking running processes:"
    ps aux | grep ollama | grep -v grep
else
    echo "✗ Ollama server is not responding"
fi
echo

# 4. System resources
echo "4. System Resources:"
echo "-------------------------------------------"
echo "CPU Info:"
lscpu | grep -E "^Model name|^CPU\(s\)|^Thread"
echo
echo "Memory:"
free -h
echo
echo "Load average:"
uptime
echo

# 5. Test inference speed
echo "5. Inference Speed Test:"
echo "-------------------------------------------"
if ollama list | grep -q "qwen-ocr"; then
    echo "Testing qwen-ocr with short prompt..."
    echo
    START=$(date +%s.%N)
    RESPONSE=$(echo "Fix OCR errors: The qu¥ck brown fox jumps over the l©zy dog." | ollama run qwen-ocr)
    END=$(date +%s.%N)
    DURATION=$(echo "$END - $START" | bc)

    echo "Response: $RESPONSE"
    echo
    echo "Time taken: ${DURATION} seconds"

    # Estimate tokens per second
    APPROX_TOKENS=30  # Rough estimate for this test
    TOKENS_PER_SEC=$(echo "$APPROX_TOKENS / $DURATION" | bc -l)
    printf "Estimated speed: %.1f tokens/sec\n" $TOKENS_PER_SEC
    echo

    if (( $(echo "$TOKENS_PER_SEC < 10" | bc -l) )); then
        echo "⚠ WARNING: Very slow! Expected 20-40 tok/s on GPU"
        echo "   This suggests GPU is not being used or is throttling"
    elif (( $(echo "$TOKENS_PER_SEC < 20" | bc -l) )); then
        echo "⚠ CAUTION: Slower than expected (20-40 tok/s on 4060)"
    else
        echo "✓ Speed looks reasonable for GPU inference"
    fi
fi
echo

# 6. Check for thermal throttling
echo "6. Thermal Check:"
echo "-------------------------------------------"
if command -v nvidia-smi &> /dev/null; then
    TEMP=$(nvidia-smi --query-gpu=temperature.gpu --format=csv,noheader)
    echo "Current GPU temperature: ${TEMP}°C"

    if [ "$TEMP" -gt 80 ]; then
        echo "⚠ WARNING: GPU is hot! May be thermal throttling"
        echo "   Consider: better cooling, lower ambient temp, clean dust"
    else
        echo "✓ Temperature is acceptable"
    fi
fi
echo

# 7. Check disk I/O (for swap detection)
echo "7. Disk/Swap Check:"
echo "-------------------------------------------"
echo "Swap usage:"
free -h | grep Swap
echo
SWAP_USED=$(free | grep Swap | awk '{print $3}')
if [ "$SWAP_USED" -gt 1048576 ]; then  # > 1GB swap
    echo "⚠ WARNING: System is using swap ($(( SWAP_USED / 1024 / 1024 ))GB)"
    echo "   This causes extreme slowdown. Close other applications."
else
    echo "✓ No significant swap usage"
fi
echo

echo "========================================="
echo "Diagnostic Summary"
echo "========================================="
echo
echo "Next steps:"
echo "1. Check GPU utilization during inference (run 'watch -n 1 nvidia-smi' in another terminal)"
echo "2. If GPU utilization is 0%, the model is running on CPU (very slow)"
echo "3. If memory is maxed out, reduce num_ctx in Modelfile"
echo "4. If temperature is high, improve cooling"
echo "5. Share this output for further help"
echo
