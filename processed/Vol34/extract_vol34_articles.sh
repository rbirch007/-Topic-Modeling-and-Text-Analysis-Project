#!/bin/bash

# Extract articles from Vol34 raw source files
# Input: cleaned-data/relief-society/txtvolumesbymonth/Vol34/vol34_No*_*.txt
# Output: processed/Vol34/ with article files, manifest.csv, and Vol34_entries.json

set -e

BASE_DIR="/Users/chris/devel/rbirch"
SOURCE_DIR="$BASE_DIR/cleaned-data/relief-society/txtvolumesbymonth/Vol34"
OUTPUT_DIR="$BASE_DIR/processed/Vol34"

# Create output directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Month mapping
declare -a MONTHS=("January" "February" "March" "April" "May" "June" "July" "August" "September" "October" "November" "December")
declare -a MONTH_NUMS=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")
declare -a FILE_NUMS=("No1" "No2" "No3" "No4" "No5" "No6" "No7" "No8" "No9" "No10" "No11" "No12")

# Initialize CSV
CSV_FILE="$OUTPUT_DIR/manifest.csv"
echo "file,path,volume,month,etype,title,author,strategy" > "$CSV_FILE"

# Initialize JSON array
JSON_FILE="$OUTPUT_DIR/Vol34_entries.json"
echo "[" > "$JSON_FILE"

FIRST_ENTRY=true

# Process each month
for i in {0..11}; do
    MONTH="${MONTHS[$i]}"
    MONTH_NUM="${MONTH_NUMS[$i]}"
    FILE_NUM="${FILE_NUMS[$i]}"

    SOURCE_FILE="$SOURCE_DIR/vol34_${FILE_NUM}_${MONTH}_1947.txt"

    if [ ! -f "$SOURCE_FILE" ]; then
        echo "Warning: File not found: $SOURCE_FILE"
        continue
    fi

    echo "Processing: $MONTH ($SOURCE_FILE)"

    # Create month directory
    MONTH_DIR="$OUTPUT_DIR/$MONTH"
    mkdir -p "$MONTH_DIR"

    # Extract TOC and parse for article titles
    # Find where TOC starts (line with "Contents") and where content starts (first non-header line after TOC)

    # For now, write the complete source to the month directory for manual article extraction
    # This is a placeholder - actual article extraction will be done via semantic analysis
    cp "$SOURCE_FILE" "$MONTH_DIR/raw_${MONTH,,}.txt"

done

# Close JSON
echo "]" >> "$JSON_FILE"

echo "Processing complete. Output in: $OUTPUT_DIR"
