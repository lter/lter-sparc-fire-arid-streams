#!/bin/bash

# Configuration
OUTPUT_DIR="/scratch/srearl/nitrate_spatial/site_results"
COMBINED_FILE="$OUTPUT_DIR/combined_distances.csv"

cd $OUTPUT_DIR

# Check if any distance files exist
if [ ! -f distance_*.csv 2>/dev/null ]; then
    echo "No distance result files found in $OUTPUT_DIR"
    exit 1
fi

# Count files
num_files=$(ls distance_*.csv 2>/dev/null | wc -l)
echo "Found $num_files result files"

# Combine all CSV files
echo "Creating combined file: $COMBINED_FILE"

# Get header from first file
first_file=$(ls distance_*.csv | head -1)
head -1 "$first_file" > "$COMBINED_FILE"

# Append data from all files (skip headers)
tail -n +2 -q distance_*.csv >> "$COMBINED_FILE"

# Count total rows (excluding header)
total_rows=$(($(wc -l < "$COMBINED_FILE") - 1))
echo "Combined file has $total_rows data rows"

# Summary
echo ""
echo "Summary:"
echo "  Combined file: $COMBINED_FILE"
echo "  Total result files: $num_files"
echo "  Total data rows: $total_rows"

if [ -f "failed_sites.txt" ]; then
    failed_count=$(wc -l < "failed_sites.txt")
    echo "  Failed sites: $failed_count (see failed_sites.txt)"
fi

if [ -f "sites_no_data.txt" ]; then
    no_data_count=$(wc -l < "sites_no_data.txt")
    echo "  Sites with no data: $no_data_count (see sites_no_data.txt)"
fi

echo ""
echo "Individual result files can be removed with:"
echo "  rm $OUTPUT_DIR/distance_*.csv"
