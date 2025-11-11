#!/bin/bash

# Configuration
DATA_DIR="/scratch/srearl/nitrate_spatial/"  # directory containing geojson inputs
OUTPUT_DIR="/scratch/srearl/nitrate_spatial/site_results"
SITES_FILE="nitrate_sites.csv"

# Create output directory
mkdir -p $OUTPUT_DIR

# Clean up any previous failed/no data files
rm -f $OUTPUT_DIR/failed_sites.txt
rm -f $OUTPUT_DIR/sites_no_data.txt

echo "Submitting jobs for individual sites..."
echo "Output directory: $OUTPUT_DIR"

job_count=0
line_num=0
while IFS=',' read -r usgs_site site_id extra
do
    line_num=$((line_num+1))

    # Trim whitespace / carriage returns
    site_id=${site_id//$'\r'/}
    site_id=$(echo "$site_id" | xargs)

    # Skip header (case-insensitive) or empty line
    if [[ -z "$site_id" ]] || [[ "$site_id" =~ ^site_id$|^SITE_ID$ ]]; then
        continue
    fi

    # Basic validation: digits only (adjust if alphanumeric expected)
    if [[ ! "$site_id" =~ ^[0-9A-Za-z_-]+$ ]]; then
        echo "Skipping invalid site_id on line $line_num: '$site_id'" >&2
        continue
    fi

    echo "Submitting job for site: $site_id"
    job_id=$(sbatch --parsable --job-name="dist_$site_id" run_single_site.sh "$site_id" "$DATA_DIR" "$OUTPUT_DIR")
    echo "  Job ID: $job_id"
    ((job_count++))
    sleep 0.05
done < "$SITES_FILE"

echo "Submitted $job_count jobs"
echo ""
echo "Monitor jobs with: squeue -u \$USER"
echo "Check results in: $OUTPUT_DIR"
echo ""
echo "When complete, combine results with:"
echo "  cd $OUTPUT_DIR"
echo "  head -1 \$(ls distance_*.csv | head -1) > combined_distances.csv"
echo "  tail -n +2 -q distance_*.csv >> combined_distances.csv"
