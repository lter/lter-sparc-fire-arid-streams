#!/bin/bash

# Configuration
OUTPUT_DIR="/scratch/srearl/nitrate_spatial/site_results"
SITES_FILE="nitrate_sites.csv"

# Create output directory
mkdir -p $OUTPUT_DIR

# Clean up any previous failed/no data files
rm -f $OUTPUT_DIR/failed_sites.txt
rm -f $OUTPUT_DIR/sites_no_data.txt

echo "Submitting jobs for individual sites..."
echo "Output directory: $OUTPUT_DIR"

# Read site IDs from CSV (skip header) and submit jobs
job_count=0
while IFS=',' read -r usgs_site site_id
do
    # Skip header line
    if [[ "$site_id" != "site_id" ]]; then
        echo "Submitting job for site: $site_id"
        job_id=$(sbatch --parsable --job-name="dist_$site_id" run_single_site.sh $site_id $OUTPUT_DIR)
        echo "  Job ID: $job_id"
        ((job_count++))
        
        # Optional: Add a small delay to avoid overwhelming the scheduler
        sleep 0.1
    fi
done < $SITES_FILE

echo "Submitted $job_count jobs"
echo ""
echo "Monitor jobs with: squeue -u \$USER"
echo "Check results in: $OUTPUT_DIR"
echo ""
echo "When complete, combine results with:"
echo "  cd $OUTPUT_DIR"
echo "  head -1 \$(ls distance_*.csv | head -1) > combined_distances.csv"
echo "  tail -n +2 -q distance_*.csv >> combined_distances.csv"
