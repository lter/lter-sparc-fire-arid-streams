#!/bin/bash
#SBATCH -c 4
#SBATCH -o distance_%j_%x.out
#SBATCH -e distance_%j_%x.err
#SBATCH -t 3-00:00:00
#SBATCH -t 12:00:00
#SBATCH --mem=32G
#SBATCH --mail-type=END
#SBATCH --mail-user=%u@asu.edu

# Get site ID and directories from command line arguments
SITE_ID=$1
DATA_DIR=$2
OUTPUT_DIR=$3

if [ -z "$SITE_ID" ] || [ -z "$DATA_DIR" ] || [ -z "$OUTPUT_DIR" ]; then
    echo "Usage: sbatch run_single_site.sh <site_id> <data_dir> <output_dir>"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p $OUTPUT_DIR

module purge
module load r-4.4.2-gcc-12.1.0

echo "Processing site: $SITE_ID"
echo "Data directory: $DATA_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "Job ID: $SLURM_JOB_ID"
echo "Allocated CPUs: $SLURM_CPUS_PER_TASK"
echo "Started at: $(date)"

Rscript run_single_site.R $SITE_ID $DATA_DIR $OUTPUT_DIR

echo "Completed at: $(date)"
