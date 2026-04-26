#!/bin/bash
#SBATCH -c 1
#SBATCH -J Qusgs
#SBATCH -o %j.out
#SBATCH -e %j.err
#SBATCH -t 3-00:00:00
#SBATCH --mem=64G
#SBATCH --mail-type=END
#SBATCH --mail-user=%u@asu.edu

set -euo pipefail

# Run from submission directory
cd "$SLURM_SUBMIT_DIR"

# Optional: load an R module if your cluster requires it
# module load R

# Optional: override output location on HPC scratch/storage
# export USGS_OUTPUT_DIR="/path/to/output"

Rscript run_usgs_latest_discharge_to_rds_hpc.R
