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

module purge
module load mamba/latest
module load r-4.5.1-gcc-12.1.0

Rscript run_usgs_latest_discharge_to_rds_hpc.R
