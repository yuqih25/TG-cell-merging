#!/bin/bash
#SBATCH --job-name=split_zarr
#SBATCH --output=logs/split_zarr_%A_%a.out
#SBATCH --error=logs/split_zarr_%A_%a.err
#SBATCH --time=01:00:00
#SBATCH --mem=20G
#SBATCH --cpus-per-task=1



#SBATCH --array=1-$(wc -l < samples.txt)

# get the sample name for this task
SAMPLE=$(sed -n "${SLURM_ARRAY_TASK_ID}p" samples.txt)

#########################################

# run your python script
python split_zarr.py --sample_name $SAMPLE
