#!/bin/bash
#SBATCH --job-name=split_zarr
#SBATCH --output=logs/split_zarr_%A_%a.out
#SBATCH --error=logs/split_zarr_%A_%a.err
#SBATCH --time=01:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --array=1-$(wc -l < samples.txt)

module load python/3.10  # or your environment
source /dfs3b/ruic20_lab/jinl14/share/script/bashrc  # if needed

# get the sample name for this task
SAMPLE=$(sed -n "${SLURM_ARRAY_TASK_ID}p" samples.txt)

# run your python script
python split_zarr.py --sample_name $SAMPLE
