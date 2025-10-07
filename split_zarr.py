from spatialdata import SpatialData
from read_labels import read_labels
import pandas as pd

# start with reading (cell_id-sampleid_legacy-label) triplets from file
labels = read_labels("/dfs3b/ruic20_lab/jinl14/mrrdir/wkfl/spatialprj/pipeline/xenium/organTG/3d_reconstruction/twoTG/tglabel/preproc/xeniummetadata2addlabelbypolygon/mouseTG.txt.gz")

# only look at the current sample
sample_name = "Round3_Slide05_Section01"
labels_subset = labels[labels["sampleid_legacy"] == sample_name]

# read a .zarr file into a spatialdata object
sdata = SpatialData.read("/dfs3b/ruic20_lab/yuqih25/Round3_Slide05_Section01_outdir/Round3_Slide05_Section01.zarr")

# access the transcript table
transcripts = sdata["transcripts"]

# merge transcripts with the subset labels
transcripts_labeled = transcripts.merge(
    labels_subset[["cell_id", "tglabel"]],
    on="cell_id",
    how="inner"   
)

# split transcripts into left and right channels by tglabel
left_transcripts = transcripts_labeled[transcripts_labeled["tglabel"] == "leftTG"]
right_transcripts = transcripts_labeled[transcripts_labeled["tglabel"] == "rightTG"]

print(f"Processed {len(transcripts_labeled)} transcripts for {sample_name}")

# access the cell_boundaries table
cell_boundaries = sdata["cell_boundaries"]

# Reset index so cell_id is a column
cb = cell_boundaries.reset_index(names="cell_id")

# Merge on the explicit 'cell_id' column
cb_labeled = cb.merge(
    labels_subset[["cell_id", "tglabel"]],
    on="cell_id",
    how="inner"
)


# merge cell boundaries with the subset labels
cell_boundaries_labeled = cell_boundaries.merge(
    labels_subset[["cell_id", "tglabel"]],
    left_index=True,    
    right_on="cell_id",
    how="inner"
)

# split cell boundaries into left and right channels by tglabel
left_boundaries = cell_boundaries_labeled[cell_boundaries_labeled["tglabel"] == "leftTG"]
right_boundaries = cell_boundaries_labeled[cell_boundaries_labeled["tglabel"] == "rightTG"]

print(f"Processed {len(cell_boundaries_labeled)} cell boundaries for {sample_name}")
