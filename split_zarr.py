from spatialdata import SpatialData
from spatialdata.models import PointsModel
from spatialdata.models import ShapesModel
import pandas as pd

# start with reading a (cell_id-sampleid_legacy-label) triplet file 
labels = pd.read_csv("/dfs3b/ruic20_lab/yuqih25/cellid_sampleid_tglabel.csv", index_col=None)

# only look at the current sample

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

# Convert back into valid spatialdata Points
left_transcripts = PointsModel.parse(left_transcripts)
right_transcripts = PointsModel.parse(right_transcripts)

# save into sdata
sdata["transcripts_left"] = left_transcripts
sdata["transcripts_right"] = right_transcripts

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

# Convert back into valid spatialdata Points
left_boundaries = ShapesModel.parse(left_boundaries)
right_boundaries = ShapesModel.parse(right_boundaries)

# save into sdata
sdata["cell_boundaries_left"] = left_boundaries
sdata["cell_boundaries_right"] = right_boundaries

print(f"Processed {len(cell_boundaries_labeled)} cell boundaries for {sample_name}")

# write to new Zarr
sdata.write("/dfs3b/ruic20_lab/yuqih25/Round3_Slide05_Section01_outdir/Round3_Slide05_Section01_split.zarr")