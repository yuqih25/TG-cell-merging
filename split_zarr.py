import argparse
from spatialdata import SpatialData
from read_labels import read_labels

import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--sample_name", required=True)
args = parser.parse_args()

labels = read_labels("/dfs3b/ruic20_lab/jinl14/mrrdir/wkfl/spatialprj/pipeline/xenium/organTG/3d_reconstruction/twoTG/tglabel/preproc/mouseTG.txt.gz")
labels_subset = labels[labels["sampleid_legacy"] == args.sample_name]

sdata = SpatialData.read(f"/dfs3b/ruic20_lab/yuqih25/{args.sample_name}_outdir/{args.sample_name}.zarr")
transcripts = sdata["transcripts"]

transcripts_labeled = transcripts.merge(
    labels_subset[["cell_id", "tglabel"]],
    on="cell_id",
    how="inner"   
)

left_transcripts = transcripts_labeled[transcripts_labeled["tglabel"] == "leftTG"]
right_transcripts = transcripts_labeled[transcripts_labeled["tglabel"] == "rightTG"]

print(f"Processed {len(transcripts_labeled)} transcripts for {args.sample_name}")

cell_boundaries = sdata["cell_boundaries"]
cb = cell_boundaries.reset_index(names="cell_id")

cb_labeled = cb.merge(
    labels_subset[["cell_id", "tglabel"]],
    on="cell_id",
    how="inner"
)

cell_boundaries_labeled = cell_boundaries.merge(
    labels_subset[["cell_id", "tglabel"]],
    left_index=True,
    right_on="cell_id",
    how="inner"
)

left_boundaries = cell_boundaries_labeled[cell_boundaries_labeled["tglabel"] == "leftTG"]
right_boundaries = cell_boundaries_labeled[cell_boundaries_labeled["tglabel"] == "rightTG"]

print(f"Processed {len(cell_boundaries_labeled)} cell boundaries for {args.sample_name}")
