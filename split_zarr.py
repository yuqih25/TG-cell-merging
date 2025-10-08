import argparse
from spatialdata import SpatialData
from read_labels import read_labels
from shapely.geometry import Point
import geopandas as gpd
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--sample_name", required=True)
args = parser.parse_args()

labels = read_labels("/dfs3b/ruic20_lab/jinl14/mrrdir/wkfl/spatialprj/pipeline/xenium/organTG/3d_reconstruction/twoTG/tglabel/preproc/xeniummetadata2addlabelbypolygon/mouseTG.txt.gz")
labels_subset = labels[labels["sampleid_legacy"] == args.sample_name]

sdata = SpatialData.read(
    f"/dfs3b/ruic20_lab/yuqih25/{args.sample_name}_outdir/{args.sample_name}.zarr",
    selection=("points", "shapes")
)

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

# 
# Left transcripts with 3D geometry
gdf_left_transcripts = gpd.GeoDataFrame(
    left_transcripts,
    geometry=[Point(x, y, z) for x, y, z in zip(
        left_transcripts.x,
        left_transcripts.y,
        left_transcripts.z
    )],
    crs="EPSG:3857"  # or your actual coordinate reference system if known
)

gdf_left_transcripts.to_file(f"{args.sample_name}_left_transcripts.geojson", driver="GeoJSON")

# Right transcripts with 3D geometry
gdf_right_transcripts = gpd.GeoDataFrame(
    right_transcripts,
    geometry=[Point(x, y, z) for x, y, z in zip(
        right_transcripts.x,
        right_transcripts.y,
        right_transcripts.z
    )],
    crs="EPSG:3857"  # or your actual CRS if different
)

# Save to GeoJSON
gdf_right_transcripts.to_file(f"{args.sample_name}_right_transcripts.geojson", driver="GeoJSON")

# Convert channels to GeoDataFrame and save to GeoJSON
gdf_left_boundaries = gpd.GeoDataFrame(left_boundaries, geometry='geometry')
gdf_left_boundaries.to_file(f"{args.sample_name}_left_boundaries.geojson", driver="GeoJSON")

gdf_right_boundaries = gpd.GeoDataFrame(right_boundaries, geometry='geometry')
gdf_right_boundaries.to_file(f"{args.sample_name}_right_boundaries.geojson", driver="GeoJSON")

print("Saved all 4 files successfully.")