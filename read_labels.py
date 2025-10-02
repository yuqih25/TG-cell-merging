import pandas as pd

input_file = "/dfs3b/ruic20_lab/jinl14/mrrdir/wkfl/spatialprj/pipeline/xenium/organTG/3d_reconstruction/twoTG/tglabel/preproc/xeniummetadata2addlabelbypolygon/mouseTG.txt.gz"

# Columns to read (0-based indices)
cols_to_use = [17, 38, 40]

# Read only these columns
df = pd.read_csv(input_file, sep="\t", usecols=cols_to_use, header=0, compression='gzip')

# Rename for clarity
df.columns = ["cell_id", "sampleid_legacy", "tglabel"]

# Save to CSV
output_file = "/dfs3b/ruic20_lab/yuqih25/cellid_sampleid_tglabel.csv"
df.to_csv(output_file, index=False)

print(f"Saved {len(df)} rows with 3 columns to {output_file}")
