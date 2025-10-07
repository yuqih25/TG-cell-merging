import pandas as pd

def read_labels(input_file, cols_to_use=["cell_id", "sampleid_legacy", "tglabel"]):
    df = pd.read_csv(
        input_file,
        sep="\t",
        usecols=cols_to_use,
        header=0,
        compression='gzip'
    )
    return df
