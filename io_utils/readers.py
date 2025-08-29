import pandas as pd

def load_table(path, kind=""):
    if path is None:
        return None
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")  # <-- Fix here
    df["source_type"] = kind
    return df