import os
import pandas as pd

def ensure_outdir(outdir):
    os.makedirs(outdir, exist_ok=True)

def write_outputs_people(records, outdir):
    df = pd.DataFrame(records)
    df.to_csv(os.path.join(outdir, "people_deduped.csv"), index=False)

def write_outputs_accounts(records, outdir):
    df = pd.DataFrame(records)
    df.to_csv(os.path.join(outdir, "accounts_deduped.csv"), index=False)
