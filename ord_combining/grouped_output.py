from pathlib import Path
import pandas as pd
import numpy as np

def generate_aal(gplt, max_period):
    # TODO: mean loss sampling results in inf + NaN values

    # todo: should group by group_set_id not output_set_id
    aal_group = gplt.groupby(by=["output_set_id", "SummaryId", "LossType"], as_index=False)

    records = []
    for name, group in aal_group:
        mean_loss = group["Loss"].sum() / max_period
        std_loss = np.sqrt(((mean_loss - group["Loss"])**2).sum() / (max_group_period - 1))

        record = {
                "output_set_id": name[0],
                "SummaryId": name[1],
                "LossType": name[2],
                "Mean": mean_loss,
                "Std": std_loss
                }

        records.append(record)

    return  pd.DataFrame(records)

def generate_ep(gplt):
    pass


# %% path def
gplt_path = Path('/home/vinulw/code/ODS_Tools/combined_ord-141125121128/gplt_full.csv')
output_dir  = gplt_path.parent

# %%
gplt = pd.read_csv(gplt_path)
max_group_period = 2000

# %% aal
aal_df = generate_aal(gplt, max_group_period)
aal_df.to_csv(output_dir / "aal.csv")

aal_df
