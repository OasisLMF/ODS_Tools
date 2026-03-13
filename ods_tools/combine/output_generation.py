import pandas as pd
import numpy as np
from tqdm import tqdm
import logging

from ods_tools.combine.common import GEPT_dtype, GALT_dtype, GEPT_headers

logger = logging.getLogger(__name__)


def generate_alt(gplt, max_period):
    # TODO: mean loss sampling results in inf + NaN values
    aal_group = gplt.groupby(by=["groupset_id", "SummaryId", "LossType"], as_index=False)

    records = []
    for name, group in aal_group:
        mean_loss = group["Loss"].sum() / max_period
        std_loss = np.sqrt(((mean_loss - group["Loss"])**2).sum() / (max_period - 1))

        record = {
            "groupset_id": name[0],
            "SummaryId": name[1],
            "LossType": name[2],
            "MeanLoss": mean_loss,
            "SDLoss": std_loss
        }

        records.append(record)

    return pd.DataFrame(records).astype(GALT_dtype)


def assign_exceedance_probability(df, max_period):
    original_cols = list(df.columns)
    df["rank"] = (df.groupby(by=["groupset_id", "SummaryId", "EPCalc"], as_index=False)["Loss"]
                  .rank(method="first", ascending=False))
    df["ReturnPeriod"] = max_period / df["rank"]
    return df[original_cols + ["ReturnPeriod"]]


def generate_ept(gplt, max_group_period, oep=True, aep=True):
    ep_df = gplt.rename(columns={"LossType": "EPCalc"})  # check if this is the correct type
    chunk_cols = ['groupset_id', 'EPCalc']

    categories = {c: ep_df[c].cat.categories for c in chunk_cols}
    categories['EPType'] = [1, 3]

    ep_df = ep_df.set_index(chunk_cols).sort_index()
    ep_chunks = []

    logger.info('Running chunked GEPT generation:')
    for idx_val in tqdm(ep_df.index.unique()):
        curr_ep = ep_df.loc[idx_val].reset_index(drop=True)
        curr_ep = curr_ep.groupby(by=['groupeventset_id', "EventId", "GroupPeriod", "SummaryId"], as_index=False, observed=True).agg({'Loss': 'sum'})

        if oep:
            curr_oep = curr_ep.groupby(by=["GroupPeriod", "SummaryId"], as_index=False).agg({'Loss': 'sum'})
            curr_oep['ReturnPeriod'] = max_group_period / curr_oep.groupby(by='SummaryId')['Loss'].rank(method='first', ascending=False)
            curr_oep['EPType'] = 1
            curr_oep[chunk_cols] = idx_val
            curr_oep = curr_oep[GEPT_headers].astype(GEPT_dtype)
            for col, cats in categories.items():
                curr_oep[col] = curr_oep[col].cat.set_categories(cats)
            ep_chunks.append(curr_oep)

        if aep:
            curr_aep = curr_ep.groupby(by=["GroupPeriod", "SummaryId"], as_index=False).agg({'Loss': 'max'})
            curr_aep['ReturnPeriod'] = max_group_period / curr_aep.groupby(by='SummaryId')['Loss'].rank(method='first', ascending=False)
            curr_aep['EPType'] = 3
            curr_aep[chunk_cols] = idx_val
            curr_aep = curr_aep[GEPT_headers].astype(GEPT_dtype)
            for col, cats in categories.items():
                curr_aep[col] = curr_aep[col].cat.set_categories(cats)
            ep_chunks.append(curr_aep)

    return pd.concat(ep_chunks).astype(GEPT_dtype)
