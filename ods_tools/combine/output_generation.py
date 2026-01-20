import pandas as pd
import numpy as np

from ods_tools.combine.common import oasis_float

alt_dtype = {
    'groupset_id': 'i4',
    'SummaryId': 'i4',
    'LossType': 'i4',
    'Mean': oasis_float,
    'Std': oasis_float

}

ept_dtype = {
    'groupset_id': 'i4',
    'SummaryId': 'i4',
    'EPCalc': 'i4',
    'EPType': 'i4',
    'RP': oasis_float,
    'Loss': oasis_float
}


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
            "Mean": mean_loss,
            "Std": std_loss
        }

        records.append(record)

    return pd.DataFrame(records).astype(alt_dtype)


def assign_exceedance_probability(df, max_period):
    original_cols = list(df.columns)
    df["rank"] = (df.groupby(by=["groupset_id", "SummaryId", "EPCalc"], as_index=False)["Loss"]
                  .rank(method="first", ascending=False))
    df["RP"] = max_period / df["rank"]
    return df[original_cols + ["RP"]]


def generate_ept(gplt, max_group_period, oep=True, aep=True):
    ep_groups = (
        gplt.rename(columns={"LossType": "EPCalc"})  # check if this is the correct type
        .groupby(by=["groupset_id", "groupeventset_id",
                     "EventId", "GroupPeriod", "SummaryId",
                     "EPCalc"], as_index=False)
    )
    grouped_df = ep_groups["Loss"].agg("sum")
    grouped_df = grouped_df.groupby(by=["groupset_id", "SummaryId", "GroupPeriod", "EPCalc"], as_index=False)

    ep_frags = []
    if oep:
        oep_df = (
            grouped_df.pipe(lambda gp: gp["Loss"].max())
            .pipe(assign_exceedance_probability, max_period=max_group_period)
            .pipe(lambda x: x.assign(EPType=1))  # todo check OEP TVAR EPCalc 2
        )

        ep_frags.append(oep_df)

    if aep:
        aep_df = (
            grouped_df.pipe(lambda gp: gp["Loss"].sum())
            .pipe(assign_exceedance_probability, max_period=max_group_period)
            .pipe(lambda x: x.assign(EPType=3))  # todo check AEP TVAR EPCalc 4
        )
        ep_frags.append(aep_df)

    return (
        pd.concat(ep_frags)[["groupset_id", "SummaryId", "EPCalc", "EPType", "RP", "Loss"]]
        .astype(ept_dtype)
        .sort_values(by=["groupset_id", "SummaryId", "EPType", "EPCalc", "Loss"],
                     ascending=[True, True, True, True, False])
    )
