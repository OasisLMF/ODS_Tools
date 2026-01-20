from ods_tools.oed.common import OdsException
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import norm
from scipy.special import betaincinv
import logging

from ods_tools.combine.common import DEFAULT_RANDOM_SEED
from ods_tools.combine import io
from ods_tools.combine.io import DEFAULT_OCC_DTYPE, load_melt, read_occurrence_bin, load_loss_table_paths

logger = logging.getLogger(__name__)

rng = np.random.default_rng(DEFAULT_RANDOM_SEED)  # todo implement configurable random seed

gpqt_dtype = {
    'GroupPeriod': np.int32,
    'Period': np.int32,
    'groupeventset_id': np.int32,
    'EventId': np.int32,
    'Quantile': np.float32,
    'outputset_id': np.int32,
}


gplt_dtype = {
    'groupset_id': np.int32,
    'outputset_id': np.int32,
    'SummaryId': "Int32",
    'GroupPeriod': np.int32,
    'Period': np.int32,
    'groupeventset_id': np.int32,
    'EventId': np.int32,
    'Loss': np.float64,
    'LossType': 'Int8'
}


# PERIOD SAMPLING


def generate_group_periods(group, max_group_periods,
                           occ_dtype=None):
    '''
    Performs period sampling for a specified `ResultGroup` with the set `max_group_periods`.

    Args:
        group (ResultGroup) : Group of ORD to generate group periods from.
        max_group_periods (int) : Maximum number of group periods.
        occ_dtype ( List[Tuple[str]] ) : dtype of records in occurrence files.

    Returns:
        group_periods (pd.DataFrame): The sampled group periods.
    '''
    group_period_fragments = []

    for groupeventset_id, curr_groupeventset in group.groupeventset.items():
        _analysis_ids = curr_groupeventset['analysis_ids']
        occurence_files = [group.analyses[a].path / 'input/occurrence.bin' for a in _analysis_ids]

        periods, max_periods = load_period_info(occurence_files, occ_dtype)
        curr_frag = gen_group_periods_event_set_analysis(periods,
                                                         max_period=max_periods,
                                                         max_group_periods=max_group_periods)
        curr_frag = pd.concat(curr_frag)
        curr_frag['groupeventset_id'] = groupeventset_id

        group_period_fragments.append(curr_frag)

    group_period = pd.concat(group_period_fragments)
    return group_period.sort_values(by=['groupeventset_id', 'GroupPeriod']).reset_index(drop=True)


def load_period_info(occ_paths, occ_dtype=None):
    '''
    Read period data for multiple analyses from the `occurrence.bin` files.

    Args:
        occ_paths (list[str]) : list of occurrence files to load periods from.
    '''
    periods = []
    max_periods = None

    for occ_path in occ_paths:
        occ_arr, _max_periods = read_occurrence_bin(occ_path, occ_dtype)
        periods += np.unique(occ_arr['period_no']).tolist()

        if max_periods is None:
            max_periods = _max_periods
        elif max_periods != _max_periods:
            raise Exception('Currently does not support different max_periods in a group.')

    return list(set(periods)), max_periods


def gen_group_periods_event_set_analysis(periods, max_period, max_group_periods):
    # generate the group cycle slices
    group_cycles = max_group_periods // max_period
    group_slices = [(i * max_period, (i + 1) * max_period) for i in range(group_cycles)]

    if group_cycles * max_period < max_group_periods:
        group_slices += [(group_cycles * max_period, max_group_periods)]

    group_period_fragments = []
    for slice in group_slices:
        shuffled_slice = np.arange(slice[0] + 1, slice[1] + 1)
        rng.shuffle(shuffled_slice)
        shuffle_filter = min(len(shuffled_slice), len(periods))

        group_period_fragments.append(pd.DataFrame({'GroupPeriod': shuffled_slice[:shuffle_filter],
                                                    'Period': periods[:shuffle_filter]}))
    return group_period_fragments

# LOSS SAMPLING

# Quantile Sampling


def generate_gpqt(group_period, group, no_quantile_sampling=False, correlation=None):
    '''
    Create the group period quantile table.

    Args:
        group_period (pd.DataFrame) : Group Period dataframe.
        group (ResultGroup) : Group object containig ORD info.
        no_quantile_sampling (bool) : If true, no quantiles sampled.
        correlation (float) : Correlation parameter

    Returns:
        gpqt (pd.DataFrame) : Group Period Quantile Table
    '''

    gpqt_fragments = []

    for groupeventset_id, groupeventset in group.groupeventset.items():
        filtered_group_period = group_period.query(f'groupeventset_id == {groupeventset_id}')
        filtered_analyses = [group.analyses[a] for a in groupeventset['analysis_ids']]

        analysis_outputsets = []
        for a in filtered_analyses:
            for os_id in group.analysis_outputset[a.id]:
                os = group.outputsets[os_id]
                analysis_outputsets.append((a, os))

        for a, os in analysis_outputsets:
            # print(f'Currently processing groupeventset_id: {groupeventset_id},  outputset: {os.id}')

            eventid_period, _ = read_occurrence_bin(Path(a.path) / 'input' / 'occurrence.bin', DEFAULT_OCC_DTYPE)

            eventid_period = (pd.DataFrame(eventid_period)[['event_id', 'period_no']]
                              .drop_duplicates(ignore_index=True)
                              .rename(columns={'period_no': 'Period',
                                               'event_id': 'EventId'})
                              )

            _gpqt_fragment = filtered_group_period.merge(eventid_period, on='Period',
                                                         how='inner')

            _gpqt_fragment['outputset_id'] = os.id

            gpqt_fragments.append(_gpqt_fragment)

    gpqt = pd.concat(gpqt_fragments).reset_index(drop=True)

    gpqt = calculate_quantiles(gpqt, no_quantile_sampling, correlation)
    return gpqt.astype(gpqt_dtype)


def calculate_quantiles(gpqt, no_quantile_sampling=False, correlation=None):
    '''
    Calculat gpqt Quantiles, handling partial / full correlations.
    '''
    if no_quantile_sampling:
        gpqt['Quantile'] = None
        return gpqt

    if correlation is None or correlation == 0.0:  # uncorrelated
        gpqt['Quantile'] = rng.random(size=len(gpqt))
        return gpqt

    output_cols = list(gpqt.columns) + ['Quantile']

    correlated = gpqt.drop(columns=['outputset_id']).drop_duplicates()
    merge_cols = list(correlated.columns)

    if correlation == 1.:  # fully correlated
        correlated['Quantile'] = rng.random(size=len(correlated))
        return gpqt.merge(correlated, on=merge_cols)

    # partial correlations
    correlated['correlated'] = rng.normal(size=len(correlated))
    gpqt = gpqt.merge(correlated, on=merge_cols)

    gpqt['uncorrelated'] = rng.normal(size=len(gpqt))
    gpqt['partial'] = (gpqt['correlated'] * np.sqrt(correlation)
                       + gpqt['uncorrelated'] * np.sqrt((1 - correlation))
                       )

    gpqt['Quantile'] = norm.cdf(gpqt['partial'])
    return gpqt[output_cols]

# Loss Sampling


def do_loss_sampling(gpqt,
                     group,
                     mean_only=False,
                     secondary_uncertainty=False,
                     parametric_distribution='beta',
                     format_priority=['M', 'Q', 'S']
                     ):
    '''
    Sample losses for grouped ORD.

    Args:
        gpqt (pd.DataFrame) : Group Period Quantile Table
        group (ResultGroup) : ORD results group
        mean_only (bool) : Do mean only loss sampling.
        secondary_uncertainry (bool) : Perform secondary uncertainty loss sampling.
        parametric_distribution (str) : The parameteric distribution used for sampling MELT files.
        format_priority (List[str]) : ELT file format priority order to load loss information. `M` = MELT, `Q` = QELT, `S` = SELT

    Returns:
        gplt (pd.DataFrame) : group period loss table
    '''
    if not mean_only and not secondary_uncertainty:
        logger.error("No loss sampling. Please set `group_mean` or `secondary_uncertainty`.")
        raise OdsException('No loss sampling specified.')

    gplt = None
    if mean_only:
        logger.info("Loss sampling: mean only")
        gplt = do_loss_sampling_mean_only(gpqt, group)

    if secondary_uncertainty:
        _gplt = do_loss_sampling_secondary_uncertainty(gpqt, group,
                                                       format_priority=format_priority,
                                                       parametric_distribution=parametric_distribution)

        gplt = _gplt if gplt is None else pd.concat([gplt, _gplt], ignore_index=True)

    return gplt.astype(gplt_dtype)[gplt_dtype.keys()]


def do_loss_sampling_mean_only(gpqt, group):
    """
    Calculate group period loss table using mean only. Requires MPLT in individual ORD results.

    Args:
        gpqt (pd.DataFrame) : Group Period Quantile Table
        group (ResultGroup) : ORD results group
    """
    gplt_fragments = []

    for outputset_id in gpqt['outputset_id'].unique():
        os = group.outputsets[outputset_id]
        analysis = group.analyses[os.analysis_id]

        elt_paths = load_loss_table_paths(analysis,
                                          summary_level_id=os.exposure_summary_level_id,
                                          perspective=os.perspective_code,
                                          output_type='elt')

        filtered_gpqt = gpqt.query(f'outputset_id == {outputset_id}')
        gplt_fragment = loss_sample_mean_only(filtered_gpqt, elt_paths)
        gplt_fragment['groupset_id'] = os.groupset_id

        # filter na summaryids (no eventid in elt file)
        gplt_fragment = _filter_missing_summaryids(gplt_fragment, outputset_id)

        # do summary id mapping
        gplt_fragment = apply_summaryid_map(gplt_fragment, outputset_id,
                                            group.summaryinfo_map)

        gplt_fragments.append(gplt_fragment)

    gplt = pd.concat(gplt_fragments, ignore_index=True)

    return gplt.astype(gplt_dtype)[gplt_dtype.keys()]


def _filter_missing_summaryids(df, outputset_id=None):
    missing_summary_ids = df["SummaryId"].isna()
    if not df[missing_summary_ids].empty:
        logger.info(f"Output set {outputset_id} has {missing_summary_ids.sum()} missing group events.")
    df = df[~missing_summary_ids].reset_index()

    return df


def apply_summaryid_map(df, outputset_id, summaryinfo_map):
    if summaryinfo_map is not None:
        summaryid_map = summaryinfo_map.get(outputset_id, None)
    else:
        summaryid_map = None

    if summaryid_map is not None:
        df["SummaryId"] = (df["SummaryId"].map(summaryid_map)
                           .fillna(df["SummaryId"]))

    return df


def loss_sample_mean_only(gpqt, elt_paths):
    assert 'melt' in elt_paths, 'Mean only can only be performed if melt files present.'

    melt_df = load_melt(elt_paths['melt'])

    grouped_df = melt_df.groupby(["SummaryId", "SampleType", "EventId"], as_index=False)
    melt_df = grouped_df.agg({'MeanLoss': 'sum'})

    melt_df["LossType"] = melt_df["SampleType"].where(melt_df["SampleType"] == 1, 3)
    melt_df = melt_df[["SummaryId", "EventId", "MeanLoss", "LossType"]]

    # GroupPeriod unique in gpqt
    gplt = gpqt.merge(melt_df, on='EventId', how='left').rename(columns={"MeanLoss": "Loss"})
    return gplt


def do_loss_sampling_secondary_uncertainty(gpqt, group,
                                           format_priority=['M', 'Q', 'S'],
                                           parametric_distribution='beta'
                                           ):
    """
    Calculate group period loss table using the secondary uncertainty loss sampling. Currently requires ELT files.

    Args:
        gpqt (pd.DataFrame) : group period quantile table
        group (ResultGroup) : ORD results group
        format_priority (List[str]) : ELT file format priority order to load loss information. `M` = MELT, `Q` = QELT, `S` = SELT
        parametric_distribution (str) : The parameteric distribution used for sampling MELT files.
    """
    gplt_fragments = []
    loss_sampling_func_map = {
        'M': mean_loss_sampling,
        'Q': quantile_loss_sampling,
        'S': sample_loss_sampling
    }

    n_outputsets = len(gpqt['outputset_id'].unique())
    count = 1

    for outputset_id in gpqt['outputset_id'].unique():
        logger.info(f'Running secondary unc loss sampling output_set_id: {outputset_id} - {count}/{n_outputsets}')
        count += 1

        os = group.outputsets[outputset_id]
        analysis = group.analyses[os.analysis_id]

        elt_paths = load_loss_table_paths(analysis,
                                          summary_level_id=os.exposure_summary_level_id,
                                          perspective=os.perspective_code,
                                          output_type='elt')

        elt_dfs = {key: getattr(io, f'load_{key}')(value) for key, value in elt_paths.items()}  # todo handle this better (lazy load)

        curr_gpqt = gpqt.query('outputset_id == @outputset_id').reset_index(drop=True)

        for p in format_priority:
            elt_df = elt_dfs.get(f'{p.lower()}elt', None)

            if elt_df is None:
                logger.warn(f"{p.lower()}elt not found for outputset_id {outputset_id}.")
                continue

            if p not in loss_sampling_func_map:
                raise NotImplementedError(f"loss sampling function for format {p}elt not implemented")

            _gplt_fragment, curr_gpqt = loss_sampling_func_map[p.upper()](curr_gpqt, elt_df)

            if _gplt_fragment is None:  # no fragment
                continue

            _gplt_fragment["groupset_id"] = os.groupset_id

            _gplt_fragment = _filter_missing_summaryids(_gplt_fragment, outputset_id)

            _gplt_fragment = apply_summaryid_map(_gplt_fragment, outputset_id,
                                                 group.summaryinfo_map)

            gplt_fragments.append(_gplt_fragment)

            if curr_gpqt.empty:  # finished processing
                break

    gplt = pd.concat(gplt_fragments, ignore_index=True)

    return gplt.astype(gplt_dtype)[gplt_dtype.keys()]


def beta_sampling_group_loss(df):
    df = df.copy()  # intermediate table for calc
    df['mu'] = df['MeanLoss'] / df['MaxLoss']  # TODO need to verify form of this - also should we use MaxImpactedExposure as outlined in joh's sheet?
    df['sigma'] = df['SDLoss'] / df['MaxLoss']

    df['alpha'] = df['mu'] * (df['mu'] * (1 - df['mu']) / (df['sigma'] ** 2) - 1)
    df['beta'] = df['alpha'] * (1 / df['mu'] - 1)

    df_filter = df[['alpha', 'beta']].notna().all(axis='columns')

    df.loc[df_filter, 'Loss'] = df.loc[df_filter, 'MaxLoss'] * \
        betaincinv(df.loc[df_filter, 'alpha'], df.loc[df_filter, 'beta'], df.loc[df_filter, 'Quantile'])

    df.loc[~df_filter, 'Loss'] = df.loc[~df_filter, 'MeanLoss']  # default to MeanLoss

    df = df.drop(columns=['alpha', 'beta', 'mu', 'sigma'])

    df['LossType'] = 2  # set loss type

    return df


def mean_loss_sampling(gpqt, melt, sampling_func=beta_sampling_group_loss):
    original_cols = list(gpqt.columns)

    # sampling only works on SampleType 2
    _melt = melt.query('SampleType==2')[['SummaryId', 'EventId', 'MeanLoss', 'SDLoss', 'MaxLoss']]

    merged = gpqt['EventId'].isin(_melt["EventId"])

    remaining_gpqt = gpqt[~merged].reset_index(drop=True)

    loss_sampled_df = gpqt[merged].merge(_melt, on='EventId', how='left')
    loss_sampled_df = sampling_func(loss_sampled_df)

    if loss_sampled_df.empty:
        return None, remaining_gpqt

    loss_sampled_df["LossType"] = 2
    loss_sampled_df = loss_sampled_df[original_cols + ['SummaryId', 'LossType', 'Loss']]

    return loss_sampled_df, remaining_gpqt


def quantile_loss_sampling(gpqt, qelt):
    original_cols = list(gpqt.columns)

    merged = gpqt["EventId"].isin(qelt["EventId"].unique())
    remaining_gpqt = gpqt[~merged].reset_index(drop=True)

    summary_ids = qelt["SummaryId"].unique()
    sample_loss_frags = []
    curr_gpqt = gpqt[merged]
    for summary_id in summary_ids:
        qelt_summary_id = qelt.query(f"SummaryId == {summary_id}")
        curr_loss_frag = quantile_loss_sampling__summary_id(curr_gpqt, qelt_summary_id)
        curr_loss_frag["SummaryId"] = summary_id
        sample_loss_frags.append(curr_loss_frag)

    sample_loss_df = pd.concat(sample_loss_frags)

    if sample_loss_df.empty:
        return None, remaining_gpqt

    sample_loss_df["LossType"] = 2
    sample_loss_df = sample_loss_df[original_cols + ["SummaryId", "LossType", "Loss"]]

    return sample_loss_df, remaining_gpqt[original_cols]


def quantile_loss_sampling__summary_id(gpqt, qelt):
    quantiles = sorted(qelt['LTQuantile'].unique().tolist())
    quantile_map = {q: i for i, q in enumerate(quantiles)}
    quantiles = [-1] + quantiles  # capture zero index
    quantile_labels = range(len(quantiles) - 1)

    loss_df = gpqt.copy().reset_index(drop=True)
    _qelt = qelt.copy()

    _qelt['quantile_idx'] = _qelt['LTQuantile'].replace(quantile_map).astype(int)
    loss_df['quantile_idx_right'] = pd.cut(loss_df['Quantile'], quantiles, labels=quantile_labels).astype(int)
    loss_df['quantile_idx_left'] = loss_df['quantile_idx_right'] - 1

    loss_df[['QuantileLossLeft', 'QuantileLeft']] = loss_df.merge(_qelt, left_on=['EventId', 'quantile_idx_left'], right_on=[
                                                                  'EventId', 'quantile_idx'], how='left')[['QuantileLoss', 'LTQuantile']]

    loss_df[['QuantileLossRight', 'QuantileRight']] = loss_df.merge(_qelt, left_on=['EventId', 'quantile_idx_right'], right_on=[
                                                                    'EventId', 'quantile_idx'], how='left')[['QuantileLoss', 'LTQuantile']]

    loss_df['Loss'] = (loss_df['Quantile'] - loss_df['QuantileLeft']) / (loss_df['QuantileRight'] - loss_df['QuantileLeft'])
    loss_df['Loss'] = loss_df['QuantileLossLeft'] + loss_df['Loss'] * (loss_df['QuantileLossRight'] - loss_df['QuantileLossLeft'])

    return loss_df


def quantile_single_row(row, qelt):  # todo speedup
    filtered_qelt = qelt.query(f"EventId == {row['EventId']}")
    previous_quantile = filtered_qelt[filtered_qelt["LTQuantile"] <= row["Quantile"]].iloc[-1]
    next_quantile = filtered_qelt[filtered_qelt["LTQuantile"] > row["Quantile"]].iloc[0]

    if row["Quantile"] == previous_quantile["LTQuantile"]:
        row["Loss"] = previous_quantile["QuantileLoss"]
        return row

    quantile_range = next_quantile["LTQuantile"] - previous_quantile["LTQuantile"]
    quantile_loss_range = next_quantile["QuantileLoss"] - previous_quantile["QuantileLoss"]
    row["Loss"] = previous_quantile["QuantileLoss"] + (row["Quantile"] - previous_quantile["LTQuantile"]) * quantile_loss_range / quantile_range

    return row


def sample_loss_sampling__summary_id(gpqt, selt, number_of_samples):
    loss_df = gpqt.copy()
    selt_loss = selt.copy()

    selt_by_eventid = selt_loss.groupby('EventId')

    missing_sampleids = selt_by_eventid.agg({'SampleId': 'count'}).rename(columns={'SampleId': 'count'})
    missing_sampleids['missing'] = number_of_samples - missing_sampleids['count']

    if missing_sampleids['count'].max() > number_of_samples:
        logger.error('sample id count greater than number of samples')

    loss_df = loss_df.merge(missing_sampleids[['missing']], how='left', left_on='EventId', right_index=True)
    loss_df['sample_idx'] = (loss_df['Quantile'] * number_of_samples).astype(int) - loss_df['missing']

    selt_loss['SampleLossRank'] = selt_by_eventid['SampleLoss'].rank(method='first').astype(int) - 1

    selt_loss = selt_loss[['EventId', 'SampleLossRank', 'SampleLoss']]
    loss_df = loss_df.merge(selt_loss, left_on=['EventId', 'sample_idx'],
                            right_on=['EventId', 'SampleLossRank'],
                            how='left')
    loss_df['Loss'] = loss_df['SampleLoss'].fillna(0.0)
    return loss_df


def sample_loss_sampling(gpqt, selt, number_of_samples=10):
    original_cols = list(gpqt.columns)
    merged = gpqt["EventId"].isin(selt["EventId"].unique())

    remaining_gpqt = gpqt[~merged][original_cols].reset_index(drop=True)

    # selt = selt.sort_values(by=['EventId', 'SampleLoss'], ascending=True)
    summary_ids = selt["SummaryId"].unique()

    curr_gpqt = gpqt[merged]
    sample_loss_frags = []
    for summary_id in summary_ids:
        selt_summary_id = selt.query(f"SummaryId == {summary_id}").reset_index(drop=True)
        curr_loss_frag = sample_loss_sampling__summary_id(curr_gpqt, selt_summary_id, number_of_samples)
        curr_loss_frag["SummaryId"] = summary_id
        sample_loss_frags.append(curr_loss_frag)

    sample_loss_df = pd.concat(sample_loss_frags)

    if sample_loss_df.empty:
        return None, remaining_gpqt

    sample_loss_df["LossType"] = 2
    sample_loss_df = sample_loss_df[original_cols + ["SummaryId", "LossType", "Loss"]]

    return sample_loss_df, remaining_gpqt
