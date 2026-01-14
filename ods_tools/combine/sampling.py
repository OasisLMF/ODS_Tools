from ods_tools.oed.common import OdsException
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import norm
import logging

from ods_tools.combine.common import DEFAULT_RANDOM_SEED
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
        analysis_outpusets = [(a, os) for a in filtered_analyses for os in a.outputsets]

        for a, os in analysis_outpusets:
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
        gplt = pd.concat([gplt, _gplt])

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
        missing_summary_ids = gplt_fragment["SummaryId"].isna()
        if not gplt_fragment[missing_summary_ids].empty:
            logger.info(f"Output set {outputset_id} has {missing_summary_ids.sum()} missing group events.")
        gplt_fragment = gplt_fragment[~missing_summary_ids]

        # do summary id mapping
        if group.summaryinfo_map is not None:
            summaryid_map = group.summaryinfo_map.get(outputset_id, None)
        else:
            summaryid_map = None

        if summaryid_map is not None:
            gplt_fragment["SummaryId"] = (gplt_fragment["SummaryId"].map(summaryid_map)
                                                                    .fillna(gplt_fragment["SummaryId"]))

        gplt_fragments.append(gplt_fragment)

    return pd.concat(gplt_fragments).astype(gplt_dtype)[gplt_dtype.keys()]


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
