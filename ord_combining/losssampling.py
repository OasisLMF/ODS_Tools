import pandas as pd
import numpy as np
from pathlib import Path
from scipy.special import betaincinv
from tqdm import tqdm

from ord_combining.ordhandling import merge_melt, merge_qelt, merge_selt, read_melt, read_qelt, read_selt

rng = np.random.default_rng(12345)

gpqt_dtype = {
        'GroupPeriod': np.int32,
        'Period': np.int32,
        'group_event_set_id': np.int32,
        'EventId': np.int32,
        'Quantile': np.float32,
        'output_set_id': np.int32,
        }


gplt_dtype = {
        'GroupPeriod': np.int32,
        'Period': np.int32,
        'group_event_set_id': np.int32,
        'EventId': np.int32,
        'output_set_id': np.int32,
        'Loss': np.float64,
        'LossType': 'Int8'
        }


def construct_gpqt(group_period_df, group_event_set_analysis_df, output_set_df, analysis,
                   mean_only=False):

    gpqt_fragments = []

    for group_event_set_id in group_period_df['group_event_set_id'].unique():
        filtered_group_period = group_period_df.query(f'group_event_set_id == {group_event_set_id}')

        filtered_analyses = group_event_set_analysis_df.query(f'group_event_set_id == {group_event_set_id}')['analysis_id']

        filtered_output_set = output_set_df.loc[output_set_df['analysis_id'].isin(filtered_analyses)]

        for id, curr_os in filtered_output_set.iterrows():

            print(f'Currently processing group_event_set_id: {group_event_set_id},  outputset: {id}')

            selected_analysis = analysis[curr_os['analysis_id']]

            plt_paths = load_loss_table_paths(selected_analysis,
                                              summary_level_id=curr_os['exposure_summary_level_id'],
                                              perspective=curr_os['perspective_code'],
                                              output_type='plt')

            period_eventid = load_period_eventid_from_plt(plt_paths)

            if period_eventid is None:
                raise Exception(f"No period files found for this outputset: {curr_os['id']}.")

            _gpqt_fragment = filtered_group_period.merge(period_eventid, on='Period',
                                                         how='inner')


            _gpqt_fragment['output_set_id'] = curr_os['id']

            if mean_only:
                _gpqt_fragment['Quantile'] = None
            else:
                _gpqt_fragment['Quantile'] = rng.random(size=len(_gpqt_fragment))

            gpqt_fragments.append(_gpqt_fragment)

    gpqt = pd.concat(gpqt_fragments)
    return gpqt.astype(gpqt_dtype)

def load_loss_table_paths(analysis, summary_level_id, perspective, output_type):
    '''Load loss table paths to of type `output_type` from ord output directory of the selected
    analysis, summary_id and perspective.

    Args
    ----
    analysis (Analysis): Analysis info object
    output_type (str): Either `plt`, `elt`, or `lt` (for both)
    summary_id (int)
    perspective (str): Either `gul`, `il`, or `ri`
    '''

    analysis_dir = Path(analysis.path)
    glob_str = f'*{perspective}*S{summary_level_id}*{output_type}.csv'
    output = list(analysis_dir.glob(glob_str))
    output = {path.stem.split('_')[-1]: path for path in output}

    return output

def load_period_eventid_from_plt(plt_paths, priority=['m', 'q', 's']):
    '''Load period eventid info from plt files.
    '''
    period_eventid_frags = []
    for p in priority:
        curr_path = plt_paths.get(f'{p}plt', None)

        if curr_path is None:
            continue

        curr_df = pd.read_csv(curr_path)
        curr_df = curr_df[['Period', 'EventId']].drop_duplicates()

        period_eventid_frags.append(curr_df)

    if len(period_eventid_frags) == 0:
        return None

    return pd.concat(period_eventid_frags).drop_duplicates()


def loss_sample_mean_only(gpqt, elt_paths):
    assert 'mplt' in elt_paths

    mplt_df = pd.read_csv(elt_paths['mplt'])
    mplt_df = mplt_df[["SummaryId", "SampleType", "EventId", "MeanLoss", "SDLoss"]]

    mplt_dtypes = {
            "SummaryId": "Int64",
            "SampleType": "Int64",
            "EventId": "Int64",
            "MeanLoss": "float",
            "SDLoss": "float"
            }

    mplt_df = mplt_df.astype(mplt_dtypes)

    grouped_df = mplt_df.groupby(["SummaryId", "SampleType", "EventId"], as_index=False)
    mplt_df = grouped_df.agg({
            'SDLoss': lambda x: np.sqrt(np.sum(x**2)),
            'MeanLoss': 'sum'
        })

    mplt_df["LossType"] = mplt_df["SampleType"].where(mplt_df["SampleType"] == 1, 3)
    mplt_df = mplt_df[["SummaryId", "EventId", "MeanLoss", "SDLoss", "LossType"]]

    gplt = gpqt.merge(mplt_df, on='EventId', how='left').rename(columns={"MeanLoss": "Loss"})

    output_cols =["group_event_set_id", "GroupPeriod", "output_set_id",
                  "SummaryId", "EventId", "LossType", "Loss", "SDLoss"]
    return gplt[output_cols]


def do_loss_sampling_mean_only(gpqt, output_set_df, analysis_dict):
    gplt_fragments = []

    for output_set_id in gpqt['output_set_id'].unique():
        os = output_set_df.loc[output_set_id]
        analysis = analysis_dict[os['analysis_id']]

        elt_paths = load_loss_table_paths(analysis, summary_level_id=os['exposure_summary_level_id'],
                                          perspective=os['perspective_code'], output_type='mplt')

        filtered_gpqt = gpqt.query(f'output_set_id == {output_set_id}')
        gplt_fragments.append(loss_sample_mean_only(filtered_gpqt, elt_paths))

    return pd.concat(gplt_fragments)

## Loss sampling functions

def beta_sampling_group_loss(df):
    df['mu'] = df['MeanLoss'] / df['MaxLoss'] # TODO need to verify form of this - also should we use MaxImpactedExposure as outlined in joh's sheet?
    df['sigma'] = df['SDLoss'] / df['MaxLoss']

    df['alpha'] = df['mu'] * (df['mu'] * (1 - df['mu']) / (df['sigma'] ** 2) - 1)
    df['beta'] = df['alpha'] * (1 / df['mu'] - 1)

    df_filter = df[['alpha', 'beta']].notna().all(axis='columns')

    df.loc[df_filter, 'Loss'] = df.loc[df_filter, 'MaxLoss']*betaincinv(df[df_filter]['alpha'], df[df_filter]['beta'], df[df_filter]['Quantile'])

    df.loc[~df_filter, 'Loss'] = df.loc[~df_filter, 'MeanLoss'] # default to MeanLoss

    df = df.drop(columns=['alpha', 'beta', 'mu', 'sigma'])

    df['LossType'] = 2 # set loss type

    return df


def mean_loss_sampling(gpqt, melt, sampling_func=beta_sampling_group_loss):
    original_cols = list(gpqt.columns)
    merged = merge_melt(gpqt, melt)

    # sampling only works on SampleType 2
    loss_sampled_df = sampling_func(merged.query('SampleType ==2 & not_merged == False'))
    loss_sampled_df = loss_sampled_df[original_cols + ['Loss']]
    loss_sampled_df["LossType"] = 2

    remaining_gpqt = gpqt.loc[merged['not_merged']]
    return loss_sampled_df, remaining_gpqt


def quantile_loss_sampling(gpqt, qelt):
    original_cols = list(gpqt.columns)

    gpqt["merged"] = gpqt["EventId"].isin(qelt["EventId"].unique())
    remaining_gpqt = gpqt[~gpqt["merged"]][original_cols]

    summary_ids = qelt["SummaryId"].unique()
    sample_loss_frags = []
    curr_gpqt = gpqt[gpqt["merged"]]
    for summary_id in tqdm(summary_ids, desc="quantile loss sampling"):
        qelt_summary_id = qelt.query(f"SummaryId == {summary_id}")
        curr_loss_frag = curr_gpqt.apply(lambda x: quantile_single_row(x, qelt_summary_id), axis=1)
        curr_loss_frag["SummaryId"] = summary_id
        sample_loss_frags.append(curr_loss_frag)

    sample_loss_df = pd.concat(sample_loss_frags)
    sample_loss_df = sample_loss_df[original_cols + ["SummaryId", "Loss"]]
    sample_loss_df["LossType"] = 2

    return sample_loss_df, remaining_gpqt[original_cols]


def quantile_single_row(row, qelt): # todo speedup
    filtered_qelt = qelt.query(f"EventId == {row['EventId']}")
    previous_quantile = filtered_qelt[filtered_qelt["LTQuantile"] <= row["Quantile"]].iloc[-1]
    next_quantile = filtered_qelt[filtered_qelt["LTQuantile"] > row["Quantile"]].iloc[0]

    if row["Quantile"] == previous_quantile["LTQuantile"]:
        row["Loss"] = previous_quantile["QuantileLoss"]
        return row

    quantile_range = next_quantile["LTQuantile"] - previous_quantile["LTQuantile"]
    quantile_loss_range = next_quantile["QuantileLoss"] - previous_quantile["QuantileLoss"]
    row["Loss"] =  previous_quantile["QuantileLoss"] + (row["Quantile"] - previous_quantile["LTQuantile"])*quantile_loss_range / quantile_range

    return row


def sample_loss_sampling(gpqt, selt):
    original_cols = list(gpqt.columns)
    gpqt["merged"] = gpqt["EventId"].isin(selt["EventId"].unique())

    remaining_gpqt = gpqt[~gpqt["merged"]][original_cols]

    selt = selt.sort_values(by=['EventId', 'SampleLoss'], ascending=True)
    summary_ids = selt["SummaryId"].unique()

    curr_gpqt = gpqt[gpqt["merged"]]
    sample_loss_frags = []
    for summary_id in tqdm(summary_ids, desc="sample loss sampling"):
        selt_summary_id = selt.query(f"SummaryId == {summary_id}")
        curr_loss_frag = curr_gpqt.apply(lambda x: sample_single_row(x, selt_summary_id), axis=1)
        curr_loss_frag["SummaryId"] = summary_id
        sample_loss_frags.append(curr_loss_frag)

    sample_loss_df = pd.concat(sample_loss_frags)
    sample_loss_df = sample_loss_df[original_cols + ["SummaryId", "Loss"]]
    sample_loss_df["LossType"] = 2

    return sample_loss_df, remaining_gpqt

def sample_single_row(row, selt):
    selt_loss = selt.query(f"EventId == {row['EventId']}")["SampleLoss"].sort_values()
    row["Loss"] = selt_loss.iloc[int(row["Quantile"] * len(selt_loss))]
    return row


def do_loss_sampling_full_uncertainty(gpqt, output_set_df, analysis_dict,
                                      priority=['m', 'q', 's']):
    '''Perform the full uncertainty loss sampling.'''
    gplt_fragments = []
    loss_sampling_func_map = {
        'm': mean_loss_sampling,
        'q': quantile_loss_sampling,
        's': sample_loss_sampling
    }

    for output_set_id in gpqt['output_set_id'].unique():
        os = output_set_df.loc[output_set_id]
        analysis = analysis_dict[os['analysis_id']]

        elt_paths = load_loss_table_paths(analysis, summary_level_id=os['exposure_summary_level_id'],
                                          perspective=os['perspective_code'], output_type='elt')

        elt_dfs = {key: globals()[f'read_{key}'](value) for key, value in elt_paths.items()} # todo handle this better (lazy load)

        curr_gpqt = gpqt.query(f'output_set_id == {output_set_id}')

        for p in priority:
            print(f"Running outputset_id: {output_set_id}, priority: {p}")
            elt_df = elt_dfs.get(f'{p}elt', None)

            if elt_df is None:
                continue

            assert p in loss_sampling_func_map, f"Loss sampling {p} not defined."

            _gplt_fragment, curr_gpqt = loss_sampling_func_map[p](curr_gpqt, elt_df)
            gplt_fragments.append(_gplt_fragment)

            if curr_gpqt.empty:
                print(f"curr_gpqt is empty, exiting at {p}")
                break

        if not curr_gpqt.empty:
            print('Could not perform loss sampling for all events.')
            print('Missed events: ')
            print(curr_gpqt['EventId'])

    gplt = pd.concat(gplt_fragments)
    gplt.drop(columns=['Quantile'])

    return gplt.astype(gplt_dtype)
