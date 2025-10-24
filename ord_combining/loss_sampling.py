'''
Python file to demonstrate loss sampling
'''

# %% imports

import pandas as pd
import json
from pathlib import Path
import numpy as np
from scipy.special import betaincinv

from ord_combining.common import Analysis

rng = np.random.default_rng(12345)
# %% read input data

output_path = Path("/home/vinulw/code/ODS_Tools/ord_combining/outputs/")

with open(output_path / 'analysis.json', 'r') as f:
    analysis = json.load(f)

analysis_dict = {a['id']: Analysis(**a) for a in analysis}
group_event_set_analysis_df = pd.read_csv(output_path / 'group_event_set_analysis.csv')
event_occurrence_set_df = pd.read_csv(output_path / 'event_occurrence_set.csv')
group_period_df = pd.read_csv(output_path / 'group_period.csv')
output_set_df = pd.read_csv(output_path / 'output_set.csv' )

config = {
        'mean_only': False
        }

# %% generate group period quantile table
def load_ord_lt(analysis, occurence_file=False,
                format_priority=['M', 'Q', 'S'],
                summary_id = None, perspective = 'gul'):
    assert occurence_file == False, 'Occurrence file loading not yet implemented'
    analysis_dir = Path(analysis.path)

    for fmt in format_priority:
        glob_str = f'*{perspective}'
        if summary_id is not None:
            glob_str += f'*S{summary_id}'
        glob_str += f'*{fmt.lower()}plt.csv'

        output = list(analysis_dir.glob(glob_str))

        if output:
            return output

    return None

# %% Loading period info

analysis_dict.keys()

def load_loss_table_paths(analysis, summary_id, perspective, output_type):
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
    glob_str = f'*{perspective}*S{summary_id}*{output_type}.csv'
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


# %% ## generate quantiles

def construct_gpqt(group_period_df, group_event_set_analysis_df, output_set_df, mean_only=False):

    gpqt_fragments = []

    for group_event_set_id in group_period_df['group_event_set_id'].unique():
        filtered_group_period = group_period_df.query(f'group_event_set_id == {group_event_set_id}')

        filtered_analyses = group_event_set_analysis_df.query(f'group_event_set_id == {group_event_set_id}')['analysis_id']

        filtered_output_set = output_set_df.loc[output_set_df['analysis_id'].isin(filtered_analyses)]

        for id, curr_os in filtered_output_set.iterrows():

            print(f'Currently processing group_event_set_id: {group_event_set_id},  outputset: {id}')

            selected_analysis = analysis_dict[curr_os['analysis_id']]

            plt_paths = load_loss_table_paths(selected_analysis,
                                              summary_id=curr_os['exposure_summary_level_id'],
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

    return pd.concat(gpqt_fragments)


gpqt = construct_gpqt(group_period_df, group_event_set_analysis_df, output_set_df, config.get('mean_only', False))

gpqt.to_csv(output_path / "group_period_quantile_1.csv", index=False)

gpqt_dtype = {
        'GroupPeriod': np.int32,
        'Period': np.int32,
        'group_event_set_id': np.int32,
        'EventId': np.int32,
        'Quantile': np.float32,
        'output_set_id': np.int32,
        }

gpqt.astype(gpqt_dtype)

# %% loss sampling MELT

def beta_sampling_group_loss(df):
    df['mu'] = df['MeanLoss'] / df['MaxLoss'] # TODO need to verify form of this - also should we use MaxImpactedExposure as outlined in joh's sheet?
    df['sigma'] = df['SDLoss'] / df['MaxLoss']

    df['alpha'] = df['mu'] * (df['mu'] * (1 - df['mu']) / (df['sigma'] ** 2) - 1)
    df['beta'] = df['alpha'] * (1 / df['mu'] - 1)

    df_filter = df[['alpha', 'beta']].notna().all(axis='columns')

    df.loc[df_filter, 'Loss'] = df.loc[df_filter, 'MaxLoss']*betaincinv(df[df_filter]['alpha'], df[df_filter]['beta'], df[df_filter]['Quantile'])

    df.loc[~df_filter, 'Loss'] = df.loc[~df_filter, 'MeanLoss'] # default to MeanLoss

    df = df.drop(columns=['alpha', 'beta', 'mu', 'sigma'])

    return df

def loss_sample_mean_only(gpqt, elt_paths):
    assert 'mplt' in elt_paths

    mplt_df = pd.read_csv(elt_paths['m']).query('SampleType == 2')
    mplt_df = mplt_df[["EventId", "MeanLoss"]]

    return gpqt.merge(mplt_df, on='EventId', how='left').rename(columns={"MeanLoss", "Loss"})


def do_loss_sampling_mean_only(gpqt, output_set_df, analysis_dict):
    loss_sample_fragments = []

    for output_set_id in gpqt['output_set_id'].unique():
        os = output_set_df.loc[output_set_id]
        analysis = analysis_dict[os['analysis_id']]

        elt_paths = load_loss_table_paths(analysis, summary_id=os['exposure_summary_level_id'],
                                          perspective=os['perspective_code'], output_type='elt')

        filtered_gpqt = gpqt.query(f'output_set_id == {output_set_id}')
        loss_sample_fragments.append(loss_sample_mean_only(filtered_gpqt, elt_paths))

    return pd.concat(loss_sample_fragments)


def merge_melt(gpqt, melt):
    _melt = melt[['EventId', 'MeanLoss', 'SDLoss', 'MaxLoss']]
    merged = gpqt.merge(_melt, on='EventId', how='outer')
    merged['not_merged'] = merged[['MeanLoss', 'SDLoss', 'MaxLoss']].isna().any(axis='columns')

    return merged

def read_melt(path):
    return pd.read_csv(path).query('SampleType == 2')

def read_qelt(path):
    return pd.read_csv(path)

def read_selt(path):
    return pd.read_csv(path)

loss_sampling_func_map = {
        'm': beta_sampling_group_loss
        }


def do_loss_sampling_full_uncertainty(gpqt, output_set_df, analysis_dict,
                                      priority=['m', 'q', 's']):
    '''Perform the full uncertainty loss sampling.'''
    loss_sample_fragments = []

    for output_set_id in gpqt['output_set_id'].unique():
        os = output_set_df.loc[output_set_id]
        analysis = analysis_dict[os['analysis_id']]

        elt_paths = load_loss_table_paths(analysis, summary_id=os['exposure_summary_level_id'],
                                          perspective=os['perspective_code'], output_type='elt')

        elt_dfs = {key: globals()[f'read_{key}'](value) for key, value in elt_paths.items()} # todo handle this better (lazy load)

        curr_gpqt = gpqt.query(f'output_set_id == {output_set_id}')

        original_cols = list(curr_gpqt.columns)

        loss_sampled_fragments = []
        for p in priority:
            elt_df = elt_dfs.get(f'{p}elt', None)

            if elt_df is None:
                continue

            merged = globals()[f'merge_{p}elt'](curr_gpqt, elt_df)

            assert p in loss_sampling_func_map, f'Loss sampling not definied for type: {p}'
            loss_sampled_df = loss_sampling_func_map[p](merged.query('not_merged == False'))
            loss_sampled_df = loss_sampled_df[original_cols + ['Loss']]

            loss_sample_fragments.append(loss_sampled_df)

            curr_gpqt = curr_gpqt[merged['not_merged']]

            if curr_gpqt.empty:
                print(f"curr_gpqt is empty, exiting at {p}")
                break

        if not curr_gpqt.empty:
            print('Could not perform loss sampling for all events.')
            print('Missed events: ')
            print(curr_gpqt['EventId'])

        return pd.concat(loss_sample_fragments)


output = do_loss_sampling_full_uncertainty(gpqt, output_set_df, analysis_dict)
output
