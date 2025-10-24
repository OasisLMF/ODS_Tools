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
        'mean_only': True
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

            plt_path = load_ord_lt(selected_analysis,
                                 occurence_file=False,
                                 summary_id=curr_os['exposure_summary_level_id'],
                                 perspective=curr_os['perspective_code']
                                 )[0]

            plt_df = pd.read_csv(plt_path)

            plt_df = plt_df.query("SampleType == 2")

            period_event_df = plt_df[['Period', 'EventId', 'MeanLoss', 'SDLoss', 'MaxLoss']]

            _gpqt_fragment = filtered_group_period.merge(period_event_df, on='Period',
                                                         how='inner')


            _gpqt_fragment['output_set_id'] = curr_os['id']
            if mean_only:
                _gpqt_fragment['Quantile'] = None
                _gpqt_fragment['output_type'] = 'mean'
            else:
                _gpqt_fragment['Quantile'] = rng.random(size=len(_gpqt_fragment))
                _gpqt_fragment['output_type'] = plt_path.stem[-4]

            gpqt_fragments.append(_gpqt_fragment)

    return pd.concat(gpqt_fragments)


gpqt = construct_gpqt(group_period_df, group_event_set_analysis_df, output_set_df, config.get('mean_only', False))

gpqt.to_csv(output_path / "group_period_quantile_1.csv", index=False)

gpqt_dtype = {
        'GroupPeriod': np.int32,
        'Period': np.int32,
        'group_event_set_id': np.int32,
        'EventId': np.int32,
        'output_set_id': np.int32
        }

if 'Quantile' in gpqt:
    gpqt_dtype['Quantile'] = np.float32,

gpqt.astype(gpqt_dtype)
print(gpqt)

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

def mean_only_group_loss(df):
    df['Loss'] = df['MeanLoss']
    return df

def do_loss_sampling(gpqt):
    '''Perform the loss sampling on gpqt'''

    map_output_type_to_func = {
            'm': beta_sampling_group_loss,
            'mean': mean_only_group_loss
            }

    loss_sample_fragments = []
    for output_type in gpqt['output_type'].unique():
        print("Running output_type: ", output_type)
        loss_func = map_output_type_to_func.get(output_type, None)

        if loss_func is None:
            print(f"output_type: {output_type} not recognised.")
            raise Exception("Output type not recognised.")

        filtered_gpqt = gpqt.query(f'output_type == "{output_type}"')
        loss_sample_fragments.append(loss_func(filtered_gpqt))

    return pd.concat(loss_sample_fragments)

output = do_loss_sampling(gpqt)

output[['MeanLoss', 'Quantile', 'Loss']]
