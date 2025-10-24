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

def construct_gpqt(group_period_df, group_event_set_analysis_df, output_set_df):

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

            period_event_df = plt_df[['Period', 'EventId']].drop_duplicates() # might want to do the outputset sampling calc here

            _gpqt_fragment = filtered_group_period.merge(period_event_df, on='Period',
                                                         how='inner')

            _gpqt_fragment['Quantile'] = rng.random(size=len(_gpqt_fragment))

            _gpqt_fragment['output_set_id'] = curr_os['id']
            _gpqt_fragment['output_type'] = plt_path.stem[-4]

            gpqt_fragments.append(_gpqt_fragment)

    return pd.concat(gpqt_fragments)

gpqt_dtype = {
        'GroupPeriod': np.int32,
        'Period': np.int32,
        'group_event_set_id': np.int32,
        'EventId': np.int32,
        'Quantile': np.float32,
        'output_set_id': np.int32
        }

gpqt = construct_gpqt(group_period_df, group_event_set_analysis_df, output_set_df)

gpqt.to_csv(output_path / "group_period_quantile_1.csv", index=False)


gpqt.astype(gpqt_dtype)
print(gpqt)

# %% loss sampling MELT

def perform_beta_parameterisation(df):
    df['mu'] = df['MeanLoss'] / df['MaxLoss'] # TODO need to verify form of this - also should we use MaxImpactedExposure as outlined in joh's sheet?
    df['sigma'] = df['SDLoss'] / df['MaxLoss']

    df['alpha'] = df['mu'] * (df['mu'] * (1 - df['mu']) / (df['sigma'] ** 2) - 1)
    df['beta'] = df['alpha'] * (1 / df['mu'] - 1)

    df_filter = df[['alpha', 'beta']].notna().all(axis='columns')

    df.loc[df_filter, 'sampled'] = df.loc[df_filter, 'MaxLoss']*betaincinv(df[df_filter]['alpha'], df[df_filter]['beta'], 0.5)

    df.loc[~df_filter, 'sampled'] = df.loc[~df_filter, 'MeanLoss'] # default to MeanLoss

    return df


def loss_sample_melt(gpqt, output_set_df, analysis):
    assert (gpqt['output_type'] == 'm').all()

    for output_set_id in gpqt['output_set_id'].unique():
        filtered_gpqt = gpqt.query(f'output_set_id == {output_set_id}')
        curr_os = output_set_df.loc[output_set_id]

        curr_analysis = analysis[curr_os['analysis_id']]

        plt_path = load_ord_lt(curr_analysis,
                             occurence_file=False,
                             summary_id=curr_os['exposure_summary_level_id'],
                             perspective=curr_os['perspective_code']
                             )[0]

        plt_df = pd.read_csv(plt_path)
        plt_df = plt_df.query("SampleType == 2")

        duplicated_df = plt_df[plt_df[['Period', 'EventId']].duplicated()]

        print(f"Current os: {output_set_id}")
        print(f"Path: {plt_path}")
        print(f"Duplicated: ")
        print(duplicated_df)
        print()

        plt_df = plt_df.query('SampleType == 2')

        plt_df = plt_df[['EventId', 'MeanLoss', 'SDLoss', 'MaxLoss']]

        plt_df = perform_beta_parameterisation(plt_df)

        return plt_df



output = loss_sample_melt(gpqt, output_set_df, analysis_dict)

output
