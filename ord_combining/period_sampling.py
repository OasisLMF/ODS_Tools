'''
Performing period sampling.
'''
# %% imports
from os import wait
import numpy as np
from numpy.random import shuffle
import pandas as pd
from pathlib import Path
import json

from ord_combining.common import Analysis

# %% load serialised object

output_path = Path("/home/vinulw/code/ODS_Tools/ord_combining/outputs/")

with open(output_path / 'analysis.json', 'r') as f:
    analysis = json.load(f)

analysis_dict = {a['id']: Analysis(**a) for a in analysis}
group_event_set_analysis_df = pd.read_csv(output_path / 'group_event_set_analysis.csv')
event_occurrence_set_df = pd.read_csv(output_path / 'event_occurrence_set.csv')

# %% extract plt files

def load_plt_file(analysis):
    '''
    Load a single plt file if it exists.
    '''

    output_path = Path(analysis.path)

    plt_file_priority = ["splt", "qplt", "mplt"]

    plt_files = []
    for plt_file_type in plt_file_priority:
        plt_files = list(output_path.glob(f"*{plt_file_type}.csv"))
        if plt_files:
            return sorted(plt_files, key=lambda x: x.name)[0]

    return None

def load_analysis_periods(selected_analyses):
    period_table_fragments = []
    for analysis in selected_analyses:
        plt_file = load_plt_file(analysis)
        if plt_file is None:
            continue

        plt_df = pd.read_csv(plt_file)
        period_table_fragments += [plt_df['Period'].drop_duplicates()]

    return pd.concat(period_table_fragments).drop_duplicates().reset_index(drop=True)


# %% generate group periods

def gen_group_periods_event_set_analysis(periods, max_period, max_group_periods):
    # generate the group cycle slices
    group_cycles = max_group_periods // max_period
    group_slices = [(i*max_period, (i+1) * max_period) for i in range(group_cycles)]

    if group_cycles * max_period < max_group_periods:
        group_slices += [(group_cycles * max_period, max_group_periods)]

    group_period_fragments = []
    for slice in group_slices:
        shuffled_slice = np.arange(slice[0] + 1, slice[1] + 1)
        shuffle(shuffled_slice)
        shuffle_filter = min(len(shuffled_slice), len(periods))

        group_period_fragments.append(pd.DataFrame({'GroupPeriod': shuffled_slice[:shuffle_filter],
                                                    'Period': periods[:shuffle_filter]}))
    return group_period_fragments

def generate_group_periods(group_event_set_analysis, analysis, max_periods, max_group_periods):
    group_period_fragments = []
    for event_set_id in group_event_set_analysis_df['group_event_set_id'].unique():
        print(f"Procseeing event_set_id: {event_set_id}")
        filtered_group_event_set = group_event_set_analysis_df[group_event_set_analysis_df['group_event_set_id'] == event_set_id]

        filtered_analysis_ids = filtered_group_event_set['analysis_id'].to_list()
        filtered_analysis = [analysis[a] for a in filtered_analysis_ids]

        periods = load_analysis_periods(filtered_analysis)
        print(f'len periods: {len(periods)}')
        print(f'max periods: {max(periods)}')
        print(f'min periods: {min(periods)}')

        curr_frag = gen_group_periods_event_set_analysis(periods,
                                                         max_period=max_periods,
                                                         max_group_periods=max_group_periods)
        curr_frag = pd.concat(curr_frag)
        curr_frag['group_event_set_id'] = event_set_id

        group_period_fragments.append(curr_frag)

    group_period = pd.concat(group_period_fragments)
    return group_period.sort_values(by=['group_event_set_id', 'GroupPeriod']).reset_index(drop=True)

# %% demo group period for single event set analysis id

max_periods = 5
max_group_periods = 10
periods = list(range(max_periods))

group_period = gen_group_periods_event_set_analysis(periods,
                                                         max_period=max_periods,
                                                         max_group_periods=max_group_periods)

group_period = pd.concat(group_period)
group_period = group_period.sort_values(by='GroupPeriod').reset_index(drop=True)

for i in range(max_group_periods // max_periods):
    slice = (i*max_periods, (i+1) * max_periods)
    print(f"Current cycle: {i+1} : {slice}")
    print(group_period[(group_period['GroupPeriod'] >= slice[0]) &
                       (group_period['GroupPeriod'] < slice[1])])


# %% run group periods

max_periods = 1000
max_group_periods = 2000
group_period = generate_group_periods(group_event_set_analysis_df, analysis_dict,
                       max_periods=max_periods,
                       max_group_periods=max_group_periods)
print("Generated full group period: ")
print(group_period)
group_period.to_csv(output_path / 'group_period.csv', index=False)
