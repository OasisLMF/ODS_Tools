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

analysis = [Analysis(**a) for a in analysis]

print("Loaded analyses: ")
for a in analysis:
    print(a.id)

group_event_set_analysis_df = pd.read_csv(output_path / 'group_event_set_analysis.csv')
event_occurrence_set_df = pd.read_csv(output_path / 'event_occurrence_set.csv')

group_event_set_analysis_df
event_occurrence_set_df

selected_group_event_set = 1

selected_group_event_set = group_event_set_analysis_df[group_event_set_analysis_df['group_event_set_id'] == selected_group_event_set]

selected_analysis_ids = selected_group_event_set['analysis_id'].to_list()
selected_analysis = [a for a in analysis if a.id in selected_analysis_ids]

print("Selected analysis: ")
for a in selected_analysis:
    print(a.id)

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
    for analysis in selected_analysis:
        plt_file = load_plt_file(analysis)
        if plt_file is None:
            continue

        plt_df = pd.read_csv(plt_file)
        period_table_fragments += [plt_df['Period'].drop_duplicates()]

    return pd.concat(period_table_fragments).drop_duplicates().reset_index(drop=True)

periods = load_analysis_periods(selected_analysis)

# %% generate group periods

def generate_group_periods(periods, max_period, max_group_periods):
    # generate the group cycle slices
    group_cycles = max_group_periods // max_period
    group_slices = [(i*max_period, (i+1) * max_period) for i in range(group_cycles)]

    if group_cycles * max_period < max_group_periods:
        group_slices += [(group_cycles * max_period, max_group_periods)]

    group_period_fragments = []
    for slice in group_slices:
        shuffled_slice = np.arange(slice[0], slice[1])
        shuffle(shuffled_slice)
        shuffle_filter = min(len(shuffled_slice), len(periods))

        group_period_fragments.append(pd.DataFrame({'GroupPeriod':
                                                     shuffled_slice[:shuffle_filter],
                                                     'Period':
                                                     periods[:shuffle_filter]}))

    return pd.concat(group_period_fragments)


max_periods = 25
max_group_periods = 100
_periods = [2, 10, 12, 15, 21]
group_period = generate_group_periods(_periods, max_periods, max_group_periods)

_periods
group_period = group_period.sort_values(by='GroupPeriod')

for i in range(4):
    group_slice = (i*max_periods, (i+1)*max_periods)
    print(f"Cycle {i}")
    print(group_period[(group_period["GroupPeriod"] > group_slice[0]) & (group_period["GroupPeriod"] < group_slice[1])])
