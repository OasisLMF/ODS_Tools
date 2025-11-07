# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
%load_ext autoreload
%autoreload 2

# %% [markdown]
# # Combining Results in ORD
#
# This notebook provides a proof of concept example for combining catastrophe
# loss model results in the Open Results Data (ORD) format. We follow the
# methodology outlined in *Combining_results_in_ORD_v1.1.pdf*.
#
# This notebook is split into the workflow sequence as follows:
#
# 1. Load and Group
# 2. Period Sampling
# 3. Loss Sampling
# 4. Output Preparation

# %%
# imports
from datetime import datetime
from pathlib import Path
import json
from dataclasses import asdict
import pandas as pd

# %%
# make sure relative imports work
# todo: remove after packaging
import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

# %%
# specify directory for outputs

output_dir = Path("./combined_ord-" + datetime.now().strftime("%d%m%y%H%M%S"))
output_dir.mkdir(exist_ok=True)
print(f"Output dir: {output_dir}")

# %% [markdown]
# ## 1. Load and Group
# ### Creating Analysis and OutputSet
# In this section we create the objects required prior to grouping, namely:
# - Analysis table which contains the meta data from the analyses
# - OutputSet table which contains references to the ORD results.
#
# The `Analysis` and `OutputSet` dataclasses are defined in `common.py`.
# Note that a single analysis can have multiple OutputSets.

# %%
from ord_combining.common import Analysis, OutputSet

# %%
?Analysis
# %%
?OutputSet

# %% [markdown]
# The `analysis_settings.json` files for each ORD analysis contains the necessary information to instantiate both the Analysis object and all the OutputSet objects assosiated with a given analysis.

# %%
from ord_combining.outputset import parse_analysis_settings
from ord_combining.common import dataclass_list_to_dataframe

# specify the ORD directory paths for each analysis
ord_output_dirs = [
                    "/home/vinulw/code/ODS_Tools/ord_combining/losses-20251017133750/output",
                    "/home/vinulw/code/ODS_Tools/ord_combining/losses-20251017134021/output",
                    "/home/vinulw/code/ODS_Tools/ord_combining/losses-20251021131718/output"
                   ]

ord_output_dirs = [Path(p) for p in ord_output_dirs]

def load_analysis_and_output_sets(ord_output_dirs):
    analysis_set = []
    output_sets = []
    analysis_id = 1
    for ord_dir in ord_output_dirs:
        analysis, _outputsets = parse_analysis_settings(ord_dir / 'analysis_settings.json')

        # set the uid for the analysis
        analysis.id = analysis_id
        for i in range(len(_outputsets)):
            _outputsets[i].analysis_id = analysis_id
        analysis_id += 1

        analysis_set.append(analysis)
        output_sets += _outputsets

    return analysis_set, output_sets

analysis, outputsets = load_analysis_and_output_sets(ord_output_dirs)
analysis = {a.id: a for a in analysis}
outputsets_df = dataclass_list_to_dataframe(outputsets)
outputsets_df['id'] = outputsets_df.index # set id col

# %%
outputsets_df.head()

# %%
outputsets_df.columns

# %% [markdown]
# ### Creating GroupEventSet
# The GroupEventSet are used to define common events, thereby allowing for a
# list of consistent unique events that can be used to create GroupPeriods.
#
# There is a config option `group_event_set_fields` which specifies which fields to use to specify the unique event.
#
# The EventOccurenceSet table contains the meta information for each event set based on the `group_event_set_fields`.

# %%
from ord_combining.groupeventset import generate_group_analysis, generate_group_event_set
group_event_set_fields = ['event_set_id', 'event_occurrence_id', 'model_supplier_id']

group_analysis = generate_group_analysis(outputsets_df) # intermediary table
event_occurrence_set_df, group_event_set_analysis = generate_group_event_set(analysis, group_analysis, group_event_set_fields)

# %%
event_occurrence_set_df.head()

# %%
group_event_set_analysis.head()

# %%
# save outputs
with open(output_dir / 'analysis.json', 'w') as f:
    _analysis_dict = {key: asdict(value) for key, value in analysis.items()}
    json.dump(_analysis_dict, f, indent=4)

group_event_set_analysis.to_csv(output_dir / 'group_event_set_analysis.csv', index=False)
event_occurrence_set_df.to_csv(output_dir / 'event_occurrence_set.csv', index=False)

outputsets_df.to_csv(output_dir / 'output_set.csv', index=False)

# %% [markdown]
# ## 2. Period Sampling
# Now that each analysis has been grouped, we need to generate the GroupPeriods
# into which the events are assigned to for the combined output.
#
# We extract the Period for a given GroupEventSet that has a loss causing event
# and the total number of Periods. These Periods are then assigned to the
# GroupPeriod randomly, and if the total number of GroupPeriods is larger than
# the total number of Period then the GroupEventSet periods are cycled.
#
# In the example below we have a GroupEventSet with 25 total periods and 5 loss
# causing events in periods 2, 10, 12, 15 and 21. The total number of GroupPeriods is 100.

# %%
from ord_combining.groupperiod import gen_group_periods_event_set_analysis

total_periods = 25
loss_periods = [2, 10, 12, 15, 21]
total_group_periods = 100

example_group_period = gen_group_periods_event_set_analysis(loss_periods,
                                                    max_period=total_periods,
                                                    max_group_periods=total_group_periods)

# %%
example_group_period = pd.concat(example_group_period)
example_group_period = example_group_period.sort_values(by='GroupPeriod').reset_index(drop=True)

for i in range(total_group_periods // total_periods):
    slice = (i*total_periods, (i+1) * total_periods)
    print(f"Current cycle: {i+1} : {slice}")
    print(example_group_period[(example_group_period['GroupPeriod'] >= slice[0]) &
                               (example_group_period['GroupPeriod'] < slice[1])])

# %% [markdown]
# The period information can be extracted from the PLT files or event
# occurrence file. In this instance we load the periods from the PLT files.

# %%
from ord_combining.groupperiod import generate_group_periods

total_periods = 1000 # from model
total_group_periods = 2000

# %%
group_period = generate_group_periods(group_event_set_analysis, analysis, total_periods, total_group_periods)


# %% [markdown]
# Questions:
# - Currently loading Periods from single PLT file in ORD. Should this actually read all PLT files?
# - How to load total periods from analysis settings (is it possible to have different total periods for individual grouped analyses)?
