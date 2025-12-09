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
import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

# %% [markdown]
# The input files are multiple runs of PiWind.

# %% specify input ORD dirs
parent_path = Path().absolute() / 'piwind-ord'
# parent_path = Path().absolute() / 'piwind-ord'

ord_output_dirs = [parent_path / "split/1/runs/losses-20251201164501/output/",
                   parent_path / "split/2/runs/losses-20251201164618/output/"]

# %%
# specify directory for outputs

output_dir = Path("./combined_ord-" + datetime.now().strftime("%d%m%y%H%M%S"))
output_dir.mkdir(exist_ok=True)
print(f'Output Path: {output_dir}')

# %% [markdown]
# ## 1. Load and Group
# ### Creating Analysis and OutputSet
# In this section we create the objects required prior to grouping, namely:
# - Analysis table which contains the meta data from the analyses
# - OutputSet table which contains references to the ORD results.
#
#
# The `analysis_settings.json` files for each ORD analysis are parsed to read the Analysis and OutputSet tables.

# %%
from ord_combining.outputset import load_analysis_and_outputsets
from ord_combining.common import dataclass_list_to_dataframe

analysis, outputsets = load_analysis_and_outputsets(ord_output_dirs)

# Convert to dict / df for remainder of notebook
analysis = {a.id: a for a in analysis}
outputsets_df = dataclass_list_to_dataframe(outputsets)

outputsets_df['id'] = outputsets_df.index  # set id col

# %%
outputsets_df

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
from ord_combining.groupeventset import generate_group_set, generate_group_event_set
group_event_set_fields = ['event_set_id', 'event_occurrence_id', 'model_supplier_id']

group_set, group_output_set = generate_group_set(outputsets_df)
event_occurrence_set_df, event_occurrence_set_analysis = generate_group_event_set(analysis, group_event_set_fields)

# %%
group_output_set

# %%
group_set

# %%
event_occurrence_set_df

# %%
event_occurrence_set_analysis

# %% [markdown]
# Once the groups have been assigned the SummaryId is aligned within each group_set.
# To do so we find each unique grouping of summary level fields in each group set and aggregate the tiv by summing.
# Then we produce a `outputset_summary_id_map` which contains dicts which maps
# the summary_id of the ORD files to the group `SummaryId` indexed by a key
# value of `output_set_id`.
# Note only adds mapping where summary_id != SummaryId
#
# To demo this swapped LocNumber for summary_id 1 and 2 in SummaryLevel 2


# %%
from ord_combining.summaryinfo import load_summary_info, assign_summary_ids, generate_summary_id_map
os_summary_info = load_summary_info(analysis, outputsets_df)
group_set_summary_info = assign_summary_ids(group_output_set, os_summary_info)

# %%

outputset_summary_id_map = generate_summary_id_map(os_summary_info, group_set_summary_info, group_output_set)

outputset_summary_id_map

# %%
# save outputs
with open(output_dir / 'analysis.json', 'w') as f:
    _analysis_dict = {key: asdict(value) for key, value in analysis.items()}
    json.dump(_analysis_dict, f, indent=4)

with open(output_dir / 'group_output_set.json', 'w') as f:
    json.dump(group_output_set, f, indent=4)

group_set.to_csv(output_dir / 'group_set.csv')
event_occurrence_set_analysis.to_csv(output_dir / 'group_event_set_analysis.csv', index=False)
event_occurrence_set_df.to_csv(output_dir / 'event_occurrence_set.csv', index=False)

outputsets_df.to_csv(output_dir / 'output_set.csv', index=False)

# Serialise summary-info
for gs, g_summary_info_df in group_set_summary_info.items():
    gs_info = group_set.loc[gs]
    summary_info_fname = f'{gs_info['perspective_code']}_GS{gs}_summary-info.csv'
    g_summary_info_df.to_csv(output_dir / summary_info_fname, index=False)


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
# The period information can be extracted from the header info of the `occurrence.bin` file.

# %%
from ord_combining.groupperiod import generate_group_periods

total_group_periods = 10000  # config: set by user

# %%
group_event_set_analysis = event_occurrence_set_analysis.rename(columns={'event_occurrence_set_id': 'group_event_set_id'})

group_period = generate_group_periods(group_event_set_analysis, analysis, total_group_periods)

group_period.head()

print('No. of group periods: ', len(group_period))
# %%
# save csv
group_period.to_csv(output_dir / 'group_period.csv', index=False)

# %% [markdown]
# ## 3. Loss Sampling
# The final step involves sampling losses for each event in the GroupPeriod.
# There are two types of loss sampling:
# - Mean only (only for MELT files)
# - Full uncertainty sampling
#
# The additional config options are demonstrated below. An example of a full config is:
#
# ```python
# loss_sampling_config = {
#     "group_mean": False, # mean only
#     "group_mean_type": 1,  # SampleType filter
#     "group_secondary_uncertainty": False,
#     "group_parametric_distribution": 'gamma',  # either gamma or beta
#     "group_format_priority": ["m", "q", "s"}
# }
# ```
#
# So far only `q` and `s` loss sampling are implemented. We output both mean only and full secondary uncertainty sampling below.

# %%
group_format_priority = ['s']

# %% [markdown]
# The first stage in loss sampling is generating the GroupPeriodQuantile table.

# %%
from ord_combining.losssampling import construct_gpqt

gpqt = construct_gpqt(group_period, group_event_set_analysis, outputsets_df, analysis)

# %%
# save gpqt
gpqt.to_csv(output_dir / "gpqt.csv", index=False)

# %% [markdown]
# Finally the loss sampling can be done to produce the group period loss table (GPLT).

# %%
from ord_combining.losssampling import do_loss_sampling_full_uncertainty, do_loss_sampling_mean_only

# %%
# secondary uncertainty sampling
gplt_full = do_loss_sampling_full_uncertainty(gpqt, outputsets_df,
                                              group_output_set, analysis,
                                              priority=group_format_priority,
                                              outputset_summary_id_map=outputset_summary_id_map,
                                              output_dir=output_dir)

gplt_full.head()

# %%
# mean only sampling
gplt_mean = do_loss_sampling_mean_only(gpqt, outputsets_df, group_output_set, analysis,
                                       outputset_summary_id_map=outputset_summary_id_map)

gplt_mean.head()

# %% [markdown]
#
# ## 4. Output Generation
# The output options are:
# - Group Period Loss Table (GPLT)
#   - full (all group_set_id) <-- current implementation
#   - file based (each group_set_id in new file) <-- probably better
# - Group Average Loss Table (GALT)
# - Group Exceedance Probability Table (GEPT)

# %% [markdown]
# ### GPLT output

# %%
sort_cols = ['group_set_id', 'output_set_id', 'SummaryId', 'GroupPeriod']
gplt_full.sort_values(by=sort_cols).to_csv(output_dir / "gplt_full.csv", index=False)
gplt_mean.sort_values(by=sort_cols).to_csv(output_dir / "gplt_mean.csv", index=False)

# %%
from ord_combining.grouped_output import generate_al, generate_ep


def save_output(full_df, output_dir, output_name, factor_col='group_set_id', float_format='%.6f'):
    for i in full_df[factor_col].unique():
        save_path = output_dir / f'{i}_{output_name}'
        full_df.query(f"{factor_col} == {i}").to_csv(save_path, index=False,
                                                     float_format=float_format)
        print('Saved: ', save_path)


# %% [markdown]
# ### GALT Output
#
# %%
dtypes_aal = {
    'group_set_id': 'int',
    'SummaryId': 'int',
    'LossType': 'int',
    'Mean': 'float',
    'Std': 'float'
}

aal_full = generate_al(gplt_full, total_group_periods).astype(dtypes_aal)
aal_mean = generate_al(gplt_mean, total_group_periods).astype(dtypes_aal)

save_output(aal_full, output_dir, 'aal_full.csv')
save_output(aal_mean, output_dir, 'aal_mean.csv')

# %% [markdown]
# ### GEPT Output

# %%
dtypes_ep = {
    'group_set_id': 'int',
    'SummaryId': 'int',
    'EPCalc': 'int',
    'EPType': 'int',
    'RP': 'float',
    'Loss': 'float'
}
ep_full_df = generate_ep(gplt_full, total_group_periods, oep=True, aep=True).astype(dtypes_ep)
ep_mean_df = generate_ep(gplt_mean, total_group_periods, oep=True, aep=True).astype(dtypes_ep)

save_output(ep_full_df, output_dir, 'ep_full.csv')
save_output(ep_mean_df, output_dir, 'ep_mean.csv')
