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

# %% imports
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# %% [markdown]
#
# # Combining ORD - Output Comparison
#
# This notebook compares the combined results from running the split PiWind
# exposure to a run of PiwWind with the full exposure set.

# %%
# run options

runtype = 'qelt'
expected_summary_level = 1
generated_summary_level = expected_summary_level - 1

# %%
# Load files

assert runtype in ['qelt', 'selt'], 'Runtype not recognised'

if runtype == 'qelt':
    generated_path = Path('~/code/ODS_Tools/ord_combining/combined_ord-qelt/').expanduser()
else:
    generated_path = Path('~/code/ODS_Tools/ord_combining/combined_ord-selt/').expanduser()

expected_path = Path('/home/vinulw/code/ODS_Tools/ord_combining/piwind-ord/full/runs/losses-20251202160403/output').expanduser()

# %% [markdown]
# ### Compare summary-info

# %%
expected_summary_info = pd.read_csv(expected_path / f'gul_S{expected_summary_level}_summary-info.csv')
generated_summary_info = pd.read_csv(generated_path / f'gul_GS{generated_summary_level}_summary-info.csv')

# %%
if 'LocNumber' in generated_summary_info.columns:
    merged_df = generated_summary_info.merge(expected_summary_info, on='LocNumber',
                                             suffixes=['_generated', '_expected'])

    merged_df['tiv_diff'] = np.abs(merged_df['tiv_generated'] - merged_df['tiv_expected'])

    print(merged_df[['LocNumber', 'tiv_generated', 'tiv_expected', 'tiv_diff']])
else:
    print('Generated summary info: ')
    print(generated_summary_info)
    print('Expected summary info: ')
    print(expected_summary_info)

# %% [markdown]
# ### Compare ALT

# %%
expected_aal = pd.read_csv(expected_path / f'gul_S{expected_summary_level}_palt.csv')
generated_aal_mean = pd.read_csv(generated_path / f'{generated_summary_level}_aal_mean.csv')
generated_aal_full = pd.read_csv(generated_path / f'{generated_summary_level}_aal_full.csv')

generated_aal_mean = generated_aal_mean.sort_values(by=['LossType',
                                                        'SummaryId']).reset_index(drop=True)
generated_aal_full = generated_aal_full.sort_values(by=['LossType',
                                                        'SummaryId']).reset_index(drop=True)

# %%
# mean only
generated_aal_mean['expected_loss'] = expected_aal['MeanLoss']
generated_aal_mean['expected_diff'] = np.abs(generated_aal_mean['expected_loss'] - generated_aal_mean['Mean'])
generated_aal_mean['diff_percent'] = generated_aal_mean['expected_diff'] / generated_aal_mean['Mean'] * 100
generated_aal_mean

# %%
# secondary uncertainty
generated_aal_full['expected_loss'] = expected_aal.query('SampleType==2').reset_index()['MeanLoss']
generated_aal_full['expected_diff'] = np.abs(generated_aal_full['expected_loss'] - generated_aal_full['Mean'])
generated_aal_full['diff_percent'] = generated_aal_full['expected_diff'] / generated_aal_full['Mean'] * 100
generated_aal_full

# %% [markdown]
# ### Compare EP

# %%
# EP comparison options
mean_only = True
EPType = 1  # 1 = oep, 3 = aep

if mean_only:
    EP_Calc = 1
else:
    EP_Calc = 2

if EPType == 1:
    ep_ext = 'oep'
elif EPType == 3:
    ep_ext = 'aep'
else:
    raise Exception('EPType not supported')

total_group_periods = 10000

# %%
# load EP tables
mean_ext = 'mean' if mean_only else 'full'
dtypes_ep = {
    'group_set_id': 'int',
    'SummaryId': 'int',
    'EPCalc': 'int',
    'EPType': 'int',
    'RP': 'float',
    'Loss': 'float'
}

generated_ep = pd.read_csv(generated_path /
                           f'{generated_summary_level}_ep_{mean_ext}.csv').astype(dtypes_ep)

expected_ep = pd.read_csv(expected_path / f'gul_S{expected_summary_level}_ept.csv')

# %% [markdown]
# To compare EP tables we find the matching ReturnPeriods and compare + plot the EP tables.
# Note this is done for each SummaryId separately. The plots and merged ep tables are output in the desired output path.

# %%
# Output directory to save plots + comaprison df
output_dir = Path('~/code/ODS_Tools/ord_combining/compare_ep/').expanduser()

if not output_dir.exists():
    output_dir.mkdir(parents=True)

# %%
for summary_id in generated_ep['SummaryId'].unique():
    _generated_ep = generated_ep.query(f'SummaryId == {summary_id}')
    _expected_ep = expected_ep.query(f'SummaryId == {summary_id}')

# extract mean damage loss EP table
    mean_expected_ep = _expected_ep.query(f'EPCalc == {EP_Calc}')
    mean_generated_ep = _generated_ep.query(f'EPCalc == {EP_Calc}')

    ep_mean_expected_ep = mean_expected_ep.query(f'EPType=={EPType}')
    ep_mean_generated_ep = mean_generated_ep.query(f'EPType=={EPType}')

    # Aligning summary id
    filtered_oep_mean_generated_ep = ep_mean_generated_ep[ep_mean_generated_ep['RP'].isin(ep_mean_expected_ep['ReturnPeriod'])].reset_index(drop=True)

    filtered_oep_mean_expected_ep = ep_mean_expected_ep[ep_mean_expected_ep['ReturnPeriod'].isin(
        filtered_oep_mean_generated_ep['RP'])].reset_index(drop=True)

    to_merge_expected = filtered_oep_mean_expected_ep.filter(items=['ReturnPeriod', 'Loss'])
    merged_oep_mean_expected_ep = (filtered_oep_mean_generated_ep.filter(items=['RP', 'Loss'])
                                   .merge(to_merge_expected, left_on='RP', right_on='ReturnPeriod',
                                          suffixes=['_gen', '_expected']))

    merged_oep_mean_expected_ep['Loss_Diff'] = np.abs(merged_oep_mean_expected_ep['Loss_expected'] - merged_oep_mean_expected_ep['Loss_gen'])

    merged_oep_mean_expected_ep['Percent_Diff'] = merged_oep_mean_expected_ep['Loss_Diff'] / merged_oep_mean_expected_ep['Loss_gen'] * 100.0

    merged_oep_mean_expected_ep.to_csv(
        output_dir / f'{runtype}_{mean_ext}_{ep_ext}_gen_expected_merged_S{expected_summary_level}_id{summary_id}.csv', index=False)

    max_ret_period = ep_mean_expected_ep['ReturnPeriod'].max()
    gen_oep_in_range = ep_mean_generated_ep.query(f'RP <= {max_ret_period}')

    xlim = (0, 1000)
    ax = (gen_oep_in_range.rename(columns={'Loss': 'Generated Loss'})
          .plot('RP', 'Generated Loss', style=['--'], xlim=xlim,
                title=f'Comparing EP S{expected_summary_level} summary_id {summary_id}')
          )
    ep_mean_expected_ep.rename(columns={'Loss': 'Expected Loss'}).plot('ReturnPeriod', 'Expected Loss',
                                                                       ylabel='Loss', grid=True,
                                                                       xlim=xlim, ax=ax, style=[':'])
    plt.savefig(output_dir / f'{runtype}_{mean_ext}_{ep_ext}_{expected_summary_level}_id{summary_id}.png')
    plt.close()
