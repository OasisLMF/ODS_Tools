import pandas as pd
from pathlib import Path

generated_path = Path('~/code/ODS_Tools/combined_ord-041225154521/').expanduser()
expected_path = Path('~/code/ODS_Tools/piwind-ord/full/runs/losses-20251202160403/output/').expanduser()

expected_slevel = 2
generated_slevel = 1


# %% Compare summary-info

expected_summary_info = pd.read_csv(expected_path / f'gul_S{expected_slevel}_summary-info.csv')
generated_summary_info = pd.read_csv(generated_path / f'gul_GS{generated_slevel}_summary-info.csv')

print(generated_summary_info)
print(expected_summary_info)

# Comparing ept tables

expected_ep = pd.read_csv(expected_path / f'gul_S{expected_slevel}_ept.csv')
generated_ep = pd.read_csv(generated_path / f'{generated_slevel}_ep_full.csv')

expected_ep = expected_ep.query('EPCalc==2 & (EPType == 1 | EPType == 3)')  # Only full uncertainty oep or aep
expected_ep.to_csv(expected_path / f'gul_S{expected_slevel}_ept_expected.csv', index=False)

expected_ep
generated_ep

# Comparing alt tables

expected_aal = pd.read_csv(expected_path / f'gul_S{expected_slevel}_palt.csv')
generated_aal_mean = pd.read_csv(generated_path / f'{generated_slevel}_aal_mean.csv')
generated_aal_full = pd.read_csv(generated_path / f'{generated_slevel}_aal_full.csv')

generated_aal_mean = generated_aal_mean.sort_values(by=['LossType', 'SummaryId'])
generated_aal_full = generated_aal_full.sort_values(by=['LossType', 'SummaryId'])

print('Mean ratio: ')
expected_aal['MeanLoss'] / generated_aal_mean.reset_index()['Mean']

print('Full ratio: ')
expected_aal.query('SampleType==1').reset_index()['MeanLoss'] / generated_aal_full['Mean']
