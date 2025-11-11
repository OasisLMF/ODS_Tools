import pandas as pd
from pathlib import Path

def summary_info_fname(row):
    "Return summary info file name from outputset row"
    return f'{row["perspective_code"]}_S{row["exposure_summary_level_id"]}_summary-info.csv'


def load_summary_info(analysis, outputsets):
    summary_info_dict = {}

    for _, row in outputsets.iterrows():
        summary_info_path = Path(analysis[row['analysis_id']].path) / summary_info_fname(row)
        summary_info_dict[row['id']] = pd.read_csv(summary_info_path)

    return summary_info_dict


def assign_summary_ids(group_output_set, outputset_summary_info):
    group_summary_info = {}

    ignored_cols = ['summary_id', 'tiv']

    for group_set_id, outputset_ids in group_output_set.groupby('group_set_id').groups.items():
        curr_group_summary_info = pd.concat([outputset_summary_info[os_id] for os_id in outputset_ids])
        summary_oed_cols = [c for c in curr_group_summary_info.columns.to_list() if c not in ignored_cols]
        curr_group_summary_info = curr_group_summary_info.groupby(summary_oed_cols, as_index=False).agg({'tiv': 'sum'})
        curr_group_summary_info['SummaryId'] = curr_group_summary_info.index + 1

        group_summary_info[group_set_id] = curr_group_summary_info

    return group_summary_info
