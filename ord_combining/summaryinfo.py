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

    group_output_set_inv = {}
    for k, v in group_output_set.items():
        group_output_set_inv.setdefault(v, []).append(k)

    for group_set_id, outputset_ids in group_output_set_inv.items():
        curr_group_summary_info = pd.concat([outputset_summary_info[os_id] for os_id in outputset_ids])
        summary_oed_cols = [c for c in curr_group_summary_info.columns.to_list() if c not in ignored_cols]
        curr_group_summary_info = curr_group_summary_info.groupby(summary_oed_cols, as_index=False).agg({'tiv': 'sum'})
        curr_group_summary_info['SummaryId'] = curr_group_summary_info.index + 1

        group_summary_info[group_set_id] = curr_group_summary_info[['SummaryId'] + summary_oed_cols + ['tiv']]

    return group_summary_info

def generate_summary_id_map(outputset_summary_info, group_summary_info, group_output_set):
    """
    Create a map for each outputset which maps the input summary_id to the group SummaryId.
    """
    ignored_cols = ['SummaryId', 'summary_id']

    outputset_summary_id = {}
    for output_set_id, group_set_id in group_output_set.items():
        curr_group_summary_info = group_summary_info[group_set_id].drop(columns=['tiv'])
        curr_os_summary_info = outputset_summary_info[output_set_id].drop(columns=['tiv'])

        summary_oed_cols = [c for c in curr_group_summary_info.columns.to_list() if c not in ignored_cols]
        output_set_map = pd.merge(curr_os_summary_info, curr_group_summary_info, how="left", on=summary_oed_cols)
        output_set_map = output_set_map[['summary_id', 'SummaryId']]
        output_set_map = output_set_map.query('~(summary_id == SummaryId)')

        if not output_set_map.empty:
            outputset_summary_id[output_set_id] = output_set_map.set_index('summary_id').to_dict()['SummaryId']

    return outputset_summary_id
