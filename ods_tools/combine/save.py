'''
This module handles serialising and saving data produced by grouping.
'''
from pathlib import Path


def save_summary_info(groupset_summaryinfo, groupset_info, output_dir):
    '''Save summary info
    Args
    groupset_summaryinfo (dict): Dict with key as groupset_id and value as summaryinfo to save.
    groupset_info (dict): Groupset info dict.
    output_dir (str, Path): path to save summary info.
    '''

    for gs, g_summary_info_df in groupset_summaryinfo.items():
        summary_info_fname = f'{groupset_info[gs]['perspective_code']}_GS{gs}_summary-info.csv'
        g_summary_info_df.to_csv(Path(output_dir) / summary_info_fname, index=False)
