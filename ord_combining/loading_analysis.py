'''
File outlining how to load in and interact with ord analyses.
'''
# %% imports

import pandas as pd
from dataclasses import dataclass, asdict, field
import json
from pathlib import Path
from pprint import pprint
from typing import List

# %% define data classes

@dataclass
class OutputSet():
    '''OutputSetTable field data class'''
    id: int = None
    perspective_code: str = None
    settings_id: int = None # link to AnalysisTable
    exposure_summary_level_fields: List[str] = field(default_factory=lambda : list)
    exposure_summary_level_id: int = None

@dataclass
class Analysis():
    '''AnalysisTable field data class'''
    id: int = None
    run_id: str = None
    description: str = ''
    settings: dict = field(default_factory=lambda : dict)

# %% specify file paths

ord_output_dirs = [
                   "/home/vinulw/code/ODS_Tools/ord_combining/losses-20251017134045/output",
                   "/home/vinulw/code/ODS_Tools/ord_combining/losses-20251017134021/output",
                   "/home/vinulw/code/ODS_Tools/ord_combining/losses-20251017133750/output"
                   ]

ord_output_dirs = [Path(p) for p in ord_output_dirs]

# %% meta pre grouping

def parse_analysis_settings(settings_fp):
    '''
    Parse an analysis settings file and extract necessary details for OutputSet and AnalysisSet
    '''
    # load settings file
    with open(settings_fp, 'r') as f:
        a_settings = json.load(f)

    # parse the Analysis details
    analysis = Analysis()
    analysis.run_id = a_settings.get('analysis_tag')
    analysis.settings = a_settings

    # parse the output sets
    perspectives = ['gul', 'il', 'ri']
    outputsets = []

    for p in perspectives:
        if a_settings.get(f'{p}_output', False):
            outputsets += parse_analysis_output_sets(p, analysis)

    return analysis, outputsets


def parse_analysis_output_sets(perspective, analysis):
    '''Parse the output sets from the analysis'''

    summaries = analysis.settings.get(f'{perspective}_summaries', {})
    outputset_list = []
    for summary_level in summaries:
        outputset = OutputSet()
        outputset.perspective_code = perspective
        outputset.exposure_summary_level_id = summary_level['id']
        outputset.exposure_summary_level_fields = summary_level.get('oed_fields', [])
        outputset.settings_id = analysis.id

        outputset_list.append(outputset)

    return outputset_list


def load_output_sets(ord_output_dirs):
    outputsets = None

    analysis_set = []
    output_sets = []
    analysis_id = 1
    for ord_dir in ord_output_dirs:
        analysis, _outputsets = parse_analysis_settings(ord_dir / 'analysis_settings.json')

        # set the analysis
        analysis.id = analysis_id
        for i in range(len(_outputsets)):
            _outputsets[i].settings_id = analysis_id
        analysis_id += 1

        analysis_set.append(analysis)
        output_sets += _outputsets

    return analysis_set, output_sets

def dataclass_list_to_dataframe(dataclass_list):
    return pd.DataFrame([asdict(c) for c in dataclass_list])

# %% run generate outputsets

analysis, outputsets = load_output_sets(ord_output_dirs)

# prepare dfs
analysis_df = dataclass_list_to_dataframe(analysis)
analysis_df = analysis_df.drop(columns=['settings'])
outputsets_df = dataclass_list_to_dataframe(outputsets)
outputsets_df['id'] = outputsets_df.index # set id col

# %% compute potential groups

def list_col_to_string(list_col):
    return [','.join(map(str, l)) for l in list_col]

def create_group_set_df(outputsets_df, group_id):
    group_set_cols = ['perspective_code', 'exposure_summary_level_fields_string']
    group_set_df = outputsets_df.drop_duplicates(subset=group_set_cols)

    group_set_df = group_set_df.reset_index(drop=True)
    group_set_df['group_id'] = group_id
    group_set_df['id'] = group_set_df.index

    return group_set_df


def merge_group_set_output_set(group_set_df, outputsets_df):
    group_set_cols = ['perspective_code', 'exposure_summary_level_fields_string']
    # create merged group output set mapping
    merged_df = pd.merge(outputsets_df, group_set_df,
                how='inner', on=group_set_cols, suffixes=("_output_set", "_group_set"))
    merged_df = merged_df.rename(columns={"id_output_set": "output_set_id",
                                        "id_group_set": "group_set_id"})
    merged_df['id'] = merged_df.index

    return merged_df


def compute_potential_group_set(outputsets_df, group_id = 1):
    # convert to categorical col
    outputsets_df['exposure_summary_level_fields_string'] = list_col_to_string(outputsets_df['exposure_summary_level_fields'])

    group_set_df = create_group_set_df(outputsets_df, group_id)

    merged_df = merge_group_set_output_set(group_set_df, outputsets_df)

    # extract group analysis
    group_analysis_df = merged_df[['group_id', 'settings_id_output_set']]
    group_analysis_df = group_analysis_df.reset_index(drop=True)
    group_analysis_df['id'] = group_analysis_df.index
    group_analysis_df = group_analysis_df.rename(columns={"settings_id_output_set": "analysis_id"})

    # extract group output set
    group_output_set_df = merged_df[["id", "group_set_id", "output_set_id"]]

    # remove additional cols
    group_set_df = group_set_df[['id', 'group_id', 'perspective_code', 'exposure_summary_level_fields']]

    return group_set_df, group_output_set_df, group_analysis_df

outputsets_df.columns

group_set_df, group_output_set_df, group_analysis_df = compute_potential_group_set(outputsets_df)

# note at this point you can determine if a group_set has more than one element
print("Value counts for each group_set: \n", group_output_set_df['group_set_id'].value_counts())

# %% grouping


