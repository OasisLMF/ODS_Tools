"""
This module handles the grouping interface.
"""
from dataclasses import dataclass, field
from typing import List
from collections import defaultdict, namedtuple
from pathlib import Path
import pandas as pd

from ods_tools.combine.utils import hash_summary_level_fields
from ods_tools.combine.io import save_summary_info


@dataclass
class OutputSet():
    '''OutputSet class'''
    id: int = None
    perspective_code: str = None
    analysis_id: int = None  # link to Analysis
    groupset_id: int = None  # link to GroupSet
    exposure_summary_level_fields: List[str] = field(default_factory=lambda: list)
    exposure_summary_level_id: int = None


class ResultGroup:
    """
    A class for managing and running the grouping for a set of ORD analyses.

    Args:
        analyses (list[Analysis]): List of analyses to group together.
        id (int): id to identify grouping
        group_event_set_fields (list[str]): list of fields to group event sets
    """

    def __init__(self, analyses: dict = None,
                 outputsets: dict = None,
                 groupset: dict = None,
                 groupeventset: dict = None,
                 summaryinfo_map: dict = None):
        self.analyses = analyses
        self.outputsets = outputsets
        self.groupset = groupset
        self.groupeventset = groupeventset
        self.summaryinfo_map = summaryinfo_map


def create_combine_group(analyses, groupeventset_fields):
    '''
    Create and prepare a ResultGroup for the analyses. Returns group and group summaryinfo.

    Args:
        analyses (dict): Map of analysis_id to anlayses in group.
        groupeventset_fields (list[str]): List of columns used to define groupevenset.
    '''
    group = ResultGroup()
    group.analyses = analyses

    group.outputsets, group.analysis_outputset = prepare_outputsets(group.analyses)
    group.groupset, group.outputsets = prepare_groupset(group.outputsets)
    group.groupeventset = prepare_groupeventset(analyses, groupeventset_fields)

    # Handle summaryinfo + alignment
    outputset_summaryinfo = prepare_outputset_summaryinfo(group.analyses,
                                                          group.outputsets)
    groupset_summaryinfo = prepare_groupset_summaryinfo(group.groupset,
                                                        outputset_summaryinfo)

    group.summaryinfo_map = prepare_summaryinfo_map(outputset_summaryinfo,
                                                    groupset_summaryinfo,
                                                    group.groupset)

    return group, groupset_summaryinfo


def prepare_outputsets(analyses):
    '''
    Prepare outputset map. Also returns map between analysis and outputset.

    Args:
        analyses (dict[Analysis]) : map of anlaysis_id to anlaysis in group.
    '''
    outputsets = {}
    analysis_outputset = defaultdict(list)
    outputset_offset = 0
    for a_id, analysis in analyses.items():
        curr_outputsets, outputset_offset = _outputset_from_analysis(analysis,
                                                                     outputset_offset)

        analysis_outputset[a_id].extend(list(curr_outputsets.keys()))
        outputsets.update(curr_outputsets)
    return outputsets, analysis_outputset


def _outputset_from_analysis(analysis, outputset_id_offset=0):
    '''Prepare outputsets for a single analysis.
    '''
    perspectives = ['gul', 'il', 'ri']
    outputsets = {}
    outputset_id = outputset_id_offset
    for p in perspectives:
        if not analysis.settings.get(f'{p}_output', False):
            continue

        summaries = analysis.settings.get(f'{p}_summaries', {})
        for summary_level in summaries:
            outputset = OutputSet()
            outputset.perspective_code = p
            outputset.exposure_summary_level_id = summary_level['id']
            outputset.exposure_summary_level_fields = summary_level.get('oed_fields', [])
            outputset.analysis_id = analysis.id
            outputset.id = outputset_id

            outputsets[outputset_id] = outputset
            outputset_id += 1

    return outputsets, outputset_id


def prepare_groupset(outputsets):
    """
    Prepares the GroupSet info. Needed to create summary info mappings.

    Args:
        outputsets (List[OutputSet]) : list of outputsets in group.

    Returns:
        groupset (dict) : Dict representing groupset
        outputsets (List[OutputSet]): updated outputsets
    """
    groupset_def_dict = {'gul': {}, 'il': {}, 'ri': {}}
    groupset_outputsets = {}

    max_groupset_id = 0
    for os_id, outputset in outputsets.items():
        perspective_code = outputset.perspective_code
        summary_level_hash = hash_summary_level_fields(outputset.exposure_summary_level_fields)

        groupset_id = groupset_def_dict.get(perspective_code, {}).get(summary_level_hash, None)
        if groupset_id is None:
            groupset_def_dict[perspective_code][summary_level_hash] = max_groupset_id
            groupset_outputsets[max_groupset_id] = [os_id]
            outputsets[os_id].groupset_id = max_groupset_id
            max_groupset_id += 1
        else:
            groupset_outputsets[groupset_id] += [os_id]
            outputsets[os_id].groupset_id = groupset_id

    groupset_dict = {}
    for key, value in groupset_outputsets.items():
        curr_groupset_dict = {'id': key,
                              'outputsets': value}
        tmp_outputset = outputsets[value[0]]
        curr_groupset_dict['exposure_summary_level_fields'] = tmp_outputset.exposure_summary_level_fields
        curr_groupset_dict['perspective_code'] = tmp_outputset.perspective_code
        groupset_dict[key] = curr_groupset_dict

    return groupset_dict, outputsets


def prepare_outputset_summaryinfo(analyses, outputsets):
    '''Prepare a map of outputset_id to summaryinfo
    '''
    outputset_summary_info = {}

    for outpuset_id, outputset in outputsets.items():
        summaryinfo_fname = f'{outputset.perspective_code}_S{outputset.exposure_summary_level_id}_summary-info.csv'
        summary_info_path = Path(analyses[outputset.analysis_id].path) / 'output' / summaryinfo_fname
        outputset_summary_info[outpuset_id] = pd.read_csv(summary_info_path)

    return outputset_summary_info


def prepare_groupset_summaryinfo(groupset, outputset_summaryinfo):
    '''Prepare map of groupset_id to summaryinfo
    '''
    groupset_summaryinfo = {}

    ignored_cols = ['summary_id', 'tiv']

    for group_set_id, group in groupset.items():
        outputset_ids = group['outputsets']
        curr_group_summary_info = pd.concat([outputset_summaryinfo[os_id] for os_id in outputset_ids])
        summary_oed_cols = [c for c in curr_group_summary_info.columns.to_list() if c not in ignored_cols]
        curr_group_summary_info = curr_group_summary_info.groupby(summary_oed_cols, as_index=False).agg({'tiv': 'sum'})
        curr_group_summary_info['SummaryId'] = curr_group_summary_info.index + 1

        groupset_summaryinfo[group_set_id] = curr_group_summary_info[['SummaryId'] + summary_oed_cols + ['tiv']]

    return groupset_summaryinfo


def prepare_summaryinfo_map(outputset_summaryinfo, groupset_summaryinfo, groupset):
    """
    Prepares summaryinfo map for alignment.

    Args:
        analyses (dict[Analyses]) : Dict of anlayses in group, indexed by analysis_id.
        outputset_summaryinfo (dict) : Map of outputset_id to summaryinfo
        groupset_summaryinfo (dict): Map of groupset_id to summaryinfo
    """
    ignored_cols = ['SummaryId', 'summary_id']

    groupset_outputset = []
    for groupset_id, groupset in groupset.items():
        groupset_outputset += [(groupset_id, outputset_id) for outputset_id in groupset['outputsets']]

    summaryinfo_map = {}
    for groupset_id, outputset_id in groupset_outputset:
        curr_group_summary_info = groupset_summaryinfo[groupset_id].drop(columns=['tiv'])
        curr_os_summary_info = outputset_summaryinfo[outputset_id].drop(columns=['tiv'])

        summary_oed_cols = [c for c in curr_group_summary_info.columns.to_list() if c not in ignored_cols]
        output_set_map = pd.merge(curr_os_summary_info, curr_group_summary_info, how="left", on=summary_oed_cols)
        output_set_map = output_set_map[['summary_id', 'SummaryId']]
        output_set_map = output_set_map.query('~(summary_id == SummaryId)')

        if not output_set_map.empty:
            summaryinfo_map[outputset_id] = output_set_map.set_index('summary_id').to_dict()['SummaryId']

    return summaryinfo_map


def prepare_groupeventset(analyses, groupeventset_fields):
    """
    Prepares the GroupEventSet info. This creates the event groupings based on the groupeventset_fields.

    Args:
        analyses (dict): Map of analysis_id to anlayses in group.
        groupeventset_fields (list[str]): List of columns used to define groupevenset.

    Returns:
        groupeventset (dict): Map of groupeventset_id to EventSetField
    """
    analysis_event_set_fields = {}
    EventSetField = namedtuple('EventSetField', groupeventset_fields)

    for a_id, analysis in analyses.items():
        analysis_event_set_fields[a_id] = EventSetField(**extract_groupeventset_fields(analysis, groupeventset_fields))

    groupeventset_analyses = {}
    groupeventset_ids = {}
    max_groupeventset_id = 0
    for a_id, fields in analysis_event_set_fields.items():
        ges_id = groupeventset_ids.get(fields, None)
        if ges_id is None:
            groupeventset_ids[fields] = max_groupeventset_id
            groupeventset_analyses[max_groupeventset_id] = [a_id]
        else:
            groupeventset_analyses[ges_id] += [a_id]

    # format for output
    groupeventset = {}
    for eventsetfield, groupeventset_id in groupeventset_ids.items():
        groupeventset[groupeventset_id] = {
            'id': groupeventset_id,
            'event_set_field': eventsetfield,
            'analysis_ids': groupeventset_analyses[groupeventset_id]
        }

    return groupeventset


def extract_groupeventset_fields(analysis, groupeventset_fields):
    def parse_field_from_analysis_settings(field_name, settings):
        # prepare path to walk
        field_location_map = {
            "event_set_id": ["model_settings", "event_set"],
            "event_occurrence_id": ["model_settings", "event_occurrence_id"],
        }

        field_path = field_location_map.get(field_name, [field_name])

        # walk path
        value = settings
        for key in field_path:
            value = value.get(key, {})

        if value:
            return value
        return ''

    analysis_event_set_fields = {}
    for field_name in groupeventset_fields:
        analysis_event_set_fields[field_name] = parse_field_from_analysis_settings(field_name, analysis.settings)

    return analysis_event_set_fields
