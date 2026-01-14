"""
This module handles the grouping interface.
"""
from collections import namedtuple
from copy import copy
from pathlib import Path
import pandas as pd

from ods_tools.combine.utils import hash_summary_level_fields
from ods_tools.combine.io import save_summary_info
from ods_tools.combine.common import DEFAULT_CONFIG


class ResultGroup:
    """
    A class for managing and running the grouping for a set of ORD analyses.

    Args:
        analyses (list[Analysis]): List of analyses to group together.
        id (int): id to identify grouping
        group_event_set_fields (list[str]): list of fields to group event sets
    """

    def __init__(self, analyses, group_event_set_fields=None,
                 output_dir=None, **kwargs):
        self.analyses = {a.id: copy(a) for a in analyses}
        self.outputsets = None
        self.groupset = None
        self.set_outputsets()
        self.group_event_set_fields = group_event_set_fields if group_event_set_fields is not None else DEFAULT_CONFIG['group_event_set_fields']
        self.output_dir = output_dir

    def prepare_group_info(self):
        '''
        Meta function to prepare group for grouping.
        '''
        self.prepare_groupset()
        self.prepare_groupeventset()
        self.prepare_summaryinfo_map()

    def set_outputsets(self):
        """
        Prepare outputset for current grouping.
        """
        outputsets = []
        outputset_id = 0
        for a in self.analyses.values():
            if a.outputsets is None:
                continue

            for os in a.outputsets:
                os.id = outputset_id
                outputsets.append(os)
                outputset_id += 1

        self.outputsets = {os.id: os for os in outputsets}

    def prepare_groupset(self):
        """
        Prepares the GroupSet info. Needed to create summary info mappings.
        """
        if self.outputsets is None:
            self.set_outputsets()

        outputsets = self.outputsets

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
                self.outputsets[os_id].groupset_id = max_groupset_id
                max_groupset_id += 1
            else:
                groupset_outputsets[groupset_id] += [os_id]
                self.outputsets[os_id].groupset_id = groupset_id

        groupset_dict = {}
        for key, value in groupset_outputsets.items():
            curr_groupset_dict = {'id': key,
                                  'outputsets': value}
            tmp_outputset = outputsets[value[0]]
            curr_groupset_dict['exposure_summary_level_fields'] = tmp_outputset.exposure_summary_level_fields
            curr_groupset_dict['perspective_code'] = tmp_outputset.perspective_code
            groupset_dict[key] = curr_groupset_dict

        self.groupset = groupset_dict
        return groupset_dict

    def prepare_summaryinfo_map(self):
        """
        Create a map for each outputset which maps the input summary_id to the group SummaryId.
        """
        outputset_summaryinfo = self.load_outputset_summary_info(self.analyses, self.outputsets)

        groupset_summaryinfo = self.load_groupset_summaryinfo(self.groupset, outputset_summaryinfo)

        if self.output_dir is not None:
            save_summary_info(groupset_summaryinfo, self.groupset, self.output_dir)

        ignored_cols = ['SummaryId', 'summary_id']

        groupset_outputset = []
        for groupset_id, groupset in self.groupset.items():
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

        self.summaryinfo_map = summaryinfo_map

        # todo need to save groupset_summaryinfo

        return summaryinfo_map

    @staticmethod
    def load_outputset_summary_info(analyses, outputsets):
        outputset_summary_info = {}

        for outpuset_id, outputset in outputsets.items():
            summary_info_path = Path(analyses[outputset.analysis_id].path) / 'output' / outputset.get_summary_info_fname()
            outputset_summary_info[outpuset_id] = pd.read_csv(summary_info_path)

        return outputset_summary_info

    @staticmethod
    def load_groupset_summaryinfo(groupset, outputset_summary_info):
        group_summary_info = {}

        ignored_cols = ['summary_id', 'tiv']

        for group_set_id, group in groupset.items():
            outputset_ids = group['outputsets']
            curr_group_summary_info = pd.concat([outputset_summary_info[os_id] for os_id in outputset_ids])
            summary_oed_cols = [c for c in curr_group_summary_info.columns.to_list() if c not in ignored_cols]
            curr_group_summary_info = curr_group_summary_info.groupby(summary_oed_cols, as_index=False).agg({'tiv': 'sum'})
            curr_group_summary_info['SummaryId'] = curr_group_summary_info.index + 1

            group_summary_info[group_set_id] = curr_group_summary_info[['SummaryId'] + summary_oed_cols + ['tiv']]

        return group_summary_info

    def prepare_groupeventset(self):
        """
        Prepares the GroupEventSet info. This creates the event groupings based on the group_event_set_fields.
        """
        analysis_event_set_fields = {}
        EventSetField = namedtuple('EventSetField', self.group_event_set_fields)

        for a_id, analysis in self.analyses.items():
            analysis_event_set_fields[a_id] = EventSetField(**self.extract_group_event_set_fields(analysis, self.group_event_set_fields))

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

        self.groupeventset = groupeventset
        return groupeventset

    @staticmethod
    def extract_group_event_set_fields(analysis, group_event_set_fields):
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
        for field_name in group_event_set_fields:
            analysis_event_set_fields[field_name] = parse_field_from_analysis_settings(field_name, analysis.settings)

        return analysis_event_set_fields
