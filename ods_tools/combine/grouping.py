"""
This module handles the grouping interface.
"""
from dataclasses import dataclass, field
from typing import Dict, List
from pathlib import Path
import os

from ods_tools.combine.utils import dataclass_list_to_dataframe, hash_summary_level_fields
from ods_tools.combine.result import OutputSet, Analysis, load_analysis_dirs


class ResultGroup:
    """
    A class for managing and running the grouping for a set of ORD analyses.

    Args:
        analyses (list[Analysis]): List of analyses to group together.
        config (dict): grouping config
        id (int): id to identify grouping
    """

    def __init__(self, analyses, config, id):
        self.analyses = {a.id: a for a in analyses}
        self.outputsets = None
        self.groupset = None
        self.id = id
        self.set_outputsets()

    def set_outputsets(self):
        """
        Prepare outputset for current grouping.
        """
        outputsets = []
        for a in self.analyses.values():
            outputsets += a.outputsets if a.outputsets is not None else []

        self.outputsets = {i: outputset for i, outputset in enumerate(outputsets)}

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
                max_groupset_id += 1
            else:
                groupset_outputsets[groupset_id] += [os_id]

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


@dataclass
class GroupSet:
    '''GroupSet class'''
    perspective_code: str = None
    outputsets: List[OutputSet] = field(default_factory=lambda: [])
    group_id: int = None
    exposure_summary_level_fields: List[str] = field(default_factory=lambda: [])
    outputset_summary_info_map: Dict = field(default_factory=lambda: {})


if __name__ == "__main__":
    analyses_dirs = ["./examples/inputs/1", "./examples/inputs/2/"]

    analyses = load_analysis_dirs(analyses_dirs)

    group = ResultGroup(analyses, config={}, id=1)

    output = group.prepare_groupset()

    print(output)
