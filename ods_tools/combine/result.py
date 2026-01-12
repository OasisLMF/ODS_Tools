"""
This module manages interfacing with ORD folders.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from ods_tools.oed.setting_schema import AnalysisSettingSchema


@dataclass
class OutputSet():
    '''OutputSet class'''
    perspective_code: str = None
    analysis_id: int = None  # link to Analysis
    exposure_summary_level_fields: List[str] = field(default_factory=lambda: list)
    exposure_summary_level_id: int = None

    def get_summary_info_fname(self):
        return f'{self.perspective_code}_S{self.exposure_summary_level_id}_summary-info.csv'


@dataclass
class Analysis():
    '''Analysis class'''
    id: int = None
    run_id: str = None
    description: str = ''
    settings: dict = field(default_factory=lambda: dict)
    outputsets: List[OutputSet] = None
    path: str = ''

    @classmethod
    def from_analysis_settings(cls, analysis_settings, path=None, id=None):
        '''Initialise Analysis object from analysis settings.'''
        init_params = {}
        init_params['id'] = id
        init_params['run_id'] = analysis_settings.get('analysis_tag')
        init_params['path'] = path
        init_params['settings'] = analysis_settings

        init_params['outputsets'] = cls.extract_outputsets(analysis_settings, analysis_id=id)
        return cls(**init_params)

    @staticmethod
    def extract_outputsets(analysis_settings, analysis_id=None):
        '''
        Load outputsets from an analysis_settings file.
        '''
        perspectives = ['gul', 'il', 'ri']
        outputset_list = []
        for p in perspectives:
            if not analysis_settings.get(f'{p}_output', False):
                continue

            summaries = analysis_settings.get(f'{p}_summaries', {})
            for summary_level in summaries:
                outputset = OutputSet()
                outputset.perspective_code = p
                outputset.exposure_summary_level_id = summary_level['id']
                outputset.exposure_summary_level_fields = summary_level.get('oed_fields', [])
                outputset.analysis_id = analysis_id

                outputset_list.append(outputset)

        return outputset_list if len(outputset_list) != 0 else None


def load_analysis_dirs(analysis_dirs):
    '''
    Load the ORD Analyses from a list of analysis directories.
    '''
    analyses = []

    ods_analysis_settings_schema = AnalysisSettingSchema()
    curr_id = 1
    for dir in analysis_dirs:
        curr_settings = ods_analysis_settings_schema.load(Path(dir) / 'analysis_settings.json')
        curr_analysis = Analysis.from_analysis_settings(curr_settings, path=Path(dir), id=curr_id)

        analyses.append(curr_analysis)
        curr_id += 1

    return analyses
