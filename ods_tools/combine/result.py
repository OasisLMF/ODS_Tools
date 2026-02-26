"""
This module manages interfacing with ORD folders.
"""
from dataclasses import dataclass, field
from pathlib import Path

from ods_tools.oed import AnalysisSettingHandler
from ods_tools.oed.common import OdsException

ANALYSIS_PATHS = [
    Path('output') / 'analysis_settings.json',
    Path('analysis_settings.json'),
    Path('input') / 'analysis_settings.json'
]


@dataclass
class Analysis():
    '''Analysis class'''
    id: int = None
    run_id: str = None
    description: str = ''
    settings: dict = field(default_factory=lambda: dict)
    path: str = ''

    @classmethod
    def from_analysis_settings(cls, analysis_settings, path=None, id=None):
        '''Initialise Analysis object from analysis settings.'''
        init_params = {}
        init_params['id'] = id
        init_params['run_id'] = analysis_settings.get('analysis_tag')
        init_params['path'] = path
        init_params['settings'] = analysis_settings

        return cls(**init_params)


def get_analysis_path(dir):
    for analysis_path in ANALYSIS_PATHS:
        analysis_path = Path(dir) / analysis_path
        if analysis_path.is_file():
            return analysis_path
    return None


def load_analysis_dirs(analysis_dirs):
    '''
    Load the Analyses dict from a list of analysis directories.

    Args:
        analysis_dirs (List[str]): List of analysis directories.
    '''
    analyses = {}

    ods_analysis_settings_schema = AnalysisSettingHandler.make()
    curr_id = 1
    for dir in analysis_dirs:
        analysis_path = get_analysis_path(dir)
        if analysis_path is None:
            raise OdsException(f'Combine error: could not find analysis settings file for {dir}')

        curr_settings = ods_analysis_settings_schema.load(analysis_path)
        curr_analysis = Analysis.from_analysis_settings(curr_settings, path=Path(dir), id=curr_id)

        analyses[curr_id] = curr_analysis
        curr_id += 1

    return analyses
