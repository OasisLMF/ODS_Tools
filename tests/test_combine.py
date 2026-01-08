from dataclasses import asdict
from pathlib import Path
import tempfile
import json

from ods_tools.combine.combine import combine
from ods_tools.combine.grouping import ResultGroup
from ods_tools.combine.result import load_analysis_dirs


example_path = Path(Path(__file__).parent.parent, "ods_tools", "combine", "examples")

BASIC_CONFIG = {}


def test_combine_as_expected():

    input_dir = example_path / "inputs"

    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = Path(tmp_dir, "config.json")

        with open(config_path, "w") as f:
            json.dump({
                "analysis_dirs": [str(child) for child in input_dir.iterdir()],
                "group_number_of_periods": 1000
            }, f)

        combine_result = combine(str(config_path))


def test_combine__load_analysis_dirs():
    expected_analyses = [
        {'id': 1,
         'outputsets': [{'perspective_code': 'gul',
                        'analysis_id': 1,
                         'exposure_summary_level_fields': [],
                         'exposure_summary_level_id': 1},
                        {'perspective_code': 'gul',
                        'analysis_id': 1,
                         'exposure_summary_level_fields': ['LocNumber'],
                         'exposure_summary_level_id': 2}]
         },
        {'id': 2,
         'outputsets': [{'perspective_code': 'gul',
                        'analysis_id': 2,
                         'exposure_summary_level_fields': [],
                         'exposure_summary_level_id': 1},
                        {'perspective_code': 'gul',
                        'analysis_id': 2,
                         'exposure_summary_level_fields': ['LocNumber'],
                         'exposure_summary_level_id': 2}]
         }
    ]

    analysis_dirs = [example_path / 'inputs/1', example_path / 'inputs/2']
    analyses = load_analysis_dirs(analysis_dirs)

    assert len(analyses) == len(expected_analyses)
    for analysis, expected_analysis in zip(analyses, expected_analyses):
        assert analysis.id == expected_analysis['id']
        assert len(analysis.outputsets) == len(expected_analysis['outputsets'])

        for outputset, expected_outputset in zip(analysis.outputsets, expected_analysis['outputsets']):
            outputset = asdict(outputset)
            assert outputset == expected_outputset


def test_combine__groupsets():
    expected_groupset = {0: {'id': 0, 'outputsets': [0, 2],
                             'exposure_summary_level_fields': [],
                             'perspective_code': 'gul'},
                         1: {'id': 1, 'outputsets': [1, 3],
                             'exposure_summary_level_fields': ['LocNumber'],
                             'perspective_code': 'gul'}}

    analysis_dirs = [example_path / 'inputs/1', example_path / 'inputs/2']
    analyses = load_analysis_dirs(analysis_dirs)

    group = ResultGroup(analyses, config=BASIC_CONFIG, id=1)

    groupset = group.prepare_groupset()

    assert expected_groupset.keys() == groupset.keys()
    for groupset_id in expected_groupset.keys():
        assert groupset[groupset_id] == expected_groupset[groupset_id]
