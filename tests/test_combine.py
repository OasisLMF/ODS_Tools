from dataclasses import asdict
from pathlib import Path
import tempfile
import json

from ods_tools.combine.combine import combine
from ods_tools.combine.result import load_analysis_dirs


example_path = Path(Path(__file__).parent.parent, "ods_tools", "combine", "examples")


def test_combine_as_expected():

    input_dir = example_path / "inputs"

    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = Path(tmp_dir, "config.json")

        with open(config_path, "w") as f:
            json.dump({
                "analysis_dirs": [str(child) for child in input_dir.iterdir()]
            }, f)

        with open(config_path, "r") as f:
            config = json.load(f)

        print(config)

    combine_result = combine(str(config_path))


def test_combine__load_analysis_dirs():
    expected_analyses = [
        {'id': 1,
         'outputsets': [{'id': None, 'perspective_code': 'gul',
                        'analysis_id': 1,
                         'exposure_summary_level_fields': [],
                         'exposure_summary_level_id': 1},
                        {'id': None, 'perspective_code': 'gul',
                        'analysis_id': 1,
                         'exposure_summary_level_fields': ['LocNumber'],
                         'exposure_summary_level_id': 2}]
         },
        {'id': 2,
         'outputsets': [{'id': None, 'perspective_code': 'gul',
                        'analysis_id': 2,
                         'exposure_summary_level_fields': [],
                         'exposure_summary_level_id': 1},
                        {'id': None, 'perspective_code': 'gul',
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
