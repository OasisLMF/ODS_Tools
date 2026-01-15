from dataclasses import asdict
from pathlib import Path
import tempfile
import json
import pytest
from collections import namedtuple
import pandas as pd
import numpy as np
from unittest import mock

from pandas.testing import assert_frame_equal

from ods_tools.combine.combine import DEFAULT_CONFIG, combine
from ods_tools.combine.grouping import ResultGroup
from ods_tools.combine.result import load_analysis_dirs
from ods_tools.combine.sampling import generate_gpqt, generate_group_periods, do_loss_sampling, gpqt_dtype, gplt_dtype

example_path = Path(Path(__file__).parent.parent, "ods_tools", "combine", "examples")
expected_output_path = Path(Path(__file__).parent.parent, 'expected_output')
validation_path = Path(Path(__file__).parent.parent, 'validation', 'combine_ord')

BASIC_CONFIG = {
}


def test_combine_as_expected():

    input_dir = example_path / "inputs"

    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = Path(tmp_dir, "config.json")

        with open(config_path, "w") as f:
            config = {
                "analysis_dirs": [str(child) for child in input_dir.iterdir()],
                "group_number_of_periods": 1000,
                "group_mean": True
            }

            json.dump(config, f)

        combine_result = combine(str(config_path))


def test_combine__load_analysis_dirs():
    expected_analyses = [
        {'id': 1,
         'outputsets': [{'perspective_code': 'gul',
                        'analysis_id': 1,
                         'exposure_summary_level_fields': [],
                         'groupset_id': None,
                         'id': None,
                         'exposure_summary_level_id': 1},
                        {'perspective_code': 'gul',
                        'analysis_id': 1,
                         'groupset_id': None,
                         'id': None,
                         'exposure_summary_level_fields': ['LocNumber'],
                         'exposure_summary_level_id': 2}]
         },
        {'id': 2,
         'outputsets': [{'perspective_code': 'gul',
                        'analysis_id': 2,
                         'groupset_id': None,
                         'id': None,
                         'exposure_summary_level_fields': [],
                         'exposure_summary_level_id': 1},
                        {'perspective_code': 'gul',
                        'analysis_id': 2,
                         'groupset_id': None,
                         'id': None,
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

# Grouping tests


@pytest.fixture()
def seed_default_rng():
    seeded_rng = np.random.default_rng(seed=0)
    with mock.patch("ods_tools.combine.sampling.np.random.default_rng") as mocked:
        mocked.return_value = seeded_rng
        yield


@pytest.fixture
def prepared_group_example(seed_default_rng):
    analysis_dirs = [example_path / 'inputs/1', example_path / 'inputs/2']
    analyses = load_analysis_dirs(analysis_dirs)

    group = ResultGroup(analyses, config=BASIC_CONFIG, id=1)
    group.prepare_group_info()
    return group


def test_combine__groupset_and_summaryinfo(prepared_group_example):
    expected_groupset = {0: {'id': 0, 'outputsets': [0, 2],
                             'exposure_summary_level_fields': [],
                             'perspective_code': 'gul'},
                         1: {'id': 1, 'outputsets': [1, 3],
                             'exposure_summary_level_fields': ['LocNumber'],
                             'perspective_code': 'gul'}}

    expected_summaryinfo_map = {
        1: {2: 3, 3: 6, 4: 9, 5: 10},
        3: {1: 2, 2: 4, 3: 5, 4: 7, 5: 8}
    }

    groupset = prepared_group_example.groupset

    assert expected_groupset.keys() == groupset.keys()
    for groupset_id in expected_groupset.keys():
        assert groupset[groupset_id] == expected_groupset[groupset_id]

    summaryinfo_map = prepared_group_example.prepare_summaryinfo_map()

    assert expected_summaryinfo_map.keys() == summaryinfo_map.keys()
    for outputset_id, curr_summaryinfo_map in summaryinfo_map.items():
        assert expected_summaryinfo_map[outputset_id] == curr_summaryinfo_map


def test_combine__groupeventset(prepared_group_example):
    EventSetField = namedtuple('EventSetField', DEFAULT_CONFIG['group_event_set_fields'])
    expected_groupeventset = {0: {'id': 0,
                                  'event_set_field':
                                  EventSetField(event_set_id='p',
                                                event_set_description='',
                                                event_occurrence_id='lt',
                                                event_occurrence_description='',
                                                event_occurrence_max_periods='',
                                                model_supplier_id='OasisLMF',
                                                model_name_id='PiWind',
                                                model_description='',
                                                model_version=''),
                                  'analysis_ids': [1, 2]}}

    groupeventset = prepared_group_example.groupeventset

    assert expected_groupeventset.keys() == groupeventset.keys()
    for groupeventset_id in groupeventset.keys():
        assert groupeventset[groupeventset_id] == expected_groupeventset[groupeventset_id]


def test_combine__generate_group_periods(prepared_group_example,
                                         seed_default_rng,
                                         keep_output):
    max_group_periods = 2000

    expected_group_periods = pd.read_csv(validation_path / 'group_periods.csv')

    group_periods = generate_group_periods(prepared_group_example, max_group_periods)

    if keep_output:
        save_path = expected_output_path / 'group_periods.csv'
        save_path.parent.mkdir(parents=True, exist_ok=True)
        group_periods.to_csv(save_path, index=False)

    assert_frame_equal(expected_group_periods, group_periods)


def test_combine__generate_gpqt(prepared_group_example,
                                seed_default_rng,
                                keep_output):
    group_period = pd.read_csv(validation_path / 'group_periods.csv')
    expected_gpqt = pd.read_csv(validation_path / 'gpqt.csv', dtype=gpqt_dtype)

    gpqt = generate_gpqt(group_period, prepared_group_example)

    if keep_output:
        save_path = expected_output_path / 'gpqt.csv'
        save_path.parent.mkdir(parents=True, exist_ok=True)
        gpqt.to_csv(save_path, index=False)

    assert_frame_equal(expected_gpqt, gpqt)


def test_combine__loss_sampling(prepared_group_example,
                                seed_default_rng,
                                keep_output):
    gpqt = pd.read_csv(validation_path / 'gpqt.csv', dtype=gpqt_dtype)
    expected_gplt = pd.read_csv(validation_path / 'gplt.csv', dtype=gplt_dtype)
    config = {
        'mean_only': True,
    }

    gplt = do_loss_sampling(gpqt, prepared_group_example, **config)

    if keep_output:
        save_path = expected_output_path / 'gplt.csv'
        save_path.parent.mkdir(parents=True, exist_ok=True)
        gplt.to_csv(save_path, index=False)

    assert_frame_equal(expected_gplt, gplt)
