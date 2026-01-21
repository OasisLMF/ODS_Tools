from pathlib import Path
# import tempfile
import pytest
from collections import namedtuple
import pandas as pd
import numpy as np
from unittest import mock

from pandas.testing import assert_frame_equal

from ods_tools.combine.combine import DEFAULT_CONFIG, combine
from ods_tools.combine.grouping import create_combine_group
from ods_tools.combine.io import save_output
from ods_tools.combine.output_generation import generate_alt, generate_ept, alt_dtype, ept_dtype
from ods_tools.combine.result import load_analysis_dirs
from ods_tools.combine.sampling import generate_gpqt, generate_group_periods, do_loss_sampling, gpqt_dtype, gplt_dtype

example_path = Path(Path(__file__).parent.parent, "ods_tools", "combine", "examples")
expected_output_path = Path(Path(__file__).parent.parent, 'expected_output')
validation_path = Path(Path(__file__).parent.parent, 'validation', 'combine_ord')

TEST_GROUP_PERIODS = 2000


# def test_combine__outputs_genrated():
#
#     input_dir = example_path / "inputs"
#
#     config = {
#         "analysis_dirs": [str(child) for child in input_dir.iterdir()],
#         "group_number_of_periods": TEST_GROUP_PERIODS,
#         "group_mean": True,
#         "group_alt": True,
#         "group_plt": True,
#         "group_ept": True
#     }
#
#     config = DEFAULT_CONFIG | config
#
#     groupset_ids = [0, 1]
#     expected_summaryinfo_paths = [f'gul_GS{gs_id}_summary-info.csv' for gs_id in groupset_ids]
#
#     expected_output_paths = [f'{gs_id}_alt.csv' for gs_id in groupset_ids]
#     expected_output_paths += [f'{gs_id}_ept.csv' for gs_id in groupset_ids]
#     expected_output_paths += [f'{gs_id}_plt.csv' for gs_id in groupset_ids]
#
#     with tempfile.TemporaryDirectory() as tmp_output_dir:
#         config['output_dir'] = tmp_output_dir
#
#         combine_result = combine(**config)
#
#         # make sure summary info output
#         for suminfo_path in expected_summaryinfo_paths:
#             assert (Path(tmp_output_dir) / suminfo_path).exists()
#
#         # make sure plt, ept and apt output
#         for output_path in expected_output_paths:
#             assert (Path(tmp_output_dir) / output_path).exists()

def test_combine__load_analysis_dirs():
    parent_path = Path(__file__).parent.parent / 'ods_tools' / 'combine' / 'examples' / 'inputs'
    expected_analyses = {
        1:
        {'id': 1,
         'path': parent_path / '1'
         },
        2:
        {'id': 2,
         'path': parent_path / '2'
         }
    }

    analysis_dirs = [example_path / 'inputs/1', example_path / 'inputs/2']
    analyses = load_analysis_dirs(analysis_dirs)

    assert len(analyses) == len(expected_analyses)
    for analysis_id, analysis in analyses.items():
        expected_analysis = expected_analyses[analysis_id]
        assert analysis.id == expected_analysis['id']
        assert analysis.path == expected_analysis['path']


# Grouping tests

@pytest.fixture()
def seed_default_rng():
    seeded_rng = np.random.default_rng(seed=0)
    with mock.patch("ods_tools.combine.sampling.np.random.default_rng") as mocked:
        mocked.return_value = seeded_rng
        yield


@pytest.fixture
def prepared_group_example():
    analysis_dirs = [example_path / 'inputs/1', example_path / 'inputs/2']
    analyses = load_analysis_dirs(analysis_dirs)

    group, _ = create_combine_group(analyses,
                                    groupeventset_fields=DEFAULT_CONFIG['group_event_set_fields'])

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

    summaryinfo_map = prepared_group_example.summaryinfo_map

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
    max_group_periods = TEST_GROUP_PERIODS

    expected_group_periods = pd.read_csv(validation_path / 'group_periods.csv')

    group_periods = generate_group_periods(prepared_group_example, max_group_periods)

    if keep_output:
        save_path = expected_output_path / 'group_periods.csv'
        save_path.parent.mkdir(parents=True, exist_ok=True)
        group_periods.to_csv(save_path, index=False)

    breakpoint()
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


def test_combine__output_generation(seed_default_rng, keep_output):

    gplt = pd.read_csv(validation_path / 'gplt.csv', dtype=gplt_dtype)
    groupset_ids = [0, 1]

    generated_alt = generate_alt(gplt, TEST_GROUP_PERIODS)
    generated_ept = generate_ept(gplt, TEST_GROUP_PERIODS, oep=True, aep=True)

    if keep_output:
        expected_output_path.mkdir(parents=True, exist_ok=True)
        save_output(generated_alt, expected_output_path, 'alt.csv')
        save_output(generated_ept, expected_output_path, 'ept.csv')

    for groupset_id in groupset_ids:
        expected_alt = pd.read_csv(validation_path / f"{groupset_id}_alt.csv", dtype=alt_dtype)
        expected_ept = pd.read_csv(validation_path / f"{groupset_id}_ept.csv", dtype=ept_dtype)
        _generated_alt = generated_alt.query(f"groupset_id == {groupset_id}").reset_index(drop=True)

        _generated_ept = generated_ept.query(f"groupset_id == {groupset_id}").reset_index(drop=True)

        assert_frame_equal(expected_alt, _generated_alt)
        assert_frame_equal(expected_ept, _generated_ept)
