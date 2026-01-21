from pathlib import Path
import tempfile
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


def test_combine__outputs_genrated():

    input_dir = example_path / "inputs"

    config = {
        "analysis_dirs": [str(child) for child in input_dir.iterdir()],
        "group_number_of_periods": TEST_GROUP_PERIODS,
        "group_mean": True,
        "group_alt": True,
        "group_plt": True,
        "group_ept": True
    }

    config = DEFAULT_CONFIG | config

    groupset_ids = [0, 1]
    expected_summaryinfo_paths = [f'gul_GS{gs_id}_summary-info.csv' for gs_id in groupset_ids]

    expected_output_paths = [f'{gs_id}_alt.csv' for gs_id in groupset_ids]
    expected_output_paths += [f'{gs_id}_ept.csv' for gs_id in groupset_ids]
    expected_output_paths += [f'{gs_id}_plt.csv' for gs_id in groupset_ids]

    with tempfile.TemporaryDirectory() as tmp_output_dir:
        config['output_dir'] = tmp_output_dir

        combine_result = combine(**config)

        # make sure summary info output
        for suminfo_path in expected_summaryinfo_paths:
            assert (Path(tmp_output_dir) / suminfo_path).exists()

        # make sure plt, ept and apt output
        for output_path in expected_output_paths:
            assert (Path(tmp_output_dir) / output_path).exists()
