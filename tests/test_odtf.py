import tempfile
import numpy as np
import pandas as pd
import pathlib
import yaml
import pytest

from ods_tools.odtf.controller import transform_format

base_test_path = pathlib.Path(__file__).parent
example_path = pathlib.Path(pathlib.Path(__file__).parent.parent, "ods_tools", "odtf", "examples")

def strip_quotes(s):
    return s.strip('"') if isinstance(s, str) else s

def test_transformation_as_expected():
    with tempfile.TemporaryDirectory() as tmp_dir:

        # Prepare the necessary files for the test
        config_file_path = pathlib.Path(tmp_dir, 'config.yaml')

        with open(config_file_path, 'w') as config_file:
            yaml.dump({
                "input": {
                    "path": str(pathlib.Path(base_test_path, 't_input.csv')),
                },
                "output": {
                    "path": str(pathlib.Path(tmp_dir, 't_output.csv')),
                },
                "mapping": {
                    "path": str(pathlib.Path(base_test_path, 'mapping_test.yaml')),
                },
                "batch_size": 150000,
            }, config_file)

        # Run the transformation
        transform_result = transform_format(str(config_file_path))

        # Assert the transformation result
        assert transform_result == str(pathlib.Path(tmp_dir, 't_output.csv'))

        output_df = pd.read_csv(transform_result)

        expected_values = {
            'Output_int_1': [110, 120, 111, 113, 117, 155, 201, 1099, 877, 101],
            'Output_int_2': [20, 30, 21, 23, 27, 65, 111, 1009, 787, 11],
            'Output_string_1': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', ''],
            'Output_float_1': [1.57, 4.71, 7.85, 11.304, 15.072, 16.328, 24.806, 348.8854, 0.00314, np.nan],
            'Output_float_2': [0.159235668789809, 0.477707006369427, 0.796178343949045, 1.14649681528662,
                               1.52866242038217, 1.65605095541401, 2.51592356687898, 35.3853503184713,
                               0.000318471337579618, np.nan],
            'Output_multistring_1': ["A;B;C", "A;J", "E;C", 'H', '', "C;I;A", "B;E;E", "J;I;I", "G;I;G", "B;A;G"],
            'Output_multistring_2': ["United Kingdom;Italy", "Germany;Brasil", "France;France", "Sweden",
                                     "Spain;Sweden", "Argentina", '', "United States;United Kingdom", '',
                                     "Argentina;Brasil;United States"]
        }

    for column, values in expected_values.items():
        if 'float' in column.lower():
            assert np.allclose(output_df[column].tolist(), values, equal_nan=True, rtol=1e-5, atol=1e-5)
        elif 'string' in column.lower():
            assert [strip_quotes(s) for s in output_df[column].fillna('').tolist()] == values
        else:
            assert output_df[column].tolist() == values


@pytest.mark.parametrize(
    "input_format",
    ["sqlite", "csv", ],
    ids=["simple_transform_sqlite", "simple_transform_csv"],
)
def test_simple_transform(input_format):
    # Prepare the necessary files for the test
    config_file_path = pathlib.Path(example_path, "simple_transform", input_format, 'config.yaml')

    # Run the transformation
    transform_result = transform_format(str(config_file_path))

    output_df = pd.read_csv(transform_result)

    expected_output_df = pd.read_csv(pathlib.Path(example_path, "simple_transform", 'expected_output.csv'))

    pd.testing.assert_frame_equal(output_df, expected_output_df)
