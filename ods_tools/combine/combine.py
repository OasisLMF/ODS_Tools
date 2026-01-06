from pathlib import Path
from jsonschema import ValidationError, validate
import json

from ods_tools.oed.common import OdsException

SCHEMA_PATH = Path('./config_schema.json')

DEFAULT_CONFIG = {
    "group_fill_perspective": False,
    "group_event_set_fields": [
        "event_set_id",
        "event_set_description",
        "event_occurrence_id",
        "event_occurrence_description",
        "event_occurrence_max_periods",
        "model_supplier_id",
        "model_name_id",
        "model_description",
        "model_version"
    ],
    "group_period_seed": 2479,
    "group_mean": False,
    "group_secondary_uncertainty": False
}


def read_config(config_path):
    """Validate and read combine config"""
    with open(config_path, 'r') as f:
        config = json.load(f)
    config = DEFAULT_CONFIG | config

    with open(SCHEMA_PATH, 'r') as f:
        schema = json.load(f)

    try:
        validate(config, schema)
    except ValidationError as e:
        raise OdsException(f'Config Validation error: {e.message}')

    return config


def combine(config_path):
    config = read_config(config_path)

    print('Validated')


if __name__ == "__main__":
    combine("./config.json")
