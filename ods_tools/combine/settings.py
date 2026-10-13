"""
Combine settings schema, kept free of the numba/scipy imports needed to run a combine
so it can be used to validate settings without the optional dependencies (ods-tools[combine])
"""
from pathlib import Path
import jsonschema
import json

from ods_tools.oed.common import OdsException

SCHEMA_PATH = Path(Path(__file__).parent.parent / 'data' / 'combine_settings_schema.json')


class CombineSettingsSchema:
    def __init__(self, schema_path=None):
        if schema_path is None:
            schema_path = SCHEMA_PATH

        with open(schema_path, 'r') as f:
            self.schema = json.load(f)

    def validate(self, config, raise_error=True):
        validator = jsonschema.Draft4Validator(self.schema)
        validation_errors = [e for e in validator.iter_errors(config)]

        is_valid = validator.is_valid(config)

        exception_msgs = {}
        if validation_errors:
            for err in validation_errors:
                if err.path:
                    field = '-'.join([str(e) for e in err.path])
                elif err.schema_path:
                    field = '-'.join([str(e) for e in err.schema_path])
                else:
                    field = 'error'

                if field in exception_msgs:
                    exception_msgs[field].append(err.message)
                else:
                    exception_msgs[field] = [err.message]

        if not is_valid and raise_error:
            raise OdsException("\nCombine config validation error: {}".format(
                json.dumps(exception_msgs, indent=4)
            ))

        return is_valid, exception_msgs
