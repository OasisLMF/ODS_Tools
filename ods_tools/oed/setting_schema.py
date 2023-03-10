import json
import os
from pathlib import Path

from .common import OdsException

import jsonschema

DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')


class SettingSchema:
    """
    A class for managing and validating JSON settings.

    Args:
        schema (dict, optional): The JSON schema to use for validation. Defaults to None.
        json_path (str, optional): The path to the JSON file to load. Defaults to None.
        settings_type (str, optional): A string describing the type of settings being loaded. Defaults to ''.

    Raises:
        NotImplementedError: If no default schema is set and the class is not created using the `from_json` method.

    Attributes:
        json_path (str): The path to the loaded JSON file.
        schema (dict): The JSON schema being used for validation.
        settings_type (str): A string describing the type of settings being loaded.

    Methods:
        from_json(cls, setting_json): Creates a new instance of the `SettingSchema` class by loading a JSON file.
        compatibility(self, settings_data): Updates the loaded JSON data to account for deprecated keys.
        load(self, settings_fp): Loads the JSON data from a file path.
        validate(self, setting_data): Validates the loaded JSON data against the schema.
        get(self, settings_fp, key=None, validate=True): Gets a value from the loaded JSON data.

    """
    SCHEMA_FILE = ''

    def __init__(self, schema=None, json_path=None, settings_type=''):
        """
        Initializes a new instance of the `SettingSchema` class.

        Args:
            schema (dict, optional): The JSON schema to use for validation. Defaults to None.
            json_path (str, optional): The path to the JSON file to load. Defaults to None.
            settings_type (str, optional): A string describing the type of settings being loaded. Defaults to ''.

        Raises:
            NotImplementedError: If no default schema is set and the class is not created using the `from_json` method.

        """
        if not any([schema, json_path, self.SCHEMA_FILE]):
            raise NotImplementedError('Not default schema set, must create class using <cls>.from_json method')

        if schema is None and json_path is None:
            inst = self.from_json(os.path.join(DATA_PATH, self.SCHEMA_FILE))
            self.json_path = inst.json_path
            self.schema = inst.schema
        else:
            self.json_path = json_path
            self.schema = schema
        self.settings_type = settings_type

    @property
    def info(self):
        """
        Returns the path to the loaded JSON file.

        Returns:
            str: The path to the loaded JSON file.

        """
        return self.json_path

    @classmethod
    def from_json(cls, setting_json):
        """
        Creates a new instance of the `SettingSchema` class by loading a JSON file.

        Args:
            cls (SettingSchema): The `SettingSchema` class.
            setting_json (str): The path to the JSON file to load.

        Returns:
            SettingSchema: A new instance of the `SettingSchema` class.

        """
        filepath = Path(setting_json)
        with filepath.open(encoding="UTF-8") as f:
            schema = json.load(f)
            return cls(schema, setting_json)

    def compatibility(self, settings_data):
        """
        Updates the loaded JSON data to account for deprecated keys.

        Args:
            settings_data (dict): The loaded JSON data.

        Returns:
            dict: The updated JSON data.

        """
        if getattr(self, 'compatibility_profile', None):
            obsolete_keys = set(self.compatibility_profile) & set(settings_data)
            if obsolete_keys:
                logger = logging.getLogger(__name__)
                logger.warning(f'WARNING: Deprecated key(s) in {self.settings_type} JSON')
                for key in obsolete_keys:
                    # warn user
                    logger.warning('   {} : {}'.format(
                        key,
                        self.compatibility_profile[key],
                    ))
                    # Update settings, if newer key not found
                    updated_key = self.compatibility_profile[key]['updated_to']
                    if updated_key not in settings_data:
                        settings_data[updated_key] = settings_data[key]
                    del settings_data[key]

                logger.warning('   These keys have been automatically updated, but should be fixed in the original file.\n')
        return settings_data

    def load(self, settings_fp):
        """
        Loads the JSON data from a file path.

        Args:
            settings_fp (str): The path to the JSON file.

        Raises:
            OdsException: If the JSON file is invalid.

        Returns:
            dict: The loaded JSON data.

        """
        try:
            filepath = Path(settings_fp)
            with filepath.open(encoding="UTF-8") as f:
                settings_raw = json.load(f)
                settings_data = self.compatibility(settings_raw)
        except (IOError, TypeError, ValueError):
            raise OdsException(f'Invalid {self.settings_type} file or file path: {settings_fp}')
        return settings_data

    def validate(self, setting_data, raise_error=True):
        """
        Validates the loaded JSON data against the schema.

        Args:
            setting_data (dict): The loaded JSON data.

        Returns:
            tuple: A tuple containing a boolean indicating whether the JSON data is valid
            and a dictionary containing any validation errors.

        """
        validator = jsonschema.Draft4Validator(self.schema)
        validation_errors = [e for e in validator.iter_errors(setting_data)]
        exception_msgs = {}
        is_valid = validator.is_valid(setting_data)

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
            raise OdsException("\nJSON Validation error in '{}.json': {}".format(
                self.settings_type,
                json.dumps(exception_msgs, indent=4)
            ))
        return is_valid, exception_msgs

    def get(self, settings_fp, key=None, validate=True):
        """
        Returns the settings data for a given settings file path, and optionally a specific key.

        Args:
            settings_fp (str): The file path to the settings file.
            key (str, optional): The key for which the value is desired. Defaults to None.
            validate (bool, optional): Whether to validate the settings data against the schema.
                                       Defaults to True.

        Raises:
            OdsException: If the settings file is invalid or the validation fails.

        Returns:
            The entire settings data as a dictionary if key is None, otherwise the value for the given key.
        """
        settings_data = self.load(settings_fp)
        self.validate(settings_data, raise_error=True)
        return settings_data if not key else settings_data.get(key)


class ModelSettingSchema(SettingSchema):

    def __init__(self, schema=None, json_path=None):
        self.SCHEMA_FILE = 'model_settings_schema.json'
        super(ModelSettingSchema, self).__init__(schema, json_path, 'model_settings')


class AnalysisSettingSchema(SettingSchema):

    def __init__(self, schema=None, json_path=None):
        self.SCHEMA_FILE = 'analysis_settings_schema.json'
        self.compatibility_profile = {
            "module_supplier_id": {
                "from_ver": "1.23.0",
                "updated_to": "model_supplier_id"
            },
            "model_version_id": {
                "from_ver": "1.23.0",
                "updated_to": "model_name_id"
            },
        }
        super(AnalysisSettingSchema, self).__init__(schema, json_path, 'analysis_settings')
