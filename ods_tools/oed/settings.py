import copy
from pathlib import Path
import json
import jsonschema
import jsonref
import os

from ods_tools.oed.common import OdsException

import logging
settings_logger = logging.getLogger(__name__)

ROOT_USER_ROLE = {'admin'}
DATA_PATH = Path(Path(__file__).parent.parent, 'data')


def json_dict_walk(json_dict, key_path):
    """
    Yield all the object present under a certain path for a json like dict going through each element

    >>> test_dict = {'a' : [{'b': 1}, {'b': [2, 3]}], 'c': [{'b': 4}, {'b': 5}]}
    >>> list(json_dict_walk(test_dict, ['a', 'b']))
    [1, 2, 3]

    >>> test_dict = {'a' : [{'b': 1}, {'b': [2, 3]}], 'c': [{'b': 4}, {'b': 5}]}
    >>> list(json_dict_walk(test_dict, 'a'))
    [{'b': 1}, {'b': [2, 3]}]

    >>> test_dict = {'a' : [{'b': 1}, {'b': [2, 3]}], 'c': [{'b': 4}, {'b': 5}]}
    >>> list(json_dict_walk(test_dict, ['d']))
    []

    """
    dict_stack = [(json_dict, 0)]  # where 0 is the key index in key_path
    while dict_stack:
        sub_config, key_i = dict_stack.pop()
        while True:
            if isinstance(sub_config, list):
                for elm in reversed(sub_config):
                    dict_stack.append((elm, key_i))
                break
            if key_i >= len(key_path):  # we are at the end of the key_path we yield the object we found
                yield sub_config
                break
            else:
                sub_config = sub_config.get(key_path[key_i], {})
                key_i += 1
                if not sub_config:  # dead end, no object for this key_path
                    break


class Settings:
    """
    The Setting class allow you to combine several settings file into one consistent one.
    when merging a new settings file, key value will be overwritten if the user role specified has access.
    if no user role is ever specified, the default behavior is that newer added setting will overwrite the values in the setting object.
    otherwise access can be restricted to certain user role:
    - using the keyword 'restricted' in the key info.
    - setting a value to a key and a user role in add_settings (all key that have a value will have the 'restricted' info set to the user role value)
    Validation info such as min, max and options are check when overwriting a key.

    sub_settings are also merge in the same way.
    """
    parameters_mapping = {
        "string_parameters": ("type", "string"),
        "list_parameters": ("type", "array"),
        "dictionary_parameters": ("type", "object"),
        "boolean_parameters": ("type", "boolean"),
        "float_parameters": ("type", "number"),
        "numeric_parameters": ("type", "number"),
        "integer_parameters": ("type", "integer"),
        "dropdown_parameters": ("oneOf", "options"),
    }

    def __init__(self):
        self._settings = {}
        self._sub_parts = set()

    @classmethod
    def is_sub_settings(cls, key, key_info):
        return key.endswith('_settings') and isinstance(key_info, dict) and not cls.is_key_info(key_info)

    @classmethod
    def is_parameters(cls, key, key_info):
        """check if a key, key_info pair is describe list of parameters"""
        return key.endswith("_parameters") or key == "datafile_selectors"

    @classmethod
    def is_parameter_grouping(cls, key, key_info):
        """check if key, key_info pair is describing how to group parameters (for UI purpose for example)"""
        return key == "parameter_groups" or key == "multi_parameter_options"

    @classmethod
    def is_valid_value(cls, key_info, value):
        if key_info is None:
            return True
        is_valid = True
        if "options" in key_info:
            is_valid &= value in key_info["options"]
        if "min" in key_info:
            is_valid &= value >= key_info["min"]
        if "max" in key_info:
            is_valid &= value <= key_info["max"]
        return is_valid

    @staticmethod
    def to_user_role(user_role):
        if user_role is None:
            return []
        elif isinstance(user_role, str):
            return [user_role]
        else:
            return list(user_role)

    @classmethod
    def is_key_info(cls, obj):
        return isinstance(obj, dict) and 'default' in obj

    @classmethod
    def to_key_info(cls, key_info, user_role):
        if not cls.is_key_info(key_info):
            key_info = {
                "default": key_info,
            }
            if user_role:
                key_info['restricted'] = list(user_role)
        return key_info

    @staticmethod
    def has_access(key_info, user_role):
        if key_info is None:
            return True
        restricted = key_info.get('restricted', set())
        return bool((not restricted) or ROOT_USER_ROLE.union(restricted).intersection(user_role))

    @classmethod
    def update_key(cls, settings, key, key_info, user_role):
        """update the value of a key with enrich key_info"""
        current_info = settings.get(key)
        if cls.has_access(current_info, user_role):  # If True can overwrite key
            key_info = cls.to_key_info(key_info, user_role)
            if cls.is_valid_value(current_info, key_info["default"]):
                settings[key] = key_info
            else:
                settings_logger.info(f"Value {key_info['default']} for {key} is not valid {current_info}")
        else:
            settings_logger.info(f"user_role {user_role} cannot overwrite {key}, role {current_info['restricted']} required")

    def add_key(self, key, key_info, user_role=None):
        user_role = self.to_user_role(user_role)
        self.update_key(self._settings, key, key_info, user_role)

    @classmethod
    def update_settings(cls, main_settings: dict, extra_settings: dict, user_role: list):
        extra_settings = copy.deepcopy(extra_settings)
        for key, key_info in extra_settings.items():
            if cls.is_sub_settings(key, key_info):
                cls.update_settings(main_settings.setdefault(key, {}), key_info, user_role)
            elif cls.is_parameters(key, key_info):
                for parameter in key_info:
                    sub_key_info = cls.to_key_info(parameter, user_role)
                    sub_key_info['parameter_type'] = key
                    cls.update_key(main_settings, parameter['name'], sub_key_info, user_role)
            elif cls.is_parameter_grouping(key, key_info):
                main_settings[key] = key_info
            else:
                cls.update_key(main_settings, key, key_info, user_role)

    def add_settings(self, settings, user_role=None):
        """merge a new settings dict to the setting object"""
        user_role = self.to_user_role(user_role)
        self.update_settings(self._settings, settings, user_role)

    @classmethod
    def to_dict(cls, settings):
        settings_dict = {}
        for key, key_info in settings.items():
            if cls.is_sub_settings(key, key_info):
                settings_dict[key] = cls.to_dict(key_info)
            else:
                if cls.is_parameter_grouping(key, key_info):
                    continue
                settings_dict[key] = key_info['default']
        return settings_dict

    def to_json_schema(self, setting_names):
        settings_json_schema_properties = {}
        settings_json_schema = {"properties": settings_json_schema_properties}
        for setting_name in setting_names:
            setting_info = self._settings.get(setting_name)
            if setting_info is not None:
                settings_properties = {}
                settings_object = {
                    "additionalProperties": False,
                    "type": "object",
                    "properties": settings_properties
                }
                for param_key, param_info in setting_info.items():
                    if not self.is_key_info(param_info):
                        continue

                    property_info = {}
                    schema_key, schema_val = self.parameters_mapping.get(param_info.pop('parameter_type', None), (None, None))
                    if schema_key == "type":
                        property_info[schema_key] = schema_val
                    elif schema_key == "oneOf":
                        property_info[schema_key] = param_info[schema_val]
                    else:
                        continue

                    if "desc" in param_info:
                        property_info["title"] = param_info["desc"]
                    if "tooltip" in param_info:
                        property_info["description"] = param_info["tooltip"]

                    if "min" in param_info:
                        property_info["minimum"] = param_info["min"]
                    if "max" in param_info:
                        property_info["maximum"] = param_info["max"]
                    settings_properties[param_key] = property_info

                if settings_properties:
                    settings_json_schema_properties[setting_name] = settings_object

        return settings_json_schema

    def get_settings(self):
        """return the dict with all the actual values"""
        return self.to_dict(self._settings)

    def __getitem__(self, item):
        key_info = self._settings[item]
        if 'default' in key_info:
            return key_info['default']
        else:
            return key_info


class SettingHandler:
    extra_checks = []

    def __init__(self, schemas=None, compatibility_profiles=None, settings_type='', logger=settings_logger):
        if schemas is None:
            schemas = []
        if compatibility_profiles is None:
            compatibility_profiles = []
        self.__schemas = schemas
        self.compatibility_profiles = compatibility_profiles
        self.settings_type = settings_type
        self.logger = logger

    def add_compatibility_profile(self, compatibility_profile, compatibility_path):
        if compatibility_profile is not None:
            if isinstance(compatibility_profile, (str, Path)):
                with open(compatibility_profile, encoding="UTF-8") as f:
                    compatibility_profile = jsonref.load(f)
            if isinstance(compatibility_path, str):
                compatibility_path = [compatibility_path]
            compatibility_profile['compatibility_path'] = compatibility_path
            self.compatibility_profiles.append(compatibility_profile)

    @property
    def info(self):
        """
        Returns the path to the loaded JSON file.

        Returns:
            str: The path to the loaded JSON file.

        """
        return self.__schemas

    def add_schema(self, schema, name, *, keys_path=None, schema_fp=None, convert_sub_settings=False):
        schema_info = {
            'name': name,
            'schema': schema,
        }
        if schema_fp is not None:
            schema_info['schema_fp'] = schema_fp
        if keys_path is not None:
            schema_info['key_path'] = keys_path
        schema_info['convert_sub_settings'] = convert_sub_settings
        self.__schemas.append(schema_info)

    def get_schema(self, name):
        """
        get the schema from self.__schemas with the name 'name'
            return None if not present
        """
        for schema_info in self.__schemas:
            if schema_info['name'] == name:
                return schema_info['schema']

    def add_schema_from_fp(self, schema_fp, **kwargs):
        if schema_fp in os.listdir(DATA_PATH):
            settings_logger.info(f'{schema_fp} loaded from default folder {DATA_PATH}')
            schema_fp = Path(DATA_PATH, schema_fp)
        else:
            schema_fp = Path(schema_fp)

        with open(schema_fp, encoding="UTF-8") as f:
            schema = jsonref.load(f)

        self.add_schema(schema, schema_fp=schema_fp, **kwargs)

    def update_obsolete_keys(self, settings_data, version=None, sub_path=None, delete_all=False):
        """
        Updates the loaded JSON data to account for deprecated keys.

        Args:
            settings_data (dict): The loaded JSON data.
            version: version to compare to when updating obsolete keys

        Returns:
            dict: The updated JSON data.

        """
        if version is None:
            def is_obsolete(key_version): return True
        else:
            def is_obsolete(key_version):
                return key_version <= version  # key is obsolete only if asked version is after new key has been introduced

        updated_settings_data = settings_data.copy()
        # if sub_path is passed settings_data is actually the sub part
        if sub_path is None:
            compatibility_profiles = self.compatibility_profiles
        else:
            compatibility_profiles = [compatibility_profile for compatibility_profile in self.compatibility_profiles
                                      if sub_path == compatibility_profile.get("compatibility_path", [])]

        for compatibility_profile in compatibility_profiles:
            if sub_path is None:
                key_path = compatibility_profile.get("compatibility_path", [])
            else:
                key_path = []
            for obj in json_dict_walk(updated_settings_data, key_path):
                if not isinstance(obj, dict):
                    self.logger.info(f"In setting {self.settings_type}, "
                                     f"object at path {compatibility_profile.get('compatibility_path', [])} is not a dict or a list of dict")
                    continue
                all_obsolete_keys = set(key for key, info in compatibility_profile.items()
                                        if key != 'compatibility_path' and is_obsolete(info["from_ver"]))
                obsolete_keys = set(obj) & all_obsolete_keys
                for key in obsolete_keys:
                    self.logger.info(f" '{key}' loaded as '{compatibility_profile[key]['updated_to']}'")
                    obj[compatibility_profile[key]['updated_to']] = obj[key]
                    if delete_all or compatibility_profile[key]['deleted']:
                        del obj[key]
                for key in obj:
                    if key in Settings.parameters_mapping:
                        for param in list(obj[key]):
                            if param['name'] in all_obsolete_keys:
                                if delete_all or compatibility_profile[param['name']]['deleted']:
                                    param['name'] = compatibility_profile[param['name']]['updated_to']
                                else:
                                    param = copy.deepcopy(param)
                                    param['name'] = compatibility_profile[param['name']]['updated_to']
                                    obj[key].append(param)

        return updated_settings_data

    def load(self, settings_fp, version=None, validate=True, raise_error=True):
        """
        Loads the JSON data from a file path.

        Args:
            settings_fp (str): The path to the JSON file.
            version: version to compare to when updating obsolete keys
            validate: if True validate the file after load
            raise_error:  raise exception on validation failure
        Raises:
            OdsException: If the JSON file is invalid.

        Returns:
            dict: The loaded JSON data.

        """
        try:
            filepath = Path(settings_fp)
            with filepath.open(encoding="UTF-8") as f:
                settings_raw = json.load(f)
        except (IOError, TypeError, ValueError):
            raise OdsException(f'Invalid {self.settings_type} file or file path: {settings_fp}')
        settings_data = self.update_obsolete_keys(settings_raw, version)

        if validate:
            self.validate(settings_data, raise_error=raise_error)
        return settings_data

    def validate(self, setting_data, raise_error=True):
        """
        Validates the loaded JSON data against the schema.

        Args:
            setting_data (dict): The loaded JSON data.
            raise_error (bool): raise exception on validation failure

        Returns:
            tuple: A tuple containing a boolean indicating whether the JSON data is valid
            and a dictionary containing any validation errors.

        """
        # special validation
        exception_msgs = {}
        for check in self.extra_checks:
            settings_logger.info(f"Running check_{check}")
            for field_name, message in getattr(self, f"check_{check}")(setting_data).items():
                exception_msgs.setdefault(field_name, []).extend(message)

        for schema_info in self.__schemas:
            for json_sub_part in json_dict_walk(setting_data, schema_info.get('key_path', [])):
                if schema_info['convert_sub_settings']:
                    _setting = Settings()
                    _setting.add_settings(json_sub_part)
                    json_sub_part = _setting.get_settings()
                    json_sub_part = self.update_obsolete_keys(json_sub_part, sub_path=schema_info.get('key_path', []), delete_all=True)

                for err in jsonschema.Draft202012Validator(schema_info['schema']).iter_errors(json_sub_part):
                    if err.path:
                        field = '-'.join([str(e) for e in err.path])
                    elif err.schema_path:
                        field = '-'.join([str(e) for e in err.schema_path])
                    else:
                        field = 'error'
                    exception_msgs.setdefault(f"{schema_info['name']} {field}", []).append(err.message)

        if exception_msgs and raise_error:
            raise OdsException("\nJSON Validation error in '{}.json': {}".format(
                self.settings_type,
                json.dumps(exception_msgs, indent=4)
            ))

        return not bool(exception_msgs), exception_msgs

    def validate_file(self, settings_fp, raise_error=True):
        """
        Validates the loaded JSON file against the schema.

        Args:
            settings_fp (str): The file path to the settings file.
            raise_error (bool): raise execption on validation failuer

        Returns:
            tuple: A tuple containing a boolean indicating whether the JSON data is valid
            and a dictionary containing any validation errors.
        """
        settings_data = self.load(settings_fp)
        return self.validate(settings_data, raise_error=raise_error)


class AnalysisSettingHandler(SettingHandler):
    default_analysis_setting_schema_json = 'analysis_settings_schema.json'
    extra_checks = ['unique_summary_ids']
    default_analysis_compatibility_profile = {
        "module_supplier_id": {
            "from_ver": "1.23.0",
            "deleted": True,
            "updated_to": "model_supplier_id"
        },
        "model_version_id": {
            "from_ver": "1.23.0",
            "deleted": True,
            "updated_to": "model_name_id"
        }
    }

    @classmethod
    def make(cls,
             analysis_setting_schema_json=None, model_setting_json=None, computation_settings_json=None,
             analysis_compatibility_profile=None, model_compatibility_profile=None, computation_compatibility_profile=None,
             **kwargs):

        handler = cls(settings_type='analysis_settings', **kwargs)
        if analysis_compatibility_profile is None:
            analysis_compatibility_profile = cls.default_analysis_compatibility_profile
        handler.add_compatibility_profile(analysis_compatibility_profile, [])
        handler.add_compatibility_profile(model_compatibility_profile, 'model_settings')
        handler.add_compatibility_profile(computation_compatibility_profile, 'computation_settings')

        if analysis_setting_schema_json is None:
            handler.add_schema_from_fp(cls.default_analysis_setting_schema_json, name='analysis_settings_schema')
        elif isinstance(analysis_setting_schema_json, (str, Path)):
            handler.add_schema_from_fp(analysis_setting_schema_json, name='analysis_settings_schema')
        else:
            handler.add_schema(analysis_setting_schema_json, name='analysis_settings_schema')

        model_setting_subpart_validation = ['model_settings']
        if computation_settings_json is None:
            model_setting_subpart_validation.append('computation_settings')
        elif isinstance(computation_settings_json, (str, Path)):
            handler.add_schema_from_fp(computation_settings_json, name='computation_settings_schema', keys_path=['computation_settings'])
        else:
            handler.add_schema(computation_settings_json, name='computation_settings_schema', keys_path=['computation_settings'])

        if model_setting_json is None:
            pass
        else:
            model_setting_handler = ModelSettingHandler.make(
                model_setting_schema_json={},
                computation_settings_json=computation_settings_json,
                model_compatibility_profile=model_compatibility_profile,
                computation_compatibility_profile=computation_compatibility_profile,
                **kwargs
            )

            if isinstance(model_setting_json, (str, Path)):
                model_setting_dict = model_setting_handler.load(model_setting_json)
            else:
                model_setting_dict = model_setting_handler.update_obsolete_keys(model_setting_json)
            model_setting_induce_schema = Settings()
            model_setting_induce_schema.add_settings(model_setting_dict)
            handler.add_schema(model_setting_induce_schema.to_json_schema(model_setting_subpart_validation), name='model_setting_induce_schema')

        return handler

    def check_unique_summary_ids(self, setting_data):
        """
        Ensures that the JSON data contains unique summary IDs for each
        runtype.

        Args:
            setting_data (dict): The loaded JSON data.

        Returns:
            dict: Exception messages. Will be empty if there are no unique
            summary IDs.

        """
        exception_msgs = {}
        runtype_summaries = [f'{runtype}_summaries' for runtype in ['gul', 'il', 'ri']]
        for runtype_summary in runtype_summaries:
            summary_ids = [summary.get('id', []) for summary in setting_data.get(runtype_summary, [])]
            duplicate_ids = set(summary_id for summary_id in summary_ids if summary_ids.count(summary_id) > 1)
            if duplicate_ids:
                error_msgs = [f'id {summary_id} is duplicated' for summary_id in duplicate_ids]
                exception_msgs[runtype_summary] = error_msgs

        return exception_msgs


class ModelSettingHandler(SettingHandler):
    default_model_setting_json = 'model_settings_schema.json'

    @classmethod
    def make(cls,
             model_setting_schema_json=None, computation_settings_json=None,
             model_compatibility_profile=None, computation_compatibility_profile=None,
             **kwargs):
        handler = cls(settings_type='model_settings', **kwargs)
        handler.add_compatibility_profile(model_compatibility_profile, 'model_settings')
        handler.add_compatibility_profile(computation_compatibility_profile, 'computation_settings')

        if model_setting_schema_json is None:
            handler.add_schema_from_fp(cls.default_model_setting_json, name='model_settings_schema')
        elif isinstance(model_setting_schema_json, (str, Path)):
            handler.add_schema_from_fp(model_setting_schema_json, name='model_settings_schema')
        else:
            handler.add_schema(model_setting_schema_json, name='model_settings_schema')

        if computation_settings_json is None:
            pass
        elif isinstance(computation_settings_json, (str, Path)):
            handler.add_schema_from_fp(computation_settings_json, name='computation_settings_schema',
                                       keys_path=['computation_settings'],
                                       convert_sub_settings=True)
        else:
            handler.add_schema(computation_settings_json, name='computation_settings_schema',
                               keys_path=['computation_settings'],
                               convert_sub_settings=True)

        return handler
