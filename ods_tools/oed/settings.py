import logging
logger = logging.getLogger(__name__)

ROOT_USER_ROLE = {'admin'}


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
        return key == "parameter_groups"

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

    def __init__(self):
        self._settings = {}
        self._sub_parts = set()

    @classmethod
    def update_key(cls, settings, key, key_info, user_role):
        """update the value of a key with enrich key_info"""
        current_info = settings.get(key)
        if cls.has_access(current_info, user_role):  # If True can overwrite key
            key_info = cls.to_key_info(key_info, user_role)
            if cls.is_valid_value(current_info, key_info["default"]):
                settings[key] = key_info
            else:
                logger.info(f"Value {key_info['default']} for {key} is not valid {current_info}")
        else:
            logger.info(f"user_role {user_role} cannot overwrite {key}, role {current_info['restricted']} required")

    def add_key(self, key, key_info, user_role=None):
        user_role = self.to_user_role(user_role)
        self.update_key(self._settings, key, key_info, user_role)

    @classmethod
    def update_settings(cls, main_settings: dict, extra_settings: dict, user_role: list):
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
                    pass
                settings_dict[key] = key_info['default']
        return settings_dict

    def get_settings(self):
        """return the dict with all the actual values"""
        return self.to_dict(self._settings)

    def __getitem__(self, item):
        key_info = self._settings[item]
        if 'default' in key_info:
            return key_info['default']
        else:
            return key_info
