import unittest
from ods_tools.oed.settings import Settings


class MultiSettingsPriorityCheck(unittest.TestCase):

    def setUp(self):
        self.host_settings = {
            "fmpy": {
                "type": "boolean_parameters",
                "default": True,
                "restricted": ["admin"]},
            "gulmc": {
                "type": "boolean_parameters",
                "default": True,
                "restricted": ["modeler"],
            },
            "num_process": 16,
        }

        self.model_settings = {
            "fmpy": {
                "default": True,
                "restricted": ["superuser"]
            },
            "gulmc": {
                "default": False,
            },
            'gul_threshold': {
                "default": 10.
            },
            'model_settings': {
                "setting_1": 6,
                "setting_2": {
                    "type": "int",
                    "default": 5,
                },
                "list_parameters": [
                    {
                        "name": "list_param_1",
                        "desc": "Custom attribute names",
                        "tooltip": "A list of field names",
                        "default": []
                    }]
            }

        }

        self.analysis_settings = {
            "fmpy": False,
            "gul_threshold": 5,
            "num_process": 7,
            'model_settings': {
                "setting_1": 2,
                "setting_2": 2,
            }
        }

    def test_admin_user(self):
        settings = Settings()
        settings.add_settings(self.host_settings, 'host')
        settings.add_settings(self.model_settings, 'modeler')
        settings.add_settings(self.analysis_settings, 'admin')
        dict_settings = settings.get_settings()
        assert dict_settings['fmpy'] is False
        assert dict_settings['gul_threshold'] == 5
        assert dict_settings['num_process'] == 7
        assert dict_settings['gulmc'] is False

    def test_super_user(self):
        settings = Settings()
        settings.add_settings(self.host_settings, 'host')
        settings.add_settings(self.model_settings, 'modeler')
        settings.add_settings(self.analysis_settings, ['super_user'])
        dict_settings = settings.get_settings()
        assert dict_settings['fmpy'] is True
        assert dict_settings['gul_threshold'] == 5
        assert dict_settings['num_process'] == 16

    def test_user(self):
        settings = Settings()
        settings.add_settings(self.host_settings, 'host')
        settings.add_settings(self.model_settings, 'modeler')
        settings.add_settings(self.analysis_settings)
        dict_settings = settings.get_settings()

        assert dict_settings['fmpy'] is True
        assert dict_settings['gul_threshold'] == 5
        assert dict_settings['num_process'] == 16
        assert dict_settings['model_settings']['setting_1'] == 6
        assert dict_settings['model_settings']['setting_2'] == 2
