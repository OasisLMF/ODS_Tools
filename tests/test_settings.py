import unittest
from pathlib import Path
from ods_tools.oed import Settings, ModelSettingHandler, AnalysisSettingHandler


test_data_dir = Path(Path(__file__).parent, "data")

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

class OEDSettingsChecks(unittest.TestCase):
    def test_model_settings(self):
        model_setting_handler = ModelSettingHandler.make(
            computation_settings_json=Path(test_data_dir, 'computation_settings_schema.json'),
            model_compatibility_profile=Path(test_data_dir, 'model_compatibility_profile.json'),
            computation_compatibility_profile=Path(test_data_dir, 'config_compatibility_profile.json')
        )
        # correct settings load, no error expected
        setting_data = model_setting_handler.load(Path(test_data_dir, "model_settings.json"))

        # check loaded valid data is still valid
        model_setting_handler.validate(setting_data)

        # incorrect settings validation fail
        setting_data["model_settings"]["bad_float"] = 5.0
        setting_data["computation_settings"]["float_parameters"] = [
          {
            "name": "bad_param",
            "desc": "test for param renaming",
            "tooltip": "raise an error",
            "default": 0.2,
            "min": 0.0,
            "max": 100000.0
          }
        ]
        with self.assertRaises(Exception) as context:
            model_setting_handler.validate(setting_data)

        self.assertTrue("'bad_float' was unexpected" in str(context.exception))
        self.assertTrue("'bad_param' was unexpected" in str(context.exception))

    def test_analysis_settings(self):
        analysis_settings_handler = AnalysisSettingHandler.make(
            model_setting_json=Path(test_data_dir, "model_settings.json"),
            computation_settings_json=Path(test_data_dir, 'computation_settings_schema.json'),
            model_compatibility_profile=Path(test_data_dir, 'model_compatibility_profile.json'),
            computation_compatibility_profile=Path(test_data_dir, 'config_compatibility_profile.json'),
        )

        # correct settings load, no error expected
        setting_data = analysis_settings_handler.load(Path(test_data_dir, "analysis_settings.json"))

        # check loaded valid data is still valid
        analysis_settings_handler.validate(setting_data)

        # incorrect settings validation fail
        setting_data["model_settings"]["bad_float"] = 5.0
        setting_data["computation_settings"]["bad_param"] = 0.2
        setting_data["gul_summaries"].append({
            "aalcalc": True,
            "eltcalc": True,
            "id": 1,
            "lec_output": True,
            "leccalc": {
                "full_uncertainty_aep": True,
                "full_uncertainty_oep": True,
                "return_period_file": True
            }
        })
        with self.assertRaises(Exception) as context:
            analysis_settings_handler.validate(setting_data)

        self.assertTrue("'bad_float' was unexpected" in str(context.exception))
        self.assertTrue("'bad_param' was unexpected" in str(context.exception))
        self.assertTrue("id 1 is duplicated" in str(context.exception))
