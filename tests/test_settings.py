import copy
import unittest
from pathlib import Path
from ods_tools.oed import Settings, ModelSettingHandler, AnalysisSettingHandler
from ods_tools.oed.common import OdsException


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


class ReinsuranceLossPerspectiveSchemaChecks(unittest.TestCase):
    """Schema regression tests for the ``rl`` (reinsurance loss) perspective.

    ``analysis_settings_schema.json`` already exposes ``rl_output`` /
    ``rl_summaries``; ``model_settings_schema.json`` advertises supported
    perspectives via two enums (top-level ``valid_output_perspectives`` and
    per event-set ``valid_perspectives``). Both must accept ``rl``. See
    OasisLMF#1495.
    """

    def setUp(self):
        # Tests deep-copy this fixture before mutation so an in-place change
        # by ``validate`` cannot leak across assertions within a single test.
        self.model_settings = {
            "model_settings": {
                "event_set": {
                    "name": "Event Set",
                    "desc": "Event set selector",
                    "default": "p1",
                    "options": [
                        {
                            "id": "p1",
                            "desc": "Primary event set",
                            "valid_perspectives": ["gul", "il", "ri", "rl"],
                        }
                    ],
                },
                "valid_output_perspectives": ["gul", "il", "ri", "rl"],
            },
            "lookup_settings": {},
        }

    def test_rl_accepted_in_valid_output_perspectives(self):
        handler = ModelSettingHandler.make()
        ok, errors = handler.validate(copy.deepcopy(self.model_settings), raise_error=False)
        self.assertTrue(ok, f"expected valid model_settings, got errors: {errors}")
        self.assertEqual(errors, {})

    def test_rl_accepted_in_event_set_valid_perspectives(self):
        # Drop the top-level enum so a pass here can only be explained by the
        # nested ``valid_perspectives`` enum accepting ``rl``.
        data = copy.deepcopy(self.model_settings)
        del data["model_settings"]["valid_output_perspectives"]
        handler = ModelSettingHandler.make()
        ok, errors = handler.validate(data, raise_error=False)
        self.assertTrue(ok, f"expected valid model_settings, got errors: {errors}")

    def test_unknown_perspective_rejected_in_valid_output_perspectives(self):
        data = copy.deepcopy(self.model_settings)
        data["model_settings"]["valid_output_perspectives"] = ["gul", "xyz"]
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        message = str(ctx.exception)
        self.assertIn("'xyz' is not one of", message)
        # Anchor to the failing field path so a future regression that removed
        # the nested enum can't be masked by the top-level enum's identical
        # error wording.
        self.assertIn("valid_output_perspectives", message)

    def test_unknown_perspective_rejected_in_event_set_valid_perspectives(self):
        data = copy.deepcopy(self.model_settings)
        data["model_settings"]["event_set"]["options"][0]["valid_perspectives"] = [
            "gul",
            "xyz",
        ]
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        message = str(ctx.exception)
        self.assertIn("'xyz' is not one of", message)
        self.assertIn("event_set-options", message)

    def test_existing_perspectives_still_accepted(self):
        data = copy.deepcopy(self.model_settings)
        data["model_settings"]["valid_output_perspectives"] = ["gul", "il", "ri"]
        data["model_settings"]["event_set"]["options"][0]["valid_perspectives"] = [
            "gul",
            "il",
            "ri",
        ]
        handler = ModelSettingHandler.make()
        ok, errors = handler.validate(data, raise_error=False)
        self.assertTrue(ok, f"expected valid pre-#1495 doc, got errors: {errors}")


class ModelCertificationScopeSchemaChecks(unittest.TestCase):
    """Schema regression tests for the Model Certification Programme's scope metadata.

    Adds six fields proposed by Matt Jones (CTO) so a partner platform (e.g. Moody's Risk
    Modeler) can build a model profile from model_settings.json alone, without a separate
    'Key Facts' document: ``data_settings.supported_coverages``, ``geographic_extent_notes``,
    ``geographic_exclusions``, ``geographic_schemes_supported``, ``field_behaviour``,
    ``out_of_scope``, and ``model_settings.loss_methodology_notes``. Populated by the
    certification development team during review, not authored by vendors.
    """

    def setUp(self):
        self.model_settings = {
            "model_settings": {
                "loss_methodology_notes": {
                    "ground_up_loss_capping": {
                        "gul_alloc_rule": 2,
                        "description": "Example only: ground-up loss capped at coverage TIV.",
                    },
                    "correlation_methodology": "Example only: free-text summary of the correlation methodology.",
                },
            },
            "lookup_settings": {},
            "data_settings": {
                "supported_coverages": [
                    {"oed_field": "BuildingTIV", "supported": True},
                    {"oed_field": "OtherTIV", "supported": False, "notes": "Not modelled"},
                ],
                "geographic_extent_notes": "Example only: free-text qualification of the country list.",
                "geographic_exclusions": ["Example Excluded Territory"],
                "geographic_schemes_supported": [
                    {"scheme": "Coordinate", "oed_fields": ["Latitude", "Longitude"]},
                    {"scheme": "GeogSchemeX", "oed_fields": ["GeogSchemeX"], "scheme_codes": ["EX1"]},
                ],
                "field_behaviour": [
                    {
                        "oed_field": "OccupancyCode",
                        "role": "modifier",
                        "excluded_values": [
                            {"value": "9998", "effect": "no_loss", "notes": "Example only"},
                        ],
                    },
                ],
                "out_of_scope": [
                    {"item": "Demand surge / Post Loss Amplification (PLA)", "category": "loss_element"},
                ],
            },
        }

    def test_scope_metadata_accepted(self):
        handler = ModelSettingHandler.make()
        ok, errors = handler.validate(copy.deepcopy(self.model_settings), raise_error=False)
        self.assertTrue(ok, f"expected valid model_settings, got errors: {errors}")
        self.assertEqual(errors, {})

    def test_supported_coverages_oed_field_enum_enforced(self):
        data = copy.deepcopy(self.model_settings)
        data["data_settings"]["supported_coverages"][0]["oed_field"] = "NotARealCoverage"
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        self.assertIn("'NotARealCoverage' is not one of", str(ctx.exception))

    def test_field_behaviour_effect_enum_enforced(self):
        data = copy.deepcopy(self.model_settings)
        data["data_settings"]["field_behaviour"][0]["excluded_values"][0]["effect"] = "explodes"
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        self.assertIn("'explodes' is not one of", str(ctx.exception))

    def test_field_behaviour_requires_oed_field_and_role(self):
        data = copy.deepcopy(self.model_settings)
        del data["data_settings"]["field_behaviour"][0]["role"]
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        self.assertIn("'role' is a required property", str(ctx.exception))

    def test_out_of_scope_category_enum_enforced(self):
        data = copy.deepcopy(self.model_settings)
        data["data_settings"]["out_of_scope"][0]["category"] = "not_a_category"
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        self.assertIn("'not_a_category' is not one of", str(ctx.exception))

    def test_geographic_schemes_scheme_enum_enforced(self):
        data = copy.deepcopy(self.model_settings)
        data["data_settings"]["geographic_schemes_supported"][0]["scheme"] = "CarrierPigeon"
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        self.assertIn("'CarrierPigeon' is not one of", str(ctx.exception))

    def test_loss_methodology_notes_rejects_unknown_property(self):
        data = copy.deepcopy(self.model_settings)
        data["model_settings"]["loss_methodology_notes"]["unexpected_field"] = True
        handler = ModelSettingHandler.make()
        with self.assertRaises(OdsException) as ctx:
            handler.validate(data)
        self.assertIn("'unexpected_field' was unexpected", str(ctx.exception))
