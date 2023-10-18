import json
import logging
import os
import pathlib
import pytest
import shutil
import urllib

import sys

import pandas as pd
import numpy as np
from unittest import TestCase
import tempfile

# WORKAROUND - this line to to force an import from the installed ods_tools package
# from `python3.xx/site-packages/ods_tools` instead of the local module
sys.path.append(sys.path.pop(0))

from ods_tools.main import convert
from ods_tools.oed import OedExposure, OedSchema, OdsException, ModelSettingSchema, AnalysisSettingSchema, OED_TYPE_TO_NAME, UnknownColumnSaveOption

logger = logging.getLogger(__file__)

base_test_path = pathlib.Path(__file__).parent

piwind_branch = 'main'
base_url = f'https://raw.githubusercontent.com/OasisLMF/OasisPiWind/{piwind_branch}/tests/inputs'
input_file_names = {
    "csv": {
        'Acc': 'SourceAccOEDPiWind.csv',
        'Loc': 'SourceLocOEDPiWind10.csv',
        'ReinsInfo': 'SourceReinsInfoOEDPiWind.csv',
        'ReinsScope': 'SourceReinsScopeOEDPiWind.csv',
    },
    "parquet": {
        'Acc': 'SourceAccOEDPiWind.parquet',
        'Loc': 'SourceLocOEDPiWind10.parquet',
        'ReinsInfo': 'SourceReinsInfoOEDPiWind.parquet',
        'ReinsScope': 'SourceReinsScopeOEDPiWind.parquet'
    },
    "currency": {
        'Loc': 'SourceLocOEDPiWind10Currency.csv',
        'LocExp': 'SourceLocOEDPiWind10Currency_expected.csv',
        'roe': 'roe.csv'
    }

}


def _is_non_empty_file(fp):
    # print(f'{fp} not empty: {os.path.getsize(fp) > 0}')
    return os.path.getsize(fp) > 0


class OdsPackageTests(TestCase):
    @pytest.fixture(autouse=True)
    def logging_fixtures(self, caplog):
        self._caplog = caplog

    def test_load_oed_from_config(self):
        config = {'location': base_url + '/SourceLocOEDPiWind.csv',
                  'account': base_url + '/SourceAccOEDPiWind.parquet',
                  'ri_info': {'cur_version_name': 'orig',
                              'sources': {'orig': {'source_type': 'filepath',
                                                   'filepath': base_url + '/SourceReinsInfoOEDPiWind.csv',
                                                   'read_param': {
                                                       'usecols': ['ReinsNumber', 'ReinsLayerNumber', 'ReinsName',
                                                                   'ReinsPeril',
                                                                   'ReinsInceptionDate', 'ReinsExpiryDate',
                                                                   'CededPercent', 'RiskLimit',
                                                                   'RiskAttachment', 'OccLimit', 'OccAttachment',
                                                                   'PlacedPercent',
                                                                   'ReinsCurrency', 'InuringPriority', 'ReinsType',
                                                                   'RiskLevel', 'OEDVersion']}}}},
                  'ri_scope': base_url + '/SourceReinsScopeOEDPiWind.csv',
                  'use_field': True
                  }
        with tempfile.TemporaryDirectory() as tmp_run_dir:
            exposure = OedExposure(**config)

            # check csv file are read
            location = exposure.location.dataframe
            self.assertTrue(isinstance(location, pd.DataFrame))

            # check parquet file are read
            account = exposure.account.dataframe
            self.assertTrue(isinstance(account, pd.DataFrame))

            # check support for multiple file version and read parameter option
            ri_info = exposure.ri_info.dataframe
            # we remove 'UseReinsDates' from the column read to check read_param is used
            self.assertTrue('UseReinsDates' not in ri_info.columns)

            # check support for zipped file
            ri_scope = exposure.ri_scope.dataframe
            self.assertTrue(isinstance(ri_scope, pd.DataFrame))

            for compression in ['csv', 'parquet', 'zip']:
                exposure.location.save(f'oed_test_{compression}',
                                       {'source_type': 'filepath',
                                        'filepath': os.path.join(tmp_run_dir, 'oed_test', f'location.{compression}')})

            # check you can reload
            exposure_json_fp = os.path.join(tmp_run_dir, 'oed_test', OedExposure.DEFAULT_EXPOSURE_CONFIG_NAME)
            exposure.save_config(exposure_json_fp)
            with open(exposure_json_fp) as oed_json_file:
                config = json.load(oed_json_file)
                exposure2 = OedExposure(**config)
            self.assertTrue(exposure.location.dataframe.equals(exposure2.location.dataframe))

    def test_load_oed_from_df(self):
        location_df = pd.DataFrame({
            'PortNumber': [1, 1],
            'PortName': ['1', None],
            'AccNumber': [1, 2],
            'AccName': [1, ''],
            'LocNumber': [1, 2],
            'COUNTRYCODE ': ['GB', 'FR'],
            'LocPerilsCovered': 'WTC',
            'buildingtiv': ['1000', '20000'],
            'ContentsTIV': [0, 0],
            'BITIV': [0, 0],
            'LocCurrency': ['GBP', 'EUR']})
        # check categorical column
        location_df['PortName'] = location_df['PortName'].astype('category')

        exposure = OedExposure(**{'location': location_df, 'use_field': True})
        # check PortNumber are converted to str
        self.assertTrue((exposure.location.dataframe['PortNumber'] == '1').all())

        # check None convert to ''
        self.assertTrue(exposure.location.dataframe['PortName'][1] == '')
        self.assertTrue(exposure.location.dataframe['AccName'][1] == '')

        # check BuildingTIV converted to float and use field case
        self.assertTrue(pd.api.types.is_numeric_dtype(exposure.location.dataframe['BuildingTIV']))

        # check case and extra space are ignored
        self.assertTrue(exposure.location.dataframe['CountryCode'][0] == 'GB')

    def test_load_oed_from_stream(self):
        with tempfile.TemporaryDirectory() as tmp_run_dir:
            # read_parquet needs stream with seek method which urllib.request.urlopen doesn't have
            with open(os.path.join(tmp_run_dir, 'SourceAccOEDPiWind.parquet'), 'wb') as acc_parquet:
                acc_parquet.write(urllib.request.urlopen(base_url + '/SourceAccOEDPiWind.parquet').read())

            config = {'location': urllib.request.urlopen(base_url + '/SourceLocOEDPiWind.csv'),
                      'account': {'oed_info': open(os.path.join(tmp_run_dir, 'SourceAccOEDPiWind.parquet'), 'rb'), 'format': 'parquet'},
                      'use_field': True
                      }
            exposure = OedExposure(**config)
            # check csv stream is read
            location = exposure.location.dataframe
            self.assertTrue(isinstance(location, pd.DataFrame))

    def test_parquet_and_csv_return_same_df(self):
        with tempfile.TemporaryDirectory() as tmp_run_dir:
            # read_parquet needs stream with seek method which urllib.request.urlopen doesn't have
            configs_from_file = {}
            configs_from_stream = {}
            for extension in ['csv', 'parquet']:
                configs_from_file[extension] = {'use_field': True}
                configs_from_stream[extension] = {'use_field': True}
                for oed_type, oed_name in OED_TYPE_TO_NAME.items():
                    configs_from_file[extension][oed_name] = os.path.join(tmp_run_dir, f'Source{oed_type}OEDPiWind.{extension}')
                    with open(configs_from_file[extension][oed_name], 'wb') as acc_parquet:
                        acc_parquet.write(urllib.request.urlopen(base_url + f'/Source{oed_type}OEDPiWind.{extension}').read())
                    configs_from_stream[extension][oed_name] = open(os.path.join(tmp_run_dir, f'Source{oed_type}OEDPiWind.{extension}'), 'rb')

            exposure_csv_file = OedExposure(**configs_from_file['csv'])
            exposure_parquet_file = OedExposure(**configs_from_file['parquet'])
            exposure_csv_stream = OedExposure(**configs_from_stream['csv'])
            exposure_parquet_stream = OedExposure(**configs_from_stream['parquet'])

            pd.testing.assert_frame_equal(exposure_csv_file.location.dataframe, exposure_parquet_file.location.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.account.dataframe, exposure_parquet_file.account.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.ri_info.dataframe, exposure_parquet_file.ri_info.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.ri_scope.dataframe, exposure_parquet_file.ri_scope.dataframe, check_categorical=False)

            pd.testing.assert_frame_equal(exposure_csv_file.location.dataframe, exposure_csv_stream.location.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.account.dataframe, exposure_csv_stream.account.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.ri_info.dataframe, exposure_csv_stream.ri_info.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.ri_scope.dataframe, exposure_csv_stream.ri_scope.dataframe, check_categorical=False)

            pd.testing.assert_frame_equal(exposure_csv_file.location.dataframe, exposure_parquet_stream.location.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.account.dataframe, exposure_parquet_stream.account.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.ri_info.dataframe, exposure_parquet_stream.ri_info.dataframe, check_categorical=False)
            pd.testing.assert_frame_equal(exposure_csv_file.ri_scope.dataframe, exposure_parquet_stream.ri_scope.dataframe, check_categorical=False)

    def test_load_oed_from_stream__detect_type(self):
        with tempfile.TemporaryDirectory() as tmp_run_dir:

            # read_parquet needs stream with seek method which urllib.request.urlopen doesn't have
            with open(os.path.join(tmp_run_dir, 'SourceLocOEDPiWind.csv'), 'wb') as loc_csv:
                loc_csv.write(urllib.request.urlopen(base_url + '/SourceLocOEDPiWind.csv').read())
            with open(os.path.join(tmp_run_dir, 'SourceAccOEDPiWind.parquet'), 'wb') as acc_parquet:
                acc_parquet.write(urllib.request.urlopen(base_url + '/SourceAccOEDPiWind.parquet').read())

            csv_loc_obj = open(os.path.join(tmp_run_dir, 'SourceLocOEDPiWind.csv'))
            parquet_acc_obj = open(os.path.join(tmp_run_dir, 'SourceAccOEDPiWind.parquet'), 'rb')

            config = {
                'location': csv_loc_obj,
                'account': parquet_acc_obj,
                'use_field': True
            }

            exposure = OedExposure(**config)
            # check csv stream is read
            location = exposure.location.dataframe
            account = exposure.account.dataframe
            self.assertTrue(isinstance(location, pd.DataFrame))
            self.assertTrue(isinstance(account, pd.DataFrame))

    def test_load_oed_from_stream__invalid_types(self):
        with tempfile.TemporaryDirectory() as tmp_run_dir:

            # read_parquet needs stream with seek method which urllib.request.urlopen doesn't have
            with open(os.path.join(tmp_run_dir, 'SourceLocOEDPiWind.parquet'), 'wb') as loc_csv:
                loc_csv.write(urllib.request.urlopen(base_url + '/SourceLocOEDPiWind.csv').read())
            with open(os.path.join(tmp_run_dir, 'SourceAccOEDPiWind.csv'), 'wb') as acc_parquet:
                acc_parquet.write(urllib.request.urlopen(base_url + '/SourceAccOEDPiWind.parquet').read())

            csv_as_parquet = open(os.path.join(tmp_run_dir, 'SourceLocOEDPiWind.parquet'), 'rb')
            parquet_as_csv = open(os.path.join(tmp_run_dir, 'SourceAccOEDPiWind.csv'))

            with self.assertRaises(OdsException) as ex:
                exposure = OedExposure(**{'location': csv_as_parquet})
            with self.assertRaises(OdsException) as ex:
                exposure = OedExposure(**{'account': parquet_as_csv})

    def test_reporting_currency(self):
        config = {
            'location': base_url + '/SourceLocOEDPiWind10Currency.csv',
            'account': base_url + '/SourceAccOEDPiWind.csv',
            'ri_info': base_url + '/SourceReinsInfoOEDPiWind.csv',
            'ri_scope': base_url + '/SourceReinsScopeOEDPiWind.csv',
            'currency_conversion': {
                "currency_conversion_type": "DictBasedCurrencyRates",
                "source_type": "dict",
                "currency_rates": {
                    ('USD', 'GBP'): 0.85,
                    ('USD', 'EUR'): 0.952,
                    ('GBP', 'EUR'): 1.12}
            },
        }
        exposure = OedExposure(**config)
        with tempfile.TemporaryDirectory() as tmp_run_dir:
            exposure.location.save('copy', os.path.join(tmp_run_dir, 'location.csv'))
            exposure.account.save('copy', os.path.join(tmp_run_dir, 'account.csv'))
            exposure.ri_info.save('copy', os.path.join(tmp_run_dir, 'ri_info.csv'))
            exposure.ri_scope.save('copy', os.path.join(tmp_run_dir, 'ri_scope.csv'))

            config_copy = {
                'location': os.path.join(tmp_run_dir, 'location.csv'),
                'account': os.path.join(tmp_run_dir, 'account.csv'),
                'ri_info': os.path.join(tmp_run_dir, 'ri_info.csv'),
                'ri_scope': os.path.join(tmp_run_dir, 'ri_scope.csv'),
                'currency_conversion': {
                    "currency_conversion_type": "DictBasedCurrencyRates",
                    "source_type": "dict",
                    "currency_rates": {('USD', 'GBP'): 0.85,
                                       ('USD', 'EUR'): 0.952,
                                       ('GBP', 'EUR'): 1.12}
                },
                'reporting_currency': 'USD',
            }
            exposure2 = OedExposure(**config_copy)
            exposure.reporting_currency = 'GBP'
            exposure.reporting_currency = 'USD'
            pd.testing.assert_frame_equal(exposure.location.dataframe,
                                          exposure2.location.dataframe)
            pd.testing.assert_frame_equal(exposure.account.dataframe,
                                          exposure2.account.dataframe)
            pd.testing.assert_frame_equal(exposure.ri_info.dataframe,
                                          exposure2.ri_info.dataframe)
            pd.testing.assert_frame_equal(exposure.ri_scope.dataframe,
                                          exposure2.ri_scope.dataframe)
            self.assertTrue((exposure.location.dataframe['LocCurrency'] == 'USD').all())
            # # load portfolio from folder => load the info at specific address, oed_info.json

    def test_convert_to_parquet(self):
        with tempfile.TemporaryDirectory() as tmp_run_dir:
            config = {'location': base_url + '/SourceLocOEDPiWind.csv',
                      'account': base_url + '/SourceAccOEDPiWind.csv',
                      'ri_info': base_url + '/SourceReinsInfoOEDPiWind.csv',
                      'ri_scope': base_url + '/SourceReinsScopeOEDPiWind.csv',
                      }
            with open(pathlib.Path(tmp_run_dir, 'config.json'), 'w') as config_json:
                json.dump(config, config_json)

            convert(config_json=str(pathlib.Path(tmp_run_dir, 'config.json')),
                    output_dir=pathlib.Path(tmp_run_dir, 'json'),
                    compression='parquet')
            convert(**config,
                    output_dir=pathlib.Path(tmp_run_dir, 'direct'),
                    compression='parquet')

            for folder in ['json', 'direct']:
                for oed_name in ['Loc', 'Acc', 'ReinsInfo', 'ReinsScope']:
                    self.assertTrue(
                        os.path.isfile(pathlib.Path(tmp_run_dir, folder, f'Source{oed_name}OEDPiWind.parquet')))

    def test_validation_raise_exception(self):
        config = {'location': base_url + '/SourceLocOEDPiWind10.csv',
                  'account': base_url + '/SourceAccOEDPiWind.csv',
                  'ri_info': base_url + '/SourceReinsInfoOEDPiWind.csv',
                  'ri_scope': base_url + '/SourceReinsScopeOEDPiWind.csv',
                  }
        exposure = OedExposure(**config)

        # create invalid data
        exposure.location.dataframe.loc[0:5, ('BuildingTIV',)] = -1
        exposure.location.dataframe['LocLimitType6All'] = 0
        exposure.location.dataframe['LocLimit6All'] = 10000
        exposure.location.dataframe['CondTag'] = '1'  # check CR only apply to relevant source type
        exposure.account.dataframe.loc[1:2, ('LayerParticipation',)] = 2
        exposure.ri_info.dataframe['ReinsPeril'] = 'FOOBAR'

        with pytest.raises(OdsException) as e:
            exposure.check()
            self.assertTrue("column 'BuildingTIV' has values outside range." in e.msg)
            self.assertTrue("column 'LayerParticipation' has values outside range." in e.msg)
            self.assertTrue("ReinsPeril has invalid perils." in e.msg)
            self.assertTrue("Conditionally required column missing" in e.msg and 'LocPeril' in e.msg)
            self.assertfalse('CondPriority' in e.msg)

    # load non utf-8 file
    def test_load_non_utf8(self):
        config = {'location': str(pathlib.Path(base_test_path, 'non_utf8_loc.csv'))
                  }
        exposure = OedExposure(**config)
        self.assertTrue(exposure.location.dataframe['StreetAddress'][0] == 'Ô, Avenue des Champs-Élysées')

    def test_aliases_columns(self):
        location_df = pd.DataFrame({
            'PortNumber': [1, 1],
            'AccNumber': [1, 2],
            'LocNumberAlias': [1, 2],
            'CountryCode': ['GB', 'FR'],
            'LocPerilsCovered': 'WTC',
            'BuildingTIV': ['1000', '20000'],
            'ContentsTIV': [0, 0],
            'LocCurrency': ['GBP', 'EUR']})
        with tempfile.TemporaryDirectory() as tmp_run_dir:
            # create a custom schema to init the test
            custom_schema_path = pathlib.Path(tmp_run_dir, 'custom_schema.json')
            with open(OedSchema.DEFAULT_ODS_SCHEMA_PATH) as default_schema_file, open(custom_schema_path,
                                                                                      'w') as custom_schema_file:
                default_schema = json.load(default_schema_file)
                default_schema['input_fields']['Loc']['locnumber']['alias'] = 'LocNumberAlias'
                json.dump(default_schema, custom_schema_file)

            exposure = OedExposure(**{
                'location': location_df,
                'oed_schema_info': custom_schema_path,
                'use_field': True,
                'check_oed': True})
            pd.testing.assert_series_equal(exposure.location.dataframe['LocNumber'],
                                           location_df['LocNumberAlias'].astype(str).astype('category'),
                                           check_names=False, check_categorical=False)

    def test_load_exposure_from_different_directory(self):
        """
        Check that exposure loads correctly after it has been moved to another place
        """
        config = {'location': base_url + '/SourceLocOEDPiWind.csv',
                  'account': base_url + '/SourceAccOEDPiWind.csv', }

        with tempfile.TemporaryDirectory() as tmp_save_dir, tempfile.TemporaryDirectory() as tmp_move_dir:
            exposure_save = OedExposure(**config)
            exposure_save.save(pathlib.Path(tmp_save_dir, 'oed'), save_config=True)

            shutil.move(str(pathlib.Path(tmp_save_dir, 'oed')), str(pathlib.Path(tmp_move_dir)))

            exposure_move = OedExposure.from_dir(pathlib.Path(tmp_move_dir, 'oed'))
            pd.testing.assert_frame_equal(exposure_save.location.dataframe, exposure_move.location.dataframe)
            pd.testing.assert_frame_equal(exposure_save.account.dataframe, exposure_move.account.dataframe)

    def test_field_required_allow_blank_are_set_to_default(self):
        original_exposure = OedExposure(**{
            'location': base_url + '/SourceLocOEDPiWind.csv',
            'account': base_url + '/SourceAccOEDPiWind.csv',
            'ri_info': base_url + '/SourceReinsInfoOEDPiWind.csv',
            'ri_scope': base_url + '/SourceReinsScopeOEDPiWind.csv',
            'use_field': True})

        original_exposure.location.dataframe.drop(columns=['ContentsTIV'], inplace=True)
        original_exposure.location.dataframe['BITIV'] = np.nan
        original_exposure.ri_info.dataframe.drop(columns='RiskLevel', inplace=True)

        with tempfile.TemporaryDirectory() as tmp_dir:
            original_exposure.save(pathlib.Path(tmp_dir, 'oed'), save_config=True)
            modified_exposure = OedExposure.from_dir(pathlib.Path(tmp_dir, 'oed'))

            assert (modified_exposure.location.dataframe['ContentsTIV'] == 0).all()  # check column is added with correct value
            assert (modified_exposure.location.dataframe['BITIV'] == 0).all()  # check default is applied
            assert (modified_exposure.ri_info.dataframe['RiskLevel'] == '').all()  # check it works for string

    def test_relative_and_absolute_path(self):
        original_cwd = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                abs_dir = pathlib.Path(tmp_dir, "abs")
                abs_dir.mkdir()
                with urllib.request.urlopen(base_url + '/SourceLocOEDPiWind10.csv') as response, \
                        open(pathlib.Path(tmp_dir, 'SourceLocOEDPiWind10.csv'), 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                with urllib.request.urlopen(base_url + '/SourceAccOEDPiWind.csv') as response, \
                        open(pathlib.Path(abs_dir, 'SourceAccOEDPiWind.csv'), 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)

                os.chdir(tmp_dir)
                original_exposure = OedExposure(**{
                    'location': 'SourceLocOEDPiWind10.csv',  # relative path
                    'account': str(abs_dir) + '/SourceAccOEDPiWind.csv', })   # absolute path
                original_exposure.check()
        finally:
            os.chdir(original_cwd)

    def test_NA_value_are_not_nan(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            loc_path = pathlib.Path(tmp_dir, 'location.csv')
            pd.DataFrame({
                'PortNumber': [1, 1],
                'AccNumber': [1, 2],
                'LocNumber': [1, 2],
                'CountryCode': ['NA', 'FR'],
                'LocPerilsCovered': 'WTC',
                'BuildingTIV': ['1000', '20000'],
                'ContentsTIV': [0, 0],
                'LocCurrency': ['GBP', 'EUR']}).to_csv(loc_path)
            oed = OedExposure(**{'location': loc_path})
            assert oed.location.dataframe['CountryCode'][0] == 'NA'

    def test_unknown_column_management(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            loc_path = pathlib.Path(tmp_dir, 'location.csv')
            df = pd.DataFrame({
                'PortNumber': [1, 1],
                'AccNumber': [1, 2],
                'LocNumber': [1, 2],
                'CountryCode': ['NA', 'FR'],
                'LocPerilsCovered': 'WTC',
                'BuildingTIV': ['1000', '20000'],
                'ContentsTIV': [0, 0],
                'LocCurrency': ['GBP', 'EUR'],
                'ToRename': [1, 2],
                'ToDelete': [3, 4],
                'ToIgnore': [5, 6],
                'default_rename': [7, 8],
            })
            oed = OedExposure(**{'location': df})
            save_option = {
                'ToRename': UnknownColumnSaveOption.RENAME,
                'ToDelete': UnknownColumnSaveOption.DELETE,
                'ToIgnore': UnknownColumnSaveOption.IGNORE,
                'default': UnknownColumnSaveOption.RENAME,
            }

            oed.save(tmp_dir, unknown_columns=save_option)
            oed_saved = OedExposure(**{'location': loc_path})

            assert 'ToIgnore' in oed_saved.location.dataframe.columns
            assert 'FlexiLocToRename' in oed_saved.location.dataframe.columns
            assert 'ToDelete' not in oed_saved.location.dataframe.columns
            assert 'FlexiLocdefault_rename' in oed_saved.location.dataframe.columns

    def test_setting_schema_analysis__is_valid(self):
        file_name = 'analysis_settings.json'
        file_url = f'https://raw.githubusercontent.com/OasisLMF/OasisPiWind/{piwind_branch}/{file_name}'
        ods_analysis_setting = AnalysisSettingSchema()
        assert (ods_analysis_setting.schema is not None)

        with tempfile.TemporaryDirectory() as tmp_dir:
            abs_dir = pathlib.Path(tmp_dir, "abs")
            abs_dir.mkdir()

            with urllib.request.urlopen(file_url) as response, \
                    open(pathlib.Path(tmp_dir, 'analysis_settings.json'), 'wb') as out_file:
                shutil.copyfileobj(response, out_file)

            settings_fp = pathlib.Path(tmp_dir, 'analysis_settings.json')
            settings_dict = ods_analysis_setting.load(settings_fp)
            valid, errors = ods_analysis_setting.validate(settings_dict)

            self.assertTrue(valid)
            self.assertEqual({}, errors)
            self.assertEqual(settings_dict, ods_analysis_setting.get(settings_fp))

    def test_setting_schema_analysis__is_invalid(self):
        file_name = 'analysis_settings.json'
        file_url = f'https://raw.githubusercontent.com/OasisLMF/OasisPiWind/{piwind_branch}/{file_name}'
        ods_analysis_setting = AnalysisSettingSchema()
        assert (ods_analysis_setting.schema is not None)

        with tempfile.TemporaryDirectory() as tmp_dir:
            abs_dir = pathlib.Path(tmp_dir, "abs")
            abs_dir.mkdir()

            with urllib.request.urlopen(file_url) as response, \
                    open(pathlib.Path(tmp_dir, 'analysis_settings.json'), 'wb') as out_file:
                shutil.copyfileobj(response, out_file)

            settings_fp = pathlib.Path(tmp_dir, 'analysis_settings.json')
            settings_dict = ods_analysis_setting.load(settings_fp)

            # Insert errors
            settings_dict['gul_summarie'] = settings_dict['gul_summaries']
            del settings_dict['gul_summaries']
            settings_dict['gul_output'] = "True"  # should be a bool

            valid, errors = ods_analysis_setting.validate(settings_dict, raise_error=False)
            self.assertFalse(valid)
            self.assertEqual({'gul_output': ["'True' is not of type 'boolean'"],
                              'required': ["'gul_summaries' is a required property"]}, errors)

            with self.assertRaises(OdsException):
                ods_analysis_setting.validate(settings_dict)

    def test_setting_schema_model__is_valid(self):
        file_name = 'meta-data/model_settings.json'
        file_url = f'https://raw.githubusercontent.com/OasisLMF/OasisPiWind/{piwind_branch}/{file_name}'
        ods_model_setting = ModelSettingSchema()
        assert (ods_model_setting.schema is not None)

        with tempfile.TemporaryDirectory() as tmp_dir:
            abs_dir = pathlib.Path(tmp_dir, "abs")
            abs_dir.mkdir()

            with urllib.request.urlopen(file_url) as response, \
                    open(pathlib.Path(tmp_dir, 'model_settings.json'), 'wb') as out_file:
                shutil.copyfileobj(response, out_file)

            settings_fp = pathlib.Path(tmp_dir, 'model_settings.json')
            settings_dict = ods_model_setting.load(settings_fp)
            valid, errors = ods_model_setting.validate(settings_dict)

            self.assertTrue(valid)
            self.assertEqual({}, errors)
            self.assertEqual(settings_dict, ods_model_setting.get(settings_fp))

    def test_setting_schema_model__is_invalid(self):
        file_name = 'meta-data/model_settings.json'
        file_url = f'https://raw.githubusercontent.com/OasisLMF/OasisPiWind/{piwind_branch}/{file_name}'
        ods_model_setting = ModelSettingSchema()
        assert (ods_model_setting.schema is not None)

        with tempfile.TemporaryDirectory() as tmp_dir:
            abs_dir = pathlib.Path(tmp_dir, "abs")
            abs_dir.mkdir()

            with urllib.request.urlopen(file_url) as response, \
                    open(pathlib.Path(tmp_dir, 'model_settings.json'), 'wb') as out_file:
                shutil.copyfileobj(response, out_file)

            settings_fp = pathlib.Path(tmp_dir, 'model_settings.json')
            settings_dict = ods_model_setting.load(settings_fp)

            # Insert errors
            settings_dict['unknown_key'] = 'foobar'
            settings_dict['model_setting'] = settings_dict['model_settings']
            del settings_dict['model_settings']

            valid, errors = ods_model_setting.validate(settings_dict, raise_error=False)
            self.assertFalse(valid)
            self.assertEqual({
                'additionalProperties': ["Additional properties are not allowed ('model_setting', 'unknown_key' were unexpected)"],
                'required': ["'model_settings' is a required property"]
            }, errors)

            with self.assertRaises(OdsException):
                ods_model_setting.validate(settings_dict)

    def test_empty_dataframe_logged(self):
        loc_df = pd.DataFrame({
            'PortNumber': [],
            'AccNumber': [],
            'LocNumber': [],
            'CountryCode': [],
            'LocPerilsCovered': [],
            'BuildingTIV': [],
            'ContentsTIV': [],
            'LocCurrency': []})
        with self._caplog.at_level(logging.INFO):
            oed = OedExposure(**{'location': loc_df, 'use_field': True})
            oed.check()
        assert 'location ' in self._caplog.text
        assert 'is empty' in self._caplog.text

    def test_to_version_with_invalid_format(self):
        oed_exposure = OedExposure(
            location=base_url + "/SourceLocOEDPiWind.csv",
            account=base_url + "/SourceAccOEDPiWind.csv",
            ri_info=base_url + "/SourceReinsInfoOEDPiWind.csv",
            ri_scope=base_url + "/SourceReinsScopeOEDPiWind.csv",
            use_field=True,
        )

        if "versioning" in oed_exposure.oed_schema.schema:
            with pytest.raises(ValueError, match="Version should be provided in 'major.minor' format, e.g. 3.2, 7.4, etc."):
                oed_exposure.to_version("3.x")
        else:
            assert True

    def test_to_version_with_valid_format(self):
        oed_exposure = OedExposure(
            location=base_url + "/SourceLocOEDPiWind.csv",
            account=base_url + "/SourceAccOEDPiWind.csv",
            ri_info=base_url + "/SourceReinsInfoOEDPiWind.csv",
            ri_scope=base_url + "/SourceReinsScopeOEDPiWind.csv",
            use_field=True,
        )

        if "versioning" in oed_exposure.oed_schema.schema:
            with pytest.raises(ValueError, match="Version 2.8 is not a known version present in the OED schema."):
                oed_exposure.to_version("2.8")
        else:
            assert True

    def test_versioning_fallback(self):
        oed_exposure = OedExposure(
            location=base_url + "/SourceLocOEDPiWind.csv",
            account=base_url + "/SourceAccOEDPiWind.csv",
            ri_info=base_url + "/SourceReinsInfoOEDPiWind.csv",
            ri_scope=base_url + "/SourceReinsScopeOEDPiWind.csv",
            use_field=True,
        )

        if "versioning" not in oed_exposure.oed_schema.schema:
            oed_exposure.oed_schema.schema["versioning"] = {}

        oed_exposure.oed_schema.schema["versioning"] = {
            "1.9": [
                {
                    "Category": "Occupancy",
                    "New code": 9999,
                    "Fallback": 9998
                }
            ]
        }

        # Modify the first line of exposure.location.dataframe
        oed_exposure.location.dataframe.loc[0, "OccupancyCode"] = 9999

        # Convert
        oed_exposure.to_version("1.9")

        # # Assert the OccupancyCode is as expected
        assert oed_exposure.location.dataframe.loc[0, "OccupancyCode"] == 9998
