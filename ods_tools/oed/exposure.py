"""
This package manage:
    loading an OED schema
    the loading, checking and saving of OED files
    support conversion of OED to other currencies
"""

import json
from copy import deepcopy
import logging
import numpy as np
from packaging import version
from pathlib import Path

from .common import (PANDAS_COMPRESSION_MAP,
                     USUAL_FILE_NAME, OED_TYPE_TO_NAME,
                     UnknownColumnSaveOption, CLASS_OF_BUSINESSES, OdsException,
                     ClassOfBusiness)
from .oed_schema import OedSchema
from .source import OedSource
from .validator import Validator
from .forex import create_currency_rates

logger = logging.getLogger(__name__)


class OedExposure:
    """
    Object grouping all the OED files related to the exposure Data (location, account, ri_info, ri_scope)
    and the OED schema to follow
    """
    DEFAULT_EXPOSURE_CONFIG_NAME = 'exposure_info.json'

    def __init__(self, *,
                 location=None,
                 account=None,
                 ri_info=None,
                 ri_scope=None,
                 oed_schema_info=None,
                 currency_conversion=None,
                 reporting_currency=None,
                 class_of_business=None,
                 check_oed=False,
                 use_field=False,
                 validation_config=None,
                 working_dir=None,
                 location_numbers=None,
                 account_numbers=None,
                 portfolio_numbers=None,
                 base_df_engine=None,
                 exposure_df_engine=None):
        """
        Create an OED object,
        each input can be the object itself or  information that will be used to create the object

        Args:
            location (path or dict or OedSource or pd.DataFrame): info for location
            account (path or dict or OedSource or pd.DataFrame): info for account
            ri_info (path or dict or OedSource or pd.DataFrame): info for ri_info
            ri_scope (path or dict or OedSource or pd.DataFrame): info for ri_scope
            oed_schema_info (path_to_json, OedSchema, None): info for oed_schema
            currency_conversion (path_to_json or dict or None): info  currency_conversion
            reporting_currency (str): currency to convert
            check_oed (bool): check if OED files are valid or not
            use_field (bool): if true column name are converted to OED field name on load
            location_numbers (list[str]): A list of location numbers to filter the input data by
            account_numbers (list[str]): A list of account numbers to filter the input data by
            portfolio_numbers (list[str]): A list of portfolio numbers to filter the input data by
            base_df_engine (Union[str, InputReaderConfig]): The default engine to use when loading dataframes
            exposure_df_engine (Union[str, InputReaderConfig]):
                The exposure specific engine to use when loading dataframes
        """
        self.use_field = use_field
        self.oed_schema = OedSchema.from_oed_schema_info(oed_schema_info)
        df_engine = (
            exposure_df_engine or
            base_df_engine or
            'oasis_data_manager.df_reader.reader.OasisPandasReader'
        )

        def filter_col_in(column, values):
            def fn(df):
                if column not in df.columns or not values:
                    return df

                return df[np.isin(df[column], values)]
            return fn

        loc_filters = [
            filter_col_in("LocNumber", location_numbers),
            filter_col_in("AccNumber", account_numbers),
            filter_col_in("PortNumber", portfolio_numbers),
        ]
        self.location = OedSource.from_oed_info(
            exposure=self,
            oed_type='Loc',
            oed_info=self.resolve_oed_info(location, df_engine),
            filters=loc_filters,
        )

        acc_filters = [
            filter_col_in("AccNumber", account_numbers),
            filter_col_in("PortNumber", portfolio_numbers),
        ]
        self.account = OedSource.from_oed_info(
            exposure=self,
            oed_type='Acc',
            oed_info=self.resolve_oed_info(account, df_engine),
            filters=acc_filters,
        )

        self.ri_info = OedSource.from_oed_info(
            exposure=self,
            oed_type='ReinsInfo',
            oed_info=self.resolve_oed_info(ri_info, df_engine),
        )

        ri_scope_filters = [
            filter_col_in("LocNumber", location_numbers),
            filter_col_in("AccNumber", account_numbers),
            filter_col_in("PortNumber", portfolio_numbers),
        ]
        self.ri_scope = OedSource.from_oed_info(
            exposure=self,
            oed_type='ReinsScope',
            oed_info=self.resolve_oed_info(ri_scope, df_engine),
            filters=ri_scope_filters,
        )

        self.currency_conversion = create_currency_rates(currency_conversion)

        self.reporting_currency = reporting_currency

        self.class_of_business = class_of_business

        self.validation_config = validation_config

        if not working_dir:
            self.working_dir = Path('.').absolute()
        else:
            self.working_dir = working_dir

        if check_oed:
            self.check()

    def get_class_of_business(self):
        any_field_info = next(iter(self.get_input_fields('null').values()))
        if 'Required Field' in any_field_info:
            logger.debug(f"OED schema version < 4.0.0, only support {ClassOfBusiness.prop}")
            return ClassOfBusiness.prop
        class_of_businesses = set(CLASS_OF_BUSINESSES)
        exclusion_messages = {}

        for oed_source in self.get_oed_sources():
            present_field = set(field_info['Input Field Name'] for field_info in oed_source.get_column_to_field().values())
            for field_info in self.get_input_fields(oed_source.oed_type).values():
                for class_of_business in class_of_businesses:
                    cob_field_status = CLASS_OF_BUSINESSES[class_of_business]['field_status_name']
                    if field_info.get(cob_field_status) == 'R':
                        if field_info['Input Field Name'] not in present_field:
                            exclusion_messages.setdefault(class_of_business, {}).setdefault('missing', []).append(field_info['Input Field Name'])
                    elif field_info.get(cob_field_status) == 'n/a':
                        if field_info['Input Field Name'] in present_field:
                            exclusion_messages.setdefault(class_of_business, {}).setdefault('present', []).append(field_info['Input Field Name'])

        final_cobs = class_of_businesses.difference(exclusion_messages)
        if len(final_cobs) == 1:
            final_cobs = final_cobs.pop()
            logger.info(f"detected class of business is {final_cobs}")
            return final_cobs
        elif len(final_cobs) == 0:
            error_msg = "\n".join(f"{class_of_business}:"
                                  + ("\n    " + ", ".join(messages['missing']) + " missing" if messages.get('missing') else "")
                                  + ("\n    " + ", ".join(messages['present']) + " present" if messages.get('present') else "")
                                  for class_of_business, messages in exclusion_messages.items())
            raise OdsException(error_msg)
        elif len(final_cobs) == 2 and len(final_cobs.difference({ClassOfBusiness.prop, ClassOfBusiness.mar})) == 0:
            # Marine and Property have mostly the same column, default to Property if undetermined
            return ClassOfBusiness.prop
        else:
            raise OdsException(f"could not determine the COB of the exposure between those {final_cobs}")

    @property
    def class_of_business_info(self):
        if self.class_of_business is None:
            self.class_of_business = self.get_class_of_business()
        return CLASS_OF_BUSINESSES[self.class_of_business]

    @classmethod
    def resolve_oed_info(cls, oed_info, df_engine):
        if isinstance(oed_info, (str, Path)):
            return {
                "cur_version_name": "curr",
                "sources": {
                    "curr": {
                        "source_type": "filepath",
                        "filepath": oed_info,
                        "read_param": {},
                        "engine": df_engine
                    }
                }
            }
        elif isinstance(oed_info, dict):
            if "sources" in oed_info:
                oed_info = deepcopy(oed_info)
                for k in oed_info["sources"]:
                    oed_info["sources"][k].setdefault("engine", df_engine)
                return oed_info

        return oed_info

    @classmethod
    def from_config(cls, config_fp, **kwargs):
        """
        create an OedExposure from filepath to a config json file
        Args:
            config_fp (str): path
        Returns:
            OedExposure object
        """
        with open(config_fp) as config:
            kwargs['working_dir'] = Path(config_fp).parent
            return cls(**{**json.load(config), **kwargs})

    @classmethod
    def from_dir(cls, oed_dir, **kwargs):
        """
        create an OedExposure from directory path
        Args:
            oed_dir (str): path to the directory
        Returns:
            OedExposure object
        """
        def find_fp(names):
            for name in names:
                for extension in PANDAS_COMPRESSION_MAP.values():
                    if Path(oed_dir, name).with_suffix(extension).is_file():
                        return Path(oed_dir, name).with_suffix(extension)

        necessary_files = ["location"]
        files_found = {file: False for file in necessary_files}

        if Path(oed_dir, cls.DEFAULT_EXPOSURE_CONFIG_NAME).is_file():
            return cls.from_config(Path(oed_dir, cls.DEFAULT_EXPOSURE_CONFIG_NAME), **kwargs)
        else:
            config = {}
            for attr, filenames in USUAL_FILE_NAME.items():
                file_path = find_fp(filenames)
                if file_path and attr in files_found:
                    files_found[attr] = True
                config[attr] = file_path

            if Path(oed_dir, OedSchema.DEFAULT_ODS_SCHEMA_FILE).is_file():
                config['oed_schema_info'] = Path(oed_dir, OedSchema.DEFAULT_ODS_SCHEMA_FILE)
            kwargs['working_dir'] = oed_dir

            missing_files = [file for file, found in files_found.items() if not found]
            if missing_files and False:
                raise FileNotFoundError(f"Files not found in current path ({oed_dir}): {', '.join(missing_files)}")

            return cls(**{**config, **kwargs})

    @property
    def info(self):
        """info on exposure Data to be able to reload it"""
        info = {}
        for source in self.get_oed_sources():
            info[source.oed_name] = source.info
        info['oed_schema_info'] = self.oed_schema.info
        return info

    def get_input_fields(self, oed_type):
        """
        helper function to get OED input field info for a particular oed_type
        Args:
            oed_type:

        Returns:
            dict of OED input field info
        """
        return self.oed_schema.schema['input_fields'][oed_type]

    @property
    def reporting_currency(self):
        return self._reporting_currency

    @reporting_currency.setter
    def reporting_currency(self, reporting_currency):
        """setter for reporting_currency that will automatically convert currency in all OED files"""
        self._reporting_currency = reporting_currency
        if reporting_currency:
            for oed_source in self.get_oed_sources():
                oed_source.convert_currency()

    def get_oed_sources(self):
        """
        yield all the non-null oed_sources
        Returns:
            generator of all oed_sources
        """
        for oed_type, oed_name in OED_TYPE_TO_NAME.items():
            oed_source: OedSource = getattr(self, oed_name)
            if oed_source:
                yield oed_source

    def get_subject_at_risk_source(self) -> OedSource:
        if self.class_of_business is None:
            self.class_of_business = self.get_class_of_business()
        return getattr(self, CLASS_OF_BUSINESSES[self.class_of_business]['subject_at_risk_source'])

    def save_config(self, filepath):
        """
        save data to directory, loadable later on
        Args:
            filepath (str): path to save the config file
        """
        Path(filepath).parents[0].mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as info_file:
            json.dump(self.info, info_file, indent='  ', default=str)

    def save(self, path, version_name=None, compression=None, save_config=False, unknown_columns=UnknownColumnSaveOption.IGNORE):
        """
        helper function to save all OED data to a location with specific compression
        Args:
            version_name (str): use as a prefix for the file names, if None name will be taken from the cur_version file name
                          if it's a filepath
            path (str): output folder
            compression (str): type of compression to use
            save_config (bool): if true save the Exposure config as json
        """
        self.working_dir = path

        for oed_source in self.get_oed_sources():
            if version_name is None and oed_source.sources[oed_source.cur_version_name]['source_type'] == 'filepath':
                oed_name = Path(oed_source.sources[oed_source.cur_version_name]['filepath']).name
                if oed_source.cur_version_name.rsplit('_', 1)[-1] in PANDAS_COMPRESSION_MAP:
                    saved_version_name = oed_source.cur_version_name.rsplit('_', 1)[0]
                else:
                    saved_version_name = oed_source.cur_version_name
            elif version_name:
                oed_name = f'{version_name}_{OED_TYPE_TO_NAME[oed_source.oed_type]}'
                saved_version_name = version_name
            else:
                oed_name = f'{OED_TYPE_TO_NAME[oed_source.oed_type]}'
                saved_version_name = ''

            if save_config:  # we store filepath relative to config file
                filepath = Path(oed_name)
            else:
                filepath = Path(path, oed_name)

            if compression is None:
                if oed_source.sources[oed_source.cur_version_name]['source_type'] == 'filepath':
                    compression = oed_source.sources[oed_source.cur_version_name].get('extension')
                if compression is None:
                    compression = 'csv'

            filepath = filepath.with_suffix(PANDAS_COMPRESSION_MAP[compression])

            new_info = {'source_type': 'filepath', 'filepath': filepath, 'extension': compression}
            if "engine" in oed_source.sources[oed_source.cur_version_name]:
                new_info["engine"] = oed_source.sources[oed_source.cur_version_name]["engine"]

            oed_source.save(saved_version_name + '_' + f'{compression}', new_info, unknown_columns=unknown_columns)
        if save_config:
            self.save_config(Path(path, self.DEFAULT_EXPOSURE_CONFIG_NAME))

    def check(self, validation_config=None):
        """
        check that all OED files respect rules related to an OedSchema

        Args:
            validation_config (list): list of validation to perform, if None the default validation list will be used

        Returns:
            list of invalid data where validation action was 'return'

        Raises:
            OdsException if some invalid data is found

        """
        if self.class_of_business is None:
            self.class_of_business = self.get_class_of_business()

        if validation_config is None:
            validation_config = self.validation_config
        validator = Validator(self)
        return validator(validation_config)

    def to_version(self, to_version):
        """
        goes through location to convert columns to specific version.
        Right now it works for OccupancyCode and ConstructionCode.
        (please note that it only supports minor version changes in "major.minor" format, e.g. 3.2, 7.4, etc.)

        Args:
            version (str): specific version to roll to

        Returns:
            itself (OedExposure): updated object
        """
        def strip_version(version_string):
            parsed_version = version.parse(version_string)
            return f'{parsed_version.release[0]}.{parsed_version.release[1]}'

        # Parse version string
        try:
            target_version = strip_version(to_version)
        except version.InvalidVersion:
            raise ValueError(f"Invalid version: {to_version}")

        # Select which conversions to apply
        conversions = sorted(
            [
                ver
                for ver in self.oed_schema.schema["versioning"].keys()
                if version.parse(ver) >= version.parse(target_version)
            ],
            key=lambda x: version.parse(x),
            reverse=True,
        )

        # Check for the existence of OccupancyCode and ConstructionCode columns
        has_occupancy_code = "OccupancyCode" in self.location.dataframe.columns
        has_construction_code = "ConstructionCode" in self.location.dataframe.columns

        for ver in conversions:
            # Create a dictionary for each category
            replace_dict_occupancy = {}
            replace_dict_construction = {}

            for rule in self.oed_schema.schema["versioning"][ver]:
                if rule["Category"] == "Occupancy":
                    replace_dict_occupancy[rule["New code"]] = rule["Fallback"]
                elif rule["Category"] == "Construction":
                    replace_dict_construction[rule["New code"]] = rule["Fallback"]

            # Replace and log changes for OccupancyCode
            if has_occupancy_code:
                for key, value in replace_dict_occupancy.items():
                    # Check done in advance to log what is being changed
                    changes = self.location.dataframe["OccupancyCode"][self.location.dataframe["OccupancyCode"] == key].count()
                    if changes:
                        self.location.dataframe["OccupancyCode"].replace({key: value}, inplace=True)
                        logger.info(f"{key} -> {value}: {changes} occurrences in OccupancyCode.")

            # Replace and log changes for ConstructionCode
            if has_construction_code:
                for key, value in replace_dict_construction.items():
                    # Check done in advance to log what is being changed
                    changes = self.location.dataframe["ConstructionCode"][self.location.dataframe["ConstructionCode"] == key].count()
                    if changes:
                        self.location.dataframe["ConstructionCode"].replace({key: value}, inplace=True)
                        logger.info(f"{key} -> {value}: {changes} occurrences in ConstructionCode.")

        return self  # Return the updated object
