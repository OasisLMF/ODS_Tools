import functools
import json
import numpy as np
import logging

from pathlib import Path
from collections.abc import Iterable

from .common import (OdsException, OED_PERIL_COLUMNS, OED_IDENTIFIER_FIELDS, DEFAULT_VALIDATION_CONFIG, CLASS_OF_BUSINESSES,
                     VALIDATOR_ON_ERROR_ACTION, BLANK_VALUES, is_empty)
from .oed_schema import OedSchema

logger = logging.getLogger(__name__)


class Validator:
    def __init__(self, exposure):
        """
        create a Validator object for exposure data
        Args:
            exposure: OedExposure object
        """
        self.exposure = exposure

        self.column_to_field_maps = {}
        self.identifier_field_maps = {}
        self.field_to_column_maps = {}
        for oed_source in exposure.get_oed_sources():
            self.column_to_field_maps[oed_source] = oed_source.get_column_to_field()
            self.identifier_field_maps[oed_source] = [column for column, field_info in self.column_to_field_maps[oed_source].items()
                                                      if field_info['Input Field Name'] in OED_IDENTIFIER_FIELDS[oed_source.oed_type]]
            field_to_column = {}
            self.field_to_column_maps[oed_source] = field_to_column
            for column, field_info in self.column_to_field_maps[oed_source].items():
                if field_info['Input Field Name'] in field_to_column:
                    if field_info['Input Field Name'].endswith('XX') or field_info['Input Field Name'].endswith('ZZZ'):
                        if not isinstance(field_to_column[field_info['Input Field Name']], list):
                            field_to_column[field_info['Input Field Name']] = [field_to_column[field_info['Input Field Name']]]
                        field_to_column[field_info['Input Field Name']].append(column)

                    else:
                        raise OdsException(f"Oed file {oed_source.oed_name}, {oed_source.current_source}"
                                           f" contain multiple instances of unique field {field_info['Input Field Name']}"
                                           f" {field_to_column[field_info['Input Field Name']]} and {column}")
                else:
                    field_to_column[field_info['Input Field Name']] = column

    def __call__(self, validation_config):
        """
        run all check from validation_config
        Args:
            validation_config = list of checks to perform with their action
                - ex [{'name': 'required_fields', 'on_error': 'raise'}, ...]

        Returns:
            list of errors from check with on_error "return"
        """
        if validation_config is None:
            validation = DEFAULT_VALIDATION_CONFIG
        elif isinstance(validation_config, Iterable):
            validation = validation_config
        elif isinstance(validation_config, [str, Path]):
            validation = json.load(validation_config)
        else:
            raise OdsException("Unsupported validation type")

        invalid_data_group = {}
        for check in validation:
            check_fct = getattr(self, 'check_' + str(check['name']), None)
            if check.get('on_error') not in VALIDATOR_ON_ERROR_ACTION:
                raise OdsException('Unknown check on_error action' + str(check.get('on_error')))
            if check['on_error'] == 'ignore':
                continue
            if hasattr(check_fct, '__call__'):
                invalid_data_group.setdefault(check['on_error'], []).extend(check_fct())
            else:
                raise OdsException('Unknown check name ' + str(check['name']))

        raise_msg = invalid_data_group.get('raise', [])
        log_msg = invalid_data_group.get('log', [])
        return_msg = invalid_data_group.get('return', [])

        def invalid_data_to_str(_data):
            return f"in {_data['name']} {_data['source']}\n {_data['msg']}"

        for invalid_data in log_msg:
            logger.warning(invalid_data_to_str(invalid_data))
        if raise_msg:
            raise OdsException('\n'.join(invalid_data_to_str(invalid_data) for invalid_data in raise_msg))
        return return_msg

    def check_source_coherence(self):
        """"""
        invalid_data = []
        coherence_rules = CLASS_OF_BUSINESSES[self.exposure.class_of_business]['coherence_rules']
        for coherence_rule in coherence_rules:
            r_sources = []
            if coherence_rule["type"] == "CR":
                c_sources = [getattr(self.exposure, source) for source in coherence_rule["c_sources"]]
                if any(c_sources):
                    if not all(c_sources):
                        invalid_data.append(
                            {'name': coherence_rule['name'], 'source': None,
                             'msg': f"Exposure needs all {coherence_rule['c_sources']} for {coherence_rule['name']}"
                                    f" got {c_sources}"})
                    r_sources = [getattr(self.exposure, source) for source in coherence_rule.get("r_sources", [])]
            elif coherence_rule["type"] == "R":
                r_sources = [getattr(self.exposure, source) for source in coherence_rule["r_sources"]]

            if not all(r_sources):
                invalid_data.append({'name': coherence_rule['name'], 'source': None,
                                     'msg': f"Exposure needs {coherence_rule['r_sources']}, got={r_sources}"})

        return invalid_data

    def check_required_fields(self):
        """
        using Oed input_field definition, check all require field
        error is raised if
         - required column is missing
         - value is missing and "Allow blanks?" == 'NO'
        Returns:
            list of invalid_data
        """

        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            input_fields = oed_source.get_input_fields()
            identifier_field = self.identifier_field_maps[oed_source]
            field_to_columns = self.field_to_column_maps[oed_source]

            for field_info in input_fields.values():
                if field_info['Input Field Name'] not in field_to_columns:
                    requ_field_ref = CLASS_OF_BUSINESSES[self.exposure.class_of_business]['field_status_name']
                    if requ_field_ref not in field_info:  # OED v3 only support PROP and used 'Required Field'
                        requ_field_ref = 'Required Field'
                    if field_info.get(requ_field_ref) == 'R':
                        invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                             'msg': f"missing required column {field_info['Input Field Name']}"})
                    continue
                columns = field_to_columns[field_info['Input Field Name']]
                if isinstance(columns, str):
                    columns = [columns]
                for column in columns:
                    blanks_not_allowed = any([
                        field_info.get("Allow blanks?", '').upper() == 'NO',       # OED v3
                        field_info.get('Property field status', '').upper() == 'R'  # OED v4
                    ])
                    if blanks_not_allowed:
                        missing_value_df = oed_source.dataframe[is_empty(oed_source.dataframe, column)]
                        if not missing_value_df.empty:
                            invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                                 'msg': f"column '{column}' has missing values in \n"
                                                        f"{missing_value_df[identifier_field + [column]]}"})
        return invalid_data

    def check_unknown_column(self):
        """
        using Oed input_field definition, check that all column are OED column
        Returns:
            list of invalid_data
        """

        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            column_to_field = self.column_to_field_maps[oed_source]
            for column in oed_source.dataframe.columns:
                if column not in column_to_field:
                    invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                         'msg': f"column '{column}' is not a valid oed field"})
        return invalid_data

    def check_valid_values(self):
        """
        using Oed input_field definition, check that values are valid for field that define a 'Valid value range'
        Returns:
            list of invalid_data
        """
        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            column_to_field = self.column_to_field_maps[oed_source]
            identifier_field = self.identifier_field_maps[oed_source]
            for column, field_info in column_to_field.items():
                valid_ranges = field_info['Valid value range']
                blanks_allowed = any([
                    field_info.get('Allow blanks?', '').lower() == 'yes',       # OED v3
                    field_info.get('Property field status', '').upper() != 'R',  # OED v4
                ])

                if valid_ranges != 'n/a':
                    is_valid_value = functools.partial(OedSchema.is_valid_value,
                                                       valid_ranges=valid_ranges,
                                                       allow_blanks=blanks_allowed)
                    invalid_range_data = oed_source.dataframe[~oed_source.dataframe[column].apply(is_valid_value)]
                    if not invalid_range_data.empty:
                        invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                             'msg': f"column '{column}' has values outside range.\n"
                                                    f"{invalid_range_data[identifier_field + [column]]}"})
        return invalid_data

    def check_perils(self):
        """
        using Oed perils list specification, check that Peril column have valid peril values
        Returns:
            list of invalid_data
        """
        valid_perils = set(self.exposure.oed_schema.schema['perils']['info'])

        def invalid_fct(perils):
            invalid = []
            for peril in perils.split(';'):
                if peril not in valid_perils:
                    invalid.append(peril)
            return ';'.join(invalid)

        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            identifier_field = self.identifier_field_maps[oed_source]
            if oed_source.dataframe.empty:
                continue
            for column in oed_source.dataframe.columns.intersection(set(OED_PERIL_COLUMNS)):
                invalid_perils_col = oed_source.dataframe[column].apply(invalid_fct)
                invalid_perils = oed_source.dataframe[invalid_perils_col != '']
                invalid_perils[column] = invalid_perils_col.loc[invalid_perils_col != '']
                if not invalid_perils.empty:
                    invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                         'msg': f"{column} has invalid perils.\n"
                                                f"{invalid_perils[identifier_field + [column]]}"})
        return invalid_data

    def check_occupancy_code(self):
        """
        using Oed occupancy_code list specification, check that occupancy_code column have valid occupancy_code values
        Returns:
            list of invalid_data
        """
        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            occupancy_code_column = self.field_to_column_maps[oed_source].get('OccupancyCode')
            if occupancy_code_column is None:
                continue
            identifier_field = self.identifier_field_maps[oed_source]
            invalid_occupancy_code = oed_source.dataframe[~(np.isin(oed_source.dataframe[occupancy_code_column].astype(str),
                                                                    list(self.exposure.oed_schema.schema['occupancy']))
                                                            | is_empty(oed_source.dataframe, occupancy_code_column))]
            if not invalid_occupancy_code.empty:
                invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                     'msg': f"invalid OccupancyCode.\n"
                                            f"{invalid_occupancy_code[identifier_field + [occupancy_code_column]]}"})
        return invalid_data

    def check_construction_code(self):
        """
        using Oed occupancy_code list specification, check that occupancy_code column have valid occupancy_code values
        Returns:
            list of invalid_data
        """
        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            construction_code_column = self.field_to_column_maps[oed_source].get('ConstructionCode')
            if construction_code_column is None:
                continue
            identifier_field = self.identifier_field_maps[oed_source]
            invalid_construction_code = oed_source.dataframe[~(np.isin(oed_source.dataframe[construction_code_column].astype(str),
                                                                       list(self.exposure.oed_schema.schema['construction']))
                                                               | is_empty(oed_source.dataframe, construction_code_column))]
            if not invalid_construction_code.empty:
                invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                     'msg': f"invalid ConstructionCode.\n"
                                            f"{invalid_construction_code[identifier_field + [construction_code_column]]}"})
        return invalid_data

    def check_country_and_area_code(self):
        """
        using Oed country_and_area_code list specification,
        check that country and area_code column have valid country or (country and area_code) pair values
        Returns:
            list of invalid_data
        """
        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            country_code_column = self.field_to_column_maps[oed_source].get('CountryCode')
            if country_code_column is None:
                continue
            identifier_field = self.identifier_field_maps[oed_source]
            area_code_column = self.field_to_column_maps[oed_source].get('AreaCode')
            if area_code_column is not None:
                country_only_df = oed_source.dataframe[is_empty(oed_source.dataframe, area_code_column)]
                country_area_df = oed_source.dataframe[~is_empty(oed_source.dataframe, area_code_column)]

                invalid_country_area = (country_area_df[
                    ~(country_area_df[[country_code_column, area_code_column]]
                      .apply(tuple, axis=1)
                      .isin(self.exposure.oed_schema.schema['country_area'])
                      )]
                )
                if not invalid_country_area.empty:
                    invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                         'msg': f"invalid CountryCode AreaCode pair.\n"
                                                f"{invalid_country_area[identifier_field + [country_code_column, area_code_column]]}"})
            else:
                country_only_df = oed_source.dataframe
            invalid_country = (country_only_df[~(np.isin(country_only_df[country_code_column],
                                                         list(self.exposure.oed_schema.schema['country']))
                                                 | is_empty(country_only_df, country_code_column))])
            if not invalid_country.empty:
                invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                     'msg': f"invalid CountryCode.\n"
                                            f"{invalid_country[identifier_field + [country_code_column]]}"})
        return invalid_data

    def check_conditional_requirement(self):
        invalid_data = []
        for oed_source in self.exposure.get_oed_sources():
            cr_field = self.exposure.oed_schema.schema['cr_field'].get(oed_source.oed_type)
            if not cr_field:
                continue
            column_to_field = self.column_to_field_maps[oed_source]
            identifier_field = self.identifier_field_maps[oed_source]

            def check_cr(rec):
                cr_fields = set()
                for col in column_to_field:
                    field_info = column_to_field[col]
                    if field_info['Input Field Name'] not in cr_field:
                        continue

                    if field_info['Default'] != 'n/a':
                        if oed_source.dataframe[col].dtype.name == 'category':
                            default_set = {field_info['Default']}
                        else:
                            default_set = {oed_source.dataframe[col].dtype.type(field_info['Default'])}
                    else:
                        default_set = set()

                    if rec[col] not in BLANK_VALUES | default_set and rec[col]:
                        cr_fields |= set(cr_field[field_info['Input Field Name']])
                msg = []
                for field in cr_fields:
                    col = self.field_to_column_maps[oed_source].get(field)
                    if col is None or rec[col] in BLANK_VALUES:
                        msg.append(f'{self.field_to_column_maps[oed_source].get(field) or field}')

                return ', '.join(msg)

            cr_msg = oed_source.dataframe.apply(check_cr, axis=1)
            missing_data_df = oed_source.dataframe[cr_msg != ''].copy()
            if not missing_data_df.empty:
                missing_data_df['missing value'] = cr_msg
                invalid_data.append({'name': oed_source.oed_name, 'source': oed_source.current_source,
                                     'msg': f"Conditionally required column missing .\n"
                                            f"{missing_data_df[identifier_field + ['missing value']]}"})

        return invalid_data
