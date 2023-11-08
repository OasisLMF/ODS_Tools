import json
import os
from pathlib import Path
import numba as nb
import numpy as np

from .common import OdsException, BLANK_VALUES, cached_property

ENV_ODS_SCHEMA_PATH = os.getenv('ODS_SCHEMA_PATH')


# function to check if a subperil is part of a peril
@nb.jit(cache=True, nopython=True)
def __single_peril_filtering(peril_id, peril_filter, perils_dict):
    if peril_filter:
        for peril_filter in peril_filter.split(';'):
            for p in perils_dict[peril_filter]:
                if p == peril_id:
                    return True
    return False


@nb.jit(cache=True, nopython=True)
def jit_peril_filtering(peril_ids, peril_filters, perils_dict):
    result = np.empty_like(peril_ids, dtype=np.bool_)
    for i in range(peril_ids.shape[0]):
        result[i] = __single_peril_filtering(peril_ids[i], peril_filters[i], perils_dict)

    return result


class OedSchema:
    """
    Object managing information about a certain OED schema

    Attributes:
        schema (dict): information about the OED schema
        json_path (Path or str): path to the json file from where the schema was loaded
    """
    DEFAULT_ODS_SCHEMA_FILE = 'OpenExposureData_Spec.json'
    DEFAULT_ODS_SCHEMA_PATH = (ENV_ODS_SCHEMA_PATH if ENV_ODS_SCHEMA_PATH
                               else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', DEFAULT_ODS_SCHEMA_FILE))

    def __init__(self, schema, json_path):
        """
        Args:
            schema (dict): information about the OED schema
            json_path (Path or str): path to the json file from where the schema was loaded
        """
        self.schema = schema
        self.json_path = json_path

    @property
    def info(self):
        """
        Information necessary to reload the same OedSchema
        """
        return self.json_path

    @classmethod
    def from_oed_schema_info(cls, oed_schema_info):
        """
        Args:
            oed_schema_info: info to create OedSchema object, can be:
                - str to specify your own OedSchema path
                - OedSchema object will be return directly
                - None to get the current version Oed schema

        Returns:
            OedSchema
        """
        if isinstance(oed_schema_info, (str, Path)):
            return cls.from_json(oed_schema_info)
        elif isinstance(oed_schema_info, cls):
            return oed_schema_info
        elif oed_schema_info is None:
            return cls.from_json(cls.DEFAULT_ODS_SCHEMA_PATH)
        else:
            raise OdsException(f"{oed_schema_info} is not a supported format to create {cls} object")

    @classmethod
    def from_json(cls, oed_json):
        """
        Create OedSchema from json file
        Args:
            oed_json (str): OED schema json path

        Returns:
            OedSchema
        """
        with open(oed_json) as f:
            schema = json.load(f)
            # reformat area code for efficient check
            country_area = set()
            for country, areas in schema['area'].items():
                for area in areas:
                    country_area.add((country, area))
            schema["country_area"] = country_area

            return cls(schema, oed_json)

    @cached_property
    def nb_perils_dict(self):
        nb_perils_dict = nb.typed.Dict.empty(
            key_type=nb.types.UnicodeCharSeq(3),
            value_type=nb.types.UnicodeCharSeq(3)[:],
        )
        for peril_group, perils in self.schema['perils']['covered'].items():
            nb_perils_dict[peril_group] = np.array(perils, dtype='U3')

        return nb_perils_dict

    def peril_filtering(self, peril_ids, peril_filters):
        """
        check if peril_ids are part of the peril groups in peril_filters, both array need to match size
        Args:
            peril_ids (pd.Series): peril that are checked
            peril_filters (pd.Series): peril groups to check against
        :return:
            np.array of True and False
        """
        return jit_peril_filtering(peril_ids.to_numpy().astype('str'), peril_filters.to_numpy().astype('str'), self.nb_perils_dict)

    @staticmethod
    def to_universal_field_name(column: str):
        """
        transform column name into string that can be matched directly with the schema field name
        Args:
            column (str): name of the column
        :return:
            str
        """
        return column.strip().lower()

    @staticmethod
    def column_to_field(columns: list, oed_fields: dict, use_generic_flexi=True):
        """
        map column to OED field

        Args:
            columns (list): name of the columns in OED file
            oed_fields (dict): all the field in ods_schema
            use_generic_flexi (bool): if true flexi column return the oed_schema name
                                      if false flexi column are just lower cased
        Return:
            dict mapping between exact OED column name and field name in oed_schema
        """
        # support for name different from standard OED column name
        aliases = {OedSchema.to_universal_field_name(field_info.get('alias')): field_name for field_name,
                   field_info in oed_fields.items() if field_info.get('alias')}
        result = {}
        for column in columns:
            if OedSchema.to_universal_field_name(column) in oed_fields:
                result[column] = oed_fields[OedSchema.to_universal_field_name(column)]
            elif OedSchema.to_universal_field_name(column) in aliases:
                result[column] = oed_fields[aliases[OedSchema.to_universal_field_name(column)]]
            else:
                for field_suffix in ['xx', 'zzz']:
                    for i in range(1, len(OedSchema.to_universal_field_name(column))):
                        field_name = OedSchema.to_universal_field_name(column)[:-i] + field_suffix
                        if field_name in oed_fields:
                            if use_generic_flexi:
                                result[column] = oed_fields[field_name]
                            break
                    else:
                        continue
                    break
                else:
                    pass  # unrecognized/unknown columns are not added
        return result

    @staticmethod
    def use_field(dataframe, ods_fields):
        """
        rename the column in the dataframe to their OED field name

        Args:
            dataframe (pd.DataFrame): dataframe with OED column
            ods_fields (dict): OedSchema input field definition

        Returns:
            dataframe with renamed column
        """
        mapping = {column: field['Input Field Name']
                   for column, field in OedSchema.column_to_field(dataframe.columns, ods_fields, use_generic_flexi=False).items()}
        return dataframe.rename(columns=mapping)

    @staticmethod
    def is_valid_value(value, valid_ranges, allow_blanks):
        """
        >>> OedSchema.is_valid_value(2, [{'min': 0, 'max': 5}, {'min': 10}, {'max': -5}, {'enum': [-999]}])
        True

        >>> OedSchema.is_valid_value(7, [{'min': 0, 'max': 5}, {'min': 10}, {'max': -5}, {'enum': [-999]}])
        False

        >>> OedSchema.is_valid_value(-10, [{'min': 0, 'max': 5}, {'min': 10}, {'max': -5}, {'enum': [-999]}])
        True

        >>> OedSchema.is_valid_value(-999, [{'min': 0, 'max': 5}, {'min': 10}, {'enum': [-999]}])
        True

        Args:
            value: value to test
            valid_ranges (list): list of valid range
            allow_blanks (bool): if True blank value (ie pd.nan) will be considered valid

        Returns:
            True if value is in one of the range
        """

        if value in BLANK_VALUES:
            return allow_blanks
        for valid_range in valid_ranges:
            if valid_range.get('min') is not None:
                if value < valid_range.get('min'):
                    continue
            if valid_range.get('max') is not None:
                if value > valid_range.get('max'):
                    continue
            if valid_range.get('enum') is not None:
                if value not in valid_range.get('enum'):
                    continue
            return True
        return False
