"""
common static variable and ods_tools exceptions
"""
from urllib.parse import urlparse
from pathlib import Path
import numpy as np
import pandas as pd
from enum import Enum


class OdsException(Exception):
    pass


def is_relative(filepath):
    """
    return True is path is relative meaning it is neither internet RFC nor os absolute path
    Args:
        filepath (str: path est

    Returns:
        boolean
    """
    url_parsed = urlparse(str(filepath))
    return not (all([url_parsed.scheme, url_parsed.netloc]) or Path(filepath).is_absolute())


try:
    from functools import cached_property
except ImportError:  # support for python < 3.8
    _missing = object()

    class cached_property(object):
        """A decorator that converts a function into a lazy property.  The
        function wrapped is called the first time to retrieve the result
        and then that calculated result is used the next time you access
        the value::

            class Foo(object):

                @cached_property
                def foo(self):
                    # calculate something important here
                    return 42

        The class has to have a `__dict__` in order for this property to
        work.
        """

        # implementation detail: this property is implemented as non-data
        # descriptor.  non-data descriptors are only invoked if there is
        # no entry with the same name in the instance's __dict__.
        # this allows us to completely get rid of the access function call
        # overhead.  If one choses to invoke __get__ by hand the property
        # will still work as expected because the lookup logic is replicated
        # in __get__ for manual invocation.

        def __init__(self, func, name=None, doc=None):
            self.__name__ = name or func.__name__
            self.__module__ = func.__module__
            self.__doc__ = doc or func.__doc__
            self.func = func

        def __get__(self, obj, type=None):
            if obj is None:
                return self
            value = obj.__dict__.get(self.__name__, _missing)
            if value is _missing:
                value = self.func(obj)
                obj.__dict__[self.__name__] = value
            return value


# PANDAS_COMPRESSION_MAP is also used to order the preferred input format in ExposureData.from_dir
PANDAS_COMPRESSION_MAP = {
    'parquet': '.parquet',
    'csv': '.csv',
    'zip': '.zip',
    'gzip': '.gz',
    'bz2': '.bz2',
    'zstd': '.zst',
}

PANDAS_DEFAULT_NULL_VALUES = {
    '-1.#IND',
    '1.#QNAN',
    '1.#IND',
    '-1.#QNAN',
    '#N/A N/A',
    '#N/A',
    'N/A',
    'n/a',
    'NA',
    '#NA',
    'NULL',
    'null',
    'NaN',
    '-NaN',
    '-NaN',
    'nan',
    '-nan',
    '',
}

USUAL_FILE_NAME = {
    'location': ['location'],
    'account': ['account'],
    'ri_info': ['ri_info', 'reinsinfo'],
    'ri_scope': ['ri_scope', 'reinsscope'],
}

OED_TYPE_TO_NAME = {
    'Loc': 'location',
    'Acc': 'account',
    'ReinsInfo': 'ri_info',
    'ReinsScope': 'ri_scope'
}

OED_NAME_TO_TYPE = {value: key for key, value in OED_TYPE_TO_NAME.items()}

OED_IDENTIFIER_FIELDS = {
    'Loc': ['PortNumber', 'AccNumber', 'LocNumber'],
    'Acc': ['PortNumber', 'AccNumber', 'PolNumber', 'LayerNumber'],
    'ReinsInfo': ['ReinsNumber', 'ReinsLayerNumber', 'ReinsName', 'ReinsPeril'],
    'ReinsScope': ['ReinsNumber', 'PortNumber', 'AccNumber', 'LocNumber']
}

VALIDATOR_ON_ERROR_ACTION = {'raise', 'log', 'ignore', 'return'}
DEFAULT_VALIDATION_CONFIG = [
    {'name': 'source_coherence', 'on_error': 'raise'},
    {'name': 'required_fields', 'on_error': 'raise'},
    {'name': 'unknown_column', 'on_error': 'raise'},
    {'name': 'valid_values', 'on_error': 'raise'},
    {'name': 'perils', 'on_error': 'raise'},
    {'name': 'occupancy_code', 'on_error': 'raise'},
    {'name': 'construction_code', 'on_error': 'raise'},
    {'name': 'country_and_area_code', 'on_error': 'raise'},
    {'name': 'conditional_requirement', 'on_error': 'raise'},
]

OED_PERIL_COLUMNS = ['AccPeril', 'PolPerilsCovered', 'PolPeril', 'CondPeril', 'LocPerilsCovered', 'LocPeril',
                     'ReinsPeril']

BLANK_VALUES = {np.nan, '', None, pd.NA, pd.NaT}


def fill_empty(df, columns, value):
    if isinstance(columns, str):
        columns = [columns]
    for column in columns:
        if df[column].dtypes.name == 'category' and value not in {None, np.nan}.union(df[column].cat.categories):
            df[column] = df[column].cat.add_categories(value)
        df.loc[df[column].isin(BLANK_VALUES), column] = value


class UnknownColumnSaveOption(Enum):
    IGNORE = 1
    RENAME = 2
    DELETE = 3
