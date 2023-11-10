from pathlib import Path
import mimetypes

import logging
import pandas as pd
import numpy as np
from chardet.universaldetector import UniversalDetector

from .common import (OED_TYPE_TO_NAME, OdsException, PANDAS_COMPRESSION_MAP, PANDAS_DEFAULT_NULL_VALUES, is_relative, BLANK_VALUES, fill_empty,
                     UnknownColumnSaveOption, cached_property)
from .forex import convert_currency
from .oed_schema import OedSchema

logger = logging.getLogger(__file__)


def detect_encoding(fileobj):
    """
    Given a path to a CSV of unknown encoding
    read lines to detect its encoding type

    :param filepath: Filepath to check
    :type  filepath: str

    :return: Example `{'encoding': 'ISO-8859-1', 'confidence': 0.73, 'language': ''}`
    :rtype: dict
    """
    detector = UniversalDetector()
    for line in fileobj:
        detector.feed(line)
        if detector.done:
            break
    detector.close()
    return detector.result


def detect_stream_type(stream_obj):
    """
    Given a file object try to inferr if its holding
    `csv` or `parquet` data from its attributes
    If unknown return ""

    Note: content types matching compressed formats
     'gzip', 'x-bzip2', 'zip' and 'x-bzip2'
     are assumed to be compressed csv

    Args:
        stream_obj: object with a read() method

    Returns:
        stream_type (str): 'csv' or 'parquet'
    """
    type_map = {
        'csv': [
            'csv',
            '.csv',
            'text/csv',
            'application/gzip',
            'application/x-bzip2',
            'application/zip',
            'application/x-bzip2',
        ],
        'parquet': [
            'parquet',
            '.parquet',
            'application/octet-stream',
        ]
    }
    filename = getattr(stream_obj, 'name', None)
    content_type = getattr(stream_obj, 'content_type', None)

    # detect by filename
    if isinstance(filename, str):
        extention = Path(filename).suffix.lower()
        mimetype = mimetypes.MimeTypes().guess_type(filename)[0]
        for filetype in type_map:
            # check by extention exact match
            if extention in type_map[filetype]:
                return filetype
            # check by mimetype match
            if mimetype in type_map[filetype]:
                return filetype

    # detect by content_type
    if isinstance(content_type, str):
        for filetype in type_map:
            if content_type.lower() in type_map[filetype]:
                return filetype

    # Format unknown, default to csv
    return 'csv'


def is_readable(obj):
    return hasattr(obj, 'read') and callable(getattr(obj, 'read'))


class OedSource:
    """
    Class to represent and manage oed source (location, account, ...)
    """

    def __init__(self, exposure, oed_type, cur_version_name, sources):
        """

        Args:
            exposure (OedExposure): exposure the OED source is part of
            oed_type (str): OED type of the source
            cur_version_name (str): name of the current version
            sources (dict): all the version/source of the OED source
        """
        self.exposure = exposure
        self.oed_type = oed_type
        self.oed_name = OED_TYPE_TO_NAME[oed_type]
        self.cur_version_name = cur_version_name
        self.sources = sources
        self.loaded = False

    def __str__(self):
        """
        Returns:
            string of the current version source
        """
        return str(self.sources[self.cur_version_name])

    @property
    def info(self):
        """
        information of source to be able to trace its different version and reload it if necessary
        Returns:
            info dict
        """
        return {'cur_version_name': self.cur_version_name, 'sources': self.sources}

    @classmethod
    def from_oed_info(cls, exposure, oed_type: str, oed_info, **kwargs):
        """
        Convert data in oed_info to an OedSource

        Args:
            exposure (OedExposure): Exposure the source is part of
            oed_type (str): type of oed file (Loc, Acc, ...)
            oed_info: info to create OedSource, can be:
                - str for filepath
                - dict for more complex config,
                - OedSource same object is returned
                - DataFrame no source file just oed data is path and can be saved after

        Returns:
            OedSource (or None if  oed_info is None)
        """
        if isinstance(oed_info, (str, Path)):
            return cls.from_filepath(exposure, oed_type, filepath=oed_info, **kwargs)
        elif isinstance(oed_info, dict):
            if oed_info.get('sources'):
                return cls(exposure, oed_type, **oed_info)
            else:
                return cls.from_oed_info(exposure, oed_type, **oed_info)
        elif isinstance(oed_info, OedSource):
            return oed_info
        elif isinstance(oed_info, pd.DataFrame):
            return cls.from_dataframe(exposure, oed_type, oed_info)
        elif oed_info is None:
            return None
        elif is_readable(oed_info):
            return cls.from_stream_obj(exposure, oed_type, stream_obj=oed_info, **kwargs)
        else:
            raise OdsException(f'{oed_info} is not a supported format to convert to OedSource')

    @classmethod
    def from_dataframe(cls, exposure, oed_type, oed_df: pd.DataFrame):
        """
        OedSource Constructor from a filepath
        Args:
            exposure (OedExposure): Exposure the oed source is part of
            oed_type (str): type of file (Loc, Acc, ..)
            oed_df (pd.DataFrame): DataFrame that represent the Oed Source

        Returns:
            OedSource
        """
        oed_source = cls(exposure, oed_type, 'orig', {'orig': {'source_type': 'DataFrame'}})

        ods_fields = exposure.get_input_fields(oed_type)
        column_to_field = OedSchema.column_to_field(oed_df.columns, ods_fields)
        oed_df = cls.as_oed_type(oed_df, column_to_field)
        oed_df = cls.prepare_df(oed_df, column_to_field, ods_fields)
        if exposure.use_field:
            oed_df = OedSchema.use_field(oed_df, ods_fields)
        oed_source.dataframe = oed_df
        if oed_df.empty:
            logger.info(f'{oed_source.oed_name} {oed_source} is empty')
        oed_source.loaded = True
        return oed_source

    @classmethod
    def from_filepath(cls, exposure, oed_type, filepath, read_param=None):
        """
        OedSource Constructor from a filepath
        Args:
            exposure (OedExposure): Exposure the oed source is part of
            oed_type (str): type of file (Loc, Acc, ..)
            filepath (str): path to the oed source file
            read_param (dict): extra parameters to use when reading the file

        Returns:
            OedSource
        """
        if read_param is None:
            read_param = {}
        return cls(exposure, oed_type, 'orig', {'orig': {'source_type': 'filepath', 'filepath': filepath, 'read_param': read_param}})

    @classmethod
    def from_stream_obj(cls, exposure, oed_type, stream_obj, format=None, read_param=None):
        """
        OedSource Constructor from a filepath
        Args:
            exposure (OedExposure): Exposure the oed source is part of
            oed_type (str): type of file (Loc, Acc, ..)
            stream_obj: object with a read() method

        Returns:
            OedSource
        """
        if read_param is None:
            read_param = {}
        oed_source = cls(exposure, oed_type, 'orig', {'orig': {'source_type': 'stream', 'format': format}})

        if not format:
            format = detect_stream_type(stream_obj)

        try:
            if format == 'csv':
                oed_df = pd.read_csv(stream_obj, **read_param)
            elif format == 'parquet':
                oed_df = pd.read_parquet(stream_obj, **read_param)
            else:
                raise OdsException(f'Unsupported stream format {format}')
        except Exception as e:
            raise OdsException('Failed to read stream data') from e

        ods_fields = exposure.get_input_fields(oed_type)
        column_to_field = OedSchema.column_to_field(oed_df.columns, ods_fields)
        oed_df = cls.as_oed_type(oed_df, column_to_field)
        oed_df = cls.prepare_df(oed_df, column_to_field, ods_fields)

        if exposure.use_field:
            oed_df = OedSchema.use_field(oed_df, ods_fields)

        oed_source.dataframe = oed_df
        oed_source.loaded = True
        if oed_df.empty:
            logger.info(f'{oed_source.oed_name} {oed_source} is empty')
        return oed_source

    @classmethod
    def as_oed_type(cls, oed_df, column_to_field):
        pd_dtype = {}
        to_tmp_dtype = {}
        for column in oed_df.columns:
            if column in column_to_field:
                pd_dtype[column] = column_to_field[column]['pd_dtype']
            else:
                pd_dtype[column] = 'category'
            if pd_dtype[column] == 'category':  # we need to convert to str first
                to_tmp_dtype[column] = 'str'
                if oed_df[column].dtype.name == 'category' and '' not in oed_df[column].dtype.categories:
                    oed_df[column] = oed_df[column].cat.add_categories('')
                oed_df[column] = oed_df[column]  # make a copy f the col in case it is read_only
                oed_df.loc[oed_df[column].isin(BLANK_VALUES), column] = ''
            elif pd_dtype[column].startswith('Int'):
                to_tmp_dtype[column] = 'float'

        return oed_df.astype(to_tmp_dtype).astype(pd_dtype)

    @classmethod
    def prepare_df(cls, df, column_to_field, ods_fields):
        """
        Complete the Oed Dataframe with default valued and required column
        Args:
            df: oed dataframe
            column_to_field: dict mapping column to their field info
            ods_fields: the ods_field info for this oed source type

        Returns:
            df
        """
        # set default values
        for col, field_info in column_to_field.items():
            if (field_info
                    and field_info['Default'] != 'n/a'
                    and (df[col].isna().any() or (field_info['pd_dtype'] == 'category' and df[col].isnull().any()))):
                fill_empty(df, col, df[col].dtype.type(field_info['Default']))
            elif df[col].dtype.name == 'category':
                fill_empty(df, col, '')

        # add required columns that allow blank values if missing
        present_field = set(field_info['Input Field Name'] for field_info in column_to_field.values())
        for field_info in ods_fields.values():
            col = field_info['Input Field Name']
            if col not in present_field:
                if field_info.get('Required Field') == 'R' and field_info.get("Allow blanks?").upper() == "YES":
                    if field_info['pd_dtype'] == 'category':
                        df[col] = '' if field_info['Default'] == 'n/a' else field_info['Default']
                        df[col] = df[col].astype('category')
                    else:
                        df[col] = np.nan
                        df[col] = df[col].astype(field_info['pd_dtype'])
                        if field_info['Default'] != 'n/a':
                            df[col] = df[col].fillna(df[col].dtype.type(field_info['Default'])).astype(field_info['pd_dtype'])
        return df

    @cached_property
    def dataframe(self):
        """Dataframe view of the OedSource, loaded once"""
        df = self.load_dataframe()
        if self.exposure.use_field:
            df = OedSchema.use_field(df, self.exposure.get_input_fields(self.oed_type))
        self.loaded = True
        if df.empty:
            logger.info(f'{self.oed_name} {self} is empty')
        return df

    @property
    def current_source(self):
        """
        current version of the oed source
        Returns:
            source dict of the current version
        """
        return self.sources[self.cur_version_name]

    def get_input_fields(self):
        """
        Returns:
            OED schema input field definition
        """
        return self.exposure.get_input_fields(self.oed_type)

    def get_column_to_field(self):
        """
        Returns:
            mapping between column in dataframe and field definition
        """
        return OedSchema.column_to_field(
            self.dataframe.columns,
            self.get_input_fields()
        )

    def load_dataframe(self, version_name=None):
        """
        load the dataframe from a version of oed source

        Args:
            version_name (str): name of the version in sources

        Returns:
            Dataframe representing the oed source (pd.DataFrame)
        """
        if version_name is None:
            version_name = self.cur_version_name
        source = self.sources[version_name]
        if source['source_type'] == 'filepath':
            filepath = source['filepath']
            if is_relative(filepath):
                filepath = Path(self.exposure.working_dir, filepath)
            extension = PANDAS_COMPRESSION_MAP.get(source.get('extention')) or Path(filepath).suffix
            if extension == '.parquet':
                oed_df = pd.read_parquet(filepath, **source.get('read_param', {}))
                ods_fields = self.exposure.get_input_fields(self.oed_type)
                column_to_field = OedSchema.column_to_field(oed_df.columns, ods_fields)
                oed_df = self.as_oed_type(oed_df, column_to_field)
                oed_df = self.prepare_df(oed_df, column_to_field, ods_fields)

            else:  # default we assume it is csv like
                read_params = {'keep_default_na': False,
                               'na_values': PANDAS_DEFAULT_NULL_VALUES.difference({'NA'})}
                read_params.update(source.get('read_param', {}))
                oed_df = self.read_csv(filepath, self.exposure.get_input_fields(self.oed_type), **read_params)
        else:
            raise Exception(f"Source type {source['source_type']} is not supported")

        if self.exposure.reporting_currency:
            convert_currency(oed_df,
                             self.oed_type,
                             self.exposure.reporting_currency,
                             self.exposure.currency_conversion,
                             self.exposure.oed_schema)
        return oed_df

    def convert_currency(self):
        """
        convert currency values current of the oed source to exposure.reporting_currency
        """
        if self.loaded:
            convert_currency(self.dataframe,
                             self.oed_type,
                             self.exposure.reporting_currency,
                             self.exposure.currency_conversion,
                             self.exposure.oed_schema)

    def manage_unknown_columns(self, unknown_columns):
        if unknown_columns == UnknownColumnSaveOption.IGNORE:
            return self.dataframe
        if not isinstance(unknown_columns, dict):
            option_map = {}
            default = unknown_columns
        else:
            option_map = unknown_columns
            default = unknown_columns.get('default', UnknownColumnSaveOption.IGNORE)
        rename = {}
        drop = set()
        for column in set(self.dataframe.columns).difference(self.get_column_to_field()):
            option = option_map.get(column, default)
            if option == UnknownColumnSaveOption.IGNORE:
                continue
            elif option == UnknownColumnSaveOption.RENAME:
                rename[column] = f'Flexi{self.oed_type}{column}'
            elif option == UnknownColumnSaveOption.DELETE:
                drop.add(column)

        return self.dataframe.rename(columns=rename).drop(columns=drop)

    def save(self, version_name, source, unknown_columns=UnknownColumnSaveOption.IGNORE):
        """
        save dataframe as version_name in source
        Args:
            version_name (str): name of the version
            source: str or dict with information to save the dataframe
                str : output path
                dict : {'source_type': 'filepath' # only support for the moment
                        'extension': 'parquet' or all pandas supported extension
                        'write_param' : all args you may want to pass to the pandas writer function (to_parquet, to_csv)
            unknown_columns (UnknownColumnSaveOption or Dict):  action to take for non OED column
        """
        if isinstance(source, (str, Path)):
            source = {'source_type': 'filepath',
                      'filepath': source,
                      }
        if source['source_type'] == 'filepath':
            filepath = source['filepath']
            if is_relative(filepath):
                filepath = Path(self.exposure.working_dir, filepath)
            Path(filepath).parents[0].mkdir(parents=True, exist_ok=True)
            extension = source.get('extension') or ''.join(Path(filepath).suffixes)
            dataframe = self.manage_unknown_columns(unknown_columns)
            if extension == 'parquet':
                dataframe.to_parquet(filepath, **source.get('write_param', {}))
            else:
                write_param = {'index': False}
                write_param.update(source.get('write_param', {}))
                dataframe.to_csv(filepath, **write_param)
        else:
            raise Exception(f"Source type {source['source_type']} is not supported")
        self.cur_version_name = version_name
        self.sources[version_name] = source

    @classmethod
    def read_csv(cls, filepath_or_buffer, ods_fields, df_engine=pd, **kwargs):
        """
        the function read_csv will load a csv file as a DataFrame
        with all the columns converted to the correct dtype and having the correct default.
        it will also try to save space by converting string dtype into categories
        By default, it uses pandas to create the DataFrame. In that case you will need to have pandas installed.
        You can use other options such as Dask or modin using the parameter df_engine.
        Args:
            filepath_or_buffer (str, stream): str, path object or file-like object with seek method
            ods_fields (dict): OED schema input field definition
            df_engine: engine that will convert csv to a dataframe object (default to pandas if installed)
            kwargs: extra argument that will be passed to the df_engine
        Returns:
            df_engine dataframe of the file with correct dtype and default
        Raises:
        """
        if is_readable(filepath_or_buffer):
            stream_start = filepath_or_buffer.tell()
        else:
            stream_start = None

        def read_or_try_encoding_read(df_engine, filepath_or_buffer, **read_kwargs):
            #  try to read, if it fails, try to detect the encoding and update the top function kwargs for future read
            try:
                return df_engine.read_csv(filepath_or_buffer, **read_kwargs)
            except UnicodeDecodeError as e:
                if stream_start is None:
                    with open(filepath_or_buffer, 'rb') as buffer:
                        detected_encoding = detect_encoding(buffer)['encoding']
                else:
                    detected_encoding = detect_encoding(filepath_or_buffer)['encoding']
                    filepath_or_buffer.seek(stream_start)
                if not read_kwargs.get('encoding') and detected_encoding:
                    kwargs['encoding'] = detected_encoding
                    read_kwargs.pop('encoding', None)
                    return df_engine.read_csv(filepath_or_buffer, encoding=detected_encoding, **read_kwargs)
                else:
                    raise
            finally:
                if stream_start is not None:
                    filepath_or_buffer.seek(stream_start)

        if df_engine is None:
            raise Exception("df_engine parameter not specified, you must install pandas"
                            " or pass your DataFrame engine (modin, dask,...)")

        header_read_arg = {**kwargs, 'nrows': 0, 'index_col': False}
        if (stream_start is None
                and Path(filepath_or_buffer).suffix == '.gzip' or header_read_arg.get('compression') == 'gzip'):
            # support for gzip https://stackoverflow.com/questions/60460814/pandas-read-csv-failing-on-gzipped-file-with-unicodedecodeerror-utf-8-codec-c
            with open(filepath_or_buffer, 'rb') as f:
                header_read_arg['compression'] = kwargs['compression'] = 'gzip'
                header = df_engine.read(f, **header_read_arg).columns

        else:
            header = read_or_try_encoding_read(df_engine, filepath_or_buffer, **header_read_arg).columns

        # match header column name to oed field name and prepare pd_dtype used to read the data
        pd_dtype = {}
        column_to_field = OedSchema.column_to_field(header, ods_fields)
        for col in header:
            if col in column_to_field:
                field_info = column_to_field[col]
                pd_dtype[col] = field_info['pd_dtype']
            else:
                pd_dtype[col] = 'category'

        # read the oed file
        if kwargs.get('compression') == 'gzip':
            with open(filepath_or_buffer, 'rb') as f:
                df = df_engine.read_csv(f, dtype=pd_dtype, **kwargs)
        else:
            df = df_engine.read_csv(filepath_or_buffer, dtype=pd_dtype, **kwargs)

        return cls.prepare_df(df, column_to_field, ods_fields)
