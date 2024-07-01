"""
suite of tools to read and write ODS file in csv of parquet
"""
__all__ = [
    'main',
    'convert',
    'check'
]

import argparse
import logging
import os

from ods_tools import logger
from ods_tools.oed import (
    OedExposure,
    OdsException,
    ModelSettingSchema,
    AnalysisSettingSchema,
)
try:
    from ods_tools.odtf.controller import transform_format
except ImportError:
    logger.info("Data transformation package requirements not intalled.")


def get_oed_exposure(config_json=None, oed_dir=None, **kwargs):
    if config_json:
        return OedExposure.from_config(config_json, **kwargs)
    elif oed_dir:
        return OedExposure.from_dir(oed_dir, **kwargs)
    else:
        return OedExposure(**kwargs)


def extract_exposure_args(kwargs):
    exposure_kwargs = {}
    for param in ['location', 'account', 'ri_info', 'ri_scope', 'oed_dir', 'check_oed', 'config_json', 'validation_config']:
        value = kwargs.pop(param, None)
        if value is not None:
            exposure_kwargs[param] = value
    return exposure_kwargs


def check(**kwargs):
    """run the check command on Exposure"""
    logger = logging.getLogger(__name__)
    args_set = {k for k, v in kwargs.items() if v is not None}
    args_exp = set(['location', 'account', 'ri_info', 'ri_scope'])

    try:
        if args_exp.intersection(set(args_set)):
            oed_exposure = get_oed_exposure(**extract_exposure_args(kwargs))
            oed_exposure.check()
        if 'analysis_settings_json' in args_set:
            AnalysisSettingSchema().validate_file(kwargs['analysis_settings_json'])
        if 'model_settings_json' in args_set:
            ModelSettingSchema().validate_file(kwargs['model_settings_json'])
    except OdsException as e:
        logger.error('Validation failed:')
        logger.error(e)


def convert(**kwargs):
    """Convert exposure data to an other format (ex: csv to parquet) or version (ex: 3.0 to 2.2)"""
    path = kwargs.pop('output_dir', None)

    if not kwargs.get("compression") and not kwargs.get("version"):
        raise OdsException("either --compression or --version must be provided")

    if kwargs.get("config_json"):
        if not path:
            path = os.path.dirname(kwargs.get("config_json"))
    elif kwargs.get("oed_dir"):
        path = kwargs.get("oed_dir")
    elif kwargs.get("location"):
        if not path:
            raise OdsException("output_dir must be provided when location is provided as single file")

    if not path:
        kwargs["oed_dir"] = path = os.getcwd()

    oed_exposure = get_oed_exposure(**extract_exposure_args(kwargs))

    version = kwargs.pop("version", None)
    if version:
        logger.info(f"Converting to version {version}.")  # Log the conversion version
        try:
            oed_exposure.to_version(version)
            kwargs["version_name"] = version.replace(".", "-")
        except OdsException as e:
            logger.error("Conversion failed:")
            logger.error(e)

    logger.info(f"Saving to: {path}")
    oed_exposure.save(path=path, **kwargs)


def transform(**kwargs):
    """Wrapper function for transform command.
    Transform location and account data to a new format (ex: AIR to OED)"""
    try:
        if kwargs.get('config_file') is None:
            if kwargs.get('format') is None or kwargs.get('input_file') is None or kwargs.get('output_file') is None:
                raise OdsException("When --config-file is not provided, --format, --input-file, and --output-file are required.")

        transform_result = transform_format(path_to_config_file=kwargs.get('config_file'), input_file=kwargs.get('input_file'),
                                            output_file=kwargs.get('output_file'), transformation=kwargs.get('format'))
        if not kwargs.get('nocheck'):
            for output_file in transform_result:
                if output_file[1] == 'location' and os.path.isfile(output_file[0]):
                    check(location=output_file[0])
                elif output_file[1] == 'account' and os.path.isfile(output_file[0]):
                    check(account=output_file[0])
    except OdsException as e:
        logger.error(e)
    except NameError as e:
        logger.error("Data transformation package requirements not intalled.")
        logger.error(e)


command_action = {
    'check': check,
    'convert': convert,
    'transform': transform,
}


def add_exposure_data_args(command):
    command.add_argument('--location', help='Path to the location file', default=None)
    command.add_argument('--account', help='Path to the account file', default=None)
    command.add_argument('--ri-info', help='Path to the ri_info file', default=None)
    command.add_argument('--ri-scope', help='Path to the ri_scope file', default=None)
    command.add_argument('--oed-dir', help='Path to the OED directory containing stardard named OED files', default=None)
    command.add_argument('--config-json', help='Path to the config_json file', default=None)
    command.add_argument('--validation-config', help='Path to the validation_config file', default=None)


main_parser = argparse.ArgumentParser()

oed_exposure_creation = """
there are several options to specify the exposure data,
 - by providing  the path to each OED source using `--location`, `--account`, `--ri-info`, `--ri-scope`.
 - by providing  an OED config json file using `--config-json`.
 - by providing  the path to the directory where the exposure is stored using `--oed-dir`.

if multiple options are use at the same time, --config-json will have the priority over --oed-dir
specific paths (--location, --account, --ri-info, --ri-scope) will overwrite the path found in (--config-json or --oed-dir)
 """


convert_description = """
convert OED files to an other format or version
"""

command_parser = main_parser.add_subparsers(help='command [convert]', dest='command', required=True)
convert_command = command_parser.add_parser('convert', description=convert_description +
                                            oed_exposure_creation, formatter_class=argparse.RawTextHelpFormatter)
add_exposure_data_args(convert_command)
convert_command.add_argument('--check-oed', help='if True, OED file will be checked before convertion', default=False)
convert_command.add_argument('--output-dir', help='path of the output directory', required=False)
convert_command.add_argument('-c', '--compression', help='compression to use (ex: parquet, zip, gzip, csv,...)', required=False)
convert_command.add_argument('--save-config', help='if True, OED config file will be save in the --path directory', default=False)
convert_command.add_argument('-v', '--logging-level', help='logging level (debug:10, info:20, warning:30, error:40, critical:50)',
                             default=30, type=int)
convert_command.add_argument('--version', help='specific OED version to use in the conversion', default=None, type=str)

check_description = """
check exposure data.
"""

check_command = command_parser.add_parser('check', description=check_description + oed_exposure_creation,
                                          formatter_class=argparse.RawTextHelpFormatter)
add_exposure_data_args(check_command)
check_command.add_argument('--model-settings-json', help='Path to Model settings meta-data file to check', default=None)
check_command.add_argument('--analysis-settings-json', help='Path to Analysis settings file to check', default=None)
check_command.add_argument('-v', '--logging-level', help='logging level (debug:10, info:20, warning:30, error:40, critical:50)',
                           default=30, type=int)


transform_description = """
Transform data format to/from OED.
This transformation can be done either by providing a config file or directly by specifying the input and output files.
If input and output files are provided, either --oed-air or --air-oed must be specified to indicate the transformation direction.

If a config file is provided, the transformation will be done according to the config file.
Please note that the config file allows for more options (batch size, file format, database connection, etc.)
"""
transform_command = command_parser.add_parser('transform', description=transform_description,
                                              formatter_class=argparse.RawTextHelpFormatter)
transform_command.add_argument('--config-file', help='Path to the config file')
transform_command.add_argument('-f', "--format", help='Specify which transformation to use (currently oed-air or air-oed)', default=None)
transform_command.add_argument('--input-file', help='Path to the input file', default=None)
transform_command.add_argument('--output-file', help='Path to the output file', default=None)
transform_command.add_argument('-v', '--logging-level', help='logging level (debug:10, info:20, warning:30, error:40, critical:50)',
                               default=30, type=int)
transform_command.add_argument('--nocheck', help='if True, OED file will not be checked after transformation', default=False, action='store_true')


def main():
    """command line interface for ODS conversion between csv and parquet"""
    kwargs = vars(main_parser.parse_args())
    logging_level = kwargs.pop('logging_level')
    logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s')

    command_action[kwargs.pop('command')](** kwargs)


if __name__ == "__main__":
    main()
