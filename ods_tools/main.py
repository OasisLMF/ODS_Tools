"""
suite of tools to read and write ODS file in csv of parquet
"""
__all__ = [
    'main',
    'convert',
    'check',
    'generate'
]

import argparse
import json
import logging
import os

from ods_tools import logger
from ods_tools.oed import (
    OedExposure,
    OdsException,
    AnalysisSettingHandler,
    ModelSettingHandler
)
try:
    from ods_tools.odtf.controller import transform_format
except ImportError as e:
    pass

try:
    from ods_tools.combine.combine import combine as combine_ord
    from ods_tools.combine.combine import read_config as read_combine_config
except ImportError as e:
    logger.error("Combine ORD package requirements not installed.")
    logger.error(e)


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
    args_exp = set(['location', 'account', 'ri_info', 'ri_scope', 'oed_dir'])

    try:
        if args_exp.intersection(set(args_set)):
            oed_exposure = get_oed_exposure(**extract_exposure_args(kwargs))
            oed_exposure.check()
        if 'analysis_settings_json' in args_set:
            AnalysisSettingHandler.make().load(kwargs['analysis_settings_json'])
        if 'model_settings_json' in args_set:
            ModelSettingHandler.make().load(kwargs['model_settings_json'])
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
            if kwargs.get('mapping_file') is None or kwargs.get('input_file') is None or kwargs.get('output_file') is None:
                raise OdsException("When config_file is not provided, mapping_file, input_file, and output_file are required.")

        transform_result = transform_format(path_to_config_file=kwargs.get('config_file'), input_file=kwargs.get('input_file'),
                                            output_file=kwargs.get('output_file'), mapping_file=kwargs.get('mapping_file'))
        if kwargs.get('check') is not None:
            if kwargs.get('check').lower() in ["loc", "location", "locations"]:
                check(location=transform_result)
            elif kwargs.get('check').lower() in ["acc", "account", "accounts"]:
                check(account=transform_result)
    except OdsException as e:
        logger.error(e)
    except NameError as e:
        logger.error("Data transformation package requirements not installed.")
        logger.error(e)


def combine(**kwargs):
    """Wrapper function for the combine command.
    Combines multiple ORD analyses.
    """
    try:
        config = read_combine_config(kwargs.get('config_file'))
        combine_result = combine_ord(analysis_dirs=kwargs.get('analysis_dirs'),
                                     output_dir=kwargs.get('output_dir'),
                                     **config)
    except OdsException as e:
        logger.error('Combine failed')
        logger.error(e)
    except NameError as e:
        logger.error("Combine ORD package requirements not installed.")
        logger.error(e)


def generate(**kwargs):
    """Generate synthetic OED test data files from a JSON configuration."""
    from ods_tools.oed.oed_generator import (
        OEDFileGenerator,
        validate_config,
        create_example_config,
        get_available_oed_versions,
        load_oed_schema,
    )

    if kwargs.get('example_config'):
        print(json.dumps(create_example_config(), indent=2))
        return

    if kwargs.get('list_versions'):
        versions = get_available_oed_versions()
        print("Available OED versions:")
        for v in versions:
            print(f"  {v}")
        return

    if kwargs.get('list_fields'):
        oed_version = kwargs.get('oed_version', '5.0.0')
        schema = load_oed_schema(oed_version)
        file_type = kwargs['list_fields']
        fields = schema["input_fields"].get(file_type, {})
        if not fields:
            print(f"No fields found for '{file_type}'. Available: {list(schema['input_fields'].keys())}")
            return
        print(f"Fields for {file_type} (OED {oed_version}): {len(fields)} fields")
        print(f"{'Field Name':<40} {'Status':<8} {'Data Type':<20} {'Default':<10}")
        print("-" * 80)
        for fname_lower, fspec in sorted(fields.items()):
            name = fspec.get("Input Field Name", fname_lower)
            status = fspec.get("Property field status", "?")
            dtype = fspec.get("Data Type", "?")
            default = fspec.get("Default", "")
            print(f"{name:<40} {status:<8} {dtype:<20} {default:<10}")
        return

    config_path = kwargs.get('config')
    if not config_path:
        raise OdsException("--config is required (or use --example-config to generate one)")

    with open(config_path) as f:
        config = json.load(f)

    warnings = validate_config(config)
    for w in warnings:
        logger.warning(w)

    if kwargs.get('format'):
        config["output_format"] = kwargs['format']

    file_format = config.get("output_format", "csv")
    output_dir = kwargs.get('output_dir', './oed_output')

    gen = OEDFileGenerator(config)
    logger.info(f"Generating OED {gen.oed_version} test data...")
    output_files = gen.generate(output_dir, file_format)

    logger.info(f"Generated {len(output_files)} files:")
    for ft, fp in output_files.items():
        logger.info(f"  {ft}: {fp}")


command_action = {
    'check': check,
    'convert': convert,
    'transform': transform,
    'combine': combine,
    'generate': generate
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
This transformation can be done either by providing a config file or directly by specifying the input, output and mapping files.

If a config file is provided, the transformation will be done according to the config file.
Please note that the config file allows for more options (batch size, file format, database connection, etc.)
"""
transform_command = command_parser.add_parser('transform', description=transform_description,
                                              formatter_class=argparse.RawTextHelpFormatter)
transform_command.add_argument('--config-file', help='Path to the config file')
transform_command.add_argument('--input-file', help='Path to the input file', default=None)
transform_command.add_argument('--output-file', help='Path to the output file', default=None)
transform_command.add_argument('--mapping-file', help='Path to the mapping file', default=None)
transform_command.add_argument('-v', '--logging-level', help='logging level (debug:10, info:20, warning:30, error:40, critical:50)',
                               default=30, type=int)
transform_command.add_argument('--check', help='if Location or Account, OED file will be checked on completion', default=None, action='store_true')

combine_description = """
Combine multiple analyses with ORD output. This requires a combine config to be provided.
"""
combine_command = command_parser.add_parser('combine', description=combine_description,
                                            formatter_class=argparse.RawTextHelpFormatter)
combine_command.add_argument('--analysis-dirs', '-a', required=True, nargs='+', help='List of paths to analysis results directories')
combine_command.add_argument('--output-dir', help='Path to output directory', default=None)
combine_command.add_argument('--config-file', required=True, help='Path to the config file')
combine_command.add_argument('-v', '--logging-level', help='logging level (debug:10, info:20, warning:30, error:40, critical:50)',
                             default=30, type=int)


generate_description = """
Generate synthetic OED test data files from a JSON configuration.

The configuration file controls which OED file types to generate (Loc, Acc, ReinsInfo, ReinsScope),
the number of rows, which fields to include, financial terms, and portfolio structure.

Use --example-config to print a sample configuration file.
Use --list-versions to see available OED schema versions.
Use --list-fields to inspect fields available for a given file type.
"""
generate_command = command_parser.add_parser('generate', description=generate_description,
                                             formatter_class=argparse.RawTextHelpFormatter)
generate_command.add_argument('--config', help='Path to JSON configuration file', default=None)
generate_command.add_argument('--output-dir', help='Output directory for generated files (default: ./oed_output)',
                              default='./oed_output')
generate_command.add_argument('-f', '--format', help='Output file format (overrides config)',
                              choices=['csv', 'parquet'], default=None)
generate_command.add_argument('--example-config', help='Print an example configuration JSON and exit',
                              action='store_true', default=False)
generate_command.add_argument('--list-versions', help='List available OED versions and exit',
                              action='store_true', default=False)
generate_command.add_argument('--list-fields', help='List all fields for a given file type '
                              '(e.g. Loc, Acc, ReinsInfo, ReinsScope)', metavar='FILE_TYPE', default=None)
generate_command.add_argument('--oed-version', help='OED version for --list-fields (default: 5.0.0)',
                              default='5.0.0')
generate_command.add_argument('-v', '--logging-level', help='logging level (debug:10, info:20, warning:30, error:40, critical:50)',
                              default=30, type=int)


def main():
    """command line interface for ODS conversion between csv and parquet"""
    kwargs = vars(main_parser.parse_args())
    logging_level = kwargs.pop('logging_level')
    logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s')

    command_action[kwargs.pop('command')](** kwargs)


if __name__ == "__main__":
    main()
