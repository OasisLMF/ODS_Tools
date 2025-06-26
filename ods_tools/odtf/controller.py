import importlib
import logging
import sys
import os
from datetime import datetime
from typing import Any, Type
import yaml

from .connector import BaseConnector
from .mapper import Mapper
from .runner import PandasRunner

# Default config when running without a config file
BASE_CONFIG = {
    "type": "loc",
    "input": {
        "path": "",
        "quoting": "minimal"
    },
    "output": {
        "path": "",
        "quoting": "minimal"
    },
    "mapping": {
        "path": ""
    },
    "batch_size": 150000,
    "write_header": True,
    "logging": True
}

logger = logging.getLogger(__name__)

CONNECTOR_MAPPINGS = {
    "csv": "ods_tools.odtf.connector.CsvConnector",
    "mssql": "ods_tools.odtf.connector.db.SQLServerConnector",
    "postgres": "ods_tools.odtf.connector.db.PostgresConnector",
    "sqlite": "ods_tools.odtf.connector.db.SQLiteConnector",
}


class Controller:
    """
    Class controlling the transformation flow

    :param config: The resolved normalised config
    """

    def __init__(self, config):
        self.config = config

    def _load_from_module(self, path: str) -> Any:
        module_path, cls = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, cls)

    def run(self):
        """
        Generates the converter components from the config and runs the
        transformation
        """
        start_time = datetime.now()
        logger.info("Starting transformation")

        try:
            mapper = Mapper(self.config['mapping']['path'])

            extractor_type = self.config['input'].get("format", "csv")
            if extractor_type in CONNECTOR_MAPPINGS:
                extractor_class: Type[BaseConnector] = self._load_from_module(
                    CONNECTOR_MAPPINGS[extractor_type]
                )
            extractor: BaseConnector = extractor_class(self.config, isExtractor=True)

            loader_type = self.config['output'].get("format", "csv")
            if loader_type in CONNECTOR_MAPPINGS:
                loader_class: Type[BaseConnector] = self._load_from_module(
                    CONNECTOR_MAPPINGS[loader_type]
                )
            loader: BaseConnector = loader_class(self.config, isExtractor=False)

            runner = PandasRunner(self.config)
            runner.run(extractor, mapper, loader)

        except Exception as e:
            _, _, exc_tb = sys.exc_info()
            logger.error(
                f"{repr(e)}, line {exc_tb.tb_lineno} in {exc_tb.tb_frame.f_code.co_filename}"
            )

        logger.info(f"Transformation finished in {datetime.now() - start_time}")


def generate_config(input_file, output_file, mapping_file):
    """
    This function generates a config dictionary based on the input parameters.
    When running without a config file, this will generate the config dict.

    Args:
        input_file (str): path to the input file
        output_file (str): path to the output file
        transformation_type (str): either 'oed-air' or 'air-oed'

    Raises:
        ValueError: if transformation_type is not 'oed-air' or 'air-oed'

    Returns:
        dict: the generated config dictionary
    """
    config_dict = BASE_CONFIG.copy()
    config_dict['input']['path'] = input_file
    config_dict['output']['path'] = output_file
    config_dict['mapping']['path'] = mapping_file

    return config_dict


def transform_format(path_to_config_file=None, input_file=None, output_file=None, mapping_file=None):
    """This function takes the input parameters when called from ods_tools
    and starts the transformation process. Either path_to_config_file or
    all three input_file, output_file, and transformation_type must be provided.

    Args:
        path_to_config_file (str): path to the config file. Defaults to None.
        input_file (str: path to the input file. Defaults to None.
        output_file (str): path to the output file. Defaults to None.
        transformation_type (str): Either 'oed-air' or 'air-oed'. Defaults to None.

    Returns:
        list: a list of tuples containing the output file path and the file type.
        Used for checking the output files.
    """
    if path_to_config_file:
        with open(path_to_config_file, 'r') as file:
            config = yaml.safe_load(file)
        resolve_config_paths(config, path_to_config_file)

    else:
        config = generate_config(input_file, output_file, mapping_file)

    controller = Controller(config)
    controller.run()

    return config["output"]["path"]


def resolve_config_paths(config, path_to_config_file):
    make_relative_path_from_config_absolute(config['mapping'], "path", path_to_config_file)
    if config['input'].get('format', 'csv') in {'csv', 'sqlite'}:
        make_relative_path_from_config_absolute(config['input'], "path", path_to_config_file)
    if config['output'].get('format', 'csv') in {'csv', 'sqlite'}:
        make_relative_path_from_config_absolute(config['output'], "path", path_to_config_file)
    if config.get('database', {}).get('type', None) == "sqlite":
        make_relative_path_from_config_absolute(config['database'], "sql_statement_path", path_to_config_file)


def make_relative_path_from_config_absolute(dictionary, key, config_location):
    """Ensures paths given local to config file will be correctly loaded

    Args:
        dict (dictionary): Dict in config file holding the path location
        key (str): Key in dict to access path location
        config_location (str): Location of config file
    """
    if not os.path.isabs(dictionary[key]):
        dictionary[key] = os.path.abspath(os.path.join(os.path.dirname(config_location), dictionary[key]))
