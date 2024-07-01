import importlib
import logging
import sys
import threading
from datetime import datetime
from typing import Any, Type
import yaml

from .config import Config, TransformationConfig
from .connector import BaseConnector
from .mapping import BaseMapping
from .runner import BaseRunner

# Default versions for OED and AIR when running without a config file
OED_VERSION = "3.0.2"
AIR_VERSION = "10.0.0"

FORMAT_MAPPINGS = {
    'oed-air': {
        'input_format': {'name': 'OED_Location', 'version': OED_VERSION},
        'output_format': {'name': 'Cede_Location', 'version': AIR_VERSION}
    },
    'air-oed': {
        'input_format': {'name': 'Cede_Location', 'version': AIR_VERSION},
        'output_format': {'name': 'OED_Location', 'version': OED_VERSION}
    }
}

# Default config when running without a config file
BASE_CONFIG = {
    "transformations": {
        "loc": {
            "input_format": {
                "name": "",
                "version": ""
            },
            "output_format": {
                "name": "",
                "version": ""
            },
            "runner": {
                "batch_size": 150000
            },
            "extractor": {
                "options": {
                    "path": "",
                    "quoting": "minimal"
                }
            },
            "loader": {
                "options": {
                    "path": "",
                    "quoting": "minimal"
                }
            }
        }
    }
}

logger = logging.getLogger(__name__)

CONNECTOR_MAPPINGS = {
    "csv": "ods_tools.odtf.connector.CsvConnector",
    "mssql": "ods_tools.odtf.connector.db.mssql.SQLServerConnector",
    "postgres": "ods_tools.odtf.connector.db.postgres.PostgresConnector",
    "sqlite": "ods_tools.odtf.connector.db.sqlite.SQLiteConnector",
}


class Controller:
    """
    Class controlling the transformation flow

    :param config: The resolved normalised config
    """

    def __init__(self, config: Config):
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

        transformation_configs = self.config.get_transformation_configs()
        output_file_paths = []
        if self.config.get("parallel", True):
            threads = list(
                map(
                    lambda c: threading.Thread(
                        target=lambda: output_file_paths.append(self._run_transformation(c))
                    ),
                    transformation_configs,
                )
            )

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()
            output_file_paths = [path for path in output_file_paths if path is not None]
        else:
            for c in transformation_configs:
                output_file_path = self._run_transformation(c)
                if output_file_path:
                    output_file_paths[c.file_type] = output_file_path

        logger.info(
            f"Transformation finished in {datetime.now() - start_time}"
        )
        return output_file_paths

    def _run_transformation(self, config: TransformationConfig):
        try:
            mapping_class: Type[BaseMapping] = self._load_from_module(
                config.get(
                    "mapping.path", fallback="ods_tools.odtf.mapping.FileMapping"
                )
            )
            mapping: BaseMapping = mapping_class(
                config,
                config.file_type,
                **config.get("mapping.options", fallback={}),
            )

            extractor_type = config.get("extractor.type", fallback="csv")
            if extractor_type in CONNECTOR_MAPPINGS:
                extractor_class: Type[BaseConnector] = self._load_from_module(
                    CONNECTOR_MAPPINGS[extractor_type]
                )
            extractor: BaseConnector = extractor_class(
                config, **config.get("extractor.options", fallback={})
            )

            loader_class: Type[BaseConnector] = self._load_from_module(
                config.get(
                    "loader.path", fallback="ods_tools.odtf.connector.CsvConnector"
                )
            )
            loader: BaseConnector = loader_class(
                config, **config.get("loader.options", fallback={})
            )

            runner_class: Type[BaseRunner] = self._load_from_module(
                config.get("runner.path", fallback="ods_tools.odtf.runner.PandasRunner")
            )
            runner: BaseRunner = runner_class(
                config, **config.get("runner.options", fallback={})
            )

            runner.run(extractor, mapping, loader)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logger.error(
                f"{repr(e)}, line {exc_tb.tb_lineno} in {exc_tb.tb_frame.f_code.co_filename}"
            )
            return None


def generate_config(input_file, output_file, transformation_type):
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
    if transformation_type not in FORMAT_MAPPINGS:
        raise ValueError(
            f'Invalid transformation type. Only {list(FORMAT_MAPPINGS.keys())} are supported.'
        )

    config_dict = BASE_CONFIG.copy()
    config_dict['transformations']['loc']['input_format'] = FORMAT_MAPPINGS[transformation_type]['input_format']
    config_dict['transformations']['loc']['output_format'] = FORMAT_MAPPINGS[transformation_type]['output_format']
    config_dict['transformations']['loc']['extractor']['options']['path'] = input_file
    config_dict['transformations']['loc']['loader']['options']['path'] = output_file

    return config_dict


def transform_format(path_to_config_file=None, input_file=None, output_file=None, transformation=None):
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
            config_dict = yaml.safe_load(file)
    else:
        config_dict = generate_config(input_file, output_file, transformation)
    config = Config(config_dict)
    controller = Controller(config)
    controller.run()
    outputs = []
    for key, value in config_dict.get('transformations', {}).items():
        if value.get('output_format').get('name') == 'OED_Location':
            output_file_type = 'location'
        elif value.get('output_format').get('name') == 'OED_Contract':
            output_file_type = 'account'
        else:
            output_file_type = 'other'
        output_file_path = value.get('loader', {}).get('options', {}).get('path')
        outputs.append((output_file_path, output_file_type))
    return outputs
