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

logger = logging.getLogger(__name__)


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

            extractor_class: Type[BaseConnector] = self._load_from_module(
                config.get(
                    "extractor.path", fallback="ods_tools.odtf.connector.CsvConnector"
                )
            )
            extractor: BaseConnector = extractor_class(
                config, **config.get("extractor.options", fallback={})
            )

            loader_class: Type[BaseConnector] = self._load_from_module(
                config.get(
                    "loader.path", fallback="odtf.connector.CsvConnector"
                )
            )
            loader: BaseConnector = loader_class(
                config, **config.get("loader.options", fallback={})
            )

            runner_class: Type[BaseRunner] = self._load_from_module(
                config.get("runner.path", fallback=".runner.PandasRunner")
            )
            runner: BaseRunner = runner_class(
                config, **config.get("runner.options", fallback={})
            )

            runner.run(extractor, mapping, loader)
            output_file_path = config.get("loader.options.path")
            return output_file_path
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logger.error(
                f"{repr(e)}, line {exc_tb.tb_lineno} in {exc_tb.tb_frame.f_code.co_filename}"
            )
            return None


def transform_format(path_to_config_file):
    with open(path_to_config_file, 'r') as file:
        config_dict = yaml.safe_load(file)
    config = Config(config_dict)
    controller = Controller(config)
    output_file_paths = controller.run()
    return output_file_paths
