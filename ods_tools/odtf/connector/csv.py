from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd

from .base import BaseConnector
from ..notset import NotSetType


class CsvConnector(BaseConnector):
    """
    Connects to a csv file on the local machine for reading and writing data.

    **Options:**

    * `path` - The path to the csv file to read/write
    * `write_header` - Flag whether the header row should be written to the
      target when loading data (default: `True`)
    * `quoting` - What type of quoting should be used when reading and writing
      data. Valid values are `all`, `minimal`, `nonnumeric` and `none`.
      Descriptions of these values are given in the
      `python csv module documentation
      <https://docs.python.org/3/library/csv.html#csv.QUOTE_ALL>`__.
      (default: `nonnumeric`).
    """

    name = "CSV Connector"
    options_schema = {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": (
                    "The path to the file to load relative to the config file"
                ),
                "subtype": "path",
                "title": "Path",
            },
            "write_header": {
                "type": "boolean",
                "description": "Should the header row be written?",
                "default": True,
                "title": "Include Header",
            },
            "quoting": {
                "type": "string",
                "description": (
                    "The type of quoting to use when "
                    "reading/writing entries (see "
                    "https://docs.python.org/3/library/csv.html#csv.QUOTE_ALL "
                    "for a description of the values)"
                ),
                "enum": ["all", "minimal", "none", "nonnumeric"],
                "default": "nonnumeric",
                "title": "Quoting",
            },
        },
        "required": ["path"],
    }

    def __init__(self, config, isExtractor):
        super().__init__(config, isExtractor)
        if isExtractor:
            self.file_path = config['input']['path']
        else:
            self.file_path = config['output']['path']
        self.write_header = config.get("write_header", True)

    def _data_serializer(self, row):
        def process_value(v):
            if v is None or isinstance(v, NotSetType):
                return ""
            # add quotes to values that contain special characters
            if isinstance(v, str) and any(d in v for d in [',', ';', '\t', '\n', '"']):
                return f'"{v}"'
            return str(v)

        return [process_value(v) for v in row.values()]

    def load(self, data: Iterable[Dict[str, Any]]):
        first_batch = True

        with open(self.file_path, "w", newline="") as f:
            for batch in data:
                fieldnames = list(batch.keys())

                rows = np.array([self._data_serializer(batch)])

                if first_batch and self.write_header:
                    header = ','.join(fieldnames)
                    np.savetxt(f, [], fmt='%s', delimiter=',', header=header, comments='')
                    first_batch = False

                np.savetxt(f, rows, fmt='%s', delimiter=',', comments='')

    def fetch_data(self, chunksize: int) -> Iterable[pd.DataFrame]:
        """
        Fetch data from the csv file in batches.

        :param chunksize: Number of rows per batch
        :return: Iterable of data batches as pandas DataFrames
        """
        for batch in pd.read_csv(self.file_path, chunksize=chunksize, low_memory=False):
            yield batch
