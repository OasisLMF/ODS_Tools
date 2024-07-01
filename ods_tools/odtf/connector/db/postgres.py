from typing import Dict

import pandas as pd
import psycopg2
import psycopg2.extras

from .base import BaseDBConnector
from .errors import DBConnectionError


class PostgresConnector(BaseDBConnector):
    """
    Connects to a Postgres database for reading and writing data.
    """

    name = "Postgres Connector"
    sql_params_output = "pyformat"

    def _create_connection(self, database: Dict[str, str]):
        """
        Create database connection to the Postgres database
        :param database: Dict with database connection settings

        :return: Connection object
        """
        try:
            conn = psycopg2.connect(**database)
        except Exception as e:
            raise DBConnectionError(e)

        return conn

    def fetch_data(self, batch_size: int):
        """
        Fetch data from the database in batches.

        :param batch_size: Number of rows per batch

        :yield: Data batches as pandas DataFrames
        """

        with open(self.sql_statement_path, 'r') as file:
            sql_query = file.read()

        with self._create_connection(self.database) as conn:
            for batch in pd.read_sql(sql_query, conn, chunksize=batch_size):
                yield batch
