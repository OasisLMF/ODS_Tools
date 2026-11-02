from typing import Dict

import pandas as pd
import psycopg

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
        # only pass libpq connection options, the database config also holds
        # keys such as `sql_statement_path` which psycopg would reject
        conn_kwargs = {
            key: database[key]
            for key in ("host", "port", "user", "password")
            if database.get(key)
        }
        try:
            conn = psycopg.connect(dbname=database["database"], **conn_kwargs)
        except Exception as e:
            raise DBConnectionError(e)

        return conn

    def _get_insert_statements(self, columns):
        """
        SQL string(s) to insert the data into the DB, using psycopg named placeholders

        :return: Sql statements
        """
        columns_sql = ', '.join(columns)
        placeholders = ', '.join(f'%({col})s' for col in columns)
        return f"INSERT INTO output ({columns_sql}) VALUES ({placeholders});"

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
