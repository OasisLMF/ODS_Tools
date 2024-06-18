from typing import Dict

import pandas as pd
import pyodbc

from .base import BaseDBConnector
from .errors import DBConnectionError


class SQLServerConnector(BaseDBConnector):
    """
    Connects to an Microsoft SQL Server for reading and writing data.
    """

    name = "SQL Server Connector"
    driver = "{ODBC Driver 17 for SQL Server}"

    def _create_connection(self, database: Dict[str, str]):
        """
        Create database connection to the SQLite database specified in database
        :param database: Dict object with connection info

        :return: Connection object
        """

        try:
            conn = pyodbc.connect(
                "DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}".format(
                    self.driver,
                    database["host"],
                    database["port"],
                    database["database"],
                    database["user"],
                    database["password"],
                )
            )
        except Exception:
            raise DBConnectionError()

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
