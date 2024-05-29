from typing import Dict

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
        except Exception:
            raise DBConnectionError()

        return conn

    def _get_cursor(self, conn):
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cur

    def get_columns(self, database: Dict[str, str]):
        """
        Get the column names from a specific table in the database.

        :param table_name: The name of the table to get the columns from.

        :return: A list of column names.
        """

        # NOTE: add get_columns method to the other connectors
        # NOTE: for test only, update to use sql query selected in config file

        with self._create_connection(database) as conn:
            with self._get_cursor(conn) as cur:
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table';")
                columns = [row[0] for row in cur.fetchall()]
        return columns
