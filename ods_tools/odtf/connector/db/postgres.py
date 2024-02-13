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
