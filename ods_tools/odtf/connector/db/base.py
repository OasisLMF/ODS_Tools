from typing import Any, Dict, Iterable

from ods_tools.odtf.connector import BaseConnector

from .errors import DBQueryError


class BaseDBConnector(BaseConnector):
    """
    Connects to a database for reading and writing data.

    **Options:**

    * `host` - Which host to use when connecting to the database
    * `port` - The port to use when connecting to the database
    * `database` - The database name or relative path to the file for sqlite3
    * `user` - The username to use when connecting to the database
    * `password` - The password to use when connecting to the database
    * `select_statement` - sql query to read the data from
    * `insert_statement` - sql query to insert the data from
    """

    name = "BaseDB Connector"
    options_schema = {
        "type": "object",
        "properties": {
            "host": {
                "type": "string",
                "description": (
                    "Which host to use when connecting to the database. "
                    "Not used with SQLite."
                ),
                "default": "",
                "title": "Host",
            },
            "port": {
                "type": "string",
                "description": (
                    "The port to use when connecting to the database. "
                    "Not used with SQLite."
                ),
                "default": "",
                "title": "Port",
            },
            "database": {
                "type": "string",
                "description": (
                    "The database name or relative path to the file for "
                    "sqlite3"
                ),
                "title": "Database",
            },
            "user": {
                "type": "string",
                "description": (
                    "The username to use when connecting to the database. "
                    "Not used with SQLite."
                ),
                "default": "",
                "title": "User",
            },
            "password": {
                "type": "password",
                "description": (
                    "The password to use when connecting to the database. "
                    "Not used with SQLite."
                ),
                "default": "",
                "title": "Password",
            },
            "sql_statement": {
                "type": "string",
                "description": "The path to the file which contains the "
                "sql statement to run",
                "subtype": "path",
                "title": "Select Statement File",
            },
        },
        "required": ["database", "select_statement", "insert_statement"],
    }
    sql_params_output = "qmark"

    def __init__(self, config, **options):
        super().__init__(config, **options)

        self.database = config['database']
        if self.database['output_table'] is None:
            self.database['output_table'] = 'output'
        self.sql_statement_path = config["database"]["sql_statement_path"]

    def _create_connection(self, database: Dict[str, str]):
        raise NotImplementedError()

    def _get_cursor(self, conn):
        cur = conn.cursor()
        return cur

    def _get_select_statement(self) -> str:
        """
        SQL string to select the data from the DB

        :return: string
        """
        with open(self.sql_statement_path) as f:
            select_statement = f.read()

        return select_statement

    def _get_insert_statements(self, columns):
        """
        SQL string(s) to insert the data into the DB

        :return: Sql statements
        """
        columns_sql = ', '.join(columns)
        placeholders = ', '.join(f':{col}' for col in columns)
        return f"INSERT INTO output ({columns_sql}) VALUES ({placeholders});"

    def _create_table(self, row):
        def infer_sql_type(value):
            if isinstance(value, int):
                return "INTEGER"
            elif isinstance(value, float):
                return "REAL"
            else:
                return "TEXT"

        types = {col: infer_sql_type(value) for col, value in row.items()}
        columns = ", ".join(f"{col} {types[col]}" for col in row)
        return f"CREATE TABLE IF NOT EXISTS {self.database['output_table']} ({columns});"

    def load(self, generator):
        first_row = next(generator)

        create_sql = self._create_table(first_row)
        insert_sql = self._get_insert_statements(first_row.keys())

        conn = self._create_connection(self.database)
        with conn:
            try:
                cur = conn.cursor()
                cur.execute("DROP TABLE IF EXISTS output")
                cur.execute(create_sql)
                cur.execute(insert_sql, first_row)
                cur.executemany(insert_sql, generator)

            except Exception as e:
                raise DBQueryError("Insert/Create Table", e, first_row)

    def row_to_dict(self, row):
        """
        Convert the row returned from the cursor into a dictionary

        :return: Dict
        """
        return dict(row)

    def extract(self) -> Iterable[Dict[str, Any]]:
        select_sql = self._get_select_statement()
        conn = self._create_connection(self.database)

        with conn:
            cur = self._get_cursor(conn)
            try:
                cur.execute(select_sql)
            except Exception as e:
                raise DBQueryError(select_sql, e)

            rows = cur.fetchall()
            for row in rows:
                yield self.row_to_dict(row)
