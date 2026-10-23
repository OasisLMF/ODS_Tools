from unittest import mock

import pytest

pytest.importorskip("psycopg")

from ods_tools.odtf.connector.db import PostgresConnector  # noqa: E402
from ods_tools.odtf.connector.db.errors import DBConnectionError  # noqa: E402


def make_connector(**database):
    config = {
        "database": {
            "database": "oed",
            "sql_statement_path": "select.sql",
            "output_table": None,
            **database,
        }
    }
    return PostgresConnector(config, isExtractor=True)


def test_create_connection__passes_only_connection_options():
    connector = make_connector(host="db-host", port="5432", user="oasis", password="secret")

    with mock.patch("psycopg.connect") as connect:
        conn = connector._create_connection(connector.database)

    connect.assert_called_once_with(dbname="oed", host="db-host", port="5432", user="oasis", password="secret")
    assert conn is connect.return_value


def test_create_connection__skips_empty_options():
    connector = make_connector(host="db-host", port="", user="", password="")

    with mock.patch("psycopg.connect") as connect:
        connector._create_connection(connector.database)

    connect.assert_called_once_with(dbname="oed", host="db-host")


def test_create_connection__wraps_driver_errors():
    connector = make_connector(host="db-host")

    with mock.patch("psycopg.connect", side_effect=Exception("connection refused")):
        with pytest.raises(DBConnectionError):
            connector._create_connection(connector.database)


def test_insert_statement__uses_named_placeholders():
    connector = make_connector()

    assert connector._get_insert_statements(["LocNumber", "AccNumber"]) == (
        "INSERT INTO output (LocNumber, AccNumber) VALUES (%(LocNumber)s, %(AccNumber)s);"
    )
