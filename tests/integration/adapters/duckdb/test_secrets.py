"""Integration tests for DuckDB secrets declared through the connection pool."""

from uuid import uuid4

import duckdb
import pytest

from sqlspec.adapters.duckdb.pool import _create_secret


def test_two_connections_same_database_create_secret() -> None:
    database = f":memory:sqlspec_secret_{uuid4().hex}"
    secret = {"name": "sqlspec_t", "secret_type": "gcs", "value": {"key_id": "k", "secret": "s"}, "required": True}
    first = duckdb.connect(database)
    second = duckdb.connect(database)
    try:
        assert _create_secret(first, secret)
        assert _create_secret(second, secret)
        row = first.execute("SELECT count(*) FROM duckdb_secrets() WHERE name = 'sqlspec_t'").fetchone()
    finally:
        second.close()
        first.close()

    assert row == (1,)


def test_required_secret_with_invalid_parameter_raises() -> None:
    secret = {"name": "sqlspec_bad", "secret_type": "gcs", "value": {"nonsense_key": 1}, "required": True}
    connection = duckdb.connect(":memory:")
    try:
        with pytest.raises(duckdb.Error, match="nonsense_key"):
            _create_secret(connection, secret)
    finally:
        connection.close()
