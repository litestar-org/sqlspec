"""Unit tests for DuckDB connection pool helpers."""

import pytest

pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")

from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool, _validate_sql_identifier


@pytest.mark.parametrize("identifier", ["my_openai_secret", "openai", "S3", "s3", "r2", "secret_1"])
def test_validate_sql_identifier_accepts_safe_identifiers(identifier: str) -> None:
    _validate_sql_identifier(identifier, "secret_name")


@pytest.mark.parametrize("identifier", ["evil; DROP TABLE secrets--", "bad name", "S3); DROP TABLE--", "1bad", ""])
def test_validate_sql_identifier_rejects_unsafe_identifiers(identifier: str) -> None:
    with pytest.raises(ValueError, match="secret_name"):
        _validate_sql_identifier(identifier, "secret_name")


def test_create_connection_raises_for_malicious_secret_name() -> None:
    pool = DuckDBConnectionPool(
        connection_config={"database": ":memory:"},
        secrets=[
            {"name": "evil; DROP TABLE secrets--", "secret_type": "s3", "value": {"key_id": "abc", "secret": "xyz"}}
        ],
    )

    with pytest.raises(ValueError, match="secret_name"):
        pool._create_connection()


def test_create_connection_raises_for_malicious_secret_type() -> None:
    pool = DuckDBConnectionPool(
        connection_config={"database": ":memory:"},
        secrets=[
            {
                "name": "safe_secret",
                "secret_type": "S3); DROP TABLE secrets--",
                "value": {"key_id": "abc", "secret": "xyz"},
            }
        ],
    )

    with pytest.raises(ValueError, match="secret_type"):
        pool._create_connection()
