"""Unit tests for MERGE dialect validation edge cases."""

import pytest

from sqlspec import sql
from sqlspec.exceptions import DialectNotSupportedError


def test_merge_mysql_raises_dialect_error() -> None:
    """Test MERGE with MySQL dialect raises DialectNotSupportedError."""
    query = sql.merge(dialect="mysql").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError, match=r"MERGE.*not supported.*MYSQL"):
        query.build()


def test_merge_sqlite_raises_dialect_error() -> None:
    """Test MERGE with SQLite dialect raises DialectNotSupportedError."""
    query = sql.merge(dialect="sqlite").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError, match=r"MERGE.*not supported.*SQLITE"):
        query.build()


def test_merge_duckdb_raises_dialect_error() -> None:
    """Test MERGE with DuckDB dialect raises DialectNotSupportedError."""
    query = sql.merge(dialect="duckdb").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError, match=r"MERGE.*not supported.*DUCKDB"):
        query.build()


def test_merge_error_message_includes_alternatives() -> None:
    """Test MERGE error message suggests alternatives."""
    query = sql.merge(dialect="mysql").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError, match=r"INSERT.*ON DUPLICATE KEY UPDATE"):
        query.build()


def test_merge_sqlite_error_suggests_on_conflict() -> None:
    """Test MERGE error for SQLite suggests INSERT ON CONFLICT."""
    query = sql.merge(dialect="sqlite").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError, match=r"INSERT.*ON CONFLICT"):
        query.build()


def test_merge_postgres_passes_validation() -> None:
    """Test MERGE with PostgreSQL dialect passes validation."""
    query = (
        sql
        .merge(dialect="postgres")
        .into("products", alias="t")
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id")
    )

    result = query.build()
    assert "MERGE INTO" in result.sql


def test_merge_oracle_passes_validation() -> None:
    """Test MERGE with Oracle dialect passes validation."""
    query = (
        sql
        .merge(dialect="oracle")
        .into("products", alias="t")
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id")
    )

    result = query.build()
    assert "MERGE INTO" in result.sql


def test_merge_bigquery_passes_validation() -> None:
    """Test MERGE with BigQuery dialect passes validation."""
    query = (
        sql
        .merge(dialect="bigquery")
        .into("products", alias="t")
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id")
    )

    result = query.build()
    assert "MERGE INTO" in result.sql or "MERGE" in result.sql


def test_merge_tsql_passes_validation() -> None:
    """Test MERGE with T-SQL/SQL Server dialect passes validation."""
    query = (
        sql
        .merge(dialect="tsql")
        .into("products", alias="t")
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id")
    )

    result = query.build()
    assert "MERGE INTO" in result.sql or "MERGE" in result.sql


def test_merge_teradata_passes_validation() -> None:
    """Test MERGE with Teradata dialect passes validation."""
    query = (
        sql
        .merge(dialect="teradata")
        .into("products", alias="t")
        .using({"id": 1}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id")
    )

    result = query.build()
    assert "MERGE INTO" in result.sql or "MERGE" in result.sql


def test_merge_multiple_unsupported_dialects() -> None:
    """Test MERGE dialect validation with multiple unsupported dialects."""
    query_mysql = sql.merge(dialect="mysql").into("products", alias="t")
    query_sqlite = sql.merge(dialect="sqlite").into("products", alias="t")
    query_duckdb = sql.merge(dialect="duckdb").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError):
        query_mysql.build()

    with pytest.raises(DialectNotSupportedError):
        query_sqlite.build()

    with pytest.raises(DialectNotSupportedError):
        query_duckdb.build()


def test_merge_validation_happens_at_build_time() -> None:
    """Test MERGE dialect validation happens during build(), not construction."""
    query = sql.merge(dialect="mysql").into("products", alias="t")

    assert query.dialect_name == "mysql"

    with pytest.raises(DialectNotSupportedError):
        query.build()


def test_merge_unsupported_dialect_with_full_query() -> None:
    """Test MERGE raises error even with complete query for unsupported dialect."""
    query = (
        sql
        .merge(dialect="sqlite")
        .into("products", alias="t")
        .using({"id": 1, "name": "Product 1"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    with pytest.raises(DialectNotSupportedError):
        query.build()


def test_merge_error_includes_dialect_name() -> None:
    """Test MERGE error message includes the unsupported dialect name."""
    query = sql.merge(dialect="duckdb").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError, match="DUCKDB"):
        query.build()


def test_merge_validation_with_bulk_data() -> None:
    """Test MERGE dialect validation works with bulk data."""
    bulk_data = [{"id": i, "name": f"Product {i}"} for i in range(10)]

    query = (
        sql
        .merge(dialect="mysql")
        .into("products", alias="t")
        .using(bulk_data, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    with pytest.raises(DialectNotSupportedError):
        query.build()


def test_merge_supported_dialects_complete_chain() -> None:
    """Test all supported dialects work with complete MERGE chain."""
    supported_dialects = ["postgres", "oracle", "bigquery", "tsql", "teradata"]

    for dialect in supported_dialects:
        query = (
            sql
            .merge(dialect=dialect)
            .into("products", alias="t")
            .using({"id": 1, "name": "Test"}, alias="src")
            .on("t.id = src.id")
            .when_matched_then_update(name="src.name")
            .when_not_matched_then_insert(id="src.id", name="src.name")
        )

        result = query.build()
        assert "MERGE" in result.sql, f"Failed for dialect: {dialect}"


def test_merge_validation_with_to_sql() -> None:
    """Test MERGE dialect validation also happens with to_sql()."""
    query = sql.merge(dialect="mysql").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError):
        query.to_sql()


def test_merge_validation_error_type() -> None:
    """Test MERGE validation raises correct exception type."""
    query = sql.merge(dialect="sqlite").into("products", alias="t")

    with pytest.raises(DialectNotSupportedError) as exc_info:
        query.build()

    assert isinstance(exc_info.value, DialectNotSupportedError)
