"""Unit tests for the custom SAP SQL Anywhere sqlglot dialect."""

import sqlglot
from sqlglot import exp, parse_one, transpile
from sqlglot.dialects.dialect import NormalizationStrategy

from sqlspec.dialects.sybase import SQLAnywhere, SQLAnywhereTokenizer

__all__ = ()


def test_sqlanywhere_dialect_registration() -> None:
    """Verify SQLAnywhere dialect is registered and resolvable by name."""
    dialect = sqlglot.Dialect.get_or_raise("sqlanywhere")
    assert dialect.__class__ is SQLAnywhere
    assert dialect.tokenizer_class is SQLAnywhereTokenizer
    assert dialect.NORMALIZATION_STRATEGY == NormalizationStrategy.CASE_SENSITIVE


def test_sqlanywhere_limit_and_offset_rendering() -> None:
    """Verify SQLAnywhere renders trailing LIMIT n and LIMIT n OFFSET m without TOP or FETCH."""
    limit_only = transpile("SELECT id, name FROM users LIMIT 10", write="sqlanywhere")[0]
    assert limit_only == "SELECT id, name FROM users LIMIT 10"

    limit_offset = transpile("SELECT id, name FROM users ORDER BY id LIMIT 10 OFFSET 20", write="sqlanywhere")[0]
    assert limit_offset == "SELECT id, name FROM users ORDER BY id LIMIT 10 OFFSET 20"

    offset_only = transpile("SELECT id FROM users ORDER BY id OFFSET 5", write="sqlanywhere")[0]
    assert offset_only == "SELECT id FROM users ORDER BY id OFFSET 5"


def test_sqlanywhere_parse_top_start_at() -> None:
    """Verify Watcom-SQL SELECT TOP n START AT m parses into 0-based Limit and Offset."""
    parsed = parse_one("SELECT TOP 10 START AT 21 id, name FROM users ORDER BY id", dialect="sqlanywhere")
    assert isinstance(parsed, exp.Select)
    assert isinstance(parsed.args.get("limit"), exp.Limit)
    assert isinstance(parsed.args.get("offset"), exp.Offset)
    assert parsed.sql(dialect="sqlanywhere") == "SELECT id, name FROM users ORDER BY id LIMIT 10 OFFSET 20"


def test_sqlanywhere_copy_false_preserves_ast_pagination() -> None:
    """Verify expression.sql('sqlanywhere', copy=False) never mutates limit or offset in place."""
    select_expr = parse_one("SELECT id FROM users ORDER BY id LIMIT 10 OFFSET 5")
    first_sql = select_expr.sql(dialect="sqlanywhere", copy=False)
    second_sql = select_expr.sql(dialect="sqlanywhere", copy=False)
    assert first_sql == "SELECT id FROM users ORDER BY id LIMIT 10 OFFSET 5"
    assert second_sql == first_sql
    assert isinstance(select_expr.args.get("limit"), exp.Limit)
    assert isinstance(select_expr.args.get("offset"), exp.Offset)


def test_sqlanywhere_create_and_drop_if_exists() -> None:
    """Verify SQLAnywhere emits native single-statement CREATE TABLE IF NOT EXISTS and DROP TABLE IF EXISTS."""
    create_sql = transpile(
        "CREATE TABLE IF NOT EXISTS sessions (id VARCHAR(64) PRIMARY KEY, data TEXT)",
        read="postgres",
        write="sqlanywhere",
    )[0]
    assert create_sql == "CREATE TABLE IF NOT EXISTS sessions (id VARCHAR(64) PRIMARY KEY, data LONG VARCHAR)"
    assert "OBJECT_ID" not in create_sql

    drop_sql = transpile("DROP TABLE IF EXISTS sessions", read="postgres", write="sqlanywhere")[0]
    assert drop_sql == "DROP TABLE IF EXISTS sessions"


def test_sqlanywhere_type_mappings() -> None:
    """Verify SQLAnywhere type mappings for LONG VARCHAR, LONG BINARY, TIMESTAMP, UNIQUEIDENTIFIER, and BIT."""
    assert parse_one("CAST(x AS TEXT)", read="postgres").sql("sqlanywhere") == "CAST(x AS LONG VARCHAR)"
    assert parse_one("CAST(x AS JSONB)", read="postgres").sql("sqlanywhere") == "CAST(x AS LONG VARCHAR)"
    assert parse_one("CAST(x AS BLOB)", dialect="sqlanywhere").sql("sqlanywhere") == "CAST(x AS LONG BINARY)"
    assert parse_one("CAST(x AS BYTEA)", read="postgres").sql("sqlanywhere") == "CAST(x AS VARBINARY(32767))"
    assert parse_one("CAST(x AS TIMESTAMP)", read="postgres").sql("sqlanywhere") == "CAST(x AS TIMESTAMP)"
    assert parse_one("CAST(x AS TIMESTAMPTZ)", read="postgres").sql("sqlanywhere") == (
        "CAST(x AS TIMESTAMP WITH TIME ZONE)"
    )
    assert parse_one("CAST(x AS UUID)", read="postgres").sql("sqlanywhere") == "CAST(x AS UNIQUEIDENTIFIER)"
    assert parse_one("CAST(x AS BOOLEAN)", read="postgres").sql("sqlanywhere") == "CAST(x AS BIT)"
    assert parse_one("CAST(x AS VARCHAR)", read="postgres").sql("sqlanywhere") == "CAST(x AS VARCHAR(32767))"
    assert parse_one("CAST(x AS NVARCHAR(MAX))", read="tsql").sql("sqlanywhere") == "CAST(x AS LONG NVARCHAR)"


def test_sqlanywhere_long_types_and_utc_timestamp_parsing() -> None:
    """Verify LONG VARCHAR, LONG BINARY, and CURRENT UTC TIMESTAMP parse cleanly in SQLAnywhere."""
    parsed = parse_one(
        "CREATE TABLE IF NOT EXISTS docs (id UNIQUEIDENTIFIER, body LONG VARCHAR, raw_bytes LONG BINARY, created_at TIMESTAMP DEFAULT CURRENT UTC TIMESTAMP)",
        dialect="sqlanywhere",
    )
    rendered = parsed.sql(dialect="sqlanywhere")
    assert "LONG VARCHAR" in rendered
    assert "LONG BINARY" in rendered
    assert "UNIQUEIDENTIFIER" in rendered
