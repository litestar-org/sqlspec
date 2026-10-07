"""Unit tests for Spanner DDL (column types, defaults, STORED generated columns, CHANGE STREAM)."""

import pytest
from sqlglot import exp, parse_one

from sqlspec.builder import sql as sql_builder


def test_generated_column_stored_enforcement() -> None:
    """Verify generated columns in Spanner DDL always emit STORED."""
    sql = "CREATE TABLE Items (Id INT64, Price FLOAT64, Tax FLOAT64, Total FLOAT64 AS (Price + Tax) STORED) PRIMARY KEY (Id)"
    parsed = parse_one(sql, dialect="spanner")
    rendered = parsed.sql(dialect="spanner")
    assert "Total FLOAT64 AS (Price + Tax) STORED" in rendered
    assert "PERSISTED" not in rendered


def test_create_change_stream_for_all_with_options() -> None:
    """Verify CREATE CHANGE STREAM FOR ALL with OPTIONS."""
    sql = "CREATE CHANGE STREAM AllStream FOR ALL OPTIONS (retention_period = '7d')"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Create)
    assert parsed.kind == "CHANGE STREAM"
    rendered = parsed.sql(dialect="spanner")
    assert "CREATE CHANGE STREAM AllStream" in rendered
    assert "FOR ALL" in rendered
    assert "retention_period = '7d'" in rendered


def test_create_change_stream_for_specific_tables() -> None:
    """Verify CREATE CHANGE STREAM FOR table(cols), table."""
    sql = "CREATE CHANGE STREAM TableStream FOR Singers(FirstName, LastName), Albums"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Create)
    rendered = parsed.sql(dialect="spanner")
    assert "CREATE CHANGE STREAM TableStream" in rendered
    assert "Singers(FirstName, LastName)" in rendered
    assert "Albums" in rendered


def test_alter_change_stream() -> None:
    """Verify ALTER CHANGE STREAM SET OPTIONS."""
    sql = "ALTER CHANGE STREAM AllStream SET OPTIONS (retention_period = '36h')"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Alter)
    assert parsed.args.get("kind") == "CHANGE STREAM"
    rendered = parsed.sql(dialect="spanner")
    assert "ALTER CHANGE STREAM AllStream SET OPTIONS (retention_period = '36h')" in rendered


def test_drop_change_stream() -> None:
    """Verify DROP CHANGE STREAM."""
    sql = "DROP CHANGE STREAM AllStream"
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Drop)
    rendered = parsed.sql(dialect="spanner")
    assert "DROP CHANGE STREAM AllStream" in rendered


@pytest.mark.parametrize(
    ("column_type", "expected"),
    [
        ("STRING", "STRING(MAX)"),
        ("STRING(64)", "STRING(64)"),
        ("BYTES", "BYTES(MAX)"),
        ("ARRAY<STRING>", "ARRAY<STRING(MAX)>"),
        ("INT64", "INT64"),
    ],
)
def test_unsized_column_types_render_max_length(column_type: str, expected: str) -> None:
    """Verify column definitions give unsized STRING and BYTES an explicit MAX length."""
    sql = parse_one(f"CREATE TABLE t (id INT64, c {column_type}) PRIMARY KEY (id)", read="spanner").sql(
        dialect="spanner"
    )

    assert sql == f"CREATE TABLE t (id INT64, c {expected}) PRIMARY KEY (id)"


def test_cast_types_keep_unsized_form() -> None:
    """Verify query casts keep the unsized STRING and BYTES forms."""
    sql = parse_one("SELECT CAST(a AS STRING), CAST(b AS BYTES) FROM t", read="spanner").sql(dialect="spanner")

    assert sql == "SELECT CAST(a AS STRING), CAST(b AS BYTES) FROM t"


def test_column_default_renders_parenthesized() -> None:
    """Verify builder column defaults render inside the parentheses Spanner requires."""
    builder = sql_builder.create_table("events").column("id", "STRING(36)", primary_key=True)
    builder = builder.column("created_at", "TIMESTAMP", default="CURRENT_TIMESTAMP", not_null=True)

    rendered = builder.build(dialect="spanner").sql

    assert "`created_at` TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP())" in rendered


def test_parenthesized_column_default_is_not_wrapped_twice() -> None:
    """Verify an already parenthesized default round-trips unchanged."""
    statement = "CREATE TABLE t (id INT64, d STRING(MAX) DEFAULT (CAST(1 AS STRING))) PRIMARY KEY (id)"

    assert parse_one(statement, read="spanner").sql(dialect="spanner") == statement
