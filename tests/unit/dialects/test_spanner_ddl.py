"""Unit tests for Spanner DDL (STORED generated columns and CHANGE STREAM)."""

from sqlglot import exp, parse_one


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
