"""Tests for ``-- param:`` directive parsing + introspection (Ch2, sqlspec-smgc.2)."""

import logging

import pytest

from sqlspec.core import ParameterDeclaration
from sqlspec.exceptions import SQLFileParseError, SQLStatementNotFoundError
from sqlspec.loader import PARAM_PATTERN, SQLFileLoader


@pytest.mark.parametrize(
    ("line", "name", "type_str", "required", "description"),
    [
        ("-- param: status_cd str The status code", "status_cd", "str", True, "The status code"),
        ("-- param: limit int", "limit", "int", True, None),
        ("-- param: offer_ids list[int] List of ids", "offer_ids", "list[int]", True, "List of ids"),
        ("-- param: status_cd str? Optional status filter", "status_cd", "str", False, "Optional status filter"),
        ("-- param: payload dict[str, Any]? Optional payload", "payload", "dict[str, Any]", False, "Optional payload"),
        ("--param:x bool", "x", "bool", True, None),
        ("-- PARAM: Y Decimal money", "Y", "Decimal", True, "money"),
    ],
)
def test_param_pattern(line: str, name: str, type_str: str, required: bool, description: "str | None") -> None:
    m = PARAM_PATTERN.match(line)
    assert m is not None
    assert m.group("name") == name
    assert m.group("type") == type_str
    assert (m.group("optional") != "?") is required
    assert m.group("desc") == description


def test_parse_optional_declared_param_suffix() -> None:
    content = "-- name: q\n-- param: status_cd str? Optional status filter\nselect :status_cd\n"
    statements = SQLFileLoader._parse_statements(content, "test.sql")
    assert statements["q"].parameters == (
        ParameterDeclaration("status_cd", "str", required=False, description="Optional status filter"),
    )


def test_parse_optional_declared_param_description_marker() -> None:
    content = "-- name: q\n-- param: status_cd str Status filter (optional)\nselect :status_cd\n"
    statements = SQLFileLoader._parse_statements(content, "test.sql")
    assert statements["q"].parameters == (
        ParameterDeclaration("status_cd", "str", required=False, description="Status filter"),
    )


def test_parse_declared_params_interleaved_with_dialect() -> None:
    content = """
-- name: get_offers
-- dialect: oracle
-- param: status_cd str The status code
-- param: offer_ids list[int] List of offer IDs
-- param: limit int Maximum rows
select offer_id from offers where status_cd = :status_cd and offer_id in (:offer_ids)
fetch first :limit rows only
"""
    statements = SQLFileLoader._parse_statements(content, "test.sql")
    stmt = statements["get_offers"]
    assert stmt.dialect == "oracle"
    assert stmt.parameters == (
        ParameterDeclaration("status_cd", "str", description="The status code"),
        ParameterDeclaration("offer_ids", "list[int]", description="List of offer IDs"),
        ParameterDeclaration("limit", "int", description="Maximum rows"),
    )
    assert stmt.sql.startswith("select offer_id from offers")
    assert "-- param" not in stmt.sql


def test_query_without_params_is_unchanged() -> None:
    content = "-- name: plain\nselect 1\n"
    statements = SQLFileLoader._parse_statements(content, "test.sql")
    assert statements["plain"].parameters == ()
    assert statements["plain"].sql == "select 1"


def test_malformed_param_warns_and_skips_by_default(caplog: pytest.LogCaptureFixture) -> None:
    content = "-- name: q\n-- param: oops\nselect 1\n"
    with caplog.at_level(logging.WARNING):
        statements = SQLFileLoader._parse_statements(content, "test.sql")
    assert statements["q"].parameters == ()
    assert statements["q"].sql == "select 1"
    assert any("malformed" in r.message or "param" in r.message.lower() for r in caplog.records)


def test_malformed_param_raises_in_strict_mode() -> None:
    content = "-- name: q\n-- param: oops\nselect 1\n"
    with pytest.raises(SQLFileParseError):
        SQLFileLoader._parse_statements(content, "test.sql", strict_parameter_annotations=True)


def test_malformed_param_strict_error_reports_line_number() -> None:
    content = "-- name: q\n-- param: oops\nselect 1\n"
    with pytest.raises(SQLFileParseError) as exc_info:
        SQLFileLoader._parse_statements(content, "test.sql", strict_parameter_annotations=True)
    assert "(line 2)" in str(exc_info.value)
    assert exc_info.value.line == 2


def test_add_named_sql_with_parameters() -> None:
    loader = SQLFileLoader()
    decls = [ParameterDeclaration("a", "int")]
    loader.add_named_sql("q", "select :a", parameters=decls)
    assert loader.get_query_parameters("q") == (ParameterDeclaration("a", "int"),)


def test_get_query_parameters_empty_and_missing() -> None:
    loader = SQLFileLoader()
    loader.add_named_sql("plain", "select 1")
    assert loader.get_query_parameters("plain") == ()
    with pytest.raises(SQLStatementNotFoundError):
        loader.get_query_parameters("nope")


def test_declared_params_survive_file_cache_roundtrip(tmp_path: "object") -> None:
    """Declarations ride on NamedStatement inside SQLFileCacheEntry, so a second
    loader that hits the file cache must see the same declarations."""
    from pathlib import Path

    sql_path = Path(str(tmp_path)) / "q.sql"
    sql_path.write_text("-- name: q\n-- param: a int Identifier\nselect :a\n")

    first = SQLFileLoader()
    first.load_sql(sql_path)
    assert first.get_query_parameters("q") == (ParameterDeclaration("a", "int", description="Identifier"),)

    # A fresh loader reuses the populated file cache (same content hash).
    second = SQLFileLoader()
    second.load_sql(sql_path)
    assert second.get_query_parameters("q") == (ParameterDeclaration("a", "int", description="Identifier"),)
