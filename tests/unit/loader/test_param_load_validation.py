"""Load-time validation: declared-name drift + positional count (Ch4, sqlspec-smgc.4)."""

import pytest

from sqlspec.core import ParameterDeclaration
from sqlspec.exceptions import SQLFileParseError
from sqlspec.loader import SQLFileLoader


def test_named_drift_raises() -> None:
    content = "-- name: q\n-- param: status_cd str The code\nselect 1 from t where status = :status\n"
    with pytest.raises(SQLFileParseError, match="status_cd"):
        SQLFileLoader._parse_sql_content(content, "test.sql")


def test_named_all_present_loads() -> None:
    content = "-- name: q\n-- param: a int\n-- param: b int\nselect :a, :b\n"
    statements = SQLFileLoader._parse_sql_content(content, "test.sql")
    assert len(statements["q"].parameters) == 2


def test_undeclared_placeholder_is_allowed() -> None:
    # declared subset of placeholders -> OK (filters/undeclared params are legal)
    content = "-- name: q\n-- param: a int\nselect :a, :b\n"
    statements = SQLFileLoader._parse_sql_content(content, "test.sql")
    assert statements["q"].parameters == (ParameterDeclaration("a", "int"),)


def test_positional_count_mismatch_raises() -> None:
    content = "-- name: q\n-- param: a int\n-- param: b int\n-- param: c int\nselect ?, ?\n"
    with pytest.raises(SQLFileParseError, match="positional"):
        SQLFileLoader._parse_sql_content(content, "test.sql")


def test_positional_count_match_loads() -> None:
    content = "-- name: q\n-- param: a int\n-- param: b int\nselect ?, ?\n"
    statements = SQLFileLoader._parse_sql_content(content, "test.sql")
    assert len(statements["q"].parameters) == 2


def test_no_declarations_skips_validation() -> None:
    # mismatched counts but no declarations -> no validation, loads fine
    content = "-- name: q\nselect ?, ?, ?\n"
    statements = SQLFileLoader._parse_sql_content(content, "test.sql")
    assert statements["q"].parameters == ()


def test_add_named_sql_validates_drift() -> None:
    loader = SQLFileLoader()
    with pytest.raises(SQLFileParseError, match="nope"):
        loader.add_named_sql("q", "select :a", parameters=[ParameterDeclaration("nope", "int")])
