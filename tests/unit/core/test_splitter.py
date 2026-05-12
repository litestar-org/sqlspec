"""Unit tests for SQL splitter helpers."""

from sqlspec.core.splitter import _join_string_fragments, split_sql_script


def test_join_string_fragments_returns_joined_text() -> None:
    """The optional writer path should preserve plain string assembly behavior."""
    assert _join_string_fragments(["SELECT", " ", "1"]) == "SELECT 1"


def test_split_sql_script_preserves_statement_output() -> None:
    """Statement splitting should preserve existing semicolon handling."""
    assert split_sql_script("SELECT 1; SELECT 2;", strip_trailing_terminator=True) == ["SELECT 1", "SELECT 2"]
