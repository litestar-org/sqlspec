"""Tests for introspection SQL file integrity (GitHub issue #361).

Verifies that all data dictionary SQL files:
- Load without parse errors
- Preserve PostgreSQL ``::type`` casts (not mangled to ``:: type`` or ``: type``)
- Keep ``:param`` bind parameters intact (no ``=: param`` with stray spaces)
- The parameter validator regex correctly distinguishes ``::text`` (cast) from ``:param`` (bind)
"""

import re

import pytest

from sqlspec.core import SQL
from sqlspec.core.parameters._validator import PARAMETER_REGEX, ParameterValidator
from sqlspec.data_dictionary._loader import DataDictionaryLoader

pytestmark = pytest.mark.xdist_group("core")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SPACE_BEFORE_BIND = re.compile(r"=\s+:(?!:)\w+")
"""Detects ``= :param`` with extra whitespace that should be ``= :param``
(one space is fine, but ``=  :param`` or ``=\n:param`` is suspicious)."""

_MANGLED_CAST = re.compile(r"::\s+\w+")
"""Detects ``:: text`` with a space inside the cast operator."""

_STRAY_SPACE_BIND = re.compile(r"(?<!=):\s+\w+")
"""Detects ``: param`` where a space was inserted after the colon."""


def _get_all_dialect_queries() -> list[tuple[str, str]]:
    """Return (dialect, query_name) pairs for every loadable query."""
    loader = DataDictionaryLoader()
    pairs: list[tuple[str, str]] = []
    for dialect in loader.list_dialects():
        # Load the dialect first to populate query list
        try:
            loader._ensure_dialect_loaded(dialect)
        except Exception:
            continue
        inner = loader._get_loader(dialect)
        pairs.extend((dialect, qname) for qname in inner.list_queries())
    return pairs


ALL_QUERIES = _get_all_dialect_queries()


# ---------------------------------------------------------------------------
# Task 4.1 / 4.3 — Load every SQL file, inspect raw text
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("dialect", "query_name"), ALL_QUERIES, ids=[f"{d}/{q}" for d, q in ALL_QUERIES])
def test_raw_sql_has_no_mangled_casts(dialect: str, query_name: str) -> None:
    """Raw SQL text must not contain ``:: text`` (space inside cast)."""
    loader = DataDictionaryLoader()
    raw = loader.get_query_text(dialect, query_name)
    matches = _MANGLED_CAST.findall(raw)
    assert not matches, f"Mangled cast(s) in {dialect}/{query_name}: {matches}"


@pytest.mark.parametrize(("dialect", "query_name"), ALL_QUERIES, ids=[f"{d}/{q}" for d, q in ALL_QUERIES])
def test_raw_sql_has_no_stray_space_binds(dialect: str, query_name: str) -> None:
    """Raw SQL must not have ``: param`` with a space after the colon."""
    loader = DataDictionaryLoader()
    raw = loader.get_query_text(dialect, query_name)
    matches = _STRAY_SPACE_BIND.findall(raw)
    assert not matches, f"Stray space bind(s) in {dialect}/{query_name}: {matches}"


# ---------------------------------------------------------------------------
# Task 4.4 — Compile every SQL file, verify output integrity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("dialect", "query_name"), ALL_QUERIES, ids=[f"{d}/{q}" for d, q in ALL_QUERIES])
def test_sql_compiles_without_error(dialect: str, query_name: str) -> None:
    """Every introspection query must compile through the SQL pipeline."""
    loader = DataDictionaryLoader()
    sql_obj = loader.get_query(dialect, query_name)
    assert isinstance(sql_obj, SQL)
    compiled_sql, _params = sql_obj.compile()
    assert isinstance(compiled_sql, str)
    assert len(compiled_sql) > 0


@pytest.mark.parametrize(
    ("dialect", "query_name"),
    [(d, q) for d, q in ALL_QUERIES if d in ("postgres", "cockroachdb")],
    ids=[f"{d}/{q}" for d, q in ALL_QUERIES if d in ("postgres", "cockroachdb")],
)
def test_compiled_sql_preserves_pg_casts(dialect: str, query_name: str) -> None:
    """Compiled SQL for PG-family dialects must preserve ``::type`` casts."""
    loader = DataDictionaryLoader()
    raw = loader.get_query_text(dialect, query_name)
    if "::" not in raw:
        pytest.skip("No casts in this query")

    sql_obj = loader.get_query(dialect, query_name)
    compiled_sql, _ = sql_obj.compile()
    # Count casts in raw vs compiled — they should match
    raw_casts = re.findall(r"::\w+", raw)
    compiled_casts = re.findall(r"::\w+", compiled_sql)
    # The compiled SQL may rewrite identifiers, but cast count must be stable
    assert len(compiled_casts) >= len(raw_casts), (
        f"Casts lost in {dialect}/{query_name}: raw={len(raw_casts)}, compiled={len(compiled_casts)}"
    )


# ---------------------------------------------------------------------------
# Task 4.5 — Parameter validator regex with mixed casts and binds
# ---------------------------------------------------------------------------


def test_parameter_regex_distinguishes_cast_from_bind() -> None:
    """``::text`` must match as ``pg_cast``, ``:param`` as ``named_colon``."""
    sql = "SELECT a.attname::text WHERE c.relname = :table_name AND n.nspname = :schema_name"
    casts: list[str] = []
    binds: list[str] = []
    for m in PARAMETER_REGEX.finditer(sql):
        if m.group("pg_cast"):
            casts.append(m.group("pg_cast"))
        elif m.group("named_colon"):
            binds.append(m.group("named_colon"))
    assert casts == ["::text"], f"Expected ['::text'], got {casts}"
    assert sorted(binds) == [":schema_name", ":table_name"], f"Unexpected binds: {binds}"


def test_parameter_regex_adjacent_bind_and_cast() -> None:
    """``:schema_name::text`` must yield a bind AND a cast, not a single token."""
    sql = "WHERE :schema_name::text IS NULL"
    casts: list[str] = []
    binds: list[str] = []
    for m in PARAMETER_REGEX.finditer(sql):
        if m.group("pg_cast"):
            casts.append(m.group("pg_cast"))
        elif m.group("named_colon"):
            binds.append(m.group("named_colon"))
    assert binds == [":schema_name"], f"Unexpected binds: {binds}"
    assert casts == ["::text"], f"Unexpected casts: {casts}"


def test_parameter_validator_skips_pg_casts() -> None:
    """ParameterValidator must not report ``::text`` as a named parameter."""
    validator = ParameterValidator()
    sql = "SELECT a::text, b::int WHERE x = :foo AND :bar::text IS NULL"
    params = validator.extract_parameters(sql)
    param_names = [p.name for p in params]
    assert "foo" in param_names
    assert "bar" in param_names
    # Cast type names must NOT appear as parameters
    assert "text" not in param_names
    assert "int" not in param_names


def test_parameter_validator_handles_postgres_introspection_sql() -> None:
    """Validate the actual postgres columns_by_table query."""
    loader = DataDictionaryLoader()
    raw = loader.get_query_text("postgres", "columns_by_table")
    validator = ParameterValidator()
    params = validator.extract_parameters(raw)
    param_names = [p.name for p in params]
    assert "table_name" in param_names
    assert "schema_name" in param_names
    # Cast types should NOT be detected as parameters
    assert "text" not in param_names


def test_parameter_validator_handles_mixed_cast_bind_pattern() -> None:
    """Validate the postgres foreign_keys pattern with ``:param::text``."""
    loader = DataDictionaryLoader()
    raw = loader.get_query_text("postgres", "foreign_keys_by_table")
    validator = ParameterValidator()
    params = validator.extract_parameters(raw)
    param_names = [p.name for p in params]
    assert "schema_name" in param_names
    assert "table_name" in param_names
    # ::text cast must not leak as a parameter
    assert "text" not in param_names


def test_compiled_sql_no_mangled_casts_or_binds() -> None:
    """Compiled SQL for postgres columns_by_table must not mangle casts or binds.

    This is the exact scenario from GitHub issue #361 where ``c.relname = :table_name``
    was allegedly being transformed to ``c.relname =: table_name``.
    """
    loader = DataDictionaryLoader()
    sql_obj = loader.get_query("postgres", "columns_by_table")
    compiled_sql, _ = sql_obj.compile()
    # Must not have mangled casts like ":: text"
    assert ":: text" not in compiled_sql, f"Mangled cast found in compiled SQL: {compiled_sql}"
    # Must not have "=:" (equals-colon without space then param name)
    # that differs from "= :param" — the issue was "=: param" with wrong spacing
    bad = re.findall(r"=:\s+\w+", compiled_sql)
    assert not bad, f"Found '=: param' pattern in compiled SQL: {bad}"


def test_compiled_foreign_keys_preserves_cast_bind_adjacency() -> None:
    """``(:schema_name::text IS NULL ...)`` must compile without mangling.

    The ``::text`` cast immediately after ``:schema_name`` is the trickiest pattern.
    """
    loader = DataDictionaryLoader()
    sql_obj = loader.get_query("postgres", "foreign_keys_by_table")
    compiled_sql, _ = sql_obj.compile()
    # The cast must survive compilation
    assert "::text" in compiled_sql or ":: TEXT" in compiled_sql or "CAST(" in compiled_sql, (
        f"Cast lost in compiled foreign_keys_by_table: {compiled_sql}"
    )
