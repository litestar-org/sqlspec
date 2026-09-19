"""Unit tests for public table-queue primitives."""

import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

from sqlspec.extensions.events.primitives import claim_verified, lock_clause, row_limit_clause, select_limit_prefix


@pytest.mark.parametrize(
    ("select_for_update", "skip_locked", "expected"),
    [(False, False, ""), (False, True, ""), (True, False, " FOR UPDATE"), (True, True, " FOR UPDATE SKIP LOCKED")],
)
def test_lock_clause_matrix(select_for_update: bool, skip_locked: bool, expected: str) -> None:
    """Test lock_clause truth table across locking flags."""
    assert lock_clause(select_for_update=select_for_update, skip_locked=skip_locked) == expected


@pytest.mark.parametrize(
    ("dialect", "n", "expected"),
    [
        ("postgres", 1, " LIMIT 1"),
        ("postgresql", 5, " LIMIT 5"),
        ("mysql", 1, " LIMIT 1"),
        ("mysql", 10, " LIMIT 10"),
        ("oracle", 1, " FETCH FIRST 1 ROWS ONLY"),
        ("oracle", 5, " FETCH FIRST 5 ROWS ONLY"),
        ("tsql", 1, ""),
        ("tsql", 5, ""),
        ("mssql", 1, ""),
        ("sql server", 1, ""),
        ("sqlite", 1, " LIMIT 1"),
        ("sqlite", 3, " LIMIT 3"),
        ("duckdb", 1, " LIMIT 1"),
        ("duckdb", 25, " LIMIT 25"),
    ],
)
def test_row_limit_clause_matrix(dialect: str, n: int, expected: str) -> None:
    """Test row_limit_clause across supported dialects and limit counts."""
    assert row_limit_clause(dialect, n) == expected


@pytest.mark.parametrize(
    ("dialect", "n", "expected"),
    [
        ("postgres", 1, ""),
        ("postgresql", 5, ""),
        ("mysql", 1, ""),
        ("oracle", 1, ""),
        ("tsql", 1, "TOP 1 "),
        ("tsql", 5, "TOP 5 "),
        ("mssql", 1, "TOP 1 "),
        ("mssql", 10, "TOP 10 "),
        ("sql server", 1, "TOP 1 "),
        ("sqlite", 1, ""),
        ("duckdb", 1, ""),
    ],
)
def test_select_limit_prefix_matrix(dialect: str, n: int, expected: str) -> None:
    """Test select_limit_prefix across supported dialects and limit counts."""
    assert select_limit_prefix(dialect, n) == expected


def test_claim_verified_truth_table() -> None:
    """Test claim_verified across row shapes and lease timestamps."""
    leased_until = datetime(2026, 9, 13, 16, 0, 0, tzinfo=timezone.utc)
    earlier = leased_until - timedelta(seconds=10)
    later = leased_until + timedelta(seconds=10)

    assert claim_verified(None, leased_until) is False
    assert claim_verified({}, leased_until) is False
    assert claim_verified({"lease_expires_at": None}, leased_until) is False
    assert claim_verified({"lease_expires_at": earlier}, leased_until) is False
    assert claim_verified({"lease_expires_at": later}, leased_until) is False
    assert claim_verified({"lease_expires_at": leased_until}, leased_until) is True
    assert claim_verified({"lease_expires_at": leased_until.isoformat()}, leased_until) is True
    assert claim_verified({"event_id": "e1", "lease_expires_at": earlier.isoformat()}, leased_until) is False
    assert claim_verified({"event_id": "e1", "lease_expires_at": leased_until.isoformat()}, leased_until) is True


def test_events_package_exports() -> None:
    """Test primitives exported from sqlspec.extensions.events."""
    import sqlspec.extensions.events as events

    assert events.claim_verified is claim_verified
    assert events.lock_clause is lock_clause
    assert events.row_limit_clause is row_limit_clause
    assert events.select_limit_prefix is select_limit_prefix
    assert "claim_verified" in events.__all__
    assert "lock_clause" in events.__all__
    assert "row_limit_clause" in events.__all__
    assert "select_limit_prefix" in events.__all__


def test_primitives_module_attribute_before_function_access() -> None:
    """The conventional module attribute resolves without an earlier child import."""
    script = """
import importlib
import sqlspec.extensions.events as events
module = events.primitives
assert module is importlib.import_module("sqlspec.extensions.events.primitives")
assert module.lock_clause(select_for_update=True, skip_locked=True) == " FOR UPDATE SKIP LOCKED"
assert "primitives" in dir(events)
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, text=True)
