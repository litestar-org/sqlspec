"""Tests to verify compile() is called exactly once per statement execution.

These tests use mock.patch to count compile() invocations and ensure
the optimization is maintained - any regression will cause test failure.
"""

import tempfile
from unittest.mock import patch

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core.statement import SQL


@pytest.fixture
def sqlite_spec():
    """Create SQLSpec with SQLite for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        spec = SQLSpec()
        config = spec.add_config(SqliteConfig(connection_config={"database": f"{tmpdir}/test.db"}))
        yield (spec, config)


def test_single_compilation_compile_called_once_per_execute(sqlite_spec):
    """Compile should be called exactly once per statement execution."""
    (spec, config) = sqlite_spec
    original_compile = SQL.compile
    call_count = 0

    def counting_compile(self):
        nonlocal call_count
        call_count += 1
        return original_compile(self)

    with spec.provide_session(config) as session:
        with patch.object(SQL, "compile", counting_compile):
            session.execute("SELECT 1")
    assert call_count == 1, f"compile() called {call_count} times, expected 1"


def test_single_compilation_compile_called_once_with_parameters(sqlite_spec):
    """Compile should be called once even with parameters."""
    (spec, config) = sqlite_spec
    original_compile = SQL.compile
    call_count = 0

    def counting_compile(self):
        nonlocal call_count
        call_count += 1
        return original_compile(self)

    with spec.provide_session(config) as session:
        with patch.object(SQL, "compile", counting_compile):
            session.execute("SELECT ?", (1,))
    assert call_count == 1, f"compile() called {call_count} times, expected 1"


def test_single_compilation_compile_called_once_for_insert(sqlite_spec):
    """INSERT statements should compile exactly once."""
    (spec, config) = sqlite_spec
    original_compile = SQL.compile
    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE test (id INTEGER)")
        call_count = 0

        def counting_compile(self):
            nonlocal call_count
            call_count += 1
            return original_compile(self)

        with patch.object(SQL, "compile", counting_compile):
            session.execute("INSERT INTO test (id) VALUES (?)", (1,))
    assert call_count == 1, f"compile() called {call_count} times, expected 1"


def test_single_compilation_multiple_executes_compile_once_each(sqlite_spec):
    """Each execute should compile exactly once, not share compilations."""
    (spec, config) = sqlite_spec
    original_compile = SQL.compile
    call_count = 0

    def counting_compile(self):
        nonlocal call_count
        call_count += 1
        return original_compile(self)

    with spec.provide_session(config) as session:
        with patch.object(SQL, "compile", counting_compile):
            session.execute("SELECT 1")
            session.execute("SELECT 2")
            session.execute("SELECT 3")
    assert call_count == 3, f"compile() called {call_count} times, expected 3"


def test_single_compilation_compile_called_once_with_named_parameters(sqlite_spec):
    """Compile should be called once with named parameters."""
    (spec, config) = sqlite_spec
    original_compile = SQL.compile
    call_count = 0

    def counting_compile(self):
        nonlocal call_count
        call_count += 1
        return original_compile(self)

    with spec.provide_session(config) as session:
        with patch.object(SQL, "compile", counting_compile):
            session.execute("SELECT :value", {"value": 42})
    assert call_count == 1, f"compile() called {call_count} times, expected 1"


def test_single_compilation_compile_called_once_for_execute_many(sqlite_spec):
    """execute_many should compile at most once for the batch.

    SQLite's thin path optimization bypasses compile() entirely for simple
    qmark batches, so compile may be called 0 times. The key guarantee is
    that it is never called more than once (i.e., not once per row).
    """
    (spec, config) = sqlite_spec
    original_compile = SQL.compile
    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE batch_test (id INTEGER)")
        call_count = 0

        def counting_compile(self):
            nonlocal call_count
            call_count += 1
            return original_compile(self)

        with patch.object(SQL, "compile", counting_compile):
            session.execute_many("INSERT INTO batch_test (id) VALUES (?)", [(1,), (2,), (3,)])
    assert call_count <= 1, f"compile() called {call_count} times, expected at most 1"
