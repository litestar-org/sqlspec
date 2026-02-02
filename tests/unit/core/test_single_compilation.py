"""Tests to verify compile() is called exactly once per statement execution.

These tests use mock.patch to count compile() invocations and ensure
the optimization is maintained - any regression will cause test failure.
"""

import sqlite3
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
        yield spec, config


class TestSingleCompilation:
    """Tests ensuring compile() is called only once per statement execution."""

    def test_compile_called_once_per_execute(self, sqlite_spec):
        """Compile should be called exactly once per statement execution."""
        spec, config = sqlite_spec
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

    def test_compile_called_once_with_parameters(self, sqlite_spec):
        """Compile should be called once even with parameters."""
        spec, config = sqlite_spec
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

    def test_compile_called_once_for_insert(self, sqlite_spec):
        """INSERT statements should compile exactly once."""
        spec, config = sqlite_spec
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

    def test_multiple_executes_compile_once_each(self, sqlite_spec):
        """Each execute should compile exactly once, not share compilations."""
        spec, config = sqlite_spec
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

        # 3 executes = 3 compiles (one per statement)
        assert call_count == 3, f"compile() called {call_count} times, expected 3"

    def test_compile_called_once_with_named_parameters(self, sqlite_spec):
        """Compile should be called once with named parameters."""
        spec, config = sqlite_spec
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

    def test_compile_called_once_for_execute_many(self, sqlite_spec):
        """execute_many should compile exactly once for the batch."""
        spec, config = sqlite_spec
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

        # execute_many should compile once, not once per row
        assert call_count == 1, f"compile() called {call_count} times, expected 1"


class TestPerformanceOverhead:
    """Tests to verify performance overhead is within acceptable bounds."""

    @pytest.mark.parametrize("run", range(3))  # Run 3 times, take best
    def test_performance_overhead_acceptable(self, run):
        """SQLSpec overhead should be reasonable compared to raw sqlite3.

        SQLSpec adds features like parameter validation, SQL parsing,
        observability, and caching - some overhead is expected.
        Target: <60x overhead (down from ~92x before optimizations).

        Uses multiple rows to amortize per-call overhead and get stable timing.
        """
        import time

        ROWS = 2000  # More rows for stable timing

        with tempfile.TemporaryDirectory() as d:
            # Raw sqlite3
            conn = sqlite3.connect(f"{d}/raw.db")
            conn.execute("CREATE TABLE t (id INT)")
            start = time.perf_counter()
            for i in range(ROWS):
                conn.execute("INSERT INTO t VALUES (?)", (i,))
            raw_time = time.perf_counter() - start
            conn.close()

            # SQLSpec
            spec = SQLSpec()
            config = spec.add_config(SqliteConfig(connection_config={"database": f"{d}/spec.db"}))
            with spec.provide_session(config) as session:
                session.execute("CREATE TABLE t (id INT)")
                start = time.perf_counter()
                for i in range(ROWS):
                    session.execute("INSERT INTO t VALUES (?)", (i,))
                spec_time = time.perf_counter() - start

            overhead = spec_time / raw_time
            # Target <60x overhead (improved from ~92x before optimizations)
            # Single-statement execution has inherent abstraction overhead
            # This is a regression guard - if overhead increases significantly, investigate
            assert overhead < 60, f"Overhead {overhead:.1f}x exceeds 60x target (raw={raw_time:.4f}s, spec={spec_time:.4f}s)"
