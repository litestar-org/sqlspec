"""Unit tests for ADBC DuckDB JSON statement profiling and per-statement script events."""

import subprocess
import sys
from pathlib import Path

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.observability import ObservabilityConfig, StatementEvent


def test_adbc_duckdb_enable_statement_profiling_and_execute_script_events() -> None:
    """AdbcConfig with driver_name='duckdb' and enable_statement_profiling=True populates StatementEvent.profile and emits per-statement script events."""
    events: list[StatementEvent] = []
    config = AdbcConfig(
        connection_config={"driver_name": "duckdb"},
        driver_features={"enable_statement_profiling": True},
        observability_config=ObservabilityConfig(statement_observers=(events.append,)),
    )
    profile_path: Path | None = None
    script = "CREATE TABLE t (x INT); INSERT INTO t VALUES (1), (2); SELECT sum(x) FROM t;"

    with config.provide_session() as session:
        result = session.execute("SELECT sum(i) AS total FROM range(10) t(i)")
        assert result.one()["total"] == 45

        raw_path = session.driver_features.get("_duckdb_profile_path")
        assert isinstance(raw_path, str)
        profile_path = Path(raw_path)
        assert profile_path.exists()

        script_result = session.execute_script(script)
        assert script_result.total_statements == 3
        assert script_result.successful_statements == 3

    assert profile_path is not None
    assert not profile_path.exists()

    assert len(events) == 5
    first_event = events[0]
    assert "enable_profiling" not in first_event.sql.lower()
    assert "profiling_output" not in first_event.sql.lower()
    assert isinstance(first_event.profile, dict)
    assert "cpu_time" in first_event.profile or (
        "query" in first_event.profile and "cpu_time" in first_event.profile["query"]
    )
    assert "children" in first_event.profile or "operator" in first_event.profile

    sub_events = events[1:4]
    outer_event = events[4]
    assert [e.script_ordinal for e in sub_events] == [0, 1, 2]
    assert [e.script_statement_index for e in sub_events] == [0, 1, 2]
    assert [e.script_statement_count for e in sub_events] == [3, 3, 3]
    assert all(e.is_script for e in sub_events)
    for sub_event in sub_events:
        assert isinstance(sub_event.profile, dict)
        assert "cpu_time" in sub_event.profile or (
            "query" in sub_event.profile and "cpu_time" in sub_event.profile["query"]
        )
        assert "children" in sub_event.profile or "operator" in sub_event.profile

    assert outer_event.operation == "SCRIPT"
    assert outer_event.is_script is True
    assert outer_event.script_ordinal is None


def test_adbc_duckdb_statement_profiling_without_duckdb_package() -> None:
    """ADBC statement profiling with DuckDB backend works without the duckdb Python library."""
    script = """
import importlib.abc
import sys

class BlockDuckDB(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "duckdb" or fullname.startswith("duckdb."):
            raise ModuleNotFoundError("duckdb blocked", name=fullname)

sys.meta_path.insert(0, BlockDuckDB())

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.observability import ObservabilityConfig, StatementEvent

events: list[StatementEvent] = []
config = AdbcConfig(
    connection_config={"driver_name": "duckdb"},
    driver_features={"enable_statement_profiling": True},
    observability_config=ObservabilityConfig(statement_observers=(events.append,)),
)

with config.provide_session() as session:
    result = session.execute("SELECT 42 AS val")
    assert result.one()["val"] == 42

assert len(events) == 1
event = events[0]
assert isinstance(event.profile, dict)
assert "cpu_time" in event.profile
assert "children" in event.profile
assert "duckdb" not in sys.modules
assert "sqlspec.adapters.duckdb" not in sys.modules
"""
    subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
