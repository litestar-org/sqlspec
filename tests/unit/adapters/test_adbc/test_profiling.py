"""Unit tests for ADBC DuckDB JSON statement profiling and per-statement script events."""

import subprocess
import sys
import tempfile
from importlib import import_module
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.observability import ObservabilityConfig, StatementEvent


@pytest.mark.parametrize("stage", ["path", "configuration"])
@pytest.mark.parametrize("borrowed", [False, True])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_adbc_profiling_setup_failure_releases_owned_connection(
    monkeypatch: pytest.MonkeyPatch, stage: str, borrowed: bool, cleanup_fails: bool
) -> None:
    """Profiling failures preserve setup errors and close only owned connections."""
    connection = MagicMock()
    primary = RuntimeError("profile setup failed")
    removed: list[Path] = []
    original_unlink = Path.unlink

    def remove_profile(path: Path, missing_ok: bool = False) -> None:
        if path.name.startswith("sqlspec_duckdb_profile_"):
            removed.append(path)
            if cleanup_fails:
                raise KeyboardInterrupt("profile cleanup interrupted")
        else:
            original_unlink(path, missing_ok=missing_ok)

    def temporary_directory() -> str:
        raise primary

    monkeypatch.setattr(import_module("adbc_driver_duckdb.dbapi"), "connect", lambda **_kwargs: connection)
    monkeypatch.setattr(Path, "unlink", remove_profile)
    if stage == "path":
        monkeypatch.setattr(tempfile, "gettempdir", temporary_directory)
    else:
        connection.cursor.return_value.execute.side_effect = primary
    if cleanup_fails:
        connection.close.side_effect = SystemExit("connection cleanup interrupted")
    config = AdbcConfig(
        connection_config={"driver_name": "duckdb"},
        connection_instance=connection if borrowed else None,
        driver_features={"enable_statement_profiling": True},
    )
    context = config.provide_session()
    with pytest.raises(RuntimeError) as caught:
        context.__enter__()
    assert caught.value is primary
    context.__exit__(None, None, None)
    if borrowed:
        connection.close.assert_not_called()
    else:
        connection.close.assert_called_once_with()
    if stage == "configuration":
        assert len(removed) == 1
        connection.cursor.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("setup_fails", [False, True])
def test_adbc_profiling_cleanup_attempts_both_paths_and_release_once(
    monkeypatch: pytest.MonkeyPatch, setup_fails: bool
) -> None:
    """A profile cleanup failure clears ownership and still attempts both path cleanups."""
    connection = MagicMock()
    primary = KeyboardInterrupt("metadata interrupted")
    cleanup_error = RuntimeError("profile cleanup failed")
    removed: list[Path] = []
    original_unlink = Path.unlink

    def remove_profile(path: Path, missing_ok: bool = False) -> None:
        if path.name.startswith("sqlspec_duckdb_profile_"):
            removed.append(path)
            raise cleanup_error
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(import_module("adbc_driver_duckdb.dbapi"), "connect", lambda **_kwargs: connection)
    monkeypatch.setattr(Path, "unlink", remove_profile)
    if setup_fails:
        connection.adbc_get_info.side_effect = primary
    config = AdbcConfig(
        connection_config={"driver_name": "duckdb"}, driver_features={"enable_statement_profiling": True}
    )
    context = config.provide_session()
    if setup_fails:
        with pytest.raises(KeyboardInterrupt) as setup_caught:
            context.__enter__()
        assert setup_caught.value is primary
    else:
        context.__enter__()
        with pytest.raises(RuntimeError) as cleanup_caught:
            context.__exit__(None, None, None)
        assert cleanup_caught.value is cleanup_error
    assert len(removed) == 2
    assert removed[0] == removed[1]
    context.__exit__(None, None, None)
    assert len(removed) == 2
    connection.close.assert_called_once_with()


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


def test_adbc_duckdb_profiles_remain_scoped_after_execution_error() -> None:
    """Each successful statement consumes its own profile without leaking a failed query's state."""
    events: list[StatementEvent] = []
    config = AdbcConfig(
        connection_config={"driver_name": "duckdb"},
        driver_features={"enable_statement_profiling": True},
        observability_config=ObservabilityConfig(statement_observers=(events.append,)),
    )
    with config.provide_session() as session:
        assert session.select_value("SELECT 1 AS value") == 1
        with pytest.raises(SQLSpecError):
            session.execute("SELECT missing_column FROM missing_table")
        session.rollback()
        assert session.select_value("SELECT 2 AS value") == 2
    assert [event.sql for event in events] == ["SELECT 1 AS value", "SELECT 2 AS value"]
    assert all(isinstance(event.profile, dict) for event in events)


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
