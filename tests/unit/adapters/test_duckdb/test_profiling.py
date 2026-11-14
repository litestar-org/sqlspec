"""Unit tests for DuckDB JSON statement profiling and per-statement script events."""

import stat
from pathlib import Path

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.core import cleanup_duckdb_profile_path, create_duckdb_profile_path
from sqlspec.observability import ObservabilityConfig, StatementEvent


def test_duckdb_enable_statement_profiling_attaches_json_profile_to_statement_event(tmp_path: Path) -> None:
    """enable_statement_profiling=True attaches parsed JSON operator trees to StatementEvent.profile."""
    events: list[StatementEvent] = []
    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "profiling.duckdb")},
        driver_features={"enable_statement_profiling": True},
        observability_config=ObservabilityConfig(statement_observers=(events.append,)),
    )
    try:
        with config.provide_session() as session:
            result = session.execute("SELECT sum(i) AS total FROM range(10) t(i)")
            assert result.one()["total"] == 45
            session.execute("SELECT ?::INTEGER AS val", (1,))
            session.execute("SELECT ?::INTEGER AS val", (2,))
    finally:
        config.close_pool()

    assert len(events) == 3
    for event in events:
        assert "enable_profiling" not in event.sql.lower()
        assert "profiling_output" not in event.sql.lower()
        assert isinstance(event.profile, dict)
        assert "cpu_time" in event.profile or ("query" in event.profile and "cpu_time" in event.profile["query"])
        assert "children" in event.profile or "operator" in event.profile


def test_duckdb_execute_script_emits_per_statement_events_with_ordinals_and_profiles(tmp_path: Path) -> None:
    """execute_script emits per-statement StatementEvents with ordinals, counts, and profiles before the outer event."""
    events: list[StatementEvent] = []
    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "script_profiling.duckdb")},
        driver_features={"enable_statement_profiling": True},
        observability_config=ObservabilityConfig(statement_observers=(events.append,)),
    )
    script = "CREATE TABLE t (x INT); INSERT INTO t VALUES (1), (2); SELECT sum(x) FROM t;"
    try:
        with config.provide_session() as session:
            result = session.execute_script(script)
            assert result.total_statements == 3
            assert result.successful_statements == 3
    finally:
        config.close_pool()

    assert len(events) == 4
    sub_events = events[:3]
    outer_event = events[3]

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


def test_duckdb_profiling_scratch_file_cleaned_up_on_close_pool(tmp_path: Path) -> None:
    """Temporary JSON profile files created by DuckDBConnectionPool are unlinked on close_pool."""
    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "cleanup.duckdb")},
        driver_features={"enable_statement_profiling": True},
    )
    profile_path: Path | None = None
    try:
        with config.provide_session() as session:
            session.execute("SELECT 1")
            raw_path = session.driver_features.get("_duckdb_profile_path")
            assert raw_path is not None
            profile_path = Path(raw_path)
            assert profile_path.exists()
    finally:
        config.close_pool()

    assert profile_path is not None
    assert not profile_path.exists()


def test_duckdb_profile_path_uses_private_permissions(tmp_path: Path) -> None:
    """Profile temp files are created with 0o600 permissions before and after queries."""
    standalone = create_duckdb_profile_path()
    try:
        assert stat.S_IMODE(standalone.stat().st_mode) == 0o600
    finally:
        cleanup_duckdb_profile_path(standalone)

    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "perm.duckdb")},
        driver_features={"enable_statement_profiling": True},
    )
    try:
        with config.provide_session() as session:
            session.execute("SELECT 1")
            raw_path = session.driver_features.get("_duckdb_profile_path")
            assert raw_path is not None
            assert stat.S_IMODE(Path(raw_path).stat().st_mode) == 0o600
    finally:
        config.close_pool()


def test_duckdb_secret_creation_excludes_credentials_from_profile_json(tmp_path: Path) -> None:
    """Secret DDL and refreshes disable profiling so credential values never enter profile JSON."""
    secret_value = "TOP_SECRET_CREDENTIAL_98765"
    refresh_calls = 0

    def next_credentials() -> dict[str, str]:
        nonlocal refresh_calls
        refresh_calls += 1
        return {"key_id": f"KID_{refresh_calls}", "secret": secret_value}

    events: list[StatementEvent] = []
    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "secret_prof.duckdb")},
        driver_features={
            "enable_statement_profiling": True,
            "secrets": [
                {
                    "name": "dyn_s3_secret",
                    "secret_type": "s3",
                    "required": True,
                    "refresh_interval_seconds": 0.0,
                    "value_provider": next_credentials,
                }
            ],
        },
        observability_config=ObservabilityConfig(statement_observers=(events.append,)),
    )
    try:
        with config.provide_session() as session:
            session.execute("SELECT 1 AS first_query")
            raw_path = session.driver_features.get("_duckdb_profile_path")
            assert raw_path is not None
            assert secret_value not in Path(raw_path).read_text(encoding="utf-8")

        with config.provide_session() as session:
            session.execute("SELECT 2 AS second_query")
            raw_path = session.driver_features.get("_duckdb_profile_path")
            assert raw_path is not None
            assert secret_value not in Path(raw_path).read_text(encoding="utf-8")
    finally:
        config.close_pool()

    assert refresh_calls >= 2
    assert len(events) == 2
    assert all(isinstance(event.profile, dict) for event in events)
    assert all(secret_value not in repr(event.profile) for event in events)
