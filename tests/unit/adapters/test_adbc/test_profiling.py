"""Unit tests for ADBC DuckDB JSON statement profiling and per-statement script events."""

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
    assert "cpu_time" in first_event.profile
    assert "children" in first_event.profile

    sub_events = events[1:4]
    outer_event = events[4]
    assert [e.script_ordinal for e in sub_events] == [0, 1, 2]
    assert [e.script_statement_index for e in sub_events] == [0, 1, 2]
    assert [e.script_statement_count for e in sub_events] == [3, 3, 3]
    assert all(e.is_script for e in sub_events)
    for sub_event in sub_events:
        assert isinstance(sub_event.profile, dict)
        assert "cpu_time" in sub_event.profile
        assert "children" in sub_event.profile

    assert outer_event.operation == "SCRIPT"
    assert outer_event.is_script is True
    assert outer_event.script_ordinal is None
