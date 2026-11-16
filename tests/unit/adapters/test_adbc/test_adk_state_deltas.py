"""Unit tests for ADBC ADK scoped state deltas, locking/CAS, and newest-N get_events."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.adbc.adk import AdbcADKStore
from sqlspec.adapters.adbc.adk.store import DIALECT_DB2, DIALECT_MSSQL, DIALECT_ORACLE, DIALECT_POSTGRESQL
from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.exceptions import SerializationConflictError


def test_adbc_sqlite_create_session_and_falsy_deltas(tmp_path: Path) -> None:
    """SQLite ADBC persists session-only state, atomically merges falsy/None deltas, and reads scoped state."""
    db_file = tmp_path / "adk_sqlite.db"
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite", "uri": str(db_file)})
    store = AdbcADKStore(config)
    store.create_tables()

    created = store.create_session(
        "s1",
        "app",
        "u1",
        {"session_only": "initial"},
        app_state_delta={"app:flag": True, "app:keep": "stay", "app:drop": "before"},
        user_state_delta={"user:count": 5, "user:note": "before"},
    )
    assert created["state"] == {
        "session_only": "initial",
        "app:flag": True,
        "app:keep": "stay",
        "app:drop": "before",
        "user:count": 5,
        "user:note": "before",
    }

    with pytest.raises(TypeError):
        cast("Any", store).append_event_and_update_state(
            cast("Any", _make_event("bad")), "app", "u1", "s1", {}, app_state={"a": 1}
        )

    updated = store.append_event_and_update_state(
        cast("Any", _make_event("e1", offset_seconds=1)),
        "app",
        "u1",
        "s1",
        {"session_only": "updated"},
        app_state_delta={"app:flag": False, "app:drop": None, "app:empty": ""},
        user_state_delta={"user:count": 0, "user:note": None},
    )
    assert updated["state"] == {
        "session_only": "updated",
        "app:flag": False,
        "app:keep": "stay",
        "app:drop": None,
        "app:empty": "",
        "user:count": 0,
        "user:note": None,
    }
    assert store.get_app_state("app") == {"app:flag": False, "app:keep": "stay", "app:drop": None, "app:empty": ""}
    assert store.get_user_state("app", "u1") == {"user:count": 0, "user:note": None}

    raw_session = store.get_session("app", "u1", "s1")
    assert raw_session is not None
    assert raw_session["state"] == {"session_only": "updated"}

    created_no_delta = store.create_session("s2", "app", "u1", {"s2_key": 1}, app_state_delta={}, user_state_delta=None)
    assert created_no_delta["state"] == {
        "s2_key": 1,
        "app:flag": False,
        "app:keep": "stay",
        "app:drop": None,
        "app:empty": "",
        "user:count": 0,
        "user:note": None,
    }


@pytest.mark.parametrize("driver_name", ["adbc_driver_sqlite", "adbc_driver_duckdb"])
def test_adbc_real_concurrent_scoped_state_merges_and_newest_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, driver_name: str
) -> None:
    """Real ADBC SQLite and DuckDB stores preserve concurrent deltas and return newest-N events ascending."""
    from contextlib import contextmanager

    db_file = tmp_path / f"concurrent_{driver_name}.db"
    if driver_name == "adbc_driver_sqlite":
        config = AdbcConfig(connection_config={"driver_name": driver_name, "uri": str(db_file)})
        seed_conn = None
    else:
        adbc_duckdb = pytest.importorskip("adbc_driver_duckdb.dbapi")
        seed_conn = adbc_duckdb.connect(str(db_file))
        config = AdbcConfig(connection_config={"driver_name": driver_name, "path": str(db_file)})

        @contextmanager
        def provide_cloned_connection(_: AdbcConfig) -> Any:
            cloned = seed_conn.adbc_clone()
            try:
                yield cloned
            finally:
                cloned.close()

        monkeypatch.setattr(AdbcConfig, "provide_connection", provide_cloned_connection)

    try:
        store = AdbcADKStore(config)
        store.create_tables()
        store.create_session("s0", "app", "u1", {"worker": 0})
        store.create_session("s1", "app", "u1", {"worker": 1})
        store.create_session("s2", "app", "u1", {"worker": 2})
        store.create_session("s3", "app", "u1", {"worker": 3})
        store.create_session("s4", "app", "u1", {"worker": 4})
        store.create_session("s5", "app", "u1", {"worker": 5})
        store.create_session("s6", "app", "u1", {"worker": 6})
        store.create_session("s7", "app", "u1", {"worker": 7})

        def worker(idx: int) -> None:
            event = _make_event(f"evt-{idx}", session_id=f"s{idx}", offset_seconds=idx)
            store.append_event_and_update_state(
                cast("Any", event),
                "app",
                "u1",
                f"s{idx}",
                {"last": idx},
                app_state_delta={f"app:k{idx}": False if idx == 0 else idx},
                user_state_delta={f"user:k{idx}": None if idx == 1 else idx * 10},
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(worker, range(8)))

        expected_app = {f"app:k{idx}": False if idx == 0 else idx for idx in range(8)}
        expected_user = {f"user:k{idx}": None if idx == 1 else idx * 10 for idx in range(8)}
        assert store.get_app_state("app") == expected_app
        assert store.get_user_state("app", "u1") == expected_user

        store.create_session("s-events", "app", "u1", {})
        store.append_event(cast("Any", _make_event("e1", session_id="s-events", offset_seconds=1)))
        store.append_event(cast("Any", _make_event("e2", session_id="s-events", offset_seconds=2)))
        store.append_event(cast("Any", _make_event("e3", session_id="s-events", offset_seconds=3)))
        store.append_event(cast("Any", _make_event("e4", session_id="s-events", offset_seconds=4)))
        store.append_event(cast("Any", _make_event("e5", session_id="s-events", offset_seconds=5)))

        newest_three = store.get_events("app", "u1", "s-events", limit=3)
        assert [event["id"] for event in newest_three] == ["e3", "e4", "e5"]
        assert store.get_events("app", "u1", "s-events", limit=0) == []
    finally:
        if seed_conn is not None:
            seed_conn.close()


def test_adbc_postgresql_uses_single_statement_jsonb_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    """PostgreSQL ADBC uses atomic JSONB ``||`` upsert with ``RETURNING state`` preserving falsy/None keys."""
    executed: list[tuple[str, tuple[Any, ...]]] = []
    now = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)
    cursor = MagicMock()

    def run_sql(sql: str, params: Any = ()) -> None:
        normalized = _normalized_sql(sql)
        executed.append((normalized, tuple(params)))
        if normalized.startswith("SELECT id, app_name, user_id, state"):
            cursor.fetchone.return_value = ("s1", "app", "u1", {"s": 1}, now, now)
        elif "ON CONFLICT (app_name) DO UPDATE" in normalized:
            cursor.fetchone.return_value = ({"app:keep": 1, "app:flag": False, "app:gone": None},)
        elif "ON CONFLICT (app_name, user_id) DO UPDATE" in normalized:
            cursor.fetchone.return_value = ({"user:zero": 0},)

    cursor.execute.side_effect = run_sql
    conn = SimpleNamespace(cursor=lambda: cursor, commit=MagicMock(), rollback=MagicMock())
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_postgresql", "uri": "postgresql://localhost/db"})
    store = AdbcADKStore(config)
    assert store.dialect == DIALECT_POSTGRESQL

    updated = store.append_event_and_update_state(
        cast("Any", _make_event("e1")),
        "app",
        "u1",
        "s1",
        {"s": 1},
        app_state_delta={"app:flag": False, "app:gone": None},
        user_state_delta={"user:zero": 0},
    )
    assert updated["state"] == {"s": 1, "app:keep": 1, "app:flag": False, "app:gone": None, "user:zero": 0}
    sql_statements = [stmt for stmt, _ in executed]
    assert any(
        "COALESCE(adk_app_state.state, '{}'::jsonb) || EXCLUDED.state" in stmt and "RETURNING state" in stmt
        for stmt in sql_statements
    )
    assert any(
        "COALESCE(adk_user_state.state, '{}'::jsonb) || EXCLUDED.state" in stmt and "RETURNING state" in stmt
        for stmt in sql_statements
    )


@pytest.mark.parametrize(
    ("driver_name", "expected_dialect", "lock_sql", "unique_error"),
    [
        ("oracle", DIALECT_ORACLE, "SELECT state FROM adk_app_state WHERE app_name = :1 FOR UPDATE", "ORA-00001"),
        (
            "db2",
            DIALECT_DB2,
            "SELECT state FROM adk_app_state WHERE app_name = ? FETCH FIRST 1 ROWS ONLY FOR UPDATE WITH RS",
            "SQLSTATE=23505",
        ),
        (
            "mssql",
            DIALECT_MSSQL,
            "SELECT TOP 1 state FROM adk_app_state WITH (UPDLOCK, HOLDLOCK) WHERE app_name = ?",
            "Violation of PRIMARY KEY constraint 2627",
        ),
    ],
)
def test_adbc_row_lock_dialects_retry_unique_race_and_merge(
    monkeypatch: pytest.MonkeyPatch, driver_name: str, expected_dialect: str, lock_sql: str, unique_error: str
) -> None:
    """Oracle, Db2, and MSSQL ADBC use row locks and recover from concurrent initial INSERT unique races."""
    executed: list[tuple[str, tuple[Any, ...]]] = []
    now = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)
    cursor = MagicMock()
    app_lock_reads = {"count": 0}

    def run_sql(sql: str, params: Any = ()) -> None:
        normalized = _normalized_sql(sql)
        executed.append((normalized, tuple(params)))
        if normalized.startswith("SELECT id, app_name, user_id, state"):
            cursor.fetchone.return_value = ("s1", "app", "u1", '{"s": 1}', now, now)
        elif normalized == lock_sql:
            app_lock_reads["count"] += 1
            cursor.fetchone.return_value = None if app_lock_reads["count"] == 1 else ('{"app:other": 9}',)
        elif normalized.startswith("INSERT INTO adk_app_state"):
            raise RuntimeError(unique_error)
        elif normalized.startswith("SELECT state FROM adk_user_state"):
            cursor.fetchone.return_value = ('{"user:old": 1}',)

    cursor.execute.side_effect = run_sql
    conn = SimpleNamespace(cursor=lambda: cursor, commit=MagicMock(), rollback=MagicMock())
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    config = AdbcConfig(connection_config={"driver_name": driver_name})
    store = AdbcADKStore(config)
    assert store.dialect == expected_dialect

    updated = store.append_event_and_update_state(
        cast("Any", _make_event("e1")), "app", "u1", "s1", {"s": 1}, app_state_delta={"app:new": False}
    )
    assert updated["state"] == {"s": 1, "app:other": 9, "app:new": False, "user:old": 1}
    assert lock_sql in [stmt for stmt, _ in executed]


@pytest.mark.parametrize("driver_name", ["bigquery", "snowflake", "teradata", "generic"])
def test_adbc_cas_dialects_retry_and_raise_on_exhaustion(monkeypatch: pytest.MonkeyPatch, driver_name: str) -> None:
    """BigQuery, Snowflake, Teradata, and generic ADBC retry update_time CAS races and fail cleanly after exhaustion."""
    t0 = datetime(2026, 11, 12, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 11, 12, 12, 0, 1, tzinfo=UTC)
    app_state_reads = {"count": 0}
    cursor = MagicMock()

    def run_sql_succeeds(sql: str, params: Any = ()) -> None:
        normalized = _normalized_sql(sql)
        if normalized.startswith("SELECT id, app_name, user_id, state"):
            cursor.fetchone.return_value = ("s1", "app", "u1", '{"s": 1}', t0, t0)
        elif normalized.startswith("SELECT state, update_time FROM adk_app_state"):
            app_state_reads["count"] += 1
            if app_state_reads["count"] == 1:
                cursor.fetchone.return_value = ('{"app:k1": 1}', t0)
            else:
                cursor.fetchone.return_value = ('{"app:k1": 1, "app:k2": 2}', t1)
        elif normalized.startswith("UPDATE adk_app_state"):
            cursor.rowcount = 0 if app_state_reads["count"] == 1 else 1
        elif normalized.startswith("SELECT state FROM adk_user_state"):
            cursor.fetchone.return_value = None

    cursor.execute.side_effect = run_sql_succeeds
    conn = SimpleNamespace(
        cursor=lambda: cursor,
        commit=MagicMock(),
        rollback=MagicMock(),
        _autocommit=False,
        adbc_connection=SimpleNamespace(
            set_autocommit=lambda _: None, get_option=lambda _: "false", rollback=lambda: None
        ),
    )
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    config = AdbcConfig(connection_config={"driver_name": driver_name})
    store = AdbcADKStore(config)
    updated = store.append_event_and_update_state(
        cast("Any", _make_event("e1")), "app", "u1", "s1", {"s": 1}, app_state_delta={"app:k3": None}
    )
    assert updated["state"] == {"s": 1, "app:k1": 1, "app:k2": 2, "app:k3": None}
    assert app_state_reads["count"] == 2

    def run_sql_always_conflicts(sql: str, params: Any = ()) -> None:
        normalized = _normalized_sql(sql)
        if normalized.startswith("SELECT id, app_name, user_id, state"):
            cursor.fetchone.return_value = ("s1", "app", "u1", '{"s": 1}', t0, t0)
        elif normalized.startswith("SELECT state, update_time FROM adk_app_state"):
            cursor.fetchone.return_value = ('{"app:k1": 1}', t0)
        elif normalized.startswith("UPDATE adk_app_state"):
            cursor.rowcount = 0

    cursor.execute.side_effect = run_sql_always_conflicts
    with pytest.raises(SerializationConflictError):
        store.append_event_and_update_state(
            cast("Any", _make_event("e2")), "app", "u1", "s1", {"s": 1}, app_state_delta={"app:k3": 3}
        )


@pytest.mark.parametrize(
    ("driver_name", "expected_fragment"),
    [
        ("teradata", "SELECT TOP 2 e.id"),
        ("oracle", "ORDER BY e.timestamp DESC, e.id DESC OFFSET 0 ROWS FETCH NEXT :"),
        ("adbc_driver_postgresql", "ORDER BY e.timestamp DESC, e.id DESC LIMIT $"),
    ],
)
def test_adbc_get_events_newest_n_reverses_descending_rows(
    monkeypatch: pytest.MonkeyPatch, driver_name: str, expected_fragment: str
) -> None:
    """get_events(limit=N) selects newest rows descending and returns them chronologically ascending."""
    t_old = datetime(2026, 11, 12, 12, 0, 1, tzinfo=UTC)
    t_new = datetime(2026, 11, 12, 12, 0, 2, tzinfo=UTC)
    executed_sql: list[str] = []
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("e-new", "s1", "inv2", t_new, '{"seq": 2}', "app", "u1"),
        ("e-old", "s1", "inv1", t_old, '{"seq": 1}', "app", "u1"),
    ]
    cursor.execute.side_effect = lambda sql, params=(): executed_sql.append(_normalized_sql(sql))
    conn = SimpleNamespace(cursor=lambda: cursor, commit=MagicMock(), rollback=MagicMock())
    monkeypatch.setattr(AdbcConfig, "provide_connection", lambda _: nullcontext(conn))

    store = AdbcADKStore(AdbcConfig(connection_config={"driver_name": driver_name}))
    events = store.get_events("app", "u1", "s1", limit=2)
    assert [event["id"] for event in events] == ["e-old", "e-new"]
    assert expected_fragment in executed_sql[0]


def _make_event(
    event_id: str, session_id: str = "s1", app_name: str = "app", user_id: str = "u1", offset_seconds: int = 0
) -> dict[str, Any]:
    return {
        "id": event_id,
        "app_name": app_name,
        "user_id": user_id,
        "session_id": session_id,
        "invocation_id": f"inv-{event_id}",
        "author": "user",
        "actions": b"",
        "long_running_tool_ids_json": None,
        "grounding_metadata_json": None,
        "custom_metadata_json": None,
        "usage_metadata_json": None,
        "citation_metadata_json": None,
        "timestamp": datetime(2026, 11, 12, 12, 0, offset_seconds, tzinfo=UTC),
        "event_data": {"seq": event_id},
    }


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())
