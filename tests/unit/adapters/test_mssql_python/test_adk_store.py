# pyright: reportPrivateUsage=false
"""Unit tests for mssql-python ADK store wiring and T-SQL generation."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from typing_extensions import NotRequired

from sqlspec.adapters.mssql_python.adk import MssqlPythonADKConfig, MssqlPythonADKStore
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import BaseSyncADKStore


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_mssql_python_adk_exports_sync_store_type() -> None:
    """The adapter exposes a sync store that implements the current ADK base."""

    sync_store = MssqlPythonADKStore(_mock_config())

    assert isinstance(sync_store, BaseSyncADKStore)


def test_mssql_python_adk_config_extends_base_config_without_redeclaring_base_fields() -> None:
    """Adapter-local ADK config should only add MSSQL-specific fields."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", MssqlPythonADKConfig).__optional_keys__

    base_fields = set(ADKConfig.__annotations__)
    local_fields = set(MssqlPythonADKConfig.__annotations__) - base_fields
    assert local_fields == {"native_json"}
    annotation = cast("Any", MssqlPythonADKConfig.__annotations__["native_json"])
    assert get_origin(annotation) is NotRequired
    assert get_args(annotation) == (bool,)


def test_sync_store_reads_adk_table_names_from_extension_config() -> None:
    """Store table names come from extension_config['adk'], not driver features."""

    store = MssqlPythonADKStore(
        _mock_config({
            "session_table": "custom_session",
            "events_table": "custom_event",
            "app_state_table": "custom_app_state",
            "user_state_table": "custom_user_state",
            "metadata_table": "custom_metadata",
        })
    )

    assert store.session_table == "custom_session"
    assert store.events_table == "custom_event"
    assert store.app_state_table == "custom_app_state"
    assert store.user_state_table == "custom_user_state"
    assert store.metadata_table == "custom_metadata"


def test_sync_store_generates_tsql_idempotent_schema_with_conservative_json() -> None:
    """The default MSSQL DDL uses idempotent sys.tables probes and NVARCHAR JSON storage."""

    store = MssqlPythonADKStore(_mock_config())

    sessions_sql = store._sessions_table_ddl()
    events_sql = store._events_table_ddl()
    app_state_sql = store._app_states_table_ddl()

    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in sessions_sql
    assert "schema_id = SCHEMA_ID(N'dbo')" in sessions_sql
    assert "row_id UNIQUEIDENTIFIER NOT NULL" in sessions_sql
    assert "DEFAULT NEWSEQUENTIALID()" in sessions_sql
    assert "state NVARCHAR(MAX) NOT NULL" in sessions_sql
    assert "DATETIME2(6)" in sessions_sql
    assert "sys.indexes" not in sessions_sql
    assert "ON DELETE CASCADE" in events_sql
    assert "event_data NVARCHAR(MAX) NOT NULL" in events_sql
    assert "state NVARCHAR(MAX) NOT NULL" in app_state_sql


def test_sync_store_can_force_native_json_from_extension_config() -> None:
    """MSSQL-native JSON is opt-in unless version detection proves support."""

    store = MssqlPythonADKStore(_mock_config({"native_json": True}))

    assert "state JSON NOT NULL" in store._sessions_table_ddl()
    assert "event_data JSON NOT NULL" in store._events_table_ddl()


def test_sync_store_uses_tsql_upsert_and_top_limit() -> None:
    """Write/read SQL uses MERGE for scoped upserts and TOP for limited reads."""

    store = MssqlPythonADKStore(_mock_config())

    app_upsert = store._upsert_app_state_sql()
    events_sql, params = store._events_query("app", "user", "session", limit=5)

    assert "MERGE INTO [dbo].[adk_app_state]" in app_upsert
    assert "WITH (HOLDLOCK)" in app_upsert
    assert "SELECT TOP (?)" in events_sql
    assert params[0] == 5
    assert "LIMIT" not in events_sql


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_capture(
    monkeypatch: "pytest.MonkeyPatch",
) -> "tuple[MssqlPythonADKStore, list[tuple[str, tuple[Any, ...]]]]":
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: MssqlPythonADKStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    monkeypatch.setattr(MssqlPythonADKStore, "_execute_fetchall", capture)
    return MssqlPythonADKStore(_mock_config()), calls


def test_mssql_python_list_sessions_binds_order_and_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """SQL Server row limiting binds the offset before the row count."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    )
    assert params == ("app", "u1", 20, 10)


def test_mssql_python_list_sessions_defaults_to_recent_first_without_a_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """The default listing keeps recent-first ordering and emits no row limiting."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app")

    sql, params = calls[0]
    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_mssql_python_list_sessions_pages_an_unfiltered_listing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Row limiting composes with an app-only listing."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", limit=5)

    sql, params = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY")
    assert params == ("app", 0, 5)


def test_mssql_python_list_sessions_zero_limit_never_queries(monkeypatch: "pytest.MonkeyPatch") -> None:
    """A zero limit short-circuits before any database work."""
    store, calls = _session_store_with_capture(monkeypatch)

    assert store.list_sessions("app", limit=0) == []
    assert calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_mssql_python_list_sessions_rejects_invalid_options(
    monkeypatch: "pytest.MonkeyPatch", options: "dict[str, Any]"
) -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, calls = _session_store_with_capture(monkeypatch)

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []
