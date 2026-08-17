# pyright: reportPrivateUsage=false
"""Unit tests for psqlpy ADK store extension configuration."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from typing_extensions import NotRequired, Self

from sqlspec.adapters.psqlpy.adk import PsqlpyADKConfig, PsqlpyADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_psqlpy_adk_config_types_adapter_local_optimizations() -> None:
    """Psqlpy ADK optimization switches live on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", PsqlpyADKConfig).__optional_keys__
    assert cast("Any", PsqlpyADKConfig).__optional_keys__ - cast("Any", ADKConfig).__optional_keys__ == {
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "enable_event_generated_columns",
        "enable_covering_indexes",
        "fillfactor",
    }

    for feature_name in ("enable_event_generated_columns", "enable_covering_indexes"):
        annotation = cast("Any", PsqlpyADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (bool,)


async def test_psqlpy_adk_events_table_uses_plain_schema_by_default() -> None:
    """Psqlpy ADK optimization DDL stays opt-in through extension config."""

    store = PsqlpyADKStore(_mock_config())

    sql = await store._events_table_ddl()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


async def test_psqlpy_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Psqlpy ADK extension settings enable PostgreSQL-specific event DDL."""

    store = PsqlpyADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))

    sql = await store._events_table_ddl()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


class _RecordingConnection:
    """Captures the SQL and bound parameters a session listing issues."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, sql: str, params: "list[Any]") -> None:
        self.calls.append((sql, tuple(params)))
        return

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_connection() -> "tuple[PsqlpyADKStore, _RecordingConnection]":
    conn = _RecordingConnection()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    return PsqlpyADKStore(config), conn


async def test_psqlpy_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as numbered parameters."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = conn.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = $1 AND user_id = $2 "
        "ORDER BY create_time ASC, id ASC LIMIT $3 OFFSET $4"
    )
    assert params == ("app", "u1", 10, 20)


async def test_psqlpy_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app")

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = $1 ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


async def test_psqlpy_list_sessions_numbers_page_placeholders_without_a_user_filter() -> None:
    """Page placeholders follow the scope parameters that are actually present."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app", limit=5)

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT $2 OFFSET $3")
    assert params == ("app", 5, 0)


async def test_psqlpy_list_sessions_zero_limit_never_opens_a_connection() -> None:
    """A zero limit short-circuits before any database work."""
    store, conn = _session_store_with_connection()

    assert await store.list_sessions("app", limit=0) == []
    assert conn.calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
async def test_psqlpy_list_sessions_rejects_invalid_options_before_connecting(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, conn = _session_store_with_connection()

    with pytest.raises(ValueError):
        await store.list_sessions("app", **options)

    assert conn.calls == []
