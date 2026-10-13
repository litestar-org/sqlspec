"""Verify that framework extension packages re-export the SQLSpec service base classes.

The base classes live in :mod:`sqlspec.service`. Each first-party framework integration
re-exports them so consumers can pull the canonical service base from the framework
namespace they are already importing from.
"""

import asyncio
import importlib
import inspect
import threading
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import SQL, CursorFilter, LimitOffsetFilter, OffsetPagination
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import __all__ as adk_all
from sqlspec.extensions.adk.artifact.service import _sanitize_path_component
from sqlspec.extensions.adk.memory.converters import memory_entry_to_record
from sqlspec.extensions.adk.memory.service import SQLSpecMemoryService, SQLSpecSyncMemoryService
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.extensions.adk.service import SQLSpecSessionService
from sqlspec.extensions.adk.store import BaseAsyncADKStore
from sqlspec.extensions.events._channel import AsyncEventChannel, SyncEventChannel
from sqlspec.extensions.events._models import EventMessage
from sqlspec.extensions.events._queue import SyncTableEventQueue
from sqlspec.extensions.events._store import BaseEventQueueStore
from sqlspec.extensions.litestar import __all__ as litestar_all
from sqlspec.extensions.litestar._utils import (
    delete_sqlspec_scope_state,
    get_sqlspec_scope_state,
    set_sqlspec_scope_state,
)
from sqlspec.extensions.prometheus._observer import PrometheusStatementObserver
from sqlspec.observability import StatementEvent
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService


@pytest.mark.parametrize(
    "module_name",
    [
        "sqlspec.extensions.litestar",
        "sqlspec.extensions.fastapi",
        "sqlspec.extensions.starlette",
        "sqlspec.extensions.sanic",
        "sqlspec.extensions.flask",
    ],
)
def test_async_and_sync_service_reexports(module_name: str) -> None:
    """Every framework package re-exports both service base classes.

    Flask is included because its plugin can portal an async driver, so
    consumers need access to the async service even from a sync framework.
    """
    module = importlib.import_module(module_name)
    assert module.SQLSpecAsyncService is SQLSpecAsyncService
    assert module.SQLSpecSyncService is SQLSpecSyncService
    assert "SQLSpecAsyncService" in module.__all__
    assert "SQLSpecSyncService" in module.__all__


def test_service_exists_bounds_multi_row_queries() -> None:
    """Verify sync and async exists() bound queries to 1 row when multiple rows match."""
    sync_config = SqliteConfig()
    try:
        with sync_config.provide_session() as session:
            session.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT)")
            session.execute_many("INSERT INTO items (id, category) VALUES (?, ?)", [(1, "a"), (2, "a"), (3, "a")])
            service = SQLSpecSyncService(session)
            assert service.exists(SQL("SELECT id FROM items WHERE category = :cat", cat="a")) is True
            assert service.exists(SQL("SELECT id FROM items WHERE category = :cat", cat="b")) is False
    finally:
        sync_config.close_pool()

    async def _run_async() -> None:
        async_config = AiosqliteConfig()
        try:
            async with async_config.provide_session() as session:
                await session.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT)")
                await session.execute_many(
                    "INSERT INTO items (id, category) VALUES (?, ?)", [(1, "a"), (2, "a"), (3, "a")]
                )
                service = SQLSpecAsyncService(session)
                assert await service.exists(SQL("SELECT id FROM items WHERE category = :cat", cat="a")) is True
                assert await service.exists(SQL("SELECT id FROM items WHERE category = :cat", cat="b")) is False
        finally:
            await async_config.close_pool()

    asyncio.run(_run_async())


def test_session_bound_begin_transaction_nested_in_session_transaction_enforces_owner() -> None:
    """Verify session-bound begin_transaction() nested inside session.transaction() tracks owner."""
    sync_config = SqliteConfig()
    try:
        with sync_config.provide_session() as session:
            service = SQLSpecSyncService(session)
            with session.transaction():
                with service.begin_transaction():
                    assert service._transaction_state is not None
                    errors: list[Exception] = []

                    def _other_thread() -> None:
                        try:
                            with service.begin_transaction():
                                pass
                        except ImproperConfigurationError as exc:
                            errors.append(exc)

                    worker = threading.Thread(target=_other_thread)
                    worker.start()
                    worker.join()
                    assert len(errors) == 1
                assert service._transaction_state is None
    finally:
        sync_config.close_pool()

    async def _run_async() -> None:
        async_config = AiosqliteConfig()
        try:
            async with async_config.provide_session() as session:
                service = SQLSpecAsyncService(session)
                async with session.transaction():
                    async with service.begin_transaction():
                        assert service._transaction_state is not None

                        async def _other_task() -> None:
                            with pytest.raises(ImproperConfigurationError, match="another task or thread"):
                                async with service.begin_transaction():
                                    pass

                        await asyncio.create_task(_other_task())
                    assert service._transaction_state is None
        finally:
            await async_config.close_pool()

    asyncio.run(_run_async())


def test_paginate_honors_pre_attached_limit_offset_and_cursor_rejects_count_with_window() -> None:
    """Verify paginate() reads statement.filters for LimitOffsetFilter and paginate_cursor() rejects count_with_window."""
    sync_config = SqliteConfig()
    try:
        with sync_config.provide_session() as session:
            session.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            session.execute_many("INSERT INTO items (id, name) VALUES (?, ?)", [(1, "a"), (2, "b")])
            service = SQLSpecSyncService(session)

            stmt = SQL("SELECT id, name FROM items ORDER BY id", LimitOffsetFilter(limit=10, offset=20))
            page = service.paginate(stmt)
            assert isinstance(page, OffsetPagination)
            assert page.items == []
            assert page.limit == 10
            assert page.offset == 20
            assert page.total == 2

            caller_kwargs: dict[str, Any] = {"count_with_window": False}
            with pytest.raises(ImproperConfigurationError, match="count_with_window"):
                service.paginate_cursor(SQL("SELECT id, name FROM items"), CursorFilter("id", limit=5), **caller_kwargs)
            assert caller_kwargs == {"count_with_window": False}
    finally:
        sync_config.close_pool()

    async def _run_async() -> None:
        async_config = AiosqliteConfig()
        try:
            async with async_config.provide_session() as session:
                await session.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
                await session.execute_many("INSERT INTO items (id, name) VALUES (?, ?)", [(1, "a"), (2, "b")])
                service = SQLSpecAsyncService(session)

                stmt = SQL("SELECT id, name FROM items ORDER BY id", LimitOffsetFilter(limit=10, offset=20))
                page = await service.paginate(stmt)
                assert isinstance(page, OffsetPagination)
                assert page.items == []
                assert page.limit == 10
                assert page.offset == 20
                assert page.total == 2

                caller_kwargs: dict[str, Any] = {"count_with_window": False}
                with pytest.raises(ImproperConfigurationError, match="count_with_window"):
                    await service.paginate_cursor(
                        SQL("SELECT id, name FROM items"), CursorFilter("id", limit=5), **caller_kwargs
                    )
                assert caller_kwargs == {"count_with_window": False}
        finally:
            await async_config.close_pool()

    asyncio.run(_run_async())


def test_adk_sanitize_path_component_rejects_slashes_and_empty() -> None:
    with pytest.raises(ValueError, match="Unsafe path component"):
        _sanitize_path_component("")
    with pytest.raises(ValueError, match="Unsafe path component"):
        _sanitize_path_component("..")
    with pytest.raises(ValueError, match="Unsafe path component"):
        _sanitize_path_component("foo/bar")
    with pytest.raises(ValueError, match="Unsafe path component"):
        _sanitize_path_component("foo\\bar")
    assert _sanitize_path_component("report.v2.pdf") == "report.v2.pdf"


def test_adk_base_async_store_abstract_methods() -> None:
    abstract_methods: frozenset[str] = getattr(BaseAsyncADKStore, "__abstractmethods__", frozenset())
    assert "create_tables" in abstract_methods
    assert "create_session" in abstract_methods


def test_adk_memory_store_validates_table_name_and_commits_drop() -> None:
    class _DummySyncMemoryStore(BaseSyncADKMemoryStore[Any]):
        def create_tables(self) -> None:
            return None

        def insert_memory_entries(self, entries: Any, owner_id: Any = None) -> int:
            return 0

        def search_entries(self, *args: Any, **kwargs: Any) -> list[Any]:
            return []

        def delete_entries_by_session(self, session_id: str) -> int:
            return 0

        def delete_entries_older_than(self, days: int, app_name: Any = None, scope: Any = None) -> int:
            return 0

        def _memory_table_ddl(self) -> str:
            return "CREATE TABLE adk_memory (id TEXT)"

        def _drop_memory_table_sql(self) -> list[str]:
            return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    bad_config = MagicMock()
    bad_config.extension_config = {"adk": {"memory_table": "bad; DROP TABLE users;--"}}
    with pytest.raises(ValueError, match="Invalid table name"):
        _DummySyncMemoryStore(bad_config)

    driver = MagicMock()
    session_cm = MagicMock()
    session_cm.__enter__.return_value = driver
    session_cm.__exit__.return_value = False
    good_config = MagicMock()
    good_config.extension_config = {"adk": {"memory_table": "valid_memory_table"}}
    good_config.provide_session.return_value = session_cm

    store = _DummySyncMemoryStore(good_config)
    store.drop_tables()
    driver.execute.assert_called_once_with("DROP TABLE IF EXISTS valid_memory_table")
    driver.commit.assert_called_once()


@pytest.mark.anyio
async def test_adk_async_memory_store_drop_tables_commits_and_bridges_sync_session() -> None:
    class _DummyAsyncMemoryStore(BaseAsyncADKMemoryStore[Any]):
        async def create_tables(self) -> None:
            return None

        async def insert_memory_entries(self, entries: Any, owner_id: Any = None) -> int:
            return 0

        async def search_entries(self, *args: Any, **kwargs: Any) -> list[Any]:
            return []

        async def delete_entries_by_session(self, session_id: str) -> int:
            return 0

        async def delete_entries_older_than(self, days: int, app_name: Any = None, scope: Any = None) -> int:
            return 0

        async def _memory_table_ddl(self) -> str:
            return "CREATE TABLE adk_memory (id TEXT)"

        def _drop_memory_table_sql(self) -> list[str]:
            return [f"DROP TABLE IF EXISTS {self._memory_table}"]

    driver = MagicMock()
    session_cm = MagicMock()
    session_cm.__enter__.return_value = driver
    session_cm.__exit__.return_value = False
    del session_cm.__aenter__

    config = MagicMock()
    config.extension_config = {"adk": {"memory_table": "valid_memory"}}
    config.provide_session.return_value = session_cm

    store = _DummyAsyncMemoryStore(config)
    await store.drop_tables()
    driver.execute.assert_called_once_with("DROP TABLE IF EXISTS valid_memory")
    driver.commit.assert_called_once()


def test_adk_memory_converter_normalizes_naive_timestamp_to_utc() -> None:
    entry = MagicMock()
    part = MagicMock()
    part.text = "remember this"
    entry.content.parts = [part]
    entry.content.model_dump.return_value = {"parts": [{"text": "remember this"}]}
    entry.custom_metadata = None
    entry.timestamp = "2026-05-01T12:00:00"
    entry.id = "mem-1"
    entry.author = "user"

    record = memory_entry_to_record(entry, app_name="app", user_id="u1")
    assert record is not None
    assert record["timestamp"].tzinfo == UTC


def test_adk_memory_service_accepts_embeddings_parameter() -> None:
    for cls in (SQLSpecMemoryService, SQLSpecSyncMemoryService):
        assert "embeddings" in inspect.signature(cls.add_session_to_memory).parameters
        assert "embeddings" in inspect.signature(cls.add_events_to_memory).parameters
        assert "embeddings" in inspect.signature(cls.add_memory).parameters


@pytest.mark.anyio
async def test_adk_session_service_list_sessions_merges_scoped_state() -> None:
    store = AsyncMock()
    now = datetime.now(UTC)
    store.list_sessions.return_value = [
        {
            "id": "s1",
            "app_name": "app1",
            "user_id": "u1",
            "state": {"session_key": "sval"},
            "create_time": now,
            "update_time": now,
        }
    ]
    store.get_app_state.return_value = {"app:app_key": "aval"}
    store.get_user_state.return_value = {"user:user_key": "uval"}

    service = SQLSpecSessionService(store)
    resp = await service.list_sessions(app_name="app1", user_id="u1")
    assert len(resp.sessions) == 1
    state = resp.sessions[0].state
    assert state["session_key"] == "sval"
    assert state["app:app_key"] == "aval"
    assert state["user:user_key"] == "uval"


def test_events_store_extension_config_options_complete() -> None:
    options = BaseEventQueueStore.extension_config_options
    assert "cleanup_on_ack" in options


def test_events_queue_nack_sql_does_not_double_increment_attempts() -> None:
    config = MagicMock()
    config.statement_config.dialect = "sqlite"
    config.extension_config = {}
    queue = SyncTableEventQueue(config)
    nack_sql = queue._nack_sql()
    assert "attempts = attempts + 1" not in nack_sql


def test_events_queue_retention_zero_deletes_immediately_on_ack() -> None:
    driver = MagicMock()
    driver.execute.return_value = MagicMock(rows_affected=1)
    session_cm = MagicMock()
    session_cm.__enter__.return_value = driver
    session_cm.__exit__.return_value = False
    config = MagicMock()
    config.statement_config.dialect = "sqlite"
    config.extension_config = {"events": {"retention_seconds": 0, "cleanup_on_ack": True}}
    config.provide_session.return_value = session_cm

    queue = SyncTableEventQueue(config)
    queue.ack("evt-1")
    executed_stmt = driver.execute.call_args[0][0]
    assert "DELETE FROM" in executed_stmt.sql


def test_sync_event_channel_survives_transient_dequeue_error() -> None:
    config = MagicMock()
    config.is_async = False
    config.statement_config.dialect = "sqlite"
    config.extension_config = {}
    channel = SyncEventChannel(config)
    stop_event = threading.Event()

    calls = 0

    def _flaky_dequeue(*args: Any, **kwargs: Any) -> EventMessage | None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient connection error")
        stop_event.set()
        return None

    def _noop_handler(message: EventMessage) -> None:
        _ = message

    channel._backend = MagicMock(dequeue=_flaky_dequeue)
    channel._run_listener("l1", "notifications", _noop_handler, stop_event, 0.001, True)
    assert calls >= 2


@pytest.mark.anyio
async def test_async_event_channel_nacks_on_handler_failure() -> None:
    config = MagicMock()
    config.is_async = True
    config.statement_config.dialect = "sqlite"
    config.extension_config = {}
    channel = AsyncEventChannel(config)
    stop_event = asyncio.Event()

    now = datetime.now(UTC)
    message = EventMessage(
        event_id="evt-fail",
        channel="jobs",
        payload={"x": 1},
        metadata=None,
        attempts=1,
        available_at=now,
        lease_expires_at=now,
        created_at=now,
    )
    nacked: list[str] = []

    async def _dequeue(*args: Any, **kwargs: Any) -> EventMessage | None:
        if not nacked:
            return message
        stop_event.set()
        return None

    async def _nack(event_id: str) -> None:
        nacked.append(event_id)
        stop_event.set()

    channel._backend = MagicMock(dequeue=_dequeue, nack=_nack)

    def _failing_handler(message: EventMessage) -> None:
        _ = message
        raise ValueError("boom")

    await channel._run_listener("l1", "jobs", _failing_handler, stop_event, 0.001, True)
    assert nacked == ["evt-fail"]


def test_litestar_delete_scope_state_is_idempotent() -> None:
    scope: Any = {}
    set_sqlspec_scope_state(scope, "conn", "value")
    assert get_sqlspec_scope_state(scope, "conn") == "value"
    delete_sqlspec_scope_state(scope, "conn")
    delete_sqlspec_scope_state(scope, "conn")
    assert get_sqlspec_scope_state(scope, "conn") is None


def test_prometheus_observer_rejects_high_cardinality_labels_and_lazy_payload() -> None:
    with pytest.raises(ValueError, match="High-cardinality Prometheus label names"):
        PrometheusStatementObserver(label_names=("db_system", "sql"))

    observer = PrometheusStatementObserver.__new__(PrometheusStatementObserver)
    observer._label_names = ("db_system", "operation")
    event = MagicMock(spec=StatementEvent)
    event.db_system = "postgresql"
    event.operation = "SELECT"
    values = observer._label_values(event)
    assert values == ("postgresql", "SELECT")
    event.as_dict.assert_not_called()


def test_lazy_package_barrels_for_litestar_and_adk() -> None:
    import sqlspec.extensions.adk as adk_mod
    import sqlspec.extensions.litestar as litestar_mod

    for name in litestar_all:
        assert hasattr(litestar_mod, name)
    for name in adk_all:
        assert hasattr(adk_mod, name)
    with pytest.raises(AttributeError):
        _ = litestar_mod.NonExistentSymbol  # type: ignore[attr-defined]
    with pytest.raises(AttributeError):
        _ = adk_mod.NonExistentSymbol  # type: ignore[attr-defined]
