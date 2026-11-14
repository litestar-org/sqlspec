"""Unit tests for Spanner session-control behavior."""

import asyncio
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, Self, cast

import pytest

import sqlspec.adapters.spanner.config as spanner_config
from sqlspec.adapters.spanner.config import SpannerSyncConfig


def test_provide_session_uses_config_driver_features(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _SessionContext:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    request_options = {"priority": 1, "request_tag": "sqlspec-test"}
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        driver_features={"request_options": request_options},
    )
    monkeypatch.setattr(spanner_config, "SpannerSyncSessionContext", _SessionContext)

    context = config.provide_session()

    assert isinstance(context, _SessionContext)
    assert captured["driver_features"] is config.driver_features
    assert captured["driver_features"]["request_options"] is request_options


def test_provide_session_accepts_spanner_execution_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _SessionContext:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    config_request_options = {"request_tag": "config"}
    session_request_options = {"request_tag": "session"}
    directed_read_options = cast(Any, SimpleNamespace(tag="directed"))
    retry = cast(Any, object())
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        driver_features={"request_options": config_request_options},
    )
    monkeypatch.setattr(spanner_config, "SpannerSyncSessionContext", _SessionContext)

    config.provide_session(
        request_options=session_request_options, directed_read_options=directed_read_options, retry=retry, timeout=12.0
    )

    assert captured["driver_features"] is not config.driver_features
    assert captured["driver_features"]["request_options"] is session_request_options
    assert captured["driver_features"]["directed_read_options"] is directed_read_options
    assert captured["driver_features"]["retry"] is retry
    assert captured["driver_features"]["timeout"] == 12.0
    assert config.driver_features["request_options"] is config_request_options


def test_close_pool_closes_the_database() -> None:
    """Sessions go back before Database.close() releases the manager that owns them."""
    calls: list[str] = []

    class _Database:
        def close(self) -> None:
            calls.append("database")

    class _Pool:
        def clear(self) -> None:
            calls.append("pool")

    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    config._database = cast("Any", _Database())
    config.connection_instance = cast("Any", _Pool())

    config.close_pool()

    assert calls == ["pool", "database"]
    assert config._database is None


def test_write_transactions_reuse_pooled_sessions() -> None:
    """Two write transactions must check out from the pool, not create and delete sessions."""

    class _Txn:
        _transaction_id = "txn"
        _mutations: tuple[object, ...] = ()
        committed = None
        rolled_back = False

        def __enter__(self) -> "Self":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    class _Session:
        def __init__(self) -> None:
            self.create_calls = 0
            self.delete_calls = 0

        def create(self) -> None:
            self.create_calls += 1

        def delete(self) -> None:
            self.delete_calls += 1

        def transaction(self) -> _Txn:
            return _Txn()

    class _SessionsManager:
        def __init__(self) -> None:
            self.session = _Session()
            self.checked_out = 0
            self.returned = 0

        def get_session(self, _transaction_type: object) -> _Session:
            self.checked_out += 1
            return self.session

        def put_session(self, _session: object) -> None:
            self.returned += 1

    class _Database:
        def __init__(self) -> None:
            self.sessions_manager = _SessionsManager()

    database = _Database()
    config = SpannerSyncConfig(connection_config={"project": "p", "instance_id": "i", "database_id": "d"})
    config.get_database = lambda: cast("Any", database)  # type: ignore[assignment]

    with config.provide_connection(transaction=True):
        pass
    with config.provide_connection(transaction=True):
        pass

    assert database.sessions_manager.checked_out == 2
    assert database.sessions_manager.returned == 2
    assert database.sessions_manager.session.create_calls == 0
    assert database.sessions_manager.session.delete_calls == 0


def test_driver_nested_in_transaction_guard() -> None:
    """_connection_in_transaction returns True when _owns_transaction is False, preventing commit."""
    from unittest.mock import Mock

    from sqlspec.adapters.spanner.driver import SpannerSyncDriver

    mock_conn = Mock()
    driver = SpannerSyncDriver(connection=mock_conn)
    driver.bind_external_transaction()
    assert driver._connection_in_transaction() is True
    driver.commit()
    assert mock_conn.commit.call_count == 0
    driver.rollback()
    assert mock_conn.rollback.call_count == 0


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("mode", ["default", "primary", "pinned", "bounded", "context", "cancelled"])
@pytest.mark.parametrize("managed", [False, True])
async def test_native_snapshot_bounds_and_selected_config_receipt(
    monkeypatch: pytest.MonkeyPatch, is_async: bool, mode: str, managed: bool
) -> None:
    from datetime import timedelta

    from sqlspec import SQLSpec
    from sqlspec.adapters.spanner.config import SpannerAsyncConfig
    from sqlspec.exceptions import ReadOnlyTransactionError, SQLConversionError
    from sqlspec.utils.routing import RoutingContext

    lifecycle: list[str] = []
    snapshots: list[dict[str, Any]] = []
    exits: list[object] = []

    class Snapshot:
        def __enter__(self) -> "Self":
            return self

        def __exit__(self, exc_type: object, *_: object) -> None:
            exits.append(exc_type)

        async def __aenter__(self) -> "Self":
            return self

        async def __aexit__(self, exc_type: object, *_: object) -> None:
            exits.append(exc_type)

    def snapshot(**kwargs: Any) -> Snapshot:
        snapshots.append(kwargs)
        return Snapshot()

    async def async_noop() -> None:
        return None

    close = async_noop if is_async else lambda: None
    database = SimpleNamespace(snapshot=snapshot, close=close)

    async def async_database(*args: Any, **kwargs: Any) -> Any:
        return database

    database_factory = async_database if is_async else lambda *args, **kwargs: database
    client = SimpleNamespace(
        instance=lambda *args, **kwargs: SimpleNamespace(database=database_factory), close=lambda: None
    )
    monkeypatch.setattr(spanner_config, "AsyncClient" if is_async else "Client", lambda **kwargs: client)
    config_type = SpannerAsyncConfig if is_async else SpannerSyncConfig
    config = cast(
        "Any",
        config_type(
            connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
            connection_instance=cast("Any", SimpleNamespace(clear=close)),
            driver_features={
                "default_staleness": {"exact_staleness": 10},
                "on_session_start": lambda driver: lifecycle.append("start"),
                "on_session_end": lambda driver: lifecycle.append("end"),
            },
            replicas=True,
            bind_key="orders",
        ),
    )
    assert not config.has_replicas
    assert config.supports_read_routing
    registry = SQLSpec()
    registry.add_config(config)
    scope = (
        RoutingContext.primary("orders")
        if mode == "primary"
        else RoutingContext.replica("orders", max_staleness_seconds=6)
        if mode == "context"
        else RoutingContext.scope()
    )
    with scope:
        if mode == "pinned":
            RoutingContext.mark_write("orders")
        explicit: dict[str, Any] = (
            {"max_staleness": timedelta(seconds=8)} if mode in {"primary", "pinned", "bounded"} else {}
        )
        context = (
            registry.provide_read_session(config, **explicit) if managed else config.provide_read_session(**explicit)
        )
        error_type = asyncio.CancelledError if is_async else RuntimeError
        expectation = pytest.raises(error_type) if mode == "cancelled" else nullcontext()
        with expectation:
            if is_async:
                async with context as session:
                    assert isinstance(session.connection, Snapshot)
                    with pytest.raises(ReadOnlyTransactionError):
                        await session.execute("INSERT INTO items (id) VALUES (1)")
                    with pytest.raises(ReadOnlyTransactionError):
                        await session.execute_many("INSERT INTO items (id) VALUES (@id)", [{"id": 1}])
                    with pytest.raises(SQLConversionError):
                        await session.execute("CREATE TABLE items (id INT64)")
                    if mode == "cancelled":
                        raise asyncio.CancelledError
            else:
                with context as session:
                    assert isinstance(session.connection, Snapshot)
                    with pytest.raises(ReadOnlyTransactionError):
                        session.execute("INSERT INTO items (id) VALUES (1)")
                    with pytest.raises(ReadOnlyTransactionError):
                        session.execute_many("INSERT INTO items (id) VALUES (@id)", [{"id": 1}])
                    with pytest.raises(SQLConversionError):
                        session.execute("CREATE TABLE items (id INT64)")
                    if mode == "cancelled":
                        raise RuntimeError("caller failure")
        if not managed:
            assert cast("Any", context)._selection_receipt.config is config
        raw = (
            registry.provide_read_connection(config, **explicit)
            if managed
            else config.provide_read_connection(**explicit)
        )
        if is_async:
            async with raw as connection:
                assert isinstance(connection, Snapshot)
        else:
            with raw as connection:
                assert isinstance(connection, Snapshot)
    expected = (
        {"multi_use": True}
        if mode in {"primary", "pinned"}
        else {"max_staleness": timedelta(seconds=8), "multi_use": False}
        if mode == "bounded"
        else {"max_staleness": timedelta(seconds=6), "multi_use": False}
        if mode == "context"
        else {"exact_staleness": timedelta(seconds=10), "multi_use": True}
    )
    assert snapshots == [expected, expected]
    assert exits == [error_type if mode == "cancelled" else None, None]
    assert lifecycle == (["start", "end"] if managed else [])
    if is_async:
        await config.close_pool()
    else:
        config.close_pool()
