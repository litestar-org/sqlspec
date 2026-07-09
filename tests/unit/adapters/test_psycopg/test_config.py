"""Psycopg configuration tests covering statement config builders."""

from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncCursor, Connection, Cursor
from psycopg.abc import AdaptContext
from psycopg.rows import dict_row

import sqlspec.adapters.psycopg.config as psycopg_config
from sqlspec.adapters.psycopg._typing import PsycopgAsyncSessionContext, PsycopgSyncSessionContext
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgPoolParams, PsycopgSyncConfig
from sqlspec.adapters.psycopg.core import (
    build_postgres_extension_probe_names,
    build_statement_config,
    default_statement_config,
    resolve_postgres_extension_state,
)
from sqlspec.core import SQL, StatementConfig


class _CapturedSyncPool:
    """Capture psycopg pool constructor arguments without opening a database connection."""

    calls: list[tuple[str, dict[str, Any]]] = []

    def __init__(self, conninfo: str = "", **kwargs: Any) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.calls.append((conninfo, kwargs))


class _CapturedAsyncPool:
    """Capture psycopg async pool constructor arguments without opening a database connection."""

    calls: list[tuple[str, dict[str, Any]]] = []
    open_calls: int = 0

    def __init__(self, conninfo: str = "", **kwargs: Any) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.calls.append((conninfo, kwargs))

    async def open(self) -> None:
        type(self).open_calls += 1


def _configure_sync(_: object) -> None:
    return None


def _check_sync(_: object) -> None:
    return None


def _reset_sync(_: object) -> None:
    return None


def _reconnect_failed_sync(_: object) -> None:
    return None


async def _configure_async(_: object) -> None:
    return None


async def _check_async(_: object) -> None:
    return None


async def _reset_async(_: object) -> None:
    return None


async def _reconnect_failed_async(_: object) -> None:
    return None


def test_build_default_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_build_default_statement_config_custom_deserializer() -> None:
    """Custom deserializer should propagate into the parameter configuration."""

    def deserializer(_: str) -> object:
        return {"deserialized": True}

    statement_config = build_statement_config(json_deserializer=deserializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_deserializer is deserializer


def test_psycopg_sync_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the sync Psycopg statement configuration."""

    def serializer(_: object) -> str:
        return "sync"

    config = PsycopgSyncConfig(driver_features={"json_serializer": serializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_psycopg_sync_config_applies_driver_feature_deserializer() -> None:
    """Driver features should mutate the sync Psycopg JSON deserializer."""

    def deserializer(_: str) -> object:
        return {"feature": True}

    config = PsycopgSyncConfig(driver_features={"json_deserializer": deserializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_deserializer is deserializer


def test_psycopg_build_postgres_extension_probe_names_filters_disabled_features() -> None:
    """Only enabled extension probes should be returned."""
    assert build_postgres_extension_probe_names({"enable_pgvector": True, "enable_paradedb": False}) == ["vector"]


def test_psycopg_resolve_postgres_extension_state_promotes_paradedb() -> None:
    """Detected extensions should promote the runtime dialect."""
    statement_config, pgvector_available, paradedb_available = resolve_postgres_extension_state(
        StatementConfig(dialect="postgres"), {"enable_pgvector": True, "enable_paradedb": True}, {"vector", "pg_search"}
    )

    assert statement_config.dialect == "paradedb"
    assert pgvector_available is True
    assert paradedb_available is True


def test_psycopg_numeric_placeholders_convert_to_pyformat() -> None:
    """Numeric placeholders should be rewritten for psycopg execution."""

    statement = SQL(
        "SELECT * FROM bridge_validation WHERE label IN ($1, $2, $3)",
        "alpha",
        "beta",
        "gamma",
        statement_config=default_statement_config,
    )
    compiled_sql, parameters = statement.compile()

    assert "$1" not in compiled_sql
    assert compiled_sql.count("%s") == 3
    assert parameters == ["alpha", "beta", "gamma"]


def test_psycopg_sync_pool_preserves_conninfo_with_explicit_connection_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sync pool creation should pass conninfo and explicit connection kwargs together."""
    context = cast("AdaptContext", object())
    connection_config: PsycopgPoolParams = {
        "conninfo": "postgresql://user:pass@localhost/db",
        "user": "explicit_user",
        "prepare_threshold": 0,
        "context": context,
        "row_factory": dict_row,
        "cursor_factory": Cursor,
    }

    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    PsycopgSyncConfig(connection_config=connection_config)._create_pool()  # pyright: ignore[reportPrivateUsage]

    conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    assert conninfo == "postgresql://user:pass@localhost/db"
    assert pool_kwargs["kwargs"] == {
        "user": "explicit_user",
        "prepare_threshold": 0,
        "context": context,
        "row_factory": dict_row,
        "cursor_factory": Cursor,
    }


def test_psycopg_sync_minimal_pool_omits_prepare_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal sync pool config should not inject prepare_threshold."""
    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    PsycopgSyncConfig(connection_config={"conninfo": "postgresql://user:pass@localhost/db"})._create_pool()  # pyright: ignore[reportPrivateUsage]

    conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    assert conninfo == "postgresql://user:pass@localhost/db"
    assert pool_kwargs["kwargs"] == {}


@pytest.mark.anyio
async def test_psycopg_async_minimal_pool_omits_prepare_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal async pool config should not inject prepare_threshold."""
    _CapturedAsyncPool.calls.clear()
    _CapturedAsyncPool.open_calls = 0
    monkeypatch.setattr(psycopg_config, "AsyncConnectionPool", _CapturedAsyncPool)

    await PsycopgAsyncConfig(connection_config={"conninfo": "postgresql://user:pass@localhost/db"})._create_pool()  # pyright: ignore[reportPrivateUsage]

    conninfo, pool_kwargs = _CapturedAsyncPool.calls[-1]
    assert conninfo == "postgresql://user:pass@localhost/db"
    assert pool_kwargs["kwargs"] == {}


@pytest.mark.parametrize(
    ("open_value", "expect_open_call"), [({}, True), ({"open": True}, True), ({"open": False}, False)]
)
async def test_psycopg_async_pool_always_constructs_closed(
    monkeypatch: pytest.MonkeyPatch, open_value: "dict[str, Any]", expect_open_call: bool
) -> None:
    """The async pool is always constructed with open=False; pool.open() is awaited only when open resolves truthy."""
    _CapturedAsyncPool.calls.clear()
    _CapturedAsyncPool.open_calls = 0
    monkeypatch.setattr(psycopg_config, "AsyncConnectionPool", _CapturedAsyncPool)

    connection_config = {"conninfo": "postgresql://user:pass@localhost/db", **open_value}
    await PsycopgAsyncConfig(connection_config=cast("Any", connection_config))._create_pool()  # pyright: ignore[reportPrivateUsage]

    _, pool_kwargs = _CapturedAsyncPool.calls[-1]
    assert pool_kwargs["open"] is False
    assert _CapturedAsyncPool.open_calls == (1 if expect_open_call else 0)


@pytest.mark.anyio
async def test_psycopg_async_pool_preserves_conninfo_with_explicit_connection_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Async pool creation should pass conninfo and explicit connection kwargs together."""
    context = cast("AdaptContext", object())
    connection_config: PsycopgPoolParams = {
        "conninfo": "postgresql://user:pass@localhost/db",
        "user": "explicit_user",
        "prepare_threshold": None,
        "context": context,
        "row_factory": dict_row,
        "cursor_factory": AsyncCursor,
    }

    _CapturedAsyncPool.calls.clear()
    _CapturedAsyncPool.open_calls = 0
    monkeypatch.setattr(psycopg_config, "AsyncConnectionPool", _CapturedAsyncPool)

    await PsycopgAsyncConfig(connection_config=connection_config)._create_pool()  # pyright: ignore[reportPrivateUsage]

    conninfo, pool_kwargs = _CapturedAsyncPool.calls[-1]
    assert conninfo == "postgresql://user:pass@localhost/db"
    assert pool_kwargs["kwargs"] == {
        "user": "explicit_user",
        "prepare_threshold": None,
        "context": context,
        "row_factory": dict_row,
        "cursor_factory": AsyncCursor,
    }
    assert _CapturedAsyncPool.open_calls == 1


def test_psycopg_sync_pool_forwards_lifecycle_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sync pool creation should route psycopg-pool lifecycle options to the pool."""
    connection_config: PsycopgPoolParams = {
        "dbname": "app",
        "connection_class": Connection,
        "configure": _configure_sync,
        "check": _check_sync,
        "reset": _reset_sync,
        "close_returns": True,
        "reconnect_failed": _reconnect_failed_sync,
        "open": False,
    }

    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    PsycopgSyncConfig(connection_config=connection_config)._create_pool()  # pyright: ignore[reportPrivateUsage]

    _conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    assert pool_kwargs["kwargs"] == {"dbname": "app"}
    assert pool_kwargs["connection_class"] is Connection
    assert pool_kwargs["configure"] is _configure_sync
    assert pool_kwargs["check"] is _check_sync
    assert pool_kwargs["reset"] is _reset_sync
    assert pool_kwargs["close_returns"] is True
    assert pool_kwargs["reconnect_failed"] is _reconnect_failed_sync
    assert pool_kwargs["open"] is False


@pytest.mark.anyio
async def test_psycopg_async_pool_forwards_lifecycle_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async pool creation should route psycopg-pool lifecycle options to the pool."""
    connection_config: PsycopgPoolParams = {
        "dbname": "app",
        "configure": _configure_async,
        "check": _check_async,
        "reset": _reset_async,
        "close_returns": True,
        "reconnect_failed": _reconnect_failed_async,
        "open": False,
    }

    _CapturedAsyncPool.calls.clear()
    _CapturedAsyncPool.open_calls = 0
    monkeypatch.setattr(psycopg_config, "AsyncConnectionPool", _CapturedAsyncPool)

    await PsycopgAsyncConfig(connection_config=connection_config)._create_pool()  # pyright: ignore[reportPrivateUsage]

    _conninfo, pool_kwargs = _CapturedAsyncPool.calls[-1]
    assert pool_kwargs["kwargs"] == {"dbname": "app"}
    assert pool_kwargs["configure"] is _configure_async
    assert pool_kwargs["check"] is _check_async
    assert pool_kwargs["reset"] is _reset_async
    assert pool_kwargs["close_returns"] is True
    assert pool_kwargs["reconnect_failed"] is _reconnect_failed_async
    assert pool_kwargs["open"] is False
    assert _CapturedAsyncPool.open_calls == 0


def test_psycopg_sync_session_context_resolves_callable_statement_config() -> None:
    """Sync session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = PsycopgSyncSessionContext(
        acquire_connection=lambda: object(),
        release_connection=lambda _conn: None,
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_psycopg_async_session_context_resolves_callable_statement_config() -> None:
    """Async session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = PsycopgAsyncSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


def test_psycopg_sync_session_context_preserves_explicit_statement_config() -> None:
    """Explicit StatementConfig should be used directly without calling."""
    explicit_config = StatementConfig(dialect="postgres")
    context = PsycopgSyncSessionContext(
        acquire_connection=lambda: object(),
        release_connection=lambda _conn: None,
        statement_config=explicit_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as driver:
        assert driver.statement_config is explicit_config


def test_psycopg_sync_provide_session_tracks_promoted_statement_config() -> None:
    """Sync runtime statement config should resolve the current config dialect lazily."""
    config = PsycopgSyncConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"


def test_psycopg_async_provide_session_tracks_promoted_statement_config() -> None:
    """Async runtime statement config should resolve the current config dialect lazily."""
    config = PsycopgAsyncConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"
