"""Psycopg configuration tests covering statement config builders."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import psycopg
import psycopg_pool
import pytest
from psycopg import AsyncCursor, Connection, Cursor, pq
from psycopg import Error as PsycopgError
from psycopg.abc import AdaptContext
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

import sqlspec.adapters.psycopg.config as psycopg_config
from sqlspec.adapters.psycopg._typing import PsycopgAsyncSessionContext, PsycopgSyncSessionContext
from sqlspec.adapters.psycopg.config import (
    PsycopgAsyncConfig,
    PsycopgConnectionParams,
    PsycopgDriverFeatures,
    PsycopgPoolParams,
    PsycopgSyncConfig,
    build_connection_config,
)
from sqlspec.adapters.psycopg.core import build_statement_config, default_statement_config
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.core import SQL, StatementConfig
from sqlspec.core.config_runtime import build_postgres_extension_probe_names, resolve_postgres_extension_state
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError


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


def test_psycopg_sync_bm25_probe_is_cached_and_does_not_raise_from_pool_callback() -> None:
    """The sync pool callback caches missing pg_textsearch without raising."""
    connection = MagicMock()
    connection.autocommit = True
    connection.execute.return_value.fetchall.return_value = [("pg_search",)]
    config = PsycopgSyncConfig(
        driver_features={"enable_pgvector": False, "enable_paradedb": True},
        extension_config={"adk": {"enable_bm25": True}},
    )

    config._configure_connection(connection)  # pyright: ignore[reportPrivateUsage]
    config._configure_connection(connection)  # pyright: ignore[reportPrivateUsage]

    connection.execute.assert_called_once()
    assert connection.execute.call_args.args[1] == (["pg_search", "pg_textsearch"],)
    with pytest.raises(ImproperConfigurationError, match="requires the pg_textsearch"):
        config._ensure_pg_textsearch_available()  # pyright: ignore[reportPrivateUsage]


async def test_psycopg_async_bm25_probe_failure_is_preserved_without_callback_failure() -> None:
    """The async pool callback records a failed probe and exposes it after acquisition."""
    probe_error = RuntimeError("probe failed")
    connection = AsyncMock()
    connection.autocommit = True
    connection.execute.side_effect = probe_error
    config = PsycopgAsyncConfig(
        driver_features={"enable_pgvector": False, "enable_paradedb": False},
        extension_config={"adk": {"enable_bm25": True}},
    )

    await config._configure_async_connection(connection)  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(ImproperConfigurationError) as exc_info:
        config._ensure_pg_textsearch_available()  # pyright: ignore[reportPrivateUsage]
    assert exc_info.value.__cause__ is probe_error


def test_psycopg_pg_textsearch_available_property() -> None:
    """pg_textsearch_available reflects detected extension state."""
    connection = MagicMock()
    connection.autocommit = True
    connection.execute.return_value.fetchall.return_value = [("pg_textsearch",)]
    config = PsycopgSyncConfig(driver_features={"enable_pgvector": False, "enable_paradedb": False})

    assert config.pg_textsearch_available is False
    config._configure_connection(connection)  # pyright: ignore[reportPrivateUsage]
    assert config.pg_textsearch_available is True
    assert config.statement_config.dialect == "pg_textsearch"


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
    """Sync pool creation should route psycopg-pool lifecycle options to the pool and chain custom configure."""
    seen: list[str] = []

    def custom_configure(_: object) -> None:
        seen.append("user")

    connection_config: PsycopgPoolParams = {
        "dbname": "app",
        "connection_class": Connection,
        "configure": custom_configure,
        "check": _check_sync,
        "reset": _reset_sync,
        "close_returns": True,
        "reconnect_failed": _reconnect_failed_sync,
        "open": False,
    }

    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    config = PsycopgSyncConfig(
        connection_config=connection_config, driver_features={"on_connection_create": lambda _: seen.append("internal")}
    )
    config._create_pool()  # pyright: ignore[reportPrivateUsage]

    _conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    assert pool_kwargs["kwargs"] == {"dbname": "app"}
    assert pool_kwargs["connection_class"] is Connection
    assert callable(pool_kwargs["configure"])
    conn = MagicMock()
    conn.autocommit = True
    conn.execute.return_value.fetchall.return_value = []
    pool_kwargs["configure"](conn)
    assert seen == ["internal", "user"]
    assert pool_kwargs["check"] is _check_sync
    assert pool_kwargs["reset"] is _reset_sync
    assert pool_kwargs["close_returns"] is True
    assert pool_kwargs["reconnect_failed"] is _reconnect_failed_sync
    assert pool_kwargs["open"] is False


@pytest.mark.anyio
async def test_psycopg_async_pool_forwards_lifecycle_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async pool creation should route psycopg-pool lifecycle options to the pool and chain custom configure."""
    seen: list[str] = []

    async def custom_configure(_: object) -> None:
        seen.append("user")

    async def internal_hook(_: object) -> None:
        seen.append("internal")

    connection_config: PsycopgPoolParams = {
        "dbname": "app",
        "configure": custom_configure,
        "check": _check_async,
        "reset": _reset_async,
        "close_returns": True,
        "reconnect_failed": _reconnect_failed_async,
        "open": False,
    }

    _CapturedAsyncPool.calls.clear()
    _CapturedAsyncPool.open_calls = 0
    monkeypatch.setattr(psycopg_config, "AsyncConnectionPool", _CapturedAsyncPool)

    config = PsycopgAsyncConfig(
        connection_config=connection_config, driver_features={"on_connection_create": internal_hook}
    )
    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

    _conninfo, pool_kwargs = _CapturedAsyncPool.calls[-1]
    assert pool_kwargs["kwargs"] == {"dbname": "app"}
    assert callable(pool_kwargs["configure"])
    conn = AsyncMock()
    conn.autocommit = True
    cursor = MagicMock()
    cursor.fetchall = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=cursor)
    configure_fn = cast("Any", pool_kwargs["configure"])
    await configure_fn(conn)
    assert seen == ["internal", "user"]
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
        release_connection=lambda _conn, **_kwargs: None,
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
        release_connection=lambda _conn, **_kwargs: None,
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


def test_sync_create_connection_does_not_create_a_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """A standalone connection must not build or consume the pool."""
    sentinel = MagicMock()
    connect = MagicMock(return_value=sentinel)
    monkeypatch.setattr("psycopg.Connection.connect", connect)
    config = PsycopgSyncConfig(connection_config={"host": "localhost", "min_size": 1, "max_size": 1})

    connection = config.create_connection()

    assert connection is sentinel
    assert config.connection_instance is None
    assert "min_size" not in connect.call_args.kwargs
    assert "max_size" not in connect.call_args.kwargs
    assert "timeout" not in connect.call_args.kwargs


def test_sync_create_connection_applies_the_connection_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pool applies on_connection_create through configure; the standalone path must too."""
    seen: list[object] = []
    sentinel = MagicMock()
    monkeypatch.setattr("psycopg.Connection.connect", MagicMock(return_value=sentinel))
    config = PsycopgSyncConfig(
        connection_config={"host": "localhost"}, driver_features={"on_connection_create": seen.append}
    )

    config.create_connection()

    assert seen == [sentinel]


class _RecordingPoolCtx:
    """Stands in for the psycopg pool's connection context manager."""

    def __init__(self, released: "list[tuple[object, object, object]]") -> None:
        self._released = released

    def __enter__(self) -> object:
        return object()

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self._released.append((exc_type, exc_val, exc_tb))


def _pooled_config(
    monkeypatch: pytest.MonkeyPatch, released: "list[tuple[object, object, object]]"
) -> PsycopgSyncConfig:
    class _Pool:
        def connection(self) -> _RecordingPoolCtx:
            return _RecordingPoolCtx(released)

    config = PsycopgSyncConfig(connection_config={"host": "localhost"})
    monkeypatch.setattr(PsycopgSyncConfig, "provide_pool", lambda _self, *_a, **_k: _Pool())
    monkeypatch.setattr("psycopg.Connection.connect", MagicMock(side_effect=AssertionError("must not bypass the pool")))
    return config


def test_sync_connection_context_uses_the_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """provide_connection must go through the pool, not open an uncounted connection."""
    released: list[tuple[object, object, object]] = []
    config = _pooled_config(monkeypatch, released)

    with config.provide_connection():
        pass

    assert released == [(None, None, None)]


def test_sync_connection_context_forwards_the_exception_to_the_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pool needs the exception so it can roll back or discard the connection."""
    released: list[tuple[object, object, object]] = []
    config = _pooled_config(monkeypatch, released)

    with pytest.raises(RuntimeError), config.provide_connection():
        raise RuntimeError

    exc_type, exc_val, _ = released[0]
    assert exc_type is RuntimeError
    assert isinstance(exc_val, RuntimeError)


def test_sync_session_uses_the_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """provide_session must go through the pool so connections stay counted."""
    released: list[tuple[object, object, object]] = []
    config = _pooled_config(monkeypatch, released)
    handler = psycopg_config._PsycopgSyncSessionConnectionHandler(config)

    connection = handler.acquire_connection()
    handler.release_connection(connection)

    assert released == [(None, None, None)]


def test_sync_session_forwards_the_exception_to_the_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed session must tell the pool so it can roll back or discard."""
    released: list[tuple[object, object, object]] = []
    config = _pooled_config(monkeypatch, released)
    handler = psycopg_config._PsycopgSyncSessionConnectionHandler(config)
    error = RuntimeError("boom")

    connection = handler.acquire_connection()
    handler.release_connection(connection, exc_type=RuntimeError, exc_val=error, exc_tb=None)

    assert released == [(RuntimeError, error, None)]


def test_build_connection_config_resolves_aliases() -> None:
    """build_connection_config should map dsn/url to conninfo, database/db to dbname, and username to user."""
    cfg = build_connection_config({
        "url": "postgresql://usr:pwd@host.internal:5432/main",
        "database": "override_db",
        "username": "override_usr",
    })
    assert cfg["conninfo"] == "postgresql://usr:pwd@host.internal:5432/main"
    assert cfg["dbname"] == "override_db"
    assert cfg["user"] == "override_usr"
    assert "url" not in cfg
    assert "database" not in cfg
    assert "username" not in cfg


def test_build_connection_config_resolves_db_and_dsn() -> None:
    """build_connection_config should resolve db and dsn aliases and remove them from output."""
    cfg = build_connection_config({"dsn": "postgresql://localhost/test", "db": "mydb"})
    assert cfg["conninfo"] == "postgresql://localhost/test"
    assert cfg["dbname"] == "mydb"
    assert "dsn" not in cfg
    assert "db" not in cfg


def test_sync_config_normalizes_aliases() -> None:
    """PsycopgSyncConfig should normalize aliases in connection_config."""
    config = PsycopgSyncConfig(connection_config={"dsn": "postgresql://localhost:5432/initial", "database": "override"})
    assert config.connection_config["conninfo"] == "postgresql://localhost:5432/initial"
    assert config.connection_config["dbname"] == "override"
    assert "dsn" not in config.connection_config
    assert "database" not in config.connection_config


def test_async_config_normalizes_aliases() -> None:
    """PsycopgAsyncConfig should normalize aliases in connection_config."""
    config = PsycopgAsyncConfig(
        connection_config={"connection_string": "postgresql://localhost:5432/initial", "db": "override"}
    )
    assert config.connection_config["conninfo"] == "postgresql://localhost:5432/initial"
    assert config.connection_config["dbname"] == "override"
    assert "connection_string" not in config.connection_config
    assert "db" not in config.connection_config


def test_psycopg_sync_configure_connection_commits_hook_after_extension_probe() -> None:
    """The probe and SQL hook both leave a non-autocommit connection idle."""
    events: list[str] = []
    connection = MagicMock()
    connection.autocommit = False
    connection.execute.return_value.fetchall.return_value = [{"extname": "pg_textsearch"}]
    connection.rollback.side_effect = lambda: events.append("rollback")
    connection.commit.side_effect = lambda: events.append("commit")

    config = PsycopgSyncConfig(
        driver_features={
            "enable_pgvector": False,
            "enable_paradedb": False,
            "enable_pg_textsearch": True,
            "on_connection_create": lambda _: events.append("hook"),
        }
    )

    config._configure_connection(connection)  # pyright: ignore[reportPrivateUsage]

    assert config.pg_textsearch_available is True
    assert config.statement_config.dialect == "pg_textsearch"
    assert events == ["rollback", "hook", "commit"]


@pytest.mark.anyio
async def test_psycopg_async_configure_connection_commits_hook_after_extension_probe() -> None:
    """The probe and SQL hook both leave a non-autocommit connection idle."""
    events: list[str] = []
    connection = AsyncMock()
    connection.autocommit = False
    cursor = MagicMock()
    cursor.fetchall = AsyncMock(return_value=[{"extname": "pg_textsearch"}])
    connection.execute = AsyncMock(return_value=cursor)

    async def record_rollback() -> None:
        events.append("rollback")

    async def record_hook(_: object) -> None:
        events.append("hook")

    connection.rollback = AsyncMock(side_effect=record_rollback)
    connection.commit = AsyncMock(side_effect=lambda: events.append("commit"))

    config = PsycopgAsyncConfig(
        driver_features={
            "enable_pgvector": False,
            "enable_paradedb": False,
            "enable_pg_textsearch": True,
            "on_connection_create": record_hook,
        }
    )

    await config._configure_async_connection(connection)  # pyright: ignore[reportPrivateUsage]

    assert config.pg_textsearch_available is True
    assert config.statement_config.dialect == "pg_textsearch"
    assert events == ["rollback", "hook", "commit"]


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("failure_phase", ["hook", "commit"])
async def test_failed_connection_initialization_rolls_back(is_async: bool, failure_phase: str) -> None:
    """A failed hook or setup commit rolls its transaction back before propagating the error."""
    connection = AsyncMock() if is_async else MagicMock()
    connection.autocommit = False
    native_error = RuntimeError("initialization failed")
    hook = AsyncMock() if is_async else MagicMock()
    if failure_phase == "hook":
        hook.side_effect = native_error
    else:
        connection.commit.side_effect = native_error
    config = (
        PsycopgAsyncConfig(driver_features={"on_connection_create": hook})
        if is_async
        else PsycopgSyncConfig(driver_features={"on_connection_create": hook})
    )
    config._pgvector_available = False  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(RuntimeError, match="initialization failed"):
        if isinstance(config, PsycopgAsyncConfig):
            await config._configure_async_connection(connection)  # pyright: ignore[reportPrivateUsage]
        else:
            config._configure_connection(connection)  # pyright: ignore[reportPrivateUsage]
    assert connection.rollback.call_count == 1


@pytest.mark.parametrize("is_async", [False, True])
async def test_direct_config_preserves_connection_initialization_hook(is_async: bool) -> None:
    """Bypassing a pooler keeps the configured physical-connection initialization."""
    hook = AsyncMock() if is_async else MagicMock()
    config = (
        PsycopgAsyncConfig(
            driver_features={"direct_dsn": "postgresql://localhost/direct", "on_connection_create": hook}
        )
        if is_async
        else PsycopgSyncConfig(
            driver_features={"direct_dsn": "postgresql://localhost/direct", "on_connection_create": hook}
        )
    )
    direct = config.derive_direct_config()
    connection = AsyncMock() if is_async else MagicMock()
    connection.autocommit = True
    if isinstance(direct, PsycopgAsyncConfig):
        await direct._configure_async_connection(connection)  # pyright: ignore[reportPrivateUsage]
    else:
        direct._configure_connection(connection)  # pyright: ignore[reportPrivateUsage]
    hook.assert_called_once_with(connection)


def test_psycopg_pooler_mode_transaction_sets_prepare_threshold_none_and_alloydb_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """pooler_mode in {'transaction', 'statement'} sets prepare_threshold=None on sync/async configs and AlloyDB connection class."""
    sync_config = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://localhost/db"}, driver_features={"pooler_mode": "transaction"}
    )
    assert sync_config.connection_config["prepare_threshold"] is None
    assert sync_config.driver_features["pooler_mode"] == "transaction"

    async_config = PsycopgAsyncConfig(
        connection_config={"conninfo": "postgresql://localhost/db"}, driver_features={"pooler_mode": "statement"}
    )
    assert async_config.connection_config["prepare_threshold"] is None
    assert async_config.driver_features["pooler_mode"] == "statement"

    assert "pgbouncer" not in PsycopgDriverFeatures.__annotations__
    assert "pgbouncer_mode" not in PsycopgDriverFeatures.__annotations__
    assert "null_pool" not in PsycopgDriverFeatures.__annotations__
    assert "null_pool" not in PsycopgConnectionParams.__annotations__
    assert "enable_pooling" in PsycopgDriverFeatures.__annotations__

    mock_connector = MagicMock()
    expected_conn = MagicMock()
    expected_conn.prepare_threshold = 5
    mock_connector.connect.return_value = expected_conn
    monkeypatch.setattr(psycopg_config, "ALLOYDB_CONNECTOR_INSTALLED", True)
    monkeypatch.setattr(psycopg_config._psycopg_typing, "PsycopgAlloydbConnector", lambda: mock_connector)
    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    alloydb_config = PsycopgSyncConfig(
        connection_config={"user": "u", "password": "p", "dbname": "app"},
        driver_features={
            "enable_alloydb": True,
            "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
            "pooler_mode": "transaction",
        },
    )
    alloydb_config._create_pool()  # pyright: ignore[reportPrivateUsage]

    _conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    connection_class = pool_kwargs["connection_class"]
    assert connection_class.default_prepare_threshold is None
    conn = connection_class.connect("ignored")
    assert conn.prepare_threshold is None


@pytest.mark.anyio
async def test_psycopg_enable_alloydb_columnar_and_derive_direct_config() -> None:
    """enable_alloydb_columnar configures GUC by pooler_mode and derive_direct_config builds a session config."""
    session_sync = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://localhost/db", "options": "-c statement_timeout=5000"},
        driver_features={"enable_alloydb_columnar": True, "pooler_mode": "session"},
    )
    assert "-c statement_timeout=5000" in session_sync.connection_config["options"]
    assert "-c google_columnar_engine.enable_columnar_scan=on" in session_sync.connection_config["options"]
    assert session_sync.derive_direct_config() is session_sync

    auto_sync = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://localhost/db"},
        driver_features={"enable_alloydb_columnar": True, "pooler_mode": "auto"},
    )
    assert "-c google_columnar_engine.enable_columnar_scan=on" in auto_sync.connection_config["options"]

    tx_sync = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://pooler:6432/db"},
        driver_features={"enable_alloydb_columnar": True, "pooler_mode": "transaction"},
    )
    assert tx_sync.driver_features["pooler_mode"] == "transaction"
    assert "options" not in tx_sync.connection_config
    assert tx_sync.connection_config["prepare_threshold"] is None

    with pytest.raises(ImproperConfigurationError, match="Invalid pooler_mode"):
        PsycopgSyncConfig(
            connection_config={"conninfo": "postgresql://localhost/db"},
            driver_features=cast("Any", {"pooler_mode": "invalid"}),
        )

    with pytest.raises(ImproperConfigurationError, match="Invalid pooler_mode"):
        PsycopgAsyncConfig(
            connection_config={"conninfo": "postgresql://localhost/db"},
            driver_features=cast("Any", {"pooler_mode": "invalid"}),
        )

    txn_sync = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://pooler:6432/db"},
        driver_features={
            "enable_alloydb_columnar": True,
            "pooler_mode": "transaction",
            "direct_dsn": "postgresql://direct:5432/db",
        },
    )
    assert "options" not in txn_sync.connection_config
    assert txn_sync.connection_config["prepare_threshold"] is None

    derived_sync = txn_sync.derive_direct_config()
    assert derived_sync is not txn_sync
    assert derived_sync.connection_config["conninfo"] == "postgresql://direct:5432/db"
    assert derived_sync.driver_features["pooler_mode"] == "session"
    assert derived_sync.driver_features["direct_dsn"] is None
    assert "prepare_threshold" not in derived_sync.connection_config
    assert "-c google_columnar_engine.enable_columnar_scan=on" in derived_sync.connection_config["options"]

    txn_async = PsycopgAsyncConfig(
        connection_config={"conninfo": "postgresql://pooler:6432/db"},
        driver_features={
            "enable_alloydb_columnar": True,
            "pooler_mode": "transaction",
            "direct_dsn": "postgresql://direct:5432/db",
        },
    )
    derived_async = txn_async.derive_direct_config()
    assert derived_async is not txn_async
    assert derived_async.connection_config["conninfo"] == "postgresql://direct:5432/db"
    assert derived_async.driver_features["pooler_mode"] == "session"
    assert derived_async.driver_features["direct_dsn"] is None
    assert "prepare_threshold" not in derived_async.connection_config
    assert "-c google_columnar_engine.enable_columnar_scan=on" in derived_async.connection_config["options"]

    sync_conn = MagicMock()
    sync_conn.autocommit = True
    sync_conn.info = SimpleNamespace(transaction_status=0)
    sync_driver = PsycopgSyncDriver(
        connection=sync_conn, statement_config=txn_sync.statement_config, driver_features=txn_sync.driver_features
    )
    sync_driver.begin()
    sync_conn.execute.assert_called_once_with("SET LOCAL google_columnar_engine.enable_columnar_scan = 'on'")

    async_conn = AsyncMock()
    async_conn.autocommit = True
    async_conn.info = SimpleNamespace(transaction_status=0)
    async_driver = PsycopgAsyncDriver(
        connection=async_conn, statement_config=txn_async.statement_config, driver_features=txn_async.driver_features
    )
    await async_driver.begin()
    async_conn.execute.assert_awaited_once_with("SET LOCAL google_columnar_engine.enable_columnar_scan = 'on'")


def test_psycopg_driver_features_enable_pgcopy_binary() -> None:
    """PsycopgDriverFeatures accepts enable_pgcopy_binary and apply_driver_features provides default."""
    from sqlspec.adapters.psycopg.config import PsycopgDriverFeatures
    from sqlspec.adapters.psycopg.core import apply_driver_features

    features: PsycopgDriverFeatures = {"enable_pgcopy_binary": True}
    assert features["enable_pgcopy_binary"] is True

    _, resolved = apply_driver_features(StatementConfig(), {})
    assert resolved["enable_pgcopy_binary"] is False


def test_psycopg_connection_type_aliases() -> None:
    """PsycopgSyncConnection and PsycopgAsyncConnection type aliases allow generic connections."""
    from sqlspec.adapters.psycopg._typing import PsycopgAsyncConnection, PsycopgSyncConnection

    assert PsycopgSyncConnection is not None
    assert PsycopgAsyncConnection is not None


@pytest.mark.parametrize("config_type", [PsycopgSyncConfig, PsycopgAsyncConfig])
@pytest.mark.parametrize("strategy", ["default", "rls", True])
def test_tenant_shorthand_defaults_to_session(config_type: Any, strategy: Any) -> None:
    from sqlspec.adapters.psycopg import PsycopgAsyncRLSStrategy, PsycopgSyncRLSStrategy

    config = config_type(driver_features={"tenant_strategy": strategy})
    expected = PsycopgSyncRLSStrategy if config_type is PsycopgSyncConfig else PsycopgAsyncRLSStrategy
    assert isinstance(config.driver_features["tenant_strategy"], expected)
    assert config.driver_features["pooler_mode"] == "session"


@pytest.mark.parametrize("config_type", [PsycopgSyncConfig, PsycopgAsyncConfig])
@pytest.mark.parametrize("mode", ["transaction", "statement", "auto"])
def test_tenant_rejects_non_session_pooler(config_type: Any, mode: str) -> None:
    with pytest.raises(ImproperConfigurationError, match="session"):
        config_type(driver_features={"tenant_strategy": "rls", "pooler_mode": mode})


@pytest.mark.parametrize("config_type", [PsycopgSyncConfig, PsycopgAsyncConfig])
@pytest.mark.parametrize("source", ["config", "pool"])
def test_tenant_rejects_close_returns(config_type: Any, source: str) -> None:
    from types import SimpleNamespace

    with pytest.raises(ImproperConfigurationError, match="close_returns"):
        config_type(
            connection_config={"close_returns": True} if source == "config" else {},
            connection_instance=SimpleNamespace(close_returns=True) if source == "pool" else None,
            driver_features={"tenant_strategy": "rls"},
        )


@pytest.mark.parametrize("config_type", [PsycopgSyncConfig, PsycopgAsyncConfig])
@pytest.mark.parametrize("disabled", [None, False])
def test_tenant_disabled_retains_pooler_mode(config_type: Any, disabled: Any) -> None:
    config = config_type(driver_features={"tenant_strategy": disabled, "pooler_mode": "transaction"})
    assert config.driver_features["tenant_strategy"] is None
    assert config.driver_features["pooler_mode"] == "transaction"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("provider", ["provide_connection", "provide_session"])
async def test_tenant_public_providers_capture_and_release(asynchronous: bool, provider: str) -> None:
    from dataclasses import dataclass
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from psycopg import pq

    from sqlspec.core.tenant import TenantLeaseState
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    calls: list[str] = []

    @dataclass
    class SyncStrategy:
        label: str

        def checkout(self, _connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
            calls.append(f"checkout:{payload.tenant_id}")
            return TenantLeaseState(payload)

        def checkin(self, _connection: Any, state: TenantLeaseState) -> None:
            calls.append(f"checkin:{state.payload.tenant_id}")

    @dataclass
    class AsyncStrategy:
        label: str

        async def checkout(self, _connection: Any, payload: TenantContextPayload) -> TenantLeaseState:
            calls.append(f"checkout:{payload.tenant_id}")
            return TenantLeaseState(payload)

        async def checkin(self, _connection: Any, state: TenantLeaseState) -> None:
            calls.append(f"checkin:{state.payload.tenant_id}")

    connection = AsyncMock() if asynchronous else MagicMock()
    connection.info = SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE)
    connection.closed = False
    context = AsyncMock() if asynchronous else MagicMock()
    if asynchronous:
        context.__aenter__.return_value = connection
        context.__aexit__.return_value = None
    else:
        context.__enter__.return_value = connection
        context.__exit__.return_value = None
    pool = MagicMock()
    pool.close_returns = False
    pool.connection.return_value = context
    strategy = AsyncStrategy("custom") if asynchronous else SyncStrategy("custom")
    config_type = PsycopgAsyncConfig if asynchronous else PsycopgSyncConfig
    config = config_type(connection_instance=pool, driver_features={"tenant_strategy": strategy})
    assert config.driver_features["tenant_strategy"] is strategy
    with TenantContext.scope("a"):
        if asynchronous:
            async with getattr(config, provider)() as value:
                assert calls == ["checkout:a"]
                assert (value if provider == "provide_connection" else value.connection) is connection
        else:
            with getattr(config, provider)() as value:
                assert calls == ["checkout:a"]
                assert (value if provider == "provide_connection" else value.connection) is connection
    assert calls == ["checkout:a", "checkin:a"]
    if asynchronous:
        context.__aexit__.assert_awaited_once()
    else:
        context.__exit__.assert_called_once()


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("provider", ["provide_connection", "provide_session"])
async def test_tenant_public_providers_fail_before_acquisition(asynchronous: bool, provider: str) -> None:
    from unittest.mock import MagicMock

    from sqlspec.exceptions import TenantContextError

    pool = MagicMock()
    pool.close_returns = False
    config_type = PsycopgAsyncConfig if asynchronous else PsycopgSyncConfig
    config = config_type(connection_instance=pool, driver_features={"tenant_strategy": "rls"})
    with pytest.raises(TenantContextError):
        if asynchronous:
            async with getattr(config, provider)():
                pass
        else:
            with getattr(config, provider)():
                pass
    pool.connection.assert_not_called()


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_standalone_connection_is_rejected(asynchronous: bool) -> None:
    config_type = PsycopgAsyncConfig if asynchronous else PsycopgSyncConfig
    config = config_type(driver_features={"tenant_strategy": "rls"})
    with pytest.raises(ImproperConfigurationError, match="lease"):
        if asynchronous:
            await cast("PsycopgAsyncConfig", config).create_connection()
        else:
            cast("PsycopgSyncConfig", config).create_connection()


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_tenant_prepare_failure_releases_native_context(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from psycopg import pq

    from sqlspec.core.tenant import TenantLeaseState
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload

    payload = TenantContextPayload("a")
    strategy = AsyncMock() if asynchronous else MagicMock()
    strategy.checkout.return_value = TenantLeaseState(payload)
    connection = AsyncMock() if asynchronous else MagicMock()
    connection.info = SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE)
    connection.closed = False
    context = AsyncMock() if asynchronous else MagicMock()
    if asynchronous:
        context.__aenter__.return_value = connection
    else:
        context.__enter__.return_value = connection
    pool = MagicMock()
    pool.close_returns = False
    pool.connection.return_value = context
    config_type = PsycopgAsyncConfig if asynchronous else PsycopgSyncConfig
    config = config_type(connection_instance=pool, driver_features={"tenant_strategy": strategy})
    error = RuntimeError("prepare failure")

    def fail_prepare(_driver: Any) -> Any:
        raise error

    monkeypatch.setattr(config, "_prepare_driver", fail_prepare)
    with TenantContext.scope(payload), pytest.raises(RuntimeError) as caught:
        if asynchronous:
            async with cast("PsycopgAsyncConfig", config).provide_session():
                pass
        else:
            with cast("PsycopgSyncConfig", config).provide_session():
                pass
    assert caught.value is error
    if asynchronous:
        strategy.checkin.assert_awaited_once()
        connection.rollback.assert_awaited_once()
        context.__aexit__.assert_awaited_once()
    else:
        strategy.checkin.assert_called_once()
        connection.rollback.assert_called_once()
        context.__exit__.assert_called_once()


@pytest.mark.parametrize("config_type", (PsycopgSyncConfig, PsycopgAsyncConfig))
@pytest.mark.parametrize("explicit_threshold", (False, True))
def test_replica_derivation_preserves_explicit_prepare_threshold(config_type: Any, explicit_threshold: bool) -> None:
    connection: dict[str, Any] = {"conninfo": "postgresql://app:secret@writer/orders?sslmode=require"}
    if explicit_threshold:
        connection["prepare_threshold"] = None
    primary = config_type(connection_config=connection, driver_features={"pooler_mode": "transaction"})
    replica = primary.derive_pool(host="reader-a,reader-b", port="5432,5433", pooler_mode="session")
    assert replica.connection_config["conninfo"] == "postgresql://app:secret@writer/orders?sslmode=require"
    assert ("prepare_threshold" in replica.connection_config) is explicit_threshold
    assert replica.connection_config["host"] == "reader-a,reader-b"
    assert primary.connection_config["conninfo"] == "postgresql://app:secret@writer/orders?sslmode=require"


@pytest.mark.parametrize("config_type", (PsycopgSyncConfig, PsycopgAsyncConfig))
def test_replica_alias_overrides_replace_primary_values(config_type: Any) -> None:
    primary = config_type(connection_config={"conninfo": "postgresql://writer/app", "dbname": "app"})
    replica = primary.derive_pool(url="postgresql://reader/reader_app", database="reader_app")
    assert replica.connection_config["conninfo"] == "postgresql://reader/reader_app"
    assert replica.connection_config["dbname"] == "reader_app"


@pytest.mark.parametrize("async_mode", (False, True))
@pytest.mark.parametrize("mapping_rows", (False, True))
async def test_wal_helpers_handle_row_factories_validate_and_map_errors(async_mode: bool, mapping_rows: bool) -> None:
    import inspect

    from psycopg.errors import InvalidTextRepresentation

    from sqlspec.exceptions import DataError, SQLSpecError

    connection = MagicMock()
    cursor = AsyncMock() if async_mode else MagicMock()
    connection.cursor.return_value = cursor
    config = PsycopgAsyncConfig() if async_mode else PsycopgSyncConfig()
    driver_type = PsycopgAsyncDriver if async_mode else PsycopgSyncDriver
    driver = driver_type(connection, statement_config=config.statement_config)

    async def invoke(function: Any, *args: Any) -> Any:
        result = function(*args)
        return await result if inspect.isawaitable(result) else result

    cursor.fetchone.return_value = {"lsn": "0/16B3748"} if mapping_rows else ("0/16B3748",)
    assert await invoke(driver.current_wal_lsn) == "0/16B3748"
    cursor.fetchone.return_value = {"coalesce": True} if mapping_rows else (True,)
    assert await invoke(driver.has_replayed_wal_lsn, "0/16B3748") is True
    assert cursor.execute.call_args.args[1] == ("0/16B3748",)
    with pytest.raises(ImproperConfigurationError):
        await invoke(driver.has_replayed_wal_lsn, " ")
    cursor.fetchone.return_value = None
    with pytest.raises(SQLSpecError):
        await invoke(driver.current_wal_lsn)
    cursor.execute.side_effect = InvalidTextRepresentation("invalid WAL location")
    with pytest.raises(DataError):
        await invoke(driver.has_replayed_wal_lsn, "bad-lsn")


def test_derived_host_allows_explicit_connection_identity_overrides() -> None:
    primary = PsycopgSyncConfig(connection_config={"conninfo": "postgresql://app:secret@writer/orders?sslmode=require"})
    reader = primary.derive_pool(host="reader", user="reporter", password="reader-secret", database="reports")
    assert reader.connection_config["conninfo"] == "postgresql://app:secret@writer/orders?sslmode=require"
    assert reader.connection_config["user"] == "reporter"
    assert reader.connection_config["password"] == "reader-secret"
    assert reader.connection_config["dbname"] == "reports"


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("tx_active", "in_tx_probe"), [(True, False), (False, True)], ids=["driver-flag", "connection-probe"]
)
async def test_psycopg_session_context_rolls_back_active_transaction_on_clean_exit(
    asynchronous: bool, tx_active: bool, in_tx_probe: bool
) -> None:
    """Session contexts roll back uncommitted driver work before releasing the connection."""
    conn = AsyncMock() if asynchronous else MagicMock()
    conn.autocommit = False
    conn.info = SimpleNamespace(
        transaction_status=pq.TransactionStatus.INTRANS if in_tx_probe else pq.TransactionStatus.IDLE
    )
    release = AsyncMock() if asynchronous else MagicMock()
    context_cls = PsycopgAsyncSessionContext if asynchronous else PsycopgSyncSessionContext
    ctx = context_cls(
        acquire_connection=AsyncMock(return_value=conn) if asynchronous else MagicMock(return_value=conn),
        release_connection=release,
        statement_config=StatementConfig(dialect="postgres"),
        driver_features={},
        prepare_driver=lambda d: d,
    )
    if isinstance(ctx, PsycopgAsyncSessionContext):
        async with ctx as driver:
            driver._transaction_active = tx_active  # pyright: ignore[reportPrivateUsage]
        assert driver.connection is _DETACHED_CONNECTION
        conn.rollback.assert_awaited_once()
        release.assert_awaited_once_with(conn, exc_type=None, exc_val=None, exc_tb=None)
    else:
        with ctx as sync_driver:
            sync_driver._transaction_active = tx_active  # pyright: ignore[reportPrivateUsage]
        assert sync_driver.connection is _DETACHED_CONNECTION
        conn.rollback.assert_called_once()
        release.assert_called_once_with(conn, exc_type=None, exc_val=None, exc_tb=None)


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("body_raises", [False, True], ids=["clean-exit", "body-exception"])
async def test_psycopg_session_context_propagates_or_preserves_rollback_error_and_informs_pool(
    asynchronous: bool, body_raises: bool
) -> None:
    """Rollback failure propagates on clean exit, preserves body exceptions, and passes error info to release."""
    rollback_err = PsycopgError("rollback failed")
    body_err = ValueError("body failed")
    expected_type: type[BaseException] = ValueError if body_raises else SQLSpecError
    conn = AsyncMock() if asynchronous else MagicMock()
    conn.autocommit = False
    conn.info = SimpleNamespace(transaction_status=pq.TransactionStatus.INTRANS)
    conn.rollback = AsyncMock(side_effect=rollback_err) if asynchronous else MagicMock(side_effect=rollback_err)
    release = AsyncMock() if asynchronous else MagicMock()
    context_cls = PsycopgAsyncSessionContext if asynchronous else PsycopgSyncSessionContext
    ctx = context_cls(
        acquire_connection=AsyncMock(return_value=conn) if asynchronous else MagicMock(return_value=conn),
        release_connection=release,
        statement_config=StatementConfig(dialect="postgres"),
        driver_features={},
        prepare_driver=lambda d: d,
    )
    with pytest.raises(expected_type) as caught:
        if isinstance(ctx, PsycopgAsyncSessionContext):
            async with ctx as driver:
                driver._transaction_active = True  # pyright: ignore[reportPrivateUsage]
                if body_raises:
                    raise body_err
        else:
            with ctx as sync_driver:
                sync_driver._transaction_active = True  # pyright: ignore[reportPrivateUsage]
                if body_raises:
                    raise body_err
    if body_raises:
        assert caught.value is body_err
    else:
        assert caught.value.__cause__ is rollback_err
    assert release.call_args.args == (conn,)
    assert release.call_args.kwargs["exc_type"] is type(caught.value)
    assert release.call_args.kwargs["exc_val"] is caught.value


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_psycopg_session_context_enter_failure_releases_and_detaches_driver(asynchronous: bool) -> None:
    conn = AsyncMock() if asynchronous else MagicMock()
    captured: list[Any] = []
    release_events: list[tuple[bool, bool, Any]] = []

    def fail_prepare(driver: Any) -> Any:
        captured.append(driver)
        raise RuntimeError("prepare boom")

    if asynchronous:

        async def acquire_async() -> Any:
            return conn

        async def release_async(released: Any, **kwargs: Any) -> None:
            release_events.append((released is conn, captured[0].connection is conn, kwargs.get("exc_type")))

        ctx = PsycopgAsyncSessionContext(
            acquire_connection=acquire_async,
            release_connection=release_async,
            statement_config=StatementConfig(dialect="postgres"),
            driver_features={},
            prepare_driver=fail_prepare,
        )
        with pytest.raises(RuntimeError, match="prepare boom"):
            async with ctx:
                pytest.fail("prepare failure must abort __aenter__")
    else:

        def release_sync(released: Any, **kwargs: Any) -> None:
            release_events.append((released is conn, captured[0].connection is conn, kwargs.get("exc_type")))

        ctx_sync = PsycopgSyncSessionContext(
            acquire_connection=lambda: conn,
            release_connection=release_sync,
            statement_config=StatementConfig(dialect="postgres"),
            driver_features={},
            prepare_driver=fail_prepare,
        )
        with pytest.raises(RuntimeError, match="prepare boom"), ctx_sync:
            pytest.fail("prepare failure must abort __enter__")

    assert release_events == [(True, True, RuntimeError)]
    assert captured[0].connection is _DETACHED_CONNECTION


@pytest.mark.parametrize("failure_stage", ["pool", "acquire"])
def test_psycopg_sync_read_replica_failover_maps_driver_errors(
    failure_stage: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PsycopgSyncConfig pool and acquire errors map via create_mapped_exception for ReadReplicaGroup failover."""
    good_conn = SimpleNamespace(autocommit=True, info=SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE))
    good_ctx = MagicMock()
    good_ctx.__enter__.return_value = good_conn
    good_ctx.__exit__.return_value = None
    good_pool = MagicMock()
    good_pool.connection.return_value = good_ctx

    bad_ctx = MagicMock()
    bad_ctx.__enter__.side_effect = psycopg_pool.PoolTimeout("pool timed out")
    bad_pool = MagicMock()
    bad_pool.connection.return_value = bad_ctx

    def fake_pool_cls(conninfo: str, **kwargs: Any) -> Any:
        _ = kwargs
        if "bad" in conninfo:
            if failure_stage == "pool":
                raise psycopg.OperationalError("replica down")
            return bad_pool
        return good_pool

    monkeypatch.setattr(psycopg_config, "ConnectionPool", fake_pool_cls)
    config = PsycopgSyncConfig(
        connection_config={"conninfo": "postgresql://primary"}, replicas=["postgresql://bad", "postgresql://good"]
    )
    with config.provide_read_connection() as conn:
        assert conn is good_conn
    with config.provide_read_session() as reader:
        assert reader.connection is good_conn


@pytest.mark.parametrize("failure_stage", ["pool", "acquire"])
async def test_psycopg_async_read_replica_failover_maps_driver_errors(
    failure_stage: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PsycopgAsyncConfig pool and acquire errors map via create_mapped_exception for ReadReplicaGroup failover."""
    good_conn = SimpleNamespace(autocommit=True, info=SimpleNamespace(transaction_status=pq.TransactionStatus.IDLE))
    good_ctx = AsyncMock()
    good_ctx.__aenter__.return_value = good_conn
    good_ctx.__aexit__.return_value = None
    good_pool = MagicMock()
    good_pool.open = AsyncMock()
    good_pool.connection.return_value = good_ctx

    bad_ctx = AsyncMock()
    bad_ctx.__aenter__.side_effect = psycopg_pool.PoolTimeout("pool timed out")
    bad_pool = MagicMock()
    bad_pool.open = (
        AsyncMock(side_effect=psycopg.OperationalError("replica down")) if failure_stage == "pool" else AsyncMock()
    )
    bad_pool.connection.return_value = bad_ctx

    def fake_async_pool_cls(conninfo: str, **kwargs: Any) -> Any:
        _ = kwargs
        if "bad" in conninfo:
            return bad_pool
        return good_pool

    monkeypatch.setattr(psycopg_config, "AsyncConnectionPool", fake_async_pool_cls)
    config = PsycopgAsyncConfig(
        connection_config={"conninfo": "postgresql://primary"}, replicas=["postgresql://bad", "postgresql://good"]
    )
    async with config.provide_read_connection() as conn:
        assert conn is good_conn
    async with config.provide_read_session() as reader:
        assert reader.connection is good_conn


@pytest.mark.parametrize("config_type", (PsycopgSyncConfig, PsycopgAsyncConfig))
def test_psycopg_derive_pool_pops_database_and_username_aliases(config_type: Any) -> None:
    """Field-based derived configs drop alias keys so libpq make_conninfo accepts them."""
    primary = config_type(connection_config={"host": "primary", "dbname": "app", "user": "u"})
    derived = primary.derive_pool(database="ro_db", username="ro_user")
    assert "database" not in derived.connection_config
    assert "db" not in derived.connection_config
    assert "username" not in derived.connection_config
    parsed = conninfo_to_dict(make_conninfo("", **derived.connection_config))
    assert parsed["dbname"] == "ro_db"
    assert parsed["user"] == "ro_user"


@pytest.mark.parametrize("config_type", (PsycopgSyncConfig, PsycopgAsyncConfig))
def test_psycopg_derive_conninfo_replica_and_direct_dsn_clear_inherited_endpoints(config_type: Any) -> None:
    """Explicit conninfo, string replicas, and direct_dsn clear inherited field-based primary endpoints."""
    primary = config_type(
        connection_config={"host": "primary", "port": 5432, "dbname": "rw", "user": "u", "password": "pw"},
        driver_features={"pooler_mode": "transaction", "direct_dsn": "postgresql://du:dp@direct:5439/direct_db"},
        replicas=["postgresql://ro:ropw@replica:6432/ro_db"],
    )
    derived = primary.derive_pool(conninfo="postgresql://ro:ropw@replica:6432/ro_db")
    replica = primary.replica_groups["default"].configs[0]
    direct = primary.derive_direct_config()

    for cfg in (derived, replica):
        parsed = conninfo_to_dict(
            make_conninfo(
                cfg.connection_config["conninfo"], **{k: v for k, v in cfg.connection_config.items() if k != "conninfo"}
            )
        )
        assert parsed["host"] == "replica"
        assert parsed["port"] == "6432"
        assert parsed["dbname"] == "ro_db"
        assert parsed["user"] == "ro"

    parsed_direct = conninfo_to_dict(
        make_conninfo(
            direct.connection_config["conninfo"],
            **{k: v for k, v in direct.connection_config.items() if k != "conninfo"},
        )
    )
    assert parsed_direct["host"] == "direct"
    assert parsed_direct["port"] == "5439"
    assert parsed_direct["dbname"] == "direct_db"
    assert parsed_direct["user"] == "du"
