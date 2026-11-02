"""Tests for IBM Db2 database configuration."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest

from sqlspec.adapters.db2 import Db2AsyncConfig as ExportedDb2AsyncConfig
from sqlspec.adapters.db2.config import (
    Db2AsyncConfig,
    Db2AsyncConnectionContext,
    Db2AsyncPoolParams,
    Db2ConnectionParams,
    Db2DriverFeatures,
    Db2PoolParams,
    Db2SyncConfig,
    Db2SyncConnectionContext,
)
from sqlspec.adapters.db2.core import _parse_db2_dsn, build_connection_config, build_dsn_string
from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver
from sqlspec.adapters.db2.migrations import Db2AsyncMigrationTracker, Db2SyncMigrationTracker
from sqlspec.adapters.db2.pool import Db2AsyncConnectionPool, Db2SyncConnectionPool
from sqlspec.exceptions import ConnectionTimeoutError, ImproperConfigurationError
from tests.unit.adapters.test_db2._fakes import (
    DriverMode,
    FakeDb2AsyncConnection,
    FakeDb2Connection,
    FakeDb2Cursor,
    FakeIbmDbDbiModule,
    FakeIbmDbModule,
)

FakeModules = tuple[FakeIbmDbModule, FakeIbmDbDbiModule]


@pytest.mark.parametrize("mode", ["Sync", "Async"])
def test_connection_signature_namespace_preserves_native_types(mode: str) -> None:
    native = pytest.importorskip("ibm_db_dbi")
    config = Db2SyncConfig() if mode == "Sync" else Db2AsyncConfig()
    connection_type = native.Connection if mode == "Sync" else native.AsyncConnection
    cursor_type = native.Cursor if mode == "Sync" else native.AsyncCursor
    namespace = config.get_signature_namespace()

    assert config.connection_type is connection_type
    assert namespace[f"Db2{mode}Connection"] is connection_type
    assert namespace["Db2RawCursor" if mode == "Sync" else "Db2AsyncRawCursor"] is cursor_type


def test_parse_db2_dsn_cli_format() -> None:
    """Known CLI keywords map to canonical connection keys."""
    dsn = "DATABASE=mytestdb;HOSTNAME=127.0.0.1;PORT=50000;PROTOCOL=TCPIP;UID=db2admin;PWD=secretpass;"

    parsed = _parse_db2_dsn(dsn)

    assert parsed == {
        "database": "mytestdb",
        "hostname": "127.0.0.1",
        "port": 50000,
        "protocol": "TCPIP",
        "user": "db2admin",
        "password": "secretpass",
    }


def test_url_dsn_parsing() -> None:
    """URL DSNs decode credentials and map query keywords like keyword DSNs."""
    parsed = _parse_db2_dsn("db2://u:p%3Bx@h:50001/d?Security=SSL&ClientApplName=svc&autocommit=false")

    assert parsed["user"] == "u"
    assert parsed["password"] == "p;x"
    assert parsed["hostname"] == "h"
    assert parsed["port"] == 50001
    assert parsed["database"] == "d"
    assert parsed["security"] == "SSL"
    assert parsed["autocommit"] is False
    assert parsed["extra"] == {"ClientApplName": "svc"}


def test_braced_dsn_value_parses() -> None:
    """A braced DSN value keeps its semicolons."""
    parsed = _parse_db2_dsn("PWD={a;b};DATABASE=d")

    assert parsed["password"] == "a;b"
    assert parsed["database"] == "d"


def test_dsn_security_keywords_are_preserved() -> None:
    """TLS and schema keywords from a DSN reach the rendered connection string."""
    config = Db2SyncConfig(
        connection_config={
            "dsn": "DATABASE=d;HOSTNAME=h;PORT=50001;SECURITY=SSL;SSLServerCertificate=/c.arm;CURRENTSCHEMA=APP"
        }
    )

    rendered = config.get_connection_string()

    assert "SECURITY=SSL;" in rendered
    assert "SSLSERVERCERTIFICATE=/c.arm;" in rendered
    assert "CURRENTSCHEMA=APP;" in rendered
    assert rendered.startswith("DATABASE=d;HOSTNAME=h;PORT=50001;PROTOCOL=TCPIP;")


def test_dsn_unknown_keywords_move_to_extra() -> None:
    """Keywords SQLSpec does not model survive verbatim through ``extra``."""
    config = Db2SyncConfig(connection_config={"dsn": "DATABASE=d;ClientApplName=svc;"})

    assert config.connection_config["extra"] == {"ClientApplName": "svc"}
    assert config.get_connection_string() == "DATABASE=d;ClientApplName=svc;"


def test_explicit_keys_override_dsn_values() -> None:
    """Explicit parameters and ``extra`` entries win over values parsed from the DSN."""
    config = Db2SyncConfig(
        connection_config={
            "dsn": "DATABASE=from_dsn;UID=dsn_user;ClientApplName=dsn_app;",
            "user": "explicit_user",
            "extra": {"CLIENTAPPLNAME": "explicit_app"},
        }
    )

    assert config.connection_config["database"] == "from_dsn"
    assert config.connection_config["user"] == "explicit_user"
    assert config.connection_config["extra"] == {"CLIENTAPPLNAME": "explicit_app"}


def test_password_with_semicolon_is_brace_escaped(fake_ibm_db: FakeModules) -> None:
    """A password containing ``;`` is braced so the driver receives one ``PWD`` keyword."""
    _, fake_module = fake_ibm_db
    config = Db2SyncConfig(connection_config={"database": "d", "user": "u", "password": "p;x"})

    config.create_connection()

    dsn, user, password, host, database, _ = fake_module.connect_calls[0]
    assert "PWD={p;x};" in dsn
    assert dsn.count("PWD=") == 1
    assert (user, password, host, database) == ("", "", "", "")


@pytest.mark.anyio
@pytest.mark.parametrize("schema", ["APP", "SQLSPEC;SEMICOLON", "Mixed Case"])
async def test_schema_dsn_roundtrip_is_applied_before_creation_hook(
    db2_mode: DriverMode, fake_ibm_db: FakeModules, schema: str
) -> None:
    """Schema values reach SQL as one bound value instead of CLI keyword fragments."""
    _, fake_module = fake_ibm_db
    cursor = FakeDb2Cursor()
    native_connection = FakeDb2Connection(cursors=[cursor])
    fake_module.pending_connections.append(native_connection)
    hook_observations: list[tuple[list[tuple[str, Any]], str | None]] = []
    source = db2_mode.config(
        connection_config={"database": "d", "user": "u", "password": "p;x", "current_schema": schema}
    )
    config = db2_mode.config(
        connection_config={"dsn": source.get_connection_string()},
        driver_features={
            "on_connection_create": lambda _: hook_observations.append((
                list(cursor.executed),
                native_connection.current_schema,
            ))
        },
    )
    expected = [("SET CURRENT SCHEMA = ?", (schema,))]

    assert config.get_connection_string() == source.get_connection_string()
    connection = await config.create_connection() if db2_mode.is_async else config.create_connection()
    try:
        assert cursor.executed == expected
        assert hook_observations == [(expected, schema)]
        assert cursor.closed
        assert not native_connection.closed
        assert fake_module.connect_calls[0][:5] == ("DATABASE=d;UID=u;PWD={p;x};", "", "", "", "")
    finally:
        if db2_mode.is_async:
            await connection.close()
            await config.close_pool()
        else:
            connection.close()
            config.close_pool()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["execute", "close", "setter", "execute-and-close", "cancel-and-close"])
async def test_schema_initialization_failure_closes_connection_and_preserves_error(
    db2_mode: DriverMode, fake_ibm_db: FakeModules, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """Schema initialization never hands an uninitialized or cancelled connection to the hook."""
    _, fake_module = fake_ibm_db
    primary: BaseException = (
        asyncio.CancelledError("schema cancelled")
        if failure == "cancel-and-close"
        else RuntimeError("schema initialization failed")
    )
    cursor = FakeDb2Cursor(error=primary if failure in {"execute", "execute-and-close", "cancel-and-close"} else None)
    native_connection = FakeDb2Connection(cursors=[cursor])
    fake_module.pending_connections.append(native_connection)
    hook_calls: list[object] = []

    def close_cursor() -> None:
        cursor.closed = True
        raise primary if failure == "close" else RuntimeError("schema cursor close failed")

    def set_schema(_: str) -> bool:
        raise primary

    original_close = native_connection.close

    def close_connection() -> None:
        original_close()
        raise RuntimeError("connection close failed")

    if failure in {"close", "execute-and-close", "cancel-and-close"}:
        monkeypatch.setattr(cursor, "close", close_cursor)
    if failure == "setter":
        monkeypatch.setattr(native_connection, "set_current_schema", set_schema)
    monkeypatch.setattr(native_connection, "close", close_connection)
    config = db2_mode.config(
        connection_config={"database": "d", "current_schema": "SQLSPEC;SEMICOLON"},
        driver_features={"on_connection_create": hook_calls.append},
    )
    try:
        with pytest.raises(type(primary)) as caught:
            if db2_mode.is_async:
                await config.create_connection()
            else:
                config.create_connection()
        assert caught.value is primary
        assert cursor.closed
        assert native_connection.closed
        assert hook_calls == []
    finally:
        if db2_mode.is_async:
            await config.close_pool()
        else:
            config.close_pool()


@pytest.mark.parametrize(
    ("value", "rendered"),
    [("a{b", "{a{b}"), (" padded", "{ padded}"), (True, "1"), (False, "0"), ("plain", "plain")],
    ids=["open-brace", "edge-space", "true", "false", "plain"],
)
def test_cli_values_are_quoted_when_needed(value: "str | bool", rendered: str) -> None:
    """Values with CLI punctuation or edge whitespace are braced; booleans render as 1/0."""
    dsn = build_dsn_string({"database": "d", "extra": {"Opt": value}})

    assert dsn == f"DATABASE=d;Opt={rendered};"


def test_value_with_closing_brace_is_rejected() -> None:
    """A value containing ``}`` cannot be represented and is rejected."""
    with pytest.raises(ImproperConfigurationError):
        Db2SyncConfig(connection_config={"database": "d", "password": "p}x"})


def test_invalid_extra_keyword_is_rejected() -> None:
    """Keywords outside the CLI keyword grammar are rejected."""
    with pytest.raises(ImproperConfigurationError):
        Db2SyncConfig(connection_config={"database": "d", "extra": {"A;B": "1"}})


def test_extra_cannot_shadow_a_modeled_keyword() -> None:
    """Keywords with a dedicated parameter must be configured through that parameter."""
    with pytest.raises(ImproperConfigurationError):
        Db2SyncConfig(connection_config={"database": "d", "extra": {"security": "SSL"}})


@pytest.mark.parametrize("connection_config", [{}, {"hostname": "h"}], ids=["empty", "host-only"])
def test_database_is_required(connection_config: "dict[str, Any]") -> None:
    """A configuration without a database name is rejected."""
    with pytest.raises(ImproperConfigurationError):
        Db2SyncConfig(connection_config=connection_config)


@pytest.mark.parametrize(
    "connection_config",
    [
        {"dsn": "DATABASE=d;AUTOCOMMIT=maybe"},
        {"database": "d", "port": "fifty"},
        {"database": "d", "port": True},
        {"database": "d", "extra": ["ClientApplName"]},
        {"dsn": 42},
    ],
    ids=["autocommit-text", "port-text", "port-bool", "extra-list", "dsn-int"],
)
def test_invalid_parameter_values_are_rejected(connection_config: "dict[str, Any]") -> None:
    """Values of the wrong type are rejected instead of being rendered."""
    with pytest.raises(ImproperConfigurationError):
        Db2SyncConfig(connection_config=connection_config)


def test_url_dsn_without_credentials_uses_cataloged_alias() -> None:
    """A URL with only a database path connects to a cataloged alias with no host defaults."""
    config = Db2SyncConfig(connection_config={"dsn": "db2:///SAMPLE?AutoCommit=1"})

    assert config.connection_config == {"database": "SAMPLE", "autocommit": True}


def test_host_defaults_apply_only_with_hostname() -> None:
    """Port and protocol defaults apply only to TCP/IP connections."""
    assert Db2SyncConfig(connection_config={"database": "d"}).get_connection_string() == "DATABASE=d;"
    assert (
        Db2SyncConfig(connection_config={"database": "d", "hostname": "h"}).get_connection_string()
        == "DATABASE=d;HOSTNAME=h;PORT=50000;PROTOCOL=TCPIP;"
    )


@pytest.mark.parametrize("alias", ["db", "host", "server", "username", "uid", "pwd", "url", "connection_string"])
def test_alias_keys_are_rejected(alias: str) -> None:
    """Only canonical connection keys are accepted."""
    with pytest.raises(ImproperConfigurationError, match="Unsupported Db2 connection parameter"):
        Db2SyncConfig(connection_config={"database": "d", alias: "value"})


def test_canonical_keys_render_in_order() -> None:
    """Every modeled parameter renders under its CLI keyword; autocommit is never rendered."""
    config = Db2SyncConfig(
        connection_config={
            "database": "d",
            "hostname": "h",
            "port": "50001",
            "protocol": "TCPIP",
            "user": "u",
            "password": "p",
            "current_schema": "APP",
            "security": "SSL",
            "ssl_server_certificate": "/c.arm",
            "authentication": "SERVER_ENCRYPT",
            "connect_timeout": 5,
            "autocommit": False,
            "pool_recycle_seconds": 60,
            "health_check_interval": 5.0,
            "extra": {"ClientApplName": "svc"},
        }
    )

    assert config.get_connection_string() == (
        "DATABASE=d;HOSTNAME=h;PORT=50001;PROTOCOL=TCPIP;UID=u;PWD=p;CURRENTSCHEMA=APP;SECURITY=SSL;"
        "SSLSERVERCERTIFICATE=/c.arm;AUTHENTICATION=SERVER_ENCRYPT;CONNECTTIMEOUT=5;ClientApplName=svc;"
    )
    assert config.connection_config["port"] == 50001


@pytest.mark.anyio
async def test_db2_config_defaults(db2_mode: DriverMode) -> None:
    """Sync and async configs share capabilities and pick the driver and tracker of their mode."""
    config = db2_mode.config(connection_config={"database": "SAMPLE"})

    assert config.connection_config == {"database": "SAMPLE"}
    assert config.driver_type is (Db2AsyncDriver if db2_mode.is_async else Db2SyncDriver)
    assert config.migration_tracker_type is (Db2AsyncMigrationTracker if db2_mode.is_async else Db2SyncMigrationTracker)
    assert config.supports_transactional_ddl is True
    assert config.supports_migration_schemas is True
    assert config.supports_native_row_streaming is True
    assert config.supports_native_arrow_export is False
    assert config.type_coercion_capabilities.datetime_binding == "native"
    assert config.type_coercion_capabilities.uuid_binding == "text"


def test_db2_config_provide_pool_and_create_connection(fake_ibm_db: FakeModules) -> None:
    """The pool opens connections through ibm_db_dbi and runs the creation hook."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    hook_calls: list[Any] = []

    config = Db2SyncConfig(
        connection_config={"database": "TESTDB", "pool_recycle_seconds": 60, "health_check_interval": 5.0},
        driver_features={"on_connection_create": hook_calls.append},
    )

    pool = config.provide_pool()
    assert isinstance(pool, Db2SyncConnectionPool)
    assert pool._recycle_seconds == 60

    conn = config.create_connection()
    assert cast(object, conn) is connection
    assert fake_module.connect_calls[0][0] == "DATABASE=TESTDB;"
    assert hook_calls == [connection]

    config._close_pool()
    assert config.connection_instance is None


def test_db2_config_signature_namespace() -> None:
    """Verify exported symbols in signature namespace for dependency injection."""
    config = Db2SyncConfig(connection_config={"database": "d"})
    namespace = config.get_signature_namespace()
    assert namespace["Db2SyncConfig"] is Db2SyncConfig
    assert namespace["Db2SyncConnectionContext"] is Db2SyncConnectionContext
    assert namespace["Db2ConnectionParams"] is Db2ConnectionParams
    assert namespace["Db2SyncConnectionPool"] is Db2SyncConnectionPool
    assert namespace["Db2SyncDriver"] is Db2SyncDriver
    assert namespace["Db2DriverFeatures"] is Db2DriverFeatures
    assert namespace["Db2PoolParams"] is Db2PoolParams


@pytest.mark.anyio
async def test_db2_config_event_runtime_hints(db2_mode: DriverMode) -> None:
    """Sync and async configs report the same event channel runtime hints."""
    hints = db2_mode.config(connection_config={"database": "d"}).get_event_runtime_hints()

    assert hints.poll_interval == 0.25
    assert hints.lease_seconds == 5


def test_db2_async_config_signature_namespace() -> None:
    """The async config registers its own types for dependency injection."""
    namespace = Db2AsyncConfig(connection_config={"database": "d"}).get_signature_namespace()

    assert namespace["Db2AsyncConfig"] is Db2AsyncConfig
    assert namespace["Db2AsyncConnectionContext"] is Db2AsyncConnectionContext
    assert namespace["Db2AsyncConnectionPool"] is Db2AsyncConnectionPool
    assert namespace["Db2AsyncDriver"] is Db2AsyncDriver
    assert namespace["Db2AsyncPoolParams"] is Db2AsyncPoolParams
    assert ExportedDb2AsyncConfig is Db2AsyncConfig


@pytest.mark.anyio
async def test_async_config_pool_params_are_applied(fake_ibm_db: FakeModules) -> None:
    """Pool sizing, timeout, recycle and health-check settings reach the async pool, not the DSN."""
    _, fake_module = fake_ibm_db
    hook_calls: list[Any] = []

    async def hook(connection: Any) -> None:
        hook_calls.append(connection)

    config = Db2AsyncConfig(
        connection_config={
            "database": "TESTDB",
            "max_size": 1,
            "acquire_timeout": 0.05,
            "pool_recycle_seconds": 60,
            "health_check_interval": 5.0,
        },
        driver_features={"on_connection_create": hook},
    )
    assert config.get_connection_string() == "DATABASE=TESTDB;"
    pool = await config.provide_pool()
    try:
        assert isinstance(pool, Db2AsyncConnectionPool)
        assert (pool._recycle_seconds, pool._health_check_interval) == (60, 5.0)
        held = await pool.acquire()
        with pytest.raises(ConnectionTimeoutError, match=r"0\.05s"):
            await pool.acquire()
        assert fake_module.connect_calls[0][0] == "DATABASE=TESTDB;"
        assert hook_calls == [held]
        await pool.release(held)
    finally:
        await config.close_pool()
    assert config.connection_instance is None


@pytest.mark.anyio
async def test_async_provide_session_yields_async_driver(fake_ibm_db: FakeModules) -> None:
    """Async sessions hand out an async driver over a pooled connection and return it on exit."""
    config = Db2AsyncConfig(connection_config={"database": "TESTDB"})
    try:
        async with config.provide_session() as driver:
            assert isinstance(driver, Db2AsyncDriver)
            assert isinstance(driver.connection, FakeDb2AsyncConnection)
            pool = await config.provide_pool()
            assert pool.checked_out() == 1
        assert pool.checked_out() == 0
        async with config.provide_connection() as connection:
            assert connection is driver.connection
    finally:
        await config.close_pool()


@pytest.mark.anyio
async def test_async_create_connection_is_standalone(fake_ibm_db: FakeModules) -> None:
    """``create_connection`` opens a caller-owned connection outside the pool's accounting."""
    _, fake_module = fake_ibm_db
    connection = FakeDb2Connection()
    fake_module.pending_connections.append(connection)
    config = Db2AsyncConfig(connection_config={"database": "TESTDB"})
    try:
        created = await config.create_connection()

        assert isinstance(created, FakeDb2AsyncConnection)
        assert created.sync_connection is connection
        assert connection.autocommit is True
        assert (await config.provide_pool()).size() == 0
    finally:
        await config.close_pool()
    assert connection.closed is False


@pytest.mark.parametrize(
    "connection_config",
    [
        {"database": "d", "port": 0},
        {"database": "d", "port": -1},
        {"database": "d", "port": 65536},
        {"database": "d", "connect_timeout": -1},
    ],
    ids=["port-zero", "port-negative", "port-too-high", "negative-timeout"],
)
def test_out_of_range_port_and_timeout_are_rejected(connection_config: "dict[str, Any]") -> None:
    """Port outside 1..65535 and negative connect_timeout are rejected."""
    with pytest.raises(ImproperConfigurationError):
        build_connection_config(connection_config)


def test_build_connection_config_strips_async_pool_keys() -> None:
    """build_connection_config accepts Db2AsyncPoolParams and strips max_size and acquire_timeout."""
    normalized = build_connection_config({"database": "SAMPLE", "max_size": 5, "acquire_timeout": 10.0})

    assert normalized == {"database": "SAMPLE"}


@pytest.mark.anyio
async def test_config_driver_features_defaults_and_events_extension(db2_mode: DriverMode) -> None:
    """Driver features default enable_events and events_backend and wire extension_config['events']."""
    default_config = db2_mode.config(connection_config={"database": "SAMPLE"})
    assert default_config.driver_features["enable_events"] is True
    assert default_config.driver_features["events_backend"] == "poll_queue"

    explicit_events = db2_mode.config(
        connection_config={"database": "SAMPLE"},
        driver_features={"enable_events": True, "events_backend": "poll_queue"},
    )
    assert explicit_events.extension_config["events"]["backend"] == "poll_queue"

    disabled_events = db2_mode.config(
        connection_config={"database": "SAMPLE"},
        driver_features={"enable_events": False},
        extension_config={"events": {"backend": "poll_queue"}},
    )
    assert "events" not in disabled_events.extension_config


@pytest.mark.anyio
async def test_statement_config_coerces_aware_datetime_to_naive_utc(db2_mode: DriverMode) -> None:
    """The Db2 statement parameter profile coerces timezone-aware datetimes to naive UTC."""
    config = db2_mode.config(connection_config={"database": "SAMPLE"})
    type_coercion_map = config.statement_config.parameter_config.type_coercion_map
    assert datetime in type_coercion_map

    aware = datetime(2026, 9, 24, 14, 30, tzinfo=timezone(timedelta(hours=2)))
    coerced = type_coercion_map[datetime](aware)
    assert coerced == datetime(2026, 9, 24, 12, 30)
    assert coerced.tzinfo is None
