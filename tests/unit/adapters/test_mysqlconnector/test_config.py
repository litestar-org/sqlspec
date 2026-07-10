"""Unit tests for mysql-connector configuration modernization."""

import contextlib
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from mysql.connector.constants import ClientFlag
from mysql.connector.conversion import MySQLConverter
from mysql.connector.cursor import MySQLCursor

from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorAsyncCursor, MysqlConnectorSyncCursor
from sqlspec.adapters.mysqlconnector.config import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncConnectionParams,
    MysqlConnectorDriverFeatures,
    MysqlConnectorPoolParams,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncConnectionParams,
)
from sqlspec.adapters.mysqlconnector.core import MysqlConnectorAsyncStreamSource, MysqlConnectorSyncStreamSource
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver

if TYPE_CHECKING:
    from sqlspec.adapters.mysqlconnector.config import MysqlConnectorCursorParams, MysqlConnectorFailoverTarget


def test_sync_config_uses_connector_python_host_default_and_disables_local_infile() -> None:
    """SQLSpec should preserve the driver host default and close the local infile gate."""
    config = MysqlConnectorSyncConfig()

    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["allow_local_infile"] is False


def test_async_config_uses_connector_python_host_default_and_disables_local_infile() -> None:
    """The async config should use the same connection defaults as sync."""
    config = MysqlConnectorAsyncConfig()

    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 3306
    assert config.connection_config["allow_local_infile"] is False


def test_acquire_sync_connection_lazily_creates_pool_once(monkeypatch: "pytest.MonkeyPatch") -> None:
    """_acquire_sync_connection routes through the SQLSpec pool (created once) and never leaks pool kwargs into connect()."""
    from sqlspec.adapters.mysqlconnector import config as cfg_module

    created_pools: list[Any] = []
    connect_calls: list[dict[str, Any]] = []

    class _FakePool:
        def __init__(self, **kwargs: Any) -> None:
            self.init_kwargs = kwargs
            created_pools.append(self)

        def get_connection(self) -> Any:
            return MagicMock()

    def _fake_connect(**kwargs: Any) -> Any:
        connect_calls.append(kwargs)
        return MagicMock()

    monkeypatch.setattr(cfg_module, "MysqlConnectorConnectionPool", _FakePool)
    monkeypatch.setattr(cfg_module.mysql.connector, "connect", _fake_connect)

    config = MysqlConnectorSyncConfig(
        connection_config={"pool_name": "sqlspec", "pool_size": 3, "pool_reset_session": True}
    )

    first = config._acquire_sync_connection()
    second = config._acquire_sync_connection()

    assert len(created_pools) == 1
    assert config.connection_instance is created_pools[0]
    assert first is not None
    assert second is not None
    for call in connect_calls:
        assert "pool_name" not in call
        assert "pool_size" not in call
        assert "pool_reset_session" not in call


@pytest.mark.parametrize("config_cls", [MysqlConnectorSyncConfig, MysqlConnectorAsyncConfig])
def test_local_infile_uses_connector_python_security_gate(config_cls: type[Any]) -> None:
    """LOAD DATA LOCAL INFILE should use mysql-connector's native consent gate."""
    config = config_cls(connection_config={"allow_local_infile": True})

    assert config.connection_config["allow_local_infile"] is True
    assert "local_infile" not in config.connection_config


def test_sync_connection_params_type_accepts_modern_connector_options() -> None:
    """Connection params should type the current Connector/Python sync option surface."""
    failover: list[MysqlConnectorFailoverTarget] = [
        {"host": "primary.example.com", "port": 3306},
        {"host": "replica.example.com", "port": 3307, "database": "analytics"},
    ]
    params: MysqlConnectorSyncConnectionParams = {
        "user": "scott",
        "password1": "mfa-1",
        "password2": "mfa-2",
        "password3": "mfa-3",
        "openid_token_file": "/var/run/token.jwt",
        "auth_plugin": "authentication_openid_connect_client",
        "webauthn_callback": "pkg.module.callback",
        "conn_attrs": {"program_name": "sqlspec-tests"},
        "init_command": "SET sql_mode='TRADITIONAL'",
        "use_unicode": True,
        "collation": "utf8mb4_0900_ai_ci",
        "time_zone": "+00:00",
        "sql_mode": "TRADITIONAL",
        "get_warnings": True,
        "raise_on_warnings": True,
        "connect_timeout": 5,
        "read_timeout": 30,
        "write_timeout": 30,
        "client_flags": [ClientFlag.FOUND_ROWS, -ClientFlag.LONG_FLAG],
        "buffered": True,
        "raw": False,
        "consume_results": True,
        "tls_versions": ["TLSv1.2", "TLSv1.3"],
        "tls_ciphersuites": ["TLS_AES_256_GCM_SHA384"],
        "ssl_disabled": False,
        "ssl_cipher": "DHE-RSA-AES256-SHA",
        "force_ipv6": True,
        "dns_srv": False,
        "kerberos_auth_mode": "GSSAPI",
        "krb_service_principal": "ldap/db.example.com@EXAMPLE.COM",
        "oci_config_file": "/home/app/.oci/config",
        "oci_config_profile": "DEFAULT",
        "compress": True,
        "converter_class": MySQLConverter,
        "converter_str_fallback": True,
        "failover": failover,
        "option_files": ["/etc/mysql/my.cnf"],
        "option_groups": ["client", "connector_python"],
        "allow_local_infile": False,
        "allow_local_infile_in_path": "/srv/imports",
    }

    assert params["client_flags"] == [ClientFlag.FOUND_ROWS, -ClientFlag.LONG_FLAG]
    assert params["failover"] is failover
    assert params["converter_class"] is MySQLConverter


def test_async_connection_params_type_accepts_modern_connector_options() -> None:
    """Async params should mirror supported Connector/Python connect kwargs."""
    params: MysqlConnectorAsyncConnectionParams = {
        "host": "db.example.com",
        "username": "scott",
        "passwd": "tiger",
        "db": "app",
        "read_timeout": 10,
        "write_timeout": 10,
        "client_flags": (ClientFlag.SSL,),
        "allow_local_infile": False,
        "raw": True,
        "consume_results": True,
        "dns_srv": True,
    }

    assert params["client_flags"] == (ClientFlag.SSL,)
    assert params["allow_local_infile"] is False


def test_pool_params_keep_pool_fields_and_modern_connection_fields() -> None:
    """Pool params should retain pool-specific keys while accepting connect kwargs."""
    params: MysqlConnectorPoolParams = {
        "pool_name": "sqlspec",
        "pool_size": 3,
        "pool_reset_session": False,
        "failover": ({"host": "replica.example.com"},),
        "client_flags": [ClientFlag.FOUND_ROWS],
    }

    assert params["pool_size"] == 3
    assert params["failover"] == ({"host": "replica.example.com"},)


def test_cursor_params_type_accepts_connector_cursor_options() -> None:
    """Driver features should expose cursor kwargs SQLSpec forwards itself."""
    cursor_params: MysqlConnectorCursorParams = {
        "buffered": True,
        "raw": True,
        "dictionary": False,
        "prepared": True,
        "cursor_class": MySQLCursor,
    }
    features: MysqlConnectorDriverFeatures = {"cursor_options": cursor_params}

    assert features["cursor_options"]["prepared"] is True
    assert features["cursor_options"]["cursor_class"] is MySQLCursor


def test_sync_cursor_context_passes_cursor_options() -> None:
    """Sync cursor wrappers should forward SQLSpec-owned cursor kwargs."""
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor

    with MysqlConnectorSyncCursor(connection, {"prepared": True, "raw": True}) as entered:
        assert entered is cursor

    connection.cursor.assert_called_once_with(prepared=True, raw=True)


async def test_async_cursor_context_passes_cursor_options() -> None:
    """Async cursor wrappers should forward SQLSpec-owned cursor kwargs."""
    connection = AsyncMock()
    cursor = AsyncMock()
    connection.cursor.return_value = cursor

    async with MysqlConnectorAsyncCursor(connection, {"prepared": True, "raw": True}) as entered:
        assert entered is cursor

    connection.cursor.assert_awaited_once_with(prepared=True, raw=True)


def test_sync_driver_with_cursor_uses_driver_cursor_options() -> None:
    """Sync SQLSpec execution should route configured cursor options."""
    connection = MagicMock()
    driver = MysqlConnectorSyncDriver(connection=connection, driver_features={"cursor_options": {"raw": True}})

    cursor_context = driver.with_cursor(connection)

    assert cursor_context.cursor_options == {"raw": True}


def test_async_driver_with_cursor_uses_driver_cursor_options() -> None:
    """Async SQLSpec execution should route configured cursor options."""
    connection = MagicMock()
    driver = MysqlConnectorAsyncDriver(connection=connection, driver_features={"cursor_options": {"prepared": True}})

    cursor_context = driver.with_cursor(connection)

    assert cursor_context.cursor_options == {"prepared": True}


def test_sync_dispatch_select_stream_uses_driver_cursor_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sync stream execution should reuse cursor options and force unbuffered cursors."""
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = []
    connection.cursor.return_value = cursor
    driver = MysqlConnectorSyncDriver(connection=connection, driver_features={"cursor_options": {"prepared": True}})
    statement = MagicMock()
    statement.returns_rows.return_value = True

    monkeypatch.setattr(MysqlConnectorSyncDriver, "_compiled_sql", lambda self, stmt, config: ("SELECT 1", None))
    monkeypatch.setattr(MysqlConnectorSyncDriver, "handle_database_exceptions", lambda self: contextlib.nullcontext())
    monkeypatch.setattr(MysqlConnectorSyncDriver, "_check_pending_exception", lambda self, handler: None)
    stream = driver.dispatch_select_stream(statement, 10)

    assert stream is not None
    source = stream._source
    assert isinstance(source, MysqlConnectorSyncStreamSource)
    assert source._cursor_options == {"prepared": True, "buffered": False}

    source.start()

    connection.cursor.assert_called_once_with(prepared=True, buffered=False)


@pytest.mark.anyio
async def test_async_dispatch_select_stream_uses_driver_cursor_options(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async stream execution should reuse cursor options and force unbuffered cursors."""
    connection = AsyncMock()
    cursor = AsyncMock()
    cursor.description = []
    connection.cursor.return_value = cursor
    driver = MysqlConnectorAsyncDriver(connection=connection, driver_features={"cursor_options": {"prepared": True}})
    statement = MagicMock()
    statement.returns_rows.return_value = True

    monkeypatch.setattr(MysqlConnectorAsyncDriver, "_compiled_sql", lambda self, stmt, config: ("SELECT 1", None))
    monkeypatch.setattr(MysqlConnectorAsyncDriver, "handle_database_exceptions", lambda self: _AsyncNullContext())
    monkeypatch.setattr(MysqlConnectorAsyncDriver, "_check_pending_exception", lambda self, handler: None)
    stream = driver.dispatch_select_stream(statement, 10)

    assert stream is not None
    source = stream._source
    assert isinstance(source, MysqlConnectorAsyncStreamSource)
    assert source._cursor_options == {"prepared": True, "buffered": False}

    await source.start()

    connection.cursor.assert_awaited_once_with(prepared=True, buffered=False)


def test_sync_stream_close_consumes_unread_rows_without_reconnecting() -> None:
    """Early close preserves the current session and any active transaction."""
    connection = MagicMock()
    connection.unread_result = True
    connection._cnx = None
    cursor = MagicMock()
    driver = MagicMock(connection=connection, driver_features={})
    source = MysqlConnectorSyncStreamSource(driver, "SELECT 1", None, 10, set())
    source._cursor = cursor

    source.close()

    connection.consume_results.assert_called_once_with()
    cursor.close.assert_called_once_with()
    connection.shutdown.assert_not_called()
    connection.reconnect.assert_not_called()


def test_sync_stream_close_closes_cursor_when_consuming_unread_rows_fails() -> None:
    connection = MagicMock(unread_result=True, _cnx=None)
    connection.consume_results.side_effect = RuntimeError("consume failed")
    cursor = MagicMock()
    source = MysqlConnectorSyncStreamSource(MagicMock(connection=connection), "SELECT 1", None, 10, set())
    source._cursor = cursor

    with pytest.raises(RuntimeError, match="consume failed"):
        source.close()

    cursor.close.assert_called_once_with()
    connection.reconnect.assert_not_called()


@pytest.mark.anyio
async def test_async_stream_close_consumes_unread_rows_without_reconnecting() -> None:
    """Async early close preserves the current session and active transaction."""
    connection = MagicMock()
    connection.unread_result = True
    connection._cnx = None
    connection.consume_results = AsyncMock()
    connection.shutdown = AsyncMock()
    connection.reconnect = AsyncMock()
    cursor = MagicMock()
    cursor.close = AsyncMock()
    driver = MagicMock(connection=connection, driver_features={})
    source = MysqlConnectorAsyncStreamSource(driver, "SELECT 1", None, 10, set())
    source._cursor = cursor

    await source.close()

    connection.consume_results.assert_awaited_once_with()
    cursor.close.assert_awaited_once_with()
    connection.shutdown.assert_not_awaited()
    connection.reconnect.assert_not_awaited()


@pytest.mark.anyio
async def test_async_stream_close_closes_cursor_when_consuming_unread_rows_fails() -> None:
    connection = MagicMock(unread_result=True, _cnx=None)
    connection.consume_results = AsyncMock(side_effect=RuntimeError("consume failed"))
    connection.reconnect = AsyncMock()
    cursor = MagicMock()
    cursor.close = AsyncMock()
    source = MysqlConnectorAsyncStreamSource(MagicMock(connection=connection), "SELECT 1", None, 10, set())
    source._cursor = cursor

    with pytest.raises(RuntimeError, match="consume failed"):
        await source.close()

    cursor.close.assert_awaited_once_with()
    connection.reconnect.assert_not_awaited()


def test_sync_create_connection_forwards_local_infile_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connection creation should forward mysql-connector's local infile gate."""
    calls: list[dict[str, Any]] = []
    connection = MagicMock()

    def connect(**kwargs: Any) -> MagicMock:
        calls.append(kwargs)
        return connection

    monkeypatch.setattr("sqlspec.adapters.mysqlconnector.config.mysql.connector.connect", connect)
    MysqlConnectorSyncConfig(connection_config={"allow_local_infile": True}).create_connection()

    assert calls[0]["allow_local_infile"] is True
    assert "local_infile" not in calls[0]


@pytest.mark.anyio
async def test_async_create_connection_forwards_local_infile_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async connection creation should forward mysql-connector's local infile gate."""
    calls: list[dict[str, Any]] = []
    connection = AsyncMock()

    async def connect(**kwargs: Any) -> AsyncMock:
        calls.append(kwargs)
        return connection

    monkeypatch.setattr("sqlspec.adapters.mysqlconnector.config.mysqlconnector_aio.connect", connect)

    config = MysqlConnectorAsyncConfig(connection_config={"allow_local_infile": True})
    await config.create_connection()

    assert calls[0]["allow_local_infile"] is True
    assert "local_infile" not in calls[0]


def test_sync_create_connection_passes_local_infile_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connection creation should pass the local infile gate explicitly to the driver."""
    calls: list[dict[str, Any]] = []
    connection = MagicMock()

    def connect(**kwargs: Any) -> MagicMock:
        calls.append(kwargs)
        return connection

    monkeypatch.setattr("sqlspec.adapters.mysqlconnector.config.mysql.connector.connect", connect)
    MysqlConnectorSyncConfig().create_connection()

    assert calls[0]["allow_local_infile"] is False


class _AsyncNullContext:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
    ) -> None:
        return None
