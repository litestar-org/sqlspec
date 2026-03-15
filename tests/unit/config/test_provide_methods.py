from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig, AiosqliteConnectionContext
from sqlspec.adapters.aiosqlite.driver import AiosqliteSessionContext
from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.cockroach_asyncpg.config import CockroachAsyncpgConfig
from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.mysqlconnector.config import MysqlConnectorSyncConfig
from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.pymysql.config import PyMysqlConfig
from sqlspec.adapters.spanner.config import SpannerSyncConfig
from sqlspec.adapters.sqlite.config import SqliteConfig, SqliteConnectionContext
from sqlspec.adapters.sqlite.driver import SqliteSessionContext
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.core import StatementConfig
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
from tests.conftest import requires_interpreted

if TYPE_CHECKING:
    _SyncPoolConfigBase = SyncDatabaseConfig[Any, object, "_DummySyncDriver"]
    _AsyncPoolConfigBase = AsyncDatabaseConfig[Any, object, "_DummyAsyncDriver"]
else:
    _SyncPoolConfigBase = SyncDatabaseConfig
    _AsyncPoolConfigBase = AsyncDatabaseConfig


class _DummySyncDriver(SyncDriverAdapterBase):
    __slots__ = ()

    @property
    def data_dictionary(self) -> SyncDataDictionaryBase:  # type: ignore[override]
        raise NotImplementedError

    def with_cursor(self, connection: Any) -> AbstractContextManager[Any]:  # type: ignore[override]
        @contextmanager
        def _cursor_ctx():
            yield object()

        return _cursor_ctx()

    def handle_database_exceptions(self) -> AbstractContextManager[None]:  # type: ignore[override]
        @contextmanager
        def _handler_ctx():
            yield None

        return _handler_ctx()

    def begin(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    def rollback(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    def commit(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    def dispatch_special_handling(self, cursor: Any, statement: Any):  # type: ignore[override]
        return None

    def dispatch_execute_script(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    def dispatch_execute_many(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    def dispatch_execute(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError


class _DummyAsyncDriver(AsyncDriverAdapterBase):
    __slots__ = ()

    @property
    def data_dictionary(self) -> AsyncDataDictionaryBase:  # type: ignore[override]
        raise NotImplementedError

    @asynccontextmanager
    async def with_cursor(self, connection: Any):  # type: ignore[override]
        yield object()

    @asynccontextmanager
    async def handle_database_exceptions(self):  # type: ignore[override]
        yield None

    async def begin(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    async def rollback(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    async def commit(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    async def dispatch_special_handling(self, cursor: Any, statement: Any):  # type: ignore[override]
        return None

    async def dispatch_execute_script(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    async def dispatch_execute_many(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    async def dispatch_execute(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError


class _SyncConnectionContext:
    __slots__ = ("config",)

    def __init__(self, config: "_SyncTemplateConfig") -> None:
        self.config = config


class _SyncSessionHandler:
    __slots__ = ("config",)

    def __init__(self, config: "_SyncTemplateConfig") -> None:
        self.config = config

    def acquire_connection(self) -> object:
        return object()

    def release_connection(self, _connection: object) -> None:
        return None


class _SyncSessionContext:
    __slots__ = ("acquire_connection", "driver_features", "prepare_driver", "release_connection", "statement_config")

    def __init__(
        self,
        *,
        acquire_connection: Any,
        release_connection: Any,
        statement_config: StatementConfig,
        driver_features: dict[str, Any],
        prepare_driver: Any,
    ) -> None:
        self.acquire_connection = acquire_connection
        self.release_connection = release_connection
        self.statement_config = statement_config
        self.driver_features = driver_features
        self.prepare_driver = prepare_driver


class _AsyncConnectionContext:
    __slots__ = ("config",)

    def __init__(self, config: "_AsyncTemplateConfig") -> None:
        self.config = config


class _AsyncSessionHandler:
    __slots__ = ("config",)

    def __init__(self, config: "_AsyncTemplateConfig") -> None:
        self.config = config

    async def acquire_connection(self) -> object:
        return object()

    async def release_connection(self, _connection: object) -> None:
        return None


class _AsyncSessionContext:
    __slots__ = ("acquire_connection", "driver_features", "prepare_driver", "release_connection", "statement_config")

    def __init__(
        self,
        *,
        acquire_connection: Any,
        release_connection: Any,
        statement_config: StatementConfig,
        driver_features: dict[str, Any],
        prepare_driver: Any,
    ) -> None:
        self.acquire_connection = acquire_connection
        self.release_connection = release_connection
        self.statement_config = statement_config
        self.driver_features = driver_features
        self.prepare_driver = prepare_driver


class _SyncTemplateConfig(_SyncPoolConfigBase):
    driver_type = _DummySyncDriver
    connection_type = object
    _connection_context_class = _SyncConnectionContext
    _session_factory_class = _SyncSessionHandler
    _session_context_class = _SyncSessionContext
    _default_statement_config = StatementConfig(dialect="sqlite")

    def create_connection(self) -> object:
        return object()

    def _create_pool(self) -> object:
        return object()

    def _close_pool(self) -> None:
        return None


class _AsyncTemplateConfig(_AsyncPoolConfigBase):
    driver_type = _DummyAsyncDriver
    connection_type = object
    _connection_context_class = _AsyncConnectionContext
    _session_factory_class = _AsyncSessionHandler
    _session_context_class = _AsyncSessionContext
    _default_statement_config = StatementConfig(dialect="sqlite")

    async def create_connection(self) -> object:
        return object()

    async def _create_pool(self) -> object:
        return object()

    async def _close_pool(self) -> None:
        return None


@requires_interpreted
def test_sync_database_config_template_provides_connection_and_session() -> None:
    config = _SyncTemplateConfig(
        statement_config=StatementConfig(dialect="postgres"), driver_features={"enable_events": True}
    )

    connection_context = config.provide_connection()
    session_context = cast(_SyncSessionContext, config.provide_session())

    assert isinstance(connection_context, _SyncConnectionContext)
    assert connection_context.config is config
    assert isinstance(session_context, _SyncSessionContext)
    assert session_context.statement_config.dialect == "postgres"
    assert session_context.driver_features["enable_events"] is True
    assert session_context.prepare_driver.__self__ is config


@requires_interpreted
def test_sync_database_config_template_uses_default_statement_config_when_unset() -> None:
    config = _SyncTemplateConfig(statement_config=None)
    cast(Any, config).statement_config = None

    session_context = cast(_SyncSessionContext, config.provide_session())

    assert session_context.statement_config.dialect == "sqlite"


@pytest.mark.anyio
@requires_interpreted
async def test_async_database_config_template_provides_connection_and_session() -> None:
    config = _AsyncTemplateConfig(
        statement_config=StatementConfig(dialect="postgres"), driver_features={"enable_events": True}
    )

    connection_context = config.provide_connection()
    session_context = cast(_AsyncSessionContext, config.provide_session())

    assert isinstance(connection_context, _AsyncConnectionContext)
    assert connection_context.config is config
    assert isinstance(session_context, _AsyncSessionContext)
    assert session_context.statement_config.dialect == "postgres"
    assert session_context.driver_features["enable_events"] is True
    assert session_context.prepare_driver.__self__ is config


@pytest.mark.anyio
@requires_interpreted
async def test_async_database_config_template_uses_explicit_statement_override() -> None:
    config = _AsyncTemplateConfig(statement_config=None)
    explicit_config = StatementConfig(dialect="mysql")

    session_context = cast(_AsyncSessionContext, config.provide_session(statement_config=explicit_config))

    assert session_context.statement_config is explicit_config


def test_sqlite_config_inherited_base_methods_build_expected_contexts() -> None:
    config = SqliteConfig()
    config.statement_config = config.statement_config.replace(dialect="sqlite")

    connection_context = config.provide_connection()
    session_context = config.provide_session()

    assert isinstance(connection_context, SqliteConnectionContext)
    assert isinstance(session_context, SqliteSessionContext)
    assert session_context._statement_config is config.statement_config  # pyright: ignore[reportPrivateUsage]


@pytest.mark.anyio
async def test_aiosqlite_config_inherited_base_methods_build_expected_contexts() -> None:
    config = AiosqliteConfig()
    config.statement_config = config.statement_config.replace(dialect="sqlite")

    connection_context = config.provide_connection()
    session_context = config.provide_session()

    assert isinstance(connection_context, AiosqliteConnectionContext)
    assert isinstance(session_context, AiosqliteSessionContext)
    assert session_context._statement_config is config.statement_config  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    ("config_type", "base_method"),
    [
        (SqliteConfig, SyncDatabaseConfig.provide_connection),
        (PyMysqlConfig, SyncDatabaseConfig.provide_connection),
        (DuckDBConfig, SyncDatabaseConfig.provide_connection),
        (OracleSyncConfig, SyncDatabaseConfig.provide_connection),
        (MysqlConnectorSyncConfig, SyncDatabaseConfig.provide_connection),
        (AiosqliteConfig, AsyncDatabaseConfig.provide_connection),
        (AsyncmyConfig, AsyncDatabaseConfig.provide_connection),
        (AsyncpgConfig, AsyncDatabaseConfig.provide_connection),
        (PsqlpyConfig, AsyncDatabaseConfig.provide_connection),
        (PsycopgSyncConfig, SyncDatabaseConfig.provide_connection),
        (PsycopgAsyncConfig, AsyncDatabaseConfig.provide_connection),
        (OracleAsyncConfig, AsyncDatabaseConfig.provide_connection),
        (CockroachAsyncpgConfig, AsyncDatabaseConfig.provide_connection),
        (CockroachPsycopgSyncConfig, SyncDatabaseConfig.provide_connection),
        (CockroachPsycopgAsyncConfig, AsyncDatabaseConfig.provide_connection),
    ],
)
def test_pooled_adapters_inherit_base_provide_connection(config_type: type[Any], base_method: Any) -> None:
    assert config_type.provide_connection is base_method


@pytest.mark.parametrize(
    "config_type",
    [
        SqliteConfig,
        PyMysqlConfig,
        DuckDBConfig,
        AiosqliteConfig,
        AsyncmyConfig,
        OracleSyncConfig,
        OracleAsyncConfig,
        MysqlConnectorSyncConfig,
    ],
)
def test_template_only_adapters_inherit_base_provide_session(config_type: type[Any]) -> None:
    base_method = (
        SyncDatabaseConfig.provide_session
        if issubclass(config_type, SyncDatabaseConfig)
        else AsyncDatabaseConfig.provide_session
    )
    assert config_type.provide_session is base_method


@pytest.mark.parametrize(
    "config_type",
    [
        AsyncpgConfig,
        PsqlpyConfig,
        PsycopgSyncConfig,
        PsycopgAsyncConfig,
        CockroachAsyncpgConfig,
        CockroachPsycopgSyncConfig,
        CockroachPsycopgAsyncConfig,
        SpannerSyncConfig,
    ],
)
def test_specialized_adapters_keep_provide_session_override(config_type: type[Any]) -> None:
    base_method = (
        SyncDatabaseConfig.provide_session
        if issubclass(config_type, SyncDatabaseConfig)
        else AsyncDatabaseConfig.provide_session
    )
    assert config_type.provide_session is not base_method
