"""PyMySQL database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.pymysql._typing import PyMysqlConnection, PyMysqlSessionContext
from sqlspec.adapters.pymysql.core import apply_driver_features, default_statement_config
from sqlspec.adapters.pymysql.driver import PyMysqlCursor, PyMysqlDriver, PyMysqlExceptionHandler
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("PyMysqlConfig", "PyMysqlConnectionParams", "PyMysqlDriverFeatures", "PyMysqlPoolParams")


class PyMysqlConnectionParams(TypedDict):
    """PyMySQL connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connect_timeout: NotRequired[int]
    read_timeout: NotRequired[int]
    write_timeout: NotRequired[int]
    autocommit: NotRequired[bool]
    ssl: NotRequired["dict[str, Any]"]
    client_flag: NotRequired[int]
    cursorclass: NotRequired[type]
    init_command: NotRequired[str]
    sql_mode: NotRequired[str]
    extra: NotRequired["dict[str, Any]"]


class PyMysqlPoolParams(PyMysqlConnectionParams):
    """PyMySQL pool parameters."""

    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]


class PyMysqlDriverFeatures(TypedDict):
    """PyMySQL driver feature flags."""

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class PyMysqlConnectionContext:
    """Context manager for PyMySQL connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "PyMysqlConfig") -> None:
        self._config = config
        self._ctx: Any = None

    def __enter__(self) -> PyMysqlConnection:
        pool = self._config.provide_pool()
        self._ctx = pool.get_connection()
        return cast("PyMysqlConnection", self._ctx.__enter__())

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._ctx:
            return cast("bool | None", self._ctx.__exit__(exc_type, exc_val, exc_tb))
        return None


class _PyMysqlSessionConnectionHandler:
    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "PyMysqlConfig") -> None:
        self._config = config
        self._ctx: Any = None

    def acquire_connection(self) -> "PyMysqlConnection":
        pool = self._config.provide_pool()
        self._ctx = pool.get_connection()
        return cast("PyMysqlConnection", self._ctx.__enter__())

    def release_connection(self, _conn: "PyMysqlConnection") -> None:
        if self._ctx is None:
            return
        self._ctx.__exit__(None, None, None)
        self._ctx = None


class PyMysqlConfig(SyncDatabaseConfig[PyMysqlConnection, PyMysqlConnectionPool, PyMysqlDriver]):
    """Configuration for PyMySQL synchronous connections."""

    driver_type: "ClassVar[type[PyMysqlDriver]]" = PyMysqlDriver
    connection_type: "ClassVar[type[PyMysqlConnection]]" = cast("type[PyMysqlConnection]", PyMysqlConnection)
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        connection_config: "PyMysqlPoolParams | dict[str, Any] | None" = None,
        connection_instance: "PyMysqlConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PyMysqlDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = normalize_connection_config(connection_config)
        connection_config.setdefault("host", "localhost")
        connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def _create_pool(self) -> "PyMysqlConnectionPool":
        config = dict(self.connection_config)
        pool_recycle = config.pop("pool_recycle_seconds", 86400)
        health_check = config.pop("health_check_interval", 30.0)
        extra = config.pop("extra", {})
        config.update(extra)
        return PyMysqlConnectionPool(config, recycle_seconds=pool_recycle, health_check_interval=health_check)

    def _close_pool(self) -> None:
        if self.connection_instance:
            self.connection_instance.close()

    def create_connection(self) -> PyMysqlConnection:
        pool = self.provide_pool()
        return pool.acquire()

    def provide_connection(self, *args: Any, **kwargs: Any) -> "PyMysqlConnectionContext":
        return PyMysqlConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "PyMysqlSessionContext":
        handler = _PyMysqlSessionConnectionHandler(self)

        return PyMysqlSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "PyMysqlConnectionContext": PyMysqlConnectionContext,
            "PyMysqlConnection": PyMysqlConnection,
            "PyMysqlConnectionParams": PyMysqlConnectionParams,
            "PyMysqlConnectionPool": PyMysqlConnectionPool,
            "PyMysqlCursor": PyMysqlCursor,
            "PyMysqlDriver": PyMysqlDriver,
            "PyMysqlDriverFeatures": PyMysqlDriverFeatures,
            "PyMysqlExceptionHandler": PyMysqlExceptionHandler,
            "PyMysqlPoolParams": PyMysqlPoolParams,
            "PyMysqlSessionContext": PyMysqlSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)
