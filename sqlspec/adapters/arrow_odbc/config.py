"""arrow-odbc database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection, ArrowOdbcSessionContext, arrow_odbc_connect
from sqlspec.adapters.arrow_odbc.core import (
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    default_statement_config,
    resolve_dialect_from_dbms_name,
)
from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver
from sqlspec.config import ExtensionConfigs, NoPoolSyncConfig
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.protocols import SupportsCloseProtocol
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("ArrowOdbcConfig", "ArrowOdbcConnectionParams", "ArrowOdbcDriverFeatures")


class ArrowOdbcConnectionParams(TypedDict):
    """arrow-odbc connection parameters."""

    connection_string: NotRequired[str]
    dsn: NotRequired[str]
    driver: NotRequired[str]
    server: NotRequired[str]
    host: NotRequired[str]
    database: NotRequired[str]
    uid: NotRequired[str]
    pwd: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    trusted_connection: NotRequired[bool | str]
    trust_server_certificate: NotRequired[bool | str]
    encrypt: NotRequired[bool | str]
    login_timeout: NotRequired[int]
    login_timeout_sec: NotRequired[int]
    packet_size: NotRequired[int]
    autocommit: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class ArrowOdbcDriverFeatures(TypedDict):
    """arrow-odbc driver feature flags."""

    chunk_size: NotRequired[int]
    max_bytes_per_batch: NotRequired[int]
    max_text_size: NotRequired[int]
    max_binary_size: NotRequired[int]
    fetch_concurrently: NotRequired[bool]
    query_timeout_sec: NotRequired[int]
    connection_string: NotRequired[str]
    dbms_name: NotRequired[str]
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    enable_events: NotRequired[bool]
    on_connection_create: "NotRequired[Callable[[ArrowOdbcConnection], None]]"


def _apply_json_serializer_override(statement_config: Any, features: dict[str, Any]) -> Any:
    serializer = cast("Callable[[Any], str] | None", features.get("json_serializer"))
    deserializer = cast("Callable[[str], Any] | None", features.get("json_deserializer"))
    if serializer is to_json and deserializer is from_json:
        return statement_config
    return statement_config.replace(
        parameter_config=statement_config.parameter_config.with_json_serializers(
            serializer or to_json, deserializer=deserializer
        )
    )


class ArrowOdbcConnectionContext(SyncPoolConnectionContext):
    """Context manager for arrow-odbc connections."""

    __slots__ = ("_connection",)

    def __init__(self, config: "ArrowOdbcConfig") -> None:
        super().__init__(config)
        self._connection: ArrowOdbcConnection | None = None

    def __enter__(self) -> "ArrowOdbcConnection":
        self._connection = self._config.create_connection()
        return cast("ArrowOdbcConnection", self._connection)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            _close_arrow_odbc_connection(self._connection)
            self._connection = None
        return None


class _ArrowOdbcSessionConnectionHandler(SyncPoolSessionFactory):
    """Session connection handler for no-pool arrow-odbc sessions."""

    __slots__ = ("_connection",)

    def __init__(self, config: "ArrowOdbcConfig") -> None:
        super().__init__(config)
        self._connection: ArrowOdbcConnection | None = None

    def acquire_connection(self) -> "ArrowOdbcConnection":
        self._connection = self._config.create_connection()
        return cast("ArrowOdbcConnection", self._connection)

    def release_connection(self, _conn: "ArrowOdbcConnection", **kwargs: Any) -> None:
        if self._connection is None:
            return
        _close_arrow_odbc_connection(self._connection)
        self._connection = None


class ArrowOdbcConfig(NoPoolSyncConfig[ArrowOdbcConnection, ArrowOdbcDriver]):
    """Configuration for synchronous arrow-odbc connections."""

    driver_type: "ClassVar[type[ArrowOdbcDriver]]" = ArrowOdbcDriver
    connection_type: "ClassVar[type[ArrowOdbcConnection]]" = ArrowOdbcConnection
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_arrow_streaming: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    _connection_context_class: "ClassVar[type[ArrowOdbcConnectionContext]]" = ArrowOdbcConnectionContext
    _session_factory_class: "ClassVar[type[_ArrowOdbcSessionConnectionHandler]]" = _ArrowOdbcSessionConnectionHandler
    _session_context_class: "ClassVar[type[ArrowOdbcSessionContext]]" = ArrowOdbcSessionContext
    _default_statement_config = default_statement_config
    __slots__ = ("_user_connection_hook",)

    def __init__(
        self,
        *,
        connection_config: "ArrowOdbcConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "ArrowOdbcConnection | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "ArrowOdbcDriverFeatures | dict[str, Any] | None" = None,
        bind_key: str | None = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize arrow-odbc configuration."""
        normalized = normalize_connection_config(connection_config)
        provided_statement_config = statement_config
        statement_config, features = apply_driver_features(
            statement_config or default_statement_config, driver_features
        )
        connection_string = normalized.get("connection_string")
        if connection_string is not None:
            features.setdefault("connection_string", str(connection_string))
        elif normalized.get("driver") is not None:
            features.setdefault("dbms_name", str(normalized["driver"]))
        if provided_statement_config is None:
            statement_config = _resolve_statement_config(features)
        self._user_connection_hook = cast(
            "Callable[[ArrowOdbcConnection], None] | None", features.pop("on_connection_create", None)
        )

        super().__init__(
            connection_config=normalized,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_connection(self) -> "ArrowOdbcConnection":
        """Create and return a new arrow-odbc connection."""
        if self.connection_instance is not None:
            return cast("ArrowOdbcConnection", self.connection_instance)
        connection_string, connect_kwargs = build_connection_config(self.connection_config)
        try:
            connection = cast("ArrowOdbcConnection", arrow_odbc_connect(connection_string, **connect_kwargs))
            if self._user_connection_hook is not None:
                self._user_connection_hook(connection)
            return cast("ArrowOdbcConnection", connection)
        except Exception as exc:
            msg = f"Could not configure arrow-odbc connection. Error: {exc}"
            raise ImproperConfigurationError(msg) from exc

    def provide_connection(self, *args: Any, **kwargs: Any) -> "ArrowOdbcConnectionContext":
        """Provide a connection context manager."""
        return ArrowOdbcConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "ArrowOdbcSessionContext":
        """Provide a driver session context manager."""
        handler = _ArrowOdbcSessionConnectionHandler(self)
        return ArrowOdbcSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for ArrowOdbcConfig types."""
        namespace = super().get_signature_namespace()
        namespace.update({
            "ArrowOdbcConfig": ArrowOdbcConfig,
            "ArrowOdbcConnection": ArrowOdbcConnection,
            "ArrowOdbcConnectionContext": ArrowOdbcConnectionContext,
            "ArrowOdbcConnectionParams": ArrowOdbcConnectionParams,
            "ArrowOdbcDriver": ArrowOdbcDriver,
            "ArrowOdbcDriverFeatures": ArrowOdbcDriverFeatures,
            "ArrowOdbcSessionContext": ArrowOdbcSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults suitable for generic ODBC sources."""
        return EventRuntimeHints(poll_interval=2.0, lease_seconds=60, retention_seconds=172_800)


def _close_arrow_odbc_connection(connection: "ArrowOdbcConnection") -> None:
    """Close connection objects from compatible wrappers when they expose close()."""
    if isinstance(connection, SupportsCloseProtocol):
        connection.close()


def _resolve_statement_config(features: dict[str, Any]) -> "StatementConfig":
    dialect = resolve_dialect_from_dbms_name(str(features.get("dbms_name") or features.get("connection_string") or ""))
    if dialect == "sqlite":
        return _apply_json_serializer_override(default_statement_config, features)
    if dialect == "mssql":
        return _apply_json_serializer_override(build_statement_config(dialect="tsql"), features)
    return _apply_json_serializer_override(build_statement_config(dialect=dialect), features)
