"""arrow-odbc database configuration."""

import threading
from typing import TYPE_CHECKING, Any, ClassVar, NotRequired, TypedDict, cast

from sqlspec.adapters.arrow_odbc._typing import (
    ArrowOdbcConnection,
    ArrowOdbcSessionContext,
    TextEncoding,
    arrow_odbc_connect,
    enable_odbc_connection_pooling,
)
from sqlspec.adapters.arrow_odbc.core import (
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    default_statement_config,
    resolve_dialect_from_dbms_name,
)
from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcDriver
from sqlspec.config import ExtensionConfigs, NoPoolSyncConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import supports_close

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("ArrowOdbcConfig", "ArrowOdbcConnectionParams", "ArrowOdbcDriverFeatures")


_DRIVER_POOLING_ENABLED = False
_DRIVER_POOLING_LOCK = threading.Lock()


class ArrowOdbcConnectionParams(TypedDict):
    """ODBC connection parameters compiled into an ``arrow-odbc`` connection string."""

    connection_string: NotRequired[str]
    """Full ODBC ``KEY=VALUE;...`` connection string overridden by explicit keyword fields."""
    dsn: NotRequired[str]
    """Registered ODBC Data Source Name (``DSN=...``)."""
    driver: NotRequired[str]
    """Installed ODBC driver name or library path (``Driver={...}``)."""
    server: NotRequired[str]
    """Target database server address or ``host,port`` endpoint."""
    host: NotRequired[str]
    """Hostname alias used when ``server`` is not specified."""
    database: NotRequired[str]
    """Target catalog or database name (``Database=...``)."""
    uid: NotRequired[str]
    """ODBC authentication user identifier (``UID=...``)."""
    pwd: NotRequired[str]
    """ODBC authentication password (``PWD=...``)."""
    user: NotRequired[str]
    """Alias normalized to ``uid`` when building the ODBC connection string."""
    password: NotRequired[str]
    """Alias normalized to ``pwd`` when building the ODBC connection string."""
    trusted_connection: NotRequired[bool | str]
    """Use integrated OS or Kerberos authentication (``Trusted_Connection``)."""
    trust_server_certificate: NotRequired[bool | str]
    """Trust self-signed server TLS certificates without CA chain validation."""
    encrypt: NotRequired[bool | str]
    """Require or configure TLS encryption on the ODBC connection."""
    login_timeout: NotRequired[int]
    """Connection login timeout in seconds forwarded to ``arrow_odbc.connect``."""
    login_timeout_sec: NotRequired[int]
    """Alias for ``login_timeout`` in seconds."""
    packet_size: NotRequired[int]
    """Network packet size in bytes negotiated by the ODBC driver."""
    autocommit: NotRequired[bool]
    """Initial connection autocommit state (defaults to ``True``)."""
    extra: NotRequired[dict[str, Any]]
    """Additional ODBC connection-string attributes merged verbatim."""


class ArrowOdbcDriverFeatures(TypedDict):
    """``arrow-odbc`` driver feature flags controlling Arrow batch sizing, dialect hints, and codecs."""

    chunk_size: NotRequired[int]
    """Maximum rows per Arrow ``RecordBatch`` fetched from ODBC cursors (defaults to ``65535``)."""
    max_bytes_per_batch: NotRequired[int]
    """Upper memory budget in bytes per Arrow ``RecordBatch`` buffer."""
    max_text_size: NotRequired[int]
    """Maximum byte length allocated for unbounded ``VARCHAR(MAX)`` / ``TEXT`` columns."""
    max_binary_size: NotRequired[int]
    """Maximum byte length allocated for unbounded ``VARBINARY(MAX)`` / ``BLOB`` columns."""
    fetch_concurrently: NotRequired[bool]
    """Prefetch the next ODBC batch on a background Rust thread while Arrow batches are consumed."""
    query_timeout_sec: NotRequired[int]
    """Per-statement ODBC query execution timeout in seconds."""
    payload_text_encoding: NotRequired["TextEncoding"]
    """Wire text encoding override (``'auto'``, ``'utf8'``, or ``'utf16'``) passed to ``arrow-odbc``."""
    enable_driver_pooling: NotRequired[bool]
    """Enable process-global ODBC driver-manager connection pooling prior to first connect (defaults to ``False``)."""
    connection_string: NotRequired[str]
    """Resolved connection string inspected for SQL dialect auto-detection."""
    dbms_name: NotRequired[str]
    """Database engine hint (such as ``'PostgreSQL'``, ``'Microsoft SQL Server'``, ``'Db2'``, or ``'SQLite'``) selecting the SQLGlot dialect."""
    connection_autocommit: NotRequired[bool]
    """Effective autocommit baseline derived from ``connection_config['autocommit']``."""
    enable_lowercase_column_names: NotRequired[bool]
    """Normalize unquoted uppercase column names to lowercase (defaults to ``True`` on Db2 and ``False`` elsewhere)."""
    json_serializer: "NotRequired[Callable[[Any], str]]"
    """Serializer converting dict/list parameters to JSON text (defaults to ``sqlspec.utils.serializers.to_json``)."""
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    """Deserializer parsing JSON text payloads (defaults to ``sqlspec.utils.serializers.from_json``)."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec table-backed event channel support."""
    on_connection_create: "NotRequired[Callable[[ArrowOdbcConnection], None]]"
    """Callback invoked with the raw ``arrow_odbc.Connection`` immediately after connect."""


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
    supports_native_row_streaming: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_reliable_rowcount: "ClassVar[bool]" = False
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="iso_text", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
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
        effective_config = dict(normalized)
        extra = effective_config.get("extra")
        if isinstance(extra, dict):
            effective_config.update(extra)
        provided_statement_config = statement_config
        statement_config, features = apply_driver_features(
            statement_config or default_statement_config, driver_features
        )
        connection_string = effective_config.get("connection_string")
        if connection_string is not None:
            features.setdefault("connection_string", str(connection_string))
        if effective_config.get("driver") is not None:
            features.setdefault("dbms_name", str(effective_config["driver"]))
        elif effective_config.get("dsn") is not None and (
            connection_string is None or resolve_dialect_from_dbms_name(str(connection_string)) == "sqlite"
        ):
            features.setdefault("dbms_name", str(effective_config["dsn"]))
        features["connection_autocommit"] = bool(effective_config.get("autocommit", True))
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
        try:
            _apply_driver_pooling(bool(self.driver_features.get("enable_driver_pooling", False)))
            dialect = _resolve_connection_dialect_hint(self.driver_features)
            connection_string, connect_kwargs = build_connection_config(self.connection_config, dialect=dialect)
            connection = cast("ArrowOdbcConnection", arrow_odbc_connect(connection_string, **connect_kwargs))
            if self._user_connection_hook is not None:
                self._user_connection_hook(connection)
            return cast("ArrowOdbcConnection", connection)
        except ImproperConfigurationError:
            raise
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


def _apply_driver_pooling(requested: bool) -> None:
    """Apply the ODBC driver manager's process-global connection pooling switch.

    The upstream switch can only be turned on once, before the first connection
    in the process. Declining it is therefore not a decision that can be latched:
    only an opt-in is recorded, and a later configuration that does not ask for
    pooling simply inherits it.

    Args:
        requested: Whether this configuration asks for driver-level pooling.
    """
    global _DRIVER_POOLING_ENABLED
    if not requested:
        return
    with _DRIVER_POOLING_LOCK:
        if _DRIVER_POOLING_ENABLED:
            return
        enable_odbc_connection_pooling()
        _DRIVER_POOLING_ENABLED = True


def _apply_json_serializer_override(statement_config: "StatementConfig", features: dict[str, Any]) -> "StatementConfig":
    serializer = cast("Callable[[Any], str] | None", features.get("json_serializer"))
    deserializer = cast("Callable[[str], Any] | None", features.get("json_deserializer"))
    if serializer is to_json and deserializer is from_json:
        return statement_config
    return statement_config.replace(
        parameter_config=statement_config.parameter_config.with_json_serializers(
            serializer or to_json, deserializer=deserializer
        )
    )


def _close_arrow_odbc_connection(connection: "ArrowOdbcConnection") -> None:
    """Close connection objects from compatible wrappers when they expose close()."""
    if supports_close(connection):
        connection.close()


def _resolve_connection_dialect_hint(features: "Mapping[str, Any]") -> str | None:
    for key in ("dbms_name", "connection_string"):
        value = features.get(key)
        if not value:
            continue
        resolved = resolve_dialect_from_dbms_name(str(value))
        if resolved != "sqlite" or "sqlite" in str(value).lower():
            return resolved
    return None


def _resolve_statement_config(features: dict[str, Any]) -> "StatementConfig":
    dialect = _resolve_connection_dialect_hint(features) or "sqlite"
    if dialect == "sqlite":
        return _apply_json_serializer_override(default_statement_config, features)
    if dialect == "mssql":
        return _apply_json_serializer_override(build_statement_config(dialect="tsql"), features)
    return _apply_json_serializer_override(build_statement_config(dialect=dialect), features)
