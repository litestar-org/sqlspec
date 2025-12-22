"""ADBC database configuration."""

from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.adbc._types import AdbcConnection
from sqlspec.adapters.adbc.driver import AdbcCursor, AdbcDriver, AdbcExceptionHandler, get_adbc_statement_config
from sqlspec.config import ExtensionConfigs, NoPoolSyncConfig
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events._hints import EventRuntimeHints
from sqlspec.utils.config_normalization import normalize_connection_config
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Generator
    from contextlib import AbstractContextManager

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.observability import ObservabilityConfig

logger = get_logger("adapters.adbc")

_DRIVER_ALIASES: dict[str, str] = {
    "sqlite": "adbc_driver_sqlite.dbapi.connect",
    "sqlite3": "adbc_driver_sqlite.dbapi.connect",
    "duckdb": "adbc_driver_duckdb.dbapi.connect",
    "postgres": "adbc_driver_postgresql.dbapi.connect",
    "postgresql": "adbc_driver_postgresql.dbapi.connect",
    "pg": "adbc_driver_postgresql.dbapi.connect",
    "snowflake": "adbc_driver_snowflake.dbapi.connect",
    "sf": "adbc_driver_snowflake.dbapi.connect",
    "bigquery": "adbc_driver_bigquery.dbapi.connect",
    "bq": "adbc_driver_bigquery.dbapi.connect",
    "flightsql": "adbc_driver_flightsql.dbapi.connect",
    "grpc": "adbc_driver_flightsql.dbapi.connect",
}

_URI_PREFIX_DRIVER: tuple[tuple[str, str], ...] = (
    ("postgresql://", "adbc_driver_postgresql.dbapi.connect"),
    ("postgres://", "adbc_driver_postgresql.dbapi.connect"),
    ("sqlite://", "adbc_driver_sqlite.dbapi.connect"),
    ("duckdb://", "adbc_driver_duckdb.dbapi.connect"),
    ("grpc://", "adbc_driver_flightsql.dbapi.connect"),
    ("snowflake://", "adbc_driver_snowflake.dbapi.connect"),
    ("bigquery://", "adbc_driver_bigquery.dbapi.connect"),
)

_DRIVER_PATH_KEYWORDS_TO_DIALECT: tuple[tuple[str, str], ...] = (
    ("postgresql", "postgres"),
    ("sqlite", "sqlite"),
    ("duckdb", "duckdb"),
    ("bigquery", "bigquery"),
    ("snowflake", "snowflake"),
    ("flightsql", "sqlite"),
    ("grpc", "sqlite"),
)

_PARAMETER_STYLES_BY_KEYWORD: tuple[tuple[str, tuple[tuple[str, ...], str]], ...] = (
    ("postgresql", (("numeric",), "numeric")),
    ("sqlite", (("qmark", "named_colon"), "qmark")),
    ("duckdb", (("qmark", "numeric"), "qmark")),
    ("bigquery", (("named_at",), "named_at")),
    ("snowflake", (("qmark", "numeric"), "qmark")),
)

_BIGQUERY_DB_KWARGS_FIELDS: tuple[str, ...] = ("project_id", "dataset_id", "token")


class AdbcConnectionParams(TypedDict):
    """ADBC connection parameters."""

    uri: NotRequired[str]
    driver_name: NotRequired[str]
    db_kwargs: NotRequired[dict[str, Any]]
    conn_kwargs: NotRequired[dict[str, Any]]
    adbc_driver_manager_entrypoint: NotRequired[str]
    autocommit: NotRequired[bool]
    isolation_level: NotRequired[str]
    batch_size: NotRequired[int]
    query_timeout: NotRequired[float]
    connection_timeout: NotRequired[float]
    ssl_mode: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_key: NotRequired[str]
    ssl_ca: NotRequired[str]
    username: NotRequired[str]
    password: NotRequired[str]
    token: NotRequired[str]
    project_id: NotRequired[str]
    dataset_id: NotRequired[str]
    account: NotRequired[str]
    warehouse: NotRequired[str]
    database: NotRequired[str]
    schema: NotRequired[str]
    role: NotRequired[str]
    authorization_header: NotRequired[str]
    grpc_options: NotRequired[dict[str, Any]]
    extra: NotRequired[dict[str, Any]]


class AdbcDriverFeatures(TypedDict):
    """ADBC driver feature configuration.

    Controls optional type handling and serialization behavior for the ADBC adapter.
    These features configure how data is converted between Python and Arrow types.

    Attributes:
        json_serializer: JSON serialization function to use.
            Callable that takes Any and returns str (JSON string).
            Default: sqlspec.utils.serializers.to_json
        enable_cast_detection: Enable cast-aware parameter processing.
            When True, detects SQL casts (e.g., ::JSONB) and applies appropriate
            serialization. Currently used for PostgreSQL JSONB handling.
            Default: True
        strict_type_coercion: Enforce strict type coercion rules.
            When True, raises errors for unsupported type conversions.
            When False, attempts best-effort conversion.
            Default: False
        arrow_extension_types: Enable PyArrow extension type support.
            When True, preserves Arrow extension type metadata when reading data.
            When False, falls back to storage types.
            Default: True
        enable_events: Enable database event channel support.
            Defaults to True when extension_config["events"] is configured.
            Provides pub/sub capabilities via table-backed queue (ADBC has no native pub/sub).
            Requires extension_config["events"] for migration setup.
        events_backend: Event channel backend selection.
            Only option: "table_queue" (durable table-backed queue with retries and exactly-once delivery).
            ADBC does not have native pub/sub, so table_queue is the only backend.
            Defaults to "table_queue".
    """

    json_serializer: "NotRequired[Callable[[Any], str]]"
    enable_cast_detection: NotRequired[bool]
    strict_type_coercion: NotRequired[bool]
    arrow_extension_types: NotRequired[bool]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["table_queue"]]


__all__ = ("AdbcConfig", "AdbcConnectionParams", "AdbcDriverFeatures")


class AdbcConfig(NoPoolSyncConfig[AdbcConnection, AdbcDriver]):
    """ADBC configuration for Arrow Database Connectivity.

    ADBC provides an interface for connecting to multiple database systems
    with Arrow-native data transfer.

    Supports multiple database backends including PostgreSQL, SQLite, DuckDB,
    BigQuery, and Snowflake with automatic driver detection and loading.
    """

    driver_type: ClassVar[type[AdbcDriver]] = AdbcDriver
    connection_type: "ClassVar[type[AdbcConnection]]" = AdbcConnection
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    storage_partition_strategies: "ClassVar[tuple[str, ...]]" = ("fixed", "rows_per_chunk")

    def __init__(
        self,
        *,
        connection_config: AdbcConnectionParams | dict[str, Any] | None = None,
        connection_instance: "Any" = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: StatementConfig | None = None,
        driver_features: "AdbcDriverFeatures | dict[str, Any] | None" = None,
        bind_key: str | None = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize configuration.

        Args:
            connection_config: Connection configuration parameters
            connection_instance: Pre-created connection instance to use instead of creating new one
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Driver feature configuration (AdbcDriverFeatures)
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """
        self.connection_config = normalize_connection_config(connection_config)

        if statement_config is None:
            detected_dialect = str(self._get_dialect() or "sqlite")
            statement_config = get_adbc_statement_config(detected_dialect)

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        json_serializer = processed_driver_features.setdefault("json_serializer", to_json)
        processed_driver_features.setdefault("enable_cast_detection", True)
        processed_driver_features.setdefault("strict_type_coercion", False)
        processed_driver_features.setdefault("arrow_extension_types", True)

        # Auto-detect events support based on extension_config
        processed_driver_features.setdefault("enable_events", "events" in (extension_config or {}))
        processed_driver_features.setdefault("events_backend", "table_queue")

        if json_serializer is not None:
            statement_config = _apply_json_serializer_to_statement_config(statement_config, json_serializer)

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def _resolve_driver_name(self) -> str:
        """Resolve and normalize the driver name.

        Returns:
            The normalized driver connect function path.
        """
        driver_name = self.connection_config.get("driver_name")
        uri = self.connection_config.get("uri")

        if isinstance(driver_name, str):
            lowered_driver = driver_name.lower()
            alias = _DRIVER_ALIASES.get(lowered_driver)
            if alias is not None:
                return alias
            return _normalize_driver_path(driver_name)

        if isinstance(uri, str):
            resolved = _driver_from_uri(uri)
            if resolved is not None:
                return resolved

        return "adbc_driver_sqlite.dbapi.connect"

    def _get_connect_func(self) -> Callable[..., AdbcConnection]:
        """Get the driver connect function.

        Returns:
            The driver connect function.

        Raises:
            ImproperConfigurationError: If driver cannot be loaded.
        """
        driver_path = self._resolve_driver_name()

        try:
            connect_func = import_string(driver_path)
        except ImportError as e:
            msg = f"Failed to import connect function from '{driver_path}'. Is the driver installed? Error: {e}"
            raise ImproperConfigurationError(msg) from e

        if not callable(connect_func):
            msg = f"The path '{driver_path}' did not resolve to a callable function."
            raise ImproperConfigurationError(msg)

        return cast("Callable[..., AdbcConnection]", connect_func)

    def _get_dialect(self) -> "DialectType":
        """Get the SQL dialect type based on the driver.

        Returns:
            The SQL dialect type for the driver.
        """
        driver_path = self._resolve_driver_name()
        for keyword, dialect in _DRIVER_PATH_KEYWORDS_TO_DIALECT:
            if keyword in driver_path:
                return dialect
        return None

    def _get_parameter_styles(self) -> tuple[tuple[str, ...], str]:
        """Get parameter styles based on the underlying driver.

        Returns:
            Tuple of (supported_parameter_styles, default_parameter_style)
        """
        try:
            driver_path = self._resolve_driver_name()
            for keyword, styles in _PARAMETER_STYLES_BY_KEYWORD:
                if keyword in driver_path:
                    return styles

        except Exception:  # pylint: disable=broad-exception-caught
            logger.debug("Error resolving parameter styles, using defaults", exc_info=True)
        return (("qmark",), "qmark")

    def create_connection(self) -> AdbcConnection:
        """Create and return a new connection using the specified driver.

        Returns:
            A new connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """

        try:
            connect_func = self._get_connect_func()
            connection_config_dict = self._get_connection_config_dict()
            connection = connect_func(**connection_config_dict)
        except Exception as e:
            driver_name = self.connection_config.get("driver_name", "Unknown")
            msg = f"Could not configure connection using driver '{driver_name}'. Error: {e}"
            raise ImproperConfigurationError(msg) from e
        return connection

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[AdbcConnection, None, None]":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A connection instance.
        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractContextManager[AdbcDriver]":
        """Provide a driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Returns:
            A context manager that yields an AdbcDriver instance.
        """

        @contextmanager
        def session_manager() -> "Generator[AdbcDriver, None, None]":
            with self.provide_connection(*args, **kwargs) as connection:
                final_statement_config = (
                    statement_config
                    or self.statement_config
                    or get_adbc_statement_config(str(self._get_dialect() or "sqlite"))
                )
                driver = self.driver_type(
                    connection=connection, statement_config=final_statement_config, driver_features=self.driver_features
                )
                yield self._prepare_driver(driver)

        return session_manager()

    def _get_connection_config_dict(self) -> dict[str, Any]:
        """Get the connection configuration dictionary.

        Returns:
            The connection configuration dictionary.
        """
        config = dict(self.connection_config)

        driver_name = config.get("driver_name")
        uri = config.get("uri")
        driver_kind: str | None = None
        if isinstance(driver_name, str):
            driver_kind = _driver_kind_from_driver_name(driver_name)
        if driver_kind is None and isinstance(uri, str):
            driver_kind = _driver_kind_from_uri(uri)

        if isinstance(uri, str) and driver_kind == "sqlite" and uri.startswith("sqlite://"):
            config["uri"] = uri[9:]
        if isinstance(uri, str) and driver_kind == "duckdb" and uri.startswith("duckdb://"):
            config["path"] = uri[9:]
            config.pop("uri", None)

        if isinstance(driver_name, str) and driver_kind == "bigquery":
            db_kwargs = config.get("db_kwargs")
            db_kwargs_dict: dict[str, Any] = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
            for param in _BIGQUERY_DB_KWARGS_FIELDS:
                if param in config:
                    db_kwargs_dict[param] = config.pop(param)
            if db_kwargs_dict:
                config["db_kwargs"] = db_kwargs_dict
        elif isinstance(driver_name, str) and "db_kwargs" in config and driver_kind != "bigquery":
            db_kwargs = config.pop("db_kwargs")
            if isinstance(db_kwargs, dict):
                config.update(db_kwargs)

        config.pop("driver_name", None)

        return config

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "AdbcConnection": AdbcConnection,
            "AdbcConnectionParams": AdbcConnectionParams,
            "AdbcCursor": AdbcCursor,
            "AdbcDriver": AdbcDriver,
            "AdbcExceptionHandler": AdbcExceptionHandler,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults suitable for ADBC warehouses."""

        return EventRuntimeHints(poll_interval=2.0, lease_seconds=60, retention_seconds=172_800)


def _apply_json_serializer_to_statement_config(
    statement_config: "StatementConfig", json_serializer: "Callable[[Any], str]"
) -> "StatementConfig":
    """Apply a JSON serializer to statement config while preserving list/tuple converters.

    Args:
        statement_config: Base statement configuration to update.
        json_serializer: JSON serializer function.

    Returns:
        Updated statement configuration.
    """
    parameter_config = statement_config.parameter_config
    previous_list_converter = parameter_config.type_coercion_map.get(list)
    previous_tuple_converter = parameter_config.type_coercion_map.get(tuple)

    updated_parameter_config = parameter_config.with_json_serializers(json_serializer)
    updated_map = dict(updated_parameter_config.type_coercion_map)

    if previous_list_converter is not None:
        updated_map[list] = previous_list_converter
    if previous_tuple_converter is not None:
        updated_map[tuple] = previous_tuple_converter

    return statement_config.replace(parameter_config=updated_parameter_config.replace(type_coercion_map=updated_map))


def _normalize_driver_path(driver_name: str) -> str:
    """Normalize a driver name to an importable connect function path.

    Args:
        driver_name: Driver name or dotted import path.

    Returns:
        A dotted path to a driver connect function.
    """
    stripped = driver_name.strip()
    if stripped.endswith(".dbapi.connect"):
        return stripped
    if stripped.endswith(".dbapi"):
        return f"{stripped}.connect"
    if "." in stripped:
        return stripped
    return f"{stripped}.dbapi.connect"


def _driver_from_uri(uri: str) -> str | None:
    """Resolve a default driver connect path from a URI.

    Args:
        uri: Connection URI.

    Returns:
        Dotted connect function path if a scheme matches, otherwise None.
    """
    for prefix, driver_path in _URI_PREFIX_DRIVER:
        if uri.startswith(prefix):
            return driver_path
    return None


def _driver_kind_from_driver_name(driver_name: str) -> str | None:
    """Return a canonical driver kind based on driver name content.

    Args:
        driver_name: Driver name or dotted path.

    Returns:
        Canonical driver kind string or None.
    """
    resolved = _DRIVER_ALIASES.get(driver_name.lower(), driver_name)
    lowered = resolved.lower()
    for keyword, _dialect in _DRIVER_PATH_KEYWORDS_TO_DIALECT:
        if keyword in lowered:
            return keyword
    return None


def _driver_kind_from_uri(uri: str) -> str | None:
    """Return a canonical driver kind based on URI scheme.

    Args:
        uri: Connection URI.

    Returns:
        Canonical driver kind string or None.
    """
    for prefix, driver_path in _URI_PREFIX_DRIVER:
        if uri.startswith(prefix):
            return _driver_kind_from_driver_name(driver_path)
    return None
