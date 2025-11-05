"""BigQuery database configuration."""

import contextlib
import logging
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from typing_extensions import NotRequired

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.adapters.bigquery.driver import (
    BigQueryCursor,
    BigQueryDriver,
    BigQueryExceptionHandler,
    build_bigquery_statement_config,
)
from sqlspec.config import ADKConfig, FastAPIConfig, FlaskConfig, LitestarConfig, NoPoolSyncConfig, StarletteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials

    from sqlspec.core import StatementConfig


logger = logging.getLogger(__name__)


class BigQueryConnectionParams(TypedDict):
    """Standard BigQuery connection parameters.

    Includes both official BigQuery client parameters and BigQuery-specific configuration options.
    """

    project: NotRequired[str]
    location: NotRequired[str]
    credentials: NotRequired["Credentials"]
    client_options: NotRequired["ClientOptions"]
    client_info: NotRequired["ClientInfo"]

    default_query_job_config: NotRequired[QueryJobConfig]
    default_load_job_config: NotRequired[LoadJobConfig]
    dataset_id: NotRequired[str]
    credentials_path: NotRequired[str]
    use_query_cache: NotRequired[bool]
    maximum_bytes_billed: NotRequired[int]
    enable_bigquery_ml: NotRequired[bool]
    enable_gemini_integration: NotRequired[bool]
    query_timeout_ms: NotRequired[int]
    job_timeout_ms: NotRequired[int]
    reservation_id: NotRequired[str]
    edition: NotRequired[str]
    enable_cross_cloud: NotRequired[bool]
    enable_bigquery_omni: NotRequired[bool]
    use_avro_logical_types: NotRequired[bool]
    parquet_enable_list_inference: NotRequired[bool]
    enable_column_level_security: NotRequired[bool]
    enable_row_level_security: NotRequired[bool]
    enable_dataframes: NotRequired[bool]
    dataframes_backend: NotRequired[str]
    enable_continuous_queries: NotRequired[bool]
    enable_vector_search: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class BigQueryDriverFeatures(TypedDict):
    """BigQuery driver-specific features configuration.

    Only non-standard BigQuery client parameters that are SQLSpec-specific extensions.

    Attributes:
        connection_instance: Pre-existing BigQuery connection instance to use.
        on_job_start: Callback invoked when a query job starts.
        on_job_complete: Callback invoked when a query job completes.
        on_connection_create: Callback invoked when connection is created.
        json_serializer: Custom JSON serializer for dict/list parameter conversion.
            Defaults to sqlspec.utils.serializers.to_json if not provided.
        enable_uuid_conversion: Enable automatic UUID string conversion.
            When True (default), UUID strings are automatically converted to UUID objects.
            When False, UUID strings are treated as regular strings.
    """

    connection_instance: NotRequired["BigQueryConnection"]
    on_job_start: NotRequired["Callable[[str], None]"]
    on_job_complete: NotRequired["Callable[[str, Any], None]"]
    on_connection_create: NotRequired["Callable[[Any], None]"]
    json_serializer: NotRequired["Callable[[Any], str]"]
    enable_uuid_conversion: NotRequired[bool]


__all__ = ("BigQueryConfig", "BigQueryConnectionParams", "BigQueryDriverFeatures")


class BigQueryConfig(NoPoolSyncConfig[BigQueryConnection, BigQueryDriver]):
    """BigQuery configuration.

    Configuration for Google Cloud BigQuery connections.
    """

    driver_type: ClassVar[type[BigQueryDriver]] = BigQueryDriver
    connection_type: "ClassVar[type[BigQueryConnection]]" = BigQueryConnection
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    requires_staging_for_load: ClassVar[bool] = True
    staging_protocols: ClassVar[tuple[str, ...]] = ("gs://",)

    def __init__(
        self,
        *,
        connection_config: "BigQueryConnectionParams | dict[str, Any] | None" = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "BigQueryDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | LitestarConfig | FastAPIConfig | StarletteConfig | FlaskConfig | ADKConfig | None" = None,
    ) -> None:
        """Initialize BigQuery configuration.

        Args:
            connection_config: Connection configuration parameters
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: BigQuery-specific driver features
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
        """

        self.connection_config: dict[str, Any] = dict(connection_config) if connection_config else {}
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        self.driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}

        if "enable_uuid_conversion" not in self.driver_features:
            self.driver_features["enable_uuid_conversion"] = True

        serializer = self.driver_features.setdefault("json_serializer", to_json)

        self._connection_instance: BigQueryConnection | None = self.driver_features.get("connection_instance")

        if "default_query_job_config" not in self.connection_config:
            self._setup_default_job_config()

        base_statement_config = statement_config or build_bigquery_statement_config(json_serializer=serializer)

        super().__init__(
            connection_config=self.connection_config,
            migration_config=migration_config,
            statement_config=base_statement_config,
            driver_features=self.driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
        )

    def _setup_default_job_config(self) -> None:
        """Set up default job configuration."""

        if self.connection_config.get("default_query_job_config") is not None:
            return

        job_config = QueryJobConfig()

        dataset_id = self.connection_config.get("dataset_id")
        project = self.connection_config.get("project")
        if dataset_id and project and "." not in dataset_id:
            job_config.default_dataset = f"{project}.{dataset_id}"

        use_query_cache = self.connection_config.get("use_query_cache")
        if use_query_cache is not None:
            job_config.use_query_cache = use_query_cache
        else:
            job_config.use_query_cache = True

        maximum_bytes_billed = self.connection_config.get("maximum_bytes_billed")
        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = maximum_bytes_billed

        query_timeout_ms = self.connection_config.get("query_timeout_ms")
        if query_timeout_ms is not None:
            job_config.job_timeout_ms = query_timeout_ms

        self.connection_config["default_query_job_config"] = job_config

    def create_connection(self) -> BigQueryConnection:
        """Create and return a new BigQuery Client instance.

        Returns:
            A new BigQuery Client instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """

        if self._connection_instance is not None:
            return self._connection_instance

        try:
            client_fields = {"project", "location", "credentials", "client_options", "client_info"}
            config_dict: dict[str, Any] = {
                field: value
                for field, value in self.connection_config.items()
                if field in client_fields and value is not None and value is not Empty
            }
            connection = self.connection_type(**config_dict)

            default_query_job_config = self.connection_config.get("default_query_job_config")
            if default_query_job_config is not None:
                self.driver_features["default_query_job_config"] = default_query_job_config

            default_load_job_config = self.connection_config.get("default_load_job_config")
            if default_load_job_config is not None:
                self.driver_features["default_load_job_config"] = default_load_job_config

            on_connection_create = self.driver_features.get("on_connection_create")
            if on_connection_create:
                on_connection_create(connection)

            self._connection_instance = connection
        except Exception as e:
            project = self.connection_config.get("project", "Unknown")
            msg = f"Could not configure BigQuery connection for project '{project}'. Error: {e}"
            raise ImproperConfigurationError(msg) from e
        return connection

    @contextlib.contextmanager
    def provide_connection(self, *_args: Any, **_kwargs: Any) -> "Generator[BigQueryConnection, None, None]":
        """Provide a BigQuery client within a context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A BigQuery Client instance.
        """
        connection = self.create_connection()
        yield connection

    @contextlib.contextmanager
    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "Generator[BigQueryDriver, None, None]":
        """Provide a BigQuery driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A context manager that yields a BigQueryDriver instance.
        """

        with self.provide_connection(*_args, **_kwargs) as connection:
            final_statement_config = statement_config or self.statement_config

            driver = self.driver_type(
                connection=connection, statement_config=final_statement_config, driver_features=self.driver_features
            )
            yield driver

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for BigQuery types.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "BigQueryConnection": BigQueryConnection,
            "BigQueryConnectionParams": BigQueryConnectionParams,
            "BigQueryCursor": BigQueryCursor,
            "BigQueryDriver": BigQueryDriver,
            "BigQueryExceptionHandler": BigQueryExceptionHandler,
        })
        return namespace
