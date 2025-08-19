"""BigQuery database configuration."""

import contextlib
import logging
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict, Union

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from typing_extensions import NotRequired

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.adapters.bigquery.driver import BigQueryCursor, BigQueryDriver, bigquery_statement_config
from sqlspec.config import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials

    from sqlspec.core.statement import StatementConfig


logger = logging.getLogger(__name__)


class BigQueryConnectionParams(TypedDict, total=False):
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


class BigQueryDriverFeatures(TypedDict, total=False):
    """BigQuery driver-specific features configuration.

    Only non-standard BigQuery client parameters that are SQLSpec-specific extensions.
    """

    connection_instance: NotRequired["BigQueryConnection"]
    on_job_start: NotRequired["Callable[[str], None]"]
    on_job_complete: NotRequired["Callable[[str, Any], None]"]
    on_connection_create: NotRequired["Callable[[Any], None]"]


__all__ = ("BigQueryConfig", "BigQueryConnectionParams", "BigQueryDriverFeatures")


class BigQueryConfig(NoPoolSyncConfig[BigQueryConnection, BigQueryDriver]):
    """BigQuery configuration.

    Configuration for Google Cloud BigQuery connections.
    """

    driver_type: ClassVar[type[BigQueryDriver]] = BigQueryDriver
    connection_type: "ClassVar[type[BigQueryConnection]]" = BigQueryConnection

    def __init__(
        self,
        *,
        connection_config: "Optional[Union[BigQueryConnectionParams, dict[str, Any]]]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[Union[BigQueryDriverFeatures, dict[str, Any]]]" = None,
    ) -> None:
        """Initialize BigQuery configuration.

        Args:
            connection_config: Connection configuration parameters
            migration_config: Migration configuration
            statement_config: Statement configuration override
            driver_features: BigQuery-specific driver features
        """

        self.connection_config: dict[str, Any] = dict(connection_config) if connection_config else {}
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        self.driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}

        self._connection_instance: Optional[BigQueryConnection] = self.driver_features.get("connection_instance")

        if "default_query_job_config" not in self.connection_config:
            self._setup_default_job_config()

        if statement_config is None:
            statement_config = bigquery_statement_config

        super().__init__(
            connection_config=self.connection_config,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=self.driver_features,
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
        self, *_args: Any, statement_config: "Optional[StatementConfig]" = None, **_kwargs: Any
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

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for BigQuery types.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({"BigQueryConnection": BigQueryConnection, "BigQueryCursor": BigQueryCursor})
        return namespace
