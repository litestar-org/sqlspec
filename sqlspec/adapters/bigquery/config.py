"""BigQuery database configuration with direct field-based configuration."""

import contextlib
import logging
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict, Union

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from typing_extensions import NotRequired

from sqlspec.adapters.bigquery._types import BigQueryConnection
from sqlspec.adapters.bigquery.driver import BigQueryCursor, BigQueryDriver
from sqlspec.config import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials

    from sqlspec.statement.sql import StatementConfig


logger = logging.getLogger(__name__)


class BigQueryConnectionParams(TypedDict, total=False):
    """BigQuery connection parameters."""

    project: NotRequired[str]
    location: NotRequired[str]
    credentials: NotRequired["Credentials"]
    dataset_id: NotRequired[str]
    credentials_path: NotRequired[str]
    client_options: NotRequired["ClientOptions"]
    client_info: NotRequired["ClientInfo"]
    default_query_job_config: NotRequired[QueryJobConfig]
    default_load_job_config: NotRequired[LoadJobConfig]
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


__all__ = ("BigQueryConfig", "BigQueryConnectionParams")


class BigQueryConfig(NoPoolSyncConfig[BigQueryConnection, BigQueryDriver]):
    """Enhanced BigQuery configuration with comprehensive feature support.

    BigQuery is Google Cloud's serverless, highly scalable data warehouse with
    advanced analytics, machine learning, and AI capabilities. This configuration
    supports all BigQuery features including:

    - Gemini in BigQuery for AI-powered analytics
    - BigQuery ML for machine learning workflows
    - BigQuery DataFrames for Python-based analytics
    - Multi-modal data analysis (text, images, video, audio)
    - Cross-cloud data access (AWS S3, Azure Blob Storage)
    - Vector search and embeddings
    - Continuous queries for real-time processing
    - Advanced security and governance features
    - Parquet and Arrow format optimization
    """

    driver_type: ClassVar[type[BigQueryDriver]] = BigQueryDriver
    connection_type: "ClassVar[type[BigQueryConnection]]" = BigQueryConnection

    def __init__(
        self,
        *,
        connection_instance: "Optional[BigQueryConnection]" = None,
        connection_config: "Optional[Union[BigQueryConnectionParams, dict[str, Any]]]" = None,
        on_connection_create: Optional[Callable[[BigQueryConnection], None]] = None,
        on_job_start: Optional[Callable[[str], None]] = None,
        on_job_complete: Optional[Callable[[str, Any], None]] = None,
        migration_config: Optional[dict[str, Any]] = None,
        statement_config: "Optional[StatementConfig]" = None,
    ) -> None:
        """Initialize BigQuery configuration with comprehensive feature support.

        Args:
            connection_config: Connection configuration parameters
            connection_instance: Existing connection instance to use
            on_connection_create: Callback executed when connection is created
            on_job_start: Callback executed when a BigQuery job starts
            on_job_complete: Callback executed when a BigQuery job completes
            migration_config: Migration configuration
            statement_config: Statement configuration override

        Example:
            >>> # Basic BigQuery connection
            >>> config = BigQueryConfig(
            ...     connection_config={
            ...         "project": "my-project",
            ...         "location": "US",
            ...     }
            ... )

            >>> # Advanced configuration with ML and AI features
            >>> config = BigQueryConfig(
            ...     connection_config={
            ...         "project": "my-project",
            ...         "location": "US",
            ...         "enable_bigquery_ml": True,
            ...         "enable_gemini_integration": True,
            ...         "enable_dataframes": True,
            ...         "enable_vector_search": True,
            ...         "maximum_bytes_billed": 1000000000,  # 1GB limit
            ...     }
            ... )

            >>> # Enterprise configuration with reservations
            >>> config = BigQueryConfig(
            ...     connection_config={
            ...         "project": "my-project",
            ...         "location": "US",
            ...         "edition": "Enterprise Plus",
            ...         "reservation_id": "my-reservation",
            ...         "enable_continuous_queries": True,
            ...         "enable_cross_cloud": True,
            ...     }
            ... )
        """

        self.connection_config: dict[str, Any] = dict(connection_config) if connection_config else {}
        if "extra" in self.connection_config:
            extras = self.connection_config.pop("extra")
            self.connection_config.update(extras)

        self._connection_instance = connection_instance

        # Store other config
        self.on_connection_create = on_connection_create
        self.on_job_start = on_job_start
        self.on_job_complete = on_job_complete

        if self.connection_config.get("default_query_job_config") is None:
            self._setup_default_job_config()

        super().__init__(migration_config=migration_config, statement_config=statement_config)

    def _setup_default_job_config(self) -> None:
        """Set up default job configuration based on connection settings."""
        job_config = QueryJobConfig()

        dataset_id = self.connection_config.get("dataset_id")
        project = self.connection_config.get("project")
        if dataset_id and project and "." not in dataset_id:
            job_config.default_dataset = f"{project}.{dataset_id}"

        use_query_cache = self.connection_config.get("use_query_cache")
        if use_query_cache is not None:
            job_config.use_query_cache = use_query_cache
        else:
            job_config.use_query_cache = True  # Default to True

        # Configure cost controls
        maximum_bytes_billed = self.connection_config.get("maximum_bytes_billed")
        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = maximum_bytes_billed

        # Configure timeouts
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
            # Filter out BigQuery-specific enhancement flags and keep only client fields
            client_fields = {"project", "location", "credentials", "client_options", "client_info"}
            config_dict: dict[str, Any] = {
                field: value
                for field, value in self.connection_config.items()
                if field in client_fields and value is not None and value is not Empty
            }
            connection = self.connection_type(**config_dict)
            if self.on_connection_create:
                self.on_connection_create(connection)

            self._connection_instance = connection

        except Exception as e:
            project = self.connection_config.get("project", "Unknown")
            msg = f"Could not configure BigQuery connection for project '{project}'. Error: {e}"
            raise ImproperConfigurationError(msg) from e
        return connection

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[BigQueryConnection, None, None]":
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
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "Generator[BigQueryDriver, None, None]":
        """Provide a BigQuery driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A context manager that yields a BigQueryDriver instance.
        """

        with self.provide_connection(*args, **kwargs) as connection:
            driver = self.driver_type(
                connection=connection,
                statement_config=statement_config,
                default_query_job_config=self.connection_config.get("default_query_job_config"),
                on_job_start=self.on_job_start,
                on_job_complete=self.on_job_complete,
            )
            yield driver

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for BigQuery types.

        This provides all BigQuery-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({"BigQueryConnection": BigQueryConnection, "BigQueryCursor": BigQueryCursor})
        return namespace
