"""BigQuery database configuration using TypedDict for better maintainability."""

import contextlib
import logging
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, TypedDict

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from typing_extensions import NotRequired

from sqlspec.adapters.bigquery.driver import BigQueryConnection, BigQueryDriver
from sqlspec.config import InstrumentationConfig, NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from collections.abc import Generator
    from contextlib import AbstractContextManager

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials

logger = logging.getLogger("sqlspec.adapters.bigquery")

__all__ = ("BigQueryConfig", "BigQueryConnectionConfig")


class BigQueryConnectionConfig(TypedDict, total=False):
    """BigQuery connection configuration as TypedDict.

    Comprehensive configuration for Google Cloud BigQuery connections supporting
    all advanced features including Gemini in BigQuery, BigQuery ML, DataFrames,
    multi-modal analytics, and cross-cloud capabilities.
    """

    # Core connection parameters
    project: NotRequired[str]
    """Google Cloud project ID."""

    location: NotRequired[str]
    """Default geographic location for jobs and datasets."""

    credentials: NotRequired["Credentials"]
    """Credentials to use for authentication."""

    dataset_id: NotRequired[str]
    """Default dataset ID to use if not specified in queries."""

    credentials_path: NotRequired[str]
    """Path to Google Cloud service account key file (JSON). If None, attempts default authentication."""

    # Client configuration
    client_options: NotRequired["ClientOptions"]
    """Client options used to set user options on the client (e.g., api_endpoint)."""

    client_info: NotRequired["ClientInfo"]
    """Client info used to send a user-agent string along with API requests."""

    # Job configuration
    default_query_job_config: NotRequired["QueryJobConfig"]
    """Default QueryJobConfig settings for query operations."""

    default_load_job_config: NotRequired["LoadJobConfig"]
    """Default LoadJobConfig settings for data loading operations."""

    # Advanced BigQuery features
    use_legacy_sql: NotRequired[bool]
    """Whether to use legacy SQL syntax (default: False for standard SQL)."""

    use_query_cache: NotRequired[bool]
    """Whether to use query cache for faster repeated queries."""

    maximum_bytes_billed: NotRequired[int]
    """Maximum bytes that can be billed for queries to prevent runaway costs."""

    # BigQuery ML and AI configuration
    enable_bigquery_ml: NotRequired[bool]
    """Enable BigQuery ML capabilities for machine learning workflows."""

    enable_gemini_integration: NotRequired[bool]
    """Enable Gemini in BigQuery for AI-powered analytics and code assistance."""

    # Performance and scaling options
    query_timeout_ms: NotRequired[int]
    """Query timeout in milliseconds."""

    job_timeout_ms: NotRequired[int]
    """Job timeout in milliseconds."""

    # BigQuery editions and reservations
    reservation_id: NotRequired[str]
    """Reservation ID for slot allocation and workload management."""

    edition: NotRequired[str]
    """BigQuery edition (Standard, Enterprise, Enterprise Plus)."""

    # Cross-cloud and external data options
    enable_cross_cloud: NotRequired[bool]
    """Enable cross-cloud data access (AWS S3, Azure Blob Storage)."""

    enable_bigquery_omni: NotRequired[bool]
    """Enable BigQuery Omni for multi-cloud analytics."""

    # Storage and format options
    use_avro_logical_types: NotRequired[bool]
    """Use Avro logical types for better type preservation."""

    parquet_enable_list_inference: NotRequired[bool]
    """Enable automatic list inference for Parquet data."""

    # Security and governance
    enable_column_level_security: NotRequired[bool]
    """Enable column-level access controls and data masking."""

    enable_row_level_security: NotRequired[bool]
    """Enable row-level security policies."""

    # DataFrames and Python integration
    enable_dataframes: NotRequired[bool]
    """Enable BigQuery DataFrames for Python-based analytics."""

    dataframes_backend: NotRequired[str]
    """Backend for BigQuery DataFrames (e.g., 'bigframes')."""

    # Continuous queries and real-time processing
    enable_continuous_queries: NotRequired[bool]
    """Enable continuous queries for real-time data processing."""

    # Vector search and embeddings
    enable_vector_search: NotRequired[bool]
    """Enable vector search capabilities for AI/ML workloads."""


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

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    def __init__(
        self,
        connection_config: Optional[BigQueryConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,  # type: ignore[assignment]
        # BigQuery-specific callbacks
        on_connection_create: Optional[Callable[[BigQueryConnection], None]] = None,
        on_job_start: Optional[Callable[[str], None]] = None,
        on_job_complete: Optional[Callable[[str, Any], None]] = None,
    ) -> None:
        """Initialize BigQuery configuration with comprehensive feature support.

        Args:
            connection_config: BigQuery connection and client parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
            on_connection_create: Callback executed when connection is created
            on_job_start: Callback executed when a BigQuery job starts
            on_job_complete: Callback executed when a BigQuery job completes

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
        self.connection_config = connection_config or {}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.on_connection_create = on_connection_create
        self.on_job_start = on_job_start
        self.on_job_complete = on_job_complete

        # Set up default query job config if not provided
        if "default_query_job_config" not in self.connection_config:
            self._setup_default_job_config()

        # Store connection instance for reuse (BigQuery doesn't support traditional pooling)
        self._connection_instance: Optional[BigQueryConnection] = None

        super().__init__(
            instrumentation=instrumentation or InstrumentationConfig(),
        )

    @property
    def connection_type(self) -> type[BigQueryConnection]:  # type: ignore[override]
        """Return the connection type."""
        return BigQueryConnection

    @property
    def driver_type(self) -> type[BigQueryDriver]:  # type: ignore[override]
        """Return the driver type."""
        return BigQueryDriver

    def _setup_default_job_config(self) -> None:
        """Set up default job configuration based on connection settings."""
        job_config = QueryJobConfig()

        # Set default dataset if specified
        if "dataset_id" in self.connection_config:
            dataset_id = self.connection_config["dataset_id"]
            if isinstance(dataset_id, str):
                job_config.default_dataset = dataset_id

        # Configure query cache
        if self.connection_config.get("use_query_cache", True):
            job_config.use_query_cache = True

        # Configure legacy SQL
        if self.connection_config.get("use_legacy_sql", False):
            job_config.use_legacy_sql = True

        # Configure cost controls
        if "maximum_bytes_billed" in self.connection_config:
            job_config.maximum_bytes_billed = self.connection_config["maximum_bytes_billed"]

        # Configure timeouts
        if "query_timeout_ms" in self.connection_config:
            job_config.job_timeout_ms = self.connection_config["query_timeout_ms"]

        self.connection_config["default_query_job_config"] = job_config

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for BigQuery Client constructor.

        Filters out BigQuery-specific enhancement flags and formats parameters
        appropriately for the google.cloud.bigquery.Client constructor.

        Returns:
            Configuration dict for BigQuery Client constructor.
        """
        # Fields that shouldn't go to the Client constructor
        excluded_fields = {
            "dataset_id",
            "credentials_path",
            "use_legacy_sql",
            "use_query_cache",
            "maximum_bytes_billed",
            "enable_bigquery_ml",
            "enable_gemini_integration",
            "query_timeout_ms",
            "job_timeout_ms",
            "reservation_id",
            "edition",
            "enable_cross_cloud",
            "enable_bigquery_omni",
            "use_avro_logical_types",
            "parquet_enable_list_inference",
            "enable_column_level_security",
            "enable_row_level_security",
            "enable_dataframes",
            "dataframes_backend",
            "enable_continuous_queries",
            "enable_vector_search",
        }

        return {
            key: value
            for key, value in self.connection_config.items()
            if key not in excluded_fields and value is not Empty
        }

    def create_connection(self) -> BigQueryConnection:
        """Create and return a new BigQuery Client instance.

        Returns:
            A new BigQuery Client instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        if self.instrumentation.log_pool_operations:
            logger.info("Creating BigQuery connection", extra={"adapter": "bigquery"})

        # Reuse existing connection for BigQuery (no traditional pooling)
        if self._connection_instance is not None:
            if self.instrumentation.log_pool_operations:
                logger.debug("Reusing existing BigQuery connection", extra={"adapter": "bigquery"})
            return self._connection_instance

        try:
            with instrument_operation(self, "bigquery_connection_create", "database"):
                config_dict = self.connection_config_dict

                if self.instrumentation.log_pool_operations:
                    # Log config without sensitive data
                    safe_config = {k: v for k, v in config_dict.items() if k != "credentials"}
                    logger.debug("BigQuery connection config: %s", safe_config, extra={"adapter": "bigquery"})

                connection = self.connection_type(**config_dict)

                # Execute connection creation hook
                if self.on_connection_create:
                    try:
                        self.on_connection_create(connection)
                        if self.instrumentation.log_pool_operations:
                            logger.debug("Executed connection creation hook", extra={"adapter": "bigquery"})
                    except Exception as e:
                        if self.instrumentation.log_pool_operations:
                            logger.warning(
                                "Connection creation hook failed", extra={"adapter": "bigquery", "error": str(e)}
                            )

                # Cache the connection instance
                self._connection_instance = connection

                if self.instrumentation.log_pool_operations:
                    logger.info("BigQuery connection created successfully", extra={"adapter": "bigquery"})

                return connection

        except Exception as e:
            project_id = self.connection_config.get("project", "Unknown")
            msg = f"Could not configure BigQuery connection for project '{project_id}'. Error: {e}"
            logger.exception(msg, extra={"adapter": "bigquery", "error": str(e)})
            raise ImproperConfigurationError(msg) from e

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[BigQueryConnection, None, None]":
        """Provide a BigQuery client within a context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A BigQuery Client instance.
        """
        with instrument_operation(self, "bigquery_provide_connection", "database"):
            connection = self.create_connection()
            yield connection
            # Note: BigQuery connections don't need explicit cleanup

    def provide_session(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[BigQueryDriver]":
        """Provide a BigQuery driver session context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            A context manager that yields a BigQueryDriver instance.
        """

        @contextlib.contextmanager
        def session_manager() -> "Generator[BigQueryDriver, None, None]":
            with self.provide_connection(*args, **kwargs) as connection:
                driver = self.driver_type(
                    connection=connection,
                    config=self.statement_config,
                    instrumentation_config=self.instrumentation,
                    default_row_type=self.default_row_type,
                    # Pass BigQuery-specific configurations
                    default_query_job_config=self.connection_config.get("default_query_job_config"),
                    on_job_start=self.on_job_start,
                    on_job_complete=self.on_job_complete,
                )
                yield driver

        return session_manager()
