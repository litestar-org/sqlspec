"""BigQuery database configuration with direct field-based configuration."""

import contextlib
import logging
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig

from sqlspec.adapters.bigquery.driver import BigQueryConnection, BigQueryDriver
from sqlspec.config import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty

if TYPE_CHECKING:
    from collections.abc import Generator
    from contextlib import AbstractContextManager

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials
    from sqlglot.dialects.dialect import DialectType

logger = logging.getLogger(__name__)

CONNECTION_FIELDS = frozenset(
    {
        "project",
        "location",
        "credentials",
        "dataset_id",
        "credentials_path",
        "client_options",
        "client_info",
        "default_query_job_config",
        "default_load_job_config",
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
)

__all__ = ("CONNECTION_FIELDS", "BigQueryConfig")


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

    __slots__ = (
        "_connection_instance",
        "_dialect",
        "client_info",
        "client_options",
        "credentials",
        "credentials_path",
        "dataframes_backend",
        "dataset_id",
        "default_load_job_config",
        "default_query_job_config",
        "default_row_type",
        "edition",
        "enable_bigquery_ml",
        "enable_bigquery_omni",
        "enable_column_level_security",
        "enable_continuous_queries",
        "enable_cross_cloud",
        "enable_dataframes",
        "enable_gemini_integration",
        "enable_row_level_security",
        "enable_vector_search",
        "extras",
        "job_timeout_ms",
        "location",
        "maximum_bytes_billed",
        "on_connection_create",
        "on_job_complete",
        "on_job_start",
        "parquet_enable_list_inference",
        "pool_instance",
        "project",
        "query_timeout_ms",
        "reservation_id",
        "statement_config",
        "use_avro_logical_types",
        "use_query_cache",
    )

    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = False

    driver_type: type[BigQueryDriver] = BigQueryDriver
    connection_type: type[BigQueryConnection] = BigQueryConnection

    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("named_at",)
    """BigQuery only supports @name (named_at) parameter style."""

    preferred_parameter_style: ClassVar[str] = "named_at"
    """BigQuery's native parameter style is @name (named_at)."""

    def __init__(
        self,
        statement_config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
        # Core connection parameters
        project: Optional[str] = None,
        location: Optional[str] = None,
        credentials: Optional["Credentials"] = None,
        dataset_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        # Client configuration
        client_options: Optional["ClientOptions"] = None,
        client_info: Optional["ClientInfo"] = None,
        # Job configuration
        default_query_job_config: Optional["QueryJobConfig"] = None,
        default_load_job_config: Optional["LoadJobConfig"] = None,
        # Advanced BigQuery features
        use_query_cache: Optional[bool] = None,
        maximum_bytes_billed: Optional[int] = None,
        # BigQuery ML and AI configuration
        enable_bigquery_ml: Optional[bool] = None,
        enable_gemini_integration: Optional[bool] = None,
        # Performance and scaling options
        query_timeout_ms: Optional[int] = None,
        job_timeout_ms: Optional[int] = None,
        # BigQuery editions and reservations
        reservation_id: Optional[str] = None,
        edition: Optional[str] = None,
        # Cross-cloud and external data options
        enable_cross_cloud: Optional[bool] = None,
        enable_bigquery_omni: Optional[bool] = None,
        # Storage and format options
        use_avro_logical_types: Optional[bool] = None,
        parquet_enable_list_inference: Optional[bool] = None,
        # Security and governance
        enable_column_level_security: Optional[bool] = None,
        enable_row_level_security: Optional[bool] = None,
        # DataFrames and Python integration
        enable_dataframes: Optional[bool] = None,
        dataframes_backend: Optional[str] = None,
        # Continuous queries and real-time processing
        enable_continuous_queries: Optional[bool] = None,
        # Vector search and embeddings
        enable_vector_search: Optional[bool] = None,
        # Callback functions
        on_connection_create: Optional[Callable[[BigQueryConnection], None]] = None,
        on_job_start: Optional[Callable[[str], None]] = None,
        on_job_complete: Optional[Callable[[str, Any], None]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize BigQuery configuration with comprehensive feature support.

        Args:
            statement_config: Default SQL statement configuration
            default_row_type: Default row type for results
            project: Google Cloud project ID
            location: Default geographic location for jobs and datasets
            credentials: Credentials to use for authentication
            dataset_id: Default dataset ID to use if not specified in queries
            credentials_path: Path to Google Cloud service account key file (JSON)
            client_options: Client options used to set user options on the client
            client_info: Client info used to send a user-agent string along with API requests
            default_query_job_config: Default QueryJobConfig settings for query operations
            default_load_job_config: Default LoadJobConfig settings for data loading operations
            use_query_cache: Whether to use query cache for faster repeated queries
            maximum_bytes_billed: Maximum bytes that can be billed for queries to prevent runaway costs
            enable_bigquery_ml: Enable BigQuery ML capabilities for machine learning workflows
            enable_gemini_integration: Enable Gemini in BigQuery for AI-powered analytics and code assistance
            query_timeout_ms: Query timeout in milliseconds
            job_timeout_ms: Job timeout in milliseconds
            reservation_id: Reservation ID for slot allocation and workload management
            edition: BigQuery edition (Standard, Enterprise, Enterprise Plus)
            enable_cross_cloud: Enable cross-cloud data access (AWS S3, Azure Blob Storage)
            enable_bigquery_omni: Enable BigQuery Omni for multi-cloud analytics
            use_avro_logical_types: Use Avro logical types for better type preservation
            parquet_enable_list_inference: Enable automatic list inference for Parquet data
            enable_column_level_security: Enable column-level access controls and data masking
            enable_row_level_security: Enable row-level security policies
            enable_dataframes: Enable BigQuery DataFrames for Python-based analytics
            dataframes_backend: Backend for BigQuery DataFrames (e.g., 'bigframes')
            enable_continuous_queries: Enable continuous queries for real-time data processing
            enable_vector_search: Enable vector search capabilities for AI/ML workloads
            on_connection_create: Callback executed when connection is created
            on_job_start: Callback executed when a BigQuery job starts
            on_job_complete: Callback executed when a BigQuery job completes
            **kwargs: Additional parameters (stored in extras)

        Example:
            >>> # Basic BigQuery connection
            >>> config = BigQueryConfig(project="my-project", location="US")

            >>> # Advanced configuration with ML and AI features
            >>> config = BigQueryConfig(
            ...     project="my-project",
            ...     location="US",
            ...     enable_bigquery_ml=True,
            ...     enable_gemini_integration=True,
            ...     enable_dataframes=True,
            ...     enable_vector_search=True,
            ...     maximum_bytes_billed=1000000000,  # 1GB limit
            ... )

            >>> # Enterprise configuration with reservations
            >>> config = BigQueryConfig(
            ...     project="my-project",
            ...     location="US",
            ...     edition="Enterprise Plus",
            ...     reservation_id="my-reservation",
            ...     enable_continuous_queries=True,
            ...     enable_cross_cloud=True,
            ... )
        """
        # Store connection parameters as instance attributes
        self.project = project
        self.location = location
        self.credentials = credentials
        self.dataset_id = dataset_id
        self.credentials_path = credentials_path
        self.client_options = client_options
        self.client_info = client_info
        self.default_query_job_config = default_query_job_config
        self.default_load_job_config = default_load_job_config
        self.use_query_cache = use_query_cache
        self.maximum_bytes_billed = maximum_bytes_billed
        self.enable_bigquery_ml = enable_bigquery_ml
        self.enable_gemini_integration = enable_gemini_integration
        self.query_timeout_ms = query_timeout_ms
        self.job_timeout_ms = job_timeout_ms
        self.reservation_id = reservation_id
        self.edition = edition
        self.enable_cross_cloud = enable_cross_cloud
        self.enable_bigquery_omni = enable_bigquery_omni
        self.use_avro_logical_types = use_avro_logical_types
        self.parquet_enable_list_inference = parquet_enable_list_inference
        self.enable_column_level_security = enable_column_level_security
        self.enable_row_level_security = enable_row_level_security
        self.enable_dataframes = enable_dataframes
        self.dataframes_backend = dataframes_backend
        self.enable_continuous_queries = enable_continuous_queries
        self.enable_vector_search = enable_vector_search

        self.extras = kwargs or {}

        # Store other config
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type
        self.on_connection_create = on_connection_create
        self.on_job_start = on_job_start
        self.on_job_complete = on_job_complete

        # Set up default query job config if not provided
        if self.default_query_job_config is None:
            self._setup_default_job_config()

        # Store connection instance for reuse (BigQuery doesn't support traditional pooling)
        self._connection_instance: Optional[BigQueryConnection] = None
        self._dialect: DialectType = None

        super().__init__()

    def _setup_default_job_config(self) -> None:
        """Set up default job configuration based on connection settings."""
        job_config = QueryJobConfig()

        if self.dataset_id and self.project and "." not in self.dataset_id:
            job_config.default_dataset = f"{self.project}.{self.dataset_id}"
        if self.use_query_cache is not None:
            job_config.use_query_cache = self.use_query_cache
        else:
            job_config.use_query_cache = True  # Default to True

        # Configure cost controls
        if self.maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = self.maximum_bytes_billed

        # Configure timeouts
        if self.query_timeout_ms is not None:
            job_config.job_timeout_ms = self.query_timeout_ms

        self.default_query_job_config = job_config

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict for BigQuery Client constructor.

        Filters out BigQuery-specific enhancement flags and formats parameters
        appropriately for the google.cloud.bigquery.Client constructor.

        Returns:
            Configuration dict for BigQuery Client constructor.
        """
        client_fields = {"project", "location", "credentials", "client_options", "client_info"}
        config = {
            field: getattr(self, field)
            for field in client_fields
            if getattr(self, field, None) is not None and getattr(self, field) is not Empty
        }
        config.update(self.extras)

        return config

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
            config_dict = self.connection_config_dict

            connection = self.connection_type(**config_dict)
            if self.on_connection_create:
                self.on_connection_create(connection)

            self._connection_instance = connection

        except Exception as e:
            msg = f"Could not configure BigQuery connection for project '{self.project or 'Unknown'}'. Error: {e}"
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
                # Create statement config with parameter style info if not already set
                statement_config = self.statement_config
                if statement_config.allowed_parameter_styles is None:
                    statement_config = replace(
                        statement_config,
                        allowed_parameter_styles=self.supported_parameter_styles,
                        target_parameter_style=self.preferred_parameter_style,
                    )

                driver = self.driver_type(
                    connection=connection,
                    config=statement_config,
                    default_row_type=self.default_row_type,
                    default_query_job_config=self.default_query_job_config,
                    on_job_start=self.on_job_start,
                    on_job_complete=self.on_job_complete,
                )
                yield driver

        return session_manager()
