"""BigQuery database configuration using TypedDict for better maintainability."""

import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from typing_extensions import NotRequired

from sqlspec.adapters.bigquery.driver import BigQueryConnection, BigQueryDriver
from sqlspec.config import InstrumentationConfig, NoPoolSyncConfig
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import DictRow, Empty
from sqlspec.utils.telemetry import instrument_sync

if TYPE_CHECKING:
    from collections.abc import Generator

    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials


__all__ = ("BigQueryConfig", "BigQueryConnectionConfig")


class BigQueryConnectionConfig(TypedDict, total=False):
    """BigQuery connection configuration as TypedDict.

    All parameters for google.cloud.bigquery.Client() constructor.
    """

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

    client_options: NotRequired["ClientOptions"]
    """Client options used to set user options on the client (e.g., api_endpoint)."""

    default_query_job_config: NotRequired["QueryJobConfig"]
    """Default QueryJobConfig settings."""

    default_load_job_config: NotRequired["LoadJobConfig"]
    """Default LoadJobConfig settings."""

    client_info: NotRequired["ClientInfo"]
    """Client info used to send a user-agent string along with API requests."""


class BigQueryConfig(NoPoolSyncConfig[BigQueryConnection, BigQueryDriver]):
    """Configuration for BigQuery database connections using TypedDict."""

    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    def __init__(
        self,
        connection_config: Optional[BigQueryConnectionConfig] = None,
        statement_config: Optional[SQLConfig] = None,
        instrumentation: Optional[InstrumentationConfig] = None,
        default_row_type: type[DictRow] = DictRow,  # type: ignore[assignment]
    ) -> None:
        """Initialize BigQuery configuration.

        Args:
            connection_config: BigQuery connection parameters
            statement_config: Default SQL statement configuration
            instrumentation: Instrumentation configuration
            default_row_type: Default row type for results
        """
        self.connection_config = connection_config or {}
        self.statement_config = statement_config or SQLConfig()
        self.default_row_type = default_row_type

        # Set up default query job config if not provided
        if "default_query_job_config" not in self.connection_config and "dataset_id" in self.connection_config:
            self.connection_config["default_query_job_config"] = QueryJobConfig(
                default_dataset=self.connection_config["dataset_id"]
            )

        # Store connection instance for reuse (BigQuery doesn't support pooling)
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

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        # Filter out empty values and exclude some fields that shouldn't go to the constructor
        excluded_fields = {"dataset_id", "credentials_path"}
        return {k: v for k, v in self.connection_config.items() if v is not Empty and k not in excluded_fields}

    @instrument_sync(operation_type="connection")
    def create_connection(self) -> BigQueryConnection:
        """Create a BigQuery Client instance.

        Returns:
            A BigQuery Client instance.
        """
        if self._connection_instance is not None:
            return self._connection_instance

        self._connection_instance = self.connection_type(**self.connection_config_dict)
        return self._connection_instance

    @instrument_sync(operation_type="connection_context")
    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[BigQueryConnection, None, None]":
        """Provide a BigQuery client within a context manager.

        Args:
            *args: Additional arguments to pass to the connection.
            **kwargs: Additional keyword arguments to pass to the connection.

        Yields:
            An iterator of BigQuery Client instances.
        """
        conn = self.create_connection()
        yield conn

    @instrument_sync(operation_type="session_context")
    @contextlib.contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[BigQueryDriver, None, None]":
        """Provide a BigQuery driver session within a context manager.

        Args:
            *args: Additional arguments to pass to the driver.
            **kwargs: Additional keyword arguments to pass to the driver.

        Yields:
            An iterator of BigQueryDriver instances.
        """
        conn = self.create_connection()
        driver = self.driver_type(
            connection=conn,
            config=self.statement_config,
            instrumentation_config=self.instrumentation,
            default_row_type=self.default_row_type,
        )
        yield driver
