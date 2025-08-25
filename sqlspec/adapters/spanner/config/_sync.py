import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.pool import AbstractSessionPool, FixedSizePool, PingingPool, TransactionPingingPool
from google.cloud.spanner_v1.snapshot import Snapshot
from google.cloud.spanner_v1.transaction import Transaction

from sqlspec.adapters.spanner.driver import SpannerDriver
from sqlspec.base import SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator

    from google.auth.credentials import Credentials

# Define the Connection Type alias
SpannerSyncConnection = Union[Snapshot, Transaction]

# Get logger instance
logger = logging.getLogger("sqlspec")

__all__ = ("SpannerConfig", "SpannerPoolConfig")


@dataclass
class SpannerPoolConfig:
    """Configuration for the Spanner session pool.

    Ref: https://cloud.google.com/python/docs/reference/spanner/latest/advanced-session-pool-topics
    """

    pool_type: type[AbstractSessionPool] = FixedSizePool
    """The type of session pool to use. Defaults to FixedSizePool."""
    min_sessions: int = 1
    """The minimum number of sessions to keep in the pool."""
    max_sessions: int = 10
    """The maximum number of sessions allowed in the pool."""
    labels: Optional[dict[str, str]] = None
    """Labels to apply to sessions created by the pool."""
    ping_interval: int = 300  # Default 5 minutes
    """Interval (in seconds) for pinging sessions in PingingPool/TransactionPingingPool."""
    # Add other pool-specific configs as needed, e.g., ping_interval for PingingPool


@dataclass
class SpannerConfig(
    SyncDatabaseConfig[SpannerSyncConnection, AbstractSessionPool, SpannerDriver]
):  # Replace Any with actual Connection/Driver types later
    """Synchronous Google Cloud Spanner database Configuration.

    This class provides the configuration for Spanner database connections.
    """

    project: Optional[str] = None
    """Google Cloud project ID."""
    instance_id: Optional[str] = None
    """Spanner instance ID."""
    database_id: Optional[str] = None
    """Spanner database ID."""
    credentials: Optional["Credentials"] = None
    """Optional Google Cloud credentials. If None, uses Application Default Credentials."""
    client_options: Optional[dict[str, Any]] = None
    """Optional dictionary of client options for the Spanner client."""
    pool_config: Optional[SpannerPoolConfig] = field(default_factory=SpannerPoolConfig)
    """Spanner session pool configuration."""
    pool_instance: Optional[AbstractSessionPool] = None
    """Optional pre-configured pool instance to use."""

    # Define actual types
    connection_type: "type[SpannerSyncConnection]" = field(init=False, default=Union[Snapshot, Transaction])  # type: ignore
    driver_type: "type[SpannerDriver]" = field(init=False, default=SpannerDriver)

    _client: Optional[Client] = field(init=False, default=None, repr=False, hash=False)
    _database: Optional[Database] = field(init=False, default=None, repr=False, hash=False)
    _ping_thread: "Optional[threading.Thread]" = field(init=False, default=None, repr=False, hash=False)

    def __post_init__(self) -> None:
        # Basic check, more robust checks might be needed later
        if self.pool_instance and not self.pool_config:
            # If a pool instance is provided, we might not need pool_config
            pass
        elif not self.pool_config:
            # Create default if not provided and pool_instance is also None
            self.pool_config = SpannerPoolConfig()

    @property
    def client(self) -> Client:
        """Provides the Spanner Client, creating it if necessary."""
        if self._client is None:
            self._client = Client(
                project=self.project,
                credentials=self.credentials,
                client_options=self.client_options,
            )
        return self._client

    @property
    def database(self) -> Database:
        """Provides the Spanner Database instance, creating client, pool, and database if necessary.

        This method ensures that the database instance is created and configured correctly.
        It also handles any additional configuration options that may be needed for the database.

        Args:
            *args: Additional positional arguments to pass to the database constructor.
            **kwargs: Additional keyword arguments to pass to the database constructor.

        Raises:
            ImproperConfigurationError: If project, instance, and database IDs are not configured.

        Returns:
            The configured database instance.
        """
        if self._database is None:
            if not self.project or not self.instance_id or not self.database_id:
                msg = "Project, instance, and database IDs must be configured."
                raise ImproperConfigurationError(msg)

            # Ensure client exists
            spanner_client = self.client
            # Ensure pool exists (this will create it if needed)
            pool = self.provide_pool()

            # Get instance object
            instance = spanner_client.instance(self.instance_id)  # type: ignore[no-untyped-call]

            # Create the final Database object using the created pool
            self._database = instance.database(database_id=self.database_id, pool=pool)
        return self._database

    def provide_pool(self, *args: Any, **kwargs: Any) -> AbstractSessionPool:
        """Provides the configured session pool, creating it if necessary   .

        This method ensures that the session pool is created and configured correctly.
        It also handles any additional configuration options that may be needed for the pool.

        Args:
            *args: Additional positional arguments to pass to the pool constructor.
            **kwargs: Additional keyword arguments to pass to the pool constructor.

        Raises:
            ImproperConfigurationError: If pool_config is not set or project, instance, and database IDs are not configured.

        Returns:
            The configured session pool.
        """
        if self.pool_instance:
            return self.pool_instance

        if not self.pool_config:
            # This should be handled by __post_init__, but double-check
            msg = "pool_config must be set if pool_instance is not provided."
            raise ImproperConfigurationError(msg)

        if not self.project or not self.instance_id or not self.database_id:
            msg = "Project, instance, and database IDs must be configured to create pool."
            raise ImproperConfigurationError(msg)

        instance = self.client.instance(self.instance_id)

        pool_kwargs = dataclass_to_dict(self.pool_config, exclude_empty=True, exclude={"pool_type"})

        # Only include ping_interval if using a relevant pool type
        if not issubclass(self.pool_config.pool_type, (PingingPool, TransactionPingingPool)):
            pool_kwargs.pop("ping_interval", None)

        self.pool_instance = self.pool_config.pool_type(
            database=Database(database_id=self.database_id, instance=instance),  # pyright: ignore
            **pool_kwargs,
        )

        # Start pinging thread if applicable and not already running
        if isinstance(self.pool_instance, (PingingPool, TransactionPingingPool)) and self._ping_thread is None:
            self._ping_thread = threading.Thread(
                target=self.pool_instance.ping,
                daemon=True,  # Ensure thread exits with application
                name=f"spanner-ping-{self.project}-{self.instance_id}-{self.database_id}",
            )
            self._ping_thread.start()
            logger.debug("Started Spanner background ping thread for %s", self.pool_instance)

        return self.pool_instance

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[SpannerSyncConnection, None, None]":
        """Provides a Spanner snapshot context (suitable for reads).

        This method ensures that the connection is created and configured correctly.
        It also handles any additional configuration options that may be needed for the connection.

        Args:
            *args: Additional positional arguments to pass to the connection constructor.
            **kwargs: Additional keyword arguments to pass to the connection constructor.

        Yields:
            The configured connection.
        """
        db = self.database  # Ensure database and pool are initialized
        with db.snapshot() as snapshot:
            yield snapshot  # Replace with actual connection object later

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Generator[SpannerDriver, None, None]":
        """Provides a driver instance initialized with a connection context (Snapshot).

        This method ensures that the driver is created and configured correctly.
        It also handles any additional configuration options that may be needed for the driver.

        Args:
            *args: Additional positional arguments to pass to the driver constructor.
            **kwargs: Additional keyword arguments to pass to the driver constructor.

        Yields:
            The configured driver.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            yield self.driver_type(connection)  # pyright: ignore

    def close_pool(self) -> None:
        """Clears internal references to the pool, database, and client."""
        # Spanner pool doesn't require explicit closing usually.
        self.pool_instance = None
        self._database = None
        self._client = None
        # Clear thread reference, but don't need to join (it's daemon)
        self._ping_thread = None

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Returns connection-related parameters."""
        config = {
            "project": self.project,
            "instance_id": self.instance_id,
            "database_id": self.database_id,
            "credentials": self.credentials,
            "client_options": self.client_options,
        }
        return {k: v for k, v in config.items() if v is not None}

    @property
    def pool_config_dict(self) -> "dict[str, Any]":
        """Returns pool configuration parameters.

        This method ensures that the pool configuration is returned correctly.
        It also handles any additional configuration options that may be needed for the pool.

        Args:
            *args: Additional positional arguments to pass to the pool constructor.
            **kwargs: Additional keyword arguments to pass to the pool constructor.

        Raises:
            ImproperConfigurationError: If pool_config is not set or project, instance, and database IDs are not configured.

        Returns:
            The pool configuration parameters.
        """
        if self.pool_config:
            return dataclass_to_dict(self.pool_config, exclude_empty=True)
        # If pool_config was not initially provided but pool_instance was,
        # this method might be called unexpectedly. Add check.
        if self.pool_instance:
            # We can't reconstruct the config dict from the instance easily.
            msg = "Cannot retrieve pool_config_dict when initialized with pool_instance."
            raise ImproperConfigurationError(msg)
        # Should not be reachable if __post_init__ runs correctly
        msg = "pool_config is not set."
        raise ImproperConfigurationError(msg)
