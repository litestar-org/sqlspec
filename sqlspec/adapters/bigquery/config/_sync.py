import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.adapters.bigquery.config._common import BigQueryConnectionConfigCommon
from sqlspec.adapters.bigquery.driver import BigQueryConnection, BigQueryDriver
from sqlspec.base import NoPoolSyncConfig
from sqlspec.typing import dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ("BigQueryConfig", "BigQueryConnectionConfig")


class BigQueryConnectionConfig(BigQueryConnectionConfigCommon):
    """BigQuery Connection Configuration."""


@dataclass
class BigQueryConfig(NoPoolSyncConfig["BigQueryConnection", "BigQueryDriver"]):
    """BigQuery Synchronous Driver Configuration."""

    connection_config: "BigQueryConnectionConfig" = field(default_factory=BigQueryConnectionConfig)
    """BigQuery Connection Configuration."""
    driver_type: "type[BigQueryDriver]" = field(init=False, repr=False, default=BigQueryDriver)
    """BigQuery Driver Type."""
    connection_type: "type[BigQueryConnection]" = field(init=False, repr=False, default=BigQueryConnection)
    """BigQuery Connection Type."""
    pool_instance: "None" = field(init=False, repr=False, default=None, hash=False)
    """This is set to have a init=False since BigQuery does not support pooling."""
    connection_instance: "Optional[BigQueryConnection]" = field(init=False, repr=False, default=None, hash=False)
    """BigQuery Connection Instance."""

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the BigQueryConnection constructor.
        """
        return dataclass_to_dict(
            self.connection_config,
            exclude_empty=True,
            exclude_none=True,
            exclude={"dataset_id", "credentials_path"},
        )

    def create_connection(self) -> "BigQueryConnection":
        """Create a BigQuery Client instance.

        Returns:
            A BigQuery Client instance.
        """
        if self.connection_instance is not None:
            return self.connection_instance

        self.connection_instance = self.connection_type(**self.connection_config_dict)
        return self.connection_instance

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Iterator[BigQueryConnection]":
        """Provide a BigQuery client within a context manager.

        Args:
            *args: Additional arguments to pass to the connection.
            **kwargs: Additional keyword arguments to pass to the connection.

        Yields:
            An iterator of BigQuery Client instances.
        """
        conn = self.create_connection()
        yield conn

    @contextlib.contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> "Iterator[BigQueryDriver]":
        """Provide a BigQuery driver session within a context manager.

        Args:
            *args: Additional arguments to pass to the driver.
            **kwargs: Additional keyword arguments to pass to the driver.

        Yields:
            An iterator of BigQueryDriver instances.
        """
        conn = self.create_connection()
        yield self.driver_type(connection=conn)
