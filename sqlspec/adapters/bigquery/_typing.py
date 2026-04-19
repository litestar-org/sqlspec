"""BigQuery adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import logging
from typing import TYPE_CHECKING, Any

from google.cloud.bigquery import (
    ArrayQueryParameter,
    Client,
    LoadJobConfig,
    QueryJob,
    QueryJobConfig,
    ScalarQueryParameter,
)
from google.cloud.exceptions import GoogleCloudError

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType
    from typing import TypeAlias

    from sqlspec.adapters.bigquery.driver import BigQueryDriver
    from sqlspec.core import StatementConfig

    BigQueryConnection: TypeAlias = Client
    BigQueryParam: TypeAlias = ArrayQueryParameter | ScalarQueryParameter

if not TYPE_CHECKING:
    BigQueryConnection = Client
    BigQueryParam = ArrayQueryParameter | ScalarQueryParameter


class BigQueryCursor:
    """BigQuery cursor with resource management."""

    __slots__ = ("connection", "job")

    def __init__(self, connection: "BigQueryConnection") -> None:
        self.connection = connection
        self.job: QueryJob | None = None

    def __enter__(self) -> "BigQueryConnection":
        return self.connection

    def __exit__(self, *_: Any) -> None:
        """Clean up cursor resources including active QueryJobs."""
        if self.job is not None:
            try:
                # Cancel the job if it's still running to free up resources
                if self.job.state in {"PENDING", "RUNNING"}:
                    self.job.cancel()
                # Clear the job reference
                self.job = None
            except Exception:
                logging.getLogger(__name__).exception("Failed to cancel BigQuery job during cursor cleanup")


class BigQuerySessionContext:
    """Sync context manager for BigQuery sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[BigQueryDriver], BigQueryDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: BigQueryDriver | None = None

    def __enter__(self) -> "BigQueryDriver":
        from sqlspec.adapters.bigquery.driver import BigQueryDriver

        self._connection = self._acquire_connection()
        self._driver = BigQueryDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = (
    "BigQueryConnection",
    "BigQueryCursor",
    "BigQueryParam",
    "BigQuerySessionContext",
    "GoogleCloudError",
    "LoadJobConfig",
    "QueryJob",
    "QueryJobConfig",
)
