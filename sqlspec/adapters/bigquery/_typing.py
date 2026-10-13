"""BigQuery adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

import contextlib
from importlib import import_module
from typing import TYPE_CHECKING, Any, TypeAlias

import google.cloud.bigquery as bigquery_module
from google.api_core import exceptions as bigquery_exceptions
from google.api_core.client_info import ClientInfo as BigQueryClientInfo
from google.api_core.client_options import ClientOptions as BigQueryClientOptions
from google.api_core.retry import Retry as BigQueryRetry
from google.auth.credentials import Credentials as BigQueryCredentials
from google.cloud.bigquery import ArrayQueryParameter, Client, ScalarQueryParameter, StructQueryParameter
from google.cloud.bigquery import ConnectionProperty as BigQueryConnectionProperty
from google.cloud.bigquery import LoadJobConfig as BigQueryLoadJobConfig
from google.cloud.bigquery import QueryJob as BigQueryQueryJob
from google.cloud.bigquery import QueryJobConfig as BigQueryQueryJobConfig
from google.cloud.bigquery.retry import DEFAULT_RETRY as BIGQUERY_DEFAULT_RETRY
from google.cloud.bigquery.retry import POLLING_DEFAULT_VALUE as BIGQUERY_POLLING_DEFAULT_VALUE
from google.cloud.exceptions import GoogleCloudError

from sqlspec.typing import import_optional

BigQueryConnection: TypeAlias = Client
BigQueryParam: TypeAlias = ArrayQueryParameter | ScalarQueryParameter | StructQueryParameter
BigQueryStorageWriteModule: Any = import_optional("google.cloud.bigquery_storage_v1")
BigQueryStorageWriteTypes: Any = import_optional("google.cloud.bigquery_storage_v1.types")

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    import google.cloud.bigquery_storage as bigquery_storage_read_module

    from sqlspec.adapters.bigquery.driver import BigQueryDriver
    from sqlspec.core import StatementConfig
else:
    bigquery_storage_read_module = import_optional("google.cloud.bigquery_storage")

__all__ = (
    "BIGQUERY_DEFAULT_RETRY",
    "BIGQUERY_POLLING_DEFAULT_VALUE",
    "BigQueryClientInfo",
    "BigQueryClientOptions",
    "BigQueryConnection",
    "BigQueryConnectionProperty",
    "BigQueryCredentials",
    "BigQueryCursor",
    "BigQueryLoadJobConfig",
    "BigQueryParam",
    "BigQueryQueryJob",
    "BigQueryQueryJobConfig",
    "BigQueryRetry",
    "BigQuerySessionContext",
    "BigQueryStorageWriteModule",
    "BigQueryStorageWriteTypes",
    "GoogleCloudError",
    "bigquery_exceptions",
    "bigquery_module",
    "bigquery_storage_read_module",
)

_DRIVER_TYPE: "type[BigQueryDriver] | None" = None


class BigQueryCursor:
    """BigQuery cursor with resource management."""

    __slots__ = ("connection", "job")

    def __init__(self, connection: "BigQueryConnection") -> None:
        self.connection = connection
        self.job: BigQueryQueryJob | None = None

    def __enter__(self) -> "BigQueryConnection":
        return self.connection

    def __exit__(self, *_: Any) -> None:
        """Clean up cursor resources including active QueryJobs."""
        if self.job is not None:
            with contextlib.suppress(Exception):
                if getattr(self.job, "state", None) in {"PENDING", "RUNNING"}:
                    self.job.cancel()
            self.job = None


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
        release_connection: "Callable[..., Any]",
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
        driver_cls = _get_driver_type()
        self._connection = self._acquire_connection()
        self._driver = driver_cls(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if exc_type is not None and self._driver is not None and self._driver._connection_in_transaction():
            with contextlib.suppress(Exception):
                self._driver.rollback()
        if self._connection is not None:
            self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            self._connection = None
        self._driver = None
        return None


def _get_driver_type() -> "type[BigQueryDriver]":
    global _DRIVER_TYPE
    driver_type = _DRIVER_TYPE
    if driver_type is None:
        module = import_module("sqlspec.adapters.bigquery.driver")
        driver_type = module.BigQueryDriver
        _DRIVER_TYPE = driver_type
    return driver_type
