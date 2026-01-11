"""BigQuery adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from google.cloud.bigquery import ArrayQueryParameter, Client, ScalarQueryParameter

    from sqlspec.adapters.bigquery.driver import BigQueryDriver
    from sqlspec.core import StatementConfig

    BigQueryConnection: TypeAlias = Client
    BigQueryParam: TypeAlias = ArrayQueryParameter | ScalarQueryParameter
else:
    try:
        from google.cloud.bigquery import ArrayQueryParameter, Client, ScalarQueryParameter
    except Exception:
        BigQueryConnection = Any
        BigQueryParam = Any
    else:
        BigQueryConnection = Client
        BigQueryParam = ArrayQueryParameter | ScalarQueryParameter


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
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = ("BigQueryConnection", "BigQueryParam", "BigQuerySessionContext")
