"""CockroachDB AsyncPG driver implementation."""

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, cast

import asyncpg

from sqlspec.adapters.asyncpg.core import create_mapped_exception, driver_profile
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgSessionContext
from sqlspec.adapters.cockroach_asyncpg.core import (
    CockroachAsyncpgRetryConfig,
    calculate_backoff_seconds,
    is_retryable_error,
)
from sqlspec.adapters.cockroach_asyncpg.data_dictionary import CockroachAsyncpgDataDictionary
from sqlspec.core import SQL, register_driver_profile
from sqlspec.driver import BaseAsyncExceptionHandler
from sqlspec.exceptions import SerializationConflictError, TransactionRetryError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgConnection
    from sqlspec.core import StatementConfig
    from sqlspec.driver import ExecutionResult

__all__ = ("CockroachAsyncpgDriver", "CockroachAsyncpgExceptionHandler", "CockroachAsyncpgSessionContext")

logger = get_logger("sqlspec.adapters.cockroach_asyncpg")


class CockroachAsyncpgExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for CockroachDB AsyncPG exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        _ = exc_type
        if isinstance(exc_val, asyncpg.PostgresError) or has_sqlstate(exc_val):
            if has_sqlstate(exc_val) and str(exc_val.sqlstate) == "40001":
                self.pending_exception = SerializationConflictError(str(exc_val))
                return True
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class CockroachAsyncpgDriver(AsyncpgDriver):
    """CockroachDB AsyncPG driver with retry support."""

    __slots__ = ("_enable_retry", "_follower_staleness", "_retry_config")
    dialect = "postgres"

    def __init__(
        self,
        connection: "CockroachAsyncpgConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._retry_config = CockroachAsyncpgRetryConfig.from_features(self.driver_features)
        self._enable_retry = bool(self.driver_features.get("enable_auto_retry", True))
        self._follower_staleness = cast("str | None", self.driver_features.get("default_staleness"))
        # Data dictionary is lazily initialized in property; use parent slot
        self._data_dictionary = None

    async def _execute_with_retry(self, operation: "Callable[..., Any]", *args: Any) -> "ExecutionResult":
        if not self._enable_retry:
            return cast("ExecutionResult", await operation(*args))

        last_error: Exception | None = None

        for attempt in range(self._retry_config.max_retries + 1):
            try:
                return cast("ExecutionResult", await operation(*args))
            except Exception as exc:
                last_error = exc
                if not is_retryable_error(exc) or attempt >= self._retry_config.max_retries:
                    raise
            with contextlib.suppress(Exception):
                await self.connection.execute("ROLLBACK")
            delay = calculate_backoff_seconds(attempt, self._retry_config)
            if self._retry_config.enable_logging:
                logger.debug("CockroachDB retry %s/%s after %.3fs", attempt + 1, self._retry_config.max_retries, delay)
            await asyncio.sleep(delay)

        msg = "CockroachDB transaction retry limit exceeded"
        raise TransactionRetryError(msg) from last_error

    async def _apply_follower_reads(self, cursor: "CockroachAsyncpgConnection") -> None:
        if not self.driver_features.get("enable_follower_reads", False):
            return
        if not self._follower_staleness:
            return
        await cursor.execute(f"SET TRANSACTION AS OF SYSTEM TIME {self._follower_staleness}")

    async def _dispatch_execute_impl(self, cursor: "CockroachAsyncpgConnection", statement: SQL) -> "ExecutionResult":
        if statement.returns_rows():
            await self._apply_follower_reads(cursor)
        return await super().dispatch_execute(cursor, statement)

    async def _dispatch_execute_many_impl(
        self, cursor: "CockroachAsyncpgConnection", statement: SQL
    ) -> "ExecutionResult":
        return await super().dispatch_execute_many(cursor, statement)

    async def _dispatch_execute_script_impl(
        self, cursor: "CockroachAsyncpgConnection", statement: SQL
    ) -> "ExecutionResult":
        return await super().dispatch_execute_script(cursor, statement)

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        if not self._enable_retry:
            return await self._dispatch_execute_impl(cursor, statement)
        return await self._execute_with_retry(self._dispatch_execute_impl, cursor, statement)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        if not self._enable_retry:
            return await super().dispatch_execute_many(cursor, statement)
        return await self._execute_with_retry(self._dispatch_execute_many_impl, cursor, statement)

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        if not self._enable_retry:
            return await super().dispatch_execute_script(cursor, statement)
        return await self._execute_with_retry(self._dispatch_execute_script_impl, cursor, statement)

    def handle_database_exceptions(self) -> "CockroachAsyncpgExceptionHandler":  # type: ignore[override]
        return CockroachAsyncpgExceptionHandler()

    @property
    def data_dictionary(self) -> "CockroachAsyncpgDataDictionary":  # type: ignore[override]
        if self._data_dictionary is None:
            # Intentionally assign CockroachDB-specific data dictionary to parent slot
            object.__setattr__(self, "_data_dictionary", CockroachAsyncpgDataDictionary())
        return cast("CockroachAsyncpgDataDictionary", self._data_dictionary)


register_driver_profile("cockroach_asyncpg", driver_profile)
