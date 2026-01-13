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


class CockroachAsyncpgExceptionHandler:
    """Async context manager for CockroachDB AsyncPG exceptions."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "CockroachAsyncpgExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_val is None:
            return False
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

    async def _execute_with_retry(self, operation: "Callable[[], Any]") -> "ExecutionResult":
        if not self._enable_retry:
            return cast("ExecutionResult", await operation())

        last_error: Exception | None = None

        async def attempt_operation() -> "tuple[ExecutionResult | None, Exception | None]":
            try:
                return await operation(), None
            except Exception as exc:
                return None, exc

        for attempt in range(self._retry_config.max_retries + 1):
            result, exc = await attempt_operation()
            if exc is None:
                return cast("ExecutionResult", result)
            last_error = exc
            if not is_retryable_error(exc) or attempt >= self._retry_config.max_retries:
                raise exc
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

    async def dispatch_execute(self, cursor: "CockroachAsyncpgConnection", statement: SQL) -> "ExecutionResult":
        async def operation() -> "ExecutionResult":
            if statement.returns_rows():
                await self._apply_follower_reads(cursor)
            return await super(CockroachAsyncpgDriver, self).dispatch_execute(cursor, statement)

        return await self._execute_with_retry(operation)

    async def dispatch_execute_many(self, cursor: "CockroachAsyncpgConnection", statement: SQL) -> "ExecutionResult":
        async def operation() -> "ExecutionResult":
            return await super(CockroachAsyncpgDriver, self).dispatch_execute_many(cursor, statement)

        return await self._execute_with_retry(operation)

    async def dispatch_execute_script(self, cursor: "CockroachAsyncpgConnection", statement: SQL) -> "ExecutionResult":
        async def operation() -> "ExecutionResult":
            return await super(CockroachAsyncpgDriver, self).dispatch_execute_script(cursor, statement)

        return await self._execute_with_retry(operation)

    def handle_database_exceptions(self) -> "CockroachAsyncpgExceptionHandler":  # type: ignore[override]
        return CockroachAsyncpgExceptionHandler()

    @property
    def data_dictionary(self) -> "CockroachAsyncpgDataDictionary":  # type: ignore[override]
        if self._data_dictionary is None:
            # Intentionally assign CockroachDB-specific data dictionary to parent slot
            object.__setattr__(self, "_data_dictionary", CockroachAsyncpgDataDictionary())
        return cast("CockroachAsyncpgDataDictionary", self._data_dictionary)


register_driver_profile("cockroach_asyncpg", driver_profile)
