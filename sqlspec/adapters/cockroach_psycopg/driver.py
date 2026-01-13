"""CockroachDB psycopg driver implementation."""

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any, cast

import psycopg

from sqlspec.adapters.cockroach_psycopg._typing import (
    CockroachAsyncConnection,
    CockroachPsycopgAsyncSessionContext,
    CockroachPsycopgSyncSessionContext,
    CockroachSyncConnection,
)
from sqlspec.adapters.cockroach_psycopg.core import (
    CockroachPsycopgRetryConfig,
    apply_driver_features,
    build_statement_config,
    calculate_backoff_seconds,
    driver_profile,
    is_retryable_error,
)
from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.psycopg.core import create_mapped_exception
from sqlspec.adapters.psycopg.driver import PsycopgAsyncDriver, PsycopgSyncDriver
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.exceptions import SerializationConflictError, TransactionRetryError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.driver import ExecutionResult

__all__ = (
    "CockroachPsycopgAsyncDriver",
    "CockroachPsycopgAsyncExceptionHandler",
    "CockroachPsycopgAsyncSessionContext",
    "CockroachPsycopgSyncDriver",
    "CockroachPsycopgSyncExceptionHandler",
    "CockroachPsycopgSyncSessionContext",
)

logger = get_logger("sqlspec.adapters.cockroach_psycopg")


class CockroachPsycopgSyncExceptionHandler:
    """Context manager for handling CockroachDB psycopg exceptions."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "CockroachPsycopgSyncExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, psycopg.Error):
            if has_sqlstate(exc_val) and str(exc_val.sqlstate) == "40001":
                self.pending_exception = SerializationConflictError(str(exc_val))
                return True
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class CockroachPsycopgAsyncExceptionHandler:
    """Async context manager for handling CockroachDB psycopg exceptions."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "CockroachPsycopgAsyncExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, psycopg.Error):
            if has_sqlstate(exc_val) and str(exc_val.sqlstate) == "40001":
                self.pending_exception = SerializationConflictError(str(exc_val))
                return True
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class CockroachPsycopgSyncDriver(PsycopgSyncDriver):
    """CockroachDB sync driver using psycopg.crdb."""

    __slots__ = ("_enable_retry", "_follower_staleness", "_retry_config")
    dialect = "postgres"

    def __init__(
        self,
        connection: CockroachSyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = build_statement_config().replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        statement_config, normalized_features = apply_driver_features(statement_config, driver_features)
        super().__init__(connection=connection, statement_config=statement_config, driver_features=normalized_features)

        self._retry_config = CockroachPsycopgRetryConfig.from_features(self.driver_features)
        self._enable_retry = bool(self.driver_features.get("enable_auto_retry", True))
        self._follower_staleness = cast("str | None", self.driver_features.get("default_staleness"))
        # Data dictionary is lazily initialized in property; use parent slot
        self._data_dictionary = None

    def _execute_with_retry(self, operation: "Callable[[], ExecutionResult]") -> "ExecutionResult":
        if not self._enable_retry:
            return operation()

        last_error: Exception | None = None

        def attempt_operation() -> "tuple[ExecutionResult | None, Exception | None]":
            try:
                return operation(), None
            except Exception as exc:
                return None, exc

        for attempt in range(self._retry_config.max_retries + 1):
            result, exc = attempt_operation()
            if exc is None:
                return cast("ExecutionResult", result)
            last_error = exc
            if not is_retryable_error(exc) or attempt >= self._retry_config.max_retries:
                raise exc
            with contextlib.suppress(Exception):
                self.connection.rollback()
            delay = calculate_backoff_seconds(attempt, self._retry_config)
            if self._retry_config.enable_logging:
                logger.debug("CockroachDB retry %s/%s after %.3fs", attempt + 1, self._retry_config.max_retries, delay)
            time.sleep(delay)

        msg = "CockroachDB transaction retry limit exceeded"
        raise TransactionRetryError(msg) from last_error

    def _apply_follower_reads(self, cursor: Any) -> None:
        if not self.driver_features.get("enable_follower_reads", False):
            return
        if not self._follower_staleness:
            return
        cursor.execute(f"SET TRANSACTION AS OF SYSTEM TIME {self._follower_staleness}")

    def dispatch_execute(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        def operation() -> "ExecutionResult":
            if statement.returns_rows():
                self._apply_follower_reads(cursor)
            return super(CockroachPsycopgSyncDriver, self).dispatch_execute(cursor, statement)

        return self._execute_with_retry(operation)

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        def operation() -> "ExecutionResult":
            return super(CockroachPsycopgSyncDriver, self).dispatch_execute_many(cursor, statement)

        return self._execute_with_retry(operation)

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        def operation() -> "ExecutionResult":
            return super(CockroachPsycopgSyncDriver, self).dispatch_execute_script(cursor, statement)

        return self._execute_with_retry(operation)

    def handle_database_exceptions(self) -> "CockroachPsycopgSyncExceptionHandler":  # type: ignore[override]
        return CockroachPsycopgSyncExceptionHandler()

    @property
    def data_dictionary(self) -> "CockroachPsycopgSyncDataDictionary":  # type: ignore[override]
        if self._data_dictionary is None:
            # Intentionally assign CockroachDB-specific data dictionary to parent slot
            object.__setattr__(self, "_data_dictionary", CockroachPsycopgSyncDataDictionary())
        return cast("CockroachPsycopgSyncDataDictionary", self._data_dictionary)


class CockroachPsycopgAsyncDriver(PsycopgAsyncDriver):
    """CockroachDB async driver using psycopg.crdb."""

    __slots__ = ("_enable_retry", "_follower_staleness", "_retry_config")
    dialect = "postgres"

    def __init__(
        self,
        connection: CockroachAsyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = build_statement_config().replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        statement_config, normalized_features = apply_driver_features(statement_config, driver_features)
        super().__init__(connection=connection, statement_config=statement_config, driver_features=normalized_features)

        self._retry_config = CockroachPsycopgRetryConfig.from_features(self.driver_features)
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
                await self.connection.rollback()
            delay = calculate_backoff_seconds(attempt, self._retry_config)
            if self._retry_config.enable_logging:
                logger.debug("CockroachDB retry %s/%s after %.3fs", attempt + 1, self._retry_config.max_retries, delay)
            await asyncio.sleep(delay)

        msg = "CockroachDB transaction retry limit exceeded"
        raise TransactionRetryError(msg) from last_error

    async def _apply_follower_reads(self, cursor: Any) -> None:
        if not self.driver_features.get("enable_follower_reads", False):
            return
        if not self._follower_staleness:
            return
        await cursor.execute(f"SET TRANSACTION AS OF SYSTEM TIME {self._follower_staleness}")

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        async def operation() -> "ExecutionResult":
            if statement.returns_rows():
                await self._apply_follower_reads(cursor)
            return await super(CockroachPsycopgAsyncDriver, self).dispatch_execute(cursor, statement)

        return await self._execute_with_retry(operation)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        async def operation() -> "ExecutionResult":
            return await super(CockroachPsycopgAsyncDriver, self).dispatch_execute_many(cursor, statement)

        return await self._execute_with_retry(operation)

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        async def operation() -> "ExecutionResult":
            return await super(CockroachPsycopgAsyncDriver, self).dispatch_execute_script(cursor, statement)

        return await self._execute_with_retry(operation)

    def handle_database_exceptions(self) -> "CockroachPsycopgAsyncExceptionHandler":  # type: ignore[override]
        return CockroachPsycopgAsyncExceptionHandler()

    @property
    def data_dictionary(self) -> "CockroachPsycopgAsyncDataDictionary":  # type: ignore[override]
        if self._data_dictionary is None:
            # Intentionally assign CockroachDB-specific data dictionary to parent slot
            object.__setattr__(self, "_data_dictionary", CockroachPsycopgAsyncDataDictionary())
        return cast("CockroachPsycopgAsyncDataDictionary", self._data_dictionary)


register_driver_profile("cockroach_psycopg", driver_profile)
