"""CockroachDB AsyncPG driver implementation."""

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, TypeVar, cast

from sqlspec.adapters.asyncpg.core import create_mapped_exception, driver_profile
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgPostgresError, CockroachAsyncpgSessionContext
from sqlspec.adapters.cockroach_asyncpg.core import (
    CockroachAsyncpgRetryConfig,
    build_native_export,
    build_native_import,
    calculate_backoff_seconds,
    is_retryable_error,
    native_export_telemetry,
    native_import_telemetry,
    normalize_native_export_query,
)
from sqlspec.adapters.cockroach_asyncpg.data_dictionary import CockroachAsyncpgDataDictionary
from sqlspec.core import SQL, register_driver_profile
from sqlspec.driver import BaseAsyncExceptionHandler
from sqlspec.exceptions import SerializationConflictError, TransactionRetryError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgConnection
    from sqlspec.core import StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("CockroachAsyncpgDriver", "CockroachAsyncpgExceptionHandler", "CockroachAsyncpgSessionContext")

logger = get_logger("sqlspec.adapters.cockroach_asyncpg")
_T = TypeVar("_T")


class CockroachAsyncpgExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for CockroachDB AsyncPG exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        _ = exc_type
        if isinstance(exc_val, CockroachAsyncpgPostgresError) or has_sqlstate(exc_val):
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

    async def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Export native CSV/Parquet to generated files under a remote prefix.

        CSV is headerless. NULL values require an explicit nullas convention;
        server failures propagate without replay through the inherited path.
        """
        file_format = format_hint or "parquet"
        resolved = None
        if self._native_storage_ready() and file_format in {"csv", "parquet"}:
            resolved = self._storage_pipeline().resolve_destination(destination)
        if resolved is not None and resolved.protocol in {"s3", "gs", "gcs", "azure"}:
            prepared = self.prepare_statement(statement, parameters, statement_config=statement_config, kwargs=kwargs)
            prepared.compile()
            compiled_query, values = self._compiled_sql(prepared, prepared.statement_config)
            query = normalize_native_export_query(compiled_query)
            if (
                query is not None
                and not prepared.is_script
                and not prepared.is_many
                and prepared.operation_type == "SELECT"
                and isinstance(values, (list, tuple))
            ):
                command, bound = build_native_export(
                    query,
                    list(values),
                    resolved.uri,
                    file_format,
                    self.driver_features.get("native_storage_csv_options", {}),
                )
                rows = await self._execute_native_storage(command, bound)
                produced = native_export_telemetry(rows, resolved.uri, resolved.protocol, file_format)
                self._attach_partition_telemetry(produced, partitioner)
                return self._storage_job(produced, telemetry)
        return await super().select_to_storage(
            statement,
            destination,
            *parameters,
            statement_config=statement_config,
            partitioner=partitioner,
            format_hint=format_hint,
            telemetry=telemetry,
            **kwargs,
        )

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Append via native IMPORT when eligible, taking the table offline.

        CockroachDB invalidates foreign keys during IMPORT. Overwrite and active
        transactions use the inherited path. CSV needs explicit skip (0 for no
        header); nullif is never inferred. Native failures are never replayed.
        """
        options = self.driver_features.get("native_storage_csv_options", {})
        resolved = None
        if (
            self._native_storage_ready()
            and not overwrite
            and (file_format == "parquet" or (file_format == "csv" and "skip" in options))
        ):
            resolved = self._storage_pipeline().resolve_destination(source)
        if resolved is not None and resolved.protocol in {"s3", "gs", "gcs", "azure"}:
            command, bound = build_native_import(table, resolved.uri, file_format, options)
            rows = await self._execute_native_storage(command, bound)
            produced = native_import_telemetry(rows, table, resolved.protocol, file_format)
            self._attach_partition_telemetry(produced, partitioner)
            return self._storage_job(produced)
        return await super().load_from_storage(
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite
        )

    def _native_storage_ready(self) -> bool:
        return bool(self.driver_features.get("enable_native_storage")) and not self._connection_in_transaction()

    async def _execute_native_storage(self, command: str, parameters: "list[Any]") -> "list[dict[str, Any]]":
        handler = self.handle_database_exceptions()
        rows = []
        async with handler:
            rows = [dict(row) for row in await self.connection.fetch(command, *parameters)]
        if handler.pending_exception is not None:
            raise handler.pending_exception
        return rows

    async def run_transaction_with_retry(self, operation: "Callable[[], Awaitable[_T]]") -> _T:
        """Execute a full CockroachDB transaction callback with serialization retries."""
        if not self._enable_retry or self._connection_in_transaction():
            return await operation()

        last_error: BaseException | None = None

        for attempt in range(self._retry_config.max_retries + 1):
            try:
                await self.begin()
                result = await operation()
                await self.commit()
            except Exception as exc:
                last_error = exc
                with contextlib.suppress(Exception):
                    await self.rollback()
                if not is_retryable_error(exc) or attempt >= self._retry_config.max_retries:
                    raise
            else:
                return result
            delay = calculate_backoff_seconds(attempt, self._retry_config)
            if self._retry_config.enable_logging:
                logger.debug("CockroachDB retry %s/%s after %.3fs", attempt + 1, self._retry_config.max_retries, delay)
            await asyncio.sleep(delay)

        msg = "CockroachDB transaction retry limit exceeded"
        raise TransactionRetryError(msg) from last_error

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        return await self._dispatch_execute_impl(cursor, statement)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        return await self._dispatch_execute_many_impl(cursor, statement)

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        return await self._dispatch_execute_script_impl(cursor, statement)

    def handle_database_exceptions(self) -> "CockroachAsyncpgExceptionHandler":  # type: ignore[override]
        return CockroachAsyncpgExceptionHandler()

    @property
    def data_dictionary(self) -> "CockroachAsyncpgDataDictionary":  # type: ignore[override]
        if self._data_dictionary is None:
            # Intentionally assign CockroachDB-specific data dictionary to parent slot
            object.__setattr__(self, "_data_dictionary", CockroachAsyncpgDataDictionary())
        return cast("CockroachAsyncpgDataDictionary", self._data_dictionary)

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
        return await AsyncpgDriver.dispatch_execute_many(self, cursor, statement)

    async def _dispatch_execute_script_impl(
        self, cursor: "CockroachAsyncpgConnection", statement: SQL
    ) -> "ExecutionResult":
        return await AsyncpgDriver.dispatch_execute_script(self, cursor, statement)


register_driver_profile("cockroach_asyncpg", driver_profile)
