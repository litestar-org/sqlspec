"""CockroachDB AsyncPG driver implementation."""

import asyncio
import sys
from contextlib import suppress
from typing import TYPE_CHECKING, Any, TypeVar, cast

from sqlspec.adapters.asyncpg.core import create_mapped_exception, driver_profile
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver, AsyncpgExceptionHandler
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
    validate_follower_read_staleness,
)
from sqlspec.adapters.cockroach_asyncpg.data_dictionary import CockroachAsyncpgDataDictionary
from sqlspec.core import SQL, register_driver_profile
from sqlspec.driver import AsyncRowStream
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.adapters.asyncpg.core import AsyncpgStreamSource
    from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgConnection
    from sqlspec.core import SQLResult, StatementConfig
    from sqlspec.driver import CachedQuery, ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = (
    "CockroachAsyncpgDriver",
    "CockroachAsyncpgExceptionHandler",
    "CockroachAsyncpgSessionContext",
    "CockroachAsyncpgStreamSource",
)

logger = get_logger("sqlspec.adapters.cockroach_asyncpg")
_T = TypeVar("_T")


class CockroachAsyncpgStreamSource:
    """Async chunk source for CockroachDB AsyncPG with follower read support."""

    __slots__ = ("_auto_started", "_driver", "_statement", "_underlying")

    def __init__(self, driver: "CockroachAsyncpgDriver", statement: "SQL", underlying: "AsyncpgStreamSource") -> None:
        self._driver = driver
        self._statement = statement
        self._underlying = underlying
        self._auto_started = False

    async def start(self) -> None:
        if self._driver._wants_follower_read(self._statement):
            self._auto_started = await self._driver._begin_follower_read_transaction()
        try:
            await self._underlying.start()
        except BaseException:
            if self._auto_started and self._driver._connection_in_transaction():
                with suppress(Exception):
                    await self._driver.rollback()
            raise

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        return await self._underlying.fetch_chunk()

    async def close(self, error: bool = False) -> None:
        try:
            await self._underlying.close(error=error)
        finally:
            if self._auto_started and self._driver._connection_in_transaction():
                if error:
                    with suppress(Exception):
                        await self._driver.rollback()
                else:
                    await self._driver.commit()


class CockroachAsyncpgExceptionHandler(AsyncpgExceptionHandler):
    """Async context manager for CockroachDB AsyncPG exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        _ = exc_type
        if isinstance(exc_val, CockroachAsyncpgPostgresError) or has_sqlstate(exc_val):
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
        self._data_dictionary = None

    @property
    def data_dictionary(self) -> "CockroachAsyncpgDataDictionary":
        if self._data_dictionary is None:
            object.__setattr__(self, "_data_dictionary", CockroachAsyncpgDataDictionary())
        return cast("CockroachAsyncpgDataDictionary", self._data_dictionary)

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
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Append via native IMPORT when eligible, taking the table offline.

        CockroachDB invalidates foreign keys during IMPORT. Overwrite and active
        transactions use the inherited path. CSV needs explicit skip (0 for no
        header); nullif is never inferred. Native failures are never replayed.
        """
        options = self.driver_features.get("native_storage_csv_options", {})
        resolved = None
        if (
            not kwargs
            and self._native_storage_ready()
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
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
        )

    def _wants_follower_read(self, statement: "SQL") -> bool:
        """Return True when a statement may run under a follower-read transaction."""
        return statement.returns_rows() and not statement.is_modifying_operation()

    async def begin(self) -> None:
        """Begin a transaction and apply follower-read staleness to it.

        The staleness clause must be the transaction's first statement, so it is
        applied only when this call is what opened the transaction. A session
        configured for follower reads is read-only: CockroachDB rejects writes
        against a historical timestamp.
        """
        if self._connection_in_transaction():
            await super().begin()
            return
        await super().begin()
        try:
            await self._apply_follower_reads()
        except Exception as exc:
            with suppress(Exception):
                await self.rollback()
            raise create_mapped_exception(exc) from exc

    async def run_transaction_with_retry(self, operation: "Callable[[], Awaitable[_T]]") -> _T:
        """Execute a full CockroachDB transaction callback with serialization retries.

        A rollback that itself fails leaves the transaction aborted, and every
        statement on it would then fail with a non-retryable error that hides the
        real conflict. The original exception is raised instead of retrying, so
        the rollback failure never replaces the transaction's outcome.
        """
        if not self._enable_retry or self._connection_in_transaction():
            return await operation()

        attempt = 0
        while True:
            try:
                await self.begin()
                result = await operation()
                await self.commit()
            except Exception as exc:
                try:
                    await self.rollback()
                except Exception:
                    raise exc
                if not is_retryable_error(exc) or attempt >= self._retry_config.max_retries:
                    raise
            else:
                return result
            delay = calculate_backoff_seconds(attempt, self._retry_config)
            if self._retry_config.enable_logging:
                logger.debug("CockroachDB retry %s/%s after %.3fs", attempt + 1, self._retry_config.max_retries, delay)
            await asyncio.sleep(delay)
            attempt += 1

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        auto_started = await self._begin_follower_read_transaction() if self._wants_follower_read(statement) else False
        try:
            result = await super().dispatch_execute(cursor, statement)
        except BaseException:
            if auto_started and self._connection_in_transaction():
                with suppress(Exception):
                    await self.rollback()
            raise
        if auto_started and self._connection_in_transaction():
            await self.commit()
        return result

    async def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        auto_started = (
            await self._begin_follower_read_transaction()
            if (cached.operation_profile.returns_rows and not cached.operation_profile.modifies_rows)
            else False
        )
        try:
            result = await super()._execute_cache_hit(sql, params, cached)
        except BaseException:
            if auto_started and self._connection_in_transaction():
                with suppress(Exception):
                    await self.rollback()
            raise
        if auto_started and self._connection_in_transaction():
            await self.commit()
        return result

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native asyncpg row stream wrapped for follower reads."""
        stream = super().dispatch_select_stream(statement, chunk_size)
        if stream is None:
            return None
        source = CockroachAsyncpgStreamSource(self, statement, cast("AsyncpgStreamSource", stream._source))
        return AsyncRowStream(source)

    def handle_database_exceptions(self) -> "CockroachAsyncpgExceptionHandler":
        return CockroachAsyncpgExceptionHandler()

    def _native_storage_ready(self) -> bool:
        return bool(self.driver_features.get("enable_native_storage")) and not self._connection_in_transaction()

    async def _execute_native_storage(self, command: str, parameters: "list[Any]") -> "list[dict[str, Any]]":
        handler = self.handle_database_exceptions()

        async def _fetch() -> list[dict[str, Any]]:
            return [dict(row) for row in await self.connection.fetch(command, *parameters)]

        if sys.exc_info()[1] is None:
            async with handler:
                rows = await _fetch()
        else:
            res = await self._run_with_exception_handler(handler, _fetch)
            rows = res if res is not None else []
        self._check_pending_exception(handler)
        return rows

    async def _apply_follower_reads(self) -> None:
        if not self.driver_features.get("enable_follower_reads", False):
            return
        if not self._follower_staleness:
            return
        staleness = validate_follower_read_staleness(self._follower_staleness)
        await self.connection.execute(f"SET TRANSACTION AS OF SYSTEM TIME {staleness}")

    async def _begin_follower_read_transaction(self) -> bool:
        """Open the transaction a follower read needs so the staleness clause can lead it.

        A statement run outside a transaction gets its own implicit one, which
        the clause could not precede, so a read opens a transaction here when the
        caller has not already done so.
        """
        if not self.driver_features.get("enable_follower_reads", False):
            return False
        if not self._follower_staleness:
            return False
        if self._connection_in_transaction():
            return False
        await self.begin()
        return True


register_driver_profile("cockroach_asyncpg", driver_profile)
