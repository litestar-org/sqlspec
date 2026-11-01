"""CockroachDB psycopg driver implementation."""

import asyncio
import sys
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any, TypeVar, cast

from sqlspec.adapters.cockroach_psycopg._typing import (
    CockroachAsyncConnection,
    CockroachPsycopgAsyncSessionContext,
    CockroachPsycopgSyncSessionContext,
    CockroachSyncConnection,
)
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_dict_row as dict_row
from sqlspec.adapters.cockroach_psycopg.core import (
    CockroachPsycopgRetryConfig,
    build_native_export,
    build_native_import,
    build_statement_config,
    calculate_backoff_seconds,
    driver_profile,
    is_retryable_error,
    native_export_telemetry,
    native_import_telemetry,
    normalize_native_export_query,
    validate_follower_read_staleness,
)
from sqlspec.adapters.cockroach_psycopg.data_dictionary import (
    CockroachPsycopgAsyncDataDictionary,
    CockroachPsycopgSyncDataDictionary,
)
from sqlspec.adapters.psycopg.core import create_mapped_exception
from sqlspec.adapters.psycopg.driver import (
    PsycopgAsyncDriver,
    PsycopgAsyncExceptionHandler,
    PsycopgSyncDriver,
    PsycopgSyncExceptionHandler,
)
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncRowStream, SyncRowStream
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.adapters.cockroach_psycopg._typing import CockroachAsyncCursor, CockroachSyncCursor
    from sqlspec.adapters.psycopg.core import PsycopgAsyncStreamSource, PsycopgSyncStreamSource
    from sqlspec.core import SQLResult
    from sqlspec.driver import CachedQuery, ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = (
    "CockroachPsycopgAsyncDriver",
    "CockroachPsycopgAsyncExceptionHandler",
    "CockroachPsycopgAsyncSessionContext",
    "CockroachPsycopgAsyncStreamSource",
    "CockroachPsycopgSyncDriver",
    "CockroachPsycopgSyncExceptionHandler",
    "CockroachPsycopgSyncSessionContext",
    "CockroachPsycopgSyncStreamSource",
)

logger = get_logger("sqlspec.adapters.cockroach_psycopg")
_T = TypeVar("_T")


class CockroachPsycopgSyncStreamSource:
    """Sync chunk source for CockroachDB Psycopg with follower read support."""

    __slots__ = ("_auto_started", "_driver", "_underlying")

    def __init__(
        self, driver: "CockroachPsycopgSyncDriver", underlying: "PsycopgSyncStreamSource", auto_started: bool
    ) -> None:
        self._driver = driver
        self._underlying = underlying
        self._auto_started = auto_started

    def start(self) -> None:
        try:
            self._underlying.start()
        except BaseException:
            if self._auto_started and self._driver._connection_in_transaction():
                with suppress(Exception):
                    self._driver.rollback()
            raise

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        return self._underlying.fetch_chunk()

    def close(self, error: bool = False) -> None:
        try:
            self._underlying.close(error=error)
        finally:
            if self._auto_started and self._driver._connection_in_transaction():
                if error:
                    with suppress(Exception):
                        self._driver.rollback()
                else:
                    self._driver.commit()


class CockroachPsycopgAsyncStreamSource:
    """Async chunk source for CockroachDB Psycopg with follower read support."""

    __slots__ = ("_auto_started", "_driver", "_statement", "_underlying")

    def __init__(
        self, driver: "CockroachPsycopgAsyncDriver", statement: "SQL", underlying: "PsycopgAsyncStreamSource"
    ) -> None:
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


class CockroachPsycopgSyncExceptionHandler(PsycopgSyncExceptionHandler):
    """Context manager for handling CockroachDB psycopg exceptions."""

    __slots__ = ()


class CockroachPsycopgAsyncExceptionHandler(PsycopgAsyncExceptionHandler):
    """Async context manager for handling CockroachDB psycopg exceptions."""

    __slots__ = ()


class CockroachPsycopgSyncDriver(PsycopgSyncDriver):
    """CockroachDB sync driver using psycopg.crdb."""

    __slots__ = ("_enable_retry", "_follower_reads_enabled", "_follower_staleness", "_retry_config")
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
        driver_features = dict(driver_features) if driver_features else {}
        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

        self._retry_config = CockroachPsycopgRetryConfig.from_features(self.driver_features)
        self._enable_retry = bool(self.driver_features.get("enable_auto_retry", True))
        self._follower_reads_enabled = bool(self.driver_features.get("enable_follower_reads", False))
        raw_staleness = cast("str | None", self.driver_features.get("default_staleness"))
        self._follower_staleness = validate_follower_read_staleness(raw_staleness) if raw_staleness else None
        self._data_dictionary = None

    @property
    def data_dictionary(self) -> "CockroachPsycopgSyncDataDictionary":  # type: ignore[override]
        if self._data_dictionary is None:
            self._data_dictionary = CockroachPsycopgSyncDataDictionary()  # type: ignore[assignment]
        return cast("CockroachPsycopgSyncDataDictionary", self._data_dictionary)

    def select_to_storage(
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
                rows = self._execute_native_storage(command, bound)
                produced = native_export_telemetry(rows, resolved.uri, resolved.protocol, file_format)
                self._attach_partition_telemetry(produced, partitioner)
                return self._storage_job(produced, telemetry)
        return super().select_to_storage(
            statement,
            destination,
            *parameters,
            statement_config=statement_config,
            partitioner=partitioner,
            format_hint=format_hint,
            telemetry=telemetry,
            **kwargs,
        )

    def load_from_storage(
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
            rows = self._execute_native_storage(command, bound)
            produced = native_import_telemetry(rows, table, resolved.protocol, file_format)
            self._attach_partition_telemetry(produced, partitioner)
            return self._storage_job(produced)
        return super().load_from_storage(
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
        )

    def _wants_follower_read(self, statement: "SQL") -> bool:
        """Return True when a statement may run under a follower-read transaction."""
        return statement.returns_rows() and not statement.is_modifying_operation()

    def begin(self) -> None:
        """Begin a transaction and apply follower-read staleness to it.

        The staleness clause must be the transaction's first statement, so it is
        applied only when this call is what opened the transaction. A session
        configured for follower reads is read-only: CockroachDB rejects writes
        against a historical timestamp.
        """
        if self._connection_in_transaction():
            super().begin()
            return
        super().begin()
        try:
            self._apply_follower_reads()
        except Exception as exc:
            with suppress(Exception):
                self.rollback()
            self._transaction_active = False
            raise create_mapped_exception(exc) from exc

    def run_transaction_with_retry(self, operation: "Callable[[], _T]") -> _T:
        """Execute a full CockroachDB transaction callback with serialization retries.

        A rollback that itself fails leaves the transaction aborted, and every
        statement on it would then fail with a non-retryable error that hides the
        real conflict. The original exception is raised instead of retrying, so
        the rollback failure never replaces the transaction's outcome.
        """
        if not self._enable_retry or self._connection_in_transaction():
            return operation()

        attempt = 0
        while True:
            try:
                self.begin()
                result = operation()
                self.commit()
            except Exception as exc:
                try:
                    self.rollback()
                except Exception:
                    raise exc
                if not is_retryable_error(exc) or attempt >= self._retry_config.max_retries:
                    raise
            else:
                return result
            delay = calculate_backoff_seconds(attempt, self._retry_config)
            if self._retry_config.enable_logging:
                logger.debug("CockroachDB retry %s/%s after %.3fs", attempt + 1, self._retry_config.max_retries, delay)
            time.sleep(delay)
            attempt += 1

    def dispatch_execute(self, cursor: "CockroachSyncCursor", statement: SQL) -> "ExecutionResult":
        auto_started = self._begin_follower_read_transaction() if self._wants_follower_read(statement) else False
        try:
            result = super().dispatch_execute(cursor, statement)
        except BaseException:
            if auto_started and self._connection_in_transaction():
                with suppress(Exception):
                    self.rollback()
            raise
        if auto_started and self._connection_in_transaction():
            self.commit()
        return result

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native psycopg row stream wrapped for follower reads."""
        auto_started = self._begin_follower_read_transaction() if self._wants_follower_read(statement) else False
        try:
            stream = super().dispatch_select_stream(statement, chunk_size)
            if stream is None:
                if auto_started and self._connection_in_transaction():
                    self.commit()
                return None
            source = CockroachPsycopgSyncStreamSource(
                self, cast("PsycopgSyncStreamSource", stream._source), auto_started
            )
            return SyncRowStream(source)
        except BaseException:
            if auto_started and self._connection_in_transaction():
                with suppress(Exception):
                    self.rollback()
            raise

    def handle_database_exceptions(self) -> "CockroachPsycopgSyncExceptionHandler":
        return CockroachPsycopgSyncExceptionHandler()

    def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        auto_started = (
            self._begin_follower_read_transaction()
            if (
                self._follower_reads_enabled
                and cached.operation_profile.returns_rows
                and not cached.operation_profile.modifies_rows
            )
            else False
        )
        try:
            result = super()._execute_cache_hit(sql, params, cached)
        except BaseException:
            if auto_started and self._connection_in_transaction():
                with suppress(Exception):
                    self.rollback()
            raise
        if auto_started and self._connection_in_transaction():
            self.commit()
        return result

    def _native_storage_ready(self) -> bool:
        return (
            bool(self.driver_features.get("enable_native_storage"))
            and self.connection.autocommit
            and not self._connection_in_transaction()
        )

    def _execute_native_storage(self, command: str, parameters: "list[Any]") -> "list[dict[str, Any]]":
        handler = self.handle_database_exceptions()

        def _fetch() -> list[dict[str, Any]]:
            with self.with_cursor(self.connection) as cursor:
                cursor.row_factory = dict_row
                cursor.execute(command.encode("utf-8"), parameters)
                return cursor.fetchall()

        if sys.exc_info()[1] is None:
            with handler:
                rows = _fetch()
        else:
            res = self._run_with_exception_handler(handler, _fetch)
            rows = res if res is not None else []
        self._check_pending_exception(handler)
        return rows

    def _apply_follower_reads(self) -> None:
        if not self._follower_reads_enabled or not self._follower_staleness:
            return
        self.connection.execute(cast("Any", f"SET TRANSACTION AS OF SYSTEM TIME {self._follower_staleness}")).close()

    def _begin_follower_read_transaction(self) -> bool:
        """Open the transaction a follower read needs so the staleness clause can lead it.

        psycopg opens a transaction on the first statement, which would leave the
        clause with nowhere to go, so a read opens one here when the caller has
        not already done so.
        """
        if not self._follower_reads_enabled or not self._follower_staleness:
            return False
        if self._connection_in_transaction():
            return False
        self.begin()
        return True


class CockroachPsycopgAsyncDriver(PsycopgAsyncDriver):
    """CockroachDB async driver using psycopg.crdb."""

    __slots__ = ("_enable_retry", "_follower_reads_enabled", "_follower_staleness", "_retry_config")
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
        driver_features = dict(driver_features) if driver_features else {}
        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

        self._retry_config = CockroachPsycopgRetryConfig.from_features(self.driver_features)
        self._enable_retry = bool(self.driver_features.get("enable_auto_retry", True))
        self._follower_reads_enabled = bool(self.driver_features.get("enable_follower_reads", False))
        raw_staleness = cast("str | None", self.driver_features.get("default_staleness"))
        self._follower_staleness = validate_follower_read_staleness(raw_staleness) if raw_staleness else None
        self._data_dictionary = None

    @property
    def data_dictionary(self) -> "CockroachPsycopgAsyncDataDictionary":  # type: ignore[override]
        if self._data_dictionary is None:
            self._data_dictionary = CockroachPsycopgAsyncDataDictionary()  # type: ignore[assignment]
        return cast("CockroachPsycopgAsyncDataDictionary", self._data_dictionary)

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
            self._transaction_active = False
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

    async def dispatch_execute(self, cursor: "CockroachAsyncCursor", statement: SQL) -> "ExecutionResult":
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

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native psycopg row stream wrapped for follower reads."""
        stream = super().dispatch_select_stream(statement, chunk_size)
        if stream is None:
            return None
        source = CockroachPsycopgAsyncStreamSource(self, statement, cast("PsycopgAsyncStreamSource", stream._source))
        return AsyncRowStream(source)

    def handle_database_exceptions(self) -> "CockroachPsycopgAsyncExceptionHandler":
        return CockroachPsycopgAsyncExceptionHandler()

    async def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        auto_started = (
            await self._begin_follower_read_transaction()
            if (
                self._follower_reads_enabled
                and cached.operation_profile.returns_rows
                and not cached.operation_profile.modifies_rows
            )
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

    def _native_storage_ready(self) -> bool:
        return (
            bool(self.driver_features.get("enable_native_storage"))
            and self.connection.autocommit
            and not self._connection_in_transaction()
        )

    async def _execute_native_storage(self, command: str, parameters: "list[Any]") -> "list[dict[str, Any]]":
        handler = self.handle_database_exceptions()

        async def _fetch() -> list[dict[str, Any]]:
            async with self.with_cursor(self.connection) as cursor:
                cursor.row_factory = dict_row
                await cursor.execute(command.encode("utf-8"), parameters)
                return await cursor.fetchall()

        if sys.exc_info()[1] is None:
            async with handler:
                rows = await _fetch()
        else:
            res = await self._run_with_exception_handler(handler, _fetch)
            rows = res if res is not None else []
        self._check_pending_exception(handler)
        return rows

    async def _apply_follower_reads(self) -> None:
        if not self._follower_reads_enabled or not self._follower_staleness:
            return
        cursor = await self.connection.execute(
            cast("Any", f"SET TRANSACTION AS OF SYSTEM TIME {self._follower_staleness}")
        )
        await cursor.close()

    async def _begin_follower_read_transaction(self) -> bool:
        """Open the transaction a follower read needs so the staleness clause can lead it.

        psycopg opens a transaction on the first statement, which would leave the
        clause with nowhere to go, so a read opens one here when the caller has
        not already done so.
        """
        if not self._follower_reads_enabled or not self._follower_staleness:
            return False
        if self._connection_in_transaction():
            return False
        await self.begin()
        return True


register_driver_profile("cockroach_psycopg", driver_profile)
