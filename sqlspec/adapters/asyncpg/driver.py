"""AsyncPG PostgreSQL driver implementation for async PostgreSQL operations."""

import re
from collections import OrderedDict
from io import BytesIO
from typing import TYPE_CHECKING, Any, Final, NamedTuple, cast

import asyncpg

from sqlspec.adapters.asyncpg.core import build_asyncpg_profile, configure_asyncpg_parameter_serializers
from sqlspec.adapters.asyncpg.data_dictionary import PostgresAsyncDataDictionary
from sqlspec.core import (
    SQL,
    StackOperation,
    StackResult,
    StatementStack,
    build_statement_config_from_profile,
    create_sql_result,
    get_cache_config,
    is_copy_from_operation,
    is_copy_operation,
    register_driver_profile,
)
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.driver._common import StackExecutionObserver, describe_stack_statement
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    StackExecutionError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.asyncpg._typing import AsyncpgConnection, AsyncpgPreparedStatement
    from sqlspec.core import ArrowResult, SQLResult, StatementConfig
    from sqlspec.driver import AsyncDataDictionaryBase, ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

from sqlspec.adapters.asyncpg._typing import AsyncpgSessionContext

__all__ = (
    "AsyncpgCursor",
    "AsyncpgDriver",
    "AsyncpgExceptionHandler",
    "AsyncpgSessionContext",
    "asyncpg_statement_config",
    "build_asyncpg_statement_config",
    "configure_asyncpg_parameter_serializers",
)

logger = get_logger("adapters.asyncpg")


class _NormalizedStackOperation(NamedTuple):
    """Normalized execution metadata used for prepared stack operations."""

    operation: "StackOperation"
    statement: "SQL"
    sql: str
    parameters: "tuple[Any, ...] | dict[str, Any] | None"


ASYNC_PG_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)
EXPECTED_REGEX_GROUPS: Final[int] = 3


class AsyncpgCursor:
    """Context manager for AsyncPG cursor management."""

    __slots__ = ("connection",)

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "AsyncpgConnection":
        return self.connection

    async def __aexit__(self, *_: Any) -> None: ...


class AsyncpgExceptionHandler:
    """Async context manager for handling AsyncPG database exceptions.

    Maps PostgreSQL SQLSTATE error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "AsyncpgExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_val is None:
            return False
        if isinstance(exc_val, asyncpg.PostgresError):
            try:
                self._map_postgres_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
            return False
        if has_sqlstate(exc_val):
            try:
                self._map_postgres_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False

    def _map_postgres_exception(self, e: Any) -> None:
        """Map PostgreSQL exception to SQLSpec exception.

        Args:
            e: asyncpg.PostgresError instance

        Raises:
            Specific SQLSpec exception based on SQLSTATE code
        """
        if isinstance(e, asyncpg.exceptions.UniqueViolationError):
            self._raise_unique_violation(e, "23505")
            return
        if isinstance(e, asyncpg.exceptions.ForeignKeyViolationError):
            self._raise_foreign_key_violation(e, "23503")
            return
        if isinstance(e, asyncpg.exceptions.NotNullViolationError):
            self._raise_not_null_violation(e, "23502")
            return
        if isinstance(e, asyncpg.exceptions.CheckViolationError):
            self._raise_check_violation(e, "23514")
            return
        if isinstance(e, asyncpg.exceptions.PostgresSyntaxError):
            self._raise_parsing_error(e, "42601")
            return

        error_code = e.sqlstate if has_sqlstate(e) and e.sqlstate is not None else None

        if not error_code:
            self._raise_generic_error(e, None)
            return

        if error_code == "23505":
            self._raise_unique_violation(e, error_code)
        elif error_code == "23503":
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == "23502":
            self._raise_not_null_violation(e, error_code)
        elif error_code == "23514":
            self._raise_check_violation(e, error_code)
        elif error_code.startswith("23"):
            self._raise_integrity_error(e, error_code)
        elif error_code.startswith("42"):
            self._raise_parsing_error(e, error_code)
        elif error_code.startswith("08"):
            self._raise_connection_error(e, error_code)
        elif error_code.startswith("40"):
            self._raise_transaction_error(e, error_code)
        elif error_code.startswith("22"):
            self._raise_data_error(e, error_code)
        elif error_code.startswith(("53", "54", "55", "57", "58")):
            self._raise_operational_error(e, error_code)
        else:
            self._raise_generic_error(e, error_code)

    def _raise_unique_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL unique constraint violation [{code}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL foreign key constraint violation [{code}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL not-null constraint violation [{code}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL check constraint violation [{code}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL integrity constraint violation [{code}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL SQL syntax error [{code}]: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL connection error [{code}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL transaction error [{code}]: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL data error [{code}]: {e}"
        raise DataError(msg) from e

    def _raise_operational_error(self, e: Any, code: str) -> None:
        msg = f"PostgreSQL operational error [{code}]: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "str | None") -> None:
        msg = f"PostgreSQL database error [{code}]: {e}" if code else f"PostgreSQL database error: {e}"
        raise SQLSpecError(msg) from e


PREPARED_STATEMENT_CACHE_SIZE: Final[int] = 32


class AsyncpgDriver(AsyncDriverAdapterBase):
    """AsyncPG PostgreSQL driver for async database operations.

    Supports COPY operations, numeric parameter style handling, PostgreSQL
    exception handling, transaction management, SQL statement compilation
    and caching, and parameter processing with type coercion.
    """

    __slots__ = ("_data_dictionary", "_prepared_statements")
    dialect = "postgres"

    def __init__(
        self,
        connection: "AsyncpgConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = asyncpg_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="postgres",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AsyncDataDictionaryBase | None = None
        self._prepared_statements: OrderedDict[str, AsyncpgPreparedStatement] = OrderedDict()

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        """Create context manager for AsyncPG cursor."""
        return AsyncpgCursor(connection)

    def handle_database_exceptions(self) -> "AsyncpgExceptionHandler":
        """Handle database exceptions with PostgreSQL error codes."""
        return AsyncpgExceptionHandler()

    async def _try_special_handling(self, cursor: "AsyncpgConnection", statement: "SQL") -> "SQLResult | None":
        """Handle PostgreSQL COPY operations and other special cases.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special operation was handled, None for standard execution
        """
        if is_copy_operation(statement.operation_type):
            await self._handle_copy_operation(cursor, statement)
            return self.build_statement_result(statement, self.create_execution_result(cursor))

        return None

    async def _handle_copy_operation(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations.

        Supports both COPY FROM STDIN and COPY TO STDOUT operations.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with COPY operation
        """

        execution_args = statement.statement_config.execution_args
        metadata: dict[str, Any] = dict(execution_args) if execution_args else {}
        sql_text = statement.sql
        sql_upper = sql_text.upper()
        copy_data = metadata.get("postgres_copy_data")

        if copy_data and is_copy_from_operation(statement.operation_type) and "FROM STDIN" in sql_upper:
            if isinstance(copy_data, dict):
                data_str = (
                    str(next(iter(copy_data.values())))
                    if len(copy_data) == 1
                    else "\n".join(str(value) for value in copy_data.values())
                )
            elif isinstance(copy_data, (list, tuple)):
                data_str = str(copy_data[0]) if len(copy_data) == 1 else "\n".join(str(value) for value in copy_data)
            else:
                data_str = str(copy_data)

            data_io = BytesIO(data_str.encode("utf-8"))
            await cursor.copy_from_query(sql_text, output=data_io)
            return

        await cursor.execute(sql_text)

    async def _execute_script(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement containing multiple statements

        Returns:
            ExecutionResult with script execution details
        """
        sql, _ = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_result = None

        for stmt in statements:
            result = await cursor.execute(stmt)
            last_result = result
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using AsyncPG's executemany.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            await cursor.executemany(sql, prepared_parameters)

            affected_rows = len(prepared_parameters)
        else:
            affected_rows = 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def execute_stack(
        self, stack: "StatementStack", *, continue_on_error: bool = False
    ) -> "tuple[StackResult, ...]":
        """Execute a StatementStack using asyncpg's rapid batching."""

        if not isinstance(stack, StatementStack) or not stack or self.stack_native_disabled:
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        return await self._execute_stack_native(stack, continue_on_error=continue_on_error)

    async def _execute_stack_native(
        self, stack: "StatementStack", *, continue_on_error: bool
    ) -> "tuple[StackResult, ...]":
        results: list[StackResult] = []

        transaction_cm = None
        if not continue_on_error and not self._connection_in_transaction():
            transaction_cm = self.connection.transaction()

        with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=True) as observer:
            if transaction_cm is not None:
                async with transaction_cm:
                    await self._run_stack_operations(stack, continue_on_error, observer, results)
            else:
                await self._run_stack_operations(stack, continue_on_error, observer, results)

        return tuple(results)

    async def _run_stack_operations(
        self,
        stack: "StatementStack",
        continue_on_error: bool,
        observer: "StackExecutionObserver",
        results: "list[StackResult]",
    ) -> None:
        """Run operations for statement stack execution.

        Extracted from _execute_stack_native to avoid closure compilation issues.
        """
        for index, operation in enumerate(stack.operations):
            try:
                normalized = None
                if operation.method == "execute":
                    normalized = self._normalize_stack_execute_operation(operation)

                if normalized is not None and self._can_prepare_stack_operation(normalized):
                    stack_result = await self._execute_stack_operation_prepared(normalized)
                else:
                    result = await self._execute_stack_operation(operation)
                    stack_result = StackResult(result=result)
            except Exception as exc:
                stack_error = StackExecutionError(
                    index,
                    describe_stack_statement(operation.statement),
                    exc,
                    adapter=type(self).__name__,
                    mode="continue-on-error" if continue_on_error else "fail-fast",
                )
                if continue_on_error:
                    observer.record_operation_error(stack_error)
                    results.append(StackResult.from_error(stack_error))
                    continue
                raise stack_error from exc

            results.append(stack_result)

    async def _execute_statement(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Handles both SELECT queries and non-SELECT operations.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if statement.returns_rows():
            records = await cursor.fetch(sql, *prepared_parameters) if prepared_parameters else await cursor.fetch(sql)

            data = [dict(record) for record in records]
            column_names = list(records[0].keys()) if records else []

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        result = await cursor.execute(sql, *prepared_parameters) if prepared_parameters else await cursor.execute(sql)

        affected_rows = self._parse_asyncpg_status(result) if isinstance(result, str) else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def _can_prepare_stack_operation(self, normalized: "_NormalizedStackOperation") -> bool:
        statement = normalized.statement
        return not statement.is_script and not statement.is_many

    async def _execute_stack_operation_prepared(self, normalized: "_NormalizedStackOperation") -> StackResult:
        prepared = await self._get_prepared_statement(normalized.sql)
        metadata = {"prepared_statement": True}

        if normalized.statement.returns_rows():
            rows = await self._invoke_prepared(prepared, normalized.parameters, fetch=True)
            data = [dict(row) for row in rows]
            sql_result = create_sql_result(normalized.statement, data=data, rows_affected=len(data), metadata=metadata)
            return StackResult.from_sql_result(sql_result)

        status = await self._invoke_prepared(prepared, normalized.parameters, fetch=False)
        rowcount = self._parse_asyncpg_status(status) if isinstance(status, str) else 0
        sql_result = create_sql_result(normalized.statement, rows_affected=rowcount, metadata=metadata)
        return StackResult.from_sql_result(sql_result)

    def _normalize_stack_execute_operation(self, operation: "StackOperation") -> "_NormalizedStackOperation":
        if operation.method != "execute":
            msg = "Prepared execution only supports execute operations"
            raise TypeError(msg)

        kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
        statement_config = kwargs.pop("statement_config", None)
        config = statement_config or self.statement_config

        sql_statement = self.prepare_statement(
            operation.statement, operation.arguments, statement_config=config, kwargs=kwargs
        )
        sql_text, prepared_parameters = self._get_compiled_sql(sql_statement, config)
        return _NormalizedStackOperation(
            operation=operation, statement=sql_statement, sql=sql_text, parameters=prepared_parameters
        )

    async def _invoke_prepared(
        self,
        prepared: "AsyncpgPreparedStatement",
        parameters: "tuple[Any, ...] | dict[str, Any] | list[Any] | None",
        *,
        fetch: bool,
    ) -> Any:
        if parameters is None:
            if fetch:
                return await prepared.fetch()
            await prepared.fetch()
            return prepared.get_statusmsg()

        if isinstance(parameters, dict):
            if fetch:
                return await prepared.fetch(**parameters)
            await prepared.fetch(**parameters)
            return prepared.get_statusmsg()

        if fetch:
            return await prepared.fetch(*parameters)
        await prepared.fetch(*parameters)
        return prepared.get_statusmsg()

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
        """Execute a query and persist results to storage once native COPY is available."""

        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_result_to_storage_async(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into a PostgreSQL table via COPY."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            await self._truncate_table(table)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            await self.connection.copy_records_to_table(table, records=records, columns=columns)
        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Read an artifact from storage and ingest it via COPY."""

        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    @staticmethod
    def _parse_asyncpg_status(status: str) -> int:
        """Parse AsyncPG status string to extract row count.

        AsyncPG returns status strings like "INSERT 0 1", "UPDATE 3", "DELETE 2"
        for non-SELECT operations. This method extracts the affected row count.

        Args:
            status: Status string from AsyncPG operation

        Returns:
            Number of affected rows, or 0 if cannot parse
        """
        if not status:
            return 0

        match = ASYNC_PG_STATUS_REGEX.match(status.strip())
        if match:
            groups = match.groups()
            if len(groups) >= EXPECTED_REGEX_GROUPS:
                try:
                    return int(groups[-1])
                except (ValueError, IndexError):
                    pass

        return 0

    async def begin(self) -> None:
        """Begin a database transaction."""
        try:
            await self.connection.execute("BEGIN")
        except asyncpg.PostgresError as e:
            msg = f"Failed to begin async transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.execute("ROLLBACK")
        except asyncpg.PostgresError as e:
            msg = f"Failed to rollback async transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.execute("COMMIT")
        except asyncpg.PostgresError as e:
            msg = f"Failed to commit async transaction: {e}"
            raise SQLSpecError(msg) from e

    async def _get_prepared_statement(self, sql: str) -> "AsyncpgPreparedStatement":
        cached = self._prepared_statements.get(sql)
        if cached is not None:
            self._prepared_statements.move_to_end(sql)
            return cached

        prepared = cast("AsyncpgPreparedStatement", await self.connection.prepare(sql))
        self._prepared_statements[sql] = prepared
        if len(self._prepared_statements) > PREPARED_STATEMENT_CACHE_SIZE:
            self._prepared_statements.popitem(last=False)
        return prepared

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = PostgresAsyncDataDictionary()
        return self._data_dictionary

    async def _truncate_table(self, table: str) -> None:
        try:
            await self.connection.execute(f"TRUNCATE TABLE {table}")
        except asyncpg.PostgresError as exc:
            msg = f"Failed to truncate table '{table}': {exc}"
            raise SQLSpecError(msg) from exc

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.is_in_transaction())


_ASYNC_PG_PROFILE = build_asyncpg_profile()

register_driver_profile("asyncpg", _ASYNC_PG_PROFILE)


def build_asyncpg_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the AsyncPG statement configuration with optional JSON codecs."""

    effective_serializer = json_serializer or to_json
    effective_deserializer = json_deserializer or from_json

    base_config = build_statement_config_from_profile(
        _ASYNC_PG_PROFILE,
        statement_overrides={"dialect": "postgres"},
        json_serializer=effective_serializer,
        json_deserializer=effective_deserializer,
    )

    parameter_config = configure_asyncpg_parameter_serializers(
        base_config.parameter_config, effective_serializer, deserializer=effective_deserializer
    )

    return base_config.replace(parameter_config=parameter_config)


asyncpg_statement_config = build_asyncpg_statement_config()
