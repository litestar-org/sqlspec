"""Synchronous Snowflake execution and native Arrow storage pipelines."""

import contextlib
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from sqlspec.adapters.snowflake._typing import SnowflakeCursor
from sqlspec.adapters.snowflake.core import (
    DEFAULT_STAGE_PARQUET_THRESHOLD_ROWS,
    build_empty_arrow_table_from_description,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    descriptor_column_name,
    descriptor_type_code,
    driver_profile,
    format_snowflake_identifier,
    is_snowflake_error,
    normalize_column_names,
    normalize_execute_parameters,
    normalize_many_parameters,
    resolve_rowcount,
)
from sqlspec.core import (
    SQL,
    ParameterStyle,
    StatementConfig,
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase, SyncRowStream
from sqlspec.exceptions import SQLSpecError
from sqlspec.storage._staging_sql import build_create_table_from_arrow_schema
from sqlspec.typing import ArrowReturnFormat, StatementParameters
from sqlspec.utils.arrow_helpers import arrow_reader_with_deferred_close
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.serializers import to_json as encode_json
from sqlspec.utils.text import normalize_identifier, quote_identifier
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from sqlspec.adapters.snowflake.data_dictionary import SnowflakeSyncDataDictionary
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, CachedQuery, SQLResult, Statement, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("SnowflakeDriver", "SnowflakeExceptionHandler")


_INSERT_SOURCE_TOKENS = re.compile(
    r"'(?:''|\\.|[^'])*'|\"(?:\"\"|[^\"])*\"|\$\$.*?\$\$|--[^\r\n]*|//[^\r\n]*|/\*.*?\*/|(?P<paren>[()])|(?P<source>\b(?:INSERT|SELECT|VALUES)\b)",
    re.IGNORECASE | re.DOTALL,
)


def _insert_uses_select(sql: str) -> bool:
    depth = 0
    insert_seen = False
    for token in _INSERT_SOURCE_TOKENS.finditer(sql):
        if token.lastgroup == "paren":
            depth += 1 if token.group() == "(" else -1
        elif token.lastgroup == "source" and depth == 0:
            keyword = token.group().upper()
            if keyword == "INSERT":
                insert_seen = True
            elif insert_seen:
                return keyword == "SELECT"
    return False


def _session_query_tag(connection: Any) -> str | None:
    """Return the ``QUERY_TAG`` session parameter a connector session was opened with."""
    session_parameters = getattr(getattr(connection, "config", None), "session_parameters", None)
    if not isinstance(session_parameters, Mapping):
        session_parameters = getattr(connection, "_session_parameters", None)
    if not isinstance(session_parameters, Mapping):
        return None
    tag = session_parameters.get("QUERY_TAG")
    return tag if isinstance(tag, str) and tag else None


def _normalize_arrow_table(table: Any, features: Mapping[str, Any]) -> Any:
    names = normalize_column_names(
        table.column_names, enable_lowercase_column_names=bool(features.get("enable_lowercase_column_names", True))
    )
    return table.rename_columns(names) if names != table.column_names else table


def _iter_arrow_tables(cursor: Any, features: Mapping[str, Any]) -> Iterator[Any]:
    from snowflake.connector.errors import NotSupportedError

    row_fallback = False
    try:
        for table in cursor.fetch_arrow_batches():
            yield _normalize_arrow_table(table, features)
    except NotSupportedError:
        row_fallback = True
    if row_fallback:
        ensure_pyarrow()
        import pyarrow as pa

        raw_names = [descriptor_column_name(column) for column in cursor.description or ()]
        names = normalize_column_names(
            raw_names, enable_lowercase_column_names=bool(features.get("enable_lowercase_column_names", True))
        )
        rows = cursor.fetchall()
        if not rows:
            yield build_empty_arrow_table_from_description(cursor.description, features)
        else:
            dictionaries = [
                {
                    name: (row[raw_name] if raw_name in row else row.get(name))
                    for raw_name, name in zip(raw_names, names, strict=True)
                }
                if isinstance(row, Mapping)
                else dict(zip(names, row, strict=False))
                for row in rows
            ]
            yield pa.Table.from_pylist(dictionaries)


def materialize_arrow_table_from_batches(cursor: Any, driver_features: Mapping[str, Any]) -> Any:
    """Read native Arrow batches, preserving metadata for empty and JSON results."""
    ensure_pyarrow()
    import pyarrow as pa

    tables = list(_iter_arrow_tables(cursor, driver_features))
    if not tables:
        return build_empty_arrow_table_from_description(cursor.description, driver_features)
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")


def build_arrow_reader_from_cursor(
    cursor: Any,
    cursor_close_callback: Callable[[], None],
    driver_features: Mapping[str, Any],
    *,
    batch_size: int | None = None,
) -> Any:
    """Stream Arrow batches and close the cursor on exhaustion or early close."""
    ensure_pyarrow()
    import pyarrow as pa

    tables = iter(_iter_arrow_tables(cursor, driver_features))
    try:
        first = next(tables, None)
        if first is None:
            first = build_empty_arrow_table_from_description(cursor.description, driver_features)
    except BaseException:
        cursor_close_callback()
        raise

    def batches() -> Iterator[Any]:
        try:
            yield from first.to_batches(max_chunksize=batch_size)
            for table in tables:
                if table.schema != first.schema:
                    table = table.cast(first.schema)
                yield from table.to_batches(max_chunksize=batch_size)
        finally:
            cursor_close_callback()

    reader = pa.RecordBatchReader.from_batches(first.schema, batches())
    return arrow_reader_with_deferred_close(reader, cursor_close_callback)


class SnowflakeExceptionHandler(BaseSyncExceptionHandler):
    """Capture mapped connector exceptions outside compiled context-manager frames."""

    __slots__ = ()

    def _handle_exception(self, exc_type: type[BaseException] | None, exc_val: BaseException) -> bool:
        if exc_type is not None and is_snowflake_error(exc_val):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class SnowflakeDriver(SyncDriverAdapterBase):
    """Execute Snowflake SQL with explicit transactions and zero-pandas Arrow I/O."""

    __slots__ = (
        "_active_stream_cursors",
        "_column_name_cache",
        "_data_dictionary",
        "_migration_schema_restore",
        "_session_query_tag",
    )
    dialect = "snowflake"
    native_arrow_export: ClassVar[bool] = True

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )
        features = dict(driver_features or {})
        features.setdefault(
            "storage_capabilities",
            {
                "arrow_export_enabled": True,
                "arrow_import_enabled": True,
                "parquet_export_enabled": True,
                "parquet_import_enabled": True,
                "partition_strategies": ["fixed", "rows_per_chunk"],
            },
        )
        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)
        self._data_dictionary: SnowflakeSyncDataDictionary | None = None
        self._active_stream_cursors: dict[int, Any] = {}
        self._column_name_cache: dict[tuple[tuple[str, ...], bool], list[str]] = {}
        self._migration_schema_restore: str | None = None
        self._session_query_tag: str | None = (
            _session_query_tag(connection) if self.driver_features.get("enable_query_tag_correlation") else None
        )

    def close(self) -> None:
        """Close outstanding native Arrow cursors before returning the connection."""
        cursors, self._active_stream_cursors = self._active_stream_cursors, {}
        for cursor in cursors.values():
            with contextlib.suppress(Exception):
                cursor.close()

    @property
    def data_dictionary(self) -> "SnowflakeSyncDataDictionary":
        """Return the Snowflake catalog dictionary."""
        if self._data_dictionary is None:
            from sqlspec.adapters.snowflake.data_dictionary import SnowflakeSyncDataDictionary

            self._data_dictionary = SnowflakeSyncDataDictionary()
        return self._data_dictionary

    def with_cursor(self, connection: Any) -> SnowflakeCursor:
        """Acquire a deterministically closed connector cursor."""
        return SnowflakeCursor(connection)

    def handle_database_exceptions(self) -> SnowflakeExceptionHandler:
        """Create a deferred connector exception handler."""
        return SnowflakeExceptionHandler()

    def _binding_mode(self, config: StatementConfig | None = None) -> str:
        style = (config or self.statement_config).parameter_config.default_execution_parameter_style
        if style in {ParameterStyle.NAMED_PYFORMAT, ParameterStyle.POSITIONAL_PYFORMAT}:
            return "pyformat"
        return "numeric" if style == ParameterStyle.POSITIONAL_COLON else "qmark"

    def _track_stream_cursor(self, cursor: Any) -> Callable[[], None]:
        self._active_stream_cursors[id(cursor)] = cursor

        def close_cursor() -> None:
            owned = self._active_stream_cursors.pop(id(cursor), None)
            if owned is not None:
                owned.close()

        return close_cursor

    def _statement_execute_options(self) -> dict[str, Any]:
        options: dict[str, Any] = {}
        if self.driver_features.get("enable_query_tag_correlation") and (correlation := CorrelationContext.get()):
            tag = self._session_query_tag
            options["_statement_params"] = {"QUERY_TAG": f"{tag}:{correlation}" if tag else correlation}
        return options

    def _execute_cursor(self, cursor: Any, sql: str, params: Any, config: StatementConfig | None = None) -> None:
        cursor.execute(
            sql,
            normalize_execute_parameters(params, paramstyle=self._binding_mode(config)),
            **self._statement_execute_options(),
        )

    def collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        """Materialize rows with consistent identifier casing."""
        from sqlspec.adapters.snowflake.type_converter import SnowflakeOutputConverter

        converter = (
            SnowflakeOutputConverter(self.driver_features.get("json_deserializer"))
            if self.driver_features.get("enable_json_deserialization", True)
            else None
        )
        data, names, _ = collect_rows(
            fetched,
            cursor.description,
            self._column_name_cache,
            converter=converter,
            enable_lowercase_column_names=bool(self.driver_features.get("enable_lowercase_column_names", True)),
        )
        return data, names, len(data)

    def _execute_cache_hit_with_cursor(
        self, sql: str, params: tuple[Any, ...] | list[Any] | dict[str, Any], cached: "CachedQuery"
    ) -> "SQLResult":
        """Apply statement correlation and Snowflake row semantics on cache hits."""
        statement = self._cached_statement(
            sql, params, cached, params, params_are_simple=True, compiled_sql=cached.compiled_sql
        )
        try:
            with self.with_cursor(self.connection) as cursor:
                return self.build_statement_result(statement, self.dispatch_execute(cursor, statement))
        finally:
            self._release_pooled_statement(statement)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Return connector DML affected rows."""
        return resolve_rowcount(cursor)

    def dispatch_execute(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Compile, bind and materialize one statement."""
        sql, params = self._compiled_sql(statement, self.statement_config)
        self._execute_cursor(cursor, sql, params, statement.statement_config)
        if statement.returns_rows() or self._should_force_select(statement, cursor):
            data, names, count = self.collect_rows(cursor, cursor.fetchall())
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=names,
                data_row_count=count,
                is_select_result=True,
                row_format="tuple",
            )
        return self.create_execution_result(cursor, rowcount_override=resolve_rowcount(cursor))

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Batch VALUES inserts and scalar-bind SELECT inserts under caller transaction control."""
        sql, params = self._compiled_sql(statement, self.statement_config)
        rows = normalize_many_parameters(params, paramstyle=self._binding_mode(statement.statement_config))
        affected = 0
        if rows and _insert_uses_select(sql):
            known_count = True
            for parameters in rows:
                self._execute_cursor(cursor, sql, parameters, statement.statement_config)
                count = resolve_rowcount(cursor)
                known_count = known_count and count >= 0
                affected += max(count, 0)
            if not known_count:
                affected = -1
        elif rows:
            cursor.executemany(sql, rows)
            affected = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Execute hardened Snowflake script splits with a cursor per statement."""
        sql, prepared_parameters = self._compiled_sql(statement, statement.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)
        runtime = self._observability
        has_observers = runtime is not None and runtime.has_statement_observers
        successful = 0
        for index, text in enumerate(statements):
            started = perf_counter() if has_observers else 0.0
            rows_affected: int | None = None
            with self.with_cursor(self.connection) as owned_cursor:
                self._execute_cursor(owned_cursor, text, None, statement.statement_config)
                if has_observers:
                    rows_affected = resolve_rowcount(owned_cursor)
            if has_observers:
                duration = perf_counter() - started
                single_statement = self._sub_statement(text, prepared_parameters)
                try:
                    operation = single_statement.operation_type
                finally:
                    self._release_pooled_statement(single_statement)
                self._emit_script_statement_event(
                    sql=text,
                    parameters=prepared_parameters,
                    operation=operation,
                    rows_affected=rows_affected,
                    duration_s=duration,
                    started_at=started,
                    profile=None,
                    script_ordinal=index,
                    script_statement_count=statement_count,
                )
            successful += 1
        return self.create_execution_result(
            cursor, statement_count=statement_count, successful_statements=successful, is_script_result=True
        )

    def begin(self) -> None:
        """Begin a DML transaction; Snowflake DDL remains autocommitting."""
        handler = self.handle_database_exceptions()
        with handler, self.with_cursor(self.connection) as cursor:
            cursor.execute("BEGIN")
        self._check_pending_exception(handler)

    def commit(self) -> None:
        """Commit the active transaction."""
        handler = self.handle_database_exceptions()
        self._run_with_exception_handler(handler, self.connection.commit)
        self._check_pending_exception(handler)

    def rollback(self) -> None:
        """Roll back the active transaction."""
        handler = self.handle_database_exceptions()
        self._run_with_exception_handler(handler, self.connection.rollback)
        self._check_pending_exception(handler)

    def _connection_in_transaction(self) -> bool:
        handler = self.handle_database_exceptions()
        active = False
        with handler, self.with_cursor(self.connection) as cursor:
            cursor.execute("SELECT CURRENT_TRANSACTION()")
            rows = cursor.fetchall()
            if rows:
                row = rows[0]
                transaction_id = next(iter(row.values()), None) if isinstance(row, Mapping) else row[0]
                active = transaction_id is not None
        self._check_pending_exception(handler)
        return active

    def create_savepoint(self, name: str) -> None:
        """Reject unsupported Snowflake savepoints."""
        msg = "Snowflake does not support savepoints"
        raise NotImplementedError(msg)

    def release_savepoint(self, name: str) -> None:
        """Reject unsupported Snowflake savepoints."""
        msg = "Snowflake does not support savepoints"
        raise NotImplementedError(msg)

    def rollback_to_savepoint(self, name: str) -> None:
        """Reject unsupported Snowflake savepoints."""
        msg = "Snowflake does not support savepoints"
        raise NotImplementedError(msg)

    def execute_async_query(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> str:
        """Submit a warehouse job and return its query identifier."""
        config = statement_config or self.statement_config
        prepared = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        handler = self.handle_database_exceptions()
        query_id = None
        with handler, self.with_cursor(self.connection) as cursor:
            sql, params = self._compiled_sql(prepared, config)
            cursor.execute_async(
                sql,
                normalize_execute_parameters(params, paramstyle=self._binding_mode(config)),
                **self._statement_execute_options(),
            )
            query_id = cursor.sfqid
        self._check_pending_exception(handler)
        if not query_id:
            msg = "Snowflake did not return an asynchronous query identifier"
            raise SQLSpecError(msg)
        return str(query_id)

    def fetch_async_query_result(
        self, query_id: str, *, statement_config: StatementConfig | None = None
    ) -> "SQLResult":
        """Wait for a submitted job and materialize its result."""
        statement = self.prepare_statement("SELECT 1", statement_config=statement_config)
        handler = self.handle_database_exceptions()
        result = None
        with handler, self.with_cursor(self.connection) as cursor:
            cursor.get_results_from_sfqid(query_id)
            fetched = cursor.fetchall()
            if cursor.description:
                data, names, count = self.collect_rows(cursor, fetched)
                execution = self.create_execution_result(
                    cursor,
                    selected_data=data,
                    column_names=names,
                    data_row_count=count,
                    is_select_result=True,
                    row_format="tuple",
                )
            else:
                execution = self.create_execution_result(cursor, rowcount_override=resolve_rowcount(cursor))
            result = self.build_statement_result(statement, execution)
        self._check_pending_exception(handler)
        if result is None:
            msg = "Snowflake asynchronous result is unavailable"
            raise SQLSpecError(msg)
        return result

    def has_schema(self, schema: str) -> bool:
        """Check a schema through a bound catalog lookup."""
        return bool(
            self.select_value_or_none(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :schema_name",
                schema_name=normalize_identifier(schema, "snowflake"),
            )
        )

    def set_migration_session_schema(self, schema: str) -> None:
        """Capture the exact current database and schema once, then switch to a validated schema."""
        quoted = format_snowflake_identifier(schema)
        if self._migration_schema_restore is None:
            current = self.select_one('SELECT CURRENT_DATABASE() AS "database_name", CURRENT_SCHEMA() AS "schema_name"')
            database_name, schema_name = current.get("database_name"), current.get("schema_name")
            if not isinstance(schema_name, str) or not schema_name:
                msg = "Cannot switch Snowflake migration schema without a restorable current schema"
                raise SQLSpecError(msg)
            previous = quote_identifier(schema_name)
            if isinstance(database_name, str) and database_name:
                previous = f"{quote_identifier(database_name)}.{previous}"
            self._migration_schema_restore = previous
        self.execute(f"USE SCHEMA {quoted}")

    def reset_migration_session_schema(self) -> None:
        """Restore the exact captured schema, clearing the capture only after success."""
        previous = self._migration_schema_restore
        if previous is None:
            return
        self.execute(f"USE SCHEMA {previous}")
        self._migration_schema_restore = None

    def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: StatementConfig | None = None,
        return_format: ArrowReturnFormat = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Export native nanoarrow batches without requiring pandas."""
        ensure_pyarrow()
        config = statement_config or self.statement_config
        prepared = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        cursor = self.connection.cursor()
        handler = self.handle_database_exceptions()
        result = None
        reader_owned = False
        try:
            with handler:
                sql, params = self._compiled_sql(prepared, config)
                self._execute_cursor(cursor, sql, params, config)
                if return_format == "reader":
                    close_cursor = self._track_stream_cursor(cursor)

                    reader = build_arrow_reader_from_cursor(
                        cursor, close_cursor, self.driver_features, batch_size=batch_size
                    )
                    result = build_arrow_result_from_reader(
                        prepared, reader, return_format=return_format, batch_size=batch_size, arrow_schema=arrow_schema
                    )
                    reader_owned = True
                else:
                    table = materialize_arrow_table_from_batches(cursor, self.driver_features)
                    result = build_arrow_result_from_table(
                        prepared, table, return_format=return_format, batch_size=batch_size, arrow_schema=arrow_schema
                    )
            self._check_pending_exception(handler)
        finally:
            if not reader_owned:
                self._active_stream_cursors.pop(id(cursor), None)
                cursor.close()
        if result is None:
            msg = "Snowflake did not produce an Arrow result"
            raise SQLSpecError(msg)
        return result

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a lazy Arrow-backed row stream."""
        return SyncRowStream(_SnowflakeArrowStreamSource(self, statement, chunk_size))

    def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: StatementConfig | None = None,
        partitioner: dict[str, object] | None = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Stream a query to object storage through the Arrow bridge."""
        self._require_capability("arrow_export_enabled")
        result = self._select_for_storage(statement, *parameters, statement_config=statement_config)
        payload = self._write_storage_result(
            result, destination, format_hint=format_hint, pipeline=self._storage_pipeline()
        )
        self._attach_partition_telemetry(payload, partitioner)
        return self._storage_job(payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        batch_size: int = 10_000,
        if_not_exists: bool = False,
        partitioner: dict[str, object] | None = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: bool | str | None = None,
        conflict_keys: Sequence[str] | None = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: Sequence[str] | None = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: Mapping[str, str] | None = None,
    ) -> "StorageBridgeJob":
        """Load Arrow rows with batched binding or a temporary Parquet stage."""
        self._require_capability("arrow_import_enabled")
        if batch_size <= 0:
            msg = "batch_size must be positive"
            raise ValueError(msg)
        arrow_table = self._coerce_arrow_table(source)
        formatted = format_snowflake_identifier(table)
        if if_not_exists:
            self.execute(build_create_table_from_arrow_schema(formatted, arrow_table, dialect="snowflake"))
        arrow_table = arrow_table.rename_columns([
            normalize_identifier(name, "snowflake") for name in arrow_table.column_names
        ])
        if stage_table or conflict_keys or drop_indexes:
            return self._execute_staged_load_sync(
                formatted,
                arrow_table,
                raw_loader=self._load_from_arrow_physical,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=format_snowflake_identifier(stage_table) if isinstance(stage_table, str) else stage_table,
                conflict_keys=[normalize_identifier(key, "snowflake") for key in conflict_keys]
                if conflict_keys
                else None,
                on_conflict=on_conflict,
                exclude_update_columns=[normalize_identifier(key, "snowflake") for key in exclude_update_columns]
                if exclude_update_columns
                else None,
                drop_stage_table=drop_stage_table,
                drop_indexes=drop_indexes,
                concurrent_indexes=concurrent_indexes,
                type_casts={normalize_identifier(key, "snowflake"): value for key, value in type_casts.items()}
                if type_casts
                else None,
            )
        return self._load_from_arrow_physical(
            formatted,
            arrow_table,
            batch_size=batch_size,
            partitioner=partitioner,
            overwrite=overwrite,
            telemetry=telemetry,
        )

    def _load_from_arrow_physical(
        self,
        table: str,
        source: Any,
        *,
        batch_size: int = 10_000,
        partitioner: dict[str, object] | None = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load already normalized physical columns without folding them again."""
        arrow_table = self._coerce_arrow_table(source)
        formatted = format_snowflake_identifier(table)
        threshold = int(self.driver_features.get("stage_parquet_threshold_rows", DEFAULT_STAGE_PARQUET_THRESHOLD_ROWS))
        if (
            arrow_table.num_rows
            and self.driver_features.get("enable_stage_parquet_load", True)
            and arrow_table.num_rows >= threshold
        ):
            self._load_parquet_stage(formatted, arrow_table, overwrite=overwrite)
        elif arrow_table.num_rows or overwrite:
            import pyarrow as pa

            nested = {field.name for field in arrow_table.schema if pa.types.is_nested(field.type)}
            serializer = self.driver_features.get("json_serializer", encode_json)
            columns = ", ".join(quote_identifier(name) for name in arrow_table.column_names)
            names = [f"p{index}" for index in range(arrow_table.num_columns)]
            values = ", ".join(
                f"PARSE_JSON(:{parameter})" if name in nested else f":{parameter}"
                for name, parameter in zip(arrow_table.column_names, names, strict=True)
            )
            insert = f"INSERT INTO {formatted} ({columns}) " + (f"SELECT {values}" if nested else f"VALUES ({values})")

            def insert_batches() -> None:
                if overwrite:
                    self.execute(f"DELETE FROM {formatted}")
                for batch in arrow_table.to_batches(max_chunksize=batch_size):
                    rows = [
                        dict(
                            zip(
                                names,
                                (
                                    serializer(value) if name in nested and value is not None else value
                                    for name, value in row.items()
                                ),
                                strict=True,
                            )
                        )
                        for row in batch.to_pylist()
                    ]
                    if nested:
                        for row in rows:
                            self.execute(insert, row)
                    else:
                        self.execute_many(insert, rows)

            self._run_in_load_transaction(insert_batches)
        payload = self._ingest_telemetry(arrow_table)
        payload["destination"] = table
        self._attach_partition_telemetry(payload, partitioner)
        return self._storage_job(payload, telemetry)

    def _run_in_load_transaction(self, operation: Callable[[], None]) -> None:
        """Run DML in the caller's transaction, or in one owned transaction when none is active."""
        owns_transaction = not self._connection_in_transaction()
        if owns_transaction:
            self.begin()
        try:
            operation()
            if owns_transaction:
                self.commit()
        except BaseException:
            if owns_transaction:
                with contextlib.suppress(Exception):
                    self.rollback()
            raise

    def _load_parquet_stage(self, table: str, arrow_table: Any, *, overwrite: bool) -> None:
        """Upload Parquet to a user stage, then replace or append rows and copy them in one DML transaction."""
        import pyarrow.parquet as pq

        stage = f"@~/sqlspec_stage/{uuid4().hex}/"
        handler = self.handle_database_exceptions()
        with handler, self.with_cursor(self.connection) as cursor:
            try:
                with TemporaryDirectory(prefix="sqlspec_sf_") as directory:
                    path = Path(directory) / "part_0.parquet"
                    pq.write_table(arrow_table, path)
                    uri = path.as_uri().replace("'", "''")
                    cursor.execute(f"PUT '{uri}' {stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

                def copy_stage() -> None:
                    if overwrite:
                        cursor.execute(f"DELETE FROM {table}")
                    cursor.execute(
                        f"COPY INTO {table} FROM {stage} FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE PURGE=TRUE"
                    )

                self._run_in_load_transaction(copy_stage)
            finally:
                with contextlib.suppress(Exception):
                    cursor.execute(f"REMOVE {stage}")
        self._check_pending_exception(handler)


class _SnowflakeArrowStreamSource:
    __slots__ = ("_chunk_size", "_driver", "_iterator", "_reader", "_statement", "_variant_names")

    def __init__(self, driver: SnowflakeDriver, statement: SQL, chunk_size: int) -> None:
        self._driver = driver
        self._statement = statement
        self._chunk_size = chunk_size
        self._reader: Any = None
        self._iterator: Any = None
        self._variant_names: set[str] = set()

    def start(self) -> None:
        cursor = self._driver.connection.cursor()
        close_cursor = self._driver._track_stream_cursor(cursor)
        handler = self._driver.handle_database_exceptions()
        try:
            with handler:
                sql, params = self._driver._compiled_sql(self._statement, self._driver.statement_config)
                self._driver._execute_cursor(cursor, sql, params, self._statement.statement_config)
                names = normalize_column_names(
                    [descriptor_column_name(item) for item in cursor.description or ()],
                    enable_lowercase_column_names=bool(
                        self._driver.driver_features.get("enable_lowercase_column_names", True)
                    ),
                )
                self._variant_names = {
                    names[index]
                    for index, item in enumerate(cursor.description or ())
                    if descriptor_type_code(item) in {5, 9, 10}
                }
                self._reader = build_arrow_reader_from_cursor(
                    cursor, close_cursor, self._driver.driver_features, batch_size=self._chunk_size
                )
                self._iterator = iter(self._reader)
            self._driver._check_pending_exception(handler)
        except BaseException:
            close_cursor()
            raise

    def fetch_chunk(self) -> list[dict[str, Any]]:
        handler = self._driver.handle_database_exceptions()
        rows: list[dict[str, Any]] = []
        with handler:
            batch = next(self._iterator, None)
            if batch is not None:
                rows = batch.to_pylist()
                if self._variant_names and self._driver.driver_features.get("enable_json_deserialization", True):
                    from sqlspec.utils.serializers import from_json as decode_json

                    decoder = self._driver.driver_features.get("json_deserializer", decode_json)
                    for row in rows:
                        for name in self._variant_names:
                            if isinstance(row.get(name), str):
                                row[name] = decoder(row[name])
        self._driver._check_pending_exception(handler)
        return rows

    def close(self, error: bool = False) -> None:
        if self._reader is not None:
            self._reader.close()
            self._reader = None


register_driver_profile("snowflake", driver_profile)
