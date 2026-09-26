"""arrow-odbc sync driver."""

import contextlib
import re
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from itertools import chain
from typing import TYPE_CHECKING, Any, Final, cast

from sqlglot import Dialect, exp
from sqlglot.tokenizer_core import TokenType

from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection, ArrowOdbcCursor, ArrowOdbcError, ArrowOdbcRawCursor
from sqlspec.adapters.arrow_odbc.core import (
    build_statement_config,
    create_mapped_exception,
    driver_profile,
    normalize_column_names,
    resolve_dialect_from_dbms_name,
)
from sqlspec.adapters.arrow_odbc.data_dictionary import ArrowOdbcDataDictionary
from sqlspec.core import (
    SQL,
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase, SyncRowStream, validate_savepoint_name
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.text import quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementConfig, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowReturnFormat, StatementParameters

__all__ = ("ArrowOdbcCursor", "ArrowOdbcDriver", "ArrowOdbcExceptionHandler", "resolve_dialect_from_dbms_name")


class ArrowOdbcExceptionHandler(BaseSyncExceptionHandler):
    """Sync context manager handling arrow-odbc exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, ArrowOdbcError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class ArrowOdbcStreamSource:
    """Native Arrow ODBC chunk source backed by Arrow record batches."""

    __slots__ = ("_chunk_size", "_driver", "_parameters", "_reader", "_sql")

    def __init__(
        self, driver: "ArrowOdbcDriver", sql: str, parameters: "list[str | None] | None", chunk_size: int
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._reader: Any = None

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        with handler:
            reader = self._driver._read_arrow_batches(self._sql, self._parameters, self._chunk_size)
            self._reader = iter(self._driver._normalize_reader(_to_pyarrow_reader(reader)))
        self._driver._check_pending_exception(handler)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        reader = self._reader
        if reader is None:
            return []
        while True:
            try:
                batch = next(reader)
            except StopIteration:
                return []
            rows = cast("list[dict[str, Any]]", batch.to_pylist())
            if rows:
                return rows

    def close(self, error: bool = False) -> None:
        reader = self._reader
        self._reader = None
        close = getattr(reader, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()


class ArrowOdbcDriver(SyncDriverAdapterBase):
    """Sync driver for generic ODBC connections with Arrow-native transfer."""

    __slots__ = (
        "_chunk_size_val",
        "_connection_autocommit",
        "_data_dictionary",
        "_dbms_name",
        "_dialect",
        "_lowercase_column_names",
        "_max_batch_bytes",
        "_max_binary_size_val",
        "_max_text_size_val",
        "_payload_text_encoding",
        "_query_timeout_sec_val",
        "_transaction_active",
        "_use_concurrent_fetch",
        "dialect",
    )

    def __init__(
        self,
        connection: "ArrowOdbcConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = dict(driver_features or {})
        self._dbms_name = self._resolve_dbms_name(features)
        self._dialect = resolve_dialect_from_dbms_name(self._dbms_name)
        statement_dialect = _statement_dialect_for(self._dialect)
        if statement_config is None:
            statement_config = build_statement_config(dialect=statement_dialect).replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )
        else:
            statement_config = statement_config.replace(dialect=statement_dialect)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)
        self._chunk_size_val: int = int(features.get("chunk_size") or 65_536)
        self._max_batch_bytes: int | None = features.get("max_bytes_per_batch")
        self._max_binary_size_val: int | None = features.get("max_binary_size")
        self._max_text_size_val: int | None = features.get("max_text_size")
        self._query_timeout_sec_val: int | None = features.get("query_timeout_sec")
        self._payload_text_encoding: Any = features.get("payload_text_encoding")
        self._use_concurrent_fetch: bool = bool(features.get("fetch_concurrently", True))
        self._connection_autocommit: bool = bool(features.get("connection_autocommit", True))
        self._lowercase_column_names: bool = bool(features.get("enable_lowercase_column_names", self._dialect == "db2"))
        self.dialect = statement_dialect
        self._data_dictionary: ArrowOdbcDataDictionary | None = None
        self._transaction_active = False

    @property
    def data_dictionary(self) -> "ArrowOdbcDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = ArrowOdbcDataDictionary(self._dialect)
        return self._data_dictionary

    def dispatch_execute(self, cursor: "ArrowOdbcRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        if self._dialect == "mssql":
            sql, prepared_parameters = _inline_mssql_pagination_parameters(sql, prepared_parameters)
        parameters = _odbc_parameters(prepared_parameters, naive_utc_datetimes=self._dialect == "db2")

        if statement.returns_rows():
            reader = self._read_arrow_batches(sql, parameters, self._chunk_size())
            table = self._normalize_table(_reader_to_table(reader))
            rows = table.to_pylist()
            column_names = table.column_names
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=column_names,
                data_row_count=table.num_rows,
                is_select_result=True,
                row_format="dict",
            )

        cursor.execute(query=sql, parameters=parameters)
        return self.create_execution_result(cursor, rowcount_override=0)

    def dispatch_execute_many(self, cursor: "ArrowOdbcRawCursor", statement: "SQL") -> "ExecutionResult":
        msg = "arrow-odbc does not expose a row-oriented executemany API; use bulk_insert_arrow() for Arrow ingestion."
        raise NotImplementedError(msg)

    def dispatch_execute_script(self, cursor: "ArrowOdbcRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        if self._dialect == "mssql":
            cursor.execute(query=sql, parameters=_odbc_parameters(prepared_parameters))
            return self.create_execution_result(
                cursor, rowcount_override=-1, statement_count=1, successful_statements=1, is_script_result=True
            )
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        parameters = _odbc_parameters(prepared_parameters, naive_utc_datetimes=self._dialect == "db2")
        successful_count = 0
        for stmt in statements:
            cursor.execute(query=stmt, parameters=parameters)
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native Arrow ODBC row stream backed by record batches."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        if self._dialect == "mssql":
            sql, prepared_parameters = _inline_mssql_pagination_parameters(sql, prepared_parameters)
        return SyncRowStream(
            ArrowOdbcStreamSource(
                self, sql, _odbc_parameters(prepared_parameters, naive_utc_datetimes=self._dialect == "db2"), chunk_size
            )
        )

    def collect_rows(self, cursor: "ArrowOdbcRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        return fetched, [], len(fetched)

    def resolve_rowcount(self, cursor: "ArrowOdbcRawCursor") -> int:
        return 0

    def begin(self) -> None:
        """Begin an explicit transaction.

        SQL Server starts one with ``BEGIN TRANSACTION``. Db2 has no begin
        statement: a connection opened with autocommit off is always inside a
        unit of work, so only the boundary is recorded, and an autocommit
        connection is refused because each statement would commit on its own.

        Raises:
            ImproperConfigurationError: If the Db2 connection was opened with autocommit on.
            SQLSpecError: If the begin statement fails.
        """
        if self._dialect == "db2":
            if self._connection_autocommit:
                msg = "Db2 transactions through arrow-odbc require connection_config={'autocommit': False}"
                raise ImproperConfigurationError(msg)
            self._transaction_active = True
            return
        try:
            self.connection.execute("BEGIN TRANSACTION" if self._dialect == "mssql" else "BEGIN")
        except Exception as exc:
            msg = f"Failed to begin transaction: {exc}"
            raise SQLSpecError(msg) from exc
        self._transaction_active = True

    def commit(self) -> None:
        try:
            if self._dialect == "mssql" and self._transaction_active:
                self.connection.execute("COMMIT TRANSACTION")
            else:
                self.connection.commit()
        except ArrowOdbcError as exc:
            msg = f"Failed to commit transaction: {exc}"
            raise SQLSpecError(msg) from exc
        self._transaction_active = False

    def rollback(self) -> None:
        try:
            if self._dialect == "mssql" and self._transaction_active:
                self.connection.execute("ROLLBACK TRANSACTION")
            else:
                self.connection.rollback()
        except ArrowOdbcError as exc:
            msg = f"Failed to rollback transaction: {exc}"
            raise SQLSpecError(msg) from exc
        finally:
            self._transaction_active = False

    def with_cursor(self, connection: "ArrowOdbcConnection") -> "ArrowOdbcCursor":
        return ArrowOdbcCursor(connection)

    def handle_database_exceptions(self) -> "ArrowOdbcExceptionHandler":
        return ArrowOdbcExceptionHandler()

    def create_savepoint(self, name: str) -> None:
        safe_name = validate_savepoint_name(name)
        if self._dialect == "mssql":
            self.execute_script(f"SAVE TRANSACTION {safe_name}")
            return
        if self._dialect == "db2":
            self.execute_script(f"SAVEPOINT {safe_name} ON ROLLBACK RETAIN CURSORS")
            return
        self.execute_script(f"SAVEPOINT {safe_name}")

    def release_savepoint(self, name: str) -> None:
        safe_name = validate_savepoint_name(name)
        if self._dialect == "mssql":
            return
        self.execute_script(f"RELEASE SAVEPOINT {safe_name}")

    def rollback_to_savepoint(self, name: str) -> None:
        safe_name = validate_savepoint_name(name)
        if self._dialect == "mssql":
            self.execute_script(f"ROLLBACK TRANSACTION {safe_name}")
            return
        self.execute_script(f"ROLLBACK TO SAVEPOINT {safe_name}")

    def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        return_format: "ArrowReturnFormat" = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute a query and return native Arrow results."""
        ensure_pyarrow()
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        prepared_statement.compile()
        sql, prepared_parameters = self._compiled_sql(prepared_statement, config)
        if self._dialect == "mssql":
            sql, prepared_parameters = _inline_mssql_pagination_parameters(sql, prepared_parameters)
        resolved_batch_size = batch_size or self._chunk_size()
        table: Any | None = None

        exc_handler = self.handle_database_exceptions()
        with exc_handler, self.with_cursor(self.connection):
            reader = self._read_arrow_batches(
                sql,
                _odbc_parameters(prepared_parameters, naive_utc_datetimes=self._dialect == "db2"),
                resolved_batch_size,
            )
            if return_format in {"reader", "batches"}:
                arrow_reader = self._normalize_reader(_to_pyarrow_reader(reader))
                return build_arrow_result_from_reader(
                    prepared_statement,
                    arrow_reader,
                    return_format=return_format,
                    batch_size=resolved_batch_size,
                    arrow_schema=arrow_schema,
                )
            table = self._normalize_table(_reader_to_table(reader))
        self._check_pending_exception(exc_handler)

        if table is None:
            msg = "arrow-odbc did not return an Arrow table."
            raise SQLSpecError(msg)
        return build_arrow_result_from_table(
            prepared_statement,
            table,
            return_format=return_format,
            batch_size=resolved_batch_size,
            arrow_schema=arrow_schema,
        )

    def bulk_insert_arrow(self, target_table: str, source: Any, *, chunk_size: int | None = None) -> None:
        """Insert an Arrow table or reader into a database table."""
        ensure_pyarrow()
        import pyarrow as pa

        resolved_chunk_size = chunk_size or self._chunk_size()
        exc_handler = self.handle_database_exceptions()
        with exc_handler, self.with_cursor(self.connection):
            if isinstance(source, pa.Table) and hasattr(self.connection, "from_table_to_db"):
                self.connection.from_table_to_db(source=source, target=target_table, chunk_size=resolved_chunk_size)
                self._check_pending_exception(exc_handler)
                return

            reader = _table_to_reader(source, resolved_chunk_size) if isinstance(source, pa.Table) else source
            if hasattr(self.connection, "insert_into_table"):
                self.connection.insert_into_table(reader=reader, table=target_table, chunk_size=resolved_chunk_size)
                self._check_pending_exception(exc_handler)
                return
        self._check_pending_exception(exc_handler)

        msg = "arrow-odbc connection does not expose table import APIs."
        raise ImproperConfigurationError(msg)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into a table via arrow-odbc bulk insert."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            target = _db2_table_reference(table) if self._dialect == "db2" else _quote_odbc_table(table)
            self.execute(f"DELETE FROM {target}")
        self.bulk_insert_arrow(table, arrow_table)
        telemetry_payload = self._ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage into a table via arrow-odbc bulk insert."""
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def _connection_in_transaction(self) -> bool:
        """Return whether SQLSpec holds an explicit transaction on the connection.

        arrow-odbc does not expose a portable transaction-state API, so the
        driver tracks the boundary opened by begin() and closed by
        commit()/rollback().
        """
        return self._transaction_active

    def _read_arrow_batches(self, sql: str, parameters: "list[str | None] | None", batch_size: int) -> Any:
        kwargs: dict[str, Any] = {
            "query": sql,
            "batch_size": batch_size,
            "parameters": parameters,
            "max_bytes_per_batch": self._max_batch_bytes,
            "max_text_size": self._max_text_size_val,
            "max_binary_size": self._max_binary_size_val,
            "fetch_concurrently": self._use_concurrent_fetch,
        }
        if self._query_timeout_sec_val is not None:
            kwargs["query_timeout_sec"] = self._query_timeout_sec_val
        if self._payload_text_encoding is not None:
            kwargs["payload_text_encoding"] = self._payload_text_encoding
        return self.connection.read_arrow_batches(**kwargs)

    def _chunk_size(self) -> int:
        return self._chunk_size_val

    def _normalize_table(self, table: Any) -> Any:
        """Rename implicit-uppercase columns of an Arrow table when lowercasing is enabled."""
        names = normalize_column_names(table.column_names, self._lowercase_column_names)
        if names == table.column_names:
            return table
        return table.rename_columns(names)

    def _normalize_reader(self, reader: "ArrowRecordBatchReader") -> "ArrowRecordBatchReader":
        """Wrap a record batch reader so its schema and batches carry normalized column names."""
        import pyarrow as pa

        schema = reader.schema
        names = normalize_column_names(schema.names, self._lowercase_column_names)
        if names == schema.names:
            return reader
        renamed = schema
        for index, name in enumerate(names):
            renamed = renamed.set(index, renamed.field(index).with_name(name))
        return pa.RecordBatchReader.from_batches(
            renamed, (pa.RecordBatch.from_arrays(batch.columns, schema=renamed) for batch in reader)
        )

    @staticmethod
    def _resolve_dbms_name(features: "dict[str, Any]") -> str | None:
        dbms_name = features.get("dbms_name")
        if dbms_name:
            return str(dbms_name)
        connection_string = features.get("connection_string")
        if connection_string:
            return str(connection_string)
        return None


def _quote_odbc_table(table: str) -> str:
    return ".".join(quote_identifier(part) for part in split_qualified_identifier(table))


def _db2_table_reference(table: str) -> str:
    """Render a table name the way Db2 resolves the bulk-insert target.

    Unquoted parts that are plain identifiers stay unquoted so Db2 folds them
    to uppercase; quoted parts and anything else are double-quoted verbatim.

    Args:
        table: Table name, optionally schema-qualified and quoted.

    Returns:
        The table reference for use in a Db2 statement.
    """
    rendered: list[str] = []
    for part in exp.to_table(table, dialect="db2").parts:
        plain = isinstance(part, exp.Identifier) and not part.quoted
        rendered.append(part.name if plain and _DB2_PLAIN_IDENTIFIER.match(part.name) else quote_identifier(part.name))
    return ".".join(rendered)


def _statement_dialect_for(dialect: str) -> str:
    if dialect == "mssql":
        return "tsql"
    return dialect


_DB2_PLAIN_IDENTIFIER: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MSSQL_OFFSET_FETCH_PATTERN = re.compile(
    r"OFFSET\s+\?\s+ROWS\s+FETCH\s+(?P<fetch_keyword>NEXT|FIRST)\s+\?\s+ROWS\s+ONLY", re.IGNORECASE
)
_MSSQL_PAGINATION_PARAMETER_COUNT: Final = 2


def _inline_mssql_pagination_parameters(sql: str, parameters: object) -> tuple[str, object]:
    if isinstance(parameters, (list, tuple)) and "TOP" in sql.upper():
        tokens = Dialect.get_or_raise("tsql").tokenize(sql)
        replacements: list[tuple[int, int, str]] = []
        consumed: set[int] = set()
        parameter_index = 0
        for index, token in enumerate(tokens):
            if token.token_type != TokenType.PLACEHOLDER:
                continue
            if (
                index >= _MSSQL_PAGINATION_PARAMETER_COUNT
                and tokens[index - 1].token_type == TokenType.L_PAREN
                and tokens[index - 2].token_type == TokenType.TOP
                and index + 1 < len(tokens)
                and tokens[index + 1].token_type == TokenType.R_PAREN
                and parameter_index < len(parameters)
            ):
                replacements.append((token.start, token.end + 1, str(_pagination_int(parameters[parameter_index]))))
                consumed.add(parameter_index)
            parameter_index += 1
        for start, end, value in reversed(replacements):
            sql = sql[:start] + value + sql[end:]
        if consumed:
            parameters = [value for index, value in enumerate(parameters) if index not in consumed]
    match = _MSSQL_OFFSET_FETCH_PATTERN.search(sql)
    if (
        match is None
        or not isinstance(parameters, (list, tuple))
        or len(parameters) < _MSSQL_PAGINATION_PARAMETER_COUNT
    ):
        return sql, parameters

    offset_value = _pagination_int(parameters[-2])
    limit_value = _pagination_int(parameters[-1])
    replacement = f"OFFSET {offset_value} ROWS FETCH {match.group('fetch_keyword')} {limit_value} ROWS ONLY"
    remaining_parameters = parameters[:-2]
    return _MSSQL_OFFSET_FETCH_PATTERN.sub(replacement, sql, count=1), remaining_parameters


def _pagination_int(value: object) -> int:
    unwrapped = getattr(value, "value", value)
    integer = int(cast("Any", unwrapped))
    if not isinstance(unwrapped, str) and unwrapped != integer:
        msg = "SQL Server pagination controls must be whole integers"
        raise ValueError(msg)
    return integer


def _unwrap_parameter(value: Any, naive_utc_datetimes: bool = False) -> Any:
    wrapped = getattr(value, "value", value)
    if wrapped is None:
        return None
    if naive_utc_datetimes and isinstance(wrapped, datetime) and wrapped.tzinfo is not None:
        wrapped = wrapped.astimezone(timezone.utc).replace(tzinfo=None)
    return str(wrapped)


def _odbc_parameters(parameters: Any, *, naive_utc_datetimes: bool = False) -> "list[str | None] | None":
    """Render statement parameters as the text values arrow-odbc binds.

    Args:
        parameters: Compiled statement parameters.
        naive_utc_datetimes: Convert timezone-aware datetimes to naive UTC before rendering.

    Returns:
        The text parameters, or ``None`` when the statement has none.
    """
    if parameters is None:
        return None
    if isinstance(parameters, Mapping):
        return [_unwrap_parameter(value, naive_utc_datetimes) for value in parameters.values()]
    if isinstance(parameters, (list, tuple)):
        if not parameters:
            return None
        return [_unwrap_parameter(value, naive_utc_datetimes) for value in parameters]
    return [_unwrap_parameter(parameters, naive_utc_datetimes)]


def _reader_to_table(reader: Any) -> Any:
    ensure_pyarrow()
    import pyarrow as pa

    if isinstance(reader, pa.Table):
        return reader
    if hasattr(reader, "read_all"):
        return reader.read_all()
    batches = list(reader)
    if not batches:
        return pa.table({})
    return pa.Table.from_batches(batches)


def _to_pyarrow_reader(reader: object) -> "ArrowRecordBatchReader":
    ensure_pyarrow()
    import pyarrow as pa

    if isinstance(reader, pa.RecordBatchReader):
        return reader
    if isinstance(reader, pa.Table):
        return pa.RecordBatchReader.from_batches(reader.schema, reader.to_batches())
    into_reader = getattr(reader, "into_pyarrow_record_batch_reader", None)
    if callable(into_reader):
        return cast("ArrowRecordBatchReader", into_reader())
    schema = getattr(reader, "schema", None)
    if isinstance(schema, pa.Schema):
        return pa.RecordBatchReader.from_batches(schema, cast("Iterable[ArrowRecordBatch]", reader))
    iterator = iter(cast("Iterable[ArrowRecordBatch]", reader))
    try:
        first_batch = next(iterator)
    except StopIteration:
        return pa.RecordBatchReader.from_batches(pa.schema([]), [])
    return pa.RecordBatchReader.from_batches(first_batch.schema, chain((first_batch,), iterator))


def _table_to_reader(table: Any, chunk_size: int) -> Any:
    ensure_pyarrow()
    import pyarrow as pa

    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=chunk_size))


register_driver_profile("arrow_odbc", driver_profile)
