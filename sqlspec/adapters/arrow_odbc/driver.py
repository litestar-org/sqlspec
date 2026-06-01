"""arrow-odbc sync driver."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection, ArrowOdbcCursor, ArrowOdbcError, ArrowOdbcRawCursor
from sqlspec.adapters.arrow_odbc.core import (
    build_statement_config,
    create_mapped_exception,
    driver_profile,
    resolve_dialect_from_dbms_name,
)
from sqlspec.adapters.arrow_odbc.data_dictionary import ArrowOdbcDataDictionary
from sqlspec.core import SQL, build_arrow_result_from_table, get_cache_config, register_driver_profile
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementConfig, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

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


class ArrowOdbcDriver(SyncDriverAdapterBase):
    """Sync driver for generic ODBC connections with Arrow-native transfer."""

    __slots__ = (
        "_chunk_size_val",
        "_data_dictionary",
        "_dbms_name",
        "_dialect",
        "_max_batch_bytes",
        "_max_binary_size_val",
        "_max_text_size_val",
        "_query_timeout_sec_val",
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
        self._dbms_name = self._resolve_dbms_name(connection, features)
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
        self._use_concurrent_fetch: bool = bool(features.get("fetch_concurrently", True))
        self.dialect = statement_dialect
        self._data_dictionary: ArrowOdbcDataDictionary | None = None

    @property
    def data_dictionary(self) -> "ArrowOdbcDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = ArrowOdbcDataDictionary(self._dialect)
        return self._data_dictionary

    def dispatch_execute(self, cursor: "ArrowOdbcRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        parameters = _odbc_parameters(prepared_parameters)

        if statement.returns_rows():
            reader = self._read_arrow_batches(sql, parameters, self._chunk_size())
            table = _reader_to_table(reader)
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
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        parameters = _odbc_parameters(prepared_parameters)
        successful_count = 0
        for stmt in statements:
            cursor.execute(query=stmt, parameters=parameters)
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def collect_rows(self, cursor: "ArrowOdbcRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        return fetched, [], len(fetched)

    def resolve_rowcount(self, cursor: "ArrowOdbcRawCursor") -> int:
        return 0

    def begin(self) -> None:
        statement = "BEGIN TRANSACTION" if self._dialect == "mssql" else "BEGIN"
        self.connection.execute(statement)

    def commit(self) -> None:
        try:
            self.connection.commit()
        except Exception as exc:
            msg = f"Failed to commit transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except Exception as exc:
            msg = f"Failed to rollback transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: "ArrowOdbcConnection") -> "ArrowOdbcCursor":
        return ArrowOdbcCursor(connection)

    def handle_database_exceptions(self) -> "ArrowOdbcExceptionHandler":
        return ArrowOdbcExceptionHandler()

    def create_savepoint(self, name: str) -> None:
        if self._dialect == "mssql":
            self.execute_script(f"SAVE TRANSACTION {name}")
            return
        self.execute_script(f"SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        if self._dialect == "mssql":
            return
        self.execute_script(f"RELEASE SAVEPOINT {name}")

    def rollback_to_savepoint(self, name: str) -> None:
        if self._dialect == "mssql":
            self.execute_script(f"ROLLBACK TRANSACTION {name}")
            return
        self.execute_script(f"ROLLBACK TO SAVEPOINT {name}")

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
        # TODO: Arrow fast paths still bypass query-start observability hooks.
        prepared_statement.compile()
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)
        resolved_batch_size = batch_size or self._chunk_size()
        table: Any | None = None

        exc_handler = self.handle_database_exceptions()
        with exc_handler, self.with_cursor(self.connection):
            reader = self._read_arrow_batches(sql, _odbc_parameters(prepared_parameters), resolved_batch_size)
            table = _reader_to_table(reader)
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
        return self.connection.read_arrow_batches(**kwargs)

    def _chunk_size(self) -> int:
        return self._chunk_size_val

    @staticmethod
    def _resolve_dbms_name(connection: "ArrowOdbcConnection", features: "dict[str, Any]") -> str | None:
        dbms_name = getattr(connection, "dbms_name", None)
        if dbms_name:
            return str(dbms_name)
        dbms_name = features.get("dbms_name")
        if dbms_name:
            return str(dbms_name)
        connection_string = features.get("connection_string")
        if connection_string:
            return str(connection_string)
        return None


def _statement_dialect_for(dialect: str) -> str:
    if dialect == "mssql":
        return "tsql"
    return dialect


def _unwrap_parameter(value: Any) -> Any:
    wrapped = getattr(value, "value", value)
    return None if wrapped is None else str(wrapped)


def _odbc_parameters(parameters: Any) -> "list[str | None] | None":
    if parameters is None:
        return None
    if isinstance(parameters, Mapping):
        return [_unwrap_parameter(value) for value in parameters.values()]
    if isinstance(parameters, (list, tuple)):
        if not parameters:
            return None
        return [_unwrap_parameter(value) for value in parameters]
    return [_unwrap_parameter(parameters)]


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


def _table_to_reader(table: Any, chunk_size: int) -> Any:
    ensure_pyarrow()
    import pyarrow as pa

    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=chunk_size))


register_driver_profile("arrow_odbc", driver_profile)
