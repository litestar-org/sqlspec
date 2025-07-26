# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

    from sqlspec.typing import ConnectionT

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver.connection import managed_transaction_sync
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncAdapterCacheMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.driver.mixins._query_tools import SyncQueryMixin
from sqlspec.statement.builder._ddl import DropTable, Truncate
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.serializers import to_json

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AdbcConnection: TypeAlias = Connection
else:
    AdbcConnection = Connection


DIALECT_PATTERNS = {
    "postgres": ["postgres", "postgresql"],
    "bigquery": ["bigquery"],
    "sqlite": ["sqlite", "flight", "flightsql"],
    "duckdb": ["duckdb"],
    "mysql": ["mysql"],
    "snowflake": ["snowflake"],
}

# Parameter style configurations
DIALECT_PARAMETER_STYLES = {
    "postgres": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "postgresql": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "bigquery": (ParameterStyle.NAMED_AT, [ParameterStyle.NAMED_AT]),
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK]),
    "duckdb": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
}


class AdbcDriver(
    SyncDriverAdapterBase,
    SyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    ToSchemaMixin,
    SyncQueryMixin,
):
    """ADBC Sync Driver Adapter with modern architecture.

    ADBC (Arrow Database Connectivity) provides a universal interface for connecting
    to multiple database systems with high-performance Arrow-native data transfer.

    This driver provides:
    - Universal connectivity across database backends (PostgreSQL, SQLite, DuckDB, etc.)
    - High-performance Arrow data streaming and bulk operations
    - Intelligent dialect detection and parameter style handling
    - Seamless integration with cloud databases (BigQuery, Snowflake)
    - Driver manager abstraction for easy multi-database support
    """

    connection_type = AdbcConnection
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = False  # TODO: Implement native Parquet export
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(self, connection: "AdbcConnection", config: "Optional[SQLConfig]" = None) -> None:
        dialect = self._get_dialect(connection)
        if config and not config.dialect:
            config = config.replace(dialect=dialect)
        elif not config:
            config = SQLConfig(dialect=dialect)

        super().__init__(connection=connection, config=config)
        self.dialect: DialectType = dialect
        default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
            self.dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
        )
        self.default_parameter_style = default_style
        self.supported_parameter_styles = tuple(supported_styles)

    def _coerce_json(self, value: "Any") -> "Any":
        """ADBC JSON handling varies by underlying driver."""
        if self.dialect == "sqlite" and isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: "Any") -> "Any":
        """ADBC array handling varies by underlying driver."""
        if self.dialect == "sqlite" and isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name."""
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            # Check against known patterns
            for dialect, patterns in DIALECT_PATTERNS.items():
                if any(pattern in vendor_name or pattern in driver_name for pattern in patterns):
                    return dialect
        except Exception:
            logger.warning("Could not reliably determine ADBC dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    def _handle_postgres_rollback(self, cursor: "Cursor") -> None:
        """Handle PostgreSQL rollback requirement after failed transaction."""
        if self.dialect == "postgres":
            with contextlib.suppress(Exception):
                cursor.execute("ROLLBACK")

    def _handle_postgres_empty_params(self, params: "Any") -> "Any":
        """Handle empty parameters for PostgreSQL to avoid struct type errors."""
        if self.dialect == "postgres" and isinstance(params, dict) and not params:
            return None
        return params

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "AdbcConnection") -> "Iterator[Cursor]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _execute_statement(
        self, statement: "SQL", connection: "Optional[ConnectionT]" = None, **kwargs: "Any"
    ) -> "SQLResult":
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        # Determine target parameter style
        target_style = self._select_parameter_style(statement)

        statement._ensure_processed()
        sql, params = self._get_compiled_sql(statement, target_style)

        params = self._handle_postgres_empty_params(self._process_parameters(params))

        if statement.is_many:
            return self._execute_many(sql, params, connection=connection, **kwargs)
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: "Any", statement: "SQL", connection: "Optional[ConnectionT]" = None, **kwargs: "Any"
    ) -> "SQLResult":
        conn = self._connection(connection)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            cursor_params = self._prepare_cursor_parameters(parameters)

            with self._get_cursor(txn_conn) as cursor:
                try:
                    self._execute_with_params(cursor, sql, cursor_params)
                except Exception as e:
                    self._handle_postgres_rollback(cursor)
                    raise e from e

                if self.returns_rows(statement.expression):
                    return self._build_select_result(cursor, statement)

                return self._build_modify_result(cursor, statement)

    def _execute_with_params(self, cursor: "Cursor", sql: str, params: "list[Any]") -> None:
        """Execute SQL with proper parameter handling."""
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

    def _build_select_result(self, cursor: "Cursor", statement: "SQL") -> "SQLResult":
        """Build result for SELECT operations."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]

        if fetched_data and isinstance(fetched_data[0], tuple):
            dict_data: list[dict[Any, Any]] = [dict(zip(column_names, row)) for row in fetched_data]
        else:
            dict_data = fetched_data  # type: ignore[assignment]

        return SQLResult(
            statement=statement,
            data=cast("list[dict[str, Any]]", dict_data),
            column_names=column_names,
            rows_affected=len(dict_data),
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: "Cursor", statement: "SQL") -> "SQLResult":
        """Build SQLResult for non-SELECT operations (INSERT, UPDATE, DELETE)."""
        rows_affected = max(cursor.rowcount, 0)
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=rows_affected,
            operation_type="EXECUTE",
            metadata={"status_message": "OK"},
        )

    def _execute_many(
        self, sql: str, param_list: "Any", connection: "Optional[ConnectionT]" = None, **kwargs: "Any"
    ) -> "SQLResult":
        if not param_list and self.dialect == "postgres":
            return SQLResult(
                statement=SQL(sql, _dialect=self.dialect),
                data=[],
                rows_affected=0,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

        with (
            managed_transaction_sync(self._connection(connection), auto_commit=True) as txn_conn,
            self._get_cursor(txn_conn) as cursor,
        ):
            try:
                cursor.executemany(sql, param_list or [])
            except Exception as e:
                self._handle_postgres_rollback(cursor)
                raise e from e

            return SQLResult(
                statement=SQL(sql, _dialect=self.dialect),
                data=[],
                rows_affected=cursor.rowcount,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    def _execute_script(self, script: str, connection: "Optional[ConnectionT]" = None, **kwargs: "Any") -> "SQLResult":
        with managed_transaction_sync(self._connection(connection), auto_commit=True) as txn_conn:
            statements = self._split_script_statements(script)
            executed_count = 0
            total_rows = 0

            with self._get_cursor(txn_conn) as cursor:
                for statement in statements:
                    if statement.strip():
                        if not kwargs.get("_suppress_warnings"):
                            temp_sql = SQL(statement, config=self.config)
                            temp_sql._ensure_processed()

                        rows = self._execute_single_script_statement(cursor, statement)
                        executed_count += 1
                        total_rows += rows

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={"status_message": "SCRIPT EXECUTED"},
                total_statements=executed_count,
                successful_statements=executed_count,
            )

    def _execute_single_script_statement(self, cursor: "Cursor", statement: str) -> int:
        """Execute a single statement from a script and handle errors."""
        try:
            cursor.execute(statement)
        except Exception as e:
            self._handle_postgres_rollback(cursor)
            raise e from e
        else:
            return cursor.rowcount or 0

    def _fetch_arrow_table(self, sql: "SQL", connection: "Optional[Any]" = None, **kwargs: "Any") -> "ArrowResult":
        """ADBC native Arrow table fetching with zero-copy data transfer."""
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            sql._ensure_processed()
            compiled_sql, params = self._get_compiled_sql(sql, self.default_parameter_style)
            params = self._process_parameters(params)
            params = self._handle_postgres_empty_params(params)
            cursor_params = self._prepare_cursor_parameters(params)

            with self._get_cursor(txn_conn) as cursor:
                self._execute_with_params(cursor, compiled_sql, cursor_params)
                arrow_table = cursor.fetch_arrow_table()
                return ArrowResult(statement=sql, data=arrow_table)

    def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: "Any") -> int:
        """ADBC-optimized Arrow table ingestion using native bulk insert."""
        self._ensure_pyarrow_installed()

        conn = self._connection(None)
        with managed_transaction_sync(conn, auto_commit=True) as txn_conn, self._get_cursor(txn_conn) as cursor:
            if mode == "replace":
                truncate_stmt = Truncate().table(table_name).to_statement(config=self.config)
                cursor.execute(truncate_stmt.to_sql(placeholder_style=ParameterStyle.STATIC))
            elif mode == "create":
                msg = "'create' mode is not supported for ADBC ingestion"
                raise NotImplementedError(msg)
            return cursor.adbc_ingest(table_name, table, mode=mode, **options)  # type: ignore[arg-type]

    def _prepare_cursor_parameters(self, parameters: "Any") -> "list[Any]":
        """Convert parameters to the format expected by ADBC cursor."""
        if parameters is None:
            return []
        if isinstance(parameters, (list, tuple)):
            return list(parameters)
        if isinstance(parameters, dict) and not parameters:
            return []
        return [parameters]

    def _import_from_storage(
        self,
        source_uri: "Union[str, Path]",
        table_name: str,
        format: "Optional[str]" = None,
        mode: str = "create",
        **options: "Any",
    ) -> int:
        """Import data from storage using ADBC's native capabilities."""
        if format is None:
            from pathlib import Path

            path_obj = Path(source_uri)
            format = path_obj.suffix.lstrip(".").lower()  # noqa: A001

        if mode == "replace":
            drop_stmt = DropTable(table_name).if_exists().to_statement(config=self.config)
            self.execute(drop_stmt)

        # Leverage native Arrow capabilities for Parquet
        if format == "parquet" and self.supports_native_parquet_import:
            import pyarrow.parquet as pq

            arrow_table = pq.read_table(source_uri)
            return self._ingest_arrow_table(
                arrow_table, table_name, mode="append" if mode in {"append", "create"} else mode, **options
            )

        return super()._import_from_storage(source_uri, table_name, format, mode, **options)

    def _connection(self, connection: "Optional[ConnectionT]" = None) -> "AdbcConnection":
        """Get the connection to use for the operation."""
        return cast("AdbcConnection", connection or self.connection)
