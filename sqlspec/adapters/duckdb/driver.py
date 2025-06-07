# ruff: noqa: PLR6301
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from duckdb import DuckDBPyConnection

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.config import InstrumentationConfig

__all__ = ("DuckDBConnection", "DuckDBDriver")


DuckDBConnection = DuckDBPyConnection


logger = get_logger("adapters.duckdb")


class DuckDBDriver(
    SyncDriverAdapterProtocol["DuckDBConnection", RowT], SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
):
    """DuckDB Sync Driver Adapter with modern architecture.

    DuckDB is a fast, in-process analytical database built for modern data analysis.
    This driver provides:

    - High-performance columnar query execution
    - Excellent Arrow integration for analytics workloads
    - Direct file querying (CSV, Parquet, JSON) without imports
    - Extension ecosystem for cloud storage and formats
    - Zero-copy operations where possible
    """

    dialect: "DialectType" = "duckdb"
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = True

    def __init__(
        self,
        connection: "DuckDBConnection",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
        )
        # Auto-register storage backends from config
        storage_config = getattr(config, "storage", None)
        if storage_config and getattr(storage_config, "auto_register", True):
            self._register_storage_backends(storage_config)

    def _register_storage_backends(self, storage_config: Any) -> None:
        for key, backend_config in getattr(storage_config, "backends", {}).items():
            self._register_storage_backend_from_config(key, backend_config)

    def _register_storage_backend_from_config(self, key: str, backend_config: Any) -> None:
        from sqlspec.storage import storage_registry

        logger = get_logger("adapters.duckdb")
        # Intentionally catch per-backend errors to allow partial registration and robust startup.
        try:
            storage_registry.register(key, backend_config)
        except Exception as e:
            logger.warning("Failed to register storage backend '%s': %s", key, e)

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "DuckDBConnection") -> Generator["DuckDBConnection", None, None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional["DuckDBConnection"] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )
        return self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.parameters,
            statement,
            connection=connection,
            **kwargs,
        )

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional["DuckDBConnection"] = None,
        **kwargs: Any,
    ) -> Any:
        conn = self._connection(connection)
        final_exec_params: Optional[list[Any]] = None
        if parameters is not None:
            if isinstance(parameters, list):
                final_exec_params = parameters
            elif isinstance(parameters, dict):
                # For dict parameters from literal extraction (param_0, param_1, etc.)
                if parameters and all(key.startswith("param_") for key in parameters):
                    final_exec_params = [parameters[f"param_{i}"] for i in range(len(parameters))]
                else:
                    # For other dict parameters, convert values to list
                    final_exec_params = list(parameters.values())
            else:
                try:
                    if not isinstance(parameters, (str, bytes)):
                        iter(parameters)  # Test if iterable
                        final_exec_params = list(parameters)
                    else:
                        final_exec_params = [parameters]
                except (AttributeError, TypeError):
                    final_exec_params = [parameters]
        with self._get_cursor(conn) as cursor:
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            if self.instrumentation_config.log_parameters and final_exec_params:
                logger.debug("Query parameters: %s", final_exec_params)
            cursor.execute(sql, final_exec_params or [])
            if self.returns_rows(statement.expression):
                fetched_data = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                return {"data": fetched_data, "columns": column_names, "rowcount": cursor.rowcount}
            return {"rowcount": cursor.rowcount if hasattr(cursor, "rowcount") else -1}

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional["DuckDBConnection"] = None,
        **kwargs: Any,
    ) -> Any:
        conn = self._connection(connection)
        final_exec_params = (
            [list(p) if isinstance(p, (list, tuple)) else [p] for p in param_list]
            if param_list and isinstance(param_list, Sequence)
            else []
        )
        with self._get_cursor(conn) as cursor:
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and final_exec_params:
                logger.debug("Query parameters (batch): %s", final_exec_params)
            for params in final_exec_params:
                cursor.execute(sql, params)
            return {"rowcount": cursor.rowcount if hasattr(cursor, "rowcount") else -1}

    def _execute_script(
        self,
        script: str,
        connection: Optional["DuckDBConnection"] = None,
        **kwargs: Any,
    ) -> str:
        conn = self._connection(connection)
        if self.instrumentation_config.log_queries:
            logger.debug("Executing SQL script: %s", script)
        with self._get_cursor(conn) as cursor:
            cursor.execute(script)
        return "SCRIPT EXECUTED"

    def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "duckdb_wrap_select", "database"):
            if not isinstance(result, dict) or "data" not in result:
                logger.warning("Unexpected result format in _wrap_select_result for DuckDB.")
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    column_names=[],
                    operation_type="SELECT",
                    rows_affected=0,
                )

            fetched_tuples: list[tuple[Any, ...]] = result.get("data", [])
            column_names: list[str] = result.get("columns", [])

            rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row)) for row in fetched_tuples]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )
            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", rows_as_dicts),
                column_names=column_names,
                operation_type="SELECT",
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "duckdb_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            rows_affected = -1

            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                operation_type = "SCRIPT"
                rows_affected = 0
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    rows_affected=rows_affected,
                    operation_type=operation_type,
                    metadata={"status_message": result},
                )
            if isinstance(result, dict) and "rowcount" in result:
                rows_affected = result["rowcount"]
            else:
                logger.warning(
                    "Unexpected result format in _wrap_execute_result for DuckDB DML. Expected dict with 'rowcount' or str 'SCRIPT EXECUTED'. Got: %s",
                    type(result),
                )

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", []),
                rows_affected=rows_affected,
                operation_type=operation_type,
            )
