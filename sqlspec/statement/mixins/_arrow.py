import datetime
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Optional, Protocol, runtime_checkable

from sqlspec.exceptions import MissingDependencyError, SQLConversionError
from sqlspec.statement.result import ArrowResult
from sqlspec.typing import ConnectionT, RowT, SQLParameterType

if TYPE_CHECKING:
    from pathlib import Path

    from sqlspec.statement.sql import SQL, SQLConfig
    from sqlspec.typing import ArrowTable


@runtime_checkable
class ArrowDriverProtocol(Protocol):
    def _execute(self, sql: str, parameters: Any, statement: "SQL", connection: Any, **kwargs: Any) -> Any: ...
    def _wrap_select_result(self, statement: "SQL", raw_result: Any, schema_type: Any = None) -> Any: ...


class ArrowParquetFetchRowsMixin(Generic[ConnectionT]):
    """Mixin for Arrow/Parquet export. Requires driver to implement _execute and _wrap_select_result."""

    # These methods are expected to be provided by the driver class.
    def _fetch_rows_for_arrow(
        self: "ArrowDriverProtocol", stmt_obj: "SQL", connection: Any, **kwargs: "Any"
    ) -> "list[RowT]":
        """Fetch rows for Arrow conversion (sync).

        Args:
            stmt_obj: SQL statement object.
            connection: Database connection.
            **kwargs: Additional options.

        Returns:
            List of row data.
        """
        raw_result = self._execute(
            stmt_obj.to_sql(placeholder_style=getattr(self, "parameter_style", None)),
            stmt_obj.parameters,
            stmt_obj,
            connection,
            **kwargs,
        )
        sql_result = self._wrap_select_result(stmt_obj, raw_result, schema_type=None)
        return sql_result.data or []

    async def _fetch_rows_for_arrow_async(
        self: "ArrowDriverProtocol", stmt_obj: "SQL", connection: Any, **kwargs: "Any"
    ) -> "list[RowT]":
        """Fetch rows for Arrow conversion (async).

        Args:
            stmt_obj: SQL statement object.
            connection: Database connection.
            **kwargs: Additional options.

        Returns:
            List of row data.
        """
        raw_result = await self._execute(
            stmt_obj.to_sql(placeholder_style=getattr(self, "parameter_style", None)),
            stmt_obj.parameters,
            stmt_obj,
            connection,
            **kwargs,
        )
        sql_result = await self._wrap_select_result(stmt_obj, raw_result, schema_type=None)
        return sql_result.data or []

    def _convert_rows_to_arrow(
        self,
        rows: "list[Any]",
        statement: "SQL",
        **kwargs: "Any",
    ) -> "ArrowTable":
        """Convert a list of row data to an Arrow Table.

        Args:
            rows: List of row data (RowT or ModelDTOT).
            statement: SQL statement object.
            **kwargs: Additional options.

        Returns:
            ArrowTable: The resulting Arrow table.

        Raises:
            MissingDependencyError: If pyarrow is not installed.
            SQLConversionError: If conversion fails.
        """
        from sqlspec.statement.mixins._result_converter import ResultConverter
        from sqlspec.typing import PYARROW_INSTALLED

        if not PYARROW_INSTALLED:
            msg = "pyarrow"
            raise MissingDependencyError(msg, "pyarrow")
        import pyarrow as pa

        if not rows:
            col_names = []
            if statement.analysis_result and getattr(statement.analysis_result, "columns", None):
                col_names = list(statement.analysis_result.columns)
            return pa.table({col: [] for col in col_names}) if col_names else pa.table({})
        first_row_dict = ResultConverter.to_schema(rows[0])
        col_names = list(first_row_dict.keys())
        columns_data = {col: [ResultConverter.to_schema(row).get(col) for row in rows] for col in col_names}
        try:
            return pa.table(columns_data)
        except Exception as e:
            msg = f"Error converting rows to Arrow: {e}"
            raise SQLConversionError(msg) from e

    def _write_rows_to_parquet(
        self,
        rows: "list[RowT]",
        statement: "SQL",
        path: "Path",
        **kwargs: "Any",
    ) -> "None":
        """Write a list of row data to a Parquet file.

        Args:
            rows: List of row data.
            statement: SQL statement object.
            path: Path to the Parquet file.
            **kwargs: Additional options.

        Raises:
            MissingDependencyError: If pyarrow is not installed.
            SQLConversionError: If writing fails.
        """
        from sqlspec.typing import PYARROW_INSTALLED

        if not PYARROW_INSTALLED:
            msg = "pyarrow"
            raise MissingDependencyError(msg, "pyarrow")
        import pyarrow.parquet as pq

        arrow_table = self._convert_rows_to_arrow(rows, statement, **kwargs)
        try:
            pq.write_table(arrow_table, path, **kwargs)
        except Exception as e:
            msg = f"Error writing Arrow table to Parquet file '{path}': {e}"
            raise SQLConversionError(msg) from e


@runtime_checkable
class ArrowSyncDriverProtocol(ArrowDriverProtocol, Protocol):
    def _build_statement(
        self,
        statement: "Any",
        parameters: "Optional[SQLParameterType]" = None,
        filters: "list[Any]" = [],
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL": ...
    @property
    def config(self) -> "SQLConfig": ...
    def returns_rows(self, expression: Any) -> bool: ...
    def _connection(self, connection: "Optional[Any]") -> "Any": ...
    def _select_to_arrow_impl(self, stmt_obj: "SQL", connection: "Any", **kwargs: "Any") -> "ArrowResult": ...
    def _fetch_rows_for_arrow(self, stmt_obj: "SQL", connection: "Any", **kwargs: "Any") -> "list[RowT]": ...
    def _convert_rows_to_arrow(self, rows: "list[Any]", statement: "SQL", **kwargs: "Any") -> "ArrowTable": ...


class SyncArrowMixin(ArrowParquetFetchRowsMixin[ConnectionT], ABC, Generic[ConnectionT]):
    @abstractmethod
    def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "Any",
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Driver-specific implementation for Arrow export (sync).

        Args:
            stmt_obj: SQL statement object.
            connection: Database connection.
            **kwargs: Additional options.

        Returns:
            ArrowResult: The Arrow result object.
        """
        msg = "Arrow support's _select_to_arrow_impl not implemented by this driver"
        raise NotImplementedError(msg)

    def select_to_arrow(
        self: "ArrowSyncDriverProtocol",
        statement: "Any",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "Any",
        connection: "Optional[Any]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Export query results as an Arrow table (sync).

        Args:
            statement: SQL statement or builder.
            parameters: Query parameters.
            *filters: Statement filters.
            connection: Database connection.
            config: SQLConfig object.
            **kwargs: Additional options.

        Returns:
            ArrowResult: The Arrow result object.
        """
        tracer = getattr(self, "_tracer", None)
        error_counter = getattr(self, "_error_counter", None)
        latency_histogram = getattr(self, "_latency_histogram", None)
        span = tracer.start_as_current_span("select_to_arrow") if tracer else None
        if span:
            span.__enter__()
        start = datetime.datetime.now()
        try:
            stmt_obj = self._build_statement(
                statement, parameters=parameters, filters=list(filters), config=config or self.config
            )
            if not self.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)
            conn_to_use = self._connection(connection)
            try:
                result = self._select_to_arrow_impl(stmt_obj, conn_to_use, **kwargs)
            except NotImplementedError:
                rows = self._fetch_rows_for_arrow(stmt_obj, conn_to_use, **kwargs)
                arrow_table = self._convert_rows_to_arrow(rows, stmt_obj, **kwargs)
                result = ArrowResult(statement=stmt_obj, data=arrow_table)
            return result
        except Exception:
            if error_counter:
                error_counter.inc()
            raise
        finally:
            if latency_histogram:
                latency_histogram.observe((datetime.datetime.now() - start).total_seconds())
            if span:
                span.__exit__(None, None, None)


@runtime_checkable
class ArrowAsyncDriverProtocol(ArrowDriverProtocol, Protocol):
    def _build_statement(
        self,
        statement: "Any",
        parameters: "Optional[SQLParameterType]" = None,
        filters: "list[Any]" = [],
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL": ...
    @property
    def config(self) -> "SQLConfig": ...
    def returns_rows(self, expression: Any) -> bool: ...
    def _connection(self, connection: "Optional[Any]") -> "Any": ...
    async def _select_to_arrow_impl(self, stmt_obj: "SQL", connection: "Any", **kwargs: "Any") -> "ArrowResult": ...
    async def _fetch_rows_for_arrow_async(
        self, stmt_obj: "SQL", connection: "Any", **kwargs: "Any"
    ) -> "list[RowT]": ...
    def _convert_rows_to_arrow(self, rows: "list[Any]", statement: "SQL", **kwargs: "Any") -> "ArrowTable": ...


class AsyncArrowMixin(ArrowParquetFetchRowsMixin[ConnectionT], ABC, Generic[ConnectionT]):
    @abstractmethod
    async def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "Any",
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Driver-specific implementation for Arrow export (async).

        Args:
            stmt_obj: SQL statement object.
            connection: Database connection.
            **kwargs: Additional options.

        Returns:
            ArrowResult: The Arrow result object.
        """
        msg = "Arrow support's _select_to_arrow_impl not implemented by this driver"
        raise NotImplementedError(msg)

    async def select_to_arrow(
        self: "ArrowAsyncDriverProtocol",
        statement: "Any",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "Any",
        connection: "Optional[Any]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Export query results as an Arrow table (async).

        Args:
            statement: SQL statement or builder.
            parameters: Query parameters.
            *filters: Statement filters.
            connection: Database connection.
            config: SQLConfig object.
            **kwargs: Additional options.

        Returns:
            ArrowResult: The Arrow result object.
        """
        tracer = getattr(self, "_tracer", None)
        error_counter = getattr(self, "_error_counter", None)
        latency_histogram = getattr(self, "_latency_histogram", None)
        span = tracer.start_as_current_span("select_to_arrow") if tracer else None
        if span:
            span.__enter__()
        start = datetime.datetime.now()
        try:
            stmt_obj = self._build_statement(
                statement, parameters=parameters, filters=list(filters), config=config or self.config
            )
            if not self.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)
            conn_to_use = self._connection(connection)
            try:
                result = await self._select_to_arrow_impl(stmt_obj, conn_to_use, **kwargs)
            except NotImplementedError:
                rows = await self._fetch_rows_for_arrow_async(stmt_obj, conn_to_use, **kwargs)
                arrow_table = self._convert_rows_to_arrow(rows, stmt_obj, **kwargs)
                result = ArrowResult(statement=stmt_obj, data=arrow_table)

        except Exception:
            if error_counter:
                error_counter.inc()
            raise
        finally:
            if latency_histogram:
                latency_histogram.observe((datetime.datetime.now() - start).total_seconds())
            if span:
                span.__exit__(None, None, None)
        return result
