"""SQL result classes for query execution results.

This module provides result classes for handling SQL query execution results
including regular results and Apache Arrow format results.

Classes:
    StatementResult: Abstract base class for SQL results.
    SQLResult: Standard implementation for regular results.
    ArrowResult: Apache Arrow format results for data interchange.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, cast, overload

from mypy_extensions import mypyc_attr
from typing_extensions import TypeVar

from sqlspec.core.result._io import rows_to_pandas, rows_to_polars
from sqlspec.core.statement import SQL
from sqlspec.storage import (
    AsyncStoragePipeline,
    StorageDestination,
    StorageFormat,
    StorageTelemetry,
    SyncStoragePipeline,
)
from sqlspec.utils.arrow_helpers import (
    arrow_table_column_names,
    arrow_table_num_columns,
    arrow_table_num_rows,
    arrow_table_to_pandas,
    arrow_table_to_polars,
    arrow_table_to_pylist,
    arrow_table_to_return_format,
    cast_arrow_table_schema,
    convert_dict_to_arrow,
    ensure_arrow_table,
)
from sqlspec.utils.schema import to_schema

if TYPE_CHECKING:
    from sqlspec.core.compiler import OperationType
    from sqlspec.typing import ArrowReturnFormat, ArrowTable, PandasDataFrame, PolarsDataFrame, SchemaT


__all__ = ("ArrowResult", "EmptyResult", "SQLResult", "StackResult", "StatementResult")

T = TypeVar("T")
_EMPTY_RESULT_STATEMENT = SQL("-- empty stack result --")


@mypyc_attr(allow_interpreted_subclasses=False)
class StatementResult(ABC, Iterable[Any]):
    """Abstract base class for SQL statement execution results.

    Provides a common interface for handling different types of SQL operation
    results. Subclasses implement specific behavior for SELECT, INSERT, UPDATE,
    DELETE, and script operations.

    Attributes:
        statement: The original SQL statement that was executed.
        data: The result data from the operation.
        rows_affected: Number of rows affected by the operation.
        last_inserted_id: Last inserted ID from INSERT operations.
        execution_time: Time taken to execute the statement in seconds.
        metadata: Additional metadata about the operation.
    """

    __slots__ = ("data", "execution_time", "last_inserted_id", "metadata", "rows_affected", "statement")

    def __init__(
        self,
        statement: "SQL",
        data: Any = None,
        rows_affected: int = 0,
        last_inserted_id: int | str | None = None,
        execution_time: float | None = None,
        metadata: "dict[str, Any] | None" = None,
    ) -> None:
        """Initialize statement result.

        Args:
            statement: The original SQL statement that was executed.
            data: The result data from the operation.
            rows_affected: Number of rows affected by the operation.
            last_inserted_id: Last inserted ID from the operation.
            execution_time: Time taken to execute the statement in seconds.
            metadata: Additional metadata about the operation.
        """
        self.statement = statement
        self.data = data
        self.rows_affected = rows_affected
        self.last_inserted_id = last_inserted_id
        self.execution_time = execution_time
        self.metadata = metadata if metadata is not None else {}

    @abstractmethod
    def __iter__(self) -> "Iterator[Any]":
        """Iterate over result rows."""

    @abstractmethod
    def is_success(self) -> bool:
        """Check if the operation was successful.

        Returns:
            True if the operation completed successfully, False otherwise.
        """

    @abstractmethod
    def get_data(self) -> "Any":
        """Get the processed data from the result.

        Returns:
            The processed result data in an appropriate format.
        """

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value by key.

        Args:
            key: The metadata key to retrieve.
            default: Default value if key is not found.

        Returns:
            The metadata value or default.
        """
        return self.metadata.get(key, default)

    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata value by key.

        Args:
            key: The metadata key to set.
            value: The value to set.
        """
        self.metadata[key] = value

    @property
    def operation_type(self) -> "OperationType":
        """Get operation type from the statement.

        Returns:
            The type of SQL operation that produced this result.
        """
        return self.statement.operation_type


@mypyc_attr(allow_interpreted_subclasses=False)
class SQLResult(StatementResult):
    """Result class for SQL operations that return rows or affect rows.

    Handles SELECT, INSERT, UPDATE, DELETE operations. For DML operations with
    RETURNING clauses, the returned data is stored in the data attribute.
    The operation_type attribute indicates the nature of the operation.

    For script execution, tracks multiple statement results and errors.
    """

    __slots__ = (
        "_operation_type",
        "column_names",
        "error",
        "errors",
        "has_more",
        "inserted_ids",
        "operation_index",
        "parameters",
        "statement_results",
        "successful_statements",
        "total_count",
        "total_statements",
    )

    _operation_type: "OperationType"

    def __init__(
        self,
        statement: "SQL",
        data: "list[dict[str, Any]] | None" = None,
        rows_affected: int = 0,
        last_inserted_id: int | str | None = None,
        execution_time: float | None = None,
        metadata: "dict[str, Any] | None" = None,
        error: Exception | None = None,
        operation_type: "OperationType" = "SELECT",
        operation_index: int | None = None,
        parameters: Any | None = None,
        column_names: "list[str] | None" = None,
        total_count: int | None = None,
        has_more: bool = False,
        inserted_ids: "list[int | str] | None" = None,
        statement_results: "list[SQLResult] | None" = None,
        errors: "list[str] | None" = None,
        total_statements: int = 0,
        successful_statements: int = 0,
    ) -> None:
        """Initialize SQL result.

        Args:
            statement: The original SQL statement that was executed.
            data: The result data from the operation.
            rows_affected: Number of rows affected by the operation.
            last_inserted_id: Last inserted ID from the operation.
            execution_time: Time taken to execute the statement in seconds.
            metadata: Additional metadata about the operation.
            error: Exception that occurred during execution.
            operation_type: Type of SQL operation performed.
            operation_index: Index of operation in a script.
            parameters: Parameters used for the query.
            column_names: Names of columns in the result set.
            total_count: Total number of rows in the complete result set.
            has_more: Whether there are additional result pages available.
            inserted_ids: List of IDs from INSERT operations.
            statement_results: Results from individual statements in a script.
            errors: List of error messages for script execution.
            total_statements: Total number of statements in a script.
            successful_statements: Count of successful statements in a script.
        """
        super().__init__(
            statement=statement,
            data=data,
            rows_affected=rows_affected,
            last_inserted_id=last_inserted_id,
            execution_time=execution_time,
            metadata=metadata,
        )
        self.error = error
        self._operation_type = operation_type
        self.operation_index = operation_index
        self.parameters = parameters

        self.column_names = column_names or []
        self.total_count = total_count
        self.has_more = has_more
        self.inserted_ids = inserted_ids or []
        self.statement_results = statement_results or []
        self.errors = errors or []
        self.total_statements = total_statements
        self.successful_statements = successful_statements

        if not self.column_names and data and len(data) > 0:
            self.column_names = list(data[0].keys())
        if self.total_count is None:
            self.total_count = len(data) if data is not None else 0

    @property
    def operation_type(self) -> "OperationType":
        """Get operation type for this result.

        Returns:
            The type of SQL operation that produced this result.
        """
        return self._operation_type

    def _get_rows(self) -> "list[dict[str, Any]]":
        """Get validated row data as list of dicts.

        Returns:
            List of row dictionaries, empty list if no data.
        """
        if self.data is None:
            return []
        if not isinstance(self.data, list):
            return []
        return self.data

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value by key.

        Args:
            key: The metadata key to retrieve.
            default: Default value if key is not found.

        Returns:
            The metadata value or default.
        """
        return self.metadata.get(key, default)

    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata value by key.

        Args:
            key: The metadata key to set.
            value: The value to set.
        """
        self.metadata[key] = value

    def is_success(self) -> bool:
        """Check if the operation was successful.

        Returns:
            True if operation was successful, False otherwise.
        """
        op_type = self.operation_type.upper()

        if op_type == "SCRIPT" or self.statement_results:
            return not self.errors and self.total_statements == self.successful_statements

        if op_type == "SELECT":
            return self.data is not None and self.rows_affected >= 0

        if op_type in {"INSERT", "UPDATE", "DELETE", "EXECUTE"}:
            return self.rows_affected >= 0

        return False

    @overload
    def get_data(self, *, schema_type: "type[SchemaT]") -> "list[SchemaT]": ...

    @overload
    def get_data(self, *, schema_type: None = None) -> "list[dict[str, Any]]": ...

    def get_data(self, *, schema_type: "type[SchemaT] | None" = None) -> "list[SchemaT] | list[dict[str, Any]]":
        """Get the data from the result.

        For regular operations, returns the list of rows.
        For script operations, returns a summary dictionary containing
        execution statistics and results.

        Args:
            schema_type: Optional schema type to transform the data into.
                Supports Pydantic models, dataclasses, msgspec structs, attrs classes, and TypedDict.

        Returns:
            List of result rows (optionally transformed to schema_type) or script summary.
        """
        op_type_upper = self.operation_type.upper()
        if op_type_upper == "SCRIPT":
            failed_statements = self.total_statements - self.successful_statements
            return [
                {
                    "total_statements": self.total_statements,
                    "successful_statements": self.successful_statements,
                    "failed_statements": failed_statements,
                    "errors": self.errors,
                    "statement_results": self.statement_results,
                    "total_rows_affected": self.get_total_rows_affected(),
                }
            ]
        data = self._get_rows()
        if schema_type:
            return cast("list[SchemaT]", to_schema(data, schema_type=schema_type))
        return data

    def add_statement_result(self, result: "SQLResult") -> None:
        """Add a statement result to the script execution results.

        Args:
            result: Statement result to add.
        """
        self.statement_results.append(result)
        self.total_statements += 1
        if result.is_success():
            self.successful_statements += 1

    def get_total_rows_affected(self) -> int:
        """Get the total number of rows affected across all statements.

        Returns:
            Total rows affected.
        """
        if self.statement_results:
            total = 0
            for stmt in self.statement_results:
                if stmt.rows_affected and stmt.rows_affected > 0:
                    total += stmt.rows_affected
            return total
        return self.rows_affected if self.rows_affected and self.rows_affected > 0 else 0

    @property
    def num_rows(self) -> int:
        """Get the number of rows affected (alias for get_total_rows_affected).

        Returns:
            Total rows affected.
        """
        return self.get_total_rows_affected()

    @property
    def num_columns(self) -> int:
        """Get the number of columns in the result data.

        Returns:
            Number of columns.
        """
        return len(self.column_names) if self.column_names else 0

    @overload
    def get_first(self, *, schema_type: "type[SchemaT]") -> "SchemaT | None": ...

    @overload
    def get_first(self, *, schema_type: None = None) -> "dict[str, Any] | None": ...

    def get_first(self, *, schema_type: "type[SchemaT] | None" = None) -> "SchemaT | dict[str, Any] | None":
        """Get the first row from the result, if any.

        Args:
            schema_type: Optional schema type to transform the data into.
                Supports Pydantic models, dataclasses, msgspec structs, attrs classes, and TypedDict.

        Returns:
            First row (optionally transformed to schema_type) or None if no data.
        """
        rows = self._get_rows()
        if not rows:
            return None
        row = rows[0]
        if schema_type:
            return to_schema(row, schema_type=schema_type)
        return row

    def get_count(self) -> int:
        """Get the number of rows in the current result set (e.g., a page of data).

        Returns:
            Number of rows in current result set.
        """
        return len(self.data) if self.data is not None else 0

    def is_empty(self) -> bool:
        """Check if the result set (self.data) is empty.

        Returns:
            True if result set is empty.
        """
        return not self.data if self.data is not None else True

    def get_affected_count(self) -> int:
        """Get the number of rows affected by a DML operation.

        Returns:
            Number of affected rows.
        """
        return self.rows_affected or 0

    def was_inserted(self) -> bool:
        """Check if this was an INSERT operation.

        Returns:
            True if INSERT operation.
        """
        return self.operation_type.upper() == "INSERT"

    def was_updated(self) -> bool:
        """Check if this was an UPDATE operation.

        Returns:
            True if UPDATE operation.
        """
        return self.operation_type.upper() == "UPDATE"

    def was_deleted(self) -> bool:
        """Check if this was a DELETE operation.

        Returns:
            True if DELETE operation.
        """
        return self.operation_type.upper() == "DELETE"

    def __len__(self) -> int:
        """Get the number of rows in the result set.

        Returns:
            Number of rows in the data.
        """
        return len(self.data) if self.data is not None else 0

    def __getitem__(self, index: int) -> "dict[str, Any]":
        """Get a row by index.

        Args:
            index: Row index

        Returns:
            The row at the specified index
        """
        rows = self._get_rows()
        if not rows:
            msg = "No data available"
            raise IndexError(msg)
        return rows[index]

    def __iter__(self) -> "Iterator[dict[str, Any]]":
        """Iterate over the rows in the result.

        Returns:
            Iterator that yields each row as a dictionary
        """
        return iter(self._get_rows())

    @overload
    def all(self, *, schema_type: "type[SchemaT]") -> "list[SchemaT]": ...

    @overload
    def all(self, *, schema_type: None = None) -> "list[dict[str, Any]]": ...

    def all(self, *, schema_type: "type[SchemaT] | None" = None) -> "list[SchemaT] | list[dict[str, Any]]":
        """Return all rows as a list.

        Args:
            schema_type: Optional schema type to transform the data into.
                Supports Pydantic models, dataclasses, msgspec structs, attrs classes, and TypedDict.

        Returns:
            List of all rows (optionally transformed to schema_type)
        """
        data = self._get_rows()
        if schema_type:
            return cast("list[SchemaT]", to_schema(data, schema_type=schema_type))
        return data

    @overload
    def one(self, *, schema_type: "type[SchemaT]") -> "SchemaT": ...

    @overload
    def one(self, *, schema_type: None = None) -> "dict[str, Any]": ...

    def one(self, *, schema_type: "type[SchemaT] | None" = None) -> "SchemaT | dict[str, Any]":
        """Return exactly one row.

        Args:
            schema_type: Optional schema type to transform the data into.
                Supports Pydantic models, dataclasses, msgspec structs, attrs classes, and TypedDict.

        Returns:
            The single row (optionally transformed to schema_type)

        Raises:
            ValueError: If no results or more than one result
        """
        rows = self._get_rows()
        if not rows:
            msg = "No result found, exactly one row expected"
            raise ValueError(msg)

        data_len = len(rows)
        if data_len == 0:
            msg = "No result found, exactly one row expected"
            raise ValueError(msg)
        if data_len > 1:
            msg = f"Multiple results found ({data_len}), exactly one row expected"
            raise ValueError(msg)

        row = rows[0]
        if schema_type:
            return to_schema(row, schema_type=schema_type)
        return row

    @overload
    def one_or_none(self, *, schema_type: "type[SchemaT]") -> "SchemaT | None": ...

    @overload
    def one_or_none(self, *, schema_type: None = None) -> "dict[str, Any] | None": ...

    def one_or_none(self, *, schema_type: "type[SchemaT] | None" = None) -> "SchemaT | dict[str, Any] | None":
        """Return at most one row.

        Args:
            schema_type: Optional schema type to transform the data into.
                Supports Pydantic models, dataclasses, msgspec structs, attrs classes, and TypedDict.

        Returns:
            The single row (optionally transformed to schema_type) or None if no results

        Raises:
            ValueError: If more than one result
        """
        rows = self._get_rows()
        if not rows:
            return None

        data_len = len(rows)
        if data_len == 0:
            return None
        if data_len > 1:
            msg = f"Multiple results found ({data_len}), at most one row expected"
            raise ValueError(msg)

        row = rows[0]
        if schema_type:
            return to_schema(row, schema_type=schema_type)
        return row

    def scalar(self) -> Any:
        """Return the first column of the first row.

        Returns:
            The scalar value from first column of first row
        """
        row = self.one()
        return next(iter(row.values()))

    def scalar_or_none(self) -> Any:
        """Return the first column of the first row, or None if no results.

        Returns:
            The scalar value from first column of first row, or None
        """
        row = self.one_or_none()
        if row is None:
            return None

        return next(iter(row.values()))

    def to_arrow(self) -> "ArrowTable":
        """Convert result data to Apache Arrow Table.

        Returns:
            Arrow Table containing the result data.

        Raises:
            ValueError: If no data available.

        Examples:
            >>> result = session.select("SELECT * FROM users")
            >>> table = result.to_arrow()
            >>> print(table.num_rows)
            3
        """
        if self.data is None:
            msg = "No data available"
            raise ValueError(msg)

        return convert_dict_to_arrow(self.data, return_format="table")

    def to_pandas(self) -> "PandasDataFrame":
        """Convert result data to pandas DataFrame.

        Returns:
            pandas DataFrame containing the result data.

        Raises:
            ValueError: If no data available.

        Examples:
            >>> result = session.select("SELECT * FROM users")
            >>> df = result.to_pandas()
            >>> print(df.head())
        """
        if self.data is None:
            msg = "No data available"
            raise ValueError(msg)

        return rows_to_pandas(self.data)

    def to_polars(self) -> "PolarsDataFrame":
        """Convert result data to Polars DataFrame.

        Returns:
            Polars DataFrame containing the result data.

        Raises:
            ValueError: If no data available.

        Examples:
            >>> result = session.select("SELECT * FROM users")
            >>> df = result.to_polars()
            >>> print(df.head())
        """
        if self.data is None:
            msg = "No data available"
            raise ValueError(msg)

        return rows_to_polars(self.data)

    def write_to_storage_sync(
        self,
        destination: "StorageDestination",
        *,
        format_hint: "StorageFormat | None" = None,
        storage_options: "dict[str, Any] | None" = None,
        pipeline: "SyncStoragePipeline | None" = None,
    ) -> "StorageTelemetry":
        active_pipeline = pipeline or SyncStoragePipeline()
        rows = self.get_data()
        return active_pipeline.write_rows(rows, destination, format_hint=format_hint, storage_options=storage_options)

    async def write_to_storage_async(
        self,
        destination: "StorageDestination",
        *,
        format_hint: "StorageFormat | None" = None,
        storage_options: "dict[str, Any] | None" = None,
        pipeline: "AsyncStoragePipeline | None" = None,
    ) -> "StorageTelemetry":
        active_pipeline = pipeline or AsyncStoragePipeline()
        rows = self.get_data()
        return await active_pipeline.write_rows(
            rows, destination, format_hint=format_hint, storage_options=storage_options
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class ArrowResult(StatementResult):
    """Result class for SQL operations that return Apache Arrow data.

    Used when database drivers support returning results in Apache Arrow
    format for data interchange. Suitable for analytics workloads and
    data science applications.

    Attributes:
        schema: Arrow schema information for the result data.
    """

    __slots__ = ("schema",)

    def __init__(
        self,
        statement: "SQL",
        data: Any,
        rows_affected: int = 0,
        last_inserted_id: int | str | None = None,
        execution_time: float | None = None,
        metadata: "dict[str, Any] | None" = None,
        schema: "dict[str, Any] | None" = None,
    ) -> None:
        """Initialize Arrow result.

        Args:
            statement: The original SQL statement that was executed.
            data: The Apache Arrow Table containing the result data.
            rows_affected: Number of rows affected by the operation.
            last_inserted_id: Last inserted ID (if applicable).
            execution_time: Time taken to execute the statement in seconds.
            metadata: Additional metadata about the operation.
            schema: Optional Arrow schema information.
        """
        super().__init__(
            statement=statement,
            data=data,
            rows_affected=rows_affected,
            last_inserted_id=last_inserted_id,
            execution_time=execution_time,
            metadata=metadata,
        )

        self.schema = schema

    def is_success(self) -> bool:
        """Check if the operation was successful.

        Returns:
            True if Arrow table data is available, False otherwise.
        """
        return self.data is not None

    def get_data(self) -> "ArrowTable":
        """Get the Apache Arrow Table from the result.

        Returns:
            The Arrow table containing the result data.

        Raises:
            ValueError: If no Arrow table is available.
            TypeError: If data is not an Arrow Table.
        """
        if self.data is None:
            msg = "No Arrow table available for this result"
            raise ValueError(msg)
        return ensure_arrow_table(self.data)

    @property
    def column_names(self) -> "list[str]":
        """Get the column names from the Arrow table.

        Returns:
            List of column names.

        Raises:
            ValueError: If no Arrow table is available.
            TypeError: If data is not an Arrow Table.
        """
        return arrow_table_column_names(self.get_data())

    @property
    def num_rows(self) -> int:
        """Get the number of rows in the Arrow table.

        Returns:
            Number of rows.

        Raises:
            ValueError: If no Arrow table is available.
            TypeError: If data is not an Arrow Table.
        """
        return arrow_table_num_rows(self.get_data())

    @property
    def num_columns(self) -> int:
        """Get the number of columns in the Arrow table.

        Returns:
            Number of columns.

        Raises:
            ValueError: If no Arrow table is available.
            TypeError: If data is not an Arrow Table.
        """
        return arrow_table_num_columns(self.get_data())

    def to_pandas(self) -> "PandasDataFrame":
        """Convert Arrow data to pandas DataFrame.

        Returns:
            pandas DataFrame containing the result data.

        Raises:
            ValueError: If no Arrow table is available.

        Examples:
            >>> result = session.select_to_arrow("SELECT * FROM users")
            >>> df = result.to_pandas()
            >>> print(df.head())
        """
        return arrow_table_to_pandas(self.get_data())

    def to_polars(self) -> "PolarsDataFrame":
        """Convert Arrow data to Polars DataFrame.

        Returns:
            Polars DataFrame containing the result data.

        Raises:
            ValueError: If no Arrow table is available.

        Examples:
            >>> result = session.select_to_arrow("SELECT * FROM users")
            >>> df = result.to_polars()
            >>> print(df.head())
        """
        return arrow_table_to_polars(self.get_data())

    def to_dict(self) -> "list[dict[str, Any]]":
        """Convert Arrow data to list of dictionaries.

        Returns:
            List of dictionaries, one per row.

        Raises:
            ValueError: If no Arrow table is available.
            TypeError: If data is not an Arrow Table.

        Examples:
            >>> result = session.select_to_arrow(
            ...     "SELECT id, name FROM users"
            ... )
            >>> rows = result.to_dict()
            >>> print(rows[0])
            {'id': 1, 'name': 'Alice'}
        """
        return arrow_table_to_pylist(self.get_data())

    def write_to_storage_sync(
        self,
        destination: "StorageDestination",
        *,
        format_hint: "StorageFormat | None" = None,
        storage_options: "dict[str, Any] | None" = None,
        compression: str | None = None,
        pipeline: "SyncStoragePipeline | None" = None,
    ) -> "StorageTelemetry":
        table = self.get_data()
        active_pipeline = pipeline or SyncStoragePipeline()
        return active_pipeline.write_arrow(
            table, destination, format_hint=format_hint, storage_options=storage_options, compression=compression
        )

    async def write_to_storage_async(
        self,
        destination: "StorageDestination",
        *,
        format_hint: "StorageFormat | None" = None,
        storage_options: "dict[str, Any] | None" = None,
        compression: str | None = None,
        pipeline: "AsyncStoragePipeline | None" = None,
    ) -> "StorageTelemetry":
        table = self.get_data()
        active_pipeline = pipeline or AsyncStoragePipeline()
        return await active_pipeline.write_arrow(
            table, destination, format_hint=format_hint, storage_options=storage_options, compression=compression
        )

    def __len__(self) -> int:
        """Return number of rows in the Arrow table.

        Returns:
            Number of rows.

        Raises:
            ValueError: If no Arrow table is available.
            TypeError: If data is not an Arrow Table.

        Examples:
            >>> result = session.select_to_arrow("SELECT * FROM users")
            >>> print(len(result))
            100
        """
        return arrow_table_num_rows(self.get_data())

    def __iter__(self) -> "Iterator[dict[str, Any]]":
        """Iterate over rows as dictionaries.

        Yields:
            Dictionary for each row.

        Raises:
            ValueError: If no Arrow table is available.

        Examples:
            >>> result = session.select_to_arrow(
            ...     "SELECT id, name FROM users"
            ... )
            >>> for row in result:
            ...     print(row["name"])
        """
        yield from arrow_table_to_pylist(self.get_data())


class EmptyResult(StatementResult):
    """Sentinel result used when a stack operation has no driver result."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(statement=_EMPTY_RESULT_STATEMENT, data=[], rows_affected=0)

    def __iter__(self) -> "Iterator[Any]":
        return iter(())

    def is_success(self) -> bool:
        return True

    def get_data(self) -> "list[Any]":
        return []


class StackResult:
    """Wrapper for per-operation stack results that surfaces driver results directly."""

    __slots__ = ("error", "metadata", "result", "rows_affected", "warning")

    def __init__(
        self,
        result: "StatementResult | ArrowResult | None" = None,
        *,
        rows_affected: int | None = None,
        error: Exception | None = None,
        warning: Any | None = None,
        metadata: "dict[str, Any] | None" = None,
    ) -> None:
        self.result: StatementResult | ArrowResult = result if result is not None else EmptyResult()
        if rows_affected is not None:
            self.rows_affected = rows_affected
        else:
            try:
                result_rows = object.__getattribute__(self.result, "rows_affected")
            except AttributeError:
                self.rows_affected = 0
            else:
                self.rows_affected = int(result_rows)
        self.error = error
        self.warning = warning
        self.metadata = dict(metadata) if metadata else None

    def get_result(self) -> "StatementResult | ArrowResult":
        """Return the underlying driver result."""

        return self.result

    @property
    def result_type(self) -> str:
        """Describe the underlying result type (SQL operation, Arrow, or custom)."""

        if isinstance(self.result, ArrowResult):
            return "ARROW"
        if isinstance(self.result, SQLResult):
            return self.result.operation_type.upper()
        return type(self.result).__name__.upper()

    def is_sql_result(self) -> bool:
        """Return True when the underlying result is an SQLResult."""

        return isinstance(self.result, StatementResult) and not isinstance(self.result, ArrowResult)

    def is_arrow_result(self) -> bool:
        """Return True when the underlying result is an ArrowResult."""

        return isinstance(self.result, ArrowResult)

    def is_error(self) -> bool:
        """Return True when the stack operation captured an error."""

        return self.error is not None

    def with_error(self, error: Exception) -> "StackResult":
        """Return a copy of the result that records the provided error."""

        return StackResult(
            result=self.result,
            rows_affected=self.rows_affected,
            warning=self.warning,
            metadata=self.metadata,
            error=error,
        )

    @classmethod
    def from_sql_result(cls, result: "SQLResult") -> "StackResult":
        """Convert a standard SQLResult into a stack-friendly representation."""

        metadata = dict(result.metadata) if result.metadata else None
        warning = metadata.get("warning") if metadata else None
        return cls(result=result, rows_affected=result.rows_affected, warning=warning, metadata=metadata)

    @classmethod
    def from_arrow_result(cls, result: "ArrowResult") -> "StackResult":
        """Create a stack result from an ArrowResult instance."""

        metadata = dict(result.metadata) if result.metadata else None
        return cls(result=result, rows_affected=result.rows_affected, metadata=metadata)

    @classmethod
    def from_error(cls, error: Exception) -> "StackResult":
        """Create an error-only stack result."""

        return cls(result=EmptyResult(), rows_affected=0, error=error)


def create_sql_result(
    statement: "SQL",
    data: "list[dict[str, Any]] | None" = None,
    rows_affected: int = 0,
    last_inserted_id: int | str | None = None,
    execution_time: float | None = None,
    metadata: "dict[str, Any] | None" = None,
    **kwargs: Any,
) -> SQLResult:
    """Create SQLResult instance.

    Args:
        statement: The SQL statement that produced this result.
        data: Result data from query execution.
        rows_affected: Number of rows affected by the operation.
        last_inserted_id: Last inserted ID (for INSERT operations).
        execution_time: Execution time in seconds.
        metadata: Additional metadata about the result.
        **kwargs: Additional arguments for SQLResult initialization.

    Returns:
        SQLResult instance.
    """
    return SQLResult(
        statement=statement,
        data=data,
        rows_affected=rows_affected,
        last_inserted_id=last_inserted_id,
        execution_time=execution_time,
        metadata=metadata,
        **kwargs,
    )


def build_arrow_result_from_table(
    statement: "SQL",
    table: "ArrowTable",
    *,
    return_format: "ArrowReturnFormat" = "table",
    batch_size: int | None = None,
    arrow_schema: Any = None,
) -> ArrowResult:
    """Create ArrowResult from a pyarrow table with optional formatting.

    Args:
        statement: SQL statement that produced the table.
        table: Arrow table to wrap.
        return_format: Output format for the Arrow data.
        batch_size: Batch size hint for batch-based formats.
        arrow_schema: Optional pyarrow.Schema for casting.

    Returns:
        ArrowResult instance.
    """

    coerced_table = cast_arrow_table_schema(table, arrow_schema)
    arrow_data = arrow_table_to_return_format(coerced_table, return_format=return_format, batch_size=batch_size)
    rows_affected = arrow_table_num_rows(coerced_table)
    return create_arrow_result(statement=statement, data=arrow_data, rows_affected=rows_affected)


def create_arrow_result(
    statement: "SQL",
    data: Any,
    rows_affected: int = 0,
    last_inserted_id: int | str | None = None,
    execution_time: float | None = None,
    metadata: "dict[str, Any] | None" = None,
    schema: "dict[str, Any] | None" = None,
) -> ArrowResult:
    """Create ArrowResult instance.

    Args:
        statement: The SQL statement that produced this result.
        data: Arrow-based result data.
        rows_affected: Number of rows affected by the operation.
        last_inserted_id: Last inserted ID (for INSERT operations).
        execution_time: Execution time in seconds.
        metadata: Additional metadata about the result.
        schema: Optional Arrow schema information.

    Returns:
        ArrowResult instance.
    """
    return ArrowResult(
        statement=statement,
        data=data,
        rows_affected=rows_affected,
        last_inserted_id=last_inserted_id,
        execution_time=execution_time,
        metadata=metadata,
        schema=schema,
    )
