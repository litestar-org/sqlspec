"""SQL result classes for query execution results.

This module provides result classes for handling SQL query execution results
including regular results and Apache Arrow format results.

Classes:
    StatementResult: Abstract base class for SQL results.
    SQLResult: Standard implementation for regular results.
    ArrowResult: Apache Arrow format results for data interchange.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from mypy_extensions import mypyc_attr
from typing_extensions import TypeVar

from sqlspec.core.compiler import OperationType

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlspec.core.statement import SQL


__all__ = ("ArrowResult", "SQLResult", "StatementResult")

T = TypeVar("T")


@mypyc_attr(allow_interpreted_subclasses=False)
class StatementResult(ABC):
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
        last_inserted_id: Optional[Union[int, str]] = None,
        execution_time: Optional[float] = None,
        metadata: Optional["dict[str, Any]"] = None,
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
    def operation_type(self) -> OperationType:
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

    Attributes:
        column_names: Names of columns in the result set.
        error: Exception that occurred during execution.
        errors: List of error messages for script execution.
        has_more: Whether there are additional result pages available.
        inserted_ids: List of IDs from INSERT operations.
        operation_index: Index of operation in a script.
        parameters: Parameters used for the query.
        statement_results: Results from individual statements in a script.
        successful_statements: Count of successful statements in a script.
        total_count: Total number of rows in the complete result set.
        total_statements: Total number of statements in a script.
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

    _operation_type: OperationType

    def __init__(
        self,
        statement: "SQL",
        data: Optional[list[dict[str, Any]]] = None,
        rows_affected: int = 0,
        last_inserted_id: Optional[Union[int, str]] = None,
        execution_time: Optional[float] = None,
        metadata: Optional["dict[str, Any]"] = None,
        error: Optional[Exception] = None,
        operation_type: OperationType = "SELECT",
        operation_index: Optional[int] = None,
        parameters: Optional[Any] = None,
        column_names: Optional["list[str]"] = None,
        total_count: Optional[int] = None,
        has_more: bool = False,
        inserted_ids: Optional["list[Union[int, str]]"] = None,
        statement_results: Optional["list[SQLResult]"] = None,
        errors: Optional["list[str]"] = None,
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
    def operation_type(self) -> OperationType:
        """Get operation type for this result.

        Returns:
            The type of SQL operation that produced this result.
        """
        return self._operation_type

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

    def get_data(self) -> "list[dict[str,Any]]":
        """Get the data from the result.

        For regular operations, returns the list of rows.
        For script operations, returns a summary dictionary containing
        execution statistics and results.

        Returns:
            List of result rows or script summary.
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
        return self.data or []

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

    def get_first(self) -> "Optional[dict[str, Any]]":
        """Get the first row from the result, if any.

        Returns:
            First row or None if no data.
        """
        return self.data[0] if self.data else None

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
        if self.data is None:
            msg = "No data available"
            raise IndexError(msg)
        return cast("dict[str, Any]", self.data[index])

    def __iter__(self) -> "Iterator[dict[str, Any]]":
        """Iterate over the rows in the result.

        Returns:
            Iterator that yields each row as a dictionary
        """
        return iter(self.data or [])

    def all(self) -> list[dict[str, Any]]:
        """Return all rows as a list.

        Returns:
            List of all rows in the result
        """
        return self.data or []

    def one(self) -> "dict[str, Any]":
        """Return exactly one row.

        Returns:
            The single row

        Raises:
            ValueError: If no results or more than one result
        """
        if not self.data:
            msg = "No result found, exactly one row expected"
            raise ValueError(msg)

        data_len = len(self.data)
        if data_len == 0:
            msg = "No result found, exactly one row expected"
            raise ValueError(msg)
        if data_len > 1:
            msg = f"Multiple results found ({data_len}), exactly one row expected"
            raise ValueError(msg)

        return cast("dict[str, Any]", self.data[0])

    def one_or_none(self) -> "Optional[dict[str, Any]]":
        """Return at most one row.

        Returns:
            The single row or None if no results

        Raises:
            ValueError: If more than one result
        """
        if not self.data:
            return None

        data_len = len(self.data)
        if data_len == 0:
            return None
        if data_len > 1:
            msg = f"Multiple results found ({data_len}), at most one row expected"
            raise ValueError(msg)

        return cast("dict[str, Any]", self.data[0])

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
        last_inserted_id: Optional[Union[int, str]] = None,
        execution_time: Optional[float] = None,
        metadata: Optional["dict[str, Any]"] = None,
        schema: Optional["dict[str, Any]"] = None,
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

    def get_data(self) -> Any:
        """Get the Apache Arrow Table from the result.

        Returns:
            The Arrow table containing the result data.

        Raises:
            ValueError: If no Arrow table is available.
        """
        if self.data is None:
            msg = "No Arrow table available for this result"
            raise ValueError(msg)
        return self.data

    @property
    def column_names(self) -> "list[str]":
        """Get the column names from the Arrow table.

        Returns:
            List of column names.

        Raises:
            ValueError: If no Arrow table is available.
        """
        if self.data is None:
            msg = "No Arrow table available"
            raise ValueError(msg)

        return cast("list[str]", self.data.column_names)

    @property
    def num_rows(self) -> int:
        """Get the number of rows in the Arrow table.

        Returns:
            Number of rows.

        Raises:
            ValueError: If no Arrow table is available.
        """
        if self.data is None:
            msg = "No Arrow table available"
            raise ValueError(msg)

        return cast("int", self.data.num_rows)

    @property
    def num_columns(self) -> int:
        """Get the number of columns in the Arrow table.

        Returns:
            Number of columns.

        Raises:
            ValueError: If no Arrow table is available.
        """
        if self.data is None:
            msg = "No Arrow table available"
            raise ValueError(msg)

        return cast("int", self.data.num_columns)


def create_sql_result(
    statement: "SQL",
    data: Optional[list[dict[str, Any]]] = None,
    rows_affected: int = 0,
    last_inserted_id: Optional[Union[int, str]] = None,
    execution_time: Optional[float] = None,
    metadata: Optional["dict[str, Any]"] = None,
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


def create_arrow_result(
    statement: "SQL",
    data: Any,
    rows_affected: int = 0,
    last_inserted_id: Optional[Union[int, str]] = None,
    execution_time: Optional[float] = None,
    metadata: Optional["dict[str, Any]"] = None,
    schema: Optional["dict[str, Any]"] = None,
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
