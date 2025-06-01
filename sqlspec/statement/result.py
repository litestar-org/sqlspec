"""SQL statement result classes for handling different types of SQL operations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Optional, TypedDict, TypeVar, Union, cast

from sqlspec.typing import ArrowTable

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.statement.sql import Statement

__all__ = ("ArrowResult", "ExecuteResult", "ScriptResult", "SelectResult", "StatementResult")

T = TypeVar("T")


class ExecuteResultData(TypedDict):
    rows_affected: Optional[int]
    last_inserted_id: Optional[Union[int, str]]
    inserted_ids: list[Union[int, str]]
    returning_data: Optional["Sequence[dict[str, Any]]"]
    operation_type: str


class ScriptResultData(TypedDict):
    total_statements: int
    successful_statements: int
    failed_statements: int
    errors: list[str]
    statement_results: list[Any]
    total_rows_affected: int


@dataclass
class StatementResult(ABC, Generic[T]):
    """Base class for SQL statement execution results.

    This class provides a common interface for handling different types of
    SQL operation results. Subclasses implement specific behavior for
    SELECT, INSERT/UPDATE/DELETE, and script operations.

    Args:
        statement: The original SQL statement that was executed.
        data: The result data from the operation.
        rows_affected: Number of rows affected by the operation (if applicable).
        last_inserted_id: Last inserted ID (if applicable).
        execution_time: Time taken to execute the statement in seconds.
        metadata: Additional metadata about the operation.
    """

    statement: "Union[Statement, str]"
    """The original SQL statement that was executed."""
    data: Any
    """The result data from the operation."""
    rows_affected: Optional[int] = None
    """Number of rows affected by the operation."""
    last_inserted_id: Optional[Union[int, str]] = None
    """Last inserted ID from the operation."""
    execution_time: Optional[float] = None
    """Time taken to execute the statement in seconds."""
    metadata: "dict[str, Any]" = field(default_factory=dict)
    """Additional metadata about the operation."""

    @abstractmethod
    def is_success(self) -> bool:
        """Check if the operation was successful.

        Returns:
            True if the operation completed successfully, False otherwise.
        """

    @abstractmethod
    def get_data(self) -> "T":
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


@dataclass
class SelectResult(StatementResult[T]):
    """Result class for SELECT operations.

    This class handles the results of SELECT queries, providing methods
    to access rows, convert to different formats, and handle pagination.
    """

    column_names: "list[str]" = field(default_factory=list)
    """Names of the columns in the result set."""
    total_count: Optional[int] = None
    """Total count of rows (for pagination)."""
    has_more: bool = False
    """Whether there are more rows available."""

    def is_success(self) -> bool:
        """Check if the SELECT operation was successful.

        Returns:
            True if rows were fetched successfully.
        """
        return self.data is not None

    def get_data(self) -> "T":
        """Get the rows from the result.

        Returns:
            The rows in the result set.
        """
        return cast("T", self.data)

    def get_first(self) -> "Optional[T]":
        """Get the first row from the result.

        Returns:
            The first row or None if no rows.
        """
        return self.data[0] if self.data else None

    def get_count(self) -> int:
        """Get the number of rows returned.

        Returns:
            The number of rows in the result.
        """
        return len(self.data)

    def is_empty(self) -> bool:
        """Check if the result set is empty.

        Returns:
            True if no rows were returned.
        """
        return len(self.data) == 0


@dataclass
class ExecuteResult(StatementResult[ExecuteResultData]):
    """Result class for INSERT, UPDATE, DELETE operations.

    This class handles the results of data modification operations,
    providing access to affected row counts and inserted IDs.
    """

    operation_type: str = "EXECUTE"
    """The type of operation (INSERT, UPDATE, DELETE)."""
    inserted_ids: "list[Union[int, str]]" = field(default_factory=list, init=False)
    """List of inserted IDs (for batch operations)."""
    returning_data: "Optional[Sequence[dict[str, Any]]]" = field(init=False, default=None)
    """Data returned by RETURNING clauses."""

    def is_success(self) -> bool:
        """Check if the execute operation was successful.

        Returns:
            True if the operation completed without errors.
        """
        return self.rows_affected is not None and self.rows_affected >= 0

    def get_data(self) -> "ExecuteResultData":
        """Get the execution result data.

        Returns:
            Dictionary containing operation results.
        """
        return {
            "rows_affected": self.rows_affected,
            "last_inserted_id": self.last_inserted_id,
            "inserted_ids": self.inserted_ids,
            "returning_data": self.returning_data,
            "operation_type": self.operation_type,
        }

    def get_affected_count(self) -> int:
        """Get the number of affected rows.

        Returns:
            The number of rows affected by the operation.
        """
        return self.rows_affected or 0

    def get_inserted_id(self) -> "Optional[Union[int, str]]":
        """Get the last inserted ID.

        Returns:
            The last inserted ID or None.
        """
        return self.last_inserted_id

    def get_inserted_ids(self) -> "list[Union[int, str]]":
        """Get all inserted IDs (for batch operations).

        Returns:
            List of inserted IDs.
        """
        return self.inserted_ids

    def get_returning_data(self) -> "Optional[Sequence[dict[str, Any]]]":
        """Get data returned by RETURNING clauses.


        Returns:
            The returning data, optionally converted to the specified schema.
        """
        if self.returning_data is None:
            return None
        return self.returning_data

    def was_inserted(self) -> bool:
        """Check if this was an INSERT operation.

        Returns:
            True if this was an INSERT operation.
        """
        return self.operation_type.upper() == "INSERT"

    def was_updated(self) -> bool:
        """Check if this was an UPDATE operation.

        Returns:
            True if this was an UPDATE operation.
        """
        return self.operation_type.upper() == "UPDATE"

    def was_deleted(self) -> bool:
        """Check if this was a DELETE operation.

        Returns:
            True if this was a DELETE operation.
        """
        return self.operation_type.upper() == "DELETE"


@dataclass
class ScriptResult(StatementResult[ScriptResultData]):
    """Result class for script execution (multiple statements).

    This class handles the results of script execution containing
    multiple SQL statements, providing aggregated results.
    """

    statement_results: "list[StatementResult[Any]]" = field(default_factory=list)
    """Results from individual statements."""
    total_statements: int = field(default=0, init=False)
    """Total number of statements executed."""
    successful_statements: int = field(default=0, init=False)
    """Number of statements that executed successfully."""
    errors: "list[str]" = field(default_factory=list, init=False)
    """List of errors encountered during execution."""

    def is_success(self) -> bool:
        """Check if all statements in the script executed successfully.

        Returns:
            True if all statements executed successfully.
        """
        return len(self.errors) == 0 and self.successful_statements == self.total_statements

    def get_data(self) -> "ScriptResultData":
        """Get the script execution summary.

        Returns:
            Dictionary containing script execution results.
        """
        return {
            "total_statements": self.total_statements,
            "successful_statements": self.successful_statements,
            "failed_statements": self.total_statements - self.successful_statements,
            "errors": self.errors,
            "statement_results": [result.get_data() for result in self.statement_results],
            "total_rows_affected": sum(
                result.rows_affected or 0 for result in self.statement_results if result.rows_affected is not None
            ),
        }

    def get_statement_result(self, index: int) -> "Optional[StatementResult[Any]]":
        """Get the result of a specific statement by index.

        Args:
            index: The index of the statement result to retrieve.

        Returns:
            The statement result or None if index is out of range.
        """
        if 0 <= index < len(self.statement_results):
            return self.statement_results[index]
        return None

    def get_total_rows_affected(self) -> int:
        """Get the total number of rows affected across all statements.

        Returns:
            The total number of rows affected.
        """
        return sum(result.rows_affected or 0 for result in self.statement_results if result.rows_affected is not None)

    def get_errors(self) -> "list[str]":
        """Get all errors that occurred during script execution.

        Returns:
            List of error messages.
        """
        return self.errors

    def has_errors(self) -> bool:
        """Check if any errors occurred during script execution.

        Returns:
            True if there were errors.
        """
        return len(self.errors) > 0

    def add_statement_result(self, result: "StatementResult[Any]") -> None:
        """Add a statement result to the script result.

        Args:
            result: The statement result to add.
        """
        self.statement_results.append(result)
        self.total_statements += 1
        if result.is_success():
            self.successful_statements += 1

    def add_error(self, error: str) -> None:
        """Add an error to the script result.

        Args:
            error: The error message to add.
        """
        self.errors.append(error)


@dataclass
class ArrowResult(StatementResult[ArrowTable]):
    """Result class for SQL operations that return Apache Arrow data.

    This class is used when database drivers support returning results as
    Apache Arrow format for high-performance data interchange, especially
    useful for analytics workloads and data science applications.

    Args:
        statement: The original SQL statement that was executed.
        data: The Apache Arrow Table containing the result data.
        schema: Optional Arrow schema information.
    """

    schema: Optional["dict[str, Any]"] = None
    """Optional Arrow schema information."""

    def is_success(self) -> bool:
        """Check if the Arrow operation was successful.

        Returns:
            True if the operation completed successfully and has valid Arrow data.
        """
        return self.data is not None

    def get_data(self) -> "ArrowTable":  # pyright: ignore
        """Get the Apache Arrow Table from the result.

        Returns:
            The Arrow table containing the result data.

        Raises:
            ValueError: If no Arrow table is available.
        """
        if self.data is None:
            msg = "No Arrow table available for this result"
            raise ValueError(msg)
        return self.data  # type: ignore[no-any-return]

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

        return self.data.column_names

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

        return self.data.num_rows

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

        return self.data.num_columns
