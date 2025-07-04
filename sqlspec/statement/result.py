"""SQL statement result classes for handling different types of SQL operations."""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional, Union

from typing_extensions import TypeVar

from sqlspec.typing import ArrowTable, RowT

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL

__all__ = ("ArrowResult", "SQLResult", "StatementResult")


T = TypeVar("T")

OperationType = Literal["SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE", "SCRIPT"]


@dataclass
class StatementResult(ABC, Generic[RowT]):
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

    statement: "SQL"
    """The original SQL statement that was executed."""
    data: "Any"
    """The result data from the operation."""
    rows_affected: int = 0
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


@dataclass
class SQLResult(StatementResult[RowT], Generic[RowT]):
    """Unified result class for SQL operations that return a list of rows
    or affect rows (e.g., SELECT, INSERT, UPDATE, DELETE).

    For DML operations with RETURNING clauses, the returned data will be in `self.data`.
    The `operation_type` attribute helps distinguish the nature of the operation.

    For script execution, this class also tracks multiple statement results and errors.
    """

    data: "list[RowT]" = field(default_factory=list)
    error: Optional[Exception] = None
    operation_type: OperationType = "SELECT"
    operation_index: Optional[int] = None
    pipeline_sql: Optional["SQL"] = None
    parameters: Optional[Any] = None
    column_names: "list[str]" = field(default_factory=list)
    total_count: Optional[int] = None
    has_more: bool = False
    inserted_ids: "list[Union[int, str]]" = field(default_factory=list)
    statement_results: "list[SQLResult[Any]]" = field(default_factory=list)
    """Individual statement results when executing scripts."""
    errors: "list[str]" = field(default_factory=list)
    """Errors encountered during script execution."""
    total_statements: int = 0
    """Total number of statements in the script."""
    successful_statements: int = 0
    """Number of statements that executed successfully."""

    def __post_init__(self) -> None:
        """Post-initialization to infer column names and total count if not provided."""
        if not self.column_names and self.data and isinstance(self.data[0], Mapping):
            self.column_names = list(self.data[0].keys())
        if self.total_count is None:
            self.total_count = len(self.data) if self.data is not None else 0

    def is_success(self) -> bool:
        """Check if the operation was successful.

        - For SELECT: True if data is not None and rows_affected is not negative.
        - For DML (INSERT, UPDATE, DELETE, EXECUTE): True if rows_affected is >= 0.
        - For SCRIPT: True if no errors and all statements succeeded.
        """
        op_type = self.operation_type.upper()

        if op_type == "SCRIPT" or self.statement_results:
            return not self.errors and self.total_statements == self.successful_statements

        if op_type == "SELECT":
            return self.data is not None and (self.rows_affected is None or self.rows_affected >= 0)

        if op_type in {"INSERT", "UPDATE", "DELETE", "EXECUTE"}:
            return self.rows_affected is not None and self.rows_affected >= 0

        return False

    def get_data(self) -> "Union[list[RowT], dict[str, Any]]":
        """Get the data from the result.

        For regular operations, returns the list of rows.
        For script operations, returns a summary dictionary.
        """
        if self.operation_type.upper() == "SCRIPT" or self.statement_results:
            return {
                "total_statements": self.total_statements,
                "successful_statements": self.successful_statements,
                "failed_statements": self.total_statements - self.successful_statements,
                "errors": self.errors,
                "statement_results": self.statement_results,
                "total_rows_affected": self.get_total_rows_affected(),
            }
        return self.data

    def add_statement_result(self, result: "SQLResult[Any]") -> None:
        """Add a statement result to the script execution results."""
        self.statement_results.append(result)
        self.total_statements += 1
        if result.is_success():
            self.successful_statements += 1

    def add_error(self, error: str) -> None:
        """Add an error message to the script execution errors."""
        self.errors.append(error)

    def get_statement_result(self, index: int) -> "Optional[SQLResult[Any]]":
        """Get a statement result by index."""
        if 0 <= index < len(self.statement_results):
            return self.statement_results[index]
        return None

    def get_total_rows_affected(self) -> int:
        """Get the total number of rows affected across all statements."""
        if self.statement_results:
            return sum(
                stmt.rows_affected for stmt in self.statement_results if stmt.rows_affected and stmt.rows_affected > 0
            )
        return self.rows_affected if self.rows_affected and self.rows_affected > 0 else 0

    @property
    def num_rows(self) -> int:
        return self.get_total_rows_affected()

    @property
    def num_columns(self) -> int:
        """Get the number of columns in the result data."""
        return len(self.column_names) if self.column_names else 0

    def get_errors(self) -> "list[str]":
        """Get all errors from script execution."""
        return self.errors.copy()

    def has_errors(self) -> bool:
        """Check if there are any errors from script execution."""
        return len(self.errors) > 0

    def get_first(self) -> "Optional[RowT]":
        """Get the first row from the result, if any."""
        return self.data[0] if self.data else None

    def get_count(self) -> int:
        """Get the number of rows in the current result set (e.g., a page of data)."""
        return len(self.data) if self.data is not None else 0

    def is_empty(self) -> bool:
        """Check if the result set (self.data) is empty."""
        return not self.data

    def get_affected_count(self) -> int:
        """Get the number of rows affected by a DML operation."""
        return self.rows_affected or 0

    def was_inserted(self) -> bool:
        """Check if this was an INSERT operation."""
        return self.operation_type.upper() == "INSERT"

    def was_updated(self) -> bool:
        """Check if this was an UPDATE operation."""
        return self.operation_type.upper() == "UPDATE"

    def was_deleted(self) -> bool:
        """Check if this was a DELETE operation."""
        return self.operation_type.upper() == "DELETE"

    def __len__(self) -> int:
        """Get the number of rows in the result set.

        Returns:
            Number of rows in the data.
        """
        return len(self.data) if self.data is not None else 0

    def __getitem__(self, index: int) -> "RowT":
        """Get a row by index.

        Args:
            index: Row index

        Returns:
            The row at the specified index

        Raises:
            TypeError: If data is None
        """
        if self.data is None:
            msg = "No data available"
            raise TypeError(msg)
        return self.data[index]

    def all(self) -> "list[RowT]":
        """Return all rows as a list.

        Returns:
            List of all rows in the result
        """
        if self.data is None:
            return []
        return self.data

    def one(self) -> "RowT":
        """Return exactly one row.

        Returns:
            The single row

        Raises:
            ValueError: If no results or more than one result
        """
        if self.data is None or len(self.data) == 0:
            msg = "No result found, exactly one row expected"
            raise ValueError(msg)
        if len(self.data) > 1:
            msg = f"Multiple results found ({len(self.data)}), exactly one row expected"
            raise ValueError(msg)
        return self.data[0]

    def one_or_none(self) -> "Optional[RowT]":
        """Return at most one row.

        Returns:
            The single row or None if no results

        Raises:
            ValueError: If more than one result
        """
        if self.data is None or len(self.data) == 0:
            return None
        if len(self.data) > 1:
            msg = f"Multiple results found ({len(self.data)}), at most one row expected"
            raise ValueError(msg)
        return self.data[0]

    def scalar(self) -> Any:
        """Return the first column of the first row.

        Returns:
            The scalar value from first column of first row

        Raises:
            ValueError: If no results
        """
        row = self.one()
        if isinstance(row, Mapping):
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            first_key = next(iter(row.keys()))
            return row[first_key]
        if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
            if len(row) == 0:
                msg = "Row has no columns"
                raise ValueError(msg)
            return row[0]
        return row

    def scalar_or_none(self) -> Any:
        """Return the first column of the first row, or None if no results.

        Returns:
            The scalar value from first column of first row, or None
        """
        row = self.one_or_none()
        if row is None:
            return None

        if isinstance(row, Mapping):
            if not row:
                return None
            first_key = next(iter(row.keys()))
            return row[first_key]
        if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
            if len(row) == 0:
                return None
            return row[0]
        return row


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
    data: "ArrowTable"
    """The result data from the operation."""

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

        return self.data.column_names

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

        return self.data.num_rows

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

        return self.data.num_columns
