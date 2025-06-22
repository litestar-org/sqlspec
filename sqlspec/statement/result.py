"""SQL statement result classes for handling different types of SQL operations."""

from abc import ABC, abstractmethod

# Import Mapping for type checking in __post_init__
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Optional, Union, cast

from typing_extensions import TypedDict, TypeVar

from sqlspec.typing import ArrowTable, RowT

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQL

__all__ = ("ArrowResult", "DMLResultDict", "SQLResult", "ScriptResultDict", "SelectResultDict", "StatementResult")


T = TypeVar("T")


class SelectResultDict(TypedDict):
    """TypedDict for SELECT/RETURNING query results.

    This structure is returned by drivers when executing SELECT queries
    or DML queries with RETURNING clauses.
    """

    data: "list[Any]"
    """List of rows returned by the query."""
    column_names: "list[str]"
    """List of column names in the result set."""
    rows_affected: int
    """Number of rows affected (-1 when unsupported)."""


class DMLResultDict(TypedDict, total=False):
    """TypedDict for DML (INSERT/UPDATE/DELETE) results without RETURNING.

    This structure is returned by drivers when executing DML operations
    that don't return data (no RETURNING clause).
    """

    rows_affected: int
    """Number of rows affected by the operation."""
    status_message: str
    """Status message from the database (-1 when unsupported)."""
    description: str
    """Optional description of the operation."""


class ScriptResultDict(TypedDict, total=False):
    """TypedDict for script execution results.

    This structure is returned by drivers when executing multi-statement
    SQL scripts.
    """

    statements_executed: int
    """Number of statements that were executed."""
    status_message: str
    """Overall status message from the script execution."""
    description: str
    """Optional description of the script execution."""


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


# RowT is introduced for clarity within SQLResult, representing the type of a single row.


@dataclass
class SQLResult(StatementResult[RowT], Generic[RowT]):
    """Unified result class for SQL operations that return a list of rows
    or affect rows (e.g., SELECT, INSERT, UPDATE, DELETE).

    For DML operations with RETURNING clauses, the returned data will be in `self.data`.
    The `operation_type` attribute helps distinguish the nature of the operation.

    For script execution, this class also tracks multiple statement results and errors.
    """

    error: Optional[Exception] = None
    operation_index: Optional[int] = None
    pipeline_sql: Optional["SQL"] = None
    parameters: Optional[Any] = None

    # Attributes primarily for SELECT-like results or results with column structure
    column_names: "list[str]" = field(default_factory=list)
    total_count: Optional[int] = None  # Total rows if pagination/limit was involved
    has_more: bool = False  # For pagination

    # Attributes primarily for DML-like results
    operation_type: str = "SELECT"  # Default, override for DML
    inserted_ids: "list[Union[int, str]]" = field(default_factory=list)
    # rows_affected and last_inserted_id are inherited from StatementResult

    # Attributes for script execution
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

        # If data is populated for a DML, it implies returning data.
        # No separate returning_data field needed; self.data serves this purpose.

    def is_success(self) -> bool:
        """Check if the operation was successful.
        - For SELECT: True if data is not None and rows_affected is not negative.
        - For DML (INSERT, UPDATE, DELETE, EXECUTE): True if rows_affected is >= 0.
        - For SCRIPT: True if no errors and all statements succeeded.
        """
        op_type_upper = self.operation_type.upper()

        # For script execution, check if there are no errors and all statements succeeded
        if op_type_upper == "SCRIPT" or self.statement_results:
            return len(self.errors) == 0 and self.total_statements == self.successful_statements

        if op_type_upper == "SELECT":
            # For SELECT, success means we got some data container and rows_affected is not negative
            data_success = self.data is not None
            rows_success = self.rows_affected is None or self.rows_affected >= 0
            return data_success and rows_success
        if op_type_upper in {"INSERT", "UPDATE", "DELETE", "EXECUTE"}:
            return self.rows_affected is not None and self.rows_affected >= 0
        return False  # Should not happen if operation_type is one of the above

    def get_data(self) -> "Union[list[RowT], dict[str, Any]]":
        """Get the data from the result.
        For regular operations, returns the list of rows.
        For script operations, returns a summary dictionary.
        """
        # For script execution, return summary data
        if self.operation_type.upper() == "SCRIPT" or self.statement_results:
            return {
                "total_statements": self.total_statements,
                "successful_statements": self.successful_statements,
                "failed_statements": self.total_statements - self.successful_statements,
                "errors": self.errors,
                "statement_results": self.statement_results,
                "total_rows_affected": self.get_total_rows_affected(),
            }

        # For regular operations, return the data as usual
        return cast("list[RowT]", self.data)

    # --- Script execution methods ---

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
            # For script execution, sum up rows affected from all statements
            total = 0
            for stmt_result in self.statement_results:
                if stmt_result.rows_affected is not None and stmt_result.rows_affected >= 0:
                    # Only count non-negative values, -1 indicates failure
                    total += stmt_result.rows_affected
            return total
        # For single statement execution
        return max(self.rows_affected or 0, 0)  # Treat negative values as 0

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

    # --- Existing methods for regular operations ---

    def get_first(self) -> "Optional[RowT]":
        """Get the first row from the result, if any."""
        return self.data[0] if self.data else None

    def get_count(self) -> int:
        """Get the number of rows in the current result set (e.g., a page of data)."""
        return len(self.data) if self.data is not None else 0

    def is_empty(self) -> bool:
        """Check if the result set (self.data) is empty."""
        return not self.data

    # --- Methods related to DML operations ---
    def get_affected_count(self) -> int:
        """Get the number of rows affected by a DML operation."""
        return self.rows_affected or 0

    def get_inserted_id(self) -> "Optional[Union[int, str]]":
        """Get the last inserted ID (typically for single row inserts)."""
        return self.last_inserted_id

    def get_inserted_ids(self) -> "list[Union[int, str]]":
        """Get all inserted IDs (useful for batch inserts)."""
        return self.inserted_ids

    def get_returning_data(self) -> "list[RowT]":
        """Get data returned by RETURNING clauses.
        This is effectively self.data for this unified class.
        """
        return cast("list[RowT]", self.data)

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
        return cast("RowT", self.data[index])

    # --- SQLAlchemy-style convenience methods ---

    def all(self) -> "list[RowT]":
        """Return all rows as a list.

        Returns:
            List of all rows in the result
        """
        if self.data is None:
            return []
        return cast("list[RowT]", self.data)

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
        return cast("RowT", self.data[0])

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
        return cast("RowT", self.data[0])

    def scalar(self) -> Any:
        """Return the first column of the first row.

        Returns:
            The scalar value from first column of first row

        Raises:
            ValueError: If no results
        """
        row = self.one()
        if isinstance(row, Mapping):
            # For dict-like rows, get the first column value
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            first_key = cast("str", next(iter(row.keys())))
            return cast("Any", row[first_key])
        if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
            # For tuple/list-like rows
            if len(row) == 0:
                msg = "Row has no columns"
                raise ValueError(msg)
            return cast("Any", row[0])
        # For scalar values returned directly
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
            # For tuple/list-like rows
            if len(row) == 0:
                return None
            return cast("Any", row[0])
        # For scalar values returned directly
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
        """Check if the Arrow operation was successful.

        Returns:
            True if the operation completed successfully and has valid Arrow data.
        """
        return bool(self.data)

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
