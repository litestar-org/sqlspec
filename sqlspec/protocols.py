"""Runtime-checkable protocols for SQLSpec to replace duck typing.

This module provides protocols that can be used for static type checking
and runtime isinstance() checks, replacing defensive hasattr() patterns.
"""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Protocol, Union, runtime_checkable

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from pathlib import Path

    from sqlglot import exp

    from sqlspec.statement.pipelines.context import SQLProcessingContext
    from sqlspec.storage.capabilities import StorageCapabilities
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = (
    # Database Connection Protocols
    "AsyncCloseableConnectionProtocol",
    "AsyncCopyCapableConnectionProtocol",
    "AsyncPipelineCapableDriverProtocol",
    "AsyncTransactionCapableConnectionProtocol",
    "AsyncTransactionStateConnectionProtocol",
    "BytesConvertibleProtocol",
    "DictProtocol",
    "FilterAppenderProtocol",
    "FilterParameterProtocol",
    "HasExpressionProtocol",
    "HasExpressionsProtocol",
    "HasLimitProtocol",
    "HasOffsetProtocol",
    "HasOrderByProtocol",
    "HasParameterBuilderProtocol",
    "HasRiskLevelProtocol",
    "HasSQLGlotExpressionProtocol",
    "HasSQLMethodProtocol",
    "HasToStatementProtocol",
    "HasWhereProtocol",
    "IndexableRow",
    "IterableParameters",
    "ObjectStoreItemProtocol",
    "ObjectStoreProtocol",
    "ParameterValueProtocol",
    "ProcessorProtocol",
    "SQLBuilderProtocol",
    "SelectBuilderProtocol",
    "SyncCloseableConnectionProtocol",
    "SyncCopyCapableConnectionProtocol",
    "SyncPipelineCapableDriverProtocol",
    "SyncTransactionCapableConnectionProtocol",
    "SyncTransactionStateConnectionProtocol",
    "WithMethodProtocol",
)


@runtime_checkable
class IndexableRow(Protocol):
    """Protocol for row types that support index access."""

    def __getitem__(self, index: int) -> Any:
        """Get item by index."""
        ...

    def __len__(self) -> int:
        """Get length of the row."""
        ...


@runtime_checkable
class IterableParameters(Protocol):
    """Protocol for parameter sequences."""

    def __iter__(self) -> Any:
        """Iterate over parameters."""
        ...

    def __len__(self) -> int:
        """Get number of parameters."""
        ...


@runtime_checkable
class WithMethodProtocol(Protocol):
    """Protocol for objects with a with_ method (SQLGlot expressions)."""

    def with_(self, *args: Any, **kwargs: Any) -> Any:
        """Add WITH clause to expression."""
        ...


@runtime_checkable
class HasWhereProtocol(Protocol):
    """Protocol for SQL expressions that support WHERE clauses."""

    def where(self, *args: Any, **kwargs: Any) -> Any:
        """Add WHERE clause to expression."""
        ...


@runtime_checkable
class HasLimitProtocol(Protocol):
    """Protocol for SQL expressions that support LIMIT clauses."""

    def limit(self, *args: Any, **kwargs: Any) -> Any:
        """Add LIMIT clause to expression."""
        ...


@runtime_checkable
class HasOffsetProtocol(Protocol):
    """Protocol for SQL expressions that support OFFSET clauses."""

    def offset(self, *args: Any, **kwargs: Any) -> Any:
        """Add OFFSET clause to expression."""
        ...


@runtime_checkable
class HasOrderByProtocol(Protocol):
    """Protocol for SQL expressions that support ORDER BY clauses."""

    def order_by(self, *args: Any, **kwargs: Any) -> Any:
        """Add ORDER BY clause to expression."""
        ...


@runtime_checkable
class HasExpressionsProtocol(Protocol):
    """Protocol for SQL expressions that have an expressions attribute."""

    expressions: Any


@runtime_checkable
class HasSQLMethodProtocol(Protocol):
    """Protocol for objects that have a sql() method for rendering SQL."""

    def sql(self, *args: Any, **kwargs: Any) -> str:
        """Render object to SQL string."""
        ...


@runtime_checkable
class FilterParameterProtocol(Protocol):
    """Protocol for filter objects that can extract parameters."""

    def extract_parameters(self) -> tuple[list[Any], dict[str, Any]]:
        """Extract parameters from the filter."""
        ...


@runtime_checkable
class FilterAppenderProtocol(Protocol):
    """Protocol for filter objects that can append to SQL statements."""

    def append_to_statement(self, sql: Any) -> Any:
        """Append this filter to a SQL statement."""
        ...


@runtime_checkable
class ParameterValueProtocol(Protocol):
    """Protocol for parameter objects with value and type_hint attributes."""

    value: Any
    type_hint: str


@runtime_checkable
class HasRiskLevelProtocol(Protocol):
    """Protocol for objects with a risk_level attribute."""

    @property
    def risk_level(self) -> Any:
        """Get the risk level of this object."""
        ...


@runtime_checkable
class DictProtocol(Protocol):
    """Protocol for objects with a __dict__ attribute."""

    __dict__: dict[str, Any]


class ProcessorProtocol(Protocol):
    """Defines the interface for a single processing step in the SQL pipeline."""

    @abstractmethod
    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        """Processes an SQL expression.

        Args:
            expression: The SQL expression to process.
            context: The SQLProcessingContext holding the current state and config.

        Returns:
            The (possibly modified) SQL expression for transformers, or None for validators/analyzers.
        """
        ...


@runtime_checkable
class BytesConvertibleProtocol(Protocol):
    """Protocol for objects that can be converted to bytes."""

    def __bytes__(self) -> bytes:
        """Convert object to bytes."""
        ...


@runtime_checkable
class ObjectStoreItemProtocol(Protocol):
    """Protocol for object store items with path/key attributes."""

    path: str
    key: "Optional[str]"


@runtime_checkable
class SyncTransactionCapableConnectionProtocol(Protocol):
    """Protocol for sync connections that support transactions."""

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the current transaction."""
        ...


@runtime_checkable
class AsyncTransactionCapableConnectionProtocol(Protocol):
    """Protocol for async connections that support transactions."""

    async def commit(self) -> None:
        """Commit the current transaction."""
        ...

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        ...


@runtime_checkable
class SyncTransactionStateConnectionProtocol(SyncTransactionCapableConnectionProtocol, Protocol):
    """Protocol for sync connections that can report transaction state."""

    def in_transaction(self) -> bool:
        """Check if connection is currently in a transaction."""
        ...

    def begin(self) -> None:
        """Begin a new transaction."""
        ...


@runtime_checkable
class AsyncTransactionStateConnectionProtocol(AsyncTransactionCapableConnectionProtocol, Protocol):
    """Protocol for async connections that can report transaction state."""

    def in_transaction(self) -> bool:
        """Check if connection is currently in a transaction."""
        ...

    async def begin(self) -> None:
        """Begin a new transaction."""
        ...


@runtime_checkable
class SyncCloseableConnectionProtocol(Protocol):
    """Protocol for sync connections that can be closed."""

    def close(self) -> None:
        """Close the connection."""
        ...


@runtime_checkable
class AsyncCloseableConnectionProtocol(Protocol):
    """Protocol for async connections that can be closed."""

    async def close(self) -> None:
        """Close the connection."""
        ...


@runtime_checkable
class SyncCopyCapableConnectionProtocol(Protocol):
    """Protocol for sync connections that support COPY operations."""

    def copy_from(self, table: str, file: Any, **kwargs: Any) -> None:
        """Copy data from file to table."""
        ...

    def copy_to(self, table: str, file: Any, **kwargs: Any) -> None:
        """Copy data from table to file."""
        ...


@runtime_checkable
class AsyncCopyCapableConnectionProtocol(Protocol):
    """Protocol for async connections that support COPY operations."""

    async def copy_from(self, table: str, file: Any, **kwargs: Any) -> None:
        """Copy data from file to table."""
        ...

    async def copy_to(self, table: str, file: Any, **kwargs: Any) -> None:
        """Copy data from table to file."""
        ...


@runtime_checkable
class SyncPipelineCapableDriverProtocol(Protocol):
    """Protocol for sync drivers that support native pipeline execution."""

    def _execute_pipeline_native(self, operations: list[Any], **options: Any) -> list[Any]:
        """Execute pipeline operations natively."""
        ...


@runtime_checkable
class AsyncPipelineCapableDriverProtocol(Protocol):
    """Protocol for async drivers that support native pipeline execution."""

    async def _execute_pipeline_native(self, operations: list[Any], **options: Any) -> list[Any]:
        """Execute pipeline operations natively."""
        ...


@runtime_checkable
class ObjectStoreProtocol(Protocol):
    """Unified protocol for object storage operations.

    This protocol defines the interface for all storage backends with built-in
    instrumentation support. Backends must implement both sync and async operations
    where possible, with async operations suffixed with _async.

    All methods use 'path' terminology for consistency with object store patterns.
    """

    # Class-level capability descriptor
    capabilities: ClassVar["StorageCapabilities"]

    def __init__(self, uri: str, **kwargs: Any) -> None:
        return

    # Core Operations (sync)
    def read_bytes(self, path: "Union[str, Path]", **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        return b""

    def write_bytes(self, path: "Union[str, Path]", data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        return

    def read_text(self, path: "Union[str, Path]", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        return ""

    def write_text(self, path: "Union[str, Path]", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        return

    # Object Operations
    def exists(self, path: "Union[str, Path]", **kwargs: Any) -> bool:
        """Check if an object exists."""
        return False

    def delete(self, path: "Union[str, Path]", **kwargs: Any) -> None:
        """Delete an object."""
        return

    def copy(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Copy an object."""
        return

    def move(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Move an object."""
        return

    # Listing Operations
    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        return []

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        return []

    # Path Operations
    def is_object(self, path: "Union[str, Path]") -> bool:
        """Check if path points to an object."""
        return False

    def is_path(self, path: "Union[str, Path]") -> bool:
        """Check if path points to a prefix (directory-like)."""
        return False

    def get_metadata(self, path: "Union[str, Path]", **kwargs: Any) -> dict[str, Any]:
        """Get object metadata."""
        return {}

    # Arrow Operations
    def read_arrow(self, path: "Union[str, Path]", **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage.

        For obstore backend, this should use native arrow operations when available.
        """
        msg = "Arrow reading not implemented"
        raise NotImplementedError(msg)

    def write_arrow(self, path: "Union[str, Path]", table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage.

        For obstore backend, this should use native arrow operations when available.
        """
        msg = "Arrow writing not implemented"
        raise NotImplementedError(msg)

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches from matching objects.

        For obstore backend, this should use native streaming when available.
        """
        msg = "Arrow streaming not implemented"
        raise NotImplementedError(msg)

    # Async versions
    async def read_bytes_async(self, path: "Union[str, Path]", **kwargs: Any) -> bytes:
        """Async read bytes from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_bytes_async(self, path: "Union[str, Path]", data: bytes, **kwargs: Any) -> None:
        """Async write bytes to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_text_async(self, path: "Union[str, Path]", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_text_async(
        self, path: "Union[str, Path]", data: str, encoding: str = "utf-8", **kwargs: Any
    ) -> None:
        """Async write text to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def exists_async(self, path: "Union[str, Path]", **kwargs: Any) -> bool:
        """Async check if an object exists."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def delete_async(self, path: "Union[str, Path]", **kwargs: Any) -> None:
        """Async delete an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects with optional prefix."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def copy_async(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Async copy an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def move_async(self, source: "Union[str, Path]", destination: "Union[str, Path]", **kwargs: Any) -> None:
        """Async move an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def get_metadata_async(self, path: "Union[str, Path]", **kwargs: Any) -> dict[str, Any]:
        """Async get object metadata."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_arrow_async(self, path: "Union[str, Path]", **kwargs: Any) -> "ArrowTable":
        """Async read an Arrow table from storage."""
        msg = "Async arrow reading not implemented"
        raise NotImplementedError(msg)

    async def write_arrow_async(self, path: "Union[str, Path]", table: "ArrowTable", **kwargs: Any) -> None:
        """Async write an Arrow table to storage."""
        msg = "Async arrow writing not implemented"
        raise NotImplementedError(msg)

    async def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        """Async stream Arrow record batches from matching objects."""
        msg = "Async arrow streaming not implemented"
        raise NotImplementedError(msg)


@runtime_checkable
class HasSQLGlotExpressionProtocol(Protocol):
    """Protocol for objects with a sqlglot_expression property."""

    @property
    def sqlglot_expression(self) -> "Optional[exp.Expression]":
        """Return the SQLGlot expression for this object."""
        ...


@runtime_checkable
class HasParameterBuilderProtocol(Protocol):
    """Protocol for objects that can add parameters."""

    def add_parameter(self, value: Any, name: "Optional[str]" = None) -> tuple[Any, str]:
        """Add a parameter to the builder."""
        ...


@runtime_checkable
class HasExpressionProtocol(Protocol):
    """Protocol for objects with an _expression attribute."""

    _expression: "Optional[exp.Expression]"


@runtime_checkable
class HasToStatementProtocol(Protocol):
    """Protocol for objects with a to_statement method."""

    def to_statement(self) -> Any:
        """Convert to SQL statement."""
        ...


@runtime_checkable
class SQLBuilderProtocol(Protocol):
    """Protocol for SQL query builders."""

    _expression: "Optional[exp.Expression]"
    _parameters: dict[str, Any]
    _parameter_counter: int
    dialect: Any
    dialect_name: "Optional[str]"

    @property
    def parameters(self) -> dict[str, Any]:
        """Public access to query parameters."""
        ...

    def add_parameter(self, value: Any, name: "Optional[str]" = None) -> tuple[Any, str]:
        """Add a parameter to the builder."""
        ...

    def _parameterize_expression(self, expression: "exp.Expression") -> "exp.Expression":
        """Replace literal values in an expression with bound parameters."""
        ...

    def build(self) -> "Union[exp.Expression, Any]":
        """Build and return the final expression."""
        ...


class SelectBuilderProtocol(SQLBuilderProtocol, Protocol):
    """Protocol for SELECT query builders."""

    def select(self, *columns: "Union[str, exp.Expression]") -> Self:
        """Add SELECT columns to the query."""
        ...
