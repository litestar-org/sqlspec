"""Runtime-checkable protocols for type safety and runtime checks.

This module provides protocols that can be used for static type checking
and runtime isinstance() checks.
"""

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from pathlib import Path

    from sqlglot import exp

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = (
    "BytesConvertibleProtocol",
    "DictProtocol",
    "ExpressionWithAliasProtocol",
    "FilterAppenderProtocol",
    "FilterParameterProtocol",
    "HasExpressionProtocol",
    "HasExpressionsProtocol",
    "HasLimitProtocol",
    "HasOffsetProtocol",
    "HasOrderByProtocol",
    "HasParameterBuilderProtocol",
    "HasSQLGlotExpressionProtocol",
    "HasSQLMethodProtocol",
    "HasToStatementProtocol",
    "HasWhereProtocol",
    "IndexableRow",
    "IterableParameters",
    "ObjectStoreItemProtocol",
    "ObjectStoreProtocol",
    "ParameterValueProtocol",
    "SQLBuilderProtocol",
    "SelectBuilderProtocol",
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
class DictProtocol(Protocol):
    """Protocol for objects with a __dict__ attribute."""

    __dict__: dict[str, Any]


@runtime_checkable
class BytesConvertibleProtocol(Protocol):
    """Protocol for objects that can be converted to bytes."""

    def __bytes__(self) -> bytes:
        """Convert object to bytes."""
        ...


@runtime_checkable
class ExpressionWithAliasProtocol(Protocol):
    """Protocol for SQL expressions that support aliasing with as_() method."""

    def as_(self, alias: str, **kwargs: Any) -> "exp.Alias":
        """Create an aliased expression."""
        ...


@runtime_checkable
class ObjectStoreItemProtocol(Protocol):
    """Protocol for object store items with path/key attributes."""

    path: str
    key: "str | None"


@runtime_checkable
class ObjectStoreProtocol(Protocol):
    """Protocol for object storage operations."""

    protocol: str
    backend_type: str

    def __init__(self, uri: str, **kwargs: Any) -> None:
        return

    def read_bytes(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Read bytes from an object."""
        return b""

    def write_bytes(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Write bytes to an object."""
        return

    def read_text(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text from an object."""
        return ""

    def write_text(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text to an object."""
        return

    def exists(self, path: "str | Path", **kwargs: Any) -> bool:
        """Check if an object exists."""
        return False

    def delete(self, path: "str | Path", **kwargs: Any) -> None:
        """Delete an object."""
        return

    def copy(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Copy an object."""
        return

    def move(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Move an object."""
        return

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects with optional prefix."""
        return []

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Find objects matching a glob pattern."""
        return []

    def is_object(self, path: "str | Path") -> bool:
        """Check if path points to an object."""
        return False

    def is_path(self, path: "str | Path") -> bool:
        """Check if path points to a prefix (directory-like)."""
        return False

    def get_metadata(self, path: "str | Path", **kwargs: Any) -> dict[str, Any]:
        """Get object metadata."""
        return {}

    def read_arrow(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        msg = "Arrow reading not implemented"
        raise NotImplementedError(msg)

    def write_arrow(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        msg = "Arrow writing not implemented"
        raise NotImplementedError(msg)

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches from matching objects."""
        msg = "Arrow streaming not implemented"
        raise NotImplementedError(msg)

    async def read_bytes_async(self, path: "str | Path", **kwargs: Any) -> bytes:
        """Async read bytes from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_bytes_async(self, path: "str | Path", data: bytes, **kwargs: Any) -> None:
        """Async write bytes to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_text_async(self, path: "str | Path", encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text from an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def write_text_async(self, path: "str | Path", data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text to an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def exists_async(self, path: "str | Path", **kwargs: Any) -> bool:
        """Async check if an object exists."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def delete_async(self, path: "str | Path", **kwargs: Any) -> None:
        """Async delete an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects with optional prefix."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def copy_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Async copy an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def move_async(self, source: "str | Path", destination: "str | Path", **kwargs: Any) -> None:
        """Async move an object."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def get_metadata_async(self, path: "str | Path", **kwargs: Any) -> dict[str, Any]:
        """Async get object metadata."""
        msg = "Async operations not implemented"
        raise NotImplementedError(msg)

    async def read_arrow_async(self, path: "str | Path", **kwargs: Any) -> "ArrowTable":
        """Async read an Arrow table from storage."""
        msg = "Async arrow reading not implemented"
        raise NotImplementedError(msg)

    async def write_arrow_async(self, path: "str | Path", table: "ArrowTable", **kwargs: Any) -> None:
        """Async write an Arrow table to storage."""
        msg = "Async arrow writing not implemented"
        raise NotImplementedError(msg)

    def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        """Async stream Arrow record batches from matching objects."""
        msg = "Async arrow streaming not implemented"
        raise NotImplementedError(msg)


@runtime_checkable
class HasSQLGlotExpressionProtocol(Protocol):
    """Protocol for objects with a sqlglot_expression property."""

    @property
    def sqlglot_expression(self) -> "exp.Expression | None":
        """Return the SQLGlot expression for this object."""
        ...


@runtime_checkable
class HasParameterBuilderProtocol(Protocol):
    """Protocol for objects that can add parameters."""

    def add_parameter(self, value: Any, name: "str | None" = None) -> tuple[Any, str]:
        """Add a parameter to the builder."""
        ...

    def get_expression(self) -> "exp.Expression | None":
        """Return the underlying SQLGlot expression."""
        ...

    def set_expression(self, expression: "exp.Expression") -> None:
        """Replace the underlying SQLGlot expression."""
        ...


@runtime_checkable
class HasExpressionProtocol(Protocol):
    """Protocol for objects with an _expression attribute."""

    _expression: "exp.Expression | None"


@runtime_checkable
class HasToStatementProtocol(Protocol):
    """Protocol for objects with a to_statement method."""

    def to_statement(self) -> Any:
        """Convert to SQL statement."""
        ...


@runtime_checkable
class SQLBuilderProtocol(Protocol):
    """Protocol for SQL query builders."""

    _expression: "exp.Expression | None"
    _parameters: dict[str, Any]
    _parameter_counter: int
    _columns: Any  # Optional attribute for some builders
    _table: Any  # Optional attribute for some builders
    _with_ctes: Any  # Optional attribute for some builders
    dialect: Any
    dialect_name: "str | None"

    @property
    def parameters(self) -> dict[str, Any]:
        """Public access to query parameters."""
        ...

    def add_parameter(self, value: Any, name: "str | None" = None) -> tuple[Any, str]:
        """Add a parameter to the builder."""
        ...

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate a unique parameter name."""
        ...

    def _parameterize_expression(self, expression: "exp.Expression") -> "exp.Expression":
        """Replace literal values in an expression with bound parameters."""
        ...

    def build(self) -> "exp.Expression | Any":
        """Build and return the final expression."""
        ...

    def _merge_sql_object_parameters(self, sql_obj: Any) -> None:
        """Merge parameters from SQL objects into the builder."""
        ...

    def _build_final_expression(self, *, copy: bool = False) -> "exp.Expression":
        """Return the expression with attached CTEs."""
        ...

    def _spawn_like_self(self) -> "Self":
        """Create a new builder with matching configuration."""
        ...

    def get_expression(self) -> "exp.Expression | None":
        """Return the underlying SQLGlot expression."""
        ...

    def set_expression(self, expression: "exp.Expression") -> None:
        """Replace the underlying SQLGlot expression."""
        ...

    def generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate a unique parameter name exposed via public API."""
        ...


class SelectBuilderProtocol(SQLBuilderProtocol, Protocol):
    """Protocol for SELECT query builders."""

    def select(self, *columns: "str | exp.Expression") -> Self:
        """Add SELECT columns to the query."""
        ...
