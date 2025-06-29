"""Runtime-checkable protocols for SQLSpec to replace duck typing.

This module provides protocols that can be used for static type checking
and runtime isinstance() checks, replacing defensive hasattr() patterns.
"""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from sqlglot import exp

    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = (
    "HasLimitProtocol",
    "HasOffsetProtocol",
    "HasOrderByProtocol",
    "HasWhereProtocol",
    "IndexableRow",
    "IterableParameters",
    "ProcessorProtocol",
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
