from typing import TYPE_CHECKING, cast

from sqlglot import exp
from typing_extensions import Self

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

from sqlspec.exceptions import SQLBuilderError

__all__ = ("LimitOffsetClauseMixin",)


class LimitOffsetClauseMixin:
    """Mixin providing LIMIT and OFFSET clauses for SELECT builders."""

    def limit(self, value: int) -> Self:
        """Add LIMIT clause.

        Args:
            value: The maximum number of rows to return.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "LIMIT is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        builder._expression = builder._expression.limit(exp.Literal.number(value), copy=False)
        return cast("Self", builder)

    def offset(self, value: int) -> Self:
        """Add OFFSET clause.

        Args:
            value: The number of rows to skip before starting to return rows.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "OFFSET is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        builder._expression = builder._expression.offset(exp.Literal.number(value), copy=False)
        return cast("Self", builder)
