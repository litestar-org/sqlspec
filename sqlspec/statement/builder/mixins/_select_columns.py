from typing import TYPE_CHECKING, Union, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._parsing_utils import parse_column_expression

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("SelectColumnsMixin",)


class SelectColumnsMixin:
    """Mixin providing SELECT column and DISTINCT clauses for SELECT builders."""

    def select(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add columns to SELECT clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add select columns to a non-SELECT expression."
            raise SQLBuilderError(msg)
        for column in columns:
            builder._expression = builder._expression.select(parse_column_expression(column), copy=False)
        return cast("Self", builder)

    def distinct(self, *columns: Union[str, exp.Expression]) -> Self:
        """Add DISTINCT clause to SELECT.

        Args:
            *columns: Optional columns to make distinct. If none provided, applies DISTINCT to all selected columns.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add DISTINCT to a non-SELECT expression."
            raise SQLBuilderError(msg)
        if not columns:
            builder._expression.set("distinct", exp.Distinct())
        else:
            distinct_columns = [parse_column_expression(column) for column in columns]
            builder._expression.set("distinct", exp.Distinct(expressions=distinct_columns))
        return cast("Self", builder)
