"""ORDER BY, LIMIT, OFFSET, and RETURNING clause mixins.

Provides mixins for query result ordering, limiting, and result
returning functionality.
"""

from typing import TYPE_CHECKING, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import parse_order_expression
from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlspec.builder._column import Column
    from sqlspec.builder._expression_wrappers import ExpressionWrapper
    from sqlspec.builder.mixins._select_operations import Case
    from sqlspec.protocols import SQLBuilderProtocol

__all__ = ("LimitOffsetClauseMixin", "OrderByClauseMixin", "ReturningClauseMixin")


@trait
class OrderByClauseMixin:
    """Mixin providing ORDER BY clause."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def order_by(self, *items: Union[str, exp.Ordered, "Column"], desc: bool = False) -> Self:
        """Add ORDER BY clause.

        Args:
            *items: Columns to order by. Can be strings (column names) or sqlglot.exp.Ordered instances for specific directions (e.g., exp.column("name").desc()).
            desc: Whether to order in descending order (applies to all items if they are strings).

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the item type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SQLBuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "ORDER BY is only supported for SELECT statements."
            raise SQLBuilderError(msg)

        current_expr = builder._expression
        for item in items:
            if isinstance(item, str):
                order_item = parse_order_expression(item)
                if desc:
                    order_item = order_item.desc()
            else:
                # Extract expression from Column objects or use as-is for sqlglot expressions
                from sqlspec._sql import SQLFactory

                extracted_item = SQLFactory._extract_expression(item)
                order_item = extracted_item
                if desc and not isinstance(item, exp.Ordered):
                    order_item = order_item.desc()
            current_expr = current_expr.order_by(order_item, copy=False)
        builder._expression = current_expr
        return cast("Self", builder)


@trait
class LimitOffsetClauseMixin:
    """Mixin providing LIMIT and OFFSET clauses."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def limit(self, value: int) -> Self:
        """Add LIMIT clause.

        Args:
            value: The maximum number of rows to return.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SQLBuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "LIMIT is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        builder._expression = builder._expression.limit(exp.convert(value), copy=False)
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
        builder = cast("SQLBuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "OFFSET is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        builder._expression = builder._expression.offset(exp.convert(value), copy=False)
        return cast("Self", builder)


@trait
class ReturningClauseMixin:
    """Mixin providing RETURNING clause."""

    __slots__ = ()
    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def returning(self, *columns: Union[str, exp.Expression, "Column", "ExpressionWrapper", "Case"]) -> Self:
        """Add RETURNING clause to the statement.

        Args:
            *columns: Columns to return. Can be strings or sqlglot expressions.

        Raises:
            SQLBuilderError: If the current expression is not INSERT, UPDATE, or DELETE.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            msg = "Cannot add RETURNING: expression is not initialized."
            raise SQLBuilderError(msg)
        valid_types = (exp.Insert, exp.Update, exp.Delete)
        if not isinstance(self._expression, valid_types):
            msg = "RETURNING is only supported for INSERT, UPDATE, and DELETE statements."
            raise SQLBuilderError(msg)
        # Extract expressions from various wrapper types
        from sqlspec._sql import SQLFactory

        returning_exprs = [SQLFactory._extract_expression(c) for c in columns]
        self._expression.set("returning", exp.Returning(expressions=returning_exprs))
        return self
