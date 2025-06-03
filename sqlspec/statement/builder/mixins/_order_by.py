from typing import TYPE_CHECKING, Any, Union, cast

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("OrderByClauseMixin", )


class OrderByClauseMixin:
    """Mixin providing ORDER BY clause for SELECT builders."""

    def order_by(self, *items: Union[str, exp.Ordered]) -> Any:
        """Add ORDER BY clause.

        Args:
            *items: Columns to order by. Can be strings (column names) or sqlglot.exp.Ordered instances for specific directions (e.g., exp.column("name").desc()).

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the item type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
        if not isinstance(builder._expression, exp.Select):
            msg = "Order by can only be applied to a SELECT expression."
            raise SQLBuilderError(msg)

        current_expr = builder._expression
        for item in items:
            order_item = exp.column(item).asc() if isinstance(item, str) else item
            current_expr = current_expr.order_by(order_item, copy=False)
        builder._expression = current_expr
        return builder
