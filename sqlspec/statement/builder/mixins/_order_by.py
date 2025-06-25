from typing import TYPE_CHECKING, Union, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._parsing_utils import parse_order_expression

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("OrderByClauseMixin",)


class OrderByClauseMixin:
    """Mixin providing ORDER BY clause for SELECT builders."""

    def order_by(self, *items: Union[str, exp.Ordered], desc: bool = False) -> Self:
        """Add ORDER BY clause.

        Args:
            *items: Columns to order by. Can be strings (column names) or sqlglot.exp.Ordered instances for specific directions (e.g., exp.column("name").desc()).
            desc: Whether to order in descending order (applies to all items if they are strings).

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the item type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
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
                order_item = item
            current_expr = current_expr.order_by(order_item, copy=False)
        builder._expression = current_expr
        return cast("Self", builder)
