# pyright: reportPrivateUsage=false
"""DELETE operation mixins.

Provides mixins for DELETE statement functionality including
FROM clause specification.
"""

from typing import Optional

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("DeleteFromClauseMixin",)


@trait
class DeleteFromClauseMixin:
    """Mixin providing FROM clause for DELETE builders."""

    __slots__ = ()

    # Type annotations for PyRight - these will be provided by the base class
    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def from_(self, table: str) -> Self:
        """Set the target table for the DELETE statement.

        Args:
            table: The table name to delete from.

        Returns:
            The current builder instance for method chaining.
        """
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Delete())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Delete):
            current_expr_type = type(current_expr).__name__
            msg = f"Base expression for Delete is {current_expr_type}, expected Delete."
            raise SQLBuilderError(msg)

        setattr(self, "_table", table)
        current_expr.set("this", exp.to_table(table))
        return self
