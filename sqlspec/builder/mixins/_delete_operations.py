"""Delete operation mixins for SQL builders."""

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

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def from_(self, table: str) -> Self:
        """Set the target table for the DELETE statement.

        Args:
            table: The table name to delete from.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Delete()
        if not isinstance(self._expression, exp.Delete):
            current_expr_type = type(self._expression).__name__
            msg = f"Base expression for Delete is {current_expr_type}, expected Delete."
            raise SQLBuilderError(msg)

        setattr(self, "_table", table)
        self._expression.set("this", exp.to_table(table))
        return self
