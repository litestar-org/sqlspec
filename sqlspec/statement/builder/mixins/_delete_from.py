from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("DeleteFromClauseMixin", )


class DeleteFromClauseMixin:
    """Mixin providing FROM clause for DELETE builders."""

    def from_(self, table: str) -> Self:
        """Set the target table for the DELETE statement.

        Args:
            table: The table name to delete from.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Delete()  # type: ignore[attr-defined]
        if not isinstance(self._expression, exp.Delete):  # type: ignore[attr-defined]
            current_expr_type = type(self._expression).__name__  # type: ignore[attr-defined]
            msg = f"Base expression for DeleteBuilder is {current_expr_type}, expected Delete."
            raise SQLBuilderError(msg)

        setattr(self, "_table", table)
        self._expression.set("this", exp.to_table(table))  # type: ignore[attr-defined]
        return self
