from typing import Optional

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("InsertIntoClauseMixin",)


class InsertIntoClauseMixin:
    """Mixin providing INTO clause for INSERT builders."""

    _expression: Optional[exp.Expression] = None

    def into(self, table: str) -> Self:
        """Set the target table for the INSERT statement.

        Args:
            table: The name of the table to insert data into.

        Raises:
            SQLBuilderError: If the current expression is not an INSERT statement.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "Cannot set target table on a non-INSERT expression."
            raise SQLBuilderError(msg)

        setattr(self, "_table", table)
        self._expression.set("this", exp.to_table(table))
        return self
