from typing import Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("HavingClauseMixin",)


class HavingClauseMixin:
    """Mixin providing HAVING clause for SELECT builders."""

    _expression: Optional[exp.Expression] = None

    def having(self, condition: Union[str, exp.Expression]) -> Self:
        """Add HAVING clause.

        Args:
            condition: The condition for the HAVING clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add HAVING to a non-SELECT expression."
            raise SQLBuilderError(msg)
        having_expr = exp.condition(condition) if isinstance(condition, str) else condition
        self._expression = self._expression.having(having_expr, copy=False)
        return self
