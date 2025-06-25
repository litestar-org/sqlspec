from typing import Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("ReturningClauseMixin",)


class ReturningClauseMixin:
    """Mixin providing RETURNING clause for INSERT, UPDATE, and DELETE builders."""

    _expression: Optional[exp.Expression] = None

    def returning(self, *columns: Union[str, exp.Expression]) -> Self:
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
        returning_exprs = [exp.column(c) if isinstance(c, str) else c for c in columns]
        self._expression.set("returning", exp.Returning(expressions=returning_exprs))
        return self
