from collections.abc import Sequence
from typing import Any, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("InsertValuesMixin", )


class InsertValuesMixin:
    """Mixin providing VALUES and columns methods for INSERT builders."""

    def columns(self, *columns: Union[str, exp.Expression]) -> Self:
        """Set the columns for the INSERT statement.

        Args:
            *columns: Column names or expressions.

        Raises:
            SQLBuilderError: If the current expression is not an INSERT statement.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Insert()  # pyright: ignore  # type: ignore[attr-defined]
        if not isinstance(self._expression, exp.Insert):  # type: ignore[attr-defined]
            msg = "Cannot set columns on a non-INSERT expression."
            raise SQLBuilderError(msg)
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        self._expression.set("columns", column_exprs)  # pyright: ignore  # type: ignore[attr-defined]
        return self

    def values(self, *values: Any) -> Self:
        """Add a row of values to the INSERT statement.

        Args:
            *values: Values for the row.

        Raises:
            SQLBuilderError: If the current expression is not an INSERT statement.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Insert()  # pyright: ignore  # type: ignore[attr-defined]
        if not isinstance(self._expression, exp.Insert):  # type: ignore[attr-defined]
            msg = "Cannot add values to a non-INSERT expression."
            raise SQLBuilderError(msg)
        row_exprs = [exp.Literal.string(str(v)) if not isinstance(v, exp.Expression) else v for v in values]
        values_expr = exp.Values(expressions=[row_exprs])
        self._expression.set("expression", values_expr)  # pyright: ignore  # type: ignore[attr-defined]
        return self

    def add_values(self, values: Sequence[Any]) -> Self:
        """Add a row of values to the INSERT statement (alternative signature).

        Args:
            values: Sequence of values for the row.

        Returns:
            The current builder instance for method chaining.
        """
        return self.values(*values)
