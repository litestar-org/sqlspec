from collections.abc import Sequence
from typing import Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("InsertValuesMixin",)


class InsertValuesMixin:
    """Mixin providing VALUES and columns methods for INSERT builders."""

    _expression: Optional[exp.Expression] = None

    def columns(self, *columns: Union[str, exp.Expression]) -> Self:
        """Set the columns for the INSERT statement and synchronize the _columns attribute on the builder."""
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "Cannot set columns on a non-INSERT expression."
            raise SQLBuilderError(msg)
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        self._expression.set("columns", column_exprs)
        # Synchronize the _columns attribute on the builder (if present)
        if hasattr(self, "_columns"):
            # If no columns, clear the list
            if not columns:
                self._columns.clear()  # pyright: ignore
            else:
                self._columns[:] = [col.name if isinstance(col, exp.Column) else str(col) for col in columns]  # pyright: ignore
        return self

    def values(self, *values: Any) -> Self:
        """Add a row of values to the INSERT statement, validating against _columns if set."""
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "Cannot add values to a non-INSERT expression."
            raise SQLBuilderError(msg)
        # Validate value count if _columns is present and non-empty
        if (
            hasattr(self, "_columns") and getattr(self, "_columns", []) and len(values) != len(self._columns)  # pyright: ignore
        ):
            msg = f"Number of values ({len(values)}) does not match the number of specified columns ({len(self._columns)})."  # pyright: ignore
            raise SQLBuilderError(msg)
        row_exprs = []
        for v in values:
            if isinstance(v, exp.Expression):
                row_exprs.append(v)
            else:
                # Add as parameter
                _, param_name = self.add_parameter(v)  # type: ignore[attr-defined]
                row_exprs.append(exp.var(param_name))
        values_expr = exp.Values(expressions=[row_exprs])
        self._expression.set("expression", values_expr)
        return self

    def add_values(self, values: Sequence[Any]) -> Self:
        """Add a row of values to the INSERT statement (alternative signature).

        Args:
            values: Sequence of values for the row.

        Returns:
            The current builder instance for method chaining.
        """
        return self.values(*values)
