from collections.abc import Mapping
from typing import Any, Optional

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("UpdateSetClauseMixin",)

MIN_SET_ARGS = 2


class UpdateSetClauseMixin:
    """Mixin providing SET clause for UPDATE builders."""

    _expression: Optional[exp.Expression] = None

    def set(self, *args: Any, **kwargs: Any) -> Self:
        """Set columns and values for the UPDATE statement.

        Supports:
        - set(column, value)
        - set(mapping)
        - set(**kwargs)
        - set(mapping, **kwargs)

        Args:
            *args: Either (column, value) or a mapping.
            **kwargs: Column-value pairs to set.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement or usage is invalid.

        Returns:
            The current builder instance for method chaining.
        """

        if self._expression is None:
            self._expression = exp.Update()
        if not isinstance(self._expression, exp.Update):
            msg = "Cannot add SET clause to non-UPDATE expression."
            raise SQLBuilderError(msg)
        assignments = []
        # (column, value) signature
        if len(args) == MIN_SET_ARGS and not kwargs:
            col, val = args
            param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
            col_expr = col if isinstance(col, exp.Column) else exp.column(col)
            assignments.append(exp.EQ(this=col_expr, expression=exp.Placeholder(this=param_name)))
        # mapping and/or kwargs
        elif (len(args) == 1 and isinstance(args[0], Mapping)) or kwargs:
            all_values = dict(args[0] if args else {}, **kwargs)
            for col, val in all_values.items():
                param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
                assignments.append(exp.EQ(this=exp.column(col), expression=exp.Placeholder(this=param_name)))
        else:
            msg = "Invalid arguments for set(): use (column, value), mapping, or kwargs."
            raise SQLBuilderError(msg)
        self._expression.set("expressions", assignments)
        return self
