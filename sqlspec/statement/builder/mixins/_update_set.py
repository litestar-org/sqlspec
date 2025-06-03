from collections.abc import Mapping
from typing import Any, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("UpdateSetClauseMixin", )


class UpdateSetClauseMixin:
    """Mixin providing SET clause for UPDATE builders."""

    def set(self, values: Union[Mapping[str, Any], None] = None, **kwargs: Any) -> Self:
        """Set columns and values for the UPDATE statement.

        Args:
            values: A mapping of column names to values.
            **kwargs: Column-value pairs to set.

        Raises:
            SQLBuilderError: If the current expression is not an UPDATE statement.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Update()  # type: ignore[attr-defined]
        if not isinstance(self._expression, exp.Update):  # type: ignore[attr-defined]
            msg = "Cannot set columns on a non-UPDATE expression."
            raise SQLBuilderError(msg)
        assignments = []
        all_values = dict(values or {}, **kwargs)
        for col, val in all_values.items():
            param_name = self.add_parameter(val)[1]  # type: ignore[attr-defined]
            assignments.append(exp.EQ(this=exp.column(col), expression=exp.Placeholder(this=param_name)))
        self._expression.set("expressions", assignments)  # type: ignore[attr-defined]
        return self
