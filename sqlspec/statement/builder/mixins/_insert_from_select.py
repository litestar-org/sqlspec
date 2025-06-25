from typing import Any, Optional

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("InsertFromSelectMixin",)


class InsertFromSelectMixin:
    """Mixin providing INSERT ... SELECT support for INSERT builders."""

    _expression: Optional[exp.Expression] = None

    def from_select(self, select_builder: Any) -> Self:
        """Sets the INSERT source to a SELECT statement.

        Args:
            select_builder: A SelectBuilder instance representing the SELECT query.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the table is not set or the select_builder is invalid.
        """
        if not getattr(self, "_table", None):
            msg = "The target table must be set using .into() before adding values."
            raise SQLBuilderError(msg)
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "Cannot set INSERT source on a non-INSERT expression."
            raise SQLBuilderError(msg)
        # Merge parameters from the SELECT builder
        subquery_params = getattr(select_builder, "_parameters", None)
        if subquery_params:
            for p_name, p_value in subquery_params.items():
                self.add_parameter(p_value, name=p_name)  # type: ignore[attr-defined]
        # Set the SELECT expression as the source
        select_expr = getattr(select_builder, "_expression", None)
        if select_expr and isinstance(select_expr, exp.Select):
            self._expression.set("expression", select_expr.copy())
        else:
            msg = "SelectBuilder must have a valid SELECT expression."
            raise SQLBuilderError(msg)
        return self
