"""Insert operation mixins for SQL builders."""

from collections.abc import Sequence
from typing import Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("InsertFromSelectMixin", "InsertIntoClauseMixin", "InsertValuesMixin")


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
        select_expr = getattr(select_builder, "_expression", None)
        if select_expr and isinstance(select_expr, exp.Select):
            self._expression.set("expression", select_expr.copy())
        else:
            msg = "SelectBuilder must have a valid SELECT expression."
            raise SQLBuilderError(msg)
        return self
