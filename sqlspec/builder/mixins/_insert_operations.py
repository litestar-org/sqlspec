"""Insert operation mixins for SQL builders."""

from collections.abc import Sequence
from typing import Any, Optional, TypeVar, Union

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.protocols import SQLBuilderProtocol

BuilderT = TypeVar("BuilderT", bound=SQLBuilderProtocol)

__all__ = ("InsertFromSelectMixin", "InsertIntoClauseMixin", "InsertValuesMixin")


@trait
class InsertIntoClauseMixin:
    """Mixin providing INTO clause for INSERT builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

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


@trait
class InsertValuesMixin:
    """Mixin providing VALUES and columns methods for INSERT builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    _columns: Any  # Provided by QueryBuilder

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def columns(self, *columns: Union[str, exp.Expression]) -> Self:
        """Set the columns for the INSERT statement and synchronize the _columns attribute on the builder."""
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "Cannot set columns on a non-INSERT expression."
            raise SQLBuilderError(msg)
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        self._expression.set("columns", column_exprs)
        try:
            cols = self._columns
            if not columns:
                cols.clear()
            else:
                cols[:] = [col.name if isinstance(col, exp.Column) else str(col) for col in columns]
        except AttributeError:
            pass
        return self

    def values(self, *values: Any, **kwargs: Any) -> Self:
        """Add a row of values to the INSERT statement.

        Supports:
        - values(val1, val2, val3)
        - values(col1=val1, col2=val2)
        - values(mapping)

        Args:
            *values: Either positional values or a single mapping.
            **kwargs: Column-value pairs.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "Cannot add values to a non-INSERT expression."
            raise SQLBuilderError(msg)

        if kwargs:
            if values:
                msg = "Cannot mix positional values with keyword values."
                raise SQLBuilderError(msg)
            try:
                _columns = self._columns
                if not _columns:
                    self.columns(*kwargs.keys())
            except AttributeError:
                pass
            row_exprs = []
            for col, val in kwargs.items():
                if isinstance(val, exp.Expression):
                    row_exprs.append(val)
                else:
                    column_name = col if isinstance(col, str) else str(col)
                    if "." in column_name:
                        column_name = column_name.split(".")[-1]
                    param_name = self._generate_unique_parameter_name(column_name)
                    _, param_name = self.add_parameter(val, name=param_name)
                    row_exprs.append(exp.var(param_name))
        elif len(values) == 1 and hasattr(values[0], "items"):
            mapping = values[0]
            try:
                _columns = self._columns
                if not _columns:
                    self.columns(*mapping.keys())
            except AttributeError:
                pass
            row_exprs = []
            for col, val in mapping.items():
                if isinstance(val, exp.Expression):
                    row_exprs.append(val)
                else:
                    column_name = col if isinstance(col, str) else str(col)
                    if "." in column_name:
                        column_name = column_name.split(".")[-1]
                    param_name = self._generate_unique_parameter_name(column_name)
                    _, param_name = self.add_parameter(val, name=param_name)
                    row_exprs.append(exp.var(param_name))
        else:
            try:
                _columns = self._columns
                if _columns and len(values) != len(_columns):
                    msg = f"Number of values ({len(values)}) does not match the number of specified columns ({len(_columns)})."
                    raise SQLBuilderError(msg)
            except AttributeError:
                pass
            row_exprs = []
            for i, v in enumerate(values):
                if isinstance(v, exp.Expression):
                    row_exprs.append(v)
                else:
                    try:
                        _columns = self._columns
                        if _columns and i < len(_columns):
                            column_name = (
                                str(_columns[i]).split(".")[-1] if "." in str(_columns[i]) else str(_columns[i])
                            )
                            param_name = self._generate_unique_parameter_name(column_name)
                        else:
                            param_name = self._generate_unique_parameter_name(f"value_{i + 1}")
                    except AttributeError:
                        param_name = self._generate_unique_parameter_name(f"value_{i + 1}")
                    _, param_name = self.add_parameter(v, name=param_name)
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


@trait
class InsertFromSelectMixin:
    """Mixin providing INSERT ... SELECT support for INSERT builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    _table: Any  # Provided by QueryBuilder

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def from_select(self, select_builder: Any) -> Self:
        """Sets the INSERT source to a SELECT statement.

        Args:
            select_builder: A SelectBuilder instance representing the SELECT query.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the table is not set or the select_builder is invalid.
        """
        try:
            if not self._table:
                msg = "The target table must be set using .into() before adding values."
                raise SQLBuilderError(msg)
        except AttributeError:
            msg = "The target table must be set using .into() before adding values."
            raise SQLBuilderError(msg)
        if self._expression is None:
            self._expression = exp.Insert()
        if not isinstance(self._expression, exp.Insert):
            msg = "Cannot set INSERT source on a non-INSERT expression."
            raise SQLBuilderError(msg)
        subquery_parameters = select_builder._parameters
        if subquery_parameters:
            for p_name, p_value in subquery_parameters.items():
                self.add_parameter(p_value, name=p_name)
        select_expr = select_builder._expression
        if select_expr and isinstance(select_expr, exp.Select):
            self._expression.set("expression", select_expr.copy())
        else:
            msg = "SelectBuilder must have a valid SELECT expression."
            raise SQLBuilderError(msg)
        return self
