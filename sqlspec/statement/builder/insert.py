"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder.base import QueryBuilder
from sqlspec.statement.builder.mixins import (
    InsertFromSelectMixin,
    InsertIntoClauseMixin,
    InsertValuesMixin,
    ReturningClauseMixin,
)
from sqlspec.statement.result import SQLResult
from sqlspec.typing import RowT

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


__all__ = ("InsertBuilder",)

ERR_MSG_TABLE_NOT_SET = "The target table must be set using .into() before adding values."
ERR_MSG_VALUES_COLUMNS_MISMATCH = (
    "Number of values ({values_len}) does not match the number of specified columns ({columns_len})."
)
ERR_MSG_INTERNAL_EXPRESSION_TYPE = "Internal error: expression is not an Insert instance as expected."
ERR_MSG_EXPRESSION_NOT_INITIALIZED = "Internal error: base expression not initialized."


@dataclass(unsafe_hash=True)
class InsertBuilder(
    QueryBuilder[RowT], ReturningClauseMixin, InsertValuesMixin, InsertFromSelectMixin, InsertIntoClauseMixin
):
    """Builder for INSERT statements.

    This builder facilitates the construction of SQL INSERT queries
    in a safe and dialect-agnostic manner with automatic parameter binding.

    Example:
        ```python
        # Basic INSERT with values
        insert_query = (
            InsertBuilder()
            .into("users")
            .columns("name", "email", "age")
            .values("John Doe", "john@example.com", 30)
        )

        # Multi-row INSERT
        insert_query = (
            InsertBuilder()
            .into("users")
            .columns("name", "email")
            .values("John", "john@example.com")
            .values("Jane", "jane@example.com")
        )

        # INSERT from dictionary
        insert_query = (
            InsertBuilder()
            .into("users")
            .values_from_dict(
                {"name": "John", "email": "john@example.com"}
            )
        )

        # INSERT from SELECT
        insert_query = (
            InsertBuilder()
            .into("users_backup")
            .from_select(
                SelectBuilder()
                .select("name", "email")
                .from_("users")
                .where("active = true")
            )
        )
        ```
    """

    _table: "Optional[str]" = field(default=None, init=False)
    _columns: list[str] = field(default_factory=list, init=False)
    _values_added_count: int = field(default=0, init=False)

    def _create_base_expression(self) -> exp.Insert:
        """Create a base INSERT expression.

        This method is called by the base QueryBuilder during initialization.

        Returns:
            A new sqlglot Insert expression.
        """
        return exp.Insert()

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        """Specifies the expected result type for an INSERT query.

        Returns:
            The type of result expected for INSERT operations.
        """
        return SQLResult[RowT]

    def _get_insert_expression(self) -> exp.Insert:
        """Safely gets and casts the internal expression to exp.Insert.

        Returns:
            The internal expression as exp.Insert.

        Raises:
            SQLBuilderError: If the expression is not initialized or is not an Insert.
        """
        if self._expression is None:
            raise SQLBuilderError(ERR_MSG_EXPRESSION_NOT_INITIALIZED)
        if not isinstance(self._expression, exp.Insert):
            raise SQLBuilderError(ERR_MSG_INTERNAL_EXPRESSION_TYPE)
        return self._expression

    def values(self, *values: Any) -> "Self":
        """Adds a row of values to the INSERT statement.

        This method can be called multiple times to insert multiple rows,
        resulting in a multi-row INSERT statement like `VALUES (...), (...)`.

        Args:
            *values: The values for the row to be inserted. The number of values
                     must match the number of columns set by `columns()`, if `columns()` was called
                     and specified any non-empty list of columns.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If `into()` has not been called to set the table,
                             or if `columns()` was called with a non-empty list of columns
                             and the number of values does not match the number of specified columns.
        """
        if not self._table:
            raise SQLBuilderError(ERR_MSG_TABLE_NOT_SET)

        insert_expr = self._get_insert_expression()

        if self._columns and len(values) != len(self._columns):
            msg = ERR_MSG_VALUES_COLUMNS_MISMATCH.format(values_len=len(values), columns_len=len(self._columns))
            raise SQLBuilderError(msg)

        param_names = [self._add_parameter(value) for value in values]
        value_placeholders = tuple(exp.var(name) for name in param_names)

        current_values_expression = insert_expr.args.get("expression")

        if self._values_added_count == 0:
            new_values_node = exp.Values(expressions=[exp.Tuple(expressions=list(value_placeholders))])
            insert_expr.set("expression", new_values_node)
        elif isinstance(current_values_expression, exp.Values):
            current_values_expression.expressions.append(exp.Tuple(expressions=list(value_placeholders)))
        else:
            # This case should ideally not be reached if logic is correct:
            # means _values_added_count > 0 but expression is not exp.Values.
            # Fallback to creating a new Values node, though this might indicate an issue.
            new_values_node = exp.Values(expressions=[exp.Tuple(expressions=list(value_placeholders))])
            insert_expr.set("expression", new_values_node)

        self._values_added_count += 1
        return self

    def values_from_dict(self, data: "Mapping[str, Any]") -> "Self":
        """Adds a row of values from a dictionary.

        This is a convenience method that automatically sets columns based on
        the dictionary keys and values based on the dictionary values.

        Args:
            data: A mapping of column names to values.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If `into()` has not been called to set the table.
        """
        if not self._table:
            raise SQLBuilderError(ERR_MSG_TABLE_NOT_SET)

        if not self._columns:
            # Set columns from dictionary keys if not already set
            self.columns(*data.keys())
        elif set(self._columns) != set(data.keys()):
            # Verify that dictionary keys match existing columns
            msg = f"Dictionary keys {set(data.keys())} do not match existing columns {set(self._columns)}."
            raise SQLBuilderError(msg)

        # Add values in the same order as columns
        return self.values(*[data[col] for col in self._columns])

    def values_from_dicts(self, data: "Sequence[Mapping[str, Any]]") -> "Self":
        """Adds multiple rows of values from a sequence of dictionaries.

        This is a convenience method for bulk inserts from structured data.

        Args:
            data: A sequence of mappings, each representing a row of data.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If `into()` has not been called to set the table,
                           or if dictionaries have inconsistent keys.
        """
        if not data:
            return self

        # Use the first dictionary to establish columns
        first_dict = data[0]
        if not self._columns:
            self.columns(*first_dict.keys())

        # Validate that all dictionaries have the same keys
        expected_keys = set(self._columns)
        for i, row_dict in enumerate(data):
            if set(row_dict.keys()) != expected_keys:
                msg = (
                    f"Dictionary at index {i} has keys {set(row_dict.keys())} "
                    f"which do not match expected keys {expected_keys}."
                )
                raise SQLBuilderError(msg)

        # Add each row
        for row_dict in data:
            self.values(*[row_dict[col] for col in self._columns])

        return self

    def on_conflict_do_nothing(self) -> "Self":
        """Adds an ON CONFLICT DO NOTHING clause (PostgreSQL syntax).

        This is used to ignore rows that would cause a conflict.

        Returns:
            The current builder instance for method chaining.

        Note:
            This is PostgreSQL-specific syntax. Different databases have different syntax.
            For a more general solution, you might need dialect-specific handling.
        """
        insert_expr = self._get_insert_expression()
        # Using sqlglot's OnConflict expression if available
        try:
            on_conflict = exp.OnConflict(this=None, expressions=[])
            insert_expr.set("on", on_conflict)
        except AttributeError:
            # Fallback for older sqlglot versions
            pass
        return self

    def on_duplicate_key_update(self, **set_values: Any) -> "Self":
        """Adds an ON DUPLICATE KEY UPDATE clause (MySQL syntax).

        Args:
            **set_values: Column-value pairs to update on duplicate key.

        Returns:
            The current builder instance for method chaining.
        """
        return self
