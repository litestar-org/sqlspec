# ruff: noqa: PLR6301
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional, Union

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder._base import QueryBuilder
from sqlspec.sql.builder._select import SelectBuilder

__all__ = ("MergeBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass
class MergeBuilder(QueryBuilder):
    """Builder for MERGE statements."""

    def _create_base_expression(self) -> "exp.Expression":
        """Create a base MERGE expression.

        Returns:
            exp.Expression: A new sqlglot Merge expression with empty clauses.
        """
        return exp.Merge(this=None, using=None, on=None, expressions=[])

    def into(self, table: "Union[str, exp.Expression]", alias: Optional[str] = None) -> "MergeBuilder":
        """Set the target table for the MERGE operation (INTO clause).

        Args:
            table: The target table name or expression for the MERGE operation.
                   Can be a string (table name) or an sqlglot Expression.
            alias: Optional alias for the target table.

        Returns:
            MergeBuilder: The current builder instance for method chaining.

        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()
        self._expression.set("this", exp.to_table(table, alias=alias) if isinstance(table, str) else table)
        return self

    def using(self, source: Union[str, exp.Expression, "SelectBuilder"], alias: Optional[str] = None) -> "MergeBuilder":
        """Set the source data for the MERGE operation (USING clause).

        Args:
            source: The source data for the MERGE operation.
                    Can be a string (table name), an sqlglot Expression, or a SelectBuilder instance.
            alias: Optional alias for the source table.

        Returns:
            MergeBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        source_expr: exp.Expression
        if isinstance(source, str):
            source_expr = exp.to_table(source, alias=alias)
        elif isinstance(source, SelectBuilder):
            subquery_exp = exp.paren(exp.maybe_parse(source.build().sql, dialect=self.dialect))
            source_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        else:
            source_expr = source
        self._expression.set("using", source_expr)
        return self

    def on(self, condition: Union[str, exp.Expression]) -> "MergeBuilder":
        """Set the join condition for the MERGE operation (ON clause).

        Args:
            condition: The join condition for the MERGE operation.
                       Can be a string (SQL condition) or an sqlglot Expression.

        Raises:
            SQLBuilderError: If the current expression is not a MERGE statement or if the condition type is unsupported.

        Returns:
            MergeBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        condition_expr: exp.Expression
        if isinstance(condition, str):
            parsed_condition = exp.maybe_parse(condition, dialect=self.dialect)  # type: ignore[var-annotated]
            if not parsed_condition:
                msg = f"Could not parse ON condition: {condition}"
                raise SQLBuilderError(msg)
            condition_expr = parsed_condition
        else:
            condition_expr = condition

        self._expression.set("on", condition_expr)
        return self

    def _create_when_clause(
        self,
        action: "exp.Expression",
        matched: bool,
        by_source: Optional[bool] = None,
        condition: "Optional[Union[str, exp.Expression]]" = None,
    ) -> "exp.Expression":
        """Helper to create a WHEN clause expression for sqlglot Merge.

        Args:
            action: The action to perform (e.g., exp.Update, exp.Delete, exp.Insert).
            matched: Whether this WHEN clause is for matched rows.
            by_source: If True, condition is "WHEN NOT MATCHED BY SOURCE", otherwise "WHEN NOT MATCHED BY TARGET".
            condition: An optional additional condition for this specific action.

        Raises:
            SQLBuilderError: If the condition is not a valid expression or string.

        Returns:
            exp.When: The constructed WHEN clause expression.
        """
        when_args: dict[str, Any] = {"then": action}

        if matched:
            when_args["matched"] = True
        else:
            when_args["matched"] = False
            if by_source is not None:
                when_args["source"] = by_source

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond = exp.maybe_parse(condition, dialect=self.dialect)  # type: ignore[var-annotated]
                if not parsed_cond:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_cond
            else:
                condition_expr = condition
            when_args["this"] = condition_expr

        return exp.When(**when_args)

    def when_matched_then_update(
        self, set_values: dict[str, Any], condition: Optional[Union[str, exp.Expression]] = None
    ) -> "MergeBuilder":
        """Define the UPDATE action for matched rows.

        Args:
            set_values: A dictionary of column names and their new values to set.
                        The values will be parameterized.
            condition: An optional additional condition for this specific action.

        Returns:
            MergeBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        update_expressions = []
        update_expressions.extend(
            [
                exp.Set(
                    this=exp.column(col),
                    expression=exp.Placeholder(this=self._add_parameter(val, name=f"update_set_{col}")),
                )
                for col, val in set_values.items()
            ]
        )

        self._expression.args.setdefault("expressions", []).append(
            self._create_when_clause(
                action=exp.Update(expressions=update_expressions), matched=True, condition=condition
            )
        )
        return self

    def when_matched_then_delete(self, condition: Optional[Union[str, exp.Expression]] = None) -> "MergeBuilder":
        """Define the DELETE action for matched rows.

        Args:
            condition: An optional additional condition for this specific action.

        Returns:
            MergeBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        delete_action = exp.Delete()

        when_clause = self._create_when_clause(action=delete_action, matched=True, condition=condition)
        self._expression.args.setdefault("expressions", []).append(when_clause)
        return self

    def when_not_matched_then_insert(
        self,
        columns: Optional[list[str]] = None,
        values: Optional[list[Any]] = None,
        condition: Optional[Union[str, exp.Expression]] = None,
        by_target: bool = True,
    ) -> "MergeBuilder":
        """Define the INSERT action for rows not matched.

        Args:
            columns: A list of column names to insert into. If None, implies INSERT DEFAULT VALUES or matching source columns.
            values: A list of values corresponding to the columns.
                    These values will be parameterized. If None, implies INSERT DEFAULT VALUES or subquery source.
            condition: An optional additional condition for this specific action.
            by_target: If True (default), condition is "WHEN NOT MATCHED [BY TARGET]".
                       If False, condition is "WHEN NOT MATCHED BY SOURCE".

        Raises:
            SQLBuilderError: If columns and values are provided but do not match in length,
                             or if columns are provided without values.

        Returns:
            MergeBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        insert_action_args: dict[str, Any] = {}
        if columns and values:
            if len(columns) != len(values):
                msg = "Number of columns must match number of values for INSERT."
                raise SQLBuilderError(msg)

            parameterized_values = []
            parameterized_values.extend(
                [exp.Placeholder(this=self._add_parameter(val, name=f"insert_val_{i}")) for i, val in enumerate(values)]
            )
            insert_action_args["this"] = exp.Schema(expressions=[exp.column(c) for c in columns])
            insert_action_args["expression"] = exp.Values(expressions=[exp.Tuple(expressions=parameterized_values)])
        elif columns and not values:
            msg = "Specifying columns without values for INSERT action is complex and not fully supported yet. Consider providing full expressions."
            raise SQLBuilderError(msg)
        elif not columns and not values:
            pass
        else:
            msg = "Cannot specify values without columns for INSERT action."
            raise SQLBuilderError(msg)

        self._expression.args.setdefault("expressions", []).append(
            self._create_when_clause(
                action=exp.Insert(**insert_action_args),
                matched=False,
                by_source=not by_target if by_target is not None else None,
                condition=condition,
            )
        )
        return self
