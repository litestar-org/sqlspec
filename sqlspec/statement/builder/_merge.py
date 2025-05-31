# ruff: noqa: PLR6301, SLF001
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._base import QueryBuilder
from sqlspec.statement.result import ExecuteResult

if TYPE_CHECKING:
    from sqlspec.statement.builder._select import SelectBuilder

__all__ = ("MergeBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass(unsafe_hash=True)
class MergeBuilder(QueryBuilder[ExecuteResult[dict[str, Any]]]):
    """Builder for MERGE statements.

    This builder provides a fluent interface for constructing SQL MERGE statements
    (also known as UPSERT in some databases) with automatic parameter binding and validation.

    Example:
        ```python
        # Basic MERGE statement
        merge_query = (
            MergeBuilder()
            .into("target_table")
            .using("source_table", "src")
            .on("target_table.id = src.id")
            .when_matched_then_update(
                {
                    "name": "src.name",
                    "updated_at": "NOW()",
                }
            )
            .when_not_matched_then_insert(
                columns=["id", "name", "created_at"],
                values=["src.id", "src.name", "NOW()"],
            )
        )

        # MERGE with subquery source
        source_query = (
            SelectBuilder()
            .select("id", "name", "email")
            .from_("temp_users")
            .where("status = 'pending'")
        )

        merge_query = (
            MergeBuilder()
            .into("users")
            .using(source_query, "src")
            .on("users.email = src.email")
            .when_matched_then_update({"name": "src.name"})
            .when_not_matched_then_insert(
                columns=["id", "name", "email"],
                values=["src.id", "src.name", "src.email"],
            )
        )
        ```
    """

    @property
    def _expected_result_type(self) -> type[ExecuteResult[dict[str, Any]]]:
        """Return the expected result type for this builder.

        Returns:
            The ExecuteResult type for MERGE statements.
        """
        return ExecuteResult[dict[str, Any]]

    def _create_base_expression(self) -> exp.Merge:
        """Create a base MERGE expression.

        Returns:
            A new sqlglot Merge expression with empty clauses.
        """
        return exp.Merge(this=None, using=None, on=None, expressions=[])

    def into(self, table: str | exp.Expression, alias: str | None = None) -> Self:
        """Set the target table for the MERGE operation (INTO clause).

        Args:
            table: The target table name or expression for the MERGE operation.
                   Can be a string (table name) or an sqlglot Expression.
            alias: Optional alias for the target table.

        Returns:
            The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()
        self._expression.set("this", exp.to_table(table, alias=alias) if isinstance(table, str) else table)
        return self

    def using(self, source: str | exp.Expression | SelectBuilder, alias: str | None = None) -> Self:
        """Set the source data for the MERGE operation (USING clause).

        Args:
            source: The source data for the MERGE operation.
                    Can be a string (table name), an sqlglot Expression, or a SelectBuilder instance.
            alias: Optional alias for the source table.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not a MERGE statement or if the source type is unsupported.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        source_expr: exp.Expression
        if isinstance(source, str):
            source_expr = exp.to_table(source, alias=alias)
        elif isinstance(source, QueryBuilder):
            # Merge parameters from the SELECT builder
            subquery_builder_params: dict[str, Any] = source._parameters
            if subquery_builder_params:
                for p_name, p_value in subquery_builder_params.items():
                    self.add_parameter(p_value, name=p_name)

            subquery_exp: exp.Expression = exp.paren(source._expression or exp.select())
            source_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
        elif isinstance(source, exp.Expression):
            source_expr = source
            if alias:
                source_expr = exp.alias_(source_expr, alias)
        else:
            msg = f"Unsupported source type for USING clause: {type(source)}"
            raise SQLBuilderError(msg)

        self._expression.set("using", source_expr)
        return self

    def on(self, condition: str | exp.Expression) -> Self:
        """Set the join condition for the MERGE operation (ON clause).

        Args:
            condition: The join condition for the MERGE operation.
                       Can be a string (SQL condition) or an sqlglot Expression.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not a MERGE statement or if the condition type is unsupported.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        condition_expr: exp.Expression
        if isinstance(condition, str):
            parsed_condition = exp.maybe_parse(condition, dialect=self.dialect)
            if not parsed_condition:
                msg = f"Could not parse ON condition: {condition}"
                raise SQLBuilderError(msg)
            condition_expr = parsed_condition
        elif isinstance(condition, exp.Expression):
            condition_expr = condition
        else:
            msg = f"Unsupported condition type for ON clause: {type(condition)}"
            raise SQLBuilderError(msg)

        self._expression.set("on", condition_expr)
        return self

    def _create_when_clause(
        self,
        action: exp.Expression,
        matched: bool,
        by_source: bool | None = None,
        condition: str | exp.Expression | None = None,
    ) -> exp.When:
        """Helper to create a WHEN clause expression for sqlglot Merge.

        Args:
            action: The action to perform (e.g., exp.Update, exp.Delete, exp.Insert).
            matched: Whether this WHEN clause is for matched rows.
            by_source: If True, condition is "WHEN NOT MATCHED BY SOURCE", otherwise "WHEN NOT MATCHED BY TARGET".
            condition: An optional additional condition for this specific action.

        Returns:
            The constructed WHEN clause expression.

        Raises:
            SQLBuilderError: If the condition is not a valid expression or string.
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
                parsed_cond = exp.maybe_parse(condition, dialect=self.dialect)
                if not parsed_cond:
                    msg = f"Could not parse WHEN clause condition: {condition}"
                    raise SQLBuilderError(msg)
                condition_expr = parsed_cond
            elif isinstance(condition, exp.Expression):
                condition_expr = condition
            else:
                msg = f"Unsupported condition type for WHEN clause: {type(condition)}"
                raise SQLBuilderError(msg)
            when_args["this"] = condition_expr

        return exp.When(**when_args)

    def when_matched_then_update(
        self, set_values: dict[str, Any], condition: str | exp.Expression | None = None
    ) -> Self:
        """Define the UPDATE action for matched rows.

        Args:
            set_values: A dictionary of column names and their new values to set.
                        The values will be parameterized.
            condition: An optional additional condition for this specific action.

        Returns:
            The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        update_expressions: list[exp.Set] = []
        for col, val in set_values.items():
            param_name = self._add_parameter(val)
            update_expressions.append(
                exp.Set(
                    this=exp.column(col),
                    expression=exp.var(param_name),
                )
            )

        self._expression.args.setdefault("expressions", []).append(
            self._create_when_clause(
                action=exp.Update(expressions=update_expressions), matched=True, condition=condition
            )
        )
        return self

    def when_matched_then_delete(self, condition: str | exp.Expression | None = None) -> Self:
        """Define the DELETE action for matched rows.

        Args:
            condition: An optional additional condition for this specific action.

        Returns:
            The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        delete_action: exp.Delete = exp.Delete()

        when_clause: exp.When = self._create_when_clause(action=delete_action, matched=True, condition=condition)
        self._expression.args.setdefault("expressions", []).append(when_clause)
        return self

    def when_not_matched_then_insert(
        self,
        columns: list[str] | None = None,
        values: list[Any] | None = None,
        condition: str | exp.Expression | None = None,
        by_target: bool = True,
    ) -> Self:
        """Define the INSERT action for rows not matched.

        Args:
            columns: A list of column names to insert into. If None, implies INSERT DEFAULT VALUES or matching source columns.
            values: A list of values corresponding to the columns.
                    These values will be parameterized. If None, implies INSERT DEFAULT VALUES or subquery source.
            condition: An optional additional condition for this specific action.
            by_target: If True (default), condition is "WHEN NOT MATCHED [BY TARGET]".
                       If False, condition is "WHEN NOT MATCHED BY SOURCE".

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If columns and values are provided but do not match in length,
                             or if columns are provided without values.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        insert_action_args: dict[str, Any] = {}
        if columns and values:
            if len(columns) != len(values):
                msg = "Number of columns must match number of values for INSERT."
                raise SQLBuilderError(msg)

            parameterized_values: list[exp.Expression] = []
            for val in values:
                param_name = self._add_parameter(val)
                parameterized_values.append(exp.var(param_name))

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

    def when_not_matched_by_source_then_update(
        self, set_values: dict[str, Any], condition: str | exp.Expression | None = None
    ) -> Self:
        """Define the UPDATE action for rows not matched by source.

        This is useful for handling rows that exist in the target but not in the source.

        Args:
            set_values: A dictionary of column names and their new values to set.
            condition: An optional additional condition for this specific action.

        Returns:
            The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        update_expressions: list[exp.Set] = []
        for col, val in set_values.items():
            param_name = self._add_parameter(val)
            update_expressions.append(
                exp.Set(
                    this=exp.column(col),
                    expression=exp.var(param_name),
                )
            )

        self._expression.args.setdefault("expressions", []).append(
            self._create_when_clause(
                action=exp.Update(expressions=update_expressions), matched=False, by_source=True, condition=condition
            )
        )
        return self

    def when_not_matched_by_source_then_delete(self, condition: str | exp.Expression | None = None) -> Self:
        """Define the DELETE action for rows not matched by source.

        This is useful for cleaning up rows that exist in the target but not in the source.

        Args:
            condition: An optional additional condition for this specific action.

        Returns:
            The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Merge):
            self._expression = self._create_base_expression()

        delete_action: exp.Delete = exp.Delete()

        when_clause: exp.When = self._create_when_clause(
            action=delete_action, matched=False, by_source=True, condition=condition
        )
        self._expression.args.setdefault("expressions", []).append(when_clause)
        return self
