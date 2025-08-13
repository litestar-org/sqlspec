"""Merge operation mixins for SQL builders."""

from typing import Any, Optional, Union

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters

__all__ = (
    "MergeIntoClauseMixin",
    "MergeMatchedClauseMixin",
    "MergeNotMatchedBySourceClauseMixin",
    "MergeNotMatchedClauseMixin",
    "MergeOnClauseMixin",
    "MergeUsingClauseMixin",
)


@trait
class MergeIntoClauseMixin:
    """Mixin providing INTO clause for MERGE builders."""

    __slots__ = ()
    _expression: Optional[exp.Expression]

    def into(self, table: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Set the target table for the MERGE operation (INTO clause).

        Args:
            table: The target table name or expression for the MERGE operation.
                   Can be a string (table name) or an sqlglot Expression.
            alias: Optional alias for the target table.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
        if not isinstance(self._expression, exp.Merge):
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
        self._expression.set("this", exp.to_table(table, alias=alias) if isinstance(table, str) else table)
        return self


@trait
class MergeUsingClauseMixin:
    """Mixin providing USING clause for MERGE builders."""

    __slots__ = ()
    _expression: Optional[exp.Expression]

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def using(self, source: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
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
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
        if not isinstance(self._expression, exp.Merge):
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))

        source_expr: exp.Expression
        if isinstance(source, str):
            source_expr = exp.to_table(source, alias=alias)
        elif has_query_builder_parameters(source) and hasattr(source, "_expression"):
            subquery_builder_parameters = source.parameters
            if subquery_builder_parameters:
                for p_name, p_value in subquery_builder_parameters.items():
                    self.add_parameter(p_value, name=p_name)

            subquery_exp = exp.paren(getattr(source, "_expression", exp.select()))
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


@trait
class MergeOnClauseMixin:
    """Mixin providing ON clause for MERGE builders."""

    __slots__ = ()
    _expression: Optional[exp.Expression]

    def on(self, condition: Union[str, exp.Expression]) -> Self:
        """Set the join condition for the MERGE operation (ON clause).

        Args:
            condition: The join condition for the MERGE operation.
                       Can be a string (SQL condition) or an sqlglot Expression.

        Returns:
            The current builder instance for method chaining.

        Raises:
            SQLBuilderError: If the current expression is not a MERGE statement or if the condition type is unsupported.
        """
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
        if not isinstance(self._expression, exp.Merge):
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))

        condition_expr: exp.Expression
        if isinstance(condition, str):
            parsed_condition: Optional[exp.Expression] = exp.maybe_parse(
                condition, dialect=getattr(self, "dialect", None)
            )
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


@trait
class MergeMatchedClauseMixin:
    """Mixin providing WHEN MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()
    _expression: Optional[exp.Expression]

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _add_when_clause(self, when_clause: exp.When) -> None:
        """Helper to add a WHEN clause to the MERGE statement.

        Args:
            when_clause: The WHEN clause to add.
        """
        if self._expression is None:
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))  # type: ignore[misc]
        if not isinstance(self._expression, exp.Merge):
            self._expression = exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))  # type: ignore[misc]

        whens = self._expression.args.get("whens")
        if not whens:
            whens = exp.Whens(expressions=[])
            self._expression.set("whens", whens)

        whens.append("expressions", when_clause)

    def when_matched_then_update(
        self,
        set_values: Optional[dict[str, Any]] = None,
        condition: Optional[Union[str, exp.Expression]] = None,
        **kwargs: Any,
    ) -> Self:
        """Define the UPDATE action for matched rows.

        Supports:
        - when_matched_then_update({"column": value})
        - when_matched_then_update(column=value, other_column=other_value)
        - when_matched_then_update({"column": value}, other_column=other_value)

        Args:
            set_values: A dictionary of column names and their new values to set.
                        The values will be parameterized.
            condition: An optional additional condition for this specific action.
            **kwargs: Column-value pairs to update on match.

        Raises:
            SQLBuilderError: If the condition type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        # Combine set_values dict and kwargs
        all_values = dict(set_values or {}, **kwargs)

        if not all_values:
            msg = "No update values provided. Use set_values dict or kwargs."
            raise SQLBuilderError(msg)

        update_expressions: list[exp.EQ] = []
        for col, val in all_values.items():
            if hasattr(val, "expression") and hasattr(val, "sql"):
                # Handle SQL objects (from sql.raw with parameters)
                expression = getattr(val, "expression", None)
                if expression is not None and isinstance(expression, exp.Expression):
                    # Merge parameters from SQL object into builder
                    if hasattr(val, "parameters"):
                        sql_parameters = getattr(val, "parameters", {})
                        for param_name, param_value in sql_parameters.items():
                            self.add_parameter(param_value, name=param_name)
                    value_expr = expression
                else:
                    # If expression is None, fall back to parsing the raw SQL
                    sql_text = getattr(val, "sql", "")
                    # Merge parameters even when parsing raw SQL
                    if hasattr(val, "parameters"):
                        sql_parameters = getattr(val, "parameters", {})
                        for param_name, param_value in sql_parameters.items():
                            self.add_parameter(param_value, name=param_name)
                    # Check if sql_text is callable (like Expression.sql method)
                    if callable(sql_text):
                        sql_text = str(val)
                    value_expr = exp.maybe_parse(sql_text) or exp.convert(str(sql_text))
            elif isinstance(val, exp.Expression):
                value_expr = val
            else:
                column_name = col if isinstance(col, str) else str(col)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                param_name = self._generate_unique_parameter_name(column_name)
                param_name = self.add_parameter(val, name=param_name)[1]
                value_expr = exp.Placeholder(this=param_name)

            update_expressions.append(exp.EQ(this=exp.column(col), expression=value_expr))

        when_args: dict[str, Any] = {"matched": True, "then": exp.Update(expressions=update_expressions)}

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
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

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)
        return self

    def when_matched_then_delete(self, condition: Optional[Union[str, exp.Expression]] = None) -> Self:
        """Define the DELETE action for matched rows.

        Args:
            condition: An optional additional condition for this specific action.

        Raises:
            SQLBuilderError: If the condition type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        when_args: dict[str, Any] = {"matched": True, "then": exp.Delete()}

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
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

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)
        return self


@trait
class MergeNotMatchedClauseMixin:
    """Mixin providing WHEN NOT MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    _expression: Optional[exp.Expression]

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _add_when_clause(self, when_clause: exp.When) -> None:
        """Helper to add a WHEN clause to the MERGE statement - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def when_not_matched_then_insert(
        self,
        columns: Optional[list[str]] = None,
        values: Optional[list[Any]] = None,
        condition: Optional[Union[str, exp.Expression]] = None,
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
        insert_args: dict[str, Any] = {}
        if columns and values:
            if len(columns) != len(values):
                msg = "Number of columns must match number of values for INSERT."
                raise SQLBuilderError(msg)

            parameterized_values: list[exp.Expression] = []
            for i, val in enumerate(values):
                column_name = columns[i] if isinstance(columns[i], str) else str(columns[i])
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                param_name = self._generate_unique_parameter_name(column_name)
                param_name = self.add_parameter(val, name=param_name)[1]
                parameterized_values.append(exp.var(param_name))

            insert_args["this"] = exp.Tuple(expressions=[exp.column(c) for c in columns])
            insert_args["expression"] = exp.Tuple(expressions=parameterized_values)
        elif columns and not values:
            msg = "Specifying columns without values for INSERT action is complex and not fully supported yet. Consider providing full expressions."
            raise SQLBuilderError(msg)
        elif not columns and not values:
            pass
        else:
            msg = "Cannot specify values without columns for INSERT action."
            raise SQLBuilderError(msg)

        when_args: dict[str, Any] = {"matched": False, "then": exp.Insert(**insert_args)}

        if not by_target:
            when_args["source"] = True

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
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

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)
        return self


@trait
class MergeNotMatchedBySourceClauseMixin:
    """Mixin providing WHEN NOT MATCHED BY SOURCE THEN ... clauses for MERGE builders."""

    __slots__ = ()

    _expression: Optional[exp.Expression]

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _add_when_clause(self, when_clause: exp.When) -> None:
        """Helper to add a WHEN clause to the MERGE statement - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def when_not_matched_by_source_then_update(
        self,
        set_values: Optional[dict[str, Any]] = None,
        condition: Optional[Union[str, exp.Expression]] = None,
        **kwargs: Any,
    ) -> Self:
        """Define the UPDATE action for rows not matched by source.

        This is useful for handling rows that exist in the target but not in the source.

        Supports:
        - when_not_matched_by_source_then_update({"column": value})
        - when_not_matched_by_source_then_update(column=value, other_column=other_value)
        - when_not_matched_by_source_then_update({"column": value}, other_column=other_value)

        Args:
            set_values: A dictionary of column names and their new values to set.
            condition: An optional additional condition for this specific action.
            **kwargs: Column-value pairs to update when not matched by source.

        Raises:
            SQLBuilderError: If the condition type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        # Combine set_values dict and kwargs
        all_values = dict(set_values or {}, **kwargs)

        if not all_values:
            msg = "No update values provided. Use set_values dict or kwargs."
            raise SQLBuilderError(msg)

        update_expressions: list[exp.EQ] = []
        for col, val in all_values.items():
            if hasattr(val, "expression") and hasattr(val, "sql"):
                # Handle SQL objects (from sql.raw with parameters)
                expression = getattr(val, "expression", None)
                if expression is not None and isinstance(expression, exp.Expression):
                    # Merge parameters from SQL object into builder
                    if hasattr(val, "parameters"):
                        sql_parameters = getattr(val, "parameters", {})
                        for param_name, param_value in sql_parameters.items():
                            self.add_parameter(param_value, name=param_name)
                    value_expr = expression
                else:
                    # If expression is None, fall back to parsing the raw SQL
                    sql_text = getattr(val, "sql", "")
                    # Merge parameters even when parsing raw SQL
                    if hasattr(val, "parameters"):
                        sql_parameters = getattr(val, "parameters", {})
                        for param_name, param_value in sql_parameters.items():
                            self.add_parameter(param_value, name=param_name)
                    # Check if sql_text is callable (like Expression.sql method)
                    if callable(sql_text):
                        sql_text = str(val)
                    value_expr = exp.maybe_parse(sql_text) or exp.convert(str(sql_text))
            elif isinstance(val, exp.Expression):
                value_expr = val
            else:
                column_name = col if isinstance(col, str) else str(col)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                param_name = self._generate_unique_parameter_name(column_name)
                param_name = self.add_parameter(val, name=param_name)[1]
                value_expr = exp.Placeholder(this=param_name)

            update_expressions.append(exp.EQ(this=exp.column(col), expression=value_expr))

        when_args: dict[str, Any] = {
            "matched": False,
            "source": True,
            "then": exp.Update(expressions=update_expressions),
        }

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
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

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)
        return self

    def when_not_matched_by_source_then_delete(self, condition: Optional[Union[str, exp.Expression]] = None) -> Self:
        """Define the DELETE action for rows not matched by source.

        This is useful for cleaning up rows that exist in the target but not in the source.

        Args:
            condition: An optional additional condition for this specific action.

        Raises:
            SQLBuilderError: If the condition type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        when_args: dict[str, Any] = {"matched": False, "source": True, "then": exp.Delete()}

        if condition:
            condition_expr: exp.Expression
            if isinstance(condition, str):
                parsed_cond: Optional[exp.Expression] = exp.maybe_parse(
                    condition, dialect=getattr(self, "dialect", None)
                )
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

        when_clause = exp.When(**when_args)
        self._add_when_clause(when_clause)
        return self
