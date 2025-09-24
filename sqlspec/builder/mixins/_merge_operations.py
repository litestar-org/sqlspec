# pyright: reportPrivateUsage=false
"""MERGE operation mixins.

Provides mixins for MERGE statement functionality including INTO,
USING, ON, WHEN MATCHED, and WHEN NOT MATCHED clauses.
"""

from typing import Any, Optional, Union

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import extract_sql_object_expression
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

    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def into(self, table: Union[str, exp.Expression], alias: Optional[str] = None) -> Self:
        """Set the target table for the MERGE operation (INTO clause).

        Args:
            table: The target table name or expression for the MERGE operation.
                   Can be a string (table name) or an sqlglot Expression.
            alias: Optional alias for the target table.

        Returns:
            The current builder instance for method chaining.
        """
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        current_expr.set("this", exp.to_table(table, alias=alias) if isinstance(table, str) else table)
        return self


@trait
class MergeUsingClauseMixin:
    """Mixin providing USING clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
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
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        source_expr: exp.Expression
        if isinstance(source, str):
            source_expr = exp.to_table(source, alias=alias)
        elif isinstance(source, dict):
            columns = list(source.keys())
            values = list(source.values())

            parameterized_values: list[exp.Expression] = []
            for col, val in zip(columns, values):
                column_name = col if isinstance(col, str) else str(col)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                param_name = self._generate_unique_parameter_name(column_name)
                _, param_name = self.add_parameter(val, name=param_name)
                parameterized_values.append(exp.Placeholder(this=param_name))

            select_expr = exp.Select()
            select_expressions = []
            for i, col in enumerate(columns):
                select_expressions.append(exp.alias_(parameterized_values[i], col))
            select_expr.set("expressions", select_expressions)

            from_expr = exp.From(this=exp.to_table("DUAL"))
            select_expr.set("from", from_expr)

            source_expr = exp.paren(select_expr)
            if alias:
                source_expr = exp.alias_(source_expr, alias, table=False)
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

        current_expr.set("using", source_expr)
        return self


@trait
class MergeOnClauseMixin:
    """Mixin providing ON clause for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

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
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
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

        current_expr.set("on", condition_expr)
        return self


@trait
class MergeMatchedClauseMixin:
    """Mixin providing WHEN MATCHED THEN ... clauses for MERGE builders."""

    __slots__ = ()

    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _is_column_reference(self, value: str) -> bool:
        """Check if a string value is a column reference rather than a literal.

        Uses sqlglot to parse the value and determine if it represents a column
        reference, function call, or other SQL expression rather than a literal.
        """
        if not isinstance(value, str):
            return False

        # If the string contains spaces and no SQL-like syntax, treat as literal
        if " " in value and not any(x in value for x in [".", "(", ")", "*", "="]):
            return False

        # Only consider strings with dots (table.column), functions, or SQL keywords as column references
        # Simple identifiers are treated as literals
        if not any(x in value for x in [".", "(", ")"]):
            # Check if it's a SQL keyword/function that should be treated as expression
            sql_keywords = {"NULL", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "DEFAULT"}
            if value.upper() not in sql_keywords:
                return False

        try:
            # Try to parse as SQL expression
            parsed: Optional[exp.Expression] = exp.maybe_parse(value)
            if parsed is None:
                return False

            # Check for SQL literals that should be treated as expressions
            return isinstance(
                parsed,
                (exp.Dot, exp.Anonymous, exp.Func, exp.Null, exp.CurrentTimestamp, exp.CurrentDate, exp.CurrentTime),
            )
        except Exception:
            # If parsing fails, treat as literal
            return False

    def _add_when_clause(self, when_clause: exp.When) -> None:
        """Helper to add a WHEN clause to the MERGE statement.

        Args:
            when_clause: The WHEN clause to add.
        """
        current_expr = self.get_expression()
        if current_expr is None or not isinstance(current_expr, exp.Merge):
            self.set_expression(exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[])))
            current_expr = self.get_expression()

        assert current_expr is not None
        whens = current_expr.args.get("whens")
        if not whens:
            whens = exp.Whens(expressions=[])
            current_expr.set("whens", whens)

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
        all_values = {}
        if set_values:
            all_values.update(set_values)
        if kwargs:
            all_values.update(kwargs)

        if not all_values:
            msg = "No update values provided. Use set_values dict or kwargs."
            raise SQLBuilderError(msg)

        update_expressions: list[exp.EQ] = []
        for col, val in all_values.items():
            if hasattr(val, "expression") and hasattr(val, "sql"):
                value_expr = extract_sql_object_expression(val, builder=self)
            elif isinstance(val, exp.Expression):
                value_expr = val
            elif isinstance(val, str) and self._is_column_reference(val):
                value_expr = exp.maybe_parse(val) or exp.column(val)
            else:
                column_name = col if isinstance(col, str) else str(col)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                param_name = self._generate_unique_parameter_name(column_name)
                _, param_name = self.add_parameter(val, name=param_name)
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

    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

    def _is_column_reference(self, value: str) -> bool:
        """Check if a string value is a column reference rather than a literal.

        Uses sqlglot to parse the value and determine if it represents a column
        reference, function call, or other SQL expression rather than a literal.
        """
        if not isinstance(value, str):
            return False

        # If the string contains spaces and no SQL-like syntax, treat as literal
        if " " in value and not any(x in value for x in [".", "(", ")", "*", "="]):
            return False

        try:
            # Try to parse as SQL expression
            parsed: Optional[exp.Expression] = exp.maybe_parse(value)
            if parsed is None:
                return False

            # If it parses to a Column, Dot (table.column), Identifier, or other SQL constructs

        except Exception:
            # If parsing fails, fall back to conservative approach
            # Only treat simple identifiers as column references
            return (
                value.replace("_", "").replace(".", "").isalnum()
                and (value[0].isalpha() or value[0] == "_")
                and " " not in value
                and "'" not in value
                and '"' not in value
            )
        return bool(
            isinstance(
                parsed,
                (
                    exp.Column,
                    exp.Dot,
                    exp.Identifier,
                    exp.Anonymous,
                    exp.Func,
                    exp.Null,
                    exp.CurrentTimestamp,
                    exp.CurrentDate,
                    exp.CurrentTime,
                ),
            )
        )

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
                if isinstance(val, str) and self._is_column_reference(val):
                    # Handle column references (like "s.data") as column expressions, not parameters
                    parameterized_values.append(exp.maybe_parse(val) or exp.column(val))
                else:
                    column_name = columns[i] if isinstance(columns[i], str) else str(columns[i])
                    if "." in column_name:
                        column_name = column_name.split(".")[-1]
                    param_name = self._generate_unique_parameter_name(column_name)
                    _, param_name = self.add_parameter(val, name=param_name)
                    parameterized_values.append(exp.Placeholder(this=param_name))

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

    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

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

    def _is_column_reference(self, value: str) -> bool:
        """Check if a string value is a column reference rather than a literal.

        Uses sqlglot to parse the value and determine if it represents a column
        reference, function call, or other SQL expression rather than a literal.

        Args:
            value: The string value to check

        Returns:
            True if the value is a column reference, False if it's a literal
        """
        if not isinstance(value, str):
            return False

        try:
            parsed: Optional[exp.Expression] = exp.maybe_parse(value)
            if parsed is None:
                return False

        except Exception:
            return False
        return bool(
            isinstance(
                parsed,
                (exp.Dot, exp.Anonymous, exp.Func, exp.Null, exp.CurrentTimestamp, exp.CurrentDate, exp.CurrentTime),
            )
        )

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
                value_expr = extract_sql_object_expression(val, builder=self)
            elif isinstance(val, exp.Expression):
                value_expr = val
            elif isinstance(val, str) and self._is_column_reference(val):
                value_expr = exp.maybe_parse(val) or exp.column(val)
            else:
                column_name = col if isinstance(col, str) else str(col)
                if "." in column_name:
                    column_name = column_name.split(".")[-1]
                param_name = self._generate_unique_parameter_name(column_name)
                _, param_name = self.add_parameter(val, name=param_name)
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
