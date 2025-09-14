# ruff: noqa: PLR2004
# pyright: reportPrivateUsage=false, reportPrivateImportUsage=false
"""WHERE and HAVING clause mixins.

Provides mixins for WHERE and HAVING clause functionality with
parameter binding and various condition operators.
"""

from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

if TYPE_CHECKING:
    from sqlspec.core.statement import SQL

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import extract_column_name, parse_column_expression, parse_condition_expression
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import (
    has_expression_and_parameters,
    has_expression_and_sql,
    has_query_builder_parameters,
    has_sqlglot_expression,
    is_iterable_parameters,
)

if TYPE_CHECKING:
    from sqlspec.builder._column import ColumnExpression
    from sqlspec.protocols import SQLBuilderProtocol

__all__ = ("HavingClauseMixin", "WhereClauseMixin")


@trait
class WhereClauseMixin:
    """Mixin providing WHERE clause methods for SELECT, UPDATE, and DELETE builders."""

    __slots__ = ()

    # Type annotations for PyRight - these will be provided by the base class
    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def _create_parameterized_condition(
        self,
        column: Union[str, exp.Column],
        value: Any,
        condition_factory: "Callable[[exp.Expression, exp.Placeholder], exp.Expression]",
    ) -> exp.Expression:
        """Create a parameterized condition using the provided factory function.

        Args:
            column: Column expression
            value: Parameter value
            condition_factory: Function that creates the condition expression

        Returns:
            The created condition expression
        """
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        placeholder = exp.Placeholder(this=param_name)
        return condition_factory(col_expr, placeholder)

    def _merge_sql_object_parameters(self, sql_obj: Any) -> None:
        """Merge parameters from a SQL object into the builder.

        Args:
            sql_obj: Object with parameters attribute containing parameter mappings
        """
        if not has_expression_and_parameters(sql_obj):
            return

        builder = cast("SQLBuilderProtocol", self)
        sql_parameters = getattr(sql_obj, "parameters", {})
        for param_name, param_value in sql_parameters.items():
            unique_name = builder._generate_unique_parameter_name(param_name)
            builder.add_parameter(param_value, name=unique_name)

    def _apply_or_where(self, where_method: "Callable[..., Self]", *args: Any, **kwargs: Any) -> Self:
        """Apply a where method but use OR logic instead of AND.

        This allows reusing all where_* methods for or_where_* functionality.

        Args:
            where_method: The where method to apply (e.g., self.where_eq)
            *args: Arguments to pass to the where method
            **kwargs: Keyword arguments to pass to the where method

        Returns:
            Self with OR condition applied
        """
        # Create a temporary clone to capture the condition
        original_expr = self.get_expression()

        # Apply the where method to get the condition
        where_method(*args, **kwargs)

        # Get the last condition added by extracting it from the modified expression
        current_expr = self.get_expression()
        if isinstance(current_expr, (exp.Select, exp.Update, exp.Delete)) and original_expr != current_expr:
            last_where = current_expr.find(exp.Where)
            if last_where and last_where.this:
                condition = last_where.this
                # Restore original expression
                if original_expr is not None:
                    self.set_expression(original_expr)
                # Apply as OR
                return self.or_where(condition)

        return self

    def _handle_in_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        """Handle IN operator."""
        builder = cast("SQLBuilderProtocol", self)
        if is_iterable_parameters(value):
            placeholders = []
            for i, v in enumerate(value):
                if len(value) == 1:
                    param_name = builder._generate_unique_parameter_name(column_name)
                else:
                    param_name = builder._generate_unique_parameter_name(f"{column_name}_{i + 1}")
                _, param_name = builder.add_parameter(v, name=param_name)
                placeholders.append(exp.Placeholder(this=param_name))
            return exp.In(this=column_exp, expressions=placeholders)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        return exp.In(this=column_exp, expressions=[exp.Placeholder(this=param_name)])

    def _handle_not_in_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        """Handle NOT IN operator."""
        builder = cast("SQLBuilderProtocol", self)
        if is_iterable_parameters(value):
            placeholders = []
            for i, v in enumerate(value):
                if len(value) == 1:
                    param_name = builder._generate_unique_parameter_name(column_name)
                else:
                    param_name = builder._generate_unique_parameter_name(f"{column_name}_{i + 1}")
                _, param_name = builder.add_parameter(v, name=param_name)
                placeholders.append(exp.Placeholder(this=param_name))
            return exp.Not(this=exp.In(this=column_exp, expressions=placeholders))
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        return exp.Not(this=exp.In(this=column_exp, expressions=[exp.Placeholder(this=param_name)]))

    def _handle_is_operator(self, column_exp: exp.Expression, value: Any) -> exp.Expression:
        """Handle IS operator."""
        value_expr = exp.Null() if value is None else exp.convert(value)
        return exp.Is(this=column_exp, expression=value_expr)

    def _handle_is_not_operator(self, column_exp: exp.Expression, value: Any) -> exp.Expression:
        """Handle IS NOT operator."""
        value_expr = exp.Null() if value is None else exp.convert(value)
        return exp.Not(this=exp.Is(this=column_exp, expression=value_expr))

    def _handle_between_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        """Handle BETWEEN operator."""
        if is_iterable_parameters(value) and len(value) == 2:
            builder = cast("SQLBuilderProtocol", self)
            low, high = value
            low_param = builder._generate_unique_parameter_name(f"{column_name}_low")
            high_param = builder._generate_unique_parameter_name(f"{column_name}_high")
            _, low_param = builder.add_parameter(low, name=low_param)
            _, high_param = builder.add_parameter(high, name=high_param)
            return exp.Between(
                this=column_exp, low=exp.Placeholder(this=low_param), high=exp.Placeholder(this=high_param)
            )
        msg = f"BETWEEN operator requires a tuple of two values, got {type(value).__name__}"
        raise SQLBuilderError(msg)

    def _handle_not_between_operator(
        self, column_exp: exp.Expression, value: Any, column_name: str = "column"
    ) -> exp.Expression:
        """Handle NOT BETWEEN operator."""
        if is_iterable_parameters(value) and len(value) == 2:
            builder = cast("SQLBuilderProtocol", self)
            low, high = value
            low_param = builder._generate_unique_parameter_name(f"{column_name}_low")
            high_param = builder._generate_unique_parameter_name(f"{column_name}_high")
            _, low_param = builder.add_parameter(low, name=low_param)
            _, high_param = builder.add_parameter(high, name=high_param)
            return exp.Not(
                this=exp.Between(
                    this=column_exp, low=exp.Placeholder(this=low_param), high=exp.Placeholder(this=high_param)
                )
            )
        msg = f"NOT BETWEEN operator requires a tuple of two values, got {type(value).__name__}"
        raise SQLBuilderError(msg)

    def _process_tuple_condition(self, condition: "tuple[Any, ...]") -> exp.Expression:
        """Process tuple-based WHERE conditions."""
        if len(condition) == 2:
            # (column, value) tuple for equality
            column, value = condition
            return self._create_parameterized_condition(
                column, value, lambda col, placeholder: exp.EQ(this=col, expression=placeholder)
            )

        if len(condition) != 3:
            msg = f"Condition tuple must have 2 or 3 elements, got {len(condition)}"
            raise SQLBuilderError(msg)

        # (column, operator, value) tuple
        column_name_raw, operator, value = condition
        operator = str(operator).upper()
        column_exp = parse_column_expression(column_name_raw)
        column_name = extract_column_name(column_name_raw)

        # Simple operators that use direct parameterization
        simple_operators = {
            "=": lambda col, placeholder: exp.EQ(this=col, expression=placeholder),
            "!=": lambda col, placeholder: exp.NEQ(this=col, expression=placeholder),
            "<>": lambda col, placeholder: exp.NEQ(this=col, expression=placeholder),
            ">": lambda col, placeholder: exp.GT(this=col, expression=placeholder),
            ">=": lambda col, placeholder: exp.GTE(this=col, expression=placeholder),
            "<": lambda col, placeholder: exp.LT(this=col, expression=placeholder),
            "<=": lambda col, placeholder: exp.LTE(this=col, expression=placeholder),
            "LIKE": lambda col, placeholder: exp.Like(this=col, expression=placeholder),
            "NOT LIKE": lambda col, placeholder: exp.Not(this=exp.Like(this=col, expression=placeholder)),
        }

        if operator in simple_operators:
            return self._create_parameterized_condition(column_name_raw, value, simple_operators[operator])

        # Complex operators that need special handling
        if operator == "IN":
            return self._handle_in_operator(column_exp, value, column_name)
        if operator == "NOT IN":
            return self._handle_not_in_operator(column_exp, value, column_name)
        if operator == "IS":
            return self._handle_is_operator(column_exp, value)
        if operator == "IS NOT":
            return self._handle_is_not_operator(column_exp, value)
        if operator == "BETWEEN":
            return self._handle_between_operator(column_exp, value, column_name)
        if operator == "NOT BETWEEN":
            return self._handle_not_between_operator(column_exp, value, column_name)

        msg = f"Unsupported operator: {operator}"
        raise SQLBuilderError(msg)

    def where(
        self,
        condition: Union[
            str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", "SQL"
        ],
        *values: Any,
        operator: Optional[str] = None,
        **kwargs: Any,
    ) -> Self:
        """Add a WHERE clause to the statement.

        Args:
            condition: The condition for the WHERE clause. Can be:
                - A string condition with or without parameter placeholders
                - A string column name (when values are provided)
                - A sqlglot Expression or Condition
                - A 2-tuple (column, value) for equality comparison
                - A 3-tuple (column, operator, value) for custom comparison
            *values: Positional values for parameter binding (when condition contains placeholders or is a column name)
            operator: Operator for comparison (when condition is a column name)
            **kwargs: Named parameters for parameter binding (when condition contains named placeholders)

        Raises:
            SQLBuilderError: If the current expression is not a supported statement type.

        Returns:
            The current builder instance for method chaining.
        """
        current_expr = self.get_expression()
        if self.__class__.__name__ == "Update" and not isinstance(current_expr, exp.Update):
            msg = "Cannot add WHERE clause to non-UPDATE expression"
            raise SQLBuilderError(msg)

        builder = cast("SQLBuilderProtocol", self)
        if current_expr is None:
            msg = "Cannot add WHERE clause: expression is not initialized."
            raise SQLBuilderError(msg)

        if isinstance(current_expr, exp.Delete) and not current_expr.args.get("this"):
            msg = "WHERE clause requires a table to be set. Use from() to set the table first."
            raise SQLBuilderError(msg)

        # Handle string conditions with external parameters
        if values or kwargs:
            if not isinstance(condition, str):
                msg = "When values are provided, condition must be a string"
                raise SQLBuilderError(msg)

            # Check if condition contains parameter placeholders
            from sqlspec.core.parameters import ParameterStyle, ParameterValidator

            validator = ParameterValidator()
            param_info = validator.extract_parameters(condition)

            if param_info:
                # String condition with placeholders - create SQL object with parameters
                from sqlspec import sql as sql_factory

                # Create parameter mapping based on the detected parameter info
                param_dict = dict(kwargs)  # Start with named parameters

                # Handle positional parameters - these are ordinal-based ($1, $2, :1, :2, ?)
                positional_params = [
                    param
                    for param in param_info
                    if param.style in {ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_COLON, ParameterStyle.QMARK}
                ]

                # Map positional values to positional parameters
                if len(values) != len(positional_params):
                    msg = f"Parameter count mismatch: condition has {len(positional_params)} positional placeholders, got {len(values)} values"
                    raise SQLBuilderError(msg)

                for i, value in enumerate(values):
                    param_dict[f"param_{i}"] = value

                # Create SQL object with parameters that will be processed correctly
                condition = sql_factory.raw(condition, **param_dict)
                # Fall through to existing SQL object handling logic

            elif len(values) == 1 and not kwargs:
                # Single value - treat as column = value
                if operator is not None:
                    where_expr = self._process_tuple_condition((condition, operator, values[0]))
                else:
                    where_expr = self._process_tuple_condition((condition, values[0]))
                # Process this condition and skip the rest
                if isinstance(current_expr, (exp.Select, exp.Update, exp.Delete)):
                    updated_expr = current_expr.where(where_expr, copy=False)
                    self.set_expression(updated_expr)
                else:
                    msg = f"WHERE clause not supported for {type(current_expr).__name__}"
                    raise SQLBuilderError(msg)
                return self
            else:
                msg = f"Cannot bind parameters to condition without placeholders: {condition}"
                raise SQLBuilderError(msg)

        # Handle all condition types (including SQL objects created above)
        if isinstance(condition, str):
            where_expr = parse_condition_expression(condition)
        elif isinstance(condition, (exp.Expression, exp.Condition)):
            where_expr = condition
        elif isinstance(condition, tuple):
            where_expr = self._process_tuple_condition(condition)
        elif has_query_builder_parameters(condition):
            column_expr_obj = cast("ColumnExpression", condition)
            where_expr = column_expr_obj._expression  # pyright: ignore
        elif has_sqlglot_expression(condition):
            raw_expr = condition.sqlglot_expression  # pyright: ignore[attr-defined]
            if raw_expr is not None:
                where_expr = builder._parameterize_expression(raw_expr)
            else:
                where_expr = parse_condition_expression(str(condition))
        elif has_expression_and_sql(condition):
            # Handle SQL objects (from sql.raw with parameters)
            expression = getattr(condition, "expression", None)
            if expression is not None and isinstance(expression, exp.Expression):
                # Merge parameters from SQL object into builder
                self._merge_sql_object_parameters(condition)
                where_expr = expression
            else:
                # If expression is None, fall back to parsing the raw SQL
                sql_text = getattr(condition, "sql", "")
                # Merge parameters even when parsing raw SQL
                self._merge_sql_object_parameters(condition)
                where_expr = parse_condition_expression(sql_text)
        else:
            msg = f"Unsupported condition type: {type(condition).__name__}"
            raise SQLBuilderError(msg)

        if isinstance(current_expr, (exp.Select, exp.Update, exp.Delete)):
            updated_expr = current_expr.where(where_expr, copy=False)
            self.set_expression(updated_expr)
        else:
            msg = f"WHERE clause not supported for {type(current_expr).__name__}"
            raise SQLBuilderError(msg)
        return self

    def where_eq(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column = value clause."""
        condition = self._create_parameterized_condition(column, value, lambda col, placeholder: col.eq(placeholder))
        return self.where(condition)

    def where_neq(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column != value clause."""
        condition = self._create_parameterized_condition(column, value, lambda col, placeholder: col.neq(placeholder))
        return self.where(condition)

    def where_lt(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column < value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.LT(this=col, expression=placeholder)
        )
        return self.where(condition)

    def where_lte(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column <= value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.LTE(this=col, expression=placeholder)
        )
        return self.where(condition)

    def where_gt(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column > value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.GT(this=col, expression=placeholder)
        )
        return self.where(condition)

    def where_gte(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column >= value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.GTE(this=col, expression=placeholder)
        )
        return self.where(condition)

    def where_between(self, column: Union[str, exp.Column], low: Any, high: Any) -> Self:
        """Add WHERE column BETWEEN low AND high clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        low_param = builder._generate_unique_parameter_name(f"{column_name}_low")
        high_param = builder._generate_unique_parameter_name(f"{column_name}_high")
        _, low_param = builder.add_parameter(low, name=low_param)
        _, high_param = builder.add_parameter(high, name=high_param)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.Placeholder(this=low_param), exp.Placeholder(this=high_param))
        return self.where(condition)

    def where_like(self, column: Union[str, exp.Column], pattern: str, escape: Optional[str] = None) -> Self:
        """Add WHERE column LIKE pattern clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(pattern, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if escape is not None:
            cond = exp.Like(this=col_expr, expression=exp.Placeholder(this=param_name), escape=exp.convert(str(escape)))
        else:
            cond = col_expr.like(exp.Placeholder(this=param_name))
        condition: exp.Expression = cond
        return self.where(condition)

    def where_not_like(self, column: Union[str, exp.Column], pattern: str) -> Self:
        """Add WHERE column NOT LIKE pattern clause."""
        condition = self._create_parameterized_condition(
            column, pattern, lambda col, placeholder: col.like(placeholder).not_()
        )
        return self.where(condition)

    def where_ilike(self, column: Union[str, exp.Column], pattern: str) -> Self:
        """Add WHERE column ILIKE pattern clause."""
        condition = self._create_parameterized_condition(
            column, pattern, lambda col, placeholder: col.ilike(placeholder)
        )
        return self.where(condition)

    def where_is_null(self, column: Union[str, exp.Column]) -> Self:
        """Add WHERE column IS NULL clause."""
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.where(condition)

    def where_is_not_null(self, column: Union[str, exp.Column]) -> Self:
        """Add WHERE column IS NOT NULL clause."""
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return self.where(condition)

    def where_in(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add WHERE column IN (values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore
                sql_str = subquery.sql
                subquery_exp = exp.paren(exp.maybe_parse(sql_str, dialect=builder.dialect_name))  # pyright: ignore
                # Merge subquery parameters into parent builder with unique naming
                if hasattr(subquery, "parameters") and isinstance(subquery.parameters, dict):  # pyright: ignore[reportAttributeAccessIssue]
                    for param_name, param_value in subquery.parameters.items():  # pyright: ignore[reportAttributeAccessIssue]
                        unique_name = builder._generate_unique_parameter_name(param_name)
                        builder.add_parameter(param_value, name=unique_name)
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = col_expr.isin(subquery_exp)
            return self.where(condition)
        if not is_iterable_parameters(values) or isinstance(values, (str, bytes)):
            msg = "Unsupported type for 'values' in WHERE IN"
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        condition = col_expr.isin(*parameters)
        return self.where(condition)

    def where_not_in(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add WHERE column NOT IN (values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore

                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=builder.dialect_name))  # pyright: ignore
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.Not(this=col_expr.isin(subquery_exp))
            return self.where(condition)
        if not is_iterable_parameters(values) or isinstance(values, (str, bytes)):
            msg = "Values for where_not_in must be a non-string iterable or subquery."
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        condition = exp.Not(this=col_expr.isin(*parameters))
        return self.where(condition)

    def where_null(self, column: Union[str, exp.Column]) -> Self:
        """Add WHERE column IS NULL clause."""
        return self.where_is_null(column)

    def where_not_null(self, column: Union[str, exp.Column]) -> Self:
        """Add WHERE column IS NOT NULL clause."""
        return self.where_is_not_null(column)

    def where_exists(self, subquery: Union[str, Any]) -> Self:
        """Add WHERE EXISTS (subquery) clause."""
        builder = cast("SQLBuilderProtocol", self)
        sub_expr: exp.Expression
        if has_query_builder_parameters(subquery):
            subquery_builder_parameters: dict[str, Any] = subquery.parameters
            if subquery_builder_parameters:
                for p_name, p_value in subquery_builder_parameters.items():
                    builder.add_parameter(p_value, name=p_name)
            sub_sql_obj = subquery.build()  # pyright: ignore

            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=builder.dialect_name)  # pyright: ignore
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=builder.dialect_name)

        if sub_expr is None:
            msg = "Could not parse subquery for EXISTS"
            raise SQLBuilderError(msg)

        exists_expr = exp.Exists(this=sub_expr)
        return self.where(exists_expr)

    def where_not_exists(self, subquery: Union[str, Any]) -> Self:
        """Add WHERE NOT EXISTS (subquery) clause."""
        builder = cast("SQLBuilderProtocol", self)
        sub_expr: exp.Expression
        if has_query_builder_parameters(subquery):
            subquery_builder_parameters: dict[str, Any] = subquery.parameters
            if subquery_builder_parameters:
                for p_name, p_value in subquery_builder_parameters.items():
                    builder.add_parameter(p_value, name=p_name)
            sub_sql_obj = subquery.build()  # pyright: ignore
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=builder.dialect_name)  # pyright: ignore
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=builder.dialect_name)

        if sub_expr is None:
            msg = "Could not parse subquery for NOT EXISTS"
            raise SQLBuilderError(msg)

        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.where(not_exists_expr)

    def where_any(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add WHERE column = ANY(values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=builder.dialect_name))  # pyright: ignore
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return self.where(condition)
        if isinstance(values, str):
            try:
                parsed_expr: Optional[exp.Expression] = exp.maybe_parse(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return self.where(condition)
            except Exception:  # noqa: S110
                pass
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in WHERE ANY"
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_any_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        tuple_expr = exp.Tuple(expressions=parameters)
        condition = exp.EQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return self.where(condition)

    def where_not_any(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add WHERE column <> ANY(values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery = values.build()  # pyright: ignore
                subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=builder.dialect_name))  # pyright: ignore
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return self.where(condition)
        if isinstance(values, str):
            try:
                parsed_expr: Optional[exp.Expression] = exp.maybe_parse(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return self.where(condition)
            except Exception:  # noqa: S110
                pass
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in WHERE NOT ANY"
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_not_any_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        tuple_expr = exp.Tuple(expressions=parameters)
        condition = exp.NEQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return self.where(condition)

    def or_where(
        self,
        condition: Union[
            str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", "SQL"
        ],
        *values: Any,
        operator: Optional[str] = None,
        **kwargs: Any,
    ) -> Self:
        """Add an OR condition to the existing WHERE clause.

        Args:
            condition: The condition for the OR WHERE clause. Can be:
                - A string condition with or without parameter placeholders
                - A string column name (when values are provided)
                - A sqlglot Expression or Condition
                - A 2-tuple (column, value) for equality comparison
                - A 3-tuple (column, operator, value) for custom comparison
            *values: Positional values for parameter binding (when condition contains placeholders or is a column name)
            operator: Operator for comparison (when condition is a column name)
            **kwargs: Named parameters for parameter binding (when condition contains named placeholders)

        Raises:
            SQLBuilderError: If the current expression is not a supported statement type or no existing WHERE clause.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            msg = "Cannot add OR WHERE clause: expression is not initialized."
            raise SQLBuilderError(msg)

        # Get the existing WHERE condition
        existing_where = None
        if isinstance(builder._expression, (exp.Select, exp.Update, exp.Delete)):
            existing_where = builder._expression.find(exp.Where)
            if existing_where:
                existing_where = existing_where.this

        if existing_where is None:
            msg = "Cannot add OR WHERE clause: no existing WHERE clause found. Use where() first."
            raise SQLBuilderError(msg)

        # Process the new condition (reuse existing logic from where method)
        new_condition = self._process_where_condition(condition, values, operator, kwargs)

        # Combine with existing WHERE using OR
        or_condition = exp.Or(this=existing_where, expression=new_condition)

        # Update the WHERE clause by modifying the existing WHERE node
        if isinstance(builder._expression, (exp.Select, exp.Update, exp.Delete)):
            where_node = builder._expression.find(exp.Where)
            if where_node:
                where_node.set("this", or_condition)
            else:
                # This shouldn't happen since we checked for existing_where above
                builder._expression = builder._expression.where(or_condition, copy=False)
        else:
            msg = f"OR WHERE clause not supported for {type(builder._expression).__name__}"
            raise SQLBuilderError(msg)

        return self

    def where_or(self, *conditions: Union[str, "tuple[Any, ...]", exp.Expression]) -> Self:
        """Combine multiple conditions with OR logic.

        Args:
            *conditions: Multiple conditions to combine with OR. Each condition can be:
                - A string condition
                - A 2-tuple (column, value) for equality comparison
                - A 3-tuple (column, operator, value) for custom comparison
                - A sqlglot Expression or Condition

        Raises:
            SQLBuilderError: If no conditions provided or current expression not supported.

        Returns:
            The current builder instance for method chaining.

        Examples:
            query.where_or(
                ("name", "John"),
                ("email", "john@email.com"),
                "age > 25"
            )
            # Produces: WHERE (name = :name OR email = :email OR age > 25)
        """
        if not conditions:
            msg = "where_or() requires at least one condition"
            raise SQLBuilderError(msg)

        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            msg = "Cannot add WHERE OR clause: expression is not initialized."
            raise SQLBuilderError(msg)

        # Process all conditions
        processed_conditions = []
        for condition in conditions:
            processed_condition = self._process_where_condition(condition, (), None, {})
            processed_conditions.append(processed_condition)

        # Create OR expression from all conditions
        or_condition = self._create_or_expression(processed_conditions)

        # Apply the OR condition
        if isinstance(builder._expression, (exp.Select, exp.Update, exp.Delete)):
            builder._expression = builder._expression.where(or_condition, copy=False)
        else:
            msg = f"WHERE OR clause not supported for {type(builder._expression).__name__}"
            raise SQLBuilderError(msg)

        return self

    def _process_where_condition(
        self,
        condition: Union[
            str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression", "SQL"
        ],
        values: tuple[Any, ...],
        operator: Optional[str],
        kwargs: dict[str, Any],
    ) -> exp.Expression:
        """Process a WHERE condition into a sqlglot expression.

        This is extracted from the where() method to be reusable by OR methods.

        Args:
            condition: The condition to process
            values: Positional values for parameter binding
            operator: Operator for comparison
            kwargs: Named parameters for parameter binding

        Returns:
            Processed sqlglot expression
        """
        builder = cast("SQLBuilderProtocol", self)

        # Handle string conditions with external parameters
        if values or kwargs:
            if not isinstance(condition, str):
                msg = "When values are provided, condition must be a string"
                raise SQLBuilderError(msg)

            # Check if condition contains parameter placeholders
            from sqlspec.core.parameters import ParameterStyle, ParameterValidator

            validator = ParameterValidator()
            param_info = validator.extract_parameters(condition)

            if param_info:
                # String condition with placeholders - create SQL object with parameters
                from sqlspec import sql as sql_factory

                # Create parameter mapping based on the detected parameter info
                param_dict = dict(kwargs)  # Start with named parameters

                # Handle positional parameters - these are ordinal-based ($1, $2, :1, :2, ?)
                positional_params = [
                    param
                    for param in param_info
                    if param.style in {ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_COLON, ParameterStyle.QMARK}
                ]

                # Map positional values to positional parameters
                if len(values) != len(positional_params):
                    msg = f"Parameter count mismatch: condition has {len(positional_params)} positional placeholders, got {len(values)} values"
                    raise SQLBuilderError(msg)

                for i, value in enumerate(values):
                    param_dict[f"param_{i}"] = value

                # Create SQL object with parameters that will be processed correctly
                condition = sql_factory.raw(condition, **param_dict)
                # Fall through to existing SQL object handling logic

            elif len(values) == 1 and not kwargs:
                # Single value - treat as column = value
                if operator is not None:
                    return self._process_tuple_condition((condition, operator, values[0]))
                return self._process_tuple_condition((condition, values[0]))
            else:
                msg = f"Cannot bind parameters to condition without placeholders: {condition}"
                raise SQLBuilderError(msg)

        # Handle all condition types (including SQL objects created above)
        if isinstance(condition, str):
            return parse_condition_expression(condition)
        if isinstance(condition, (exp.Expression, exp.Condition)):
            return condition
        if isinstance(condition, tuple):
            return self._process_tuple_condition(condition)
        if has_query_builder_parameters(condition):
            column_expr_obj = cast("ColumnExpression", condition)
            return column_expr_obj._expression  # pyright: ignore
        if has_sqlglot_expression(condition):
            raw_expr = condition.sqlglot_expression  # pyright: ignore[attr-defined]
            if raw_expr is not None:
                return builder._parameterize_expression(raw_expr)
            return parse_condition_expression(str(condition))
        if hasattr(condition, "expression") and hasattr(condition, "sql"):
            # Handle SQL objects (from sql.raw with parameters)
            expression = getattr(condition, "expression", None)
            if expression is not None and isinstance(expression, exp.Expression):
                # Merge parameters from SQL object into builder
                if hasattr(condition, "parameters") and hasattr(builder, "add_parameter"):
                    sql_parameters = getattr(condition, "parameters", {})
                    for param_name, param_value in sql_parameters.items():
                        unique_name = builder._generate_unique_parameter_name(param_name)
                        builder.add_parameter(param_value, name=unique_name)
                return cast("exp.Expression", expression)
            # If expression is None, fall back to parsing the raw SQL
            sql_text = getattr(condition, "sql", "")
            # Merge parameters even when parsing raw SQL
            if hasattr(condition, "parameters") and hasattr(builder, "add_parameter"):
                sql_parameters = getattr(condition, "parameters", {})
                for param_name, param_value in sql_parameters.items():
                    unique_name = builder._generate_unique_parameter_name(param_name)
                    builder.add_parameter(param_value, name=unique_name)
            return parse_condition_expression(sql_text)
        msg = f"Unsupported condition type: {type(condition).__name__}"
        raise SQLBuilderError(msg)

    def _create_or_expression(self, conditions: list[exp.Expression]) -> exp.Expression:
        """Create OR expression from multiple conditions.

        Args:
            conditions: List of sqlglot expressions to combine with OR

        Returns:
            Combined OR expression, or single condition if only one provided
        """
        if len(conditions) == 1:
            return conditions[0]

        result = conditions[0]
        for condition in conditions[1:]:
            result = exp.Or(this=result, expression=condition)
        return result

    # OR helper methods for consistency with existing where_* methods
    def or_where_eq(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add OR column = value clause."""
        condition = self._create_parameterized_condition(column, value, lambda col, placeholder: col.eq(placeholder))
        return self.or_where(condition)

    def or_where_neq(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add OR column != value clause."""
        condition = self._create_parameterized_condition(column, value, lambda col, placeholder: col.neq(placeholder))
        return self.or_where(condition)

    def or_where_in(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add OR column IN (values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery_builder_parameters: dict[str, Any] = values.parameters  # pyright: ignore
                param_mapping = {}
                if subquery_builder_parameters:
                    for p_name, p_value in subquery_builder_parameters.items():
                        unique_name = builder._generate_unique_parameter_name(p_name)
                        param_mapping[p_name] = unique_name
                        builder.add_parameter(p_value, name=unique_name)
                subquery = values.build()  # pyright: ignore
                subquery_parsed = exp.maybe_parse(subquery.sql, dialect=builder.dialect_name)  # pyright: ignore
                if param_mapping and subquery_parsed:
                    subquery_parsed = cast("Any", builder)._update_placeholders_in_expression(
                        subquery_parsed, param_mapping
                    )
                subquery_exp = (
                    exp.paren(subquery_parsed)
                    if subquery_parsed
                    else exp.paren(exp.maybe_parse(subquery.sql, dialect=builder.dialect_name))  # pyright: ignore
                )  # pyright: ignore
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = col_expr.isin(subquery_exp)
            return self.or_where(condition)
        if not is_iterable_parameters(values) or isinstance(values, (str, bytes)):
            msg = "Unsupported type for 'values' in OR WHERE IN"
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        condition = col_expr.isin(*parameters)
        return self.or_where(condition)

    def or_where_like(self, column: Union[str, exp.Column], pattern: str, escape: Optional[str] = None) -> Self:
        """Add OR column LIKE pattern clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(pattern, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if escape is not None:
            cond = exp.Like(this=col_expr, expression=exp.Placeholder(this=param_name), escape=exp.convert(str(escape)))
        else:
            cond = col_expr.like(exp.Placeholder(this=param_name))
        condition: exp.Expression = cond
        return self.or_where(condition)

    def or_where_is_null(self, column: Union[str, exp.Column]) -> Self:
        """Add OR column IS NULL clause."""
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null())
        return self.or_where(condition)

    def or_where_is_not_null(self, column: Union[str, exp.Column]) -> Self:
        """Add OR column IS NOT NULL clause."""
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.is_(exp.null()).not_()
        return self.or_where(condition)

    def or_where_lt(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add OR column < value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.LT(this=col, expression=placeholder)
        )
        return self.or_where(condition)

    def or_where_lte(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add OR column <= value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.LTE(this=col, expression=placeholder)
        )
        return self.or_where(condition)

    def or_where_gt(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add OR column > value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.GT(this=col, expression=placeholder)
        )
        return self.or_where(condition)

    def or_where_gte(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add OR column >= value clause."""
        condition = self._create_parameterized_condition(
            column, value, lambda col, placeholder: exp.GTE(this=col, expression=placeholder)
        )
        return self.or_where(condition)

    def or_where_between(self, column: Union[str, exp.Column], low: Any, high: Any) -> Self:
        """Add OR column BETWEEN low AND high clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = extract_column_name(column)
        low_param = builder._generate_unique_parameter_name(f"{column_name}_low")
        high_param = builder._generate_unique_parameter_name(f"{column_name}_high")
        _, low_param = builder.add_parameter(low, name=low_param)
        _, high_param = builder.add_parameter(high, name=high_param)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.between(exp.Placeholder(this=low_param), exp.Placeholder(this=high_param))
        return self.or_where(condition)

    def or_where_not_like(self, column: Union[str, exp.Column], pattern: str) -> Self:
        """Add OR column NOT LIKE pattern clause."""
        condition = self._create_parameterized_condition(
            column, pattern, lambda col, placeholder: col.like(placeholder).not_()
        )
        return self.or_where(condition)

    def or_where_not_in(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add OR column NOT IN (values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery_builder_parameters: dict[str, Any] = values.parameters  # pyright: ignore
                param_mapping = {}
                if subquery_builder_parameters:
                    for p_name, p_value in subquery_builder_parameters.items():
                        unique_name = builder._generate_unique_parameter_name(p_name)
                        param_mapping[p_name] = unique_name
                        builder.add_parameter(p_value, name=unique_name)
                subquery = values.build()  # pyright: ignore
                subquery_parsed = exp.maybe_parse(subquery.sql, dialect=builder.dialect_name)  # pyright: ignore
                if param_mapping and subquery_parsed:
                    subquery_parsed = cast("Any", builder)._update_placeholders_in_expression(
                        subquery_parsed, param_mapping
                    )
                subquery_exp = (
                    exp.paren(subquery_parsed)
                    if subquery_parsed
                    else exp.paren(exp.maybe_parse(subquery.sql, dialect=builder.dialect_name))  # pyright: ignore
                )  # pyright: ignore
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.Not(this=col_expr.isin(subquery_exp))
            return self.or_where(condition)
        if not is_iterable_parameters(values) or isinstance(values, (str, bytes)):
            msg = "Values for or_where_not_in must be a non-string iterable or subquery."
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        condition = exp.Not(this=col_expr.isin(*parameters))
        return self.or_where(condition)

    def or_where_ilike(self, column: Union[str, exp.Column], pattern: str) -> Self:
        """Add OR column ILIKE pattern clause."""
        condition = self._create_parameterized_condition(
            column, pattern, lambda col, placeholder: col.ilike(placeholder)
        )
        return self.or_where(condition)

    def or_where_null(self, column: Union[str, exp.Column]) -> Self:
        """Add OR column IS NULL clause."""
        return self.or_where_is_null(column)

    def or_where_not_null(self, column: Union[str, exp.Column]) -> Self:
        """Add OR column IS NOT NULL clause."""
        return self.or_where_is_not_null(column)

    def or_where_exists(self, subquery: Union[str, Any]) -> Self:
        """Add OR EXISTS (subquery) clause."""
        builder = cast("SQLBuilderProtocol", self)
        sub_expr: exp.Expression
        if has_query_builder_parameters(subquery):
            subquery_builder_parameters: dict[str, Any] = subquery.parameters
            param_mapping = {}
            if subquery_builder_parameters:
                for p_name, p_value in subquery_builder_parameters.items():
                    unique_name = builder._generate_unique_parameter_name(p_name)
                    param_mapping[p_name] = unique_name
                    builder.add_parameter(p_value, name=unique_name)
            sub_sql_obj = subquery.build()  # pyright: ignore
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=builder.dialect_name)  # pyright: ignore
            # Update placeholders to use unique parameter names
            if param_mapping and sub_expr:
                sub_expr = cast("Any", builder)._update_placeholders_in_expression(sub_expr, param_mapping)
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=builder.dialect_name)

        if sub_expr is None:
            msg = "Could not parse subquery for OR EXISTS"
            raise SQLBuilderError(msg)

        exists_expr = exp.Exists(this=sub_expr)
        return self.or_where(exists_expr)

    def or_where_not_exists(self, subquery: Union[str, Any]) -> Self:
        """Add OR NOT EXISTS (subquery) clause."""
        builder = cast("SQLBuilderProtocol", self)
        sub_expr: exp.Expression
        if has_query_builder_parameters(subquery):
            subquery_builder_parameters: dict[str, Any] = subquery.parameters
            param_mapping = {}
            if subquery_builder_parameters:
                for p_name, p_value in subquery_builder_parameters.items():
                    unique_name = builder._generate_unique_parameter_name(p_name)
                    param_mapping[p_name] = unique_name
                    builder.add_parameter(p_value, name=unique_name)
            sub_sql_obj = subquery.build()  # pyright: ignore
            sub_expr = exp.maybe_parse(sub_sql_obj.sql, dialect=builder.dialect_name)  # pyright: ignore
            # Update placeholders to use unique parameter names
            if param_mapping and sub_expr:
                sub_expr = cast("Any", builder)._update_placeholders_in_expression(sub_expr, param_mapping)
        else:
            sub_expr = exp.maybe_parse(str(subquery), dialect=builder.dialect_name)

        if sub_expr is None:
            msg = "Could not parse subquery for OR NOT EXISTS"
            raise SQLBuilderError(msg)

        not_exists_expr = exp.Not(this=exp.Exists(this=sub_expr))
        return self.or_where(not_exists_expr)

    def or_where_any(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add OR column = ANY(values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery_builder_parameters: dict[str, Any] = values.parameters  # pyright: ignore
                param_mapping = {}
                if subquery_builder_parameters:
                    for p_name, p_value in subquery_builder_parameters.items():
                        unique_name = builder._generate_unique_parameter_name(p_name)
                        param_mapping[p_name] = unique_name
                        builder.add_parameter(p_value, name=unique_name)
                subquery = values.build()  # pyright: ignore
                subquery_parsed = exp.maybe_parse(subquery.sql, dialect=builder.dialect_name)  # pyright: ignore
                if param_mapping and subquery_parsed:
                    subquery_parsed = cast("Any", builder)._update_placeholders_in_expression(
                        subquery_parsed, param_mapping
                    )
                subquery_exp = (
                    exp.paren(subquery_parsed)
                    if subquery_parsed
                    else exp.paren(exp.maybe_parse(subquery.sql, dialect=builder.dialect_name))  # pyright: ignore
                )  # pyright: ignore
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return self.or_where(condition)
        if isinstance(values, str):
            try:
                parsed_expr: Optional[exp.Expression] = exp.maybe_parse(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.EQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return self.or_where(condition)
            except Exception:  # noqa: S110
                pass
            msg = "Unsupported type for 'values' in OR WHERE ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in OR WHERE ANY"
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_any_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        tuple_expr = exp.Tuple(expressions=parameters)
        condition = exp.EQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return self.or_where(condition)

    def or_where_not_any(self, column: Union[str, exp.Column], values: Any) -> Self:
        """Add OR column <> ANY(values) clause."""
        builder = cast("SQLBuilderProtocol", self)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        if has_query_builder_parameters(values) or isinstance(values, exp.Expression):
            subquery_exp: exp.Expression
            if has_query_builder_parameters(values):
                subquery_builder_parameters: dict[str, Any] = values.parameters  # pyright: ignore
                param_mapping = {}
                if subquery_builder_parameters:
                    for p_name, p_value in subquery_builder_parameters.items():
                        unique_name = builder._generate_unique_parameter_name(p_name)
                        param_mapping[p_name] = unique_name
                        builder.add_parameter(p_value, name=unique_name)
                subquery = values.build()  # pyright: ignore
                subquery_parsed = exp.maybe_parse(subquery.sql, dialect=builder.dialect_name)  # pyright: ignore
                if param_mapping and subquery_parsed:
                    subquery_parsed = cast("Any", builder)._update_placeholders_in_expression(
                        subquery_parsed, param_mapping
                    )
                subquery_exp = (
                    exp.paren(subquery_parsed)
                    if subquery_parsed
                    else exp.paren(exp.maybe_parse(subquery.sql, dialect=builder.dialect_name))  # pyright: ignore
                )  # pyright: ignore
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
            return self.or_where(condition)
        if isinstance(values, str):
            try:
                parsed_expr: Optional[exp.Expression] = exp.maybe_parse(values)
                if isinstance(parsed_expr, (exp.Select, exp.Union, exp.Subquery)):
                    subquery_exp = exp.paren(parsed_expr)
                    condition = exp.NEQ(this=col_expr, expression=exp.Any(this=subquery_exp))
                    return self.or_where(condition)
            except Exception:  # noqa: S110
                pass
            msg = "Unsupported type for 'values' in OR WHERE NOT ANY"
            raise SQLBuilderError(msg)
        if not is_iterable_parameters(values) or isinstance(values, bytes):
            msg = "Unsupported type for 'values' in OR WHERE NOT ANY"
            raise SQLBuilderError(msg)
        column_name = extract_column_name(column)
        parameters = []
        for i, v in enumerate(values):
            if len(values) == 1:
                param_name = builder._generate_unique_parameter_name(column_name)
            else:
                param_name = builder._generate_unique_parameter_name(f"{column_name}_not_any_{i + 1}")
            _, param_name = builder.add_parameter(v, name=param_name)
            parameters.append(exp.Placeholder(this=param_name))
        tuple_expr = exp.Tuple(expressions=parameters)
        condition = exp.NEQ(this=col_expr, expression=exp.Any(this=tuple_expr))
        return self.or_where(condition)


@trait
class HavingClauseMixin:
    """Mixin providing HAVING clause for SELECT builders."""

    __slots__ = ()

    # Type annotations for PyRight - these will be provided by the base class
    def get_expression(self) -> Optional[exp.Expression]: ...
    def set_expression(self, expression: exp.Expression) -> None: ...

    def having(self, condition: Union[str, exp.Expression]) -> Self:
        """Add HAVING clause.

        Args:
            condition: The condition for the HAVING clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        current_expr = self.get_expression()
        if current_expr is None:
            self.set_expression(exp.Select())
            current_expr = self.get_expression()

        if not isinstance(current_expr, exp.Select):
            msg = "Cannot add HAVING to a non-SELECT expression."
            raise SQLBuilderError(msg)
        having_expr = exp.condition(condition) if isinstance(condition, str) else condition
        updated_expr = current_expr.having(having_expr, copy=False)
        self.set_expression(updated_expr)
        return self
