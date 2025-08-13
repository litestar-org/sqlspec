# ruff: noqa: PLR2004
"""Consolidated WHERE and HAVING clause mixins."""

from typing import TYPE_CHECKING, Any, Optional, Union, cast

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._parsing_utils import parse_column_expression, parse_condition_expression
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.type_guards import has_query_builder_parameters, has_sqlglot_expression, is_iterable_parameters


def _extract_column_name(column: Union[str, exp.Column]) -> str:
    """Extract column name from column expression for parameter naming.

    Args:
        column: Column expression (string or SQLGlot Column)

    Returns:
        Column name as string for use as parameter name
    """
    if isinstance(column, str):
        # Handle simple column names and table.column references
        if "." in column:
            return column.split(".")[-1]  # Return just the column part
        return column
    if isinstance(column, exp.Column):
        # Extract the column name from SQLGlot Column expression
        try:
            return str(column.this.this)
        except AttributeError:
            return str(column.this) if column.this else "column"
    return "column"


if TYPE_CHECKING:
    from sqlspec.builder._column import ColumnExpression
    from sqlspec.protocols import SQLBuilderProtocol

__all__ = ("HavingClauseMixin", "WhereClauseMixin")


@trait
class WhereClauseMixin:
    """Mixin providing WHERE clause methods for SELECT, UPDATE, and DELETE builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

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

    def _process_tuple_condition(self, condition: tuple) -> exp.Expression:
        """Process tuple-based WHERE conditions."""
        builder = cast("SQLBuilderProtocol", self)
        column_name_raw = str(condition[0])
        column_exp = parse_column_expression(column_name_raw)
        column_name = _extract_column_name(column_name_raw)

        if len(condition) == 2:
            # (column, value) tuple for equality
            value = condition[1]
            param_name = builder._generate_unique_parameter_name(column_name)
            _, param_name = builder.add_parameter(value, name=param_name)
            return exp.EQ(this=column_exp, expression=exp.Placeholder(this=param_name))

        if len(condition) == 3:
            # (column, operator, value) tuple
            operator = str(condition[1]).upper()
            value = condition[2]

            if operator == "=":
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.EQ(this=column_exp, expression=exp.Placeholder(this=param_name))
            if operator in {"!=", "<>"}:
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.NEQ(this=column_exp, expression=exp.Placeholder(this=param_name))
            if operator == ">":
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.GT(this=column_exp, expression=exp.Placeholder(this=param_name))
            if operator == ">=":
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.GTE(this=column_exp, expression=exp.Placeholder(this=param_name))
            if operator == "<":
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.LT(this=column_exp, expression=exp.Placeholder(this=param_name))
            if operator == "<=":
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.LTE(this=column_exp, expression=exp.Placeholder(this=param_name))
            if operator == "LIKE":
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.Like(this=column_exp, expression=exp.Placeholder(this=param_name))
            if operator == "NOT LIKE":
                param_name = builder._generate_unique_parameter_name(column_name)
                _, param_name = builder.add_parameter(value, name=param_name)
                return exp.Not(this=exp.Like(this=column_exp, expression=exp.Placeholder(this=param_name)))

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

        msg = f"Condition tuple must have 2 or 3 elements, got {len(condition)}"
        raise SQLBuilderError(msg)

    def where(
        self,
        condition: Union[str, exp.Expression, exp.Condition, tuple[str, Any], tuple[str, str, Any], "ColumnExpression"],
        value: Optional[Any] = None,
        operator: Optional[str] = None,
    ) -> Self:
        """Add a WHERE clause to the statement.

        Args:
            condition: The condition for the WHERE clause. Can be:
                - A string condition (when value is None)
                - A string column name (when value is provided)
                - A sqlglot Expression or Condition
                - A 2-tuple (column, value) for equality comparison
                - A 3-tuple (column, operator, value) for custom comparison
            value: Value for comparison (when condition is a column name)
            operator: Operator for comparison (when both condition and value provided)

        Raises:
            SQLBuilderError: If the current expression is not a supported statement type.

        Returns:
            The current builder instance for method chaining.
        """
        if self.__class__.__name__ == "Update" and not isinstance(self._expression, exp.Update):
            msg = "Cannot add WHERE clause to non-UPDATE expression"
            raise SQLBuilderError(msg)

        builder = cast("SQLBuilderProtocol", self)
        if builder._expression is None:
            msg = "Cannot add WHERE clause: expression is not initialized."
            raise SQLBuilderError(msg)

        if isinstance(builder._expression, exp.Delete) and not builder._expression.args.get("this"):
            msg = "WHERE clause requires a table to be set. Use from() to set the table first."
            raise SQLBuilderError(msg)

        if value is not None:
            if not isinstance(condition, str):
                msg = "When value is provided, condition must be a column name (string)"
                raise SQLBuilderError(msg)

            if operator is not None:
                where_expr = self._process_tuple_condition((condition, operator, value))
            else:
                where_expr = self._process_tuple_condition((condition, value))
        elif isinstance(condition, str):
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
        else:
            msg = f"Unsupported condition type: {type(condition).__name__}"
            raise SQLBuilderError(msg)

        if isinstance(builder._expression, (exp.Select, exp.Update, exp.Delete)):
            builder._expression = builder._expression.where(where_expr, copy=False)
        else:
            msg = f"WHERE clause not supported for {type(builder._expression).__name__}"
            raise SQLBuilderError(msg)
        return self

    def where_eq(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column = value clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.eq(exp.Placeholder(this=param_name))
        return self.where(condition)

    def where_neq(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column != value clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.neq(exp.Placeholder(this=param_name))
        return self.where(condition)

    def where_lt(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column < value clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.LT(this=col_expr, expression=exp.Placeholder(this=param_name))
        return self.where(condition)

    def where_lte(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column <= value clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.LTE(this=col_expr, expression=exp.Placeholder(this=param_name))
        return self.where(condition)

    def where_gt(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column > value clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.GT(this=col_expr, expression=exp.Placeholder(this=param_name))
        return self.where(condition)

    def where_gte(self, column: Union[str, exp.Column], value: Any) -> Self:
        """Add WHERE column >= value clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(value, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = exp.GTE(this=col_expr, expression=exp.Placeholder(this=param_name))
        return self.where(condition)

    def where_between(self, column: Union[str, exp.Column], low: Any, high: Any) -> Self:
        """Add WHERE column BETWEEN low AND high clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
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
        column_name = _extract_column_name(column)
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
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(pattern, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.like(exp.Placeholder(this=param_name)).not_()
        return self.where(condition)

    def where_ilike(self, column: Union[str, exp.Column], pattern: str) -> Self:
        """Add WHERE column ILIKE pattern clause."""
        builder = cast("SQLBuilderProtocol", self)
        column_name = _extract_column_name(column)
        param_name = builder._generate_unique_parameter_name(column_name)
        _, param_name = builder.add_parameter(pattern, name=param_name)
        col_expr = parse_column_expression(column) if not isinstance(column, exp.Column) else column
        condition: exp.Expression = col_expr.ilike(exp.Placeholder(this=param_name))
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
                # Merge subquery parameters into parent builder
                if hasattr(subquery, "parameters") and isinstance(subquery.parameters, dict):  # pyright: ignore[reportAttributeAccessIssue]
                    for param_name, param_value in subquery.parameters.items():  # pyright: ignore[reportAttributeAccessIssue]
                        builder.add_parameter(param_value, name=param_name)
            else:
                subquery_exp = values  # type: ignore[assignment]
            condition = col_expr.isin(subquery_exp)
            return self.where(condition)
        if not is_iterable_parameters(values) or isinstance(values, (str, bytes)):
            msg = "Unsupported type for 'values' in WHERE IN"
            raise SQLBuilderError(msg)
        column_name = _extract_column_name(column)
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
        column_name = _extract_column_name(column)
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
        column_name = _extract_column_name(column)
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
        column_name = _extract_column_name(column)
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


@trait
class HavingClauseMixin:
    """Mixin providing HAVING clause for SELECT builders."""

    __slots__ = ()

    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    def having(self, condition: Union[str, exp.Expression]) -> Self:
        """Add HAVING clause.

        Args:
            condition: The condition for the HAVING clause.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add HAVING to a non-SELECT expression."
            raise SQLBuilderError(msg)
        having_expr = exp.condition(condition) if isinstance(condition, str) else condition
        self._expression = self._expression.having(having_expr, copy=False)
        return self
