from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.typing import is_expression

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("FromClauseMixin",)


def _parse_table_and_alias(table_str: str, explicit_alias: Optional[str] = None) -> exp.Table:
    """Parse a table string that may contain an alias using SQLGlot's parser.

    Args:
        table_str: Table string like 'users' or 'users u' or 'users AS u'
        explicit_alias: Explicit alias to use (overrides any alias in table_str)

    Returns:
        exp.Table: Table expression with proper alias handling
    """
    if explicit_alias:
        # If explicit alias provided, use table_str as table name only
        return exp.table_(table_str, alias=explicit_alias)

    # Use SQLGlot's parser to handle table expressions with aliases
    try:
        import sqlglot

        # Parse as FROM clause and extract the table
        parsed = sqlglot.parse_one(f"FROM {table_str}")
        table_expr = parsed.find(exp.Table)
        if table_expr:
            return table_expr
    except Exception:
        # Fallback to basic table creation if parsing fails
        pass

    # Fallback: just table name
    return exp.table_(table_str)


class FromClauseMixin:
    """Mixin providing FROM clause for SELECT builders."""

    def from_(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Any:
        """Add FROM clause.

        Args:
            table: The table name, expression, or subquery to select from.
            alias: Optional alias for the table.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the table type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        builder = cast("BuilderProtocol", self)
        if builder._expression is None:
            builder._expression = exp.Select()
        if not isinstance(builder._expression, exp.Select):
            msg = "Cannot add from to a non-SELECT expression."
            raise SQLBuilderError(msg)
        from_expr: exp.Expression
        if isinstance(table, str):
            from_expr = _parse_table_and_alias(table, alias)
        elif is_expression(table):
            subquery = table.build()  # type: ignore[attr-defined]
            subquery_exp = exp.paren(exp.maybe_parse(subquery.sql, dialect=getattr(builder, "dialect", None)))
            from_expr = exp.alias_(subquery_exp, alias) if alias else subquery_exp
            current_params = getattr(builder, "_parameters", None)
            merged_params = getattr(type(builder), "ParameterConverter", None)
            if merged_params:
                merged_params = merged_params.merge_parameters(
                    parameters=subquery.parameters,
                    args=current_params if isinstance(current_params, list) else None,
                    kwargs=current_params if isinstance(current_params, dict) else {},
                )
                setattr(builder, "_parameters", merged_params)
        else:
            from_expr = table
        builder._expression = builder._expression.from_(from_expr, copy=False)
        return builder
