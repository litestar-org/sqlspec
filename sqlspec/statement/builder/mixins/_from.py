from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder._parsing_utils import parse_table_expression
from sqlspec.typing import is_expression

if TYPE_CHECKING:
    from sqlspec.statement.builder.protocols import BuilderProtocol

__all__ = ("FromClauseMixin",)


class FromClauseMixin:
    """Mixin providing FROM clause for SELECT builders."""

    def from_(self, table: Union[str, exp.Expression, Any], alias: Optional[str] = None) -> Self:
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
            msg = "FROM clause is only supported for SELECT statements."
            raise SQLBuilderError(msg)
        from_expr: exp.Expression
        if isinstance(table, str):
            from_expr = parse_table_expression(table, alias)
        elif is_expression(table):
            # Direct sqlglot expression - use as is
            from_expr = exp.alias_(table, alias) if alias else table
        elif hasattr(table, "build"):
            # Query builder with build() method
            subquery = table.build()  # pyright: ignore
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
        return cast("Self", builder)
