from typing import Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("CommonTableExpressionMixin",)


class CommonTableExpressionMixin:
    """Mixin providing WITH clause (Common Table Expressions) support for SQL builders."""

    _expression: Optional[exp.Expression] = None

    def with_(
        self,
        name: str,
        query: Union[Any, str],
        recursive: bool = False,
        columns: Optional[list[str]] = None,
    ) -> Self:
        """Add WITH clause (Common Table Expression).

        Args:
            name: The name of the CTE.
            query: The query for the CTE (builder instance or SQL string).
            recursive: Whether this is a recursive CTE.
            columns: Optional column names for the CTE.

        Raises:
            SQLBuilderError: If the query type is unsupported.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            msg = "Cannot add WITH clause: expression not initialized."
            raise SQLBuilderError(msg)

        if not hasattr(self._expression, "with_") and not isinstance(
            self._expression, (exp.Select, exp.Insert, exp.Update, exp.Delete)
        ):
            msg = f"Cannot add WITH clause to {type(self._expression).__name__} expression."
            raise SQLBuilderError(msg)

        cte_expr: Optional[exp.Expression] = None
        if hasattr(query, "build"):
            # Query is a builder instance
            built_query = query.build()  # pyright: ignore
            cte_sql = built_query.sql
            cte_expr = exp.maybe_parse(cte_sql, dialect=getattr(self, "dialect", None))

            # Merge parameters
            if hasattr(self, "add_parameter"):
                for param_name, param_value in getattr(built_query, "parameters", {}).items():
                    self.add_parameter(param_value, name=param_name)  # pyright: ignore
        elif isinstance(query, str):
            cte_expr = exp.maybe_parse(query, dialect=getattr(self, "dialect", None))
        elif isinstance(query, exp.Expression):
            cte_expr = query

        if not cte_expr:
            msg = f"Could not parse CTE query: {query}"
            raise SQLBuilderError(msg)

        cte_alias_expr = exp.alias_(cte_expr, name)
        if columns:
            cte_alias_expr = exp.alias_(cte_expr, name, table=columns)

        # Different handling for different expression types
        if hasattr(self._expression, "with_"):
            existing_with = self._expression.args.get("with")  # pyright: ignore
            if existing_with:
                existing_with.expressions.append(cte_alias_expr)
                if recursive:
                    existing_with.set("recursive", recursive)
            else:
                self._expression = self._expression.with_(  # pyright: ignore
                    cte_alias_expr, as_=cte_alias_expr.alias, copy=False
                )
                if recursive:
                    with_clause = self._expression.find(exp.With)
                    if with_clause:
                        with_clause.set("recursive", recursive)
        else:
            # Store CTEs for later application during build
            if not hasattr(self, "_with_ctes"):
                setattr(self, "_with_ctes", {})
            self._with_ctes[name] = exp.CTE(this=cte_expr, alias=exp.to_table(name))  # type: ignore[attr-defined]

        return self
