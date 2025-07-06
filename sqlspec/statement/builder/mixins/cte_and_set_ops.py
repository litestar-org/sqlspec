"""CTE (Common Table Expression) and Set Operations mixins for SQL builders."""

from typing import Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("CommonTableExpressionMixin", "SetOperationMixin")


class CommonTableExpressionMixin:
    """Mixin providing WITH clause (Common Table Expressions) support for SQL builders."""

    _expression: Optional[exp.Expression] = None

    def with_(
        self, name: str, query: Union[Any, str], recursive: bool = False, columns: Optional[list[str]] = None
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
        if hasattr(query, "to_statement"):
            # Query is a builder instance
            built_query = query.to_statement()  # pyright: ignore
            cte_sql = built_query.to_sql()
            cte_expr = exp.maybe_parse(cte_sql, dialect=getattr(self, "dialect", None))

            # Merge parameters
            if hasattr(self, "add_parameter"):
                parameters = getattr(built_query, "parameters", None) or {}
                for param_name, param_value in parameters.items():
                    self.add_parameter(param_value, name=param_name)  # pyright: ignore
        elif isinstance(query, str):
            cte_expr = exp.maybe_parse(query, dialect=getattr(self, "dialect", None))
        elif isinstance(query, exp.Expression):
            cte_expr = query

        if not cte_expr:
            msg = f"Could not parse CTE query: {query}"
            raise SQLBuilderError(msg)

        if columns:
            # CTE with explicit column list: name(col1, col2, ...)
            cte_alias_expr = exp.alias_(cte_expr, name, table=[exp.to_identifier(col) for col in columns])
        else:
            # Simple CTE alias: name
            cte_alias_expr = exp.alias_(cte_expr, name)

        # Different handling for different expression types
        if hasattr(self._expression, "with_"):
            existing_with = self._expression.args.get("with")  # pyright: ignore
            if existing_with:
                existing_with.expressions.append(cte_alias_expr)
                if recursive:
                    existing_with.set("recursive", recursive)
            else:
                self._expression = self._expression.with_(cte_alias_expr, as_=name, copy=False)  # pyright: ignore
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


class SetOperationMixin:
    """Mixin providing set operations (UNION, INTERSECT, EXCEPT) for SELECT builders."""

    _expression: Any = None
    _parameters: dict[str, Any] = {}
    dialect: Any = None

    def union(self, other: Any, all_: bool = False) -> Self:
        """Combine this query with another using UNION.

        Args:
            other: Another SelectBuilder or compatible builder to union with.
            all_: If True, use UNION ALL instead of UNION.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The new builder instance for the union query.
        """
        left_query = self.build()  # type: ignore[attr-defined]
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=getattr(self, "dialect", None))
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=getattr(self, "dialect", None))
        if not left_expr or not right_expr:
            msg = "Could not parse queries for UNION operation"
            raise SQLBuilderError(msg)
        union_expr = exp.union(left_expr, right_expr, distinct=not all_)
        new_builder = type(self)()
        new_builder.dialect = getattr(self, "dialect", None)
        new_builder._expression = union_expr
        merged_params = dict(left_query.parameters)
        for param_name, param_value in right_query.parameters.items():
            if param_name in merged_params:
                counter = 1
                new_param_name = f"{param_name}_right_{counter}"
                while new_param_name in merged_params:
                    counter += 1
                    new_param_name = f"{param_name}_right_{counter}"

                # Use AST transformation instead of string manipulation
                def rename_parameter(node: exp.Expression) -> exp.Expression:
                    if isinstance(node, exp.Placeholder) and node.name == param_name:  # noqa: B023
                        return exp.Placeholder(this=new_param_name)  # noqa: B023
                    return node

                right_expr = right_expr.transform(rename_parameter)
                union_expr = exp.union(left_expr, right_expr, distinct=not all_)
                new_builder._expression = union_expr
                merged_params[new_param_name] = param_value
            else:
                merged_params[param_name] = param_value
        new_builder._parameters = merged_params
        return new_builder

    def intersect(self, other: Any) -> Self:
        """Add INTERSECT clause.

        Args:
            other: Another SelectBuilder or compatible builder to intersect with.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The new builder instance for the intersect query.
        """
        left_query = self.build()  # type: ignore[attr-defined]
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=getattr(self, "dialect", None))
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=getattr(self, "dialect", None))
        if not left_expr or not right_expr:
            msg = "Could not parse queries for INTERSECT operation"
            raise SQLBuilderError(msg)
        intersect_expr = exp.intersect(left_expr, right_expr, distinct=True)
        new_builder = type(self)()
        new_builder.dialect = getattr(self, "dialect", None)
        new_builder._expression = intersect_expr
        # Merge parameters
        merged_params = dict(left_query.parameters)
        merged_params.update(right_query.parameters)
        new_builder._parameters = merged_params
        return new_builder

    def except_(self, other: Any) -> Self:
        """Combine this query with another using EXCEPT.

        Args:
            other: Another SelectBuilder or compatible builder to except with.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement.

        Returns:
            The new builder instance for the except query.
        """
        left_query = self.build()  # type: ignore[attr-defined]
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=getattr(self, "dialect", None))
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=getattr(self, "dialect", None))
        if not left_expr or not right_expr:
            msg = "Could not parse queries for EXCEPT operation"
            raise SQLBuilderError(msg)
        except_expr = exp.except_(left_expr, right_expr)
        new_builder = type(self)()
        new_builder.dialect = getattr(self, "dialect", None)
        new_builder._expression = except_expr
        # Merge parameters
        merged_params = dict(left_query.parameters)
        merged_params.update(right_query.parameters)
        new_builder._parameters = merged_params
        return new_builder
