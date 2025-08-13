"""CTE (Common Table Expression) and Set Operations mixins for SQL builders."""

from typing import Any, Optional, Union

from mypy_extensions import trait
from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("CommonTableExpressionMixin", "SetOperationMixin")


@trait
class CommonTableExpressionMixin:
    """Mixin providing WITH clause (Common Table Expressions) support for SQL builders."""

    __slots__ = ()
    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    _with_ctes: Any  # Provided by QueryBuilder
    dialect: Any  # Provided by QueryBuilder

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]:
        """Add parameter - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

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

        if not isinstance(self._expression, (exp.Select, exp.Insert, exp.Update, exp.Delete)):
            msg = f"Cannot add WITH clause to {type(self._expression).__name__} expression."
            raise SQLBuilderError(msg)

        cte_expr: Optional[exp.Expression] = None
        if isinstance(query, str):
            cte_expr = exp.maybe_parse(query, dialect=self.dialect)
        elif isinstance(query, exp.Expression):
            cte_expr = query
        else:
            built_query = query.to_statement()
            cte_sql = built_query.sql
            cte_expr = exp.maybe_parse(cte_sql, dialect=self.dialect)

            parameters = built_query.parameters
            if parameters:
                if isinstance(parameters, dict):
                    for param_name, param_value in parameters.items():
                        self.add_parameter(param_value, name=param_name)
                elif isinstance(parameters, (list, tuple)):
                    for param_value in parameters:
                        self.add_parameter(param_value)

        if not cte_expr:
            msg = f"Could not parse CTE query: {query}"
            raise SQLBuilderError(msg)

        if columns:
            cte_alias_expr = exp.alias_(cte_expr, name, table=[exp.to_identifier(col) for col in columns])
        else:
            cte_alias_expr = exp.alias_(cte_expr, name)

        existing_with = self._expression.args.get("with")
        if existing_with:
            existing_with.expressions.append(cte_alias_expr)
            if recursive:
                existing_with.set("recursive", recursive)
        else:
            # Only SELECT, INSERT, UPDATE support WITH clauses
            if hasattr(self._expression, "with_") and isinstance(
                self._expression, (exp.Select, exp.Insert, exp.Update)
            ):
                self._expression = self._expression.with_(cte_alias_expr, as_=name, copy=False)
                if recursive:
                    with_clause = self._expression.find(exp.With)
                    if with_clause:
                        with_clause.set("recursive", recursive)
            self._with_ctes[name] = exp.CTE(this=cte_expr, alias=exp.to_table(name))

        return self


@trait
class SetOperationMixin:
    """Mixin providing set operations (UNION, INTERSECT, EXCEPT) for SELECT builders."""

    __slots__ = ()
    # Type annotation for PyRight - this will be provided by the base class
    _expression: Optional[exp.Expression]

    _parameters: dict[str, Any]
    dialect: Any = None

    def build(self) -> Any:
        """Build the query - provided by QueryBuilder."""
        msg = "Method must be provided by QueryBuilder subclass"
        raise NotImplementedError(msg)

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
        left_query = self.build()
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=self.dialect)
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=self.dialect)
        if not left_expr or not right_expr:
            msg = "Could not parse queries for UNION operation"
            raise SQLBuilderError(msg)
        union_expr = exp.union(left_expr, right_expr, distinct=not all_)
        new_builder = type(self)()
        new_builder.dialect = self.dialect
        new_builder._expression = union_expr
        merged_parameters = dict(left_query.parameters)
        for param_name, param_value in right_query.parameters.items():
            if param_name in merged_parameters:
                counter = 1
                new_param_name = f"{param_name}_right_{counter}"
                while new_param_name in merged_parameters:
                    counter += 1
                    new_param_name = f"{param_name}_right_{counter}"

                def rename_parameter(
                    node: exp.Expression, old_name: str = param_name, new_name: str = new_param_name
                ) -> exp.Expression:
                    if isinstance(node, exp.Placeholder) and node.name == old_name:
                        return exp.Placeholder(this=new_name)
                    return node

                right_expr = right_expr.transform(rename_parameter)
                union_expr = exp.union(left_expr, right_expr, distinct=not all_)
                new_builder._expression = union_expr
                merged_parameters[new_param_name] = param_value
            else:
                merged_parameters[param_name] = param_value
        new_builder._parameters = merged_parameters
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
        left_query = self.build()
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=self.dialect)
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=self.dialect)
        if not left_expr or not right_expr:
            msg = "Could not parse queries for INTERSECT operation"
            raise SQLBuilderError(msg)
        intersect_expr = exp.intersect(left_expr, right_expr, distinct=True)
        new_builder = type(self)()
        new_builder.dialect = self.dialect
        new_builder._expression = intersect_expr
        merged_parameters = dict(left_query.parameters)
        merged_parameters.update(right_query.parameters)
        new_builder._parameters = merged_parameters
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
        left_query = self.build()
        right_query = other.build()
        left_expr: Optional[exp.Expression] = exp.maybe_parse(left_query.sql, dialect=self.dialect)
        right_expr: Optional[exp.Expression] = exp.maybe_parse(right_query.sql, dialect=self.dialect)
        if not left_expr or not right_expr:
            msg = "Could not parse queries for EXCEPT operation"
            raise SQLBuilderError(msg)
        except_expr = exp.except_(left_expr, right_expr)
        new_builder = type(self)()
        new_builder.dialect = self.dialect
        new_builder._expression = except_expr
        merged_parameters = dict(left_query.parameters)
        merged_parameters.update(right_query.parameters)
        new_builder._parameters = merged_parameters
        return new_builder
