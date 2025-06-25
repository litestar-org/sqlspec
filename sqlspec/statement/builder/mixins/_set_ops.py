from typing import Any, Optional

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("SetOperationMixin",)


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
