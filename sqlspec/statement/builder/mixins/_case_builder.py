# mypy: disable-error-code="valid-type,type-var"
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlglot import exp

if TYPE_CHECKING:
    from sqlspec.statement.builder.base import QueryBuilder
    from sqlspec.typing import RowT

__all__ = ("CaseBuilder", "CaseBuilderMixin")


class CaseBuilderMixin:
    """Mixin providing CASE expression functionality for SQL builders."""

    def case_(self, alias: "Optional[str]" = None) -> "CaseBuilder":
        """Create a CASE expression for the SELECT clause.

        Args:
            alias: Optional alias for the CASE expression.

        Returns:
            CaseBuilder: A CaseBuilder instance for building the CASE expression.
        """
        builder = cast("QueryBuilder[RowT]", self)  # pyright: ignore
        return CaseBuilder(builder, alias)


@dataclass
class CaseBuilder:
    """Builder for CASE expressions."""

    _parent: "QueryBuilder[RowT]"  # pyright: ignore
    _alias: Optional[str]
    _case_expr: exp.Case

    def __init__(self, parent: "QueryBuilder[RowT]", alias: "Optional[str]" = None) -> None:
        """Initialize CaseBuilder.

        Args:
            parent: The parent builder.
            alias: Optional alias for the CASE expression.
        """
        self._parent = parent
        self._alias = alias
        self._case_expr = exp.Case()

    def when(self, condition: "Union[str, exp.Expression]", value: "Any") -> "CaseBuilder":
        """Add WHEN clause to CASE expression.

        Args:
            condition: The condition to test.
            value: The value to return if condition is true.

        Returns:
            CaseBuilder: The current builder instance for method chaining.
        """
        cond_expr = exp.condition(condition) if isinstance(condition, str) else condition
        param_name = self._parent.add_parameter(value)[1]
        value_expr = exp.Placeholder(this=param_name)

        when_clause = exp.When(this=cond_expr, then=value_expr)

        if not self._case_expr.args.get("ifs"):
            self._case_expr.set("ifs", [])
        self._case_expr.args["ifs"].append(when_clause)
        return self

    def else_(self, value: "Any") -> "CaseBuilder":
        """Add ELSE clause to CASE expression.

        Args:
            value: The value to return if no conditions match.

        Returns:
            CaseBuilder: The current builder instance for method chaining.
        """
        param_name = self._parent.add_parameter(value)[1]
        value_expr = exp.Placeholder(this=param_name)
        self._case_expr.set("default", value_expr)
        return self

    def end(self) -> "QueryBuilder[RowT]":
        """Finalize the CASE expression and add it to the SELECT clause.

        Returns:
            The parent builder instance.
        """
        select_expr = exp.alias_(self._case_expr, self._alias) if self._alias else self._case_expr
        return cast("QueryBuilder[RowT]", self._parent.select(select_expr))  # type: ignore[attr-defined]
