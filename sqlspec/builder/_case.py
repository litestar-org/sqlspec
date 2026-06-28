"""CASE expression builder and representation.

Provides structures and builder classes for SQL CASE WHEN expressions.
"""

from typing import Any, cast

from sqlglot import exp

from sqlspec.builder._parsing_utils import parse_condition_expression, to_expression

__all__ = ("Case", "CaseBuilder")


class Case:
    """Represent a SQL CASE expression with structured components."""

    __slots__ = ("conditions", "default")

    def __init__(self, *ifs: exp.Expr, default: exp.Expr | None = None) -> None:
        self.conditions = list(ifs)
        self.default = default

    def when(self, condition: str | exp.Expr, result: Any) -> "Case":
        condition_expr = parse_condition_expression(condition)
        result_expr = to_expression(result)
        self.conditions.append(exp.If(this=condition_expr, true=result_expr))
        return self

    def else_(self, value: Any) -> "Case":
        self.default = to_expression(value)
        return self

    def end(self) -> "Case":
        return self

    def as_(self, alias: str) -> exp.Alias:
        return cast("exp.Alias", exp.alias_(self.expression, alias))

    @property
    def expression(self) -> exp.Case:
        return exp.Case(ifs=self.conditions, default=self.default)


class CaseBuilder:
    """Fluent builder for CASE expressions used within SELECT clauses."""

    __slots__ = ()

    def __call__(self, *args: Any, default: Any | None = None) -> Case:
        conditions = [to_expression(arg) for arg in args]
        default_expr = to_expression(default) if default is not None else None
        return Case(*conditions, default=default_expr)
