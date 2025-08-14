"""Expression wrapper classes for proper type annotations."""

from typing import cast

from sqlglot import exp

__all__ = ("AggregateExpression", "ConversionExpression", "FunctionExpression", "MathExpression", "StringExpression")


class ExpressionWrapper:
    """Base wrapper for SQLGlot expressions."""

    def __init__(self, expression: exp.Expression) -> None:
        self._expression = expression

    def as_(self, alias: str) -> exp.Alias:
        """Create an aliased expression."""
        return cast("exp.Alias", exp.alias_(self._expression, alias))

    @property
    def expression(self) -> exp.Expression:
        """Get the underlying SQLGlot expression."""
        return self._expression

    def __str__(self) -> str:
        return str(self._expression)


class AggregateExpression(ExpressionWrapper):
    """Aggregate functions like COUNT, SUM, AVG."""


class FunctionExpression(ExpressionWrapper):
    """General SQL functions."""


class MathExpression(ExpressionWrapper):
    """Mathematical functions like ROUND."""


class StringExpression(ExpressionWrapper):
    """String functions like UPPER, LOWER, LENGTH."""


class ConversionExpression(ExpressionWrapper):
    """Conversion functions like CAST, COALESCE."""
