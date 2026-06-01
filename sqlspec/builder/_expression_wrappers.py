"""Expression wrapper classes for proper type annotations."""

from typing import cast, final

from sqlglot import exp

__all__ = ("AggregateExpression", "ConversionExpression", "FunctionExpression", "MathExpression", "StringExpression")


class ExpressionWrapper:
    """Base wrapper for SQLGlot expressions."""

    __slots__ = ("_expression",)

    def __init__(self, expression: exp.Expr) -> None:
        self._expression = expression

    def as_(self, alias: str) -> exp.Alias:
        """Create an aliased expression."""
        return cast("exp.Alias", exp.alias_(self._expression, alias))

    @property
    def expression(self) -> exp.Expr:
        """Get the underlying SQLGlot expression."""
        return self._expression

    def __str__(self) -> str:
        return str(self._expression)


@final
class AggregateExpression(ExpressionWrapper):
    """Aggregate functions like COUNT, SUM, AVG."""

    __slots__ = ()


@final
class FunctionExpression(ExpressionWrapper):
    """General SQL functions."""

    __slots__ = ()


@final
class MathExpression(ExpressionWrapper):
    """Mathematical functions like ROUND."""

    __slots__ = ()


@final
class StringExpression(ExpressionWrapper):
    """String functions like UPPER, LOWER, LENGTH."""

    __slots__ = ()


@final
class ConversionExpression(ExpressionWrapper):
    """Conversion functions like CAST, COALESCE."""

    __slots__ = ()
