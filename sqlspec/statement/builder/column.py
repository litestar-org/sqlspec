"""Pythonic column expressions for query building.

This module provides Column objects that support native Python operators
for building SQL conditions with type safety and parameter binding.
"""

from collections.abc import Iterable
from typing import Any, Optional

from sqlglot import exp

__all__ = ("Column", "ColumnExpression")


class ColumnExpression:
    """Base class for column expressions that can be combined with operators."""

    def __init__(self, expression: exp.Expression) -> None:
        self._expr = expression

    def __and__(self, other: "ColumnExpression") -> "ColumnExpression":
        """Combine with AND operator (&)."""
        if not isinstance(other, ColumnExpression):
            return NotImplemented
        return ColumnExpression(exp.And(this=self._expr, expression=other._expr))

    def __or__(self, other: "ColumnExpression") -> "ColumnExpression":
        """Combine with OR operator (|)."""
        if not isinstance(other, ColumnExpression):
            return NotImplemented
        return ColumnExpression(exp.Or(this=self._expr, expression=other._expr))

    def __invert__(self) -> "ColumnExpression":
        """Apply NOT operator (~)."""
        return ColumnExpression(exp.Not(this=self._expr))

    def __bool__(self) -> bool:
        """Prevent accidental use of 'and'/'or' keywords."""
        msg = (
            "Cannot use 'and'/'or' operators on ColumnExpression. "
            "Use '&'/'|' operators instead. "
            f"Expression: {self._expr.sql()}"
        )
        raise TypeError(
            msg
        )

    @property
    def sqlglot_expression(self) -> exp.Expression:
        """Get the underlying SQLGlot expression."""
        return self._expr


class Column:
    """Represents a database column with Python operator support."""

    def __init__(self, name: str, table: Optional[str] = None) -> None:
        self.name = name
        self.table = table

        # Create SQLGlot column expression
        if table:
            self._expr = exp.Column(
                this=exp.Identifier(this=name),
                table=exp.Identifier(this=table)
            )
        else:
            self._expr = exp.Column(this=exp.Identifier(this=name))

    # Comparison operators
    def __eq__(self, other: object) -> ColumnExpression:
        """Equal to (==)."""
        if other is None:
            return ColumnExpression(exp.Is(this=self._expr, expression=exp.Null()))
        return ColumnExpression(exp.EQ(this=self._expr, expression=exp.convert(other)))

    def __ne__(self, other: object) -> ColumnExpression:
        """Not equal to (!=)."""
        if other is None:
            return ColumnExpression(exp.Not(this=exp.Is(this=self._expr, expression=exp.Null())))
        return ColumnExpression(exp.NEQ(this=self._expr, expression=exp.convert(other)))

    def __gt__(self, other: Any) -> ColumnExpression:
        """Greater than (>)."""
        return ColumnExpression(exp.GT(this=self._expr, expression=exp.convert(other)))

    def __ge__(self, other: Any) -> ColumnExpression:
        """Greater than or equal (>=)."""
        return ColumnExpression(exp.GTE(this=self._expr, expression=exp.convert(other)))

    def __lt__(self, other: Any) -> ColumnExpression:
        """Less than (<)."""
        return ColumnExpression(exp.LT(this=self._expr, expression=exp.convert(other)))

    def __le__(self, other: Any) -> ColumnExpression:
        """Less than or equal (<=)."""
        return ColumnExpression(exp.LTE(this=self._expr, expression=exp.convert(other)))

    # SQL-specific methods
    def like(self, pattern: str, escape: Optional[str] = None) -> ColumnExpression:
        """SQL LIKE pattern matching."""
        like_expr = exp.Like(
            this=self._expr,
            expression=exp.convert(pattern)
        )
        if escape:
            like_expr = like_expr.set("escape", exp.convert(escape))
        return ColumnExpression(like_expr)

    def ilike(self, pattern: str) -> ColumnExpression:
        """Case-insensitive LIKE."""
        return ColumnExpression(exp.ILike(this=self._expr, expression=exp.convert(pattern)))

    def in_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL IN clause."""
        converted_values = [exp.convert(v) for v in values]
        return ColumnExpression(exp.In(this=self._expr, expressions=converted_values))

    def not_in(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL NOT IN clause."""
        return ~self.in_(values)

    def between(self, start: Any, end: Any) -> ColumnExpression:
        """SQL BETWEEN clause."""
        return ColumnExpression(exp.Between(
            this=self._expr,
            low=exp.convert(start),
            high=exp.convert(end)
        ))

    def is_null(self) -> ColumnExpression:
        """SQL IS NULL."""
        return ColumnExpression(exp.Is(this=self._expr, expression=exp.Null()))

    def is_not_null(self) -> ColumnExpression:
        """SQL IS NOT NULL."""
        return ColumnExpression(exp.Not(this=exp.Is(this=self._expr, expression=exp.Null())))

    # SQL Functions
    def lower(self) -> "FunctionColumn":
        """SQL LOWER() function."""
        return FunctionColumn(exp.Lower(this=self._expr))

    def upper(self) -> "FunctionColumn":
        """SQL UPPER() function."""
        return FunctionColumn(exp.Upper(this=self._expr))

    def __repr__(self) -> str:
        if self.table:
            return f"Column<{self.table}.{self.name}>"
        return f"Column<{self.name}>"

    def __hash__(self) -> int:
        """Hash based on table and column name."""
        return hash((self.table, self.name))


class FunctionColumn:
    """Represents the result of a SQL function call on a column."""

    def __init__(self, expression: exp.Expression) -> None:
        self._expr = expression

    def __eq__(self, other: object) -> ColumnExpression:
        return ColumnExpression(exp.EQ(this=self._expr, expression=exp.convert(other)))

    def __ne__(self, other: object) -> ColumnExpression:
        return ColumnExpression(exp.NEQ(this=self._expr, expression=exp.convert(other)))

    def like(self, pattern: str) -> ColumnExpression:
        return ColumnExpression(exp.Like(this=self._expr, expression=exp.convert(pattern)))

    # Add other operators as needed...

    def __hash__(self) -> int:
        """Hash based on the SQL expression."""
        return hash(self._expr.sql() if hasattr(self._expr, "sql") else str(self._expr))
