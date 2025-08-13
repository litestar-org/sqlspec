"""Pythonic column expressions for query building.

This module provides Column objects that support native Python operators
for building SQL conditions with type safety and parameter binding.
"""

from collections.abc import Iterable
from typing import Any, Optional

from sqlglot import exp

from sqlspec.utils.type_guards import has_sql_method

__all__ = ("Column", "ColumnExpression", "FunctionColumn")


class ColumnExpression:
    """Base class for column expressions that can be combined with operators."""

    __slots__ = ("_expression",)

    def __init__(self, expression: exp.Expression) -> None:
        self._expression = expression

    def __and__(self, other: "ColumnExpression") -> "ColumnExpression":
        """Combine with AND operator (&)."""
        if not isinstance(other, ColumnExpression):
            return NotImplemented
        return ColumnExpression(exp.And(this=self._expression, expression=other._expression))

    def __or__(self, other: "ColumnExpression") -> "ColumnExpression":
        """Combine with OR operator (|)."""
        if not isinstance(other, ColumnExpression):
            return NotImplemented
        return ColumnExpression(exp.Or(this=self._expression, expression=other._expression))

    def __invert__(self) -> "ColumnExpression":
        """Apply NOT operator (~)."""
        return ColumnExpression(exp.Not(this=self._expression))

    def __bool__(self) -> bool:
        """Prevent accidental use of 'and'/'or' keywords."""
        msg = (
            "Cannot use 'and'/'or' operators on ColumnExpression. "
            "Use '&'/'|' operators instead. "
            f"Expression: {self._expression.sql()}"
        )
        raise TypeError(msg)

    @property
    def sqlglot_expression(self) -> exp.Expression:
        """Get the underlying SQLGlot expression."""
        return self._expression


class Column:
    """Represents a database column with Python operator support."""

    __slots__ = ("_expression", "name", "table")

    def __init__(self, name: str, table: Optional[str] = None) -> None:
        self.name = name
        self.table = table

        if table:
            self._expression = exp.Column(this=exp.Identifier(this=name), table=exp.Identifier(this=table))
        else:
            self._expression = exp.Column(this=exp.Identifier(this=name))

    def __eq__(self, other: object) -> ColumnExpression:  # type: ignore[override]
        """Equal to (==)."""
        if other is None:
            return ColumnExpression(exp.Is(this=self._expression, expression=exp.Null()))
        return ColumnExpression(exp.EQ(this=self._expression, expression=exp.convert(other)))

    def __ne__(self, other: object) -> ColumnExpression:  # type: ignore[override]
        """Not equal to (!=)."""
        if other is None:
            return ColumnExpression(exp.Not(this=exp.Is(this=self._expression, expression=exp.Null())))
        return ColumnExpression(exp.NEQ(this=self._expression, expression=exp.convert(other)))

    def __gt__(self, other: Any) -> ColumnExpression:
        """Greater than (>)."""
        return ColumnExpression(exp.GT(this=self._expression, expression=exp.convert(other)))

    def __ge__(self, other: Any) -> ColumnExpression:
        """Greater than or equal (>=)."""
        return ColumnExpression(exp.GTE(this=self._expression, expression=exp.convert(other)))

    def __lt__(self, other: Any) -> ColumnExpression:
        """Less than (<)."""
        return ColumnExpression(exp.LT(this=self._expression, expression=exp.convert(other)))

    def __le__(self, other: Any) -> ColumnExpression:
        """Less than or equal (<=)."""
        return ColumnExpression(exp.LTE(this=self._expression, expression=exp.convert(other)))

    def __invert__(self) -> ColumnExpression:
        """Apply NOT operator (~)."""
        return ColumnExpression(exp.Not(this=self._expression))

    def like(self, pattern: str, escape: Optional[str] = None) -> ColumnExpression:
        """SQL LIKE pattern matching."""
        if escape:
            like_expr = exp.Like(this=self._expression, expression=exp.convert(pattern), escape=exp.convert(escape))
        else:
            like_expr = exp.Like(this=self._expression, expression=exp.convert(pattern))
        return ColumnExpression(like_expr)

    def ilike(self, pattern: str) -> ColumnExpression:
        """Case-insensitive LIKE."""
        return ColumnExpression(exp.ILike(this=self._expression, expression=exp.convert(pattern)))

    def in_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL IN clause."""
        converted_values = [exp.convert(v) for v in values]
        return ColumnExpression(exp.In(this=self._expression, expressions=converted_values))

    def not_in(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL NOT IN clause."""
        return ~self.in_(values)

    def between(self, start: Any, end: Any) -> ColumnExpression:
        """SQL BETWEEN clause."""
        return ColumnExpression(exp.Between(this=self._expression, low=exp.convert(start), high=exp.convert(end)))

    def is_null(self) -> ColumnExpression:
        """SQL IS NULL."""
        return ColumnExpression(exp.Is(this=self._expression, expression=exp.Null()))

    def is_not_null(self) -> ColumnExpression:
        """SQL IS NOT NULL."""
        return ColumnExpression(exp.Not(this=exp.Is(this=self._expression, expression=exp.Null())))

    def not_like(self, pattern: str, escape: Optional[str] = None) -> ColumnExpression:
        """SQL NOT LIKE pattern matching."""
        return ~self.like(pattern, escape)

    def not_ilike(self, pattern: str) -> ColumnExpression:
        """Case-insensitive NOT LIKE."""
        return ~self.ilike(pattern)

    def any_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL = ANY(...) clause."""
        converted_values = [exp.convert(v) for v in values]
        return ColumnExpression(exp.EQ(this=self._expression, expression=exp.Any(expressions=converted_values)))

    def not_any_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL <> ANY(...) clause."""
        converted_values = [exp.convert(v) for v in values]
        return ColumnExpression(exp.NEQ(this=self._expression, expression=exp.Any(expressions=converted_values)))

    def lower(self) -> "FunctionColumn":
        """SQL LOWER() function."""
        return FunctionColumn(exp.Lower(this=self._expression))

    def upper(self) -> "FunctionColumn":
        """SQL UPPER() function."""
        return FunctionColumn(exp.Upper(this=self._expression))

    def length(self) -> "FunctionColumn":
        """SQL LENGTH() function."""
        return FunctionColumn(exp.Length(this=self._expression))

    def trim(self) -> "FunctionColumn":
        """SQL TRIM() function."""
        return FunctionColumn(exp.Trim(this=self._expression))

    def abs(self) -> "FunctionColumn":
        """SQL ABS() function."""
        return FunctionColumn(exp.Abs(this=self._expression))

    def round(self, decimals: int = 0) -> "FunctionColumn":
        """SQL ROUND() function."""
        if decimals == 0:
            return FunctionColumn(exp.Round(this=self._expression))
        return FunctionColumn(exp.Round(this=self._expression, expression=exp.convert(decimals)))

    def floor(self) -> "FunctionColumn":
        """SQL FLOOR() function."""
        return FunctionColumn(exp.Floor(this=self._expression))

    def ceil(self) -> "FunctionColumn":
        """SQL CEIL() function."""
        return FunctionColumn(exp.Ceil(this=self._expression))

    def substring(self, start: int, length: Optional[int] = None) -> "FunctionColumn":
        """SQL SUBSTRING() function."""
        args = [exp.convert(start)]
        if length is not None:
            args.append(exp.convert(length))
        return FunctionColumn(exp.Substring(this=self._expression, expressions=args))

    def coalesce(self, *values: Any) -> "FunctionColumn":
        """SQL COALESCE() function."""
        expressions = [self._expression] + [exp.convert(v) for v in values]
        return FunctionColumn(exp.Coalesce(expressions=expressions))

    def cast(self, data_type: str) -> "FunctionColumn":
        """SQL CAST() function."""
        return FunctionColumn(exp.Cast(this=self._expression, to=exp.DataType.build(data_type)))

    def count(self) -> "FunctionColumn":
        """SQL COUNT() function."""
        return FunctionColumn(exp.Count(this=self._expression))

    def sum(self) -> "FunctionColumn":
        """SQL SUM() function."""
        return FunctionColumn(exp.Sum(this=self._expression))

    def avg(self) -> "FunctionColumn":
        """SQL AVG() function."""
        return FunctionColumn(exp.Avg(this=self._expression))

    def min(self) -> "FunctionColumn":
        """SQL MIN() function."""
        return FunctionColumn(exp.Min(this=self._expression))

    def max(self) -> "FunctionColumn":
        """SQL MAX() function."""
        return FunctionColumn(exp.Max(this=self._expression))

    def count_distinct(self) -> "FunctionColumn":
        """SQL COUNT(DISTINCT column) function."""
        return FunctionColumn(exp.Count(this=exp.Distinct(expressions=[self._expression])))

    @staticmethod
    def count_all() -> "FunctionColumn":
        """SQL COUNT(*) function."""
        return FunctionColumn(exp.Count(this=exp.Star()))

    def alias(self, alias_name: str) -> exp.Expression:
        """Create an aliased column expression."""
        return exp.Alias(this=self._expression, alias=alias_name)

    def asc(self) -> exp.Ordered:
        """Create an ASC ordering expression."""
        return exp.Ordered(this=self._expression, desc=False)

    def desc(self) -> exp.Ordered:
        """Create a DESC ordering expression."""
        return exp.Ordered(this=self._expression, desc=True)

    def __repr__(self) -> str:
        if self.table:
            return f"Column<{self.table}.{self.name}>"
        return f"Column<{self.name}>"

    def __hash__(self) -> int:
        """Hash based on table and column name."""
        return hash((self.table, self.name))


class FunctionColumn:
    """Represents the result of a SQL function call on a column."""

    __slots__ = ("_expression",)

    def __init__(self, expression: exp.Expression) -> None:
        self._expression = expression

    def __eq__(self, other: object) -> ColumnExpression:  # type: ignore[override]
        return ColumnExpression(exp.EQ(this=self._expression, expression=exp.convert(other)))

    def __ne__(self, other: object) -> ColumnExpression:  # type: ignore[override]
        return ColumnExpression(exp.NEQ(this=self._expression, expression=exp.convert(other)))

    def like(self, pattern: str) -> ColumnExpression:
        return ColumnExpression(exp.Like(this=self._expression, expression=exp.convert(pattern)))

    def ilike(self, pattern: str) -> ColumnExpression:
        """Case-insensitive LIKE."""
        return ColumnExpression(exp.ILike(this=self._expression, expression=exp.convert(pattern)))

    def in_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL IN clause."""
        converted_values = [exp.convert(v) for v in values]
        return ColumnExpression(exp.In(this=self._expression, expressions=converted_values))

    def not_in_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL NOT IN clause."""
        return ~self.in_(values)

    def not_like(self, pattern: str) -> ColumnExpression:
        """SQL NOT LIKE."""
        return ~self.like(pattern)

    def not_ilike(self, pattern: str) -> ColumnExpression:
        """Case-insensitive NOT LIKE."""
        return ~self.ilike(pattern)

    def between(self, start: Any, end: Any) -> ColumnExpression:
        """SQL BETWEEN clause."""
        return ColumnExpression(exp.Between(this=self._expression, low=exp.convert(start), high=exp.convert(end)))

    def is_null(self) -> ColumnExpression:
        """SQL IS NULL."""
        return ColumnExpression(exp.Is(this=self._expression, expression=exp.Null()))

    def is_not_null(self) -> ColumnExpression:
        """SQL IS NOT NULL."""
        return ColumnExpression(exp.Not(this=exp.Is(this=self._expression, expression=exp.Null())))

    def any_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL = ANY(...) clause."""
        converted_values = [exp.convert(v) for v in values]
        return ColumnExpression(exp.EQ(this=self._expression, expression=exp.Any(expressions=converted_values)))

    def not_any_(self, values: Iterable[Any]) -> ColumnExpression:
        """SQL <> ANY(...) clause."""
        converted_values = [exp.convert(v) for v in values]
        return ColumnExpression(exp.NEQ(this=self._expression, expression=exp.Any(expressions=converted_values)))

    def alias(self, alias_name: str) -> exp.Expression:
        """Create an aliased function expression."""
        return exp.Alias(this=self._expression, alias=alias_name)

    def __hash__(self) -> int:
        """Hash based on the SQL expression."""
        return hash(self._expression.sql() if has_sql_method(self._expression) else str(self._expression))
