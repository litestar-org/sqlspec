"""Column representations for query builders."""

from dataclasses import dataclass
from typing import Any, Optional, Union

from sqlglot import exp


@dataclass
class Column:
    """Represents a column in SQL queries."""

    name: str
    table: str = ""
    _expr: Optional[exp.Expression] = None

    def __post_init__(self) -> None:
        """Initialize the column expression."""
        if self._expr is None:
            if self.table:
                self._expr = exp.column(self.name, table=self.table)
            else:
                self._expr = exp.column(self.name)

    def __str__(self) -> str:
        """String representation of the column."""
        if self.table:
            return f"{self.table}.{self.name}"
        return self.name


@dataclass
class ColumnExpression:
    """Represents a column expression (e.g., with functions or operations)."""

    _expr: exp.Expression

    def __str__(self) -> str:
        """String representation of the expression."""
        return self._expr.sql()


@dataclass
class FunctionColumn(ColumnExpression):
    """Represents a function call on a column."""

    function_name: str
    column: Union[str, Column]
    args: tuple[Any, ...] = ()

    def __post_init__(self) -> None:
        """Initialize the function expression."""
        col_expr = self.column._expr if isinstance(self.column, Column) else exp.column(self.column)

        # Create function expression based on function name
        func_map = {
            "COUNT": exp.Count,
            "SUM": exp.Sum,
            "AVG": exp.Avg,
            "MAX": exp.Max,
            "MIN": exp.Min,
            "LENGTH": exp.Length,
            "UPPER": exp.Upper,
            "LOWER": exp.Lower,
        }

        func_class = func_map.get(self.function_name.upper(), exp.Anonymous)
        if func_class == exp.Anonymous:
            self._expr = exp.Anonymous(this=self.function_name, expressions=[col_expr])
        else:
            self._expr = func_class(this=col_expr)


__all__ = ("Column", "ColumnExpression", "FunctionColumn")
