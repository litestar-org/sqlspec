"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import re
from typing import Any, Callable, Final, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.builder._base import QueryBuilder, SafeQuery
from sqlspec.builder.mixins import (
    CommonTableExpressionMixin,
    HavingClauseMixin,
    JoinClauseMixin,
    LimitOffsetClauseMixin,
    OrderByClauseMixin,
    PivotClauseMixin,
    SelectClauseMixin,
    SetOperationMixin,
    UnpivotClauseMixin,
    WhereClauseMixin,
)
from sqlspec.core.result import SQLResult

__all__ = ("Select",)


TABLE_HINT_PATTERN: Final[str] = r"\b{}\b(\s+AS\s+\w+)?"


class Select(
    QueryBuilder,
    WhereClauseMixin,
    OrderByClauseMixin,
    LimitOffsetClauseMixin,
    SelectClauseMixin,
    JoinClauseMixin,
    HavingClauseMixin,
    SetOperationMixin,
    CommonTableExpressionMixin,
    PivotClauseMixin,
    UnpivotClauseMixin,
):
    """Type-safe builder for SELECT queries with schema/model integration.

    This builder provides a fluent, safe interface for constructing SQL SELECT statements.

    Example:
        >>> class User(BaseModel):
        ...     id: int
        ...     name: str
        >>> builder = Select("id", "name").from_("users")
        >>> result = driver.execute(builder)
    """

    __slots__ = ("_hints", "_with_parts")
    _expression: Optional[exp.Expression]

    def __init__(self, *columns: str, **kwargs: Any) -> None:
        """Initialize SELECT with optional columns.

        Args:
            *columns: Column names to select (e.g., "id", "name", "u.email")
            **kwargs: Additional QueryBuilder arguments (dialect, schema, etc.)

        Examples:
            Select("id", "name")  # Shorthand for Select().select("id", "name")
            Select()              # Same as Select() - start empty
        """
        super().__init__(**kwargs)

        # Initialize Select-specific attributes
        self._with_parts: dict[str, Union[exp.CTE, Select]] = {}
        self._hints: list[dict[str, object]] = []

        self._initialize_expression()

        if columns:
            self.select(*columns)

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Get the expected result type for SELECT operations.

        Returns:
            type: The SelectResult type.
        """
        return SQLResult

    def _create_base_expression(self) -> exp.Select:
        """Create base SELECT expression."""
        if self._expression is None or not isinstance(self._expression, exp.Select):
            self._expression = exp.Select()
        return self._expression

    def with_hint(
        self,
        hint: "str",
        *,
        location: "str" = "statement",
        table: "Optional[str]" = None,
        dialect: "Optional[str]" = None,
    ) -> "Self":
        """Attach an optimizer or dialect-specific hint to the query.

        Args:
            hint: The raw hint string (e.g., 'INDEX(users idx_users_name)').
            location: Where to apply the hint ('statement', 'table').
            table: Table name if the hint is for a specific table.
            dialect: Restrict the hint to a specific dialect (optional).

        Returns:
            The current builder instance for method chaining.
        """
        self._hints.append({"hint": hint, "location": location, "table": table, "dialect": dialect})
        return self

    def build(self) -> "SafeQuery":
        """Builds the SQL query string and parameters with hint injection.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        safe_query = super().build()

        if not self._hints:
            return safe_query

        modified_expr = self._expression or self._create_base_expression()

        if isinstance(modified_expr, exp.Select):
            statement_hints = [h["hint"] for h in self._hints if h.get("location") == "statement"]
            if statement_hints:

                def parse_hint_safely(hint: Any) -> exp.Expression:
                    try:
                        hint_str = str(hint)
                        hint_expr: Optional[exp.Expression] = exp.maybe_parse(hint_str, dialect=self.dialect_name)
                        return hint_expr or exp.Anonymous(this=hint_str)
                    except Exception:
                        return exp.Anonymous(this=str(hint))

                hint_expressions: list[exp.Expression] = [parse_hint_safely(hint) for hint in statement_hints]

                if hint_expressions:
                    modified_expr.set("hint", exp.Hint(expressions=hint_expressions))

        modified_sql = modified_expr.sql(dialect=self.dialect_name, pretty=True)

        for hint_dict in self._hints:
            if hint_dict.get("location") == "table" and hint_dict.get("table"):
                table = str(hint_dict["table"])
                hint = str(hint_dict["hint"])
                pattern = TABLE_HINT_PATTERN.format(re.escape(table))

                def make_replacement(hint_val: str, table_val: str) -> "Callable[[re.Match[str]], str]":
                    def replacement_func(match: re.Match[str]) -> str:
                        alias_part = match.group(1) or ""
                        return f"/*+ {hint_val} */ {table_val}{alias_part}"

                    return replacement_func

                modified_sql = re.sub(
                    pattern, make_replacement(hint, table), modified_sql, count=1, flags=re.IGNORECASE
                )

        return SafeQuery(sql=modified_sql, parameters=safe_query.parameters, dialect=safe_query.dialect)
