"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Optional, Union, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.statement.builder._base import QueryBuilder, SafeQuery
from sqlspec.statement.builder.mixins import (
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
from sqlspec.statement.result import SQLResult
from sqlspec.typing import RowT

__all__ = ("Select",)


# This pattern is formatted with the table name and compiled for each hint.
TABLE_HINT_PATTERN = r"\b{}\b(\s+AS\s+\w+)?"


@dataclass
class Select(
    QueryBuilder[RowT],
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
    It supports type-safe result mapping via the `as_schema()` method, allowing users to
    associate a schema/model (such as a Pydantic model, dataclass, or msgspec.Struct) with
    the query for static type checking and IDE support.

    Example:
        >>> class User(BaseModel):
        ...     id: int
        ...     name: str
        >>> builder = (
        ...     SelectBuilder()
        ...     .select("id", "name")
        ...     .from_("users")
        ...     .as_schema(User)
        ... )
        >>> result: list[User] = driver.execute(builder)

    Attributes:
        _schema: The schema/model class for row typing, if set via as_schema().
    """

    _with_parts: "dict[str, Union[exp.CTE, Select]]" = field(default_factory=dict, init=False)
    _expression: Optional[exp.Expression] = field(default=None, init=False, repr=False, compare=False, hash=False)
    _schema: Optional[type[RowT]] = None
    _hints: "list[dict[str, object]]" = field(default_factory=list, init=False, repr=False)

    def __init__(self, *columns: str, **kwargs: Any) -> None:
        """Initialize SELECT with optional columns.

        Args:
            *columns: Column names to select (e.g., "id", "name", "u.email")
            **kwargs: Additional QueryBuilder arguments (dialect, schema, etc.)

        Examples:
            Select("id", "name")  # Shorthand for Select().select("id", "name")
            Select()              # Same as SelectBuilder() - start empty
        """
        super().__init__(**kwargs)

        # Manually initialize dataclass fields here because a custom __init__ is defined.
        # This is necessary to support the `Select("col1", "col2")` shorthand initialization.
        self._with_parts = {}
        self._expression = None
        self._schema = None
        self._hints = []

        self._create_base_expression()

        # Add columns if provided - just a shorthand for .select()
        if columns:
            self.select(*columns)

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        """Get the expected result type for SELECT operations.

        Returns:
            type: The SelectResult type.
        """
        return SQLResult[RowT]

    def _create_base_expression(self) -> "exp.Select":
        if self._expression is None or not isinstance(self._expression, exp.Select):
            self._expression = exp.Select()
        # At this point, self._expression is exp.Select
        return self._expression

    def as_schema(self, schema: "type[RowT]") -> "Select[RowT]":
        """Return a new Select instance parameterized with the given schema/model type.

        This enables type-safe result mapping: the returned builder will carry the schema type
        for static analysis and IDE autocompletion. The schema should be a class such as a Pydantic
        model, dataclass, or msgspec.Struct that describes the expected row shape.

        Args:
            schema: The schema/model class to use for row typing (e.g., a Pydantic model, dataclass, or msgspec.Struct).

        Returns:
            Select[RowT]: A new Select instance with RowT set to the provided schema/model type.
        """
        new_builder = Select()
        new_builder._expression = self._expression.copy() if self._expression is not None else None
        new_builder._parameters = self._parameters.copy()
        new_builder._parameter_counter = self._parameter_counter
        new_builder.dialect = self.dialect
        new_builder._schema = schema  # type: ignore[assignment]
        return cast("Select[RowT]", new_builder)

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

        modified_expr = self._expression.copy() if self._expression else self._create_base_expression()

        if isinstance(modified_expr, exp.Select):
            statement_hints = [h["hint"] for h in self._hints if h.get("location") == "statement"]
            if statement_hints:
                # Parse each hint and create proper hint expressions
                hint_expressions = []

                def parse_hint(hint: Any) -> exp.Expression:
                    """Parse a single hint with error handling."""
                    try:
                        # Try to parse hint as an expression (e.g., "INDEX(users idx_name)")
                        hint_str = str(hint)  # Ensure hint is a string
                        hint_expr: Optional[exp.Expression] = exp.maybe_parse(hint_str, dialect=self.dialect_name)
                        if hint_expr:
                            return hint_expr
                        return exp.Anonymous(this=hint_str)
                    except Exception:
                        return exp.Anonymous(this=str(hint))

                hint_expressions = [parse_hint(hint) for hint in statement_hints]

                if hint_expressions:
                    hint_node = exp.Hint(expressions=hint_expressions)
                    modified_expr.set("hint", hint_node)

        # For table-level hints, we'll fall back to comment injection in SQL
        # since SQLGlot doesn't have a standard way to attach hints to individual tables
        modified_sql = modified_expr.sql(dialect=self.dialect_name, pretty=True)

        table_hints = [h for h in self._hints if h.get("location") == "table" and h.get("table")]
        if table_hints:
            for th in table_hints:
                table = str(th["table"])
                hint = th["hint"]
                # More precise regex that captures the table and optional alias
                pattern = TABLE_HINT_PATTERN.format(re.escape(table))
                compiled_pattern = re.compile(pattern, re.IGNORECASE)

                def replacement_func(match: re.Match[str]) -> str:
                    alias_part = match.group(1) or ""
                    return f"/*+ {hint} */ {table}{alias_part}"  # noqa: B023

                modified_sql = compiled_pattern.sub(replacement_func, modified_sql, count=1)

        return SafeQuery(sql=modified_sql, parameters=safe_query.parameters, dialect=safe_query.dialect)
