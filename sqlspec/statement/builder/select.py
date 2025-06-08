"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, Union, cast

from sqlglot import exp
from typing_extensions import Self

from sqlspec.statement.builder.base import QueryBuilder, SafeQuery
from sqlspec.statement.builder.mixins import (
    AggregateFunctionsMixin,
    CaseBuilderMixin,
    CommonTableExpressionMixin,
    FromClauseMixin,
    GroupByClauseMixin,
    HavingClauseMixin,
    JoinClauseMixin,
    LimitOffsetClauseMixin,
    OrderByClauseMixin,
    PivotClauseMixin,
    SelectColumnsMixin,
    SetOperationMixin,
    UnpivotClauseMixin,
    WhereClauseMixin,
    WindowFunctionsMixin,
)
from sqlspec.statement.result import SQLResult
from sqlspec.typing import RowT

__all__ = ("SelectBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass
class SelectBuilder(
    QueryBuilder[SQLResult[RowT]],  # pyright: ignore[reportInvalidTypeArguments]
    WhereClauseMixin,
    OrderByClauseMixin,
    LimitOffsetClauseMixin,
    SelectColumnsMixin,
    JoinClauseMixin,
    FromClauseMixin,
    GroupByClauseMixin,
    HavingClauseMixin,
    SetOperationMixin,
    CommonTableExpressionMixin,
    AggregateFunctionsMixin,
    WindowFunctionsMixin,
    CaseBuilderMixin,
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

    _with_parts: "dict[str, Union[exp.CTE, SelectBuilder]]" = field(default_factory=dict, init=False)
    _expression: Optional[exp.Expression] = field(default=None, init=False, repr=False, compare=False, hash=False)
    _schema: Optional[type[RowT]] = None
    _hints: "list[dict[str, object]]" = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> "None":
        super().__post_init__()
        if self._expression is None:
            self._create_base_expression()

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

    def as_schema(self, schema: "type[RowT]") -> "SelectBuilder[RowT]":
        """Return a new SelectBuilder instance parameterized with the given schema/model type.

        This enables type-safe result mapping: the returned builder will carry the schema type
        for static analysis and IDE autocompletion. The schema should be a class such as a Pydantic
        model, dataclass, or msgspec.Struct that describes the expected row shape.

        Args:
            schema: The schema/model class to use for row typing (e.g., a Pydantic model, dataclass, or msgspec.Struct).

        Returns:
            SelectBuilder[RowT]: A new SelectBuilder instance with RowT set to the provided schema/model type.
        """
        new_builder = SelectBuilder()
        new_builder._expression = self._expression.copy() if self._expression is not None else None
        new_builder._parameters = self._parameters.copy()
        new_builder._parameter_counter = self._parameter_counter
        new_builder.dialect = self.dialect
        new_builder._schema = schema  # type: ignore[assignment]
        return cast("SelectBuilder[RowT]", new_builder)

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
        self._hints.append(
            {
                "hint": hint,
                "location": location,
                "table": table,
                "dialect": dialect,
            }
        )
        return self

    def build(self) -> "SafeQuery":
        """Builds the SQL query string and parameters with hint injection.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        # Call parent build method which handles CTEs and optimization
        safe_query = super().build()

        # Inject statement-level hints as comments at the top of the SQL string
        if hasattr(self, "_hints") and self._hints:
            sql = safe_query.sql
            statement_hints = [h["hint"] for h in self._hints if h.get("location") == "statement"]
            if statement_hints:
                hint_comment = " ".join(f"/*+ {h} */" for h in statement_hints)
                sql = f"{hint_comment}\n{sql}"
            # Inject table-level hints as comments before table names in FROM/JOIN clauses
            table_hints = [h for h in self._hints if h.get("location") == "table" and h.get("table")]
            for th in table_hints:
                table = str(th["table"])
                hint = th["hint"]
                # Regex to match FROM <table> or JOIN <table> (optionally with alias)
                import re

                # FROM <table> [AS] <alias> - handle quotes
                pattern_from = rf'(FROM\s+)(["`]?{re.escape(table)}\b["`]?)'
                # JOIN <table> [AS] <alias> - handle quotes
                pattern_join = rf'(JOIN\s+)(["`]?{re.escape(table)}\b["`]?)'
                # Replace in FROM clause
                sql = re.sub(pattern_from, rf"\1/*+ {hint} */ \2", sql, flags=re.IGNORECASE)
                # Replace in JOIN clause
                sql = re.sub(pattern_join, rf"\1/*+ {hint} */ \2", sql, flags=re.IGNORECASE)

            # Return new SafeQuery with modified SQL
            return SafeQuery(sql=sql, parameters=safe_query.parameters, dialect=safe_query.dialect)

        return safe_query
