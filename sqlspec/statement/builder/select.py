"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import re
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


@dataclass
class SelectBuilder(
    QueryBuilder[RowT],
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
        self._hints.append({"hint": hint, "location": location, "table": table, "dialect": dialect})
        return self

    def build(self) -> "SafeQuery":
        """Builds the SQL query string and parameters with hint injection.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        # Call parent build method which handles CTEs and optimization
        safe_query = super().build()

        # Apply hints using SQLGlot's proper hint support (more robust than regex)
        if hasattr(self, "_hints") and self._hints:
            modified_expr = self._expression.copy() if self._expression else None

            if modified_expr and isinstance(modified_expr, exp.Select):
                # Apply statement-level hints using SQLGlot's Hint expression
                statement_hints = [h["hint"] for h in self._hints if h.get("location") == "statement"]
                if statement_hints:
                    # Parse each hint and create proper hint expressions
                    hint_expressions = []
                    for hint in statement_hints:
                        try:
                            # Try to parse hint as an expression (e.g., "INDEX(users idx_name)")
                            hint_str = str(hint)  # Ensure hint is a string
                            hint_expr: Optional[exp.Expression] = exp.maybe_parse(hint_str, dialect=self.dialect_name)
                            if hint_expr:
                                hint_expressions.append(hint_expr)
                            else:
                                # Create a raw identifier for unparsable hints
                                hint_expressions.append(exp.Anonymous(this=hint_str))
                        except Exception:  # noqa: PERF203
                            hint_expressions.append(exp.Anonymous(this=str(hint)))

                    # Create a Hint node and attach to SELECT
                    if hint_expressions:
                        hint_node = exp.Hint(expressions=hint_expressions)
                        modified_expr.set("hint", hint_node)

                # For table-level hints, we'll fall back to comment injection in SQL
                # since SQLGlot doesn't have a standard way to attach hints to individual tables
                modified_sql = modified_expr.sql(dialect=self.dialect_name, pretty=True)

                # Apply table-level hints via string manipulation (as fallback)
                table_hints = [h for h in self._hints if h.get("location") == "table" and h.get("table")]
                if table_hints:
                    for th in table_hints:
                        table = str(th["table"])
                        hint = th["hint"]
                        # More precise regex that captures the table and optional alias
                        pattern = rf"\b{re.escape(table)}\b(\s+AS\s+\w+)?"

                        def replacement_func(match: re.Match[str]) -> str:
                            alias_part = match.group(1) or ""
                            return f"/*+ {hint} */ {table}{alias_part}"  # noqa: B023

                        modified_sql = re.sub(pattern, replacement_func, modified_sql, flags=re.IGNORECASE, count=1)

                return SafeQuery(sql=modified_sql, parameters=safe_query.parameters, dialect=safe_query.dialect)

        return safe_query
