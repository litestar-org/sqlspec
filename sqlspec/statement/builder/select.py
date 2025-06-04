"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, Union, cast

from sqlglot import exp

from sqlspec.statement.builder.base import QueryBuilder
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
