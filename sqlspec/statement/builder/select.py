"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError
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
    SelectColumnsMixin,
    SetOperationMixin,
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
):
    """Builds SELECT queries."""

    _with_parts: "dict[str, Union[exp.CTE, SelectBuilder]]" = field(default_factory=dict, init=False)

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
        if self._expression is None:
            self._expression = exp.Select()
        return self._expression

    def order_by(self, *items: "Union[str, exp.Ordered]") -> "Self":
        """Add ORDER BY clause.

        Args:
            *items: Columns to order by. Can be strings (column names) or
                    sqlglot.exp.Ordered instances for specific directions (e.g., exp.column("name").desc()).

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or if the item type is unsupported.

        Returns:
            SelectBuilder: The current builder instance for method chaining.
        """
        if not isinstance(self._expression, exp.Select):
            msg = "Order by can only be applied to a SELECT expression."
            raise SQLBuilderError(msg)

        current_expr = self._expression
        for item in items:
            order_item = exp.column(item).asc() if isinstance(item, str) else item
            current_expr = current_expr.order_by(order_item, copy=False)
        self._expression = current_expr
        return self
