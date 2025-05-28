# ruff: noqa: PLR6301
"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, cast

from sqlglot import exp

from sqlspec.sql.builder._base import QueryBuilder

__all__ = ("InsertBuilder",)

logger = logging.getLogger("sqlspec")


@dataclass
class InsertBuilder(QueryBuilder):
    """Builder for INSERT statements."""

    _table: Optional[str] = field(default=None, init=False)
    _columns: list[str] = field(default_factory=list, init=False)

    def _create_base_expression(self) -> exp.Expression:
        """Create a base INSERT expression.

        Returns:
            exp.Expression: A new sqlglot Delete expression.
        """
        return exp.Insert()

    def into(self, table: str) -> "InsertBuilder":
        """Set target table for INSERT.

        Returns:
            InsertBuilder: The current builder instance for method chaining.
        """
        self._table = table
        if not isinstance(self._expression, exp.Insert):
            self._expression = exp.Insert()
        self._expression = cast("exp.Insert", self._expression).into(exp.to_table(table), copy=False)  # type: ignore[attr-defined,redundant-cast]
        return self
