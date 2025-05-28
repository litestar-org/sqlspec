"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.statement import ValidationResult, validate_sql

__all__ = (
    "QueryBuilder",
    "SafeQuery",
)

logger = logging.getLogger("sqlspec")


@dataclass
class SafeQuery:
    """A safely constructed SQL query with bound parameters."""

    sql: str
    parameters: dict[str, Any] = field(default_factory=dict)
    dialect: Optional[DialectType] = None
    _validation_result: Optional[ValidationResult] = field(
        init=False, repr=False, hash=False, compare=False, default=None
    )

    def __post_init__(self) -> None:
        """Validate the query after construction."""
        self._validation_result = validate_sql(self.sql, self.dialect)

    @property
    def is_safe(self) -> bool:
        """Check if the query is safe for execution."""
        return self._validation_result.is_safe if self._validation_result else False

    def validate(self) -> ValidationResult:
        """Validate the query and return detailed results.

        Returns:
            ValidationResult: The result of the validation, including risk level and any issues found.
        """
        if self._validation_result is None:
            self._validation_result = validate_sql(self.sql, self.dialect)
        return self._validation_result


@dataclass
class QueryBuilder:
    """Base class for SQL query builders."""

    dialect: Optional[DialectType] = None
    _expression: Optional[exp.Expression] = field(init=False, default=None)
    _parameters: dict[str, Any] = field(default_factory=dict, init=False)
    _parameter_counter: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        """Initialize the builder."""
        self._expression = self._create_base_expression()

    def _create_base_expression(self) -> exp.Expression:
        """Create the base expression for this builder type."""
        msg = "Subclasses must implement _create_base_expression"
        raise NotImplementedError(msg)

    def _add_parameter(self, value: Any, name: Optional[str] = None) -> str:
        """Add a parameter and return the placeholder name.

        Returns:
            str: The name of the parameter placeholder.
        """
        if name is None:
            name = f"param_{self._parameter_counter}"
            self._parameter_counter += 1
        self._parameters[name] = value
        return name

    def add_parameter(self, value: Any, name: Optional[str] = None) -> str:
        """Public method to add a parameter and return the placeholder name.

        Args:
            value: The parameter value to add.
            name: Optional name for the parameter. If not provided, a unique name will be generated.

        Returns:
            str: The name of the parameter placeholder.
        """
        return self._add_parameter(value, name)

    def build(self) -> SafeQuery:
        """Build the final SQL query with validation.

        Raises:
            SQLBuilderError: If no expression has been set.

        Returns:
            SafeQuery: The constructed SQL query with parameters.
        """
        if self._expression is None:
            msg = "No expression to build"
            raise SQLBuilderError(msg)
        sql = self._expression.sql(dialect=self.dialect)
        return SafeQuery(sql=sql, parameters=self._parameters.copy(), dialect=self.dialect)

    def __str__(self) -> str:
        """String representation of the query.

        Returns:
            str: The SQL string representation of the query.
        """
        if self._expression is None:
            return ""
        return self._expression.sql(dialect=self.dialect)
