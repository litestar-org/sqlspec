# ruff: noqa: PLR6301
"""Ensures DELETE/UPDATE statements have a WHERE clause."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


__all__ = ("ForceWhereClause",)


class ForceWhereClause(ProcessorProtocol[exp.Expression]):
    """Adds a `WHERE FALSE` or `WHERE 1=0` clause to DELETE/UPDATE statements
    if they don't have one and a strict mode is enabled.

    This is a safety mechanism to prevent accidental mass data modification.
    """

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Adds a restrictive WHERE clause if necessary.

        Args:
            expression: The SQL expression to process.
            dialect: The SQL dialect.
            config: SQL configuration (e.g., to check for a strict mode).

        Returns:
            A tuple containing the potentially modified expression and None for ValidationResult.
        """
        # Placeholder: Actual logic to check for DELETE/UPDATE and add WHERE if needed.
        # if isinstance(expression, (exp.Delete, exp.Update)) and not expression.args.get("where"):
        #     if config and getattr(config, "strict_dml_where", False): # Example config check

        return expression, None
