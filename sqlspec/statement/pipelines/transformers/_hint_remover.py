# ruff: noqa: PLR6301
"""Removes hints from a SQL expression."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


__all__ = ("HintRemover",)


class HintRemover(ProcessorProtocol[exp.Expression]):
    """Removes hints from a SQL expression."""

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Removes all hints from the given SQL expression.

        Args:
            expression: The SQL expression to process.
            dialect: The SQL dialect.
            config: SQL configuration.

        Returns:
            A tuple containing the modified expression without hints and None for ValidationResult.
        """
        # Placeholder: Actual hint removal logic would go here.
        # Example using sqlglot (this may vary based on hint types and dialect specifics):
        # expression_without_hints = expression.copy()
        # for hint_node in list(expression_without_hints.find_all(exp.Hint)):
        #    if hint_node.parent:
        #        hint_node.parent.pop(hint_node.arg_key)
        # return expression_without_hints, None
        return expression, None
