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
        # Create a copy to avoid modifying the original
        expression_without_hints = expression.copy()

        # Remove different types of hints
        self._remove_optimizer_hints(expression_without_hints)

        return expression_without_hints, None

    def _remove_optimizer_hints(self, expression: exp.Expression) -> None:
        """Remove optimizer hints from the expression."""
        # Find and remove all Hint nodes
        hints_to_remove = list(expression.find_all(exp.Hint))

        for hint in hints_to_remove:
            # Try to remove the hint by replacing it with None or removing from parent
            if hint.parent:
                try:
                    # Try to remove from parent's children
                    for key, value in hint.parent.args.items():
                        if value is hint:
                            hint.parent.args[key] = None
                            break
                except (AttributeError, TypeError):
                    # If we can't remove it cleanly, just skip
                    pass
