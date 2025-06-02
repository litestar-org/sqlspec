# ruff: noqa: PLR6301
"""Removes hints from a SQL expression."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("HintRemover",)


class HintRemover(ProcessorProtocol[exp.Expression]):
    """Removes hints from a SQL expression using sqlglot's transform method."""

    def process(self, context: "SQLProcessingContext") -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Removes all hints from the SQL expression in the context.

        Args:
            context (SQLProcessingContext): The processing context containing the current SQL expression.

        Raises:
            ValueError: If the current expression in the context is None.

        Returns:
            tuple[exp.Expression, Optional[ValidationResult]]: A tuple containing the transformed expression and None.

        """
        if context.current_expression is None:
            msg = "HintRemover received no expression in context."
            raise ValueError(msg)

        def _remove_hint_node(node: exp.Expression) -> Optional[exp.Expression]:
            if isinstance(node, exp.Hint):
                return None
            return node

        transformed_expression = context.current_expression.transform(_remove_hint_node, copy=True)

        if transformed_expression is None:
            context.current_expression = exp.Anonymous(this="")
        else:
            context.current_expression = transformed_expression

        return context.current_expression, None
