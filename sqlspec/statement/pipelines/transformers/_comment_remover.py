# ruff: noqa: PLR6301
"""Removes comments from a SQL expression."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


__all__ = ("CommentRemover",)


class CommentRemover(ProcessorProtocol[exp.Expression]):
    """Removes comments from a SQL expression."""

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Removes all comments from the given SQL expression.

        Args:
            expression: The SQL expression to process.
            dialect: The SQL dialect (not directly used for comment removal by sqlglot).
            config: SQL configuration (not directly used here).

        Returns:
            A tuple containing the modified expression without comments and None for ValidationResult.
        """
        # Placeholder: Actual comment removal logic would go here.
        # For now, it returns the expression as is.
        # A more complete implementation would be:
        # expression_without_comments = expression.copy()
        # for comment_node in list(expression_without_comments.find_all(exp.Comment)):
        #     comment_node.parent.pop(comment_node.arg_key)
        # return expression_without_comments, None
        return expression, None
