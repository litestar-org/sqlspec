# ruff: noqa: PLR6301
"""Removes SQL hints from expressions."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("HintRemover",)


class HintRemover(ProcessorProtocol[exp.Expression]):
    """Removes SQL hints from expressions using SQLGlot's AST traversal.

    This transformer removes SQL hints while preserving standard comments:
    - Removes Oracle-style hints (/*+ hint */)
    - Removes MySQL version comments (/*!50000 */)
    - Removes formal hint expressions (exp.Hint nodes)
    - Preserves standard comments (-- comment, /* comment */)
    - Uses SQLGlot's AST for reliable, context-aware hint detection

    Args:
        enabled: Whether hint removal is enabled.
        remove_oracle_hints: Whether to remove Oracle-style hints (/*+ hint */).
        remove_mysql_version_comments: Whether to remove MySQL /*!50000 */ style comments.
    """

    def __init__(
        self,
        enabled: bool = True,
        remove_oracle_hints: bool = True,
        remove_mysql_version_comments: bool = True,
    ) -> None:
        self.enabled = enabled
        self.remove_oracle_hints = remove_oracle_hints
        self.remove_mysql_version_comments = remove_mysql_version_comments

    def process(self, context: "SQLProcessingContext") -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Removes SQL hints from the expression using SQLGlot AST traversal.

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

        if not self.enabled:
            return context.current_expression, None

        def _remove_hint_node(node: exp.Expression) -> "Optional[exp.Expression]":
            # Remove formal hint expressions
            if isinstance(node, exp.Hint):
                return None

            # Handle hint comments stored in the comments property
            if hasattr(node, "comments") and node.comments:
                comments_to_keep = []

                for comment in node.comments:
                    comment_text = str(comment).strip()

                    # Check for Oracle hints - SQLGlot may strip the + when parsing
                    hint_keywords = ["INDEX", "USE_NL", "USE_HASH", "PARALLEL", "FULL", "FIRST_ROWS", "ALL_ROWS"]
                    is_oracle_hint = any(keyword in comment_text.upper() for keyword in hint_keywords)

                    # Remove Oracle hints if enabled
                    if is_oracle_hint:
                        if not self.remove_oracle_hints:
                            comments_to_keep.append(comment)
                        # Otherwise, remove this hint comment
                    # Remove MySQL version comments (/*!... */) if enabled
                    elif comment_text.startswith("!"):
                        if not self.remove_mysql_version_comments:
                            comments_to_keep.append(comment)
                        # Otherwise, remove this version comment
                    else:
                        # Keep all other standard comments
                        comments_to_keep.append(comment)

                # Update the comments list
                node.pop_comments()
                if comments_to_keep:
                    node.add_comments(comments_to_keep)

            return node

        transformed_expression = context.current_expression.transform(_remove_hint_node, copy=True)

        if transformed_expression is None:
            context.current_expression = exp.Anonymous(this="")
        else:
            context.current_expression = transformed_expression

        return context.current_expression, None
