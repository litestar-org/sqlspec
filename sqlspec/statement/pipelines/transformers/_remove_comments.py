from typing import Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("CommentRemover",)


class CommentRemover(ProcessorProtocol):
    """Removes standard SQL comments from expressions using SQLGlot's AST traversal.

    This transformer removes SQL comments while preserving functionality:
    - Removes line comments (-- comment)
    - Removes block comments (/* comment */)
    - Preserves string literals that contain comment-like patterns
    - Always preserves SQL hints and MySQL version comments (use HintRemover separately)
    - Uses SQLGlot's AST for reliable, context-aware comment detection

    Note: This transformer now focuses only on standard comments. Use HintRemover
    separately if you need to remove Oracle hints (/*+ hint */) or MySQL version
    comments (/*!50000 */).

    Args:
        enabled: Whether comment removal is enabled.
    """

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled

    def process(self, expression: Optional[exp.Expression], context: SQLProcessingContext) -> Optional[exp.Expression]:
        """Process the expression to remove comments using SQLGlot AST traversal."""
        if not self.enabled or expression is None or context.current_expression is None:
            return expression

        comments_removed_count = 0

        def _remove_comments(node: exp.Expression) -> "Optional[exp.Expression]":
            nonlocal comments_removed_count
            if hasattr(node, "comments") and node.comments:
                original_comment_count = len(node.comments)
                comments_to_keep = []

                for comment in node.comments:
                    comment_text = str(comment).strip()
                    hint_keywords = ["INDEX", "USE_NL", "USE_HASH", "PARALLEL", "FULL", "FIRST_ROWS", "ALL_ROWS"]
                    is_hint = any(keyword in comment_text.upper() for keyword in hint_keywords)

                    if is_hint or (comment_text.startswith("!") and comment_text.endswith("")):
                        comments_to_keep.append(comment)

                if len(comments_to_keep) < original_comment_count:
                    comments_removed_count += original_comment_count - len(comments_to_keep)
                    node.pop_comments()
                    if comments_to_keep:
                        node.add_comments(comments_to_keep)

            return node

        cleaned_expression = context.current_expression.transform(_remove_comments, copy=True)
        context.current_expression = cleaned_expression

        context.metadata["comments_removed"] = comments_removed_count

        return cleaned_expression
