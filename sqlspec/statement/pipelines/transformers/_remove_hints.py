"""Removes SQL hints from expressions."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("HintRemover",)


class HintRemover(ProcessorProtocol):
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
        self, enabled: bool = True, remove_oracle_hints: bool = True, remove_mysql_version_comments: bool = True
    ) -> None:
        self.enabled = enabled
        self.remove_oracle_hints = remove_oracle_hints
        self.remove_mysql_version_comments = remove_mysql_version_comments

    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        """Removes SQL hints from the expression using SQLGlot AST traversal."""
        if not self.enabled or expression is None or context.current_expression is None:
            return expression

        hints_removed_count = 0

        def _remove_hint_node(node: exp.Expression) -> "Optional[exp.Expression]":
            nonlocal hints_removed_count
            if isinstance(node, exp.Hint):
                hints_removed_count += 1
                return None

            if hasattr(node, "comments") and node.comments:
                original_comment_count = len(node.comments)
                comments_to_keep = []
                for comment in node.comments:
                    comment_text = str(comment).strip()
                    hint_keywords = ["INDEX", "USE_NL", "USE_HASH", "PARALLEL", "FULL", "FIRST_ROWS", "ALL_ROWS"]
                    is_oracle_hint = any(keyword in comment_text.upper() for keyword in hint_keywords)

                    if is_oracle_hint:
                        if self.remove_oracle_hints:
                            continue
                    elif comment_text.startswith("!") and self.remove_mysql_version_comments:
                        continue

                    comments_to_keep.append(comment)

                if len(comments_to_keep) < original_comment_count:
                    hints_removed_count += original_comment_count - len(comments_to_keep)
                    node.pop_comments()
                    if comments_to_keep:
                        node.add_comments(comments_to_keep)
            return node

        transformed_expression = context.current_expression.transform(_remove_hint_node, copy=True)
        context.current_expression = transformed_expression or exp.Anonymous(this="")

        context.metadata["hints_removed"] = hints_removed_count

        return context.current_expression
