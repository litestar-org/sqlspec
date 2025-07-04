"""Removes SQL comments and hints from expressions."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.protocols import ProcessorProtocol

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("CommentAndHintRemover",)


class CommentAndHintRemover(ProcessorProtocol):
    """Removes SQL comments and hints from expressions using SQLGlot's AST traversal."""

    def __init__(self, enabled: bool = True, remove_comments: bool = True, remove_hints: bool = False) -> None:
        self.enabled = enabled
        self.remove_comments = remove_comments
        self.remove_hints = remove_hints

    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        if not self.enabled or expression is None:
            return expression

        comments_removed_count = 0
        hints_removed_count = 0

        def _remove_comments_and_hints(node: exp.Expression) -> "Optional[exp.Expression]":
            nonlocal comments_removed_count, hints_removed_count

            if self.remove_hints and isinstance(node, exp.Hint):
                hints_removed_count += 1
                return None

            if hasattr(node, "comments") and node.comments:
                original_comment_count = len(node.comments)
                comments_to_keep = []
                for comment in node.comments:
                    comment_text = str(comment).strip()
                    is_hint = self._is_hint(comment_text)

                    if is_hint:
                        if not self.remove_hints:
                            comments_to_keep.append(comment)
                    elif not self.remove_comments:
                        comments_to_keep.append(comment)

                removed_count = original_comment_count - len(comments_to_keep)
                if removed_count > 0:
                    if self.remove_hints:
                        hints_removed_count += sum(1 for c in node.comments if self._is_hint(str(c).strip()))
                    if self.remove_comments:
                        comments_removed_count += sum(1 for c in node.comments if not self._is_hint(str(c).strip()))

                    node.pop_comments()
                    if comments_to_keep:
                        node.add_comments(comments_to_keep)

            return node

        cleaned_expression = expression.transform(_remove_comments_and_hints, copy=True)

        context.metadata["comments_removed"] = comments_removed_count
        context.metadata["hints_removed"] = hints_removed_count

        return cleaned_expression

    def _is_hint(self, comment_text: str) -> bool:
        hint_keywords = ["INDEX", "USE_NL", "USE_HASH", "PARALLEL", "FULL", "FIRST_ROWS", "ALL_ROWS"]
        return any(keyword in comment_text.upper() for keyword in hint_keywords) or (
            comment_text.startswith("!") and comment_text.endswith("")
        )
