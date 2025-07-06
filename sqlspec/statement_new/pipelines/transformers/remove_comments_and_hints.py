"""Removes SQL comments and hints from expressions."""

from typing import Optional

from sqlglot import exp

from sqlspec.statement_new.protocols import ProcessorPhase, SQLProcessingContext, SQLProcessor

__all__ = ("CommentAndHintRemover",)


class CommentAndHintRemover(SQLProcessor):
    """Removes SQL comments and hints from expressions using SQLGlot's AST traversal."""

    phase = ProcessorPhase.TRANSFORM

    def __init__(self, enabled: bool = True, remove_comments: bool = True, remove_hints: bool = False) -> None:
        self.enabled = enabled
        self.remove_comments = remove_comments
        self.remove_hints = remove_hints

    def process(self, context: "SQLProcessingContext") -> "SQLProcessingContext":
        if not self.enabled or context.current_expression is None:
            return context

        expression = context.current_expression
        comments_removed_count = 0
        hints_removed_count = 0

        def _remove_comments_and_hints(node: exp.Expression) -> "Optional[exp.Expression]":
            nonlocal comments_removed_count, hints_removed_count
            if self.remove_hints and isinstance(node, exp.Hint):
                hints_removed_count += 1
                return None
            if hasattr(node, "comments") and node.comments:
                original_comment_count = len(node.comments)
                comments_to_keep = [c for c in node.comments if self._is_hint(str(c)) and not self.remove_hints]
                comments_removed_count += original_comment_count - len(comments_to_keep)
                node.comments = comments_to_keep or None
            return node

        cleaned_expression = expression.transform(_remove_comments_and_hints, copy=True)
        context.metadata["comments_removed"] = comments_removed_count
        context.metadata["hints_removed"] = hints_removed_count
        context.current_expression = cleaned_expression
        return context

    def _is_hint(self, comment_text: str) -> bool:
        hint_keywords = ["INDEX", "USE_NL", "USE_HASH", "PARALLEL", "FULL", "FIRST_ROWS", "ALL_ROWS"]
        return any(keyword in comment_text.upper() for keyword in hint_keywords) or (
            comment_text.startswith("!") and comment_text.endswith("")
        )
