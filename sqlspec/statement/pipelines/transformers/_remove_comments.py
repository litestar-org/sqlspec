import contextlib
import re
from typing import Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult
from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("CommentRemover",)

# Combined regex pattern that properly handles string literals and avoids false positives
COMMENT_REMOVAL_PATTERN = re.compile(
    r"""
    # String literals (must be matched first to avoid false positives)
    (?P<dquote>"(?:[^"\\]|\\.)*") |                             # Double-quoted strings
    (?P<squote>'(?:[^'\\]|\\.)*') |                             # Single-quoted strings
    (?P<dollar_quoted>\$(?P<tag>\w*)?\$[\s\S]*?\$\2\$) |       # Dollar-quoted strings (PostgreSQL)

    # SQL hints (preserve based on configuration)
    (?P<oracle_hint>/\*\+[^*]*(?:\*(?!/)[^*]*)*\*/) |          # Oracle hints /*+ ... */

    # MySQL version comments (conditional execution comments)
    (?P<mysql_version>/\*!\d{5}[^*]*(?:\*(?!/)[^*]*)*\*/) |    # MySQL version /*! ... */

    # Regular comments (safe to remove)
    (?P<line_comment>--[^\r\n]*) |                             # Line comments
    (?P<block_comment>/\*[^*]*(?:\*(?!/)[^*]*)*\*/)            # Block comments
    """,
    re.VERBOSE | re.MULTILINE | re.DOTALL,
)


class CommentRemover(ProcessorProtocol[exp.Expression]):
    """Removes comments from SQL expressions for security and normalization.

    This transformer removes SQL comments while preserving functionality:
    - Removes line comments (-- comment)
    - Removes block comments (/* comment */)
    - Preserves string literals that contain comment-like patterns
    - Configurable preservation of hints and MySQL version comments

    Args:
        enabled: Whether comment removal is enabled.
        preserve_hints: Whether to preserve Oracle-style hints (/*+ hint */).
        preserve_mysql_version_comments: Whether to preserve MySQL /*!50000 */ style comments.
        strict_removal: Whether to remove all comments including hints and version comments.
    """

    def __init__(
        self,
        enabled: bool = True,
        preserve_hints: bool = True,
        preserve_mysql_version_comments: bool = False,
        strict_removal: bool = False,
    ) -> None:
        self.enabled = enabled
        self.preserve_hints = preserve_hints
        self.preserve_mysql_version_comments = preserve_mysql_version_comments
        self.strict_removal = strict_removal

    def process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]:
        """Process the expression to remove comments, using the context."""
        assert context.current_expression is not None, "CommentRemover expects a valid current_expression in context."

        if not self.enabled:
            return context.current_expression, None

        original_sql = context.current_expression.sql(dialect=context.dialect)
        cleaned_sql, removed_count = self._remove_comments_from_sql(original_sql)

        if removed_count > 0:
            with contextlib.suppress(Exception):
                import sqlglot

                cleaned_expression = sqlglot.parse_one(cleaned_sql, dialect=context.dialect)
                context.current_expression = cleaned_expression

        return context.current_expression, None

    def _remove_comments_from_sql(self, sql: str) -> tuple[str, int]:
        """Remove comments from SQL string using regex pattern."""
        removed_count = 0

        def replace_match(match: re.Match[str]) -> str:
            nonlocal removed_count

            if match.group("dquote") or match.group("squote") or match.group("dollar_quoted"):
                return match.group(0)

            if match.group("oracle_hint"):
                if self.strict_removal or not self.preserve_hints:
                    removed_count += 1
                    return " "
                return match.group(0)

            if match.group("mysql_version"):
                if self.strict_removal or not self.preserve_mysql_version_comments:
                    removed_count += 1
                    return " "
                return match.group(0)

            if match.group("line_comment") or match.group("block_comment"):
                removed_count += 1
                return " "

            return match.group(0)

        cleaned_sql = COMMENT_REMOVAL_PATTERN.sub(replace_match, sql)
        cleaned_sql = re.sub(r"\s+", " ", cleaned_sql).strip()

        return cleaned_sql, removed_count
