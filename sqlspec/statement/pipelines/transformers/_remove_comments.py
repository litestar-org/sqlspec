import re
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from sqlspec.statement.pipelines.base import SQLTransformer, TransformationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig

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


class CommentRemover(SQLTransformer):
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

    def transform(
        self,
        expression: exp.Expression,
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: Any,
    ) -> TransformationResult:
        """Transform the expression to remove comments."""
        if not self.enabled:
            return TransformationResult(expression=expression, modified=False, notes=["Comment removal is disabled"])

        # Get the SQL representation to work with comments
        original_sql = expression.sql(dialect=dialect)

        # Remove comments using regex pattern
        cleaned_sql, removed_count = self._remove_comments_from_sql(original_sql)

        if removed_count > 0:
            # Parse the cleaned SQL back into an expression
            try:
                import sqlglot

                cleaned_expression = sqlglot.parse_one(cleaned_sql, dialect=dialect)
                return TransformationResult(
                    expression=cleaned_expression, modified=True, notes=[f"Removed {removed_count} comments from SQL"]
                )
            except Exception as e:
                # If parsing fails, return original with note
                return TransformationResult(
                    expression=expression, modified=False, notes=[f"Comment removal failed: {e!s}"]
                )

        return TransformationResult(expression=expression, modified=False, notes=["No comments found to remove"])

    def _remove_comments_from_sql(self, sql: str) -> tuple[str, int]:
        """Remove comments from SQL string using regex pattern."""
        removed_count = 0

        def replace_match(match: re.Match[str]) -> str:
            nonlocal removed_count

            # Preserve string literals
            if match.group("dquote") or match.group("squote") or match.group("dollar_quoted"):
                return match.group(0)

            # Handle hints based on configuration
            if match.group("oracle_hint"):
                if self.strict_removal or not self.preserve_hints:
                    removed_count += 1
                    return " "  # Replace with space to maintain structure
                return match.group(0)  # Preserve hint

            # Handle MySQL version comments
            if match.group("mysql_version"):
                if self.strict_removal or not self.preserve_mysql_version_comments:
                    removed_count += 1
                    return " "  # Replace with space to maintain structure
                return match.group(0)  # Preserve version comment

            # Remove regular comments
            if match.group("line_comment") or match.group("block_comment"):
                removed_count += 1
                return " "  # Replace with space to maintain structure

            return match.group(0)

        cleaned_sql = COMMENT_REMOVAL_PATTERN.sub(replace_match, sql)

        # Clean up extra whitespace
        cleaned_sql = re.sub(r"\s+", " ", cleaned_sql).strip()

        return cleaned_sql, removed_count
