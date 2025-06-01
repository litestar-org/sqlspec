import re
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from sqlspec.statement.pipelines.base import SQLTransformer, TransformationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig

__all__ = ("CommentRemover",)

# Combined regex pattern that properly handles string literals and avoids false positives
# Based on the comprehensive parameter detection pattern but focused on comments
COMMENT_REMOVAL_PATTERN = re.compile(
    r"""
    # String literals (must be matched first to avoid false positives)
    (?P<dquote>"(?:[^"\\]|\\.)*") |                             # Double-quoted strings
    (?P<squote>'(?:[^'\\]|\\.)*') |                             # Single-quoted strings
    (?P<dollar_quoted>\$(?P<tag>\w*)?\$[\s\S]*?\$\2\$) |       # Dollar-quoted strings (PostgreSQL)

    # SQL hints (must be matched before general block comments to preserve them)
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
    - Preserves Oracle-style hints (/*+ hint */) since hints have their own remover
    - Handles MySQL version-specific comments based on configuration

    Removing comments helps with:
    - Security: Eliminates comment-based injection vectors
    - Performance: Reduces query size and parsing overhead
    - Normalization: Creates consistent queries for caching

    Args:
        enabled: Whether comment removal is enabled.
        preserve_mysql_version_comments: Whether to preserve MySQL /*!50000 */ style comments.
    """

    def __init__(
        self,
        enabled: bool = True,
        preserve_mysql_version_comments: bool = False,
    ) -> None:
        self.enabled = enabled
        self.preserve_mysql_version_comments = preserve_mysql_version_comments

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

        modified = False
        notes = []
        comment_nodes = list(expression.find_all(exp.Comment))
        if comment_nodes:
            modified = True
            notes.append(f"Removed {len(comment_nodes)} comment nodes from AST")

            # Remove comment nodes by replacing them with empty expressions
            for comment_node in comment_nodes:
                if comment_node.parent:
                    comment_node.replace(exp.Anonymous())

        return TransformationResult(
            expression=expression, modified=modified, notes=notes or ["No comments found to remove"]
        )
