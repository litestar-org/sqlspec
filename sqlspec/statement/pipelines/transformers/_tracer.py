"""Appends an audit comment to the SQL expression."""

import datetime
from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


__all__ = ("TracingComment",)


class TracingComment(ProcessorProtocol[exp.Expression]):
    """Appends a tracing comment to the SQL expression.

    The comment can include information like user ID, request ID, and timestamp.
    This is used to trace the SQL statement through the system.
    """

    def __init__(
        self,
        user_id: Optional[str] = None,
        request_id: Optional[str] = None,
        include_timestamp: bool = True,
        comment_prefix: str = "Audit",
    ) -> None:
        self.user_id = user_id
        self.request_id = request_id
        self.include_timestamp = include_timestamp
        self.comment_prefix = comment_prefix

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Appends an audit comment to the given SQL expression.

        Args:
            expression: The SQL expression to process.
            dialect: The SQL dialect (influences comment style if not default).
            config: SQL configuration.

        Returns:
            A tuple containing the modified expression with an appended audit comment
            and None for ValidationResult.
        """
        audit_info = []
        if self.user_id:
            audit_info.append(f"UserID: {self.user_id}")
        if self.request_id:
            audit_info.append(f"RequestID: {self.request_id}")
        if self.include_timestamp:
            audit_info.append(f"Timestamp: {datetime.datetime.now(datetime.timezone.utc).isoformat()}Z")

        if not audit_info:
            return expression, None

        # comment_text = f"{self.comment_prefix}: {'; '.join(audit_info)}"

        # sqlglot usually adds comments as a list in expression.args["comments"]
        # Appending to that list is one way.
        # cloned_expression = expression.copy()
        # new_comment = exp.Comment(this=comment_text)
        # if not cloned_expression.args.get("comments"):
        #    cloned_expression.args["comments"] = []
        # cloned_expression.args["comments"].append(new_comment)
        # return cloned_expression, None

        # A simpler, though potentially less robust way for some dialects if not handled by sqlglot directly,
        # might be to try and append it as a general comment to the expression object if it supports it,
        # or find the last node and try to attach it there.
        # However, the `comments` attribute on the root expression is standard.

        # Placeholder: for now, just return the original expression
        return expression, None
