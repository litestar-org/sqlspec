"""Flask configuration state management."""

from dataclasses import dataclass
from typing import Any, Literal

from sqlspec.config import DatabaseConfigProtocol
from sqlspec.exceptions import ImproperConfigurationError

__all__ = ("FlaskConfigState",)

HTTP_200_OK = 200
HTTP_300_MULTIPLE_CHOICES = 300
HTTP_400_BAD_REQUEST = 400


@dataclass
class FlaskConfigState:
    """Internal state for each database configuration in Flask extension.

    Holds configuration, state keys, commit settings, and transaction logic.
    """

    config: DatabaseConfigProtocol[Any, Any, Any]
    connection_key: str
    session_key: str
    commit_mode: Literal["manual", "autocommit", "autocommit_include_redirect"]
    extra_commit_statuses: "set[int] | None"
    extra_rollback_statuses: "set[int] | None"
    is_async: bool
    disable_di: bool
    enable_correlation_middleware: bool = False
    correlation_header: str = "x-request-id"
    correlation_headers: "tuple[str, ...] | None" = None
    auto_trace_headers: bool = True
    enable_sqlcommenter_middleware: bool = True

    def __post_init__(self) -> None:
        """Validate status configuration."""
        if (self.extra_commit_statuses or set()) & (self.extra_rollback_statuses or set()):
            msg = "Extra rollback statuses and commit statuses must not share any status codes"
            raise ImproperConfigurationError(msg)

    def should_commit(self, status_code: int) -> bool:
        """Determine if HTTP status code should trigger commit.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if status should trigger commit, False otherwise.
        """
        if self.extra_commit_statuses and status_code in self.extra_commit_statuses:
            return True
        if self.extra_rollback_statuses and status_code in self.extra_rollback_statuses:
            return False
        if self.commit_mode == "autocommit":
            return HTTP_200_OK <= status_code < HTTP_300_MULTIPLE_CHOICES
        if self.commit_mode == "autocommit_include_redirect":
            return HTTP_200_OK <= status_code < HTTP_400_BAD_REQUEST
        return False

    def should_rollback(self, status_code: int) -> bool:
        """Determine if HTTP status code should trigger rollback.

        In autocommit modes, anything that doesn't commit should rollback.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if status should trigger rollback, False otherwise.
        """
        return self.commit_mode != "manual" and not self.should_commit(status_code)
