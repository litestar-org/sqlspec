"""Flask configuration state management."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from sqlspec.extensions._framework_common import should_commit, should_rollback, validate_extra_statuses

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

__all__ = ("FlaskConfigState",)


@dataclass
class FlaskConfigState:
    """Internal state for each database configuration in Flask extension.

    Holds configuration, state keys, commit settings, and transaction logic.
    """

    config: "DatabaseConfigProtocol[Any, Any, Any]"
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
        validate_extra_statuses(self.extra_commit_statuses, self.extra_rollback_statuses)

    def should_commit(self, status_code: int) -> bool:
        """Determine if HTTP status code should trigger commit.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if status should trigger commit, False otherwise.
        """
        return should_commit(status_code, self.commit_mode, self.extra_commit_statuses, self.extra_rollback_statuses)

    def should_rollback(self, status_code: int) -> bool:
        """Determine if HTTP status code should trigger rollback.

        In autocommit modes, anything that doesn't commit should rollback.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if status should trigger rollback, False otherwise.
        """
        return should_rollback(status_code, self.commit_mode, self.extra_commit_statuses, self.extra_rollback_statuses)
