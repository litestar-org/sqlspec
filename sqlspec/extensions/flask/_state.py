"""Flask configuration state management."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

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

        if self.commit_mode == "manual":
            return False

        if self.commit_mode == "autocommit":
            return 200 <= status_code < 300

        if self.commit_mode == "autocommit_include_redirect":
            return 200 <= status_code < 400

        return False

    def should_rollback(self, status_code: int) -> bool:
        """Determine if HTTP status code should trigger rollback.

        In autocommit modes, anything that doesn't commit should rollback.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if status should trigger rollback, False otherwise.
        """
        if self.commit_mode == "manual":
            return False

        return not self.should_commit(status_code)
