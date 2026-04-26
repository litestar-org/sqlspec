from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

__all__ = ("CommitMode", "SanicConfigState")

CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]


@dataclass
class SanicConfigState:
    """Internal state for a Sanic database configuration.

    Tracks the keys and behavior needed to bind one SQLSpec config into a
    Sanic app and its request context.
    """

    config: "DatabaseConfigProtocol[Any, Any, Any]"
    connection_key: str
    pool_key: str
    session_key: str
    commit_mode: CommitMode
    extra_commit_statuses: "set[int] | None"
    extra_rollback_statuses: "set[int] | None"
    disable_di: bool
    enable_correlation_middleware: bool = False
    correlation_header: str = "x-request-id"
    correlation_headers: "tuple[str, ...] | None" = None
    auto_trace_headers: bool = True
    enable_sqlcommenter_middleware: bool = True
    sqlcommenter_framework: str = "sanic"

    def __post_init__(self) -> None:
        """Validate status configuration."""
        extra_commit_statuses = self.extra_commit_statuses or set()
        extra_rollback_statuses = self.extra_rollback_statuses or set()
        if extra_commit_statuses & extra_rollback_statuses:
            msg = "Extra rollback statuses and commit statuses must not share any status codes"
            raise ImproperConfigurationError(msg)
