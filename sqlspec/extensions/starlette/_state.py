from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol

__all__ = ("CommitMode", "SQLSpecConfigState")

CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]


@dataclass
class SQLSpecConfigState:
    """Internal state for each database configuration.

    Tracks all configuration parameters needed for middleware and session management.
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
