"""Internal configuration state models for the Starlette SQLSpec extension."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Final, Literal

from sqlspec.config import DatabaseConfigProtocol
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.routing import validate_routing_middleware_settings

__all__ = ("CommitMode", "SQLSpecConfigState")

CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]
HTTP_200_OK = 200
HTTP_300_MULTIPLE_CHOICES = 300
HTTP_400_BAD_REQUEST = 400


TENANT_RECEIPT_KEY: Final = "_sqlspec_tenant_context"


def tenant_receipt_key(key: str | None) -> str:
    """Return the request state key holding a tenant receipt for ``key``, or the shared receipt."""
    return TENANT_RECEIPT_KEY if key is None else f"{TENANT_RECEIPT_KEY}_{key}"


@dataclass
class SQLSpecConfigState:
    """Internal state for each database configuration.

    Tracks all configuration parameters needed for middleware and session management.
    """

    config: DatabaseConfigProtocol[Any, Any, Any]
    connection_key: str
    pool_key: str
    session_key: str
    commit_mode: CommitMode
    extra_commit_statuses: "set[int] | None"
    extra_rollback_statuses: "set[int] | None"
    disable_di: bool
    enable_correlation_middleware: bool = True
    correlation_header: str = "x-request-id"
    correlation_headers: "tuple[str, ...] | None" = None
    auto_trace_headers: bool = True
    enable_sqlcommenter_middleware: bool = True
    sqlcommenter_framework: str = "starlette"
    tenant_extractor: Callable[[Any], Any] | None = None
    tenant_receipt_keys: tuple[str, ...] = field(default=(), init=False, repr=False, compare=False)
    read_session_key: str = "read_session"
    read_connection_key: str = "read_connection"
    read_pool_key: str = "read_pool"
    routing_session_key: str | None = None
    default_routing_mode: str = "auto"
    enable_routing_middleware: bool = True
    routing_header: str | None = None
    write_pin_cookie: str | None = None
    routing_secret: str | bytes | None = None
    routing_header_bytes: bytes | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate transaction status overrides."""
        enable_routing, normalized_header, normalized_cookie, normalized_secret = validate_routing_middleware_settings(
            enable_routing_middleware=self.enable_routing_middleware,
            routing_header=self.routing_header,
            write_pin_cookie=self.write_pin_cookie,
            routing_secret=self.routing_secret,
        )
        self.enable_routing_middleware = enable_routing
        self.routing_header = normalized_header
        self.write_pin_cookie = normalized_cookie
        self.routing_secret = normalized_secret
        self.routing_header_bytes = normalized_header.lower().encode("ascii") if normalized_header is not None else None
        bind_key = getattr(self.config, "bind_key", None)
        keys = [tenant_receipt_key(self.session_key), tenant_receipt_key(self.connection_key)]
        if isinstance(bind_key, str) and bind_key:
            keys.append(tenant_receipt_key(bind_key))
        keys.append(TENANT_RECEIPT_KEY)
        self.tenant_receipt_keys = tuple(keys)
        if self.tenant_extractor is not None and not callable(self.tenant_extractor):
            msg = "tenant_extractor must be callable"
            raise ImproperConfigurationError(msg)
        if (self.extra_commit_statuses or set()) & (self.extra_rollback_statuses or set()):
            msg = "Extra rollback statuses and commit statuses must not share any status codes"
            raise ImproperConfigurationError(msg)

    def should_commit(self, status_code: int) -> bool:
        """Return whether a response status should trigger a commit."""
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
        """Return whether a response status should trigger a rollback."""
        return self.commit_mode != "manual" and not self.should_commit(status_code)
