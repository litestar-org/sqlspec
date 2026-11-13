"""Flask configuration state management."""

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Final, Literal

from sqlspec.config import DatabaseConfigProtocol
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.tenant import TenantExtractorValue

__all__ = ("FlaskConfigState",)

HTTP_200_OK = 200
HTTP_300_MULTIPLE_CHOICES = 300
HTTP_400_BAD_REQUEST = 400


TENANT_RECEIPT_KEY: Final = "_sqlspec_tenant_context"


def tenant_receipt_key(key: str | None) -> str:
    """Return the request state key holding a tenant receipt for ``key``, or the shared receipt."""
    return TENANT_RECEIPT_KEY if key is None else f"{TENANT_RECEIPT_KEY}_{key}"


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
    tenant_extractor: Callable[..., TenantExtractorValue] | None = None
    tenant_receipt_keys: tuple[str, ...] = field(default=(), init=False, repr=False, compare=False)
    extractor_takes_request: bool = field(default=False, init=False)
    enable_correlation_middleware: bool = True
    correlation_header: str = "x-request-id"
    correlation_headers: "tuple[str, ...] | None" = None
    auto_trace_headers: bool = True
    enable_sqlcommenter_middleware: bool = True
    read_session_key: str = "read_session"
    read_connection_key: str = "read_connection"
    routing_session_key: str | None = None
    default_routing_mode: str = "auto"

    def __post_init__(self) -> None:
        """Validate status configuration and synchronous tenant extractor signature."""
        bind_key = getattr(self.config, "bind_key", None)
        keys = [tenant_receipt_key(self.session_key), tenant_receipt_key(self.connection_key)]
        if isinstance(bind_key, str) and bind_key:
            keys.append(tenant_receipt_key(bind_key))
        keys.append(TENANT_RECEIPT_KEY)
        self.tenant_receipt_keys = tuple(keys)
        if (self.extra_commit_statuses or set()) & (self.extra_rollback_statuses or set()):
            msg = "Extra rollback statuses and commit statuses must not share any status codes"
            raise ImproperConfigurationError(msg)
        if self.tenant_extractor is not None:
            if not callable(self.tenant_extractor):
                msg = "Flask tenant_extractor must be callable"
                raise ImproperConfigurationError(msg)
            if inspect.iscoroutinefunction(self.tenant_extractor) or (
                not inspect.isroutine(self.tenant_extractor)
                and inspect.iscoroutinefunction(type(self.tenant_extractor).__call__)
            ):
                msg = "Flask tenant_extractor must be a synchronous callable"
                raise ImproperConfigurationError(msg)
            extractor_signature = inspect.signature(self.tenant_extractor)
            try:
                extractor_signature.bind()
            except TypeError:
                try:
                    extractor_signature.bind(object())
                except TypeError as exc:
                    msg = "Flask tenant_extractor must accept zero arguments or one request"
                    raise ImproperConfigurationError(msg) from exc
                self.extractor_takes_request = True

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
