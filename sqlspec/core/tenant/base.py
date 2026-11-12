"""Structural tenant strategy contracts and acquisition state."""

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, TypeAlias

from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError

if TYPE_CHECKING:
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = (
    "AsyncTenantStrategy",
    "SyncTenantStrategy",
    "TenantLeaseState",
    "TenantStrategy",
    "TenantStrategyShorthand",
    "require_session_pooler",
    "validate_session_setting_name",
    "validate_tenant_session_baseline",
    "with_cause",
)

_SESSION_SETTING_NAME: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")


class TenantLeaseState:
    """Capture the payload and a read-only baseline for one acquisition."""

    __slots__ = ("baseline", "payload")

    def __init__(self, payload: "TenantContextPayload", baseline: Mapping[str, str | None] | None = None) -> None:
        self.payload = payload
        self.baseline: Mapping[str, str | None] = MappingProxyType(dict(baseline or {}))


class SyncTenantStrategy(Protocol):
    """Apply and restore tenant state on a synchronous connection."""

    __slots__ = ()

    def checkout(
        self, connection: Any, payload: "TenantContextPayload"
    ) -> TenantLeaseState | Mapping[str, str | None]: ...
    def checkin(self, connection: Any, state: TenantLeaseState) -> None: ...


class AsyncTenantStrategy(Protocol):
    """Apply and restore tenant state on an asynchronous connection."""

    __slots__ = ()

    async def checkout(
        self, connection: Any, payload: "TenantContextPayload"
    ) -> TenantLeaseState | Mapping[str, str | None]: ...
    async def checkin(self, connection: Any, state: TenantLeaseState) -> None: ...


TenantStrategy: TypeAlias = SyncTenantStrategy | AsyncTenantStrategy
TenantStrategyShorthand: TypeAlias = (
    Literal["default", "rls", "session_context", "identifier", "rcac", "global_variable"] | bool
)


def validate_session_setting_name(name: object) -> str:
    """Return a session setting name containing only identifier characters.

    Raises:
        ImproperConfigurationError: The setting name is invalid.
    """
    if not isinstance(name, str) or _SESSION_SETTING_NAME.fullmatch(name) is None:
        msg = "Tenant session setting names must match [A-Za-z_][A-Za-z0-9_.]*"
        raise ImproperConfigurationError(msg)
    return name


def validate_tenant_session_baseline(baseline: object, server_settings: Mapping[str, str] | None) -> dict[str, str]:
    """Copy validated baseline settings that agree with connection settings.

    Raises:
        ImproperConfigurationError: Baseline settings are invalid or conflict.
    """
    if not isinstance(baseline, Mapping):
        msg = "Tenant session baseline must be a mapping"
        raise ImproperConfigurationError(msg)
    validated: dict[str, str] = {}
    for key, value in baseline.items():
        name = validate_session_setting_name(key)
        if not isinstance(value, str) or "\x00" in value:
            msg = "Tenant session baseline values must be NUL-free strings"
            raise ImproperConfigurationError(msg)
        if server_settings is not None and name in server_settings and server_settings[name] != value:
            msg = f"Tenant session baseline conflicts with server setting {name!r}"
            raise ImproperConfigurationError(msg)
        validated[name] = value
    return validated


def require_session_pooler(pooler_mode: object, adapter: str) -> str:
    """Require connection session affinity for the configured tenant strategy.

    Raises:
        ImproperConfigurationError: An explicit pooler mode is not session mode.
    """
    if pooler_mode is None or pooler_mode == "session":
        return "session"
    msg = f"{adapter} tenant strategies require pooler_mode='session'"
    raise ImproperConfigurationError(msg)


def with_cause(error: SQLSpecError, cause: BaseException) -> SQLSpecError:
    """Attach an explicit cause that survives compiled raise sites.

    Args:
        error: The tenant error reported to the caller.
        cause: The underlying failure.

    Returns:
        The same error with its cause recorded.
    """
    error.__cause__ = cause
    return error
