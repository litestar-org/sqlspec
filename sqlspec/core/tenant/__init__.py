"""Tenant strategy contracts and shared session validation."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.core.tenant.base import (
        AsyncTenantStrategy,
        SyncTenantStrategy,
        TenantLeaseState,
        TenantStrategy,
        TenantStrategyShorthand,
        require_session_pooler,
        validate_session_setting_name,
        validate_tenant_session_baseline,
        with_cause,
    )

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

_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncTenantStrategy": ("sqlspec.core.tenant.base", "AsyncTenantStrategy"),
    "SyncTenantStrategy": ("sqlspec.core.tenant.base", "SyncTenantStrategy"),
    "TenantLeaseState": ("sqlspec.core.tenant.base", "TenantLeaseState"),
    "TenantStrategy": ("sqlspec.core.tenant.base", "TenantStrategy"),
    "TenantStrategyShorthand": ("sqlspec.core.tenant.base", "TenantStrategyShorthand"),
    "require_session_pooler": ("sqlspec.core.tenant.base", "require_session_pooler"),
    "validate_session_setting_name": ("sqlspec.core.tenant.base", "validate_session_setting_name"),
    "validate_tenant_session_baseline": ("sqlspec.core.tenant.base", "validate_tenant_session_baseline"),
    "with_cause": ("sqlspec.core.tenant.base", "with_cause"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
