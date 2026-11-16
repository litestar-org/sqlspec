"""Tenant identities, context scopes, and request extraction helpers."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.utils.tenant._context import (
        MAX_TENANT_ID_BYTES,
        SQLSpecTenantMiddleware,
        TenantBinding,
        TenantContext,
        TenantContextPayload,
        TenantExtractor,
        TenantExtractorValue,
        TenantRequestBinding,
        TenantScope,
        coerce_tenant_payload,
        is_tenant_bypass_route,
        single_membership_tenant_extractor,
        tenant_bypass,
        tenant_context_var,
    )
    from sqlspec.utils.tenant._cursor import resolve_cursor_tenant_identity, tenant_identity_of

__all__ = (
    "MAX_TENANT_ID_BYTES",
    "SQLSpecTenantMiddleware",
    "TenantBinding",
    "TenantContext",
    "TenantContextPayload",
    "TenantExtractor",
    "TenantExtractorValue",
    "TenantRequestBinding",
    "TenantScope",
    "coerce_tenant_payload",
    "is_tenant_bypass_route",
    "resolve_cursor_tenant_identity",
    "single_membership_tenant_extractor",
    "tenant_bypass",
    "tenant_context_var",
    "tenant_identity_of",
)

_CURSOR_EXPORTS = frozenset({"resolve_cursor_tenant_identity", "tenant_identity_of"})
_EXPORTS: dict[str, tuple[str, str]] = {
    name: ("sqlspec.utils.tenant._cursor" if name in _CURSOR_EXPORTS else "sqlspec.utils.tenant._context", name)
    for name in __all__
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
