"""Tenant identities carried in pagination cursor signing keys."""

from collections.abc import Iterable
from typing import Any

from sqlspec.utils.tenant._context import TenantContext, TenantContextPayload

__all__ = ("resolve_cursor_tenant_identity", "tenant_identity_of")


def tenant_identity_of(payload: "TenantContextPayload | None") -> str:
    """Return the tenant identifier carried by a tenant payload.

    Args:
        payload: Request tenant payload, or ``None``.

    Returns:
        The tenant identifier, or an empty string without a tenant or for a bypass payload.
    """
    if payload is None or payload.is_bypass:
        return ""
    return str(payload.tenant_id)


async def resolve_cursor_tenant_identity(tenant_configs: Iterable[Any]) -> str:
    """Return the cursor key suffix naming the request tenant of every tenant-enabled configuration.

    Args:
        tenant_configs: Tenant-enabled configurations in registration order.

    Returns:
        An empty string without tenant-enabled configurations, otherwise a suffix carrying each tenant.
    """
    configs = tuple(tenant_configs)
    if not configs:
        return ""
    if len(configs) == 1:
        return f"|tenant:{tenant_identity_of(await TenantContext.get_async(configs[0]))}"
    parts: list[str] = []
    for index, config in enumerate(configs):
        label = getattr(config, "bind_key", None) or str(index)
        parts.append(f"{label}={tenant_identity_of(await TenantContext.get_async(config))}")
    return "|tenants:" + ",".join(parts)
