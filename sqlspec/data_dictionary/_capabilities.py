"""System metadata capability and safety helpers."""

from collections.abc import Sequence
from typing import Any

from sqlspec.data_dictionary._types import (
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    SystemMetadataCapability,
    SystemMetadataRequest,
    SystemMetadataResult,
)

__all__ = (
    "DEFAULT_SYSTEM_METADATA_DOMAINS",
    "ensure_system_metadata_request",
    "system_metadata_capabilities_from_domains",
    "system_metadata_gated_result",
    "unsupported_system_metadata_capability",
)

DEFAULT_SYSTEM_METADATA_DOMAINS: tuple[str, ...] = (
    "settings",
    "extensions",
    "sessions",
    "statement_history",
    "table_statistics",
    "optimizer_statistics",
    "replication",
    "performance_counters",
    "cloud_jobs",
)
"""Default system and performance metadata domains for capability disclosure."""


def ensure_system_metadata_request(
    request: SystemMetadataRequest | str | None = None, **kwargs: Any
) -> SystemMetadataRequest:
    """Normalize a system metadata request.

    Args:
        request: Explicit request object, domain string, or None.
        **kwargs: Request constructor overrides.

    Returns:
        A system metadata request.
    """
    if isinstance(request, SystemMetadataRequest) and not kwargs:
        return request
    if isinstance(request, SystemMetadataRequest):
        domain = request.domain
        values: dict[str, Any] = {
            "include_system": request.include_system,
            "include_performance": request.include_performance,
            "allow_billed_metadata": request.allow_billed_metadata,
            "allow_license_gated_diagnostics": request.allow_license_gated_diagnostics,
            "include_sensitive": request.include_sensitive,
            "table": request.table,
            "schema": request.schema,
            "redaction_policy": request.redaction_policy,
        }
        values.update(kwargs)
        return SystemMetadataRequest(domain, **values)
    if isinstance(request, str):
        return SystemMetadataRequest(request, **kwargs)
    domain = str(kwargs.pop("domain", "system"))
    return SystemMetadataRequest(domain, **kwargs)


def unsupported_system_metadata_capability(domain: str) -> SystemMetadataCapability:
    """Create the default unsupported system metadata capability.

    Args:
        domain: System metadata domain name.

    Returns:
        Unsupported capability with conservative disclosure defaults.
    """
    return SystemMetadataCapability.unsupported(domain, source=MetadataSource.SYSTEM_VIEW)


def system_metadata_capabilities_from_domains(domains: Sequence[str]) -> tuple[SystemMetadataCapability, ...]:
    """Create unsupported system metadata capabilities for each domain.

    Args:
        domains: Domain names to disclose.

    Returns:
        Tuple of unsupported system metadata capabilities.
    """
    return tuple(unsupported_system_metadata_capability(domain) for domain in domains)


def system_metadata_gated_result(
    request: SystemMetadataRequest, capability: SystemMetadataCapability
) -> SystemMetadataResult:
    """Return the fail-closed result for a system metadata request.

    Args:
        request: System metadata request.
        capability: Capability for the requested domain.

    Returns:
        Result describing whether execution is gated, unsupported, or permitted.
    """
    warnings: list[str] = []
    support = capability.support
    fidelity = capability.fidelity
    if not request.is_enabled:
        support = MetadataSupport.GATED
        fidelity = MetadataFidelity.UNSUPPORTED
        warnings.append("System metadata is disabled by default; pass include_system=True or include_performance=True.")
    elif capability.requires_billed_opt_in and not request.allow_billed_metadata:
        support = MetadataSupport.GATED
        fidelity = MetadataFidelity.UNSUPPORTED
        warnings.append("System metadata domain requires allow_billed_metadata=True.")
    elif capability.requires_license_opt_in and not request.allow_license_gated_diagnostics:
        support = MetadataSupport.GATED
        fidelity = MetadataFidelity.UNSUPPORTED
        warnings.append("System metadata domain requires allow_license_gated_diagnostics=True.")

    if not request.include_sensitive and capability.redaction_fields and MetadataRisk.REDACTED not in capability.risks:
        risks = (*capability.risks, MetadataRisk.REDACTED)
    else:
        risks = capability.risks

    gated_capability = SystemMetadataCapability(
        capability.domain,
        support,
        fidelity=fidelity,
        source=capability.source,
        risks=risks,
        required_privileges=capability.required_privileges,
        cost_implications=capability.cost_implications,
        license_gate=capability.license_gate,
        managed_service_restricted=capability.managed_service_restricted,
        redaction_fields=capability.redaction_fields,
        warnings=tuple(warnings) + capability.warnings,
    )
    return SystemMetadataResult(request, gated_capability, source=gated_capability.source)
