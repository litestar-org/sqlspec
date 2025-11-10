"""Optional helpers for enabling OpenTelemetry spans via ObservabilityConfig."""

from collections.abc import Callable
from typing import Any

from sqlspec.observability import ObservabilityConfig, TelemetryConfig
from sqlspec.typing import trace
from sqlspec.utils.module_loader import ensure_opentelemetry

__all__ = ("enable_tracing",)


def _wrap_provider(provider: Any | None) -> Callable[[], Any] | None:
    if provider is None:
        return None

    def _factory() -> Any:
        return provider

    return _factory


def enable_tracing(
    *,
    base_config: ObservabilityConfig | None = None,
    tracer_provider: Any | None = None,
    tracer_provider_factory: Callable[[], Any] | None = None,
    resource_attributes: dict[str, Any] | None = None,
    enable_spans: bool = True,
) -> ObservabilityConfig:
    """Return an ObservabilityConfig with OpenTelemetry spans enabled.

    Args:
        base_config: Existing observability config to extend. When omitted a new instance is created.
        tracer_provider: Optional provider instance to reuse. Mutually exclusive with tracer_provider_factory.
        tracer_provider_factory: Callable that returns a tracer provider when spans are first used.
        resource_attributes: Additional attributes to attach to every span.
        enable_spans: Allow disabling spans while keeping the rest of the config.

    Returns:
        ObservabilityConfig with telemetry options configured for OpenTelemetry.
    """

    ensure_opentelemetry()

    if tracer_provider is not None and tracer_provider_factory is not None:
        msg = "Provide either tracer_provider or tracer_provider_factory, not both"
        raise ValueError(msg)

    telemetry = TelemetryConfig(
        enable_spans=enable_spans,
        provider_factory=tracer_provider_factory or _wrap_provider(tracer_provider) or trace.get_tracer_provider,
        resource_attributes=resource_attributes,
    )

    config = base_config.copy() if base_config else ObservabilityConfig()
    config.telemetry = telemetry
    return config
