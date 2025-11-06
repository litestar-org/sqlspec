"""Configuration objects for the observability suite."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from sqlspec.config import LifecycleConfig
    from sqlspec.observability._observer import StatementEvent


StatementObserver = Callable[["StatementEvent"], None]


@dataclass(slots=True)
class RedactionConfig:
    """Controls SQL and parameter redaction before observers run."""

    mask_parameters: bool | None = None
    mask_literals: bool | None = None
    parameter_allow_list: tuple[str, ...] | None = None

    def copy(self) -> "RedactionConfig":
        """Return a copy to avoid sharing mutable state."""

        allow_list = tuple(self.parameter_allow_list) if self.parameter_allow_list else None
        return RedactionConfig(
            mask_parameters=self.mask_parameters, mask_literals=self.mask_literals, parameter_allow_list=allow_list
        )


@dataclass(slots=True)
class TelemetryConfig:
    """Span emission and tracer provider settings."""

    enable_spans: bool = False
    provider_factory: Callable[[], Any] | None = None
    resource_attributes: dict[str, Any] | None = None

    def copy(self) -> "TelemetryConfig":
        """Return a shallow copy preserving optional dictionaries."""

        attributes = dict(self.resource_attributes) if self.resource_attributes else None
        return TelemetryConfig(
            enable_spans=self.enable_spans, provider_factory=self.provider_factory, resource_attributes=attributes
        )


@dataclass(slots=True)
class ObservabilityConfig:
    """Aggregates lifecycle hooks, observers, and telemetry toggles."""

    lifecycle: "LifecycleConfig | None" = None
    print_sql: bool | None = None
    statement_observers: tuple[StatementObserver, ...] | None = None
    telemetry: "TelemetryConfig | None" = None
    redaction: "RedactionConfig | None" = None

    def __post_init__(self) -> None:
        if self.statement_observers is not None:
            self.statement_observers = tuple(self.statement_observers)

    def copy(self) -> "ObservabilityConfig":
        """Return a deep copy of the configuration."""

        lifecycle_copy = _normalize_lifecycle(self.lifecycle)
        observers = tuple(self.statement_observers) if self.statement_observers else None
        telemetry_copy = self.telemetry.copy() if self.telemetry else None
        redaction_copy = self.redaction.copy() if self.redaction else None
        return ObservabilityConfig(
            lifecycle=lifecycle_copy,
            print_sql=self.print_sql,
            statement_observers=observers,
            telemetry=telemetry_copy,
            redaction=redaction_copy,
        )

    @classmethod
    def merge(
        cls, base_config: "ObservabilityConfig | None", override_config: "ObservabilityConfig | None"
    ) -> "ObservabilityConfig":
        """Merge registry-level and adapter-level configuration objects."""

        if base_config is None and override_config is None:
            return cls()

        base = base_config.copy() if base_config else cls()
        override = override_config
        if override is None:
            return base

        lifecycle = _merge_lifecycle(base.lifecycle, override.lifecycle)
        observers: tuple[StatementObserver, ...] | None
        if base.statement_observers and override.statement_observers:
            observers = base.statement_observers + tuple(override.statement_observers)
        elif override.statement_observers:
            observers = tuple(override.statement_observers)
        else:
            observers = base.statement_observers

        print_sql = base.print_sql
        if override.print_sql is not None:
            print_sql = override.print_sql

        telemetry = override.telemetry.copy() if override.telemetry else base.telemetry
        redaction = _merge_redaction(base.redaction, override.redaction)

        return ObservabilityConfig(
            lifecycle=lifecycle,
            print_sql=print_sql,
            statement_observers=observers,
            telemetry=telemetry,
            redaction=redaction,
        )


def _merge_redaction(base: "RedactionConfig | None", override: "RedactionConfig | None") -> "RedactionConfig | None":
    if base is None and override is None:
        return None
    if override is None:
        return base.copy() if base else None
    if base is None:
        return override.copy()
    merged = base.copy()
    if override.mask_parameters is not None:
        merged.mask_parameters = override.mask_parameters
    if override.mask_literals is not None:
        merged.mask_literals = override.mask_literals
    if override.parameter_allow_list is not None:
        merged.parameter_allow_list = tuple(override.parameter_allow_list)
    return merged


def _normalize_lifecycle(config: "LifecycleConfig | None") -> "LifecycleConfig | None":
    if config is None:
        return None
    normalized: dict[str, list[Any]] = {}
    for event, hooks in config.items():
        normalized[event] = list(cast("Iterable[Any]", hooks))
    return cast("LifecycleConfig", normalized)


def _merge_lifecycle(base: "LifecycleConfig | None", override: "LifecycleConfig | None") -> "LifecycleConfig | None":
    if base is None and override is None:
        return None
    if base is None:
        return _normalize_lifecycle(override)
    if override is None:
        return _normalize_lifecycle(base)
    merged_dict: dict[str, list[Any]] = cast("dict[str, list[Any]]", _normalize_lifecycle(base)) or {}
    for event, hooks in override.items():
        merged_dict.setdefault(event, [])
        merged_dict[event].extend(cast("Iterable[Any]", hooks))
    return cast("LifecycleConfig", merged_dict)


__all__ = ("ObservabilityConfig", "RedactionConfig", "StatementObserver", "TelemetryConfig")
