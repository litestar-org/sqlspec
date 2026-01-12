"""Configuration objects for the observability suite."""

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from sqlspec.config import LifecycleConfig
    from sqlspec.observability._formatters._base import CloudLogFormatter
    from sqlspec.observability._observer import StatementEvent
    from sqlspec.observability._sampling import SamplingConfig


StatementObserver = Callable[["StatementEvent"], None]
LifecycleHook = Callable[[dict[str, Any]], None]


class RedactionConfig:
    """Controls SQL and parameter redaction before observers run."""

    __slots__ = ("mask_literals", "mask_parameters", "parameter_allow_list")

    def __init__(
        self,
        *,
        mask_parameters: bool | None = None,
        mask_literals: bool | None = None,
        parameter_allow_list: tuple[str, ...] | Iterable[str] | None = None,
    ) -> None:
        self.mask_parameters = mask_parameters
        self.mask_literals = mask_literals
        self.parameter_allow_list = tuple(parameter_allow_list) if parameter_allow_list is not None else None

    def __hash__(self) -> int:  # pragma: no cover - explicit to mirror dataclass behavior
        msg = "RedactionConfig objects are mutable and unhashable"
        raise TypeError(msg)

    def copy(self) -> "RedactionConfig":
        """Return a copy to avoid sharing mutable state."""

        allow_list = tuple(self.parameter_allow_list) if self.parameter_allow_list else None
        return RedactionConfig(
            mask_parameters=self.mask_parameters, mask_literals=self.mask_literals, parameter_allow_list=allow_list
        )

    def __repr__(self) -> str:
        return f"RedactionConfig(mask_parameters={self.mask_parameters!r}, mask_literals={self.mask_literals!r}, parameter_allow_list={self.parameter_allow_list!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RedactionConfig):
            return NotImplemented
        return (
            self.mask_parameters == other.mask_parameters
            and self.mask_literals == other.mask_literals
            and self.parameter_allow_list == other.parameter_allow_list
        )


class TelemetryConfig:
    """Span emission and tracer provider settings."""

    __slots__ = ("enable_spans", "provider_factory", "resource_attributes")

    def __init__(
        self,
        *,
        enable_spans: bool = False,
        provider_factory: Callable[[], Any] | None = None,
        resource_attributes: dict[str, Any] | None = None,
    ) -> None:
        self.enable_spans = enable_spans
        self.provider_factory = provider_factory
        self.resource_attributes = dict(resource_attributes) if resource_attributes else None

    def __hash__(self) -> int:  # pragma: no cover - explicit to mirror dataclass behavior
        msg = "TelemetryConfig objects are mutable and unhashable"
        raise TypeError(msg)

    def copy(self) -> "TelemetryConfig":
        """Return a shallow copy preserving optional dictionaries."""

        attributes = dict(self.resource_attributes) if self.resource_attributes else None
        return TelemetryConfig(
            enable_spans=self.enable_spans, provider_factory=self.provider_factory, resource_attributes=attributes
        )

    def __repr__(self) -> str:
        return f"TelemetryConfig(enable_spans={self.enable_spans!r}, provider_factory={self.provider_factory!r}, resource_attributes={self.resource_attributes!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TelemetryConfig):
            return NotImplemented
        return (
            self.enable_spans == other.enable_spans
            and self.provider_factory == other.provider_factory
            and self.resource_attributes == other.resource_attributes
        )


class LoggingConfig:
    """Controls log output format and verbosity."""

    __slots__ = ("include_sql_hash", "include_trace_context", "parameter_truncation_count", "sql_truncation_length")

    def __init__(
        self,
        *,
        include_sql_hash: bool = True,
        sql_truncation_length: int = 2000,
        parameter_truncation_count: int = 100,
        include_trace_context: bool = True,
    ) -> None:
        self.include_sql_hash = include_sql_hash
        self.sql_truncation_length = sql_truncation_length
        self.parameter_truncation_count = parameter_truncation_count
        self.include_trace_context = include_trace_context

    def __hash__(self) -> int:  # pragma: no cover - explicit to mirror dataclass behavior
        msg = "LoggingConfig objects are mutable and unhashable"
        raise TypeError(msg)

    def copy(self) -> "LoggingConfig":
        """Return a shallow copy of the logging configuration."""

        return LoggingConfig(
            include_sql_hash=self.include_sql_hash,
            sql_truncation_length=self.sql_truncation_length,
            parameter_truncation_count=self.parameter_truncation_count,
            include_trace_context=self.include_trace_context,
        )

    def __repr__(self) -> str:
        return (
            f"LoggingConfig(include_sql_hash={self.include_sql_hash!r}, sql_truncation_length={self.sql_truncation_length!r}, "
            f"parameter_truncation_count={self.parameter_truncation_count!r}, include_trace_context={self.include_trace_context!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LoggingConfig):
            return NotImplemented
        return (
            self.include_sql_hash == other.include_sql_hash
            and self.sql_truncation_length == other.sql_truncation_length
            and self.parameter_truncation_count == other.parameter_truncation_count
            and self.include_trace_context == other.include_trace_context
        )


class ObservabilityConfig:
    """Aggregates lifecycle hooks, observers, and telemetry toggles."""

    __slots__ = (
        "cloud_formatter",
        "lifecycle",
        "logging",
        "print_sql",
        "redaction",
        "sampling",
        "statement_observers",
        "telemetry",
    )

    def __init__(
        self,
        *,
        lifecycle: "LifecycleConfig | None" = None,
        print_sql: bool | None = None,
        statement_observers: tuple[StatementObserver, ...] | Iterable[StatementObserver] | None = None,
        telemetry: "TelemetryConfig | None" = None,
        redaction: "RedactionConfig | None" = None,
        logging: "LoggingConfig | None" = None,
        sampling: "SamplingConfig | None" = None,
        cloud_formatter: "CloudLogFormatter | None" = None,
    ) -> None:
        self.lifecycle = lifecycle
        self.print_sql = print_sql
        self.statement_observers = tuple(statement_observers) if statement_observers is not None else None
        self.telemetry = telemetry
        self.redaction = redaction
        self.logging = logging
        self.sampling = sampling
        self.cloud_formatter = cloud_formatter

    def __hash__(self) -> int:  # pragma: no cover - explicit to mirror dataclass behavior
        msg = "ObservabilityConfig objects are mutable and unhashable"
        raise TypeError(msg)

    def copy(self) -> "ObservabilityConfig":
        """Return a deep copy of the configuration."""

        lifecycle_copy = _normalize_lifecycle(self.lifecycle)
        observers = tuple(self.statement_observers) if self.statement_observers else None
        telemetry_copy = self.telemetry.copy() if self.telemetry else None
        redaction_copy = self.redaction.copy() if self.redaction else None
        logging_copy = self.logging.copy() if self.logging else None
        sampling_copy = self.sampling.copy() if self.sampling else None
        return ObservabilityConfig(
            lifecycle=lifecycle_copy,
            print_sql=self.print_sql,
            statement_observers=observers,
            telemetry=telemetry_copy,
            redaction=redaction_copy,
            logging=logging_copy,
            sampling=sampling_copy,
            cloud_formatter=self.cloud_formatter,
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
        logging = _merge_logging(base.logging, override.logging)
        sampling = _merge_sampling(base.sampling, override.sampling)
        cloud_formatter = override.cloud_formatter if override.cloud_formatter is not None else base.cloud_formatter

        return ObservabilityConfig(
            lifecycle=lifecycle,
            print_sql=print_sql,
            statement_observers=observers,
            telemetry=telemetry,
            redaction=redaction,
            logging=logging,
            sampling=sampling,
            cloud_formatter=cloud_formatter,
        )

    def __repr__(self) -> str:
        return (
            f"ObservabilityConfig(lifecycle={self.lifecycle!r}, print_sql={self.print_sql!r}, statement_observers={self.statement_observers!r}, telemetry={self.telemetry!r}, "
            f"redaction={self.redaction!r}, logging={self.logging!r}, sampling={self.sampling!r}, cloud_formatter={self.cloud_formatter!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObservabilityConfig):
            return NotImplemented
        return (
            _normalize_lifecycle(self.lifecycle) == _normalize_lifecycle(other.lifecycle)
            and self.print_sql == other.print_sql
            and self.statement_observers == other.statement_observers
            and self.telemetry == other.telemetry
            and self.redaction == other.redaction
            and self.logging == other.logging
            and self.sampling == other.sampling
            and self.cloud_formatter == other.cloud_formatter
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


def _merge_logging(base: "LoggingConfig | None", override: "LoggingConfig | None") -> "LoggingConfig | None":
    if base is None and override is None:
        return None
    if override is None:
        return base.copy() if base else None
    return override.copy()


def _merge_sampling(base: "SamplingConfig | None", override: "SamplingConfig | None") -> "SamplingConfig | None":
    if base is None and override is None:
        return None
    if override is None:
        return base.copy() if base else None
    if base is None:
        return override.copy()
    merged = base.copy()
    if override.sample_rate != 1.0:
        merged.sample_rate = override.sample_rate
    if override.deterministic:
        merged.deterministic = override.deterministic
    if override.force_sample_on_error:
        merged.force_sample_on_error = override.force_sample_on_error
    if override.force_sample_slow_queries_ms is not None:
        merged.force_sample_slow_queries_ms = override.force_sample_slow_queries_ms
    return merged


def _normalize_lifecycle(config: "LifecycleConfig | None") -> "LifecycleConfig | None":
    if config is None:
        return None
    normalized: dict[str, list[LifecycleHook]] = {}
    for event, hooks in config.items():
        normalized[event] = list(cast("Iterable[LifecycleHook]", hooks))
    return cast("LifecycleConfig", normalized)


def _merge_lifecycle(base: "LifecycleConfig | None", override: "LifecycleConfig | None") -> "LifecycleConfig | None":
    if base is None and override is None:
        return None
    if base is None:
        return _normalize_lifecycle(override)
    if override is None:
        return _normalize_lifecycle(base)
    merged_dict: dict[str, list[LifecycleHook]] = (
        cast("dict[str, list[LifecycleHook]]", _normalize_lifecycle(base)) or {}
    )
    for event, hooks in override.items():
        merged_dict.setdefault(event, [])
        merged_dict[event].extend(cast("Iterable[LifecycleHook]", hooks))
    return cast("LifecycleConfig", merged_dict)


__all__ = (
    "LifecycleHook",
    "LoggingConfig",
    "ObservabilityConfig",
    "RedactionConfig",
    "StatementObserver",
    "TelemetryConfig",
)
