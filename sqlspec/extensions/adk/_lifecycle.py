"""ADK lifecycle control resolution."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final, Literal, TypeAlias, cast

from sqlspec.exceptions import ImproperConfigurationError

__all__ = ("ADKLifecyclePlan", "resolve_adk_lifecycle_plan", "validate_adk_lifecycle_plan")

ADKLifecycleMode: TypeAlias = Literal["auto", "enable", "disable"]

ADK_INDEXING_CONTROLS: Final[tuple[str, ...]] = (
    "generated_columns",
    "covering_indexes",
    "search_indexes",
    "json_indexes",
    "vector_indexes",
)
ADK_LIFECYCLE_MODES: Final[frozenset[str]] = frozenset({"auto", "enable", "disable"})


@dataclass(frozen=True, slots=True)
class ADKLifecyclePlan:
    """Resolved ADK lifecycle controls used by backend DDL chapters."""

    partitioning: dict[str, object] | None = None
    retention: dict[str, object] | None = None
    indexing: dict[str, ADKLifecycleMode] = field(default_factory=dict)
    compression: dict[str, object] | None = None
    table_options: dict[str, str] = field(default_factory=dict)


def resolve_adk_lifecycle_plan(adk_config: Mapping[str, object] | None = None) -> ADKLifecyclePlan:
    """Resolve lifecycle controls from ADK extension config."""

    config = adk_config or {}
    lifecycle_config = _mapping(config.get("lifecycle"))
    plan = ADKLifecyclePlan(
        partitioning=_optional_mapping(_first_value(lifecycle_config.get("partitioning"), config.get("partitioning"))),
        retention=_optional_mapping(_first_value(lifecycle_config.get("retention"), config.get("retention"))),
        indexing=_resolve_indexing_config(config, lifecycle_config),
        compression=_optional_mapping(_first_value(lifecycle_config.get("compression"), config.get("compression"))),
        table_options=_resolve_table_options(config, lifecycle_config),
    )
    validate_adk_lifecycle_plan(plan)
    return plan


def validate_adk_lifecycle_plan(plan: ADKLifecyclePlan) -> None:
    """Validate lifecycle controls that have shared semantics."""

    for key, value in plan.indexing.items():
        if value not in ADK_LIFECYCLE_MODES:
            msg = f"Unsupported ADK lifecycle indexing mode {value!r} for {key}; expected auto, enable, or disable"
            raise ImproperConfigurationError(msg)


def _resolve_indexing_config(
    config: Mapping[str, object], lifecycle_config: Mapping[str, object]
) -> dict[str, ADKLifecycleMode]:
    lifecycle_indexing = _mapping(lifecycle_config.get("indexing"))
    top_level_indexing = _mapping(config.get("indexing"))
    optimizations = _mapping(config.get("optimizations"))
    resolved: dict[str, ADKLifecycleMode] = {}
    for key in ADK_INDEXING_CONTROLS:
        value = _first_value(lifecycle_indexing.get(key), top_level_indexing.get(key), optimizations.get(key), "auto")
        resolved[key] = _indexing_mode(key, value)
    return resolved


def _resolve_table_options(config: Mapping[str, object], lifecycle_config: Mapping[str, object]) -> dict[str, str]:
    flat_options = {
        "sessions": config.get("session_table_options"),
        "events": config.get("events_table_options"),
        "memory": config.get("memory_table_options"),
        "expires_index": config.get("expires_index_options"),
    }
    resolved = {key: str(value) for key, value in flat_options.items() if value is not None}
    resolved.update({key: str(value) for key, value in _mapping(lifecycle_config.get("table_options")).items()})
    return resolved


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _optional_mapping(value: object | None) -> dict[str, object] | None:
    if isinstance(value, Mapping):
        return dict(value)
    return None


def _first_value(*values: object) -> object | None:
    for value in values:
        if value is not None:
            return value
    return None


def _indexing_mode(key: str, value: object) -> ADKLifecycleMode:
    if value in ADK_LIFECYCLE_MODES:
        return cast("ADKLifecycleMode", value)
    msg = f"Unsupported ADK lifecycle indexing mode {value!r} for {key}; expected auto, enable, or disable"
    raise ImproperConfigurationError(msg)
