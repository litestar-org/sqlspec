"""ADK capability detection and override resolution."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final, Literal, TypeAlias, cast

from sqlspec.exceptions import ImproperConfigurationError

__all__ = (
    "ADKCapabilityDecision",
    "ADKCapabilityPlan",
    "normalize_adk_capability_overrides",
    "resolve_adk_capability_plan",
)

ADKCapabilityMode: TypeAlias = Literal["auto", "enable", "disable"]
ADKCapabilitySource: TypeAlias = Literal["default", "detected", "override"]

ADK_CAPABILITY_MODES: Final[frozenset[str]] = frozenset({"auto", "enable", "disable"})


@dataclass(frozen=True, slots=True)
class ADKCapabilityDecision:
    """Resolved decision for one ADK capability."""

    feature: str
    detected: bool | None
    override: ADKCapabilityMode
    enabled: bool
    source: ADKCapabilitySource
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ADKCapabilityPlan:
    """Resolved ADK capability decisions keyed by feature name."""

    decisions: dict[str, ADKCapabilityDecision] = field(default_factory=dict)

    def enabled_features(self) -> frozenset[str]:
        """Return enabled feature names."""

        return frozenset(feature for feature, decision in self.decisions.items() if decision.enabled)


def normalize_adk_capability_overrides(overrides: Mapping[str, object] | None = None) -> dict[str, ADKCapabilityMode]:
    """Normalize capability overrides from ADK config."""

    normalized: dict[str, ADKCapabilityMode] = {}
    for feature, value in (overrides or {}).items():
        if value not in ADK_CAPABILITY_MODES:
            msg = f"Unsupported ADK capability override {value!r} for {feature}; expected auto, enable, or disable"
            raise ImproperConfigurationError(msg)
        normalized[str(feature)] = cast("ADKCapabilityMode", value)
    return normalized


def resolve_adk_capability_plan(
    detected_features: Mapping[str, bool | None], overrides: Mapping[str, object] | None = None
) -> ADKCapabilityPlan:
    """Resolve detected ADK capabilities with user overrides."""

    normalized_overrides = normalize_adk_capability_overrides(overrides)
    decisions: dict[str, ADKCapabilityDecision] = {}
    for feature in sorted(set(detected_features) | set(normalized_overrides)):
        detected = detected_features.get(feature)
        override = normalized_overrides.get(feature, "auto")
        decisions[feature] = _resolve_capability_decision(feature, detected, override)
    return ADKCapabilityPlan(decisions=decisions)


def _resolve_capability_decision(
    feature: str, detected: bool | None, override: ADKCapabilityMode
) -> ADKCapabilityDecision:
    if override == "disable":
        return ADKCapabilityDecision(
            feature=feature, detected=detected, override=override, enabled=False, source="override"
        )
    if override == "enable":
        if detected is False:
            msg = f"ADK capability {feature!r} was forced enabled but detection reported it as unsupported"
            raise ImproperConfigurationError(msg)
        return ADKCapabilityDecision(
            feature=feature, detected=detected, override=override, enabled=True, source="override"
        )
    if detected is True:
        return ADKCapabilityDecision(
            feature=feature, detected=detected, override=override, enabled=True, source="detected"
        )
    if detected is False:
        return ADKCapabilityDecision(
            feature=feature, detected=detected, override=override, enabled=False, source="detected"
        )
    return ADKCapabilityDecision(
        feature=feature,
        detected=detected,
        override=override,
        enabled=False,
        source="default",
        reason="capability was not detected",
    )
