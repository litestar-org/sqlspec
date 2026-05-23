"""Tests for ADK capability detection and override resolution."""

from typing import Any

import pytest

from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk._capabilities import resolve_adk_capability_plan
from sqlspec.extensions.adk._config_utils import _get_adk_capability_overrides


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: dict[str, Any]) -> None:
        self.extension_config = {"adk": adk_config}


def test_adk_config_declares_capabilities_section() -> None:
    assert "capabilities" in ADKConfig.__annotations__


def test_capability_plan_uses_detected_features_by_default() -> None:
    plan = resolve_adk_capability_plan(detected_features={"supports_generated_columns": True, "supports_vector": False})

    assert plan.decisions["supports_generated_columns"].enabled is True
    assert plan.decisions["supports_generated_columns"].source == "detected"
    assert plan.decisions["supports_vector"].enabled is False
    assert plan.decisions["supports_vector"].source == "detected"


def test_disable_override_wins_over_detected_feature() -> None:
    plan = resolve_adk_capability_plan(
        detected_features={"supports_generated_columns": True}, overrides={"supports_generated_columns": "disable"}
    )

    decision = plan.decisions["supports_generated_columns"]
    assert decision.enabled is False
    assert decision.override == "disable"
    assert decision.source == "override"


def test_enable_override_rejects_known_unsupported_feature() -> None:
    with pytest.raises(ImproperConfigurationError, match="supports_vector"):
        resolve_adk_capability_plan(
            detected_features={"supports_vector": False}, overrides={"supports_vector": "enable"}
        )


def test_enable_override_can_force_unknown_detection_result() -> None:
    plan = resolve_adk_capability_plan(detected_features={}, overrides={"supports_json_table": "enable"})

    decision = plan.decisions["supports_json_table"]
    assert decision.enabled is True
    assert decision.detected is None
    assert decision.source == "override"


def test_config_capability_overrides_are_normalized() -> None:
    overrides = _get_adk_capability_overrides(
        _Config({"capabilities": {"overrides": {"supports_generated_columns": "disable"}}})
    )

    assert overrides == {"supports_generated_columns": "disable"}


def test_invalid_capability_override_raises_configuration_error() -> None:
    with pytest.raises(ImproperConfigurationError):
        _get_adk_capability_overrides(
            _Config({"capabilities": {"overrides": {"supports_generated_columns": "sometimes"}}})
        )
