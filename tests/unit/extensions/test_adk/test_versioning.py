"""Tests for ADK schema and payload version planning."""

from typing import Any

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk._config_utils import _get_adk_version_plan
from sqlspec.extensions.adk._versioning import (
    ADK_ARTIFACT_PAYLOAD_VERSION,
    ADK_EVENT_PAYLOAD_VERSION,
    ADK_MEMORY_PAYLOAD_VERSION,
    ADK_PAYLOAD_VERSION_KEYS,
    ADK_SCHEMA_VERSION,
    ADK_SCHEMA_VERSION_KEY,
    ADK_STATE_PAYLOAD_VERSION,
    ADKVersionPlan,
    validate_adk_version_plan,
)


class _Config:
    extension_config: dict[str, dict[str, Any]]

    def __init__(self, adk_config: dict[str, Any]) -> None:
        self.extension_config = {"adk": adk_config}


def test_default_version_plan_matches_clean_break_v1_contract() -> None:
    plan = _get_adk_version_plan(_Config({}))

    assert plan == ADKVersionPlan(
        schema_version=ADK_SCHEMA_VERSION,
        event_payload_version=ADK_EVENT_PAYLOAD_VERSION,
        state_payload_version=ADK_STATE_PAYLOAD_VERSION,
        memory_payload_version=ADK_MEMORY_PAYLOAD_VERSION,
        artifact_payload_version=ADK_ARTIFACT_PAYLOAD_VERSION,
    )


def test_schema_version_key_matches_official_adk_metadata_key() -> None:
    assert ADK_SCHEMA_VERSION_KEY == "schema_version"


def test_version_plan_metadata_items_include_schema_and_payload_versions() -> None:
    metadata_items = dict(_get_adk_version_plan(_Config({})).metadata_items())

    assert metadata_items == {
        ADK_SCHEMA_VERSION_KEY: "1",
        ADK_PAYLOAD_VERSION_KEYS["event"]: "1",
        ADK_PAYLOAD_VERSION_KEYS["state"]: "1",
        ADK_PAYLOAD_VERSION_KEYS["memory"]: "1",
        ADK_PAYLOAD_VERSION_KEYS["artifact"]: "1",
    }


def test_nested_schema_payload_versions_override_defaults() -> None:
    plan = _get_adk_version_plan(
        _Config({
            "schema": {"schema_version": 1, "payload_versions": {"event": 1, "state": 1, "memory": 1, "artifact": 1}}
        })
    )

    assert plan.event_payload_version == 1
    assert plan.state_payload_version == 1
    assert plan.memory_payload_version == 1
    assert plan.artifact_payload_version == 1


@pytest.mark.parametrize(
    "adk_config",
    [
        {"schema": {"schema_version": 2}},
        {"schema": {"payload_versions": {"event": 2}}},
        {"schema": {"payload_versions": {"state": 2}}},
        {"schema": {"payload_versions": {"memory": 2}}},
        {"schema": {"payload_versions": {"artifact": 2}}},
    ],
)
def test_unsupported_schema_or_payload_versions_raise_configuration_error(adk_config: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        _get_adk_version_plan(_Config(adk_config))


def test_validate_adk_version_plan_accepts_supported_clean_break_plan() -> None:
    validate_adk_version_plan(ADKVersionPlan())
