"""ADK schema and payload version planning."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal, TypeAlias

from sqlspec.exceptions import ImproperConfigurationError

__all__ = ("ADKVersionPlan", "resolve_adk_version_plan", "validate_adk_version_plan")


ADKPayloadKind: TypeAlias = Literal["event", "state", "memory", "artifact"]

ADK_SCHEMA_VERSION: Final = 1
ADK_EVENT_PAYLOAD_VERSION: Final = 1
ADK_STATE_PAYLOAD_VERSION: Final = 1
ADK_MEMORY_PAYLOAD_VERSION: Final = 1
ADK_ARTIFACT_PAYLOAD_VERSION: Final = 1

ADK_SCHEMA_VERSION_KEY: Final = "sqlspec.adk.schema_version"
ADK_PAYLOAD_VERSION_KEYS: Final[dict[ADKPayloadKind, str]] = {
    "event": "sqlspec.adk.payload.event",
    "state": "sqlspec.adk.payload.state",
    "memory": "sqlspec.adk.payload.memory",
    "artifact": "sqlspec.adk.payload.artifact",
}

SUPPORTED_ADK_SCHEMA_VERSIONS: Final[frozenset[int]] = frozenset({ADK_SCHEMA_VERSION})
SUPPORTED_ADK_PAYLOAD_VERSIONS: Final[dict[ADKPayloadKind, frozenset[int]]] = {
    "event": frozenset({ADK_EVENT_PAYLOAD_VERSION}),
    "state": frozenset({ADK_STATE_PAYLOAD_VERSION}),
    "memory": frozenset({ADK_MEMORY_PAYLOAD_VERSION}),
    "artifact": frozenset({ADK_ARTIFACT_PAYLOAD_VERSION}),
}


@dataclass(frozen=True, slots=True)
class ADKVersionPlan:
    """Resolved ADK schema and payload version contract."""

    schema_version: int = ADK_SCHEMA_VERSION
    event_payload_version: int = ADK_EVENT_PAYLOAD_VERSION
    state_payload_version: int = ADK_STATE_PAYLOAD_VERSION
    memory_payload_version: int = ADK_MEMORY_PAYLOAD_VERSION
    artifact_payload_version: int = ADK_ARTIFACT_PAYLOAD_VERSION

    def payload_versions(self) -> dict[ADKPayloadKind, int]:
        """Return payload versions keyed by payload kind."""

        return {
            "event": self.event_payload_version,
            "state": self.state_payload_version,
            "memory": self.memory_payload_version,
            "artifact": self.artifact_payload_version,
        }

    def metadata_items(self) -> tuple[tuple[str, str], ...]:
        """Return deterministic metadata rows for the ADK metadata table."""

        return (
            (ADK_SCHEMA_VERSION_KEY, str(self.schema_version)),
            (ADK_PAYLOAD_VERSION_KEYS["event"], str(self.event_payload_version)),
            (ADK_PAYLOAD_VERSION_KEYS["state"], str(self.state_payload_version)),
            (ADK_PAYLOAD_VERSION_KEYS["memory"], str(self.memory_payload_version)),
            (ADK_PAYLOAD_VERSION_KEYS["artifact"], str(self.artifact_payload_version)),
        )


def resolve_adk_version_plan(adk_config: Mapping[str, object] | None = None) -> ADKVersionPlan:
    """Resolve the configured ADK schema and payload versions."""

    config = adk_config or {}
    schema_config = _mapping(config.get("schema"))
    schema_payloads = _mapping(schema_config.get("payload_versions"))
    top_level_payloads = _mapping(config.get("payloads"))
    plan = ADKVersionPlan(
        schema_version=_version_value(
            _first_value(schema_config.get("schema_version"), config.get("schema_version")),
            default=ADK_SCHEMA_VERSION,
            label="schema.schema_version",
        ),
        event_payload_version=_payload_version("event", schema_payloads, top_level_payloads),
        state_payload_version=_payload_version("state", schema_payloads, top_level_payloads),
        memory_payload_version=_payload_version("memory", schema_payloads, top_level_payloads),
        artifact_payload_version=_payload_version("artifact", schema_payloads, top_level_payloads),
    )
    validate_adk_version_plan(plan)
    return plan


def validate_adk_version_plan(plan: ADKVersionPlan) -> None:
    """Validate that a resolved ADK version plan is supported."""

    if plan.schema_version not in SUPPORTED_ADK_SCHEMA_VERSIONS:
        _raise_unsupported_version(
            "schema", plan.schema_version, sorted(SUPPORTED_ADK_SCHEMA_VERSIONS), "schema.schema_version"
        )
    for payload_kind, payload_version in plan.payload_versions().items():
        supported = SUPPORTED_ADK_PAYLOAD_VERSIONS[payload_kind]
        if payload_version not in supported:
            _raise_unsupported_version(payload_kind, payload_version, sorted(supported), "schema.payload_versions")


def _payload_version(
    payload_kind: ADKPayloadKind, schema_payloads: Mapping[str, object], top_level_payloads: Mapping[str, object]
) -> int:
    default_versions = {
        "event": ADK_EVENT_PAYLOAD_VERSION,
        "state": ADK_STATE_PAYLOAD_VERSION,
        "memory": ADK_MEMORY_PAYLOAD_VERSION,
        "artifact": ADK_ARTIFACT_PAYLOAD_VERSION,
    }
    return _version_value(
        _first_value(schema_payloads.get(payload_kind), top_level_payloads.get(payload_kind)),
        default=default_versions[payload_kind],
        label=f"schema.payload_versions.{payload_kind}",
    )


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _first_value(*values: object) -> object | None:
    for value in values:
        if value is not None:
            return value
    return None


def _version_value(value: object | None, *, default: int, label: str) -> int:
    if value is None:
        return default
    if type(value) is not int:
        msg = f"ADK {label} must be an integer version, got {value!r}"
        raise ImproperConfigurationError(msg)
    return value


def _raise_unsupported_version(kind: str, version: int, supported: list[int], label: str) -> None:
    msg = f"Unsupported ADK {kind} version {version!r} from {label}; supported versions: {supported}"
    raise ImproperConfigurationError(msg)
