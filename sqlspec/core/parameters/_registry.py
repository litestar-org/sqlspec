"""Driver parameter profile registry and StatementConfig factory."""

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Literal, cast

import sqlspec.exceptions
from sqlspec.core.parameters._types import DriverParameterProfile, ParameterStyleConfig
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from sqlspec.core.statement import StatementConfig

__all__ = (
    "DRIVER_PARAMETER_PROFILES",
    "build_statement_config_from_profile",
    "get_driver_profile",
    "register_driver_profile",
)

_DEFAULT_JSON_SERIALIZER: Callable[[Any], str] = to_json
_DEFAULT_JSON_DESERIALIZER: Callable[[str], Any] = from_json

DRIVER_PARAMETER_PROFILES: "dict[str, DriverParameterProfile]" = {}


def get_driver_profile(adapter_key: str) -> "DriverParameterProfile":
    """Return the registered parameter profile for the specified adapter.

    Args:
        adapter_key: Adapter identifier (case-insensitive).

    Returns:
        Registered :class:`DriverParameterProfile` instance.

    Raises:
        ImproperConfigurationError: If the adapter does not have a profile.
    """
    key = adapter_key.lower()
    try:
        return DRIVER_PARAMETER_PROFILES[key]
    except KeyError as exc:
        msg = f"No driver parameter profile registered for adapter '{adapter_key}'."
        raise sqlspec.exceptions.ImproperConfigurationError(msg) from exc


def register_driver_profile(
    adapter_key: str, profile: "DriverParameterProfile", *, allow_override: bool = False
) -> None:
    """Register a driver profile under the canonical adapter key.

    Args:
        adapter_key: Adapter identifier (case-insensitive).
        profile: Profile describing parameter behaviour.
        allow_override: Whether to replace an existing entry.

    Raises:
        ImproperConfigurationError: If attempting to register a duplicate profile.
    """

    key = adapter_key.lower()
    if not allow_override and key in DRIVER_PARAMETER_PROFILES:
        msg = f"Profile already registered for adapter '{adapter_key}'."
        raise sqlspec.exceptions.ImproperConfigurationError(msg)
    DRIVER_PARAMETER_PROFILES[key] = profile


def _build_parameter_style_config_from_profile(
    profile: "DriverParameterProfile",
    parameter_overrides: "dict[str, Any] | None",
    json_serializer: "Callable[[Any], str] | None",
    json_deserializer: "Callable[[str], Any] | None",
) -> "ParameterStyleConfig":
    """Build a :class:`ParameterStyleConfig` instance from a driver profile.

    Args:
        profile: Source driver profile.
        parameter_overrides: Optional overrides applied before instantiation.
        json_serializer: Adapter-provided JSON serializer.
        json_deserializer: Adapter-provided JSON deserializer.

    Returns:
        Configured :class:`ParameterStyleConfig` ready for statement construction.
    """
    overrides = dict(parameter_overrides or {})
    supported_styles_override = overrides.pop("supported_parameter_styles", None)
    execution_styles_override = overrides.pop("supported_execution_parameter_styles", None)
    type_coercion_override = overrides.pop("type_coercion_map", None)
    json_serializer_override = overrides.pop("json_serializer", None)
    json_deserializer_override = overrides.pop("json_deserializer", None)
    tuple_strategy_override = overrides.pop("json_tuple_strategy", None)

    supported_styles = (
        set(supported_styles_override) if supported_styles_override is not None else set(profile.supported_styles)
    )
    if execution_styles_override is None:
        execution_supported = (
            set(profile.supported_execution_styles) if profile.supported_execution_styles is not None else None
        )
    else:
        execution_supported = set(execution_styles_override) if execution_styles_override is not None else None

    type_map = (
        dict(type_coercion_override) if type_coercion_override is not None else dict(profile.custom_type_coercions)
    )

    parameter_kwargs: dict[str, Any] = {
        "default_parameter_style": overrides.pop("default_parameter_style", profile.default_style),
        "supported_parameter_styles": supported_styles,
        "supported_execution_parameter_styles": execution_supported,
        "default_execution_parameter_style": overrides.pop(
            "default_execution_parameter_style", profile.default_execution_style
        ),
        "type_coercion_map": type_map,
        "has_native_list_expansion": overrides.pop("has_native_list_expansion", profile.has_native_list_expansion),
        "needs_static_script_compilation": overrides.pop(
            "needs_static_script_compilation", profile.needs_static_script_compilation
        ),
        "allow_mixed_parameter_styles": overrides.pop(
            "allow_mixed_parameter_styles", profile.allow_mixed_parameter_styles
        ),
        "preserve_parameter_format": overrides.pop("preserve_parameter_format", profile.preserve_parameter_format),
        "preserve_original_params_for_many": overrides.pop(
            "preserve_original_params_for_many", profile.preserve_original_params_for_many
        ),
        "strict_named_parameters": overrides.pop("strict_named_parameters", profile.strict_named_parameters),
        "output_transformer": overrides.pop("output_transformer", profile.default_output_transformer),
        "ast_transformer": overrides.pop("ast_transformer", profile.default_ast_transformer),
    }

    parameter_kwargs = {k: v for k, v in parameter_kwargs.items() if v is not None}

    strategy = profile.json_serializer_strategy
    serializer_value = json_serializer_override or json_serializer
    deserializer_value = json_deserializer_override or json_deserializer

    if serializer_value is None:
        serializer_value = profile.extras.get("default_json_serializer", _DEFAULT_JSON_SERIALIZER)
    if deserializer_value is None:
        deserializer_value = profile.extras.get("default_json_deserializer", _DEFAULT_JSON_DESERIALIZER)

    serializer = cast("Callable[[Any], str]", serializer_value)
    deserializer = cast("Callable[[str], Any] | None", deserializer_value)

    if strategy == "driver":
        parameter_kwargs["json_serializer"] = serializer
        parameter_kwargs["json_deserializer"] = deserializer

    parameter_kwargs.update(overrides)
    parameter_config = ParameterStyleConfig(**parameter_kwargs)

    if strategy == "helper":
        tuple_strategy = tuple_strategy_override or profile.extras.get("json_tuple_strategy", "list")
        tuple_strategy_literal = cast("Literal['list', 'tuple']", tuple_strategy)
        parameter_config = parameter_config.with_json_serializers(
            serializer, tuple_strategy=tuple_strategy_literal, deserializer=deserializer
        )
    elif strategy == "driver":
        parameter_config = parameter_config.replace(json_serializer=serializer, json_deserializer=deserializer)

    type_overrides = profile.extras.get("type_coercion_overrides")
    if type_overrides:
        coercion_overrides = cast("Mapping[type, Callable[[Any], Any]]", type_overrides)
        updated_map: dict[type, Callable[[Any], Any]] = {}
        updated_map.update(parameter_config.type_coercion_map)
        updated_map.update(coercion_overrides)
        parameter_config = parameter_config.replace(type_coercion_map=updated_map)

    return parameter_config


def build_statement_config_from_profile(
    profile: "DriverParameterProfile",
    *,
    parameter_overrides: "dict[str, Any] | None" = None,
    statement_overrides: "dict[str, Any] | None" = None,
    json_serializer: "Callable[[Any], str] | None" = None,
    json_deserializer: "Callable[[str], Any] | None" = None,
) -> "StatementConfig":
    """Construct a :class:`StatementConfig` seeded from a driver profile.

    Args:
        profile: Driver profile providing default parameter behaviour.
        parameter_overrides: Optional overrides for parameter config fields.
        statement_overrides: Optional overrides for resulting statement config.
        json_serializer: Optional JSON serializer supplied by the adapter.
        json_deserializer: Optional JSON deserializer supplied by the adapter.

    Returns:
        New :class:`StatementConfig` instance with merged configuration.
    """
    parameter_config = _build_parameter_style_config_from_profile(
        profile, parameter_overrides, json_serializer, json_deserializer
    )

    from sqlspec.core.statement import StatementConfig as _StatementConfig

    statement_kwargs: dict[str, Any] = {}
    if profile.default_dialect is not None:
        statement_kwargs["dialect"] = profile.default_dialect
    if profile.statement_kwargs:
        statement_kwargs.update(profile.statement_kwargs)
    if statement_overrides:
        statement_kwargs.update(statement_overrides)

    filtered_statement_kwargs = {k: v for k, v in statement_kwargs.items() if v is not None}
    return _StatementConfig(parameter_config=parameter_config, **filtered_statement_kwargs)
