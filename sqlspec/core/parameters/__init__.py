"""Parameter processing public API."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.core.parameters._alignment import (
        EXECUTE_MANY_MIN_ROWS,
        collect_null_parameter_ordinals,
        looks_like_execute_many,
        normalize_parameter_key,
        validate_parameter_alignment,
    )
    from sqlspec.core.parameters._converter import ParameterConverter
    from sqlspec.core.parameters._declared import (
        ParameterDeclaration,
        ParamTypeMatcher,
        matches_param_type,
        register_param_type,
        resolve_param_type,
    )
    from sqlspec.core.parameters._processor import (
        ParameterProcessor,
        apply_type_coercion,
        executes_qmark,
        structural_fingerprint,
        type_coercion_dispatcher,
        type_coercion_fallbacks,
        value_fingerprint,
    )
    from sqlspec.core.parameters._registry import (
        DRIVER_PARAMETER_PROFILES,
        build_statement_config_from_profile,
        get_driver_profile,
        register_driver_profile,
    )
    from sqlspec.core.parameters._transformers import (
        build_literal_inlining_transform,
        build_null_pruning_transform,
        replace_null_parameters_with_literals,
        replace_placeholders_with_literals,
    )
    from sqlspec.core.parameters._types import (
        DriverParameterProfile,
        ParameterInfo,
        ParameterMapping,
        ParameterPayload,
        ParameterProcessingResult,
        ParameterProfile,
        ParameterSequence,
        ParameterStyle,
        ParameterStyleConfig,
        TypedParameter,
        is_iterable_parameters,
        wrap_with_type,
    )
    from sqlspec.core.parameters._validator import PARAMETER_REGEX, ParameterValidator, unescape_qmark_operator

__all__ = (
    "DRIVER_PARAMETER_PROFILES",
    "EXECUTE_MANY_MIN_ROWS",
    "PARAMETER_REGEX",
    "DriverParameterProfile",
    "ParamTypeMatcher",
    "ParameterConverter",
    "ParameterDeclaration",
    "ParameterInfo",
    "ParameterMapping",
    "ParameterPayload",
    "ParameterProcessingResult",
    "ParameterProcessor",
    "ParameterProfile",
    "ParameterSequence",
    "ParameterStyle",
    "ParameterStyleConfig",
    "ParameterValidator",
    "TypedParameter",
    "apply_type_coercion",
    "build_literal_inlining_transform",
    "build_null_pruning_transform",
    "build_statement_config_from_profile",
    "collect_null_parameter_ordinals",
    "executes_qmark",
    "get_driver_profile",
    "is_iterable_parameters",
    "looks_like_execute_many",
    "matches_param_type",
    "normalize_parameter_key",
    "register_driver_profile",
    "register_param_type",
    "replace_null_parameters_with_literals",
    "replace_placeholders_with_literals",
    "resolve_param_type",
    "structural_fingerprint",
    "type_coercion_dispatcher",
    "type_coercion_fallbacks",
    "unescape_qmark_operator",
    "validate_parameter_alignment",
    "value_fingerprint",
    "wrap_with_type",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "DRIVER_PARAMETER_PROFILES": ("sqlspec.core.parameters._registry", "DRIVER_PARAMETER_PROFILES"),
    "EXECUTE_MANY_MIN_ROWS": ("sqlspec.core.parameters._alignment", "EXECUTE_MANY_MIN_ROWS"),
    "PARAMETER_REGEX": ("sqlspec.core.parameters._validator", "PARAMETER_REGEX"),
    "DriverParameterProfile": ("sqlspec.core.parameters._types", "DriverParameterProfile"),
    "ParamTypeMatcher": ("sqlspec.core.parameters._declared", "ParamTypeMatcher"),
    "ParameterConverter": ("sqlspec.core.parameters._converter", "ParameterConverter"),
    "ParameterDeclaration": ("sqlspec.core.parameters._declared", "ParameterDeclaration"),
    "ParameterInfo": ("sqlspec.core.parameters._types", "ParameterInfo"),
    "ParameterMapping": ("sqlspec.core.parameters._types", "ParameterMapping"),
    "ParameterPayload": ("sqlspec.core.parameters._types", "ParameterPayload"),
    "ParameterProcessingResult": ("sqlspec.core.parameters._types", "ParameterProcessingResult"),
    "ParameterProcessor": ("sqlspec.core.parameters._processor", "ParameterProcessor"),
    "ParameterProfile": ("sqlspec.core.parameters._types", "ParameterProfile"),
    "ParameterSequence": ("sqlspec.core.parameters._types", "ParameterSequence"),
    "ParameterStyle": ("sqlspec.core.parameters._types", "ParameterStyle"),
    "ParameterStyleConfig": ("sqlspec.core.parameters._types", "ParameterStyleConfig"),
    "ParameterValidator": ("sqlspec.core.parameters._validator", "ParameterValidator"),
    "TypedParameter": ("sqlspec.core.parameters._types", "TypedParameter"),
    "apply_type_coercion": ("sqlspec.core.parameters._processor", "apply_type_coercion"),
    "build_literal_inlining_transform": ("sqlspec.core.parameters._transformers", "build_literal_inlining_transform"),
    "build_null_pruning_transform": ("sqlspec.core.parameters._transformers", "build_null_pruning_transform"),
    "build_statement_config_from_profile": ("sqlspec.core.parameters._registry", "build_statement_config_from_profile"),
    "collect_null_parameter_ordinals": ("sqlspec.core.parameters._alignment", "collect_null_parameter_ordinals"),
    "executes_qmark": ("sqlspec.core.parameters._processor", "executes_qmark"),
    "get_driver_profile": ("sqlspec.core.parameters._registry", "get_driver_profile"),
    "is_iterable_parameters": ("sqlspec.core.parameters._types", "is_iterable_parameters"),
    "looks_like_execute_many": ("sqlspec.core.parameters._alignment", "looks_like_execute_many"),
    "matches_param_type": ("sqlspec.core.parameters._declared", "matches_param_type"),
    "normalize_parameter_key": ("sqlspec.core.parameters._alignment", "normalize_parameter_key"),
    "register_driver_profile": ("sqlspec.core.parameters._registry", "register_driver_profile"),
    "register_param_type": ("sqlspec.core.parameters._declared", "register_param_type"),
    "replace_null_parameters_with_literals": (
        "sqlspec.core.parameters._transformers",
        "replace_null_parameters_with_literals",
    ),
    "replace_placeholders_with_literals": (
        "sqlspec.core.parameters._transformers",
        "replace_placeholders_with_literals",
    ),
    "resolve_param_type": ("sqlspec.core.parameters._declared", "resolve_param_type"),
    "structural_fingerprint": ("sqlspec.core.parameters._processor", "structural_fingerprint"),
    "type_coercion_dispatcher": ("sqlspec.core.parameters._processor", "type_coercion_dispatcher"),
    "type_coercion_fallbacks": ("sqlspec.core.parameters._processor", "type_coercion_fallbacks"),
    "unescape_qmark_operator": ("sqlspec.core.parameters._validator", "unescape_qmark_operator"),
    "validate_parameter_alignment": ("sqlspec.core.parameters._alignment", "validate_parameter_alignment"),
    "value_fingerprint": ("sqlspec.core.parameters._processor", "value_fingerprint"),
    "wrap_with_type": ("sqlspec.core.parameters._types", "wrap_with_type"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
