"""Parameter processing public API."""

from sqlspec.core.parameters._alignment import (
    EXECUTE_MANY_MIN_ROWS,
    collect_null_parameter_ordinals,
    looks_like_execute_many,
    normalize_parameter_key,
    validate_parameter_alignment,
)
from sqlspec.core.parameters._converter import ParameterConverter
from sqlspec.core.parameters._processor import ParameterProcessor, fingerprint_parameters
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
from sqlspec.core.parameters._validator import PARAMETER_REGEX, ParameterValidator

__all__ = (
    "DRIVER_PARAMETER_PROFILES",
    "EXECUTE_MANY_MIN_ROWS",
    "PARAMETER_REGEX",
    "DriverParameterProfile",
    "ParameterConverter",
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
    "build_literal_inlining_transform",
    "build_null_pruning_transform",
    "build_statement_config_from_profile",
    "collect_null_parameter_ordinals",
    "fingerprint_parameters",
    "get_driver_profile",
    "is_iterable_parameters",
    "looks_like_execute_many",
    "normalize_parameter_key",
    "register_driver_profile",
    "replace_null_parameters_with_literals",
    "replace_placeholders_with_literals",
    "validate_parameter_alignment",
    "wrap_with_type",
)
