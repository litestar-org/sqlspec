"""Parameter processing pipeline orchestrator."""

import hashlib
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._alignment import looks_like_execute_many
from sqlspec.core.parameters._converter import ParameterConverter
from sqlspec.core.parameters._types import (
    ParameterInfo,
    ParameterProcessingResult,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    TypedParameter,
    wrap_with_type,
)
from sqlspec.core.parameters._validator import ParameterValidator

__all__ = ("ParameterProcessor",)


def _fingerprint_parameters(parameters: Any) -> str:
    """Return a stable fingerprint for caching parameter payloads.

    Args:
        parameters: Original parameter payload supplied by the caller.

    Returns:
        Deterministic fingerprint string derived from the parameter payload.
    """
    if parameters is None:
        return "none"

    if isinstance(parameters, Mapping):
        try:
            items = sorted(parameters.items(), key=lambda item: repr(item[0]))
        except Exception:
            items = list(parameters.items())
        data = repr(tuple(items))
    elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        data = repr(tuple(parameters))
    else:
        data = repr(parameters)

    digest = hashlib.sha256(data.encode("utf-8")).hexdigest()
    return f"{type(parameters).__name__}:{digest}"


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterProcessor:
    """Parameter processing engine coordinating conversion phases."""

    __slots__ = ("_cache", "_cache_size", "_converter", "_validator")

    DEFAULT_CACHE_SIZE = 1000

    def __init__(self) -> None:
        self._cache: dict[str, ParameterProcessingResult] = {}
        self._cache_size = 0
        self._validator = ParameterValidator()
        self._converter = ParameterConverter()

    def _handle_static_embedding(
        self, sql: str, parameters: Any, config: "ParameterStyleConfig", is_many: bool, cache_key: str
    ) -> "ParameterProcessingResult":
        coerced_params = parameters
        if config.type_coercion_map and parameters:
            coerced_params = self._apply_type_coercions(parameters, config.type_coercion_map, is_many)

        static_sql, static_params = self._converter.convert_placeholder_style(
            sql, coerced_params, ParameterStyle.STATIC, is_many
        )
        result = ParameterProcessingResult(static_sql, static_params, ParameterProfile.empty())
        if self._cache_size < self.DEFAULT_CACHE_SIZE:
            self._cache[cache_key] = result
            self._cache_size += 1
        return result

    def _determine_target_execution_style(
        self, original_styles: "set[ParameterStyle]", config: "ParameterStyleConfig"
    ) -> "ParameterStyle":
        if len(original_styles) == 1 and config.supported_execution_parameter_styles is not None:
            original_style = next(iter(original_styles))
            if original_style in config.supported_execution_parameter_styles:
                return original_style
        return config.default_execution_parameter_style or config.default_parameter_style

    def _apply_type_wrapping(self, parameters: Any) -> Any:
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [wrap_with_type(p) for p in parameters]
        if isinstance(parameters, Mapping):
            return {k: wrap_with_type(v) for k, v in parameters.items()}
        return wrap_with_type(parameters)

    def _apply_type_coercions(
        self, parameters: Any, type_coercion_map: "dict[type, Callable[[Any], Any]]", is_many: bool = False
    ) -> Any:
        def coerce_value(value: Any) -> Any:
            if value is None:
                return value

            if isinstance(value, TypedParameter):
                wrapped_value: Any = value.value
                if wrapped_value is None:
                    return wrapped_value
                original_type = value.original_type
                if original_type in type_coercion_map:
                    coerced = type_coercion_map[original_type](wrapped_value)
                    if isinstance(coerced, (list, tuple)) and not isinstance(coerced, (str, bytes)):
                        coerced = [coerce_value(item) for item in coerced]
                    elif isinstance(coerced, dict):
                        coerced = {k: coerce_value(v) for k, v in coerced.items()}
                    return coerced
                return wrapped_value

            value_type = type(value)
            if value_type in type_coercion_map:
                coerced = type_coercion_map[value_type](value)
                if isinstance(coerced, (list, tuple)) and not isinstance(coerced, (str, bytes)):
                    coerced = [coerce_value(item) for item in coerced]
                elif isinstance(coerced, dict):
                    coerced = {k: coerce_value(v) for k, v in coerced.items()}
                return coerced
            return value

        def coerce_parameter_set(param_set: Any) -> Any:
            if isinstance(param_set, Sequence) and not isinstance(param_set, (str, bytes)):
                return [coerce_value(p) for p in param_set]
            if isinstance(param_set, Mapping):
                return {k: coerce_value(v) for k, v in param_set.items()}
            return coerce_value(param_set)

        if is_many and isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [coerce_parameter_set(param_set) for param_set in parameters]

        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [coerce_value(p) for p in parameters]
        if isinstance(parameters, Mapping):
            return {k: coerce_value(v) for k, v in parameters.items()}
        return coerce_value(parameters)

    def _generate_processor_cache_key(
        self, sql: str, parameters: Any, config: "ParameterStyleConfig", is_many: bool, dialect: str | None
    ) -> str:
        param_fingerprint = _fingerprint_parameters(parameters)
        dialect_marker = dialect or "default"
        default_style = config.default_parameter_style.value if config.default_parameter_style else "unknown"
        return f"{sql}:{param_fingerprint}:{default_style}:{is_many}:{dialect_marker}"

    def process(
        self,
        sql: str,
        parameters: Any,
        config: "ParameterStyleConfig",
        dialect: str | None = None,
        is_many: bool = False,
    ) -> "ParameterProcessingResult":
        cache_key = self._generate_processor_cache_key(sql, parameters, config, is_many, dialect)
        cached_result = self._cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        param_info = self._validator.extract_parameters(sql)
        original_styles = {p.style for p in param_info} if param_info else set()
        needs_sqlglot_normalization = self._needs_sqlglot_normalization(param_info, dialect)
        needs_execution_conversion = self._needs_execution_conversion(param_info, config)

        needs_static_embedding = config.needs_static_script_compilation and param_info and parameters and not is_many

        def _requires_mapping_normalization(payload: Any) -> bool:
            if not payload or not param_info:
                return False

            has_named_placeholders = any(
                param.style
                in {
                    ParameterStyle.NAMED_COLON,
                    ParameterStyle.NAMED_AT,
                    ParameterStyle.NAMED_DOLLAR,
                    ParameterStyle.NAMED_PYFORMAT,
                }
                for param in param_info
            )
            if has_named_placeholders:
                return False

            looks_many = is_many or looks_like_execute_many(payload)
            if not looks_many:
                return False

            if isinstance(payload, Mapping):
                return True

            if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
                return any(isinstance(item, Mapping) for item in payload)

            return False

        if needs_static_embedding:
            return self._handle_static_embedding(sql, parameters, config, is_many, cache_key)

        if (
            not needs_sqlglot_normalization
            and not needs_execution_conversion
            and not config.type_coercion_map
            and not config.output_transformer
            and not _requires_mapping_normalization(parameters)
        ):
            result = ParameterProcessingResult(sql, parameters, ParameterProfile(param_info))
            if self._cache_size < self.DEFAULT_CACHE_SIZE:
                self._cache[cache_key] = result
                self._cache_size += 1
            return result

        processed_sql, processed_parameters = sql, parameters

        if _requires_mapping_normalization(processed_parameters):
            target_style = self._determine_target_execution_style(original_styles, config)
            processed_sql, processed_parameters = self._converter.convert_placeholder_style(
                processed_sql, processed_parameters, target_style, is_many
            )

        if processed_parameters:
            processed_parameters = self._apply_type_wrapping(processed_parameters)

        if needs_sqlglot_normalization:
            processed_sql, _ = self._converter.normalize_sql_for_parsing(processed_sql, dialect)

        if config.type_coercion_map and processed_parameters:
            processed_parameters = self._apply_type_coercions(processed_parameters, config.type_coercion_map, is_many)

        processed_sql, processed_parameters = self._process_parameters_conversion(
            processed_sql,
            processed_parameters,
            config,
            original_styles,
            needs_execution_conversion,
            needs_sqlglot_normalization,
            is_many,
        )

        if config.output_transformer:
            processed_sql, processed_parameters = config.output_transformer(processed_sql, processed_parameters)

        final_param_info = self._validator.extract_parameters(processed_sql)
        final_profile = ParameterProfile(final_param_info)
        result = ParameterProcessingResult(processed_sql, processed_parameters, final_profile)

        if self._cache_size < self.DEFAULT_CACHE_SIZE:
            self._cache[cache_key] = result
            self._cache_size += 1
        return result

    def get_sqlglot_compatible_sql(
        self, sql: str, parameters: Any, config: "ParameterStyleConfig", dialect: str | None = None
    ) -> "tuple[str, Any]":
        """Normalize SQL for parsing without altering execution format.

        Args:
            sql: Raw SQL text.
            parameters: Parameter payload supplied by the caller.
            config: Parameter style configuration.
            dialect: Optional SQL dialect for compatibility checks.

        Returns:
            Tuple of normalized SQL and the original parameter payload.
        """

        param_info = self._validator.extract_parameters(sql)

        if self._needs_sqlglot_normalization(param_info, dialect):
            normalized_sql, _ = self._converter.normalize_sql_for_parsing(sql, dialect)
            return normalized_sql, parameters

        return sql, parameters

    def _needs_execution_conversion(self, param_info: "list[ParameterInfo]", config: "ParameterStyleConfig") -> bool:
        """Determine whether execution placeholder conversion is required."""
        if config.needs_static_script_compilation:
            return True

        if not param_info:
            return False

        current_styles = {param.style for param in param_info}

        if (
            config.allow_mixed_parameter_styles
            and len(current_styles) > 1
            and config.supported_execution_parameter_styles is not None
            and len(config.supported_execution_parameter_styles) > 1
            and all(style in config.supported_execution_parameter_styles for style in current_styles)
        ):
            return False

        if (
            config.supported_execution_parameter_styles is not None
            and len(config.supported_execution_parameter_styles) > 1
            and all(style in config.supported_execution_parameter_styles for style in current_styles)
        ):
            return False

        if len(current_styles) > 1:
            return True

        if len(current_styles) == 1:
            current_style = next(iter(current_styles))
            supported_styles = config.supported_execution_parameter_styles
            if supported_styles is None:
                return True
            return current_style not in supported_styles

        return True

    def _needs_sqlglot_normalization(self, param_info: "list[ParameterInfo]", dialect: str | None = None) -> bool:
        incompatible_styles = self._validator.get_sqlglot_incompatible_styles(dialect)
        return any(p.style in incompatible_styles for p in param_info)

    def _process_parameters_conversion(
        self,
        sql: str,
        parameters: Any,
        config: "ParameterStyleConfig",
        original_styles: "set[ParameterStyle]",
        needs_execution_conversion: bool,
        needs_sqlglot_normalization: bool,
        is_many: bool,
    ) -> "tuple[str, Any]":
        if not (needs_execution_conversion or needs_sqlglot_normalization):
            return sql, parameters

        if is_many and config.preserve_original_params_for_many and isinstance(parameters, (list, tuple)):
            target_style = self._determine_target_execution_style(original_styles, config)
            processed_sql, _ = self._converter.convert_placeholder_style(sql, parameters, target_style, is_many)
            return processed_sql, parameters

        target_style = self._determine_target_execution_style(original_styles, config)
        return self._converter.convert_placeholder_style(sql, parameters, target_style, is_many)
