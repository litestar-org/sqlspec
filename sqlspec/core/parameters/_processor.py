"""Parameter processing pipeline orchestrator."""

import hashlib
from collections import OrderedDict
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


def _mapping_item_sort_key(item: "tuple[Any, Any]") -> str:
    return repr(item[0])


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
            items = sorted(parameters.items(), key=_mapping_item_sort_key)
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

    __slots__ = ("_cache", "_cache_max_size", "_converter", "_validator")

    DEFAULT_CACHE_SIZE = 1000

    def __init__(
        self,
        *,
        converter: "ParameterConverter | None" = None,
        validator: "ParameterValidator | None" = None,
        cache_max_size: int | None = None,
        validator_cache_max_size: int | None = None,
    ) -> None:
        self._cache: OrderedDict[str, ParameterProcessingResult] = OrderedDict()
        if cache_max_size is None:
            cache_max_size = self.DEFAULT_CACHE_SIZE
        self._cache_max_size = max(cache_max_size, 0)
        if converter is None:
            if validator is None:
                validator_cache = validator_cache_max_size
                if validator_cache is None:
                    validator_cache = self._cache_max_size
                validator = ParameterValidator(cache_max_size=validator_cache)
            self._validator = validator
            self._converter = ParameterConverter(self._validator)
        else:
            self._converter = converter
            if validator is None:
                self._validator = converter.validator
            else:
                self._validator = validator
                self._converter.validator = validator
            if validator_cache_max_size is not None and isinstance(self._validator, ParameterValidator):
                self._validator.set_cache_max_size(validator_cache_max_size)

    def _compile_static_script(
        self, sql: str, parameters: Any, config: "ParameterStyleConfig", is_many: bool, cache_key: str
    ) -> "ParameterProcessingResult":
        coerced_params = parameters
        if config.type_coercion_map and parameters:
            coerced_params = self._coerce_parameter_types(parameters, config.type_coercion_map, is_many)

        static_sql, static_params = self._converter.convert_placeholder_style(
            sql, coerced_params, ParameterStyle.STATIC, is_many
        )
        result = ParameterProcessingResult(static_sql, static_params, ParameterProfile.empty(), sqlglot_sql=static_sql)
        return self._store_cached_result(cache_key, result)

    def _select_execution_style(
        self, original_styles: "set[ParameterStyle]", config: "ParameterStyleConfig"
    ) -> "ParameterStyle":
        if len(original_styles) == 1 and config.supported_execution_parameter_styles is not None:
            original_style = next(iter(original_styles))
            if original_style in config.supported_execution_parameter_styles:
                return original_style
        return config.default_execution_parameter_style or config.default_parameter_style

    def _wrap_parameter_types(self, parameters: Any) -> Any:
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [wrap_with_type(p) for p in parameters]
        if isinstance(parameters, Mapping):
            return {k: wrap_with_type(v) for k, v in parameters.items()}
        return wrap_with_type(parameters)

    def _coerce_parameter_types(
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

    def _store_cached_result(self, cache_key: str, result: "ParameterProcessingResult") -> "ParameterProcessingResult":
        if self._cache_max_size <= 0:
            return result
        self._cache[cache_key] = result
        self._cache.move_to_end(cache_key)
        if len(self._cache) > self._cache_max_size:
            self._cache.popitem(last=False)
        return result

    def _needs_mapping_normalization(self, payload: Any, param_info: "list[ParameterInfo]", is_many: bool) -> bool:
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

    def _normalize_sql_for_parsing(self, sql: str, param_info: "list[ParameterInfo]", dialect: str | None) -> str:
        if not self._needs_parse_normalization(param_info, dialect):
            return sql
        normalized_sql, _ = self._converter.normalize_sql_for_parsing(sql, dialect, param_info=param_info)
        return normalized_sql

    def _make_processor_cache_key(
        self,
        sql: str,
        parameters: Any,
        config: "ParameterStyleConfig",
        is_many: bool,
        dialect: str | None,
        wrap_types: bool,
    ) -> str:
        param_fingerprint = _fingerprint_parameters(parameters)
        dialect_marker = dialect or "default"
        default_style = config.default_parameter_style.value if config.default_parameter_style else "unknown"
        return f"{sql}:{param_fingerprint}:{default_style}:{is_many}:{dialect_marker}:{wrap_types}"

    def process(
        self,
        sql: str,
        parameters: Any,
        config: "ParameterStyleConfig",
        dialect: str | None = None,
        is_many: bool = False,
        wrap_types: bool = True,
    ) -> "ParameterProcessingResult":
        cache_key = self._make_processor_cache_key(sql, parameters, config, is_many, dialect, wrap_types)
        if self._cache_max_size > 0:
            cached_result = self._cache.get(cache_key)
            if cached_result is not None:
                self._cache.move_to_end(cache_key)
                return cached_result

        param_info = self._validator.extract_parameters(sql)
        original_styles = {p.style for p in param_info} if param_info else set()
        needs_execution_conversion = self._needs_execution_placeholder_conversion(param_info, config)

        if config.needs_static_script_compilation and param_info and parameters and not is_many:
            return self._compile_static_script(sql, parameters, config, is_many, cache_key)

        requires_mapping = self._needs_mapping_normalization(parameters, param_info, is_many)
        if (
            not needs_execution_conversion
            and not config.type_coercion_map
            and not config.output_transformer
            and not requires_mapping
        ):
            normalized_sql = self._normalize_sql_for_parsing(sql, param_info, dialect)
            result = ParameterProcessingResult(
                sql, parameters, ParameterProfile(param_info), sqlglot_sql=normalized_sql
            )
            return self._store_cached_result(cache_key, result)

        processed_sql, processed_parameters = sql, parameters

        if requires_mapping:
            target_style = self._select_execution_style(original_styles, config)
            processed_sql, processed_parameters = self._converter.convert_placeholder_style(
                processed_sql, processed_parameters, target_style, is_many
            )

        if processed_parameters and wrap_types:
            processed_parameters = self._wrap_parameter_types(processed_parameters)

        if config.type_coercion_map and processed_parameters:
            processed_parameters = self._coerce_parameter_types(processed_parameters, config.type_coercion_map, is_many)

        processed_sql, processed_parameters = self._convert_placeholders_for_execution(
            processed_sql, processed_parameters, config, original_styles, needs_execution_conversion, is_many
        )

        if config.output_transformer:
            processed_sql, processed_parameters = config.output_transformer(processed_sql, processed_parameters)

        final_param_info = self._validator.extract_parameters(processed_sql)
        final_profile = ParameterProfile(final_param_info)
        sqlglot_sql = self._normalize_sql_for_parsing(processed_sql, final_param_info, dialect)
        result = ParameterProcessingResult(processed_sql, processed_parameters, final_profile, sqlglot_sql=sqlglot_sql)

        return self._store_cached_result(cache_key, result)

    def _needs_execution_placeholder_conversion(
        self, param_info: "list[ParameterInfo]", config: "ParameterStyleConfig"
    ) -> bool:
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

    def _needs_parse_normalization(self, param_info: "list[ParameterInfo]", dialect: str | None = None) -> bool:
        incompatible_styles = self._validator.get_sqlglot_incompatible_styles(dialect)
        return any(p.style in incompatible_styles for p in param_info)

    def _convert_placeholders_for_execution(
        self,
        sql: str,
        parameters: Any,
        config: "ParameterStyleConfig",
        original_styles: "set[ParameterStyle]",
        needs_execution_conversion: bool,
        is_many: bool,
    ) -> "tuple[str, Any]":
        if not needs_execution_conversion:
            return sql, parameters

        if is_many and config.preserve_original_params_for_many and isinstance(parameters, (list, tuple)):
            target_style = self._select_execution_style(original_styles, config)
            processed_sql, _ = self._converter.convert_placeholder_style(sql, parameters, target_style, is_many)
            return processed_sql, parameters

        target_style = self._select_execution_style(original_styles, config)
        return self._converter.convert_placeholder_style(sql, parameters, target_style, is_many)
