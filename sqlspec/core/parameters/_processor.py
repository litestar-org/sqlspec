"""Parameter processing pipeline orchestrator."""

import hashlib
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._alignment import looks_like_execute_many
from sqlspec.core.parameters._converter import ParameterConverter
from sqlspec.core.parameters._types import (
    ConvertedParameters,
    ParameterInfo,
    ParameterPayload,
    ParameterProcessingResult,
    ParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    TypedParameter,
    wrap_with_type,
)
from sqlspec.core.parameters._validator import ParameterValidator

__all__ = ("ParameterProcessor", "fingerprint_parameters")


def _mapping_item_sort_key(item: "tuple[object, object]") -> str:
    return repr(item[0])


def _fingerprint_parameters(parameters: "ParameterPayload") -> str:
    """Return a stable fingerprint for caching parameter payloads.

    Args:
        parameters: Original parameter payload supplied by the caller.

    Returns:
        Deterministic fingerprint string derived from the parameter payload.
    """
    if parameters is None:
        return "none"

    if isinstance(parameters, Mapping):
        if not parameters:
            return f"{type(parameters).__name__}:empty"
        try:
            items = sorted(parameters.items(), key=_mapping_item_sort_key)
        except Exception:
            items = list(parameters.items())
        data = repr(tuple(items))
    elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        if not parameters:
            return f"{type(parameters).__name__}:empty"
        data = repr(tuple(parameters))
    else:
        data = repr(parameters)

    digest = hashlib.blake2b(data.encode("utf-8"), digest_size=8).hexdigest()
    return f"{type(parameters).__name__}:{digest}"


def fingerprint_parameters(parameters: "ParameterPayload") -> str:
    """Return a stable fingerprint for parameter payloads.

    Args:
        parameters: Original parameter payload supplied by the caller.

    Returns:
        Deterministic fingerprint string derived from the parameter payload.
    """
    return _fingerprint_parameters(parameters)


def _coerce_nested_value(value: object, type_coercion_map: "dict[type, Callable[[Any], Any]]") -> object:
    if isinstance(value, (list, tuple)) and not isinstance(value, (str, bytes)):
        return [_coerce_parameter_value(item, type_coercion_map) for item in value]
    if isinstance(value, dict):
        return {key: _coerce_parameter_value(val, type_coercion_map) for key, val in value.items()}
    return value


def _coerce_parameter_value(value: object, type_coercion_map: "dict[type, Callable[[Any], Any]]") -> object:
    if value is None:
        return value

    if isinstance(value, TypedParameter):
        wrapped_value: object = value.value
        if wrapped_value is None:
            return wrapped_value
        original_type = value.original_type
        if original_type in type_coercion_map:
            coerced = type_coercion_map[original_type](wrapped_value)
            return _coerce_nested_value(coerced, type_coercion_map)
        return wrapped_value

    value_type = type(value)
    if value_type in type_coercion_map:
        coerced = type_coercion_map[value_type](value)
        return _coerce_nested_value(coerced, type_coercion_map)
    return value


def _coerce_parameter_set(param_set: object, type_coercion_map: "dict[type, Callable[[Any], Any]]") -> object:
    if isinstance(param_set, Sequence) and not isinstance(param_set, (str, bytes)):
        return [_coerce_parameter_value(item, type_coercion_map) for item in param_set]
    if isinstance(param_set, Mapping):
        return {key: _coerce_parameter_value(val, type_coercion_map) for key, val in param_set.items()}
    return _coerce_parameter_value(param_set, type_coercion_map)


def _coerce_parameters_payload(
    parameters: "ParameterPayload", type_coercion_map: "dict[type, Callable[[Any], Any]]", is_many: bool
) -> object:
    if is_many and isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
        return [_coerce_parameter_set(param_set, type_coercion_map) for param_set in parameters]
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
        return [_coerce_parameter_value(item, type_coercion_map) for item in parameters]
    if isinstance(parameters, Mapping):
        return {key: _coerce_parameter_value(val, type_coercion_map) for key, val in parameters.items()}
    return _coerce_parameter_value(parameters, type_coercion_map)


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterProcessor:
    """Parameter processing engine coordinating conversion phases."""

    __slots__ = ("_cache", "_cache_hits", "_cache_max_size", "_cache_misses", "_converter", "_validator")

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
        self._cache_hits = 0
        self._cache_misses = 0
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

    def clear_cache(self) -> None:
        """Clear cached processing results and reset stats."""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        if isinstance(self._validator, ParameterValidator):
            self._validator.clear_cache()

    def cache_stats(self) -> "dict[str, int]":
        """Return cache statistics for parameter processing."""
        stats = {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "size": len(self._cache),
            "max_size": self._cache_max_size,
        }
        if isinstance(self._validator, ParameterValidator):
            validator_stats = self._validator.cache_stats()
            stats["validator_hits"] = validator_stats["hits"]
            stats["validator_misses"] = validator_stats["misses"]
            stats["validator_size"] = validator_stats["size"]
            stats["validator_max_size"] = validator_stats["max_size"]
        else:
            stats["validator_hits"] = 0
            stats["validator_misses"] = 0
            stats["validator_size"] = 0
            stats["validator_max_size"] = 0
        return stats

    def _compile_static_script(
        self, sql: str, parameters: "ParameterPayload", config: "ParameterStyleConfig", is_many: bool, cache_key: str
    ) -> "ParameterProcessingResult":
        coerced_params = parameters
        if config.type_coercion_map and parameters:
            coerced_params = self._coerce_parameter_types(parameters, config.type_coercion_map, is_many)

        static_sql, static_params = self._converter.convert_placeholder_style(
            sql, coerced_params, ParameterStyle.STATIC, is_many, strict_named_parameters=config.strict_named_parameters
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

    def _wrap_parameter_types(self, parameters: "ParameterPayload") -> "ConvertedParameters":
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [wrap_with_type(p) for p in parameters]
        if isinstance(parameters, Mapping):
            return {k: wrap_with_type(v) for k, v in parameters.items()}
        return None

    def _coerce_parameter_types(
        self,
        parameters: "ParameterPayload",
        type_coercion_map: "dict[type, Callable[[Any], Any]]",
        is_many: bool = False,
    ) -> "ConvertedParameters":
        result = _coerce_parameters_payload(parameters, type_coercion_map, is_many)
        # Type narrow the result - _coerce_parameters_payload returns object but we know it produces concrete types
        if result is None:
            return None
        if isinstance(result, dict):
            return result
        if isinstance(result, list):
            return result
        if isinstance(result, tuple):
            return result
        return None

    def _store_cached_result(self, cache_key: str, result: "ParameterProcessingResult") -> "ParameterProcessingResult":
        if self._cache_max_size <= 0:
            return result
        self._cache[cache_key] = result
        self._cache.move_to_end(cache_key)
        if len(self._cache) > self._cache_max_size:
            self._cache.popitem(last=False)
        return result

    def _needs_mapping_normalization(
        self, payload: "ParameterPayload", param_info: "list[ParameterInfo]", is_many: bool
    ) -> bool:
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
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        is_many: bool,
        dialect: str | None,
        wrap_types: bool,
        normalize_for_parsing: bool,
    ) -> str:
        param_fingerprint = _fingerprint_parameters(parameters)
        dialect_marker = dialect or "default"
        default_style = config.default_parameter_style.value if config.default_parameter_style else "unknown"
        return (
            f"{sql}:{param_fingerprint}:{default_style}:{is_many}:{dialect_marker}:{wrap_types}:{normalize_for_parsing}"
        )

    def process(
        self,
        sql: str,
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        dialect: str | None = None,
        is_many: bool = False,
        wrap_types: bool = True,
    ) -> "ParameterProcessingResult":
        return self._process_internal(
            sql, parameters, config, dialect=dialect, is_many=is_many, wrap_types=wrap_types, normalize_for_parsing=True
        )

    def process_for_execution(
        self,
        sql: str,
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        dialect: str | None = None,
        is_many: bool = False,
        wrap_types: bool = True,
    ) -> "ParameterProcessingResult":
        """Process parameters for execution without parse normalization.

        Args:
            sql: SQL string to process.
            parameters: Parameter payload.
            config: Parameter style configuration.
            dialect: Optional SQL dialect.
            is_many: Whether this is execute_many.
            wrap_types: Whether to wrap parameters with type metadata.

        Returns:
            ParameterProcessingResult with execution SQL and parameters.
        """
        return self._process_internal(
            sql,
            parameters,
            config,
            dialect=dialect,
            is_many=is_many,
            wrap_types=wrap_types,
            normalize_for_parsing=False,
        )

    def _process_internal(
        self,
        sql: str,
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        *,
        dialect: str | None,
        is_many: bool,
        wrap_types: bool,
        normalize_for_parsing: bool,
    ) -> "ParameterProcessingResult":
        cache_key = self._make_processor_cache_key(
            sql, parameters, config, is_many, dialect, wrap_types, normalize_for_parsing
        )
        if self._cache_max_size > 0:
            cached_result = self._cache.get(cache_key)
            if cached_result is not None:
                self._cache.move_to_end(cache_key)
                self._cache_hits += 1
                return cached_result
            self._cache_misses += 1

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
            normalized_sql = self._normalize_sql_for_parsing(sql, param_info, dialect) if normalize_for_parsing else sql
            result = ParameterProcessingResult(
                sql, parameters, ParameterProfile(param_info), sqlglot_sql=normalized_sql
            )
            return self._store_cached_result(cache_key, result)

        processed_sql, processed_parameters = sql, parameters

        if requires_mapping:
            target_style = self._select_execution_style(original_styles, config)
            processed_sql, processed_parameters = self._converter.convert_placeholder_style(
                processed_sql,
                processed_parameters,
                target_style,
                is_many,
                strict_named_parameters=config.strict_named_parameters,
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
        sqlglot_sql = (
            self._normalize_sql_for_parsing(processed_sql, final_param_info, dialect)
            if normalize_for_parsing
            else processed_sql
        )
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
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        original_styles: "set[ParameterStyle]",
        needs_execution_conversion: bool,
        is_many: bool,
    ) -> "tuple[str, ConvertedParameters]":
        if not needs_execution_conversion:
            # Convert parameters to concrete type for return
            if parameters is None:
                return sql, None
            if isinstance(parameters, dict):
                return sql, parameters
            if isinstance(parameters, list):
                return sql, parameters
            if isinstance(parameters, tuple):
                return sql, parameters
            if isinstance(parameters, Mapping):
                return sql, dict(parameters)
            if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
                return sql, list(parameters)
            return sql, None

        if is_many and config.preserve_original_params_for_many and isinstance(parameters, (list, tuple)):
            target_style = self._select_execution_style(original_styles, config)
            processed_sql, _ = self._converter.convert_placeholder_style(
                sql, parameters, target_style, is_many, strict_named_parameters=config.strict_named_parameters
            )
            return processed_sql, parameters

        target_style = self._select_execution_style(original_styles, config)
        return self._converter.convert_placeholder_style(
            sql, parameters, target_style, is_many, strict_named_parameters=config.strict_named_parameters
        )
