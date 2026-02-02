"""Parameter processing pipeline orchestrator."""

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

__all__ = (
    "ParameterProcessor",
    "_structural_fingerprint",
    "_value_fingerprint",
    "structural_fingerprint",
    "value_fingerprint",
)

# Threshold for sampling execute_many parameters instead of full iteration
_EXECUTE_MANY_SAMPLE_THRESHOLD = 10
# Number of records to sample for type signatures
_EXECUTE_MANY_SAMPLE_SIZE = 3


def _structural_fingerprint(parameters: "ParameterPayload", is_many: bool = False) -> Any:
    """Return a structural fingerprint for caching parameter payloads.

    Returns a hashable tuple representing the structure (keys, types, count).
    Avoids string formatting for performance.

    Note: Uses Python 3.7+ dict insertion order instead of sorted() for determinism.
    This means fingerprints depend on the order keys were inserted, which is typically
    consistent within a single codebase.
    """
    if parameters is None:
        return None

    # Fast type dispatch: check concrete types first (2-4x faster than ABC isinstance)
    param_type = type(parameters)

    # Handle dict (most common Mapping type) - fast path
    if param_type is dict:
        if not parameters:
            return ("dict",)
        # Use dict insertion order (Python 3.7+ guaranteed) instead of sorted()
        # This is O(n) vs O(n log n) and produces consistent fingerprints for
        # parameters constructed in the same order (typical usage pattern)
        keys = tuple(parameters.keys())
        type_sig = tuple(type(v) for v in parameters.values())
        return ("dict", keys, type_sig)

    # Handle list and tuple (most common Sequence types) - fast path
    if param_type is list or param_type is tuple:
        if not parameters:
            return ("seq",)

        # Optimization: Fast path for single-item sequence (extremely common)
        if len(parameters) == 1:
            return ("seq", (type(parameters[0]),))

        if is_many:
            return _fingerprint_execute_many(parameters)

        # Single execution with sequence parameters
        type_sig = tuple(type(v) for v in parameters)
        return ("seq", type_sig)

    # Fallback to ABC checks for custom types (Mapping, Sequence subclasses)
    if isinstance(parameters, Mapping):
        if not parameters:
            return ("dict",)
        keys = tuple(parameters.keys())
        type_sig = tuple(type(v) for v in parameters.values())
        return ("dict", keys, type_sig)

    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        if not parameters:
            return ("seq",)

        if len(parameters) == 1:
            return ("seq", (type(parameters[0]),))

        if is_many:
            return _fingerprint_execute_many(parameters)

        type_sig = tuple(type(v) for v in parameters)
        return ("seq", type_sig)

    # Scalar parameter
    return ("scalar", param_type)


def _fingerprint_execute_many(parameters: "Sequence[Any]") -> Any:
    """Generate fingerprint for execute_many parameters.

    Extracted to reduce code duplication and allow inlining of the common single-execution path.
    """
    param_count = len(parameters)
    sample_size = (
        min(_EXECUTE_MANY_SAMPLE_SIZE, param_count)
        if param_count > _EXECUTE_MANY_SAMPLE_THRESHOLD
        else param_count
    )
    first = parameters[0]
    first_type = type(first)

    # Fast type dispatch for first element
    if first_type is dict:
        keys = tuple(first.keys())
        type_sig = tuple(type(v) for v in first.values())
        return ("many_dict", keys, type_sig, param_count)

    if first_type is list or first_type is tuple:
        type_sigs: list[tuple[type, ...]] = []
        for i in range(sample_size):
            param_item: Any = parameters[i]
            type_sigs.append(tuple(type(v) for v in param_item))
        return ("many_seq", tuple(type_sigs), param_count)

    # Fallback to ABC checks
    if isinstance(first, Mapping):
        keys = tuple(first.keys())
        type_sig = tuple(type(v) for v in first.values())
        return ("many_dict", keys, type_sig, param_count)

    if isinstance(first, Sequence) and not isinstance(first, (str, bytes)):
        type_sigs = []
        for i in range(sample_size):
            param_item = parameters[i]
            type_sigs.append(tuple(type(v) for v in param_item))
        return ("many_seq", tuple(type_sigs), param_count)

    # Scalar values in sequence for execute_many
    type_sig = tuple(type(parameters[i]) for i in range(sample_size))
    return ("many_scalar", type_sig, param_count)


def structural_fingerprint(parameters: "ParameterPayload", is_many: bool = False) -> str:
    """Return a structural fingerprint for parameter payloads.

    This fingerprint is based on parameter STRUCTURE (keys, types, count) only,
    NOT on actual values. This improves cache hit rates for repeated queries
    with different parameter values.

    Args:
        parameters: Original parameter payload supplied by the caller.
        is_many: Whether this is for execute_many operation.

    Returns:
        Deterministic fingerprint string derived from parameter structure.
    """
    return str(_structural_fingerprint(parameters, is_many))


def value_fingerprint(parameters: "ParameterPayload") -> str:
    """Return a value-based fingerprint for parameter payloads.

    Unlike structural_fingerprint, this includes actual parameter VALUES in the hash.
    Used for static script compilation where SQL has values embedded directly.

    Args:
        parameters: Original parameter payload supplied by the caller.

    Returns:
        Deterministic fingerprint string including parameter values.
    """
    return str(_value_fingerprint(parameters))


def _value_fingerprint(parameters: "ParameterPayload") -> Any:
    """Return a value-based fingerprint for parameter payloads.

    Args:
        parameters: Original parameter payload supplied by the caller.

    Returns:
        Hashable representation including parameter values.
    """
    if parameters is None:
        return None

    # Use repr for value-based hashing - includes both structure and values
    # Return as tuple to match structural_fingerprint return type (hashable)
    return ("values", repr(parameters))


def _coerce_nested_value(value: object, type_coercion_map: "dict[type, Callable[[Any], Any]]") -> object:
    # Fast type dispatch for common types
    value_type = type(value)
    if value_type is list or value_type is tuple:
        return [_coerce_parameter_value(item, type_coercion_map) for item in value]  # type: ignore[union-attr]
    if value_type is dict:
        return {key: _coerce_parameter_value(val, type_coercion_map) for key, val in value.items()}  # type: ignore[union-attr]
    return value


def _coerce_parameter_value(value: object, type_coercion_map: "dict[type, Callable[[Any], Any]]") -> object:
    if value is None:
        return value

    value_type = type(value)
    # Fast path: check TypedParameter by type identity (2-4x faster than isinstance)
    if value_type is TypedParameter:
        typed_param: TypedParameter = value  # type: ignore[assignment]
        wrapped_value: object = typed_param.value
        if wrapped_value is None:
            return wrapped_value
        original_type = typed_param.original_type
        if original_type in type_coercion_map:
            coerced = type_coercion_map[original_type](wrapped_value)
            return _coerce_nested_value(coerced, type_coercion_map)
        return wrapped_value

    if value_type in type_coercion_map:
        coerced = type_coercion_map[value_type](value)
        return _coerce_nested_value(coerced, type_coercion_map)
    return value


def _coerce_parameter_set(param_set: object, type_coercion_map: "dict[type, Callable[[Any], Any]]") -> object:
    # Fast type dispatch for common types
    param_type = type(param_set)
    if param_type is list or param_type is tuple:
        return [_coerce_parameter_value(item, type_coercion_map) for item in param_set]  # type: ignore[union-attr]
    if param_type is dict:
        return {key: _coerce_parameter_value(val, type_coercion_map) for key, val in param_set.items()}  # type: ignore[union-attr]
    # Fallback to ABC checks for custom types
    if isinstance(param_set, Sequence) and not isinstance(param_set, (str, bytes)):
        return [_coerce_parameter_value(item, type_coercion_map) for item in param_set]
    if isinstance(param_set, Mapping):
        return {key: _coerce_parameter_value(val, type_coercion_map) for key, val in param_set.items()}
    return _coerce_parameter_value(param_set, type_coercion_map)


def _coerce_parameters_payload(
    parameters: "ParameterPayload", type_coercion_map: "dict[type, Callable[[Any], Any]]", is_many: bool
) -> object:
    # Fast type dispatch for common types
    param_type = type(parameters)
    if param_type is list or param_type is tuple:
        if is_many:
            return [_coerce_parameter_set(param_set, type_coercion_map) for param_set in parameters]  # type: ignore[union-attr]
        return [_coerce_parameter_value(item, type_coercion_map) for item in parameters]  # type: ignore[union-attr]
    if param_type is dict:
        return {key: _coerce_parameter_value(val, type_coercion_map) for key, val in parameters.items()}  # type: ignore[union-attr]
    # Fallback to ABC checks for custom types
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
        self._cache: OrderedDict[Any, ParameterProcessingResult] = OrderedDict()
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
        self,
        sql: str,
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        is_many: bool,
        cache_key: Any | None,
        input_named_parameters: "tuple[str, ...]",
    ) -> "ParameterProcessingResult":
        coerced_params = parameters
        if config.type_coercion_map and parameters:
            coerced_params = self._coerce_parameter_types(parameters, config.type_coercion_map, is_many)

        static_sql, static_params = self._converter.convert_placeholder_style(
            sql, coerced_params, ParameterStyle.STATIC, is_many, strict_named_parameters=config.strict_named_parameters
        )
        result = ParameterProcessingResult(
            static_sql,
            static_params,
            ParameterProfile.empty(),
            sqlglot_sql=static_sql,
            input_named_parameters=input_named_parameters,
            applied_wrap_types=False,
        )
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
        # Fast type dispatch for common types
        param_type = type(parameters)
        if param_type is list or param_type is tuple:
            return [wrap_with_type(p) for p in parameters]  # type: ignore[union-attr]
        if param_type is dict:
            return {k: wrap_with_type(v) for k, v in parameters.items()}  # type: ignore[union-attr]
        # Fallback to ABC checks for custom types
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
        # Fast type narrowing - _coerce_parameters_payload returns object but produces concrete types
        if result is None:
            return None
        result_type = type(result)
        if result_type is dict:
            return result  # type: ignore[return-value]
        if result_type is list:
            return result  # type: ignore[return-value]
        if result_type is tuple:
            return result  # type: ignore[return-value]
        return None

    def _store_cached_result(
        self, cache_key: Any | None, result: "ParameterProcessingResult"
    ) -> "ParameterProcessingResult":
        if self._cache_max_size <= 0 or cache_key is None:
            return result
        self._cache[cache_key] = result
        self._cache.move_to_end(cache_key)
        if len(self._cache) > self._cache_max_size:
            self._cache.popitem(last=False)
        return result

    def _transform_cached_parameters(
        self,
        parameters: "ParameterPayload",
        cached_profile: "ParameterProfile",
        config: "ParameterStyleConfig",
        *,
        input_named_parameters: "tuple[str, ...]",
        is_many: bool,
        apply_wrap_types: bool,
    ) -> "ConvertedParameters":
        """Apply parameter transformations for a cache hit.

        Uses cached metadata to efficiently transform parameters without re-parsing SQL.
        This ensures new parameter values undergo the same transformations as the original
        cached request (type wrapping, coercion, named-to-positional mapping).

        Args:
            parameters: New parameter payload to transform.
            cached_profile: Cached ParameterProfile with execution parameter metadata.
            config: Parameter style configuration.
            input_named_parameters: Cached input named parameter order.
            is_many: Whether this is execute_many.
            apply_wrap_types: Whether to wrap parameters with type metadata.

        Returns:
            Transformed parameters matching the cached SQL's placeholder format.
        """
        if parameters is None:
            return None

        processed: ConvertedParameters = parameters  # type: ignore[assignment]

        # Step 1: Type wrapping (must happen before coercion)
        if apply_wrap_types and processed:
            processed = self._wrap_parameter_types(processed)

        # Step 2: Type coercion
        if config.type_coercion_map and processed:
            processed = self._coerce_parameter_types(processed, config.type_coercion_map, is_many)

        # Step 3: Named-to-positional mapping only when cached SQL uses positional placeholders.
        if input_named_parameters and processed:
            positional_styles = {
                ParameterStyle.QMARK.value,
                ParameterStyle.NUMERIC.value,
                ParameterStyle.POSITIONAL_COLON.value,
                ParameterStyle.POSITIONAL_PYFORMAT.value,
            }
            if any(style in positional_styles for style in cached_profile.styles):
                processed = self._map_named_to_positional(
                    processed, input_named_parameters, is_many, strict=config.strict_named_parameters
                )

        return processed

    def _map_named_to_positional(
        self, parameters: "ConvertedParameters", named_order: "tuple[str, ...]", is_many: bool, strict: bool = False
    ) -> "ConvertedParameters":
        """Map named parameters (dict) to positional (tuple) using cached order.

        Args:
            parameters: Current parameters (dict or sequence).
            named_order: Tuple of parameter names in placeholder order.
            is_many: Whether this is execute_many.
            strict: Whether to raise an error if required parameters are missing.

        Returns:
            Parameters converted to positional tuple if input was dict, else unchanged.

        Raises:
            SQLSpecError: If strict is True and required parameters are missing.
        """
        if not named_order:
            return parameters

        param_type = type(parameters)

        if is_many and (param_type is list or param_type is tuple):
            # Process each row in execute_many
            result: list[Any] = []
            for row in parameters:  # type: ignore[union-attr]
                row_type = type(row)
                if row_type is dict:
                    if strict:
                        missing = [name for name in named_order if name not in row]
                        if missing:
                            from sqlspec.exceptions import SQLSpecError

                            msg = f"Missing required parameters: {missing}"
                            raise SQLSpecError(msg)
                    result.append(tuple(row.get(name) for name in named_order))
                elif isinstance(row, Mapping):
                    # Fallback for custom Mapping types
                    if strict:
                        missing = [name for name in named_order if name not in row]
                        if missing:
                            from sqlspec.exceptions import SQLSpecError

                            msg = f"Missing required parameters: {missing}"
                            raise SQLSpecError(msg)
                    result.append(tuple(row.get(name) for name in named_order))
                else:
                    result.append(row)
            return result

        if param_type is dict:
            if strict:
                missing = [name for name in named_order if name not in parameters]  # type: ignore[operator]
                if missing:
                    from sqlspec.exceptions import SQLSpecError

                    msg = f"Missing required parameters: {missing}"
                    raise SQLSpecError(msg)
            return tuple(parameters.get(name) for name in named_order)  # type: ignore[union-attr]

        # Fallback for custom Mapping types
        if isinstance(parameters, Mapping):
            if strict:
                missing = [name for name in named_order if name not in parameters]
                if missing:
                    from sqlspec.exceptions import SQLSpecError

                    msg = f"Missing required parameters: {missing}"
                    raise SQLSpecError(msg)
            return tuple(parameters.get(name) for name in named_order)

        return parameters

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

        # Fast type dispatch for common types
        payload_type = type(payload)
        if payload_type is dict:
            return True

        if payload_type is list or payload_type is tuple:
            # Check if any item is a dict (fast path) or Mapping (fallback)
            for item in payload:  # type: ignore[union-attr]
                item_type = type(item)
                if item_type is dict:
                    return True
                if isinstance(item, Mapping):
                    return True
            return False

        # Fallback for custom types
        if isinstance(payload, Mapping):
            return True

        if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
            return any(isinstance(item, Mapping) for item in payload)

        return False

    def _normalize_sql_for_parsing(
        self, sql: str, param_info: "list[ParameterInfo]", config: "ParameterStyleConfig"
    ) -> str:
        """Normalize SQL for sqlglot parsing by converting unsupported parameter styles.

        When a parameter style is not in config.supported_parameter_styles (what sqlglot
        can parse for this dialect), convert it to config.default_parameter_style.

        Args:
            sql: SQL string with parameters.
            param_info: List of detected parameter placeholders.
            config: Parameter style configuration.

        Returns:
            SQL string with parameters converted to a sqlglot-compatible style.
        """
        if not self._needs_parse_normalization(param_info, config):
            return sql
        # Convert to the default style that sqlglot can parse for this dialect
        target_style = config.default_parameter_style
        normalized_sql, _ = self._converter.convert_placeholder_style(sql, None, target_style, is_many=False)
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
        *,
        param_fingerprint: Any | None = None,
    ) -> tuple[Any, ...]:
        if param_fingerprint is None:
            # For static script compilation, we must include actual values in the fingerprint
            # because the SQL will have values embedded directly (e.g., VALUES (1, 'foo'))
            if config.needs_static_script_compilation:
                param_fingerprint = _value_fingerprint(parameters)
            else:
                # Use structural fingerprint (keys + types, not values) for better cache hit rates
                param_fingerprint = _structural_fingerprint(parameters, is_many)
        dialect_marker = dialect or "default"
        # Include both input and execution parameter styles to avoid cache collisions
        # (e.g., MySQL asyncmy uses ? for input but %s for execution)
        input_style = config.default_parameter_style.value if config.default_parameter_style else "unknown"
        exec_style = (
            config.default_execution_parameter_style.value if config.default_execution_parameter_style else input_style
        )

        # Optimize: Use tuple as cache key instead of hashing the string representation.
        # This avoids expensive repr() and blake2b hashing of the SQL string on every call.
        # Python's dict/OrderedDict handles tuple keys efficiently using hash().
        return (
            sql,
            param_fingerprint,
            input_style,
            exec_style,
            is_many,
            dialect_marker,
            wrap_types,
            normalize_for_parsing,
        )

    def process(
        self,
        sql: str,
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        dialect: str | None = None,
        is_many: bool = False,
        wrap_types: bool = True,
        param_fingerprint: Any | None = None,
    ) -> "ParameterProcessingResult":
        return self._process_internal(
            sql,
            parameters,
            config,
            dialect=dialect,
            is_many=is_many,
            wrap_types=wrap_types,
            normalize_for_parsing=True,
            param_fingerprint=param_fingerprint,
        )

    def process_for_execution(
        self,
        sql: str,
        parameters: "ParameterPayload",
        config: "ParameterStyleConfig",
        dialect: str | None = None,
        is_many: bool = False,
        wrap_types: bool = True,
        parsed_expression: Any = None,
        param_fingerprint: Any | None = None,
    ) -> "ParameterProcessingResult":
        """Process parameters for execution without parse normalization.

        Args:
            sql: SQL string to process.
            parameters: Parameter payload.
            config: Parameter style configuration.
            dialect: Optional SQL dialect.
            is_many: Whether this is execute_many.
            wrap_types: Whether to wrap parameters with type metadata.
            parsed_expression: Pre-parsed SQLGlot expression to preserve through pipeline.

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
            parsed_expression=parsed_expression,
            param_fingerprint=param_fingerprint,
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
        parsed_expression: Any = None,
        param_fingerprint: Any | None = None,
    ) -> "ParameterProcessingResult":
        cache_key = None
        if self._cache_max_size > 0:
            cache_key = self._make_processor_cache_key(
                sql,
                parameters,
                config,
                is_many,
                dialect,
                wrap_types,
                normalize_for_parsing,
                param_fingerprint=param_fingerprint,
            )
            cached_result = self._cache.get(cache_key)
            if cached_result is not None:
                self._cache.move_to_end(cache_key)
                self._cache_hits += 1
                # For static script compilation, parameters are embedded directly in SQL.
                # Cache key includes parameter values, so a hit means same SQL with same values.
                # Return None for parameters since the driver shouldn't receive any.
                if config.needs_static_script_compilation:
                    return ParameterProcessingResult(
                        cached_result.sql,
                        None,
                        cached_result.parameter_profile,
                        sqlglot_sql=cached_result.sqlglot_sql,
                        parsed_expression=cached_result.parsed_expression,
                        input_named_parameters=cached_result.input_named_parameters,
                        applied_wrap_types=cached_result.applied_wrap_types,
                    )
                # Return cached SQL transformation with NEW parameters transformed
                # to match the cached SQL's placeholder format
                transformed_params = self._transform_cached_parameters(
                    parameters,
                    cached_result.parameter_profile,
                    config,
                    input_named_parameters=cached_result.input_named_parameters,
                    is_many=is_many,
                    apply_wrap_types=cached_result.applied_wrap_types,
                )
                # Apply output transformer if present (it may further transform params)
                final_sql = cached_result.sql
                if config.output_transformer:
                    final_sql, transformed_params = config.output_transformer(final_sql, transformed_params)
                return ParameterProcessingResult(
                    final_sql,
                    transformed_params,
                    cached_result.parameter_profile,
                    sqlglot_sql=cached_result.sqlglot_sql,
                    parsed_expression=cached_result.parsed_expression,
                    input_named_parameters=cached_result.input_named_parameters,
                    applied_wrap_types=cached_result.applied_wrap_types,
                )
            self._cache_misses += 1

        param_info = self._validator.extract_parameters(sql)
        original_styles = {p.style for p in param_info} if param_info else set()
        needs_execution_conversion = self._needs_execution_placeholder_conversion(param_info, config)

        input_named_parameters = tuple(p.name for p in param_info if p.name is not None)

        if config.needs_static_script_compilation and param_info and parameters and not is_many:
            return self._compile_static_script(
                sql, parameters, config, is_many, cache_key, input_named_parameters=input_named_parameters
            )

        requires_mapping = self._needs_mapping_normalization(parameters, param_info, is_many)
        if (
            not needs_execution_conversion
            and not config.type_coercion_map
            and not config.output_transformer
            and not requires_mapping
        ):
            normalized_sql = self._normalize_sql_for_parsing(sql, param_info, config) if normalize_for_parsing else sql
            result = ParameterProcessingResult(
                sql,
                parameters,
                ParameterProfile(param_info),
                sqlglot_sql=normalized_sql,
                parsed_expression=parsed_expression,
                input_named_parameters=input_named_parameters,
                applied_wrap_types=False,
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

        applied_wrap_types = False
        if processed_parameters and wrap_types:
            processed_parameters = self._wrap_parameter_types(processed_parameters)
            applied_wrap_types = True

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
            self._normalize_sql_for_parsing(processed_sql, final_param_info, config)
            if normalize_for_parsing
            else processed_sql
        )
        result = ParameterProcessingResult(
            processed_sql,
            processed_parameters,
            final_profile,
            sqlglot_sql=sqlglot_sql,
            parsed_expression=parsed_expression,
            input_named_parameters=input_named_parameters,
            applied_wrap_types=applied_wrap_types,
        )

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

        if len(current_styles) > 1:
            return True

        if len(current_styles) == 1:
            current_style = next(iter(current_styles))
            supported_styles = config.supported_execution_parameter_styles
            if supported_styles is None:
                return True
            return current_style not in supported_styles

        return True

    def _needs_parse_normalization(self, param_info: "list[ParameterInfo]", config: "ParameterStyleConfig") -> bool:
        """Check if SQL needs normalization before sqlglot parsing.

        A style needs normalization if it's NOT in config.supported_parameter_styles,
        which represents what sqlglot can parse for this driver's dialect.

        Args:
            param_info: List of detected parameter placeholders.
            config: Parameter style configuration with supported_parameter_styles.

        Returns:
            True if any parameter style is not supported by sqlglot for this dialect.
        """
        supported = config.supported_parameter_styles
        return any(p.style not in supported for p in param_info)

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
