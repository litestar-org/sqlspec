"""Parameter style conversion utilities."""

from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from math import isfinite
from typing import Any, Final

from mypy_extensions import mypyc_attr
from sqlglot import exp

from sqlspec.core.parameters._types import (
    _EXPANDING_POSITIONAL_STYLES,
    _NAMED_STYLES,
    _POSITIONAL_STYLES,
    ConvertedParameters,
    NamedParameterOutput,
    ParameterInfo,
    ParameterMapping,
    ParameterPayload,
    ParameterSequence,
    ParameterStyle,
    PositionalParameterOutput,
)
from sqlspec.core.parameters._validator import ParameterValidator
from sqlspec.exceptions import SQLSpecError

__all__ = ("ParameterConverter",)

_BINARY_LITERAL_TEMPLATES: Final[dict[str | None, str]] = {
    None: "X'{}'",
    "mysql": "X'{}'",
    "sqlite": "X'{}'",
    "postgres": "decode('{}', 'hex')",
    "pgvector": "decode('{}', 'hex')",
    "paradedb": "decode('{}', 'hex')",
    "pg_textsearch": "decode('{}', 'hex')",
    "oracle": "HEXTORAW('{}')",
    "tsql": "0x{}",
    "bigquery": "FROM_HEX('{}')",
    "spanner": "FROM_HEX('{}')",
    "duckdb": "UNHEX('{}')",
}

_ORDERED_PARAM_INFO_MIN_SIZE = 2
_OCCURRENCE_KEYED_STYLES: Final[frozenset[ParameterStyle]] = frozenset({
    ParameterStyle.QMARK,
    ParameterStyle.POSITIONAL_PYFORMAT,
})
_INDEX_STYLES: Final[frozenset[ParameterStyle]] = frozenset({ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_COLON})


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterConverter:
    """Parameter style conversion helper."""

    __slots__ = ("_overrides_public_conversion", "_placeholder_generators", "validator")

    def __init__(self, validator: "ParameterValidator | None" = None) -> None:
        self.validator = validator or ParameterValidator()

        self._placeholder_generators: dict[ParameterStyle, Callable[[Any], str]] = {
            ParameterStyle.QMARK: _placeholder_qmark,
            ParameterStyle.NUMERIC: _placeholder_numeric,
            ParameterStyle.NAMED_COLON: _placeholder_named_colon,
            ParameterStyle.POSITIONAL_COLON: _placeholder_positional_colon,
            ParameterStyle.NAMED_AT: _placeholder_named_at,
            ParameterStyle.NAMED_DOLLAR: _placeholder_named_dollar,
            ParameterStyle.NAMED_PYFORMAT: _placeholder_named_pyformat,
            ParameterStyle.POSITIONAL_PYFORMAT: _placeholder_positional_pyformat,
        }
        converter_type = type(self)
        self._overrides_public_conversion = (
            converter_type.convert_placeholder_style is not ParameterConverter.convert_placeholder_style
            or converter_type.convert_parameter_info_style is not ParameterConverter.convert_parameter_info_style
        )

    def convert_placeholder_style(
        self,
        sql: str,
        parameters: "ParameterPayload",
        target_style: "ParameterStyle",
        is_many: bool = False,
        *,
        dialect: "str | None" = None,
        strict_named_parameters: bool = True,
        param_info: "list[ParameterInfo] | None" = None,
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None" = None,
    ) -> "tuple[str, ConvertedParameters]":
        extracted_param_info = param_info if param_info is not None else self.validator.extract_parameters(sql)

        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, parameters, extracted_param_info, dialect), None

        converted_sql, converted_parameters, _ = self._convert_builtin(
            sql,
            parameters,
            target_style,
            is_many,
            strict_named_parameters=strict_named_parameters,
            param_info=extracted_param_info,
            precomputed_plan=precomputed_plan,
        )
        return converted_sql, converted_parameters

    def _build_conversion_plan(
        self, param_info: "list[ParameterInfo]", target_style: "ParameterStyle"
    ) -> "tuple[list[ParameterInfo], dict[str, int]]":
        ordered_params = _ordered_parameter_info(param_info)

        unique_params: dict[str, int] = {}
        for param in ordered_params:
            param_key = _parameter_lookup_key(param)
            if param_key not in unique_params:
                unique_params[param_key] = len(unique_params)

        return ordered_params, unique_params

    def _render_conversion(
        self,
        sql: str,
        param_info: "list[ParameterInfo]",
        target_style: "ParameterStyle",
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None" = None,
        *,
        collect_metadata: bool = False,
    ) -> "tuple[str, list[ParameterInfo]]":
        """Rewrite placeholders to the target style and describe them in one traversal.

        Returns the rendered SQL and the converted parameter metadata (empty unless
        ``collect_metadata`` is true).
        """
        generator = self._placeholder_generators.get(target_style)
        if generator is None:
            msg = f"Unsupported target parameter style: {target_style}"
            raise ValueError(msg)

        if precomputed_plan is not None:
            ordered_params, unique_params = precomputed_plan
        else:
            ordered_params, unique_params = self._build_conversion_plan(param_info, target_style)

        is_positional_style = _is_positional_style(target_style)
        explicit_indexes = _uses_explicit_indexes(param_info)
        target_is_indexed = target_style in _INDEX_STYLES
        segments: list[str] = []
        last_end = 0
        converted_param_info: list[ParameterInfo] = []
        delta = 0
        names_by_key: dict[str, str] = {}

        for param in ordered_params:
            if is_positional_style:
                explicit_index = _explicit_index(param) if explicit_indexes and target_is_indexed else -1
                converted_index = explicit_index if explicit_index >= 0 else unique_params[_parameter_lookup_key(param)]
                new_placeholder = generator(converted_index)
                name = str(converted_index + 1) if target_is_indexed else None
            else:
                name = names_by_key.setdefault(
                    _parameter_lookup_key(param), _named_parameter_name(param, explicit_indexes)
                )
                new_placeholder = generator(name)

            segments.extend((sql[last_end : param.position], new_placeholder))
            last_end = param.position + len(param.placeholder_text)

            if collect_metadata:
                converted_param_info.append(
                    ParameterInfo(
                        name=name,
                        style=target_style,
                        position=param.position + delta,
                        ordinal=param.ordinal,
                        placeholder_text=new_placeholder,
                    )
                )
                delta += len(new_placeholder) - len(param.placeholder_text)

        segments.append(sql[last_end:])
        return "".join(segments), converted_param_info

    def convert_parameter_info_style(
        self,
        param_info: "list[ParameterInfo]",
        target_style: "ParameterStyle",
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None" = None,
    ) -> "list[ParameterInfo]":
        _, converted_param_info = self._render_conversion(
            "", param_info, target_style, precomputed_plan, collect_metadata=True
        )
        return converted_param_info

    def _convert_builtin(
        self,
        sql: str,
        parameters: "ParameterPayload",
        target_style: "ParameterStyle",
        is_many: bool,
        *,
        strict_named_parameters: bool,
        param_info: "list[ParameterInfo]",
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None",
        preserve_original_batch: bool = False,
        collect_metadata: bool = False,
    ) -> "tuple[str, ConvertedParameters, list[ParameterInfo]]":
        """Convert placeholders and parameters without dispatching through overridable methods."""
        current_style = _single_parameter_style(param_info)
        if current_style is not None and target_style == current_style:
            converted_sql = sql
            converted_param_info = param_info if collect_metadata else []
        else:
            converted_sql, converted_param_info = self._render_conversion(
                sql, param_info, target_style, precomputed_plan, collect_metadata=collect_metadata
            )

        converted_parameters = self._convert_parameter_format(
            parameters,
            param_info,
            target_style,
            parameters,
            preserve_parameter_format=True,
            is_many=is_many,
            strict_named_parameters=strict_named_parameters,
            preserve_original_batch=preserve_original_batch,
        )
        return converted_sql, converted_parameters, converted_param_info

    def _convert_with_metadata(
        self,
        sql: str,
        parameters: "ParameterPayload",
        target_style: "ParameterStyle",
        is_many: bool = False,
        *,
        strict_named_parameters: bool = True,
        param_info: "list[ParameterInfo] | None" = None,
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None" = None,
        preserve_original_batch: bool = False,
    ) -> "tuple[str, ConvertedParameters, list[ParameterInfo]]":
        """Convert placeholder style and return SQL, parameters, and metadata together."""
        extracted_param_info = param_info if param_info is not None else self.validator.extract_parameters(sql)

        if self._overrides_public_conversion:
            converted_sql, converted_parameters = self.convert_placeholder_style(
                sql,
                parameters,
                target_style,
                is_many,
                strict_named_parameters=strict_named_parameters,
                param_info=extracted_param_info,
                precomputed_plan=precomputed_plan,
            )
            converted_param_info = self.convert_parameter_info_style(
                extracted_param_info, target_style, precomputed_plan=precomputed_plan
            )
            if preserve_original_batch and is_many and isinstance(parameters, (list, tuple)):
                return converted_sql, parameters, converted_param_info
            return converted_sql, converted_parameters, converted_param_info

        return self._convert_builtin(
            sql,
            parameters,
            target_style,
            is_many,
            strict_named_parameters=strict_named_parameters,
            param_info=extracted_param_info,
            precomputed_plan=precomputed_plan,
            preserve_original_batch=preserve_original_batch,
            collect_metadata=True,
        )

    def _convert_sequence_to_dict(
        self, parameters: "ParameterSequence", param_info: "list[ParameterInfo]"
    ) -> "NamedParameterOutput":
        param_dict: dict[str, Any] = {}
        explicit_indexes = _uses_explicit_indexes(param_info)
        if explicit_indexes:
            _validate_explicit_index_count(param_info, len(parameters))
        expected = len({_parameter_lookup_key(param) for param in param_info})
        if len(parameters) != expected:
            msg = f"Parameter count mismatch: {len(parameters)} parameters provided but {expected} placeholders referenced."
            raise SQLSpecError(msg)
        slots: dict[str, int] = {}
        names_by_key: dict[str, str] = {}
        for param in param_info:
            explicit_index = _explicit_index(param) if explicit_indexes else -1
            if explicit_index >= 0:
                source_index = explicit_index
            else:
                lookup_key = _parameter_lookup_key(param)
                if lookup_key not in slots:
                    slots[lookup_key] = len(slots)
                source_index = slots[lookup_key]
            if source_index < len(parameters):
                name = names_by_key.setdefault(
                    _parameter_lookup_key(param), _named_parameter_name(param, explicit_indexes)
                )
                if name not in param_dict:
                    param_dict[name] = parameters[source_index]
        return param_dict

    def _align_mapping_for_named_style(
        self, parameters: "Mapping[str, Any]", param_info: "list[ParameterInfo]", strict_named_parameters: bool = True
    ) -> "NamedParameterOutput":
        """Align a mapping with the placeholder names of a named-style target."""
        explicit_indexes = _uses_explicit_indexes(param_info)
        expected_names = {_named_parameter_name(param, explicit_indexes) for param in param_info}
        if expected_names.issubset(parameters.keys()):
            return dict(parameters)
        reserved_keys = _named_placeholder_keys(param_info)
        if not reserved_keys or all(param.style in _NAMED_STYLES for param in param_info):
            return self._convert_sequence_to_dict(list(parameters.values()), param_info)

        if strict_named_parameters:
            missing_names = self._missing_named_parameters(param_info, parameters)
            if missing_names:
                msg = f"Missing named parameter(s): {', '.join(missing_names)}"
                raise SQLSpecError(msg)
        positional_keys = [key for key in parameters if key not in reserved_keys]
        positional_ranks: dict[str, int] = {}
        aligned: dict[str, Any] = {}
        for param in param_info:
            target_name = _named_parameter_name(param, explicit_indexes)
            if target_name in aligned:
                continue
            if param.style in _NAMED_STYLES:
                if param.name is not None and param.name in parameters:
                    aligned[target_name] = parameters[param.name]
                elif param.placeholder_text in parameters:
                    aligned[target_name] = parameters[param.placeholder_text]
                continue
            if target_name not in positional_ranks:
                positional_ranks[target_name] = len(positional_ranks)
            value, found = self._lookup_positional_value(
                param, parameters, positional_ranks[target_name], positional_keys
            )
            if found:
                aligned[target_name] = value
            elif strict_named_parameters:
                raise SQLSpecError(_missing_positional_message(param))
        return aligned

    def _lookup_parameter_value(
        self,
        param: "ParameterInfo",
        parameters: "ParameterMapping",
        param_keys: "list[str]",
        fallback_keys: "list[str] | None" = None,
    ) -> "tuple[object | None, bool, list[str] | None]":
        if param.name and param.name in parameters:
            return parameters[param.name], True, fallback_keys
        if param.placeholder_text in parameters:
            return parameters[param.placeholder_text], True, fallback_keys

        if f"param_{param.ordinal}" in parameters:
            return parameters[f"param_{param.ordinal}"], True, fallback_keys

        ordinal_key = str(param.ordinal + 1)
        if ordinal_key in parameters:
            return parameters[ordinal_key], True, fallback_keys

        if fallback_keys is None:
            try:
                fallback_keys = list(parameters.keys())
            except AttributeError:
                fallback_keys = []

        if fallback_keys and param.ordinal < len(fallback_keys):
            key = fallback_keys[param.ordinal]
            if key in parameters:
                return parameters[key], True, fallback_keys

        return None, False, fallback_keys

    def _lookup_positional_value(
        self,
        param: "ParameterInfo",
        parameters: "ParameterMapping",
        positional_index: int,
        positional_keys: "list[str]",
    ) -> "tuple[object | None, bool]":
        """Resolve a nameless or index-style placeholder from a mapping.

        ``positional_keys`` holds the mapping keys that are not claimed by a named placeholder
        of the same statement, so a named value is never bound to a positional placeholder.
        """
        if param.name and param.name in parameters:
            return parameters[param.name], True
        if param.placeholder_text in parameters:
            return parameters[param.placeholder_text], True
        if f"param_{param.ordinal}" in parameters:
            return parameters[f"param_{param.ordinal}"], True
        ordinal_key = str(param.ordinal + 1)
        if ordinal_key in parameters:
            return parameters[ordinal_key], True
        if 0 <= positional_index < len(positional_keys):
            return parameters[positional_keys[positional_index]], True
        return None, False

    def _missing_named_parameters(
        self, param_info: "list[ParameterInfo]", parameters: "ParameterMapping"
    ) -> "list[str]":
        missing: list[str] = []
        for param in param_info:
            if param.style not in _NAMED_STYLES or not param.name:
                continue
            if param.name in parameters or param.placeholder_text in parameters:
                continue
            missing.append(param.name)
        return sorted(set(missing))

    def _validate_batch_named_parameters(self, param_info: "list[ParameterInfo]", parameters: "Sequence[Any]") -> None:
        """Validate named parameters across batch rows in original order."""
        for param_set in parameters:
            if isinstance(param_set, Mapping):
                missing_names = self._missing_named_parameters(param_info, param_set)
                if missing_names:
                    msg = f"Missing named parameter(s): {', '.join(missing_names)}"
                    raise SQLSpecError(msg)

    def _preserve_original_format(
        self, param_values: "list[Any]", original_parameters: object
    ) -> "PositionalParameterOutput":
        if isinstance(original_parameters, list):
            return param_values
        return tuple(param_values)

    def _convert_parameter_format(
        self,
        parameters: "ParameterPayload",
        param_info: "list[ParameterInfo]",
        target_style: "ParameterStyle",
        original_parameters: object | None = None,
        preserve_parameter_format: bool = False,
        is_many: bool = False,
        *,
        strict_named_parameters: bool = True,
        preserve_original_batch: bool = False,
    ) -> "ConvertedParameters":
        if not parameters or not param_info:
            # When parameters is falsy, it's either None or empty - return None
            if parameters is None:
                return None
            # For empty containers, convert to concrete type
            if isinstance(parameters, Mapping):
                return dict(parameters)
            if isinstance(parameters, (list, tuple)):
                return list(parameters) if isinstance(parameters, list) else tuple(parameters)
            return None

        if (
            is_many
            and isinstance(parameters, Sequence)
            and not isinstance(parameters, (str, bytes, bytearray))
            and parameters
        ):
            if preserve_original_batch and isinstance(parameters, (list, tuple)):
                if strict_named_parameters and target_style not in _NAMED_STYLES:
                    self._validate_batch_named_parameters(param_info, parameters)
                return parameters

            normalized_sets: list[Any] = [
                self._convert_parameter_format(
                    param_set,
                    param_info,
                    target_style,
                    param_set,
                    preserve_parameter_format,
                    is_many=False,
                    strict_named_parameters=strict_named_parameters,
                )
                for param_set in parameters
            ]
            if preserve_parameter_format and isinstance(parameters, tuple):
                return tuple(normalized_sets)
            return normalized_sets

        is_named_style = target_style in _NAMED_STYLES
        if is_named_style:
            if isinstance(parameters, Mapping):
                return self._align_mapping_for_named_style(parameters, param_info, strict_named_parameters)
            if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
                return self._convert_sequence_to_dict(parameters, param_info)

        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return _sequence_to_positional(parameters, param_info, target_style)

        elif isinstance(parameters, Mapping):
            if strict_named_parameters:
                missing_names = self._missing_named_parameters(param_info, parameters)
                if missing_names:
                    msg = f"Missing named parameter(s): {', '.join(missing_names)}"
                    raise SQLSpecError(msg)
            param_values: list[Any] = []
            parameter_styles = {p.style for p in param_info}
            has_mixed_styles = len(parameter_styles) > 1

            unique_params: dict[str, Any] = {}
            param_order: list[str] = []

            fallback_keys: list[str] | None = None
            param_keys = list(parameters.keys()) if has_mixed_styles else []
            reserved_keys = _named_placeholder_keys(param_info)
            positional_keys = [key for key in parameters if key not in reserved_keys]
            explicit_indexes = _uses_explicit_indexes(param_info)
            positional_ranks: dict[str, int] = {}
            for param in param_info:
                param_key = param.placeholder_text if param.name else f"{param.placeholder_text}_{param.ordinal}"
                if param_key not in unique_params:
                    if param.style in _NAMED_STYLES:
                        value, found, fallback_keys = self._lookup_parameter_value(
                            param, parameters, param_keys, fallback_keys
                        )
                    else:
                        positional_ranks.setdefault(
                            param_key, _explicit_index(param) if explicit_indexes else len(positional_ranks)
                        )
                        value, found = self._lookup_positional_value(
                            param, parameters, positional_ranks[param_key], positional_keys
                        )
                        if not found and strict_named_parameters:
                            raise SQLSpecError(_missing_positional_message(param))
                    if found:
                        unique_params[param_key] = value
                        param_order.append(param_key)

            needs_expansion = target_style in _EXPANDING_POSITIONAL_STYLES

            if needs_expansion:
                param_values = []
                for param in param_info:
                    param_key = param.placeholder_text if param.name else f"{param.placeholder_text}_{param.ordinal}"
                    if param_key in unique_params:
                        param_values.append(unique_params[param_key])
            else:
                param_values = [unique_params[param_key] for param_key in param_order]

            if preserve_parameter_format and original_parameters is not None:
                return self._preserve_original_format(param_values, original_parameters)

            return param_values

        # Fallback for non-standard parameters - return None
        return None

    @staticmethod
    def _format_literal(param_value: object | None, dialect: "str | None" = None) -> str:
        """Render a literal for the server's default SQL mode.

        MySQL NO_BACKSLASH_ESCAPES stores doubled backslashes as two characters.
        """
        if param_value is None:
            return "NULL"
        if isinstance(param_value, bool):
            return "TRUE" if param_value else "FALSE"
        if isinstance(param_value, (float, Decimal)):
            finite = param_value.is_finite() if isinstance(param_value, Decimal) else isfinite(param_value)
            if not finite:
                msg = "Cannot embed a non-finite number in statically compiled SQL"
                raise SQLSpecError(msg)
            return str(param_value)
        if isinstance(param_value, int):
            return str(param_value)
        if isinstance(param_value, (bytes, bytearray, memoryview)):
            template = _BINARY_LITERAL_TEMPLATES.get(dialect.lower() if dialect else None)
            if template is None:
                msg = f"Cannot embed a bytes value in statically compiled SQL for dialect '{dialect}'"
                raise SQLSpecError(msg)
            return template.format(bytes(param_value).hex())
        return exp.Literal.string(str(param_value)).sql(dialect=dialect)

    def _embed_static_parameters(
        self, sql: str, parameters: "ParameterPayload", param_info: "list[ParameterInfo]", dialect: "str | None" = None
    ) -> str:
        if not param_info:
            return sql

        unique_params: dict[str, int] = {}
        for param in param_info:
            if param.style in _OCCURRENCE_KEYED_STYLES or not param.name:
                param_key = f"{param.placeholder_text}_{param.ordinal}"
            else:
                param_key = param.placeholder_text

            if param_key not in unique_params:
                unique_params[param_key] = len(unique_params)
        explicit_indexes = _uses_explicit_indexes(param_info)

        segments: list[str] = []
        last_start = len(sql)
        for param in reversed(param_info):
            param_value = self._parameter_value(parameters, param, unique_params, explicit_indexes)
            literal = self._format_literal(param_value, dialect)
            segments.extend((sql[param.position + len(param.placeholder_text) : last_start], literal))
            last_start = param.position
        segments.append(sql[:last_start])
        segments.reverse()
        return "".join(segments)

    def _parameter_value(
        self,
        parameters: "ParameterPayload",
        param: "ParameterInfo",
        unique_params: "dict[str, int]",
        explicit_indexes: bool = False,
    ) -> object | None:
        if param.style in _OCCURRENCE_KEYED_STYLES or not param.name:
            param_key = f"{param.placeholder_text}_{param.ordinal}"
        else:
            param_key = param.placeholder_text

        unique_ordinal = unique_params.get(param_key)
        if unique_ordinal is None:
            msg = f"Missing value for placeholder '{param.placeholder_text}' in statically compiled SQL"
            raise SQLSpecError(msg)

        if explicit_indexes:
            unique_ordinal = _explicit_index(param)

        if isinstance(parameters, Mapping):
            if param.name and param.name in parameters:
                return parameters[param.name]
            if f"param_{unique_ordinal}" in parameters:
                return parameters[f"param_{unique_ordinal}"]
            if str(unique_ordinal + 1) in parameters:
                return parameters[str(unique_ordinal + 1)]
        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            if unique_ordinal < len(parameters):
                return parameters[unique_ordinal]

        msg = f"Missing value for placeholder '{param.placeholder_text}' in statically compiled SQL"
        raise SQLSpecError(msg)


def _placeholder_qmark(_: Any) -> str:
    return "?"


def _placeholder_numeric(index: Any) -> str:
    return f"${int(index) + 1}"


def _placeholder_named_colon(name: Any) -> str:
    return f":{name}"


def _placeholder_positional_colon(index: Any) -> str:
    return f":{int(index) + 1}"


def _placeholder_named_at(name: Any) -> str:
    return f"@{name}"


def _placeholder_named_dollar(name: Any) -> str:
    return f"${name}"


def _placeholder_named_pyformat(name: Any) -> str:
    return f"%({name})s"


def _placeholder_positional_pyformat(_: Any) -> str:
    return "%s"


def _ordered_parameter_info(param_info: "list[ParameterInfo]") -> "list[ParameterInfo]":
    if len(param_info) < _ORDERED_PARAM_INFO_MIN_SIZE:
        return param_info

    previous_position = param_info[0].position
    for param in param_info[1:]:
        if param.position < previous_position:
            return sorted(param_info, key=lambda item: item.position)
        previous_position = param.position
    return param_info


def _single_parameter_style(param_info: "list[ParameterInfo]") -> "ParameterStyle | None":
    if not param_info:
        return None

    style = param_info[0].style
    for param in param_info[1:]:
        if param.style != style:
            return None
    return style


def _is_positional_style(style: "ParameterStyle") -> bool:
    return style in _POSITIONAL_STYLES


def _parameter_lookup_key(param: "ParameterInfo") -> str:
    if param.style in _OCCURRENCE_KEYED_STYLES:
        return f"{param.placeholder_text}_{param.ordinal}"
    return param.placeholder_text


def _named_parameter_name(param: "ParameterInfo", explicit_indexes: bool = False) -> str:
    param_name = param.name or f"param_{param.ordinal}"
    if param_name.isdigit():
        explicit_index = _explicit_index(param) if explicit_indexes else -1
        if explicit_index >= 0:
            return f"param_{explicit_index}"
        return f"param_{param.ordinal}"
    return param_name


def _explicit_index(param: "ParameterInfo") -> int:
    """Return the zero-based index written in a ``$n`` / ``:n`` placeholder, or -1."""
    if param.style not in _INDEX_STYLES:
        return -1
    name = param.name
    if name is None or not name.isdigit():
        return -1
    return int(name) - 1


def _uses_explicit_indexes(param_info: "list[ParameterInfo]") -> bool:
    """Return True when every placeholder is a written ``$n`` / ``:n`` index."""
    if not param_info:
        return False
    return all(_explicit_index(param) >= 0 for param in param_info)


def _validate_explicit_index_count(param_info: "list[ParameterInfo]", provided: int) -> None:
    indexes = {_explicit_index(param) for param in param_info}
    if provided != len(indexes) or max(indexes) >= provided:
        msg = (
            f"Parameter count mismatch: {provided} parameters provided but "
            f"{len(indexes)} distinct positional indexes referenced."
        )
        raise SQLSpecError(msg)


def _missing_positional_message(param: "ParameterInfo") -> str:
    return (
        f"Missing value for positional placeholder {param.placeholder_text!r} "
        f"(placeholder {param.ordinal + 1}); values supplied for named "
        "parameters are not bound to positional placeholders."
    )


def _named_placeholder_keys(param_info: "list[ParameterInfo]") -> "set[str]":
    keys: set[str] = set()
    for param in param_info:
        if param.style in _NAMED_STYLES and param.name:
            keys.add(param.name)
            keys.add(param.placeholder_text)
    return keys


def ambiguous_index_placeholder(param_info: "list[ParameterInfo]") -> "ParameterInfo | None":
    """Return the first ``$n`` / ``:n`` placeholder whose written index is not its slot.

    Only statements that mix index placeholders with another placeholder style are
    inspected. A slot is the rank of a placeholder among the distinct placeholders of
    the statement, which is the position a sequence value is bound to.
    """
    has_index = False
    has_other = False
    for param in param_info:
        if _explicit_index(param) >= 0:
            has_index = True
        else:
            has_other = True
    if not (has_index and has_other):
        return None
    slots: dict[str, int] = {}
    for param in _ordered_parameter_info(param_info):
        key = _parameter_lookup_key(param)
        if key not in slots:
            slots[key] = len(slots)
        written = _explicit_index(param)
        if written >= 0 and written != slots[key]:
            return param
    return None


def ambiguous_index_message(param: "ParameterInfo") -> str:
    return (
        f"Ambiguous positional binding: index placeholder {param.placeholder_text!r} is mixed with other "
        "placeholder styles and its index does not match its position among the statement's placeholders. "
        "Use a single placeholder style, number index placeholders by position, or pass a mapping."
    )


def _sequence_to_positional(
    parameters: "Sequence[Any]", param_info: "list[ParameterInfo]", target_style: ParameterStyle
) -> "list[Any] | tuple[Any, ...]":
    if target_style not in _OCCURRENCE_KEYED_STYLES:
        return list(parameters) if isinstance(parameters, list) else tuple(parameters)
    if _uses_explicit_indexes(param_info):
        _validate_explicit_index_count(param_info, len(parameters))
        order = [_explicit_index(param) for param in param_info]
    else:
        slots: dict[str, int] = {}
        order = []
        for param in param_info:
            key = _parameter_lookup_key(param)
            if key not in slots:
                slots[key] = len(slots)
            order.append(slots[key])
        if len(parameters) != len(slots):
            msg = f"Parameter count mismatch: {len(parameters)} parameters provided but {len(slots)} placeholders referenced."
            raise SQLSpecError(msg)
    expanded = [parameters[index] for index in order]
    return expanded if isinstance(parameters, list) else tuple(expanded)
