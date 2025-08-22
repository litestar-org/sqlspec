"""Parameter processing system for SQL statements.

This module implements parameter processing including type conversion,
style conversion, and validation for SQL statements.

Components:
- ParameterStyle enum: Supported parameter styles
- TypedParameter: Preserves type information through processing
- ParameterInfo: Tracks parameter metadata
- ParameterValidator: Extracts and validates parameters
- ParameterConverter: Handles parameter style conversions
- ParameterProcessor: Parameter processing coordinator
- ParameterStyleConfig: Configuration for parameter processing

Processing:
- Two-phase processing: compatibility and execution format
- Type-specific parameter wrapping
- Parameter style conversions
- Support for multiple parameter styles and database adapters
"""

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import singledispatch
from typing import Any, Callable, Optional

from mypy_extensions import mypyc_attr

__all__ = (
    "ParameterConverter",
    "ParameterInfo",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "ParameterValidator",
    "TypedParameter",
    "is_iterable_parameters",
    "wrap_with_type",
)


_PARAMETER_REGEX = re.compile(
    r"""
    (?P<dquote>"(?:[^"\\]|\\.)*") |
    (?P<squote>'(?:[^'\\]|\\.)*') |
    (?P<dollar_quoted_string>\$(?P<dollar_quote_tag_inner>\w*)?\$[\s\S]*?\$\4\$) |
    (?P<line_comment>--[^\r\n]*) |
    (?P<block_comment>/\*(?:[^*]|\*(?!/))*\*/) |
    (?P<pg_q_operator>\?\?|\?\||\?&) |
    (?P<pg_cast>::(?P<cast_type>\w+)) |
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |
    (?P<pyformat_pos>%s) |
    (?P<positional_colon>:(?P<colon_num>\d+)) |
    (?P<named_colon>:(?P<colon_name>\w+)) |
    (?P<named_at>@(?P<at_name>\w+)) |
    (?P<numeric>\$(?P<numeric_num>\d+)) |
    (?P<named_dollar_param>\$(?P<dollar_param_name>\w+)) |
    (?P<qmark>\?)
    """,
    re.VERBOSE | re.IGNORECASE | re.MULTILINE | re.DOTALL,
)


class ParameterStyle(str, Enum):
    """Parameter style enumeration.

    Supported parameter styles:
    - QMARK: ? placeholders
    - NUMERIC: $1, $2 placeholders
    - POSITIONAL_PYFORMAT: %s placeholders
    - NAMED_PYFORMAT: %(name)s placeholders
    - NAMED_COLON: :name placeholders
    - NAMED_AT: @name placeholders
    - NAMED_DOLLAR: $name placeholders
    - POSITIONAL_COLON: :1, :2 placeholders
    - STATIC: Direct embedding of values in SQL
    - NONE: No parameters supported
    """

    NONE = "none"
    STATIC = "static"
    QMARK = "qmark"
    NUMERIC = "numeric"
    NAMED_COLON = "named_colon"
    POSITIONAL_COLON = "positional_colon"
    NAMED_AT = "named_at"
    NAMED_DOLLAR = "named_dollar"
    NAMED_PYFORMAT = "pyformat_named"
    POSITIONAL_PYFORMAT = "pyformat_positional"


@mypyc_attr(allow_interpreted_subclasses=False)
class TypedParameter:
    """Parameter wrapper that preserves type information.

    Maintains type information through parsing and execution
    format conversion.

    Attributes:
        value: The parameter value
        original_type: The original Python type of the value
        semantic_name: Optional name for debugging purposes
    """

    __slots__ = ("_hash", "original_type", "semantic_name", "value")

    def __init__(self, value: Any, original_type: Optional[type] = None, semantic_name: Optional[str] = None) -> None:
        """Initialize typed parameter wrapper.

        Args:
            value: The parameter value
            original_type: Original type (defaults to type(value))
            semantic_name: Optional semantic name for debugging
        """
        self.value = value
        self.original_type = original_type or type(value)
        self.semantic_name = semantic_name
        self._hash: Optional[int] = None

    def __hash__(self) -> int:
        """Cached hash value."""
        if self._hash is None:
            value_id = id(self.value)
            self._hash = hash((value_id, self.original_type, self.semantic_name))
        return self._hash

    def __eq__(self, other: object) -> bool:
        """Equality comparison for TypedParameter instances."""
        if not isinstance(other, TypedParameter):
            return False
        return (
            self.value == other.value
            and self.original_type == other.original_type
            and self.semantic_name == other.semantic_name
        )

    def __repr__(self) -> str:
        """String representation for debugging."""
        name_part = f", semantic_name='{self.semantic_name}'" if self.semantic_name else ""
        return f"TypedParameter({self.value!r}, original_type={self.original_type.__name__}{name_part})"


@singledispatch
def _wrap_parameter_by_type(value: Any, semantic_name: Optional[str] = None) -> Any:
    """Type-specific parameter wrapping using singledispatch.

    Args:
        value: Parameter value to potentially wrap
        semantic_name: Optional semantic name for debugging

    Returns:
        Either the original value or TypedParameter wrapper
    """
    return value


@_wrap_parameter_by_type.register
def _(value: bool, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap boolean values to prevent SQLGlot parsing issues."""
    return TypedParameter(value, bool, semantic_name)


@_wrap_parameter_by_type.register
def _(value: Decimal, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap Decimal values to preserve precision."""
    return TypedParameter(value, Decimal, semantic_name)


@_wrap_parameter_by_type.register
def _(value: datetime, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap datetime values for database-specific formatting."""
    return TypedParameter(value, datetime, semantic_name)


@_wrap_parameter_by_type.register
def _(value: date, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap date values for database-specific formatting."""
    return TypedParameter(value, date, semantic_name)


@_wrap_parameter_by_type.register
def _(value: bytes, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap bytes values to prevent string conversion issues in ADBC/Arrow."""
    return TypedParameter(value, bytes, semantic_name)


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterInfo:
    """Information about a detected parameter in SQL.

    Tracks parameter metadata for conversion operations.

    Attributes:
        name: Parameter name (for named styles)
        style: Parameter style
        position: Character position in SQL string
        ordinal: Order of appearance (0-indexed)
        placeholder_text: Original text in SQL
    """

    __slots__ = ("name", "ordinal", "placeholder_text", "position", "style")

    def __init__(
        self, name: Optional[str], style: ParameterStyle, position: int, ordinal: int, placeholder_text: str
    ) -> None:
        """Initialize parameter information.

        Args:
            name: Parameter name (None for positional styles)
            style: Parameter style enum
            position: Character position in SQL
            ordinal: Order of appearance (0-indexed)
            placeholder_text: Original placeholder text
        """
        self.name = name
        self.style = style
        self.position = position
        self.ordinal = ordinal
        self.placeholder_text = placeholder_text

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"ParameterInfo(name={self.name!r}, style={self.style!r}, "
            f"position={self.position}, ordinal={self.ordinal}, "
            f"placeholder_text={self.placeholder_text!r})"
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterStyleConfig:
    """Configuration for parameter style processing.

    Provides configuration for parameter processing operations.

    Attributes:
        default_parameter_style: Primary parsing style
        supported_parameter_styles: All input styles supported
        supported_execution_parameter_styles: Styles driver can execute
        default_execution_parameter_style: Target execution format
        type_coercion_map: Type conversions
        output_transformer: Final SQL/parameter transformation hook
        preserve_parameter_format: Maintain original parameter structure
        needs_static_script_compilation: Embed parameters in SQL
    """

    __slots__ = (
        "allow_mixed_parameter_styles",
        "ast_transformer",
        "default_execution_parameter_style",
        "default_parameter_style",
        "has_native_list_expansion",
        "needs_static_script_compilation",
        "output_transformer",
        "preserve_original_params_for_many",
        "preserve_parameter_format",
        "supported_execution_parameter_styles",
        "supported_parameter_styles",
        "type_coercion_map",
    )

    def __init__(
        self,
        default_parameter_style: ParameterStyle,
        supported_parameter_styles: Optional[set[ParameterStyle]] = None,
        supported_execution_parameter_styles: Optional[set[ParameterStyle]] = None,
        default_execution_parameter_style: Optional[ParameterStyle] = None,
        type_coercion_map: Optional[dict[type, Callable[[Any], Any]]] = None,
        has_native_list_expansion: bool = False,
        needs_static_script_compilation: bool = False,
        allow_mixed_parameter_styles: bool = False,
        preserve_parameter_format: bool = True,
        preserve_original_params_for_many: bool = False,
        output_transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None,
        ast_transformer: Optional[Callable[[Any, Any], tuple[Any, Any]]] = None,
    ) -> None:
        """Initialize parameter style configuration.

        Args:
            default_parameter_style: Primary parameter style for parsing
            supported_parameter_styles: All input styles this config supports
            supported_execution_parameter_styles: Styles driver can execute
            default_execution_parameter_style: Target format for execution
            type_coercion_map: Driver-specific type conversions
            has_native_list_expansion: Driver supports native array parameters
            output_transformer: Final transformation hook
            needs_static_script_compilation: Embed parameters directly in SQL
            allow_mixed_parameter_styles: Support mixed styles in single query
            preserve_parameter_format: Maintain original parameter structure
            preserve_original_params_for_many: Return original list of tuples for execute_many
            ast_transformer: AST-based transformation hook for SQL/parameter manipulation
        """
        self.default_parameter_style = default_parameter_style
        self.supported_parameter_styles = (
            supported_parameter_styles if supported_parameter_styles is not None else {default_parameter_style}
        )
        self.supported_execution_parameter_styles = supported_execution_parameter_styles
        self.default_execution_parameter_style = default_execution_parameter_style or default_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.output_transformer = output_transformer
        self.ast_transformer = ast_transformer
        self.needs_static_script_compilation = needs_static_script_compilation
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles
        self.preserve_parameter_format = preserve_parameter_format
        self.preserve_original_params_for_many = preserve_original_params_for_many

    def hash(self) -> int:
        """Generate hash for cache key generation.

        Returns:
            Hash value for cache key generation
        """
        hash_components = (
            self.default_parameter_style.value,
            frozenset(s.value for s in self.supported_parameter_styles),
            (
                frozenset(s.value for s in self.supported_execution_parameter_styles)
                if self.supported_execution_parameter_styles
                else None
            ),
            self.default_execution_parameter_style.value,
            tuple(sorted(self.type_coercion_map.keys(), key=str)) if self.type_coercion_map else None,
            self.has_native_list_expansion,
            self.preserve_original_params_for_many,
            bool(self.output_transformer),
            self.needs_static_script_compilation,
            self.allow_mixed_parameter_styles,
            self.preserve_parameter_format,
            bool(self.ast_transformer),
        )
        return hash(hash_components)


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterValidator:
    """Parameter validation and extraction.

    Extracts parameter information from SQL strings and determines
    compatibility with target dialects.
    """

    __slots__ = ("_parameter_cache",)

    def __init__(self) -> None:
        """Initialize validator with parameter cache."""
        self._parameter_cache: dict[str, list[ParameterInfo]] = {}

    def _extract_parameter_style(self, match: "re.Match[str]") -> "tuple[Optional[ParameterStyle], Optional[str]]":
        """Extract parameter style and name from regex match.

        Args:
            match: Regex match object

        Returns:
            Tuple of (style, name) or (None, None) if not a parameter
        """

        if match.group("qmark"):
            return ParameterStyle.QMARK, None

        if match.group("named_colon"):
            return ParameterStyle.NAMED_COLON, match.group("colon_name")

        if match.group("numeric"):
            return ParameterStyle.NUMERIC, match.group("numeric_num")

        if match.group("named_at"):
            return ParameterStyle.NAMED_AT, match.group("at_name")

        if match.group("pyformat_named"):
            return ParameterStyle.NAMED_PYFORMAT, match.group("pyformat_name")

        if match.group("pyformat_pos"):
            return ParameterStyle.POSITIONAL_PYFORMAT, None

        if match.group("positional_colon"):
            return ParameterStyle.POSITIONAL_COLON, match.group("colon_num")

        if match.group("named_dollar_param"):
            return ParameterStyle.NAMED_DOLLAR, match.group("dollar_param_name")

        return None, None

    def extract_parameters(self, sql: str) -> "list[ParameterInfo]":
        """Extract all parameters from SQL.

        Args:
            sql: SQL string to analyze

        Returns:
            List of ParameterInfo objects for each detected parameter
        """
        cached_result = self._parameter_cache.get(sql)
        if cached_result is not None:
            return cached_result

        parameters: list[ParameterInfo] = []
        ordinal = 0

        skip_groups = (
            "dquote",
            "squote",
            "dollar_quoted_string",
            "line_comment",
            "block_comment",
            "pg_q_operator",
            "pg_cast",
        )

        for match in _PARAMETER_REGEX.finditer(sql):
            if any(match.group(g) for g in skip_groups):
                continue

            style, name = self._extract_parameter_style(match)

            if style is not None:
                parameters.append(
                    ParameterInfo(
                        name=name, style=style, position=match.start(), ordinal=ordinal, placeholder_text=match.group(0)
                    )
                )
                ordinal += 1

        self._parameter_cache[sql] = parameters
        return parameters

    def get_sqlglot_incompatible_styles(self, dialect: Optional[str] = None) -> "set[ParameterStyle]":
        """Get parameter styles incompatible with SQLGlot for dialect.

        Args:
            dialect: SQL dialect for compatibility checking

        Returns:
            Set of parameter styles incompatible with SQLGlot
        """
        base_incompatible = {
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.POSITIONAL_COLON,
        }

        if dialect and dialect.lower() in {"mysql", "mariadb"}:
            return base_incompatible
        if dialect and dialect.lower() in {"postgres", "postgresql"}:
            return {ParameterStyle.POSITIONAL_COLON}
        if dialect and dialect.lower() == "sqlite":
            return {ParameterStyle.POSITIONAL_COLON}
        if dialect and dialect.lower() in {"oracle", "bigquery"}:
            return base_incompatible
        return base_incompatible


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterConverter:
    """Parameter style conversion.

    Handles two-phase parameter processing:
    - Phase 1: Compatibility normalization
    - Phase 2: Execution format conversion
    """

    __slots__ = ("_format_converters", "_placeholder_generators", "validator")

    def __init__(self) -> None:
        """Initialize converter with lookup tables."""
        self.validator = ParameterValidator()

        self._format_converters = {
            ParameterStyle.POSITIONAL_COLON: self._convert_to_positional_colon_format,
            ParameterStyle.NAMED_COLON: self._convert_to_named_colon_format,
            ParameterStyle.NAMED_PYFORMAT: self._convert_to_named_pyformat_format,
            ParameterStyle.QMARK: self._convert_to_positional_format,
            ParameterStyle.NUMERIC: self._convert_to_positional_format,
            ParameterStyle.POSITIONAL_PYFORMAT: self._convert_to_positional_format,
            ParameterStyle.NAMED_AT: self._convert_to_named_colon_format,
            ParameterStyle.NAMED_DOLLAR: self._convert_to_named_colon_format,
        }

        self._placeholder_generators: dict[ParameterStyle, Callable[[Any], str]] = {
            ParameterStyle.QMARK: lambda _: "?",
            ParameterStyle.NUMERIC: lambda i: f"${int(i) + 1}",
            ParameterStyle.NAMED_COLON: lambda name: f":{name}",
            ParameterStyle.POSITIONAL_COLON: lambda i: f":{int(i) + 1}",
            ParameterStyle.NAMED_AT: lambda name: f"@{name}",
            ParameterStyle.NAMED_DOLLAR: lambda name: f"${name}",
            ParameterStyle.NAMED_PYFORMAT: lambda name: f"%({name})s",
            ParameterStyle.POSITIONAL_PYFORMAT: lambda _: "%s",
        }

    def normalize_sql_for_parsing(self, sql: str, dialect: Optional[str] = None) -> "tuple[str, list[ParameterInfo]]":
        """Convert SQL to parsable format.

        Takes raw SQL with potentially incompatible parameter styles and converts
        them to a canonical format for parsing.

        Args:
            sql: Raw SQL string with any parameter style
            dialect: Target SQL dialect for compatibility checking

        Returns:
            Tuple of (parsable_sql, original_parameter_info)
        """
        param_info = self.validator.extract_parameters(sql)

        incompatible_styles = self.validator.get_sqlglot_incompatible_styles(dialect)
        needs_conversion = any(p.style in incompatible_styles for p in param_info)

        if not needs_conversion:
            return sql, param_info

        converted_sql = self._convert_to_sqlglot_compatible(sql, param_info, incompatible_styles)
        return converted_sql, param_info

    def _convert_to_sqlglot_compatible(
        self, sql: str, param_info: "list[ParameterInfo]", incompatible_styles: "set[ParameterStyle]"
    ) -> str:
        """Convert SQL to SQLGlot-compatible format."""
        converted_sql = sql
        for param in reversed(param_info):
            if param.style in incompatible_styles:
                canonical_placeholder = f":param_{param.ordinal}"
                converted_sql = (
                    converted_sql[: param.position]
                    + canonical_placeholder
                    + converted_sql[param.position + len(param.placeholder_text) :]
                )

        return converted_sql

    def convert_placeholder_style(
        self, sql: str, parameters: Any, target_style: ParameterStyle, is_many: bool = False
    ) -> "tuple[str, Any]":
        """Convert SQL and parameters to execution format.

        Args:
            sql: SQL string (possibly from Phase 1 normalization)
            parameters: Parameter values in any format
            target_style: Target parameter style for execution
            is_many: Whether this is for executemany() operation

        Returns:
            Tuple of (final_sql, execution_parameters)
        """
        param_info = self.validator.extract_parameters(sql)

        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, parameters, param_info)

        current_styles = {p.style for p in param_info}
        if len(current_styles) == 1 and target_style in current_styles:
            converted_parameters = self._convert_parameter_format(
                parameters, param_info, target_style, parameters, preserve_parameter_format=True
            )
            return sql, converted_parameters

        converted_sql = self._convert_placeholders_to_style(sql, param_info, target_style)
        converted_parameters = self._convert_parameter_format(
            parameters, param_info, target_style, parameters, preserve_parameter_format=True
        )

        return converted_sql, converted_parameters

    def _convert_placeholders_to_style(
        self, sql: str, param_info: "list[ParameterInfo]", target_style: ParameterStyle
    ) -> str:
        """Convert SQL placeholders to target style."""
        generator = self._placeholder_generators.get(target_style)
        if not generator:
            msg = f"Unsupported target parameter style: {target_style}"
            raise ValueError(msg)

        param_styles = {p.style for p in param_info}
        use_sequential_for_qmark = (
            len(param_styles) == 1 and ParameterStyle.QMARK in param_styles and target_style == ParameterStyle.NUMERIC
        )

        unique_params: dict[str, int] = {}
        for param in param_info:
            param_key = (
                f"{param.placeholder_text}_{param.ordinal}"
                if use_sequential_for_qmark and param.style == ParameterStyle.QMARK
                else param.placeholder_text
            )

            if param_key not in unique_params:
                unique_params[param_key] = len(unique_params)

        converted_sql = sql
        placeholder_text_len_cache: dict[str, int] = {}

        for param in reversed(param_info):
            if param.placeholder_text not in placeholder_text_len_cache:
                placeholder_text_len_cache[param.placeholder_text] = len(param.placeholder_text)
            text_len = placeholder_text_len_cache[param.placeholder_text]

            if target_style in {
                ParameterStyle.QMARK,
                ParameterStyle.NUMERIC,
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.POSITIONAL_COLON,
            }:
                param_key = (
                    f"{param.placeholder_text}_{param.ordinal}"
                    if use_sequential_for_qmark and param.style == ParameterStyle.QMARK
                    else param.placeholder_text
                )
                new_placeholder = generator(unique_params[param_key])
            else:
                param_name = param.name or f"param_{param.ordinal}"
                new_placeholder = generator(param_name)

            converted_sql = (
                converted_sql[: param.position] + new_placeholder + converted_sql[param.position + text_len :]
            )

        return converted_sql

    def _convert_sequence_to_dict(self, parameters: Sequence, param_info: "list[ParameterInfo]") -> "dict[str, Any]":
        """Convert sequence parameters to dictionary for named styles.

        Args:
            parameters: Sequence of parameter values
            param_info: Parameter information from SQL

        Returns:
            Dictionary mapping parameter names to values
        """
        param_dict = {}
        for i, param in enumerate(param_info):
            if i < len(parameters):
                name = param.name or f"param_{param.ordinal}"
                param_dict[name] = parameters[i]
        return param_dict

    def _extract_param_value_mixed_styles(
        self, param: ParameterInfo, parameters: Mapping, param_keys: "list[str]"
    ) -> "tuple[Any, bool]":
        """Extract parameter value for mixed style parameters.

        Args:
            param: Parameter information
            parameters: Parameter mapping
            param_keys: List of parameter keys

        Returns:
            Tuple of (value, found_flag)
        """
        if param.name and param.name in parameters:
            return parameters[param.name], True

        if (
            param.style == ParameterStyle.NUMERIC
            and param.name
            and param.name.isdigit()
            and param.ordinal < len(param_keys)
        ):
            key_to_use = param_keys[param.ordinal]
            return parameters[key_to_use], True

        if f"param_{param.ordinal}" in parameters:
            return parameters[f"param_{param.ordinal}"], True

        ordinal_key = str(param.ordinal + 1)
        if ordinal_key in parameters:
            return parameters[ordinal_key], True

        return None, False

    def _extract_param_value_single_style(self, param: ParameterInfo, parameters: Mapping) -> "tuple[Any, bool]":
        """Extract parameter value for single style parameters.

        Args:
            param: Parameter information
            parameters: Parameter mapping

        Returns:
            Tuple of (value, found_flag) where found_flag indicates if parameter was found
        """
        if param.name and param.name in parameters:
            return parameters[param.name], True
        if f"param_{param.ordinal}" in parameters:
            return parameters[f"param_{param.ordinal}"], True

        ordinal_key = str(param.ordinal + 1)
        if ordinal_key in parameters:
            return parameters[ordinal_key], True

        return None, False

    def _preserve_original_format(self, param_values: "list[Any]", original_parameters: Any) -> Any:
        """Preserve the original parameter container format.

        Args:
            param_values: List of parameter values
            original_parameters: Original parameter container

        Returns:
            Parameters in original format
        """
        if isinstance(original_parameters, tuple):
            return tuple(param_values)
        if isinstance(original_parameters, list):
            return param_values

        if hasattr(original_parameters, "__class__") and callable(original_parameters.__class__):
            try:
                return original_parameters.__class__(param_values)
            except (TypeError, ValueError):
                return tuple(param_values)

        return param_values

    def _convert_parameter_format(
        self,
        parameters: Any,
        param_info: "list[ParameterInfo]",
        target_style: ParameterStyle,
        original_parameters: Any = None,
        preserve_parameter_format: bool = False,
    ) -> Any:
        """Convert parameter format to match target style requirements.

        Args:
            parameters: Current parameter values
            param_info: Parameter information extracted from SQL
            target_style: Target parameter style for conversion
            original_parameters: Original parameter container for type preservation
            preserve_parameter_format: Whether to preserve the original parameter format
        """
        if not parameters or not param_info:
            return parameters

        is_named_style = target_style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.NAMED_PYFORMAT,
        }

        if is_named_style:
            if isinstance(parameters, Mapping):
                return parameters
            if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
                return self._convert_sequence_to_dict(parameters, param_info)

        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return parameters

        elif isinstance(parameters, Mapping):
            param_values = []
            parameter_styles = {p.style for p in param_info}
            has_mixed_styles = len(parameter_styles) > 1

            if has_mixed_styles:
                param_keys = list(parameters.keys())
                for param in param_info:
                    value, found = self._extract_param_value_mixed_styles(param, parameters, param_keys)
                    if found:
                        param_values.append(value)
            else:
                for param in param_info:
                    value, found = self._extract_param_value_single_style(param, parameters)
                    if found:
                        param_values.append(value)

            if preserve_parameter_format and original_parameters is not None:
                return self._preserve_original_format(param_values, original_parameters)

            return param_values

        return parameters

    def _embed_static_parameters(
        self, sql: str, parameters: Any, param_info: "list[ParameterInfo]"
    ) -> "tuple[str, Any]":
        """Embed parameters directly into SQL for STATIC style."""
        if not param_info:
            return sql, None

        unique_params: dict[str, int] = {}
        for param in param_info:
            if param.style in {ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT}:
                param_key = f"{param.placeholder_text}_{param.ordinal}"
            elif (param.style == ParameterStyle.NUMERIC and param.name) or param.name:
                param_key = param.placeholder_text
            else:
                param_key = f"{param.placeholder_text}_{param.ordinal}"

            if param_key not in unique_params:
                unique_params[param_key] = len(unique_params)

        static_sql = sql
        for param in reversed(param_info):
            param_value = self._get_parameter_value_with_reuse(parameters, param, unique_params)

            if param_value is None:
                literal = "NULL"
            elif isinstance(param_value, str):
                escaped = param_value.replace("'", "''")
                literal = f"'{escaped}'"
            elif isinstance(param_value, bool):
                literal = "TRUE" if param_value else "FALSE"
            elif isinstance(param_value, (int, float)):
                literal = str(param_value)
            else:
                literal = f"'{param_value!s}'"

            static_sql = (
                static_sql[: param.position] + literal + static_sql[param.position + len(param.placeholder_text) :]
            )

        return static_sql, None

    def _get_parameter_value(self, parameters: Any, param: ParameterInfo) -> Any:
        """Extract parameter value based on parameter info and format."""
        if isinstance(parameters, Mapping):
            if param.name and param.name in parameters:
                return parameters[param.name]
            if f"param_{param.ordinal}" in parameters:
                return parameters[f"param_{param.ordinal}"]
            if str(param.ordinal + 1) in parameters:
                return parameters[str(param.ordinal + 1)]
        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            if param.ordinal < len(parameters):
                return parameters[param.ordinal]

        return None

    def _get_parameter_value_with_reuse(
        self, parameters: Any, param: ParameterInfo, unique_params: "dict[str, int]"
    ) -> Any:
        """Extract parameter value handling parameter reuse correctly.

        Args:
            parameters: Parameter values in any format
            param: Parameter information
            unique_params: Mapping of unique placeholders to their ordinal positions

        Returns:
            Parameter value, correctly handling reused parameters
        """

        if param.style in {ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT}:
            param_key = f"{param.placeholder_text}_{param.ordinal}"
        elif (param.style == ParameterStyle.NUMERIC and param.name) or param.name:
            param_key = param.placeholder_text
        else:
            param_key = f"{param.placeholder_text}_{param.ordinal}"

        unique_ordinal = unique_params.get(param_key)
        if unique_ordinal is None:
            return None

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

        return None

    def _convert_to_positional_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to positional format (list/tuple)."""
        return self._convert_parameter_format(
            parameters, param_info, ParameterStyle.QMARK, parameters, preserve_parameter_format=False
        )

    def _convert_to_named_colon_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to named colon format (dict)."""
        return self._convert_parameter_format(
            parameters, param_info, ParameterStyle.NAMED_COLON, parameters, preserve_parameter_format=False
        )

    def _convert_to_positional_colon_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to positional colon format with 1-based keys."""
        if isinstance(parameters, Mapping):
            return parameters

        param_dict = {}
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            for i, value in enumerate(parameters):
                param_dict[str(i + 1)] = value

        return param_dict

    def _convert_to_named_pyformat_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to named pyformat format (dict)."""
        return self._convert_parameter_format(
            parameters, param_info, ParameterStyle.NAMED_PYFORMAT, parameters, preserve_parameter_format=False
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterProcessor:
    """Parameter processing engine.

    Main entry point for the parameter processing system that coordinates
    Phase 1 (compatibility) and Phase 2 (execution format).

    Processing Pipeline:
        1. Type wrapping for compatibility (TypedParameter)
        2. Driver-specific type coercions (type_coercion_map)
        3. Phase 1: Normalization if needed
        4. Phase 2: Execution format conversion if needed
        5. Final output transformation (output_transformer)
    """

    __slots__ = ("_cache", "_cache_size", "_converter", "_validator")

    DEFAULT_CACHE_SIZE = 1000

    def __init__(self) -> None:
        """Initialize processor with component coordination."""
        self._cache: dict[str, tuple[str, Any]] = {}
        self._cache_size = 0
        self._validator = ParameterValidator()
        self._converter = ParameterConverter()

    def _handle_static_embedding(
        self, sql: str, parameters: Any, config: ParameterStyleConfig, is_many: bool, cache_key: str
    ) -> "tuple[str, Any]":
        """Handle static parameter embedding for script compilation.

        Args:
            sql: SQL string
            parameters: Parameter values
            config: Parameter configuration
            is_many: Whether this is for execute_many
            cache_key: Cache key for result

        Returns:
            Tuple of (static_sql, static_params)
        """
        coerced_params = parameters
        if config.type_coercion_map and parameters:
            coerced_params = self._apply_type_coercions(parameters, config.type_coercion_map, is_many)

        static_sql, static_params = self._converter.convert_placeholder_style(
            sql, coerced_params, ParameterStyle.STATIC, is_many
        )
        self._cache[cache_key] = (static_sql, static_params)
        return static_sql, static_params

    def _process_parameters_conversion(
        self,
        sql: str,
        parameters: Any,
        config: ParameterStyleConfig,
        original_styles: "set[ParameterStyle]",
        needs_execution_conversion: bool,
        needs_sqlglot_normalization: bool,
        is_many: bool,
    ) -> "tuple[str, Any]":
        """Process parameter conversion phase.

        Args:
            sql: Processed SQL string
            parameters: Processed parameters
            config: Parameter configuration
            original_styles: Original parameter styles detected
            needs_execution_conversion: Whether execution conversion is needed
            needs_sqlglot_normalization: Whether SQLGlot normalization is needed
            is_many: Whether this is for execute_many

        Returns:
            Tuple of (processed_sql, processed_parameters)
        """
        if not (needs_execution_conversion or needs_sqlglot_normalization):
            return sql, parameters

        if is_many and config.preserve_original_params_for_many and isinstance(parameters, (list, tuple)):
            target_style = self._determine_target_execution_style(original_styles, config)
            processed_sql, _ = self._converter.convert_placeholder_style(sql, parameters, target_style, is_many)
            return processed_sql, parameters

        target_style = self._determine_target_execution_style(original_styles, config)
        return self._converter.convert_placeholder_style(sql, parameters, target_style, is_many)

    def process(
        self,
        sql: str,
        parameters: Any,
        config: ParameterStyleConfig,
        dialect: Optional[str] = None,
        is_many: bool = False,
    ) -> "tuple[str, Any]":
        """Process parameters through the complete pipeline.

        Coordinates the entire parameter processing workflow:
            1. Type wrapping for compatibility
            2. Phase 1: Normalization if needed
            3. Phase 2: Execution format conversion
            4. Driver-specific type coercions
            5. Final output transformation

        Args:
            sql: Raw SQL string
            parameters: Parameter values in any format
            config: Parameter style configuration
            dialect: SQL dialect for compatibility
            is_many: Whether this is for execute_many operation

        Returns:
            Tuple of (final_sql, execution_parameters)
        """
        cache_key = f"{sql}:{hash(repr(parameters))}:{config.default_parameter_style}:{is_many}:{dialect}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        param_info = self._validator.extract_parameters(sql)
        original_styles = {p.style for p in param_info} if param_info else set()
        needs_sqlglot_normalization = self._needs_sqlglot_normalization(param_info, dialect)
        needs_execution_conversion = self._needs_execution_conversion(param_info, config)

        needs_static_embedding = config.needs_static_script_compilation and param_info and parameters and not is_many

        if needs_static_embedding:
            return self._handle_static_embedding(sql, parameters, config, is_many, cache_key)

        if (
            not needs_sqlglot_normalization
            and not needs_execution_conversion
            and not config.type_coercion_map
            and not config.output_transformer
        ):
            return sql, parameters

        processed_sql, processed_parameters = sql, parameters

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

        if self._cache_size < self.DEFAULT_CACHE_SIZE:
            self._cache[cache_key] = (processed_sql, processed_parameters)
            self._cache_size += 1

        return processed_sql, processed_parameters

    def _get_sqlglot_compatible_sql(
        self, sql: str, parameters: Any, config: ParameterStyleConfig, dialect: Optional[str] = None
    ) -> "tuple[str, Any]":
        """Get SQL normalized for parsing only (Phase 1 only).

        Performs only Phase 1 normalization to make SQL compatible
        with parsing, without converting to execution format.

        Args:
            sql: Raw SQL string
            parameters: Parameter values
            config: Parameter style configuration
            dialect: SQL dialect for compatibility

        Returns:
            Tuple of (compatible_sql, parameters)
        """

        param_info = self._validator.extract_parameters(sql)

        if self._needs_sqlglot_normalization(param_info, dialect):
            normalized_sql, _ = self._converter.normalize_sql_for_parsing(sql, dialect)
            return normalized_sql, parameters

        return sql, parameters

    def _needs_execution_conversion(self, param_info: "list[ParameterInfo]", config: ParameterStyleConfig) -> bool:
        """Determine if execution format conversion is needed.

        Preserves the original parameter style if it's supported by the execution
        environment, otherwise converts to the default execution style.
        """
        if not param_info:
            return False

        current_styles = {p.style for p in param_info}

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

    def _needs_sqlglot_normalization(self, param_info: "list[ParameterInfo]", dialect: Optional[str] = None) -> bool:
        """Check if SQLGlot normalization is needed for this SQL."""
        incompatible_styles = self._validator.get_sqlglot_incompatible_styles(dialect)
        return any(p.style in incompatible_styles for p in param_info)

    def _determine_target_execution_style(
        self, original_styles: "set[ParameterStyle]", config: ParameterStyleConfig
    ) -> ParameterStyle:
        """Determine the target execution style based on original styles and config.

        Logic:
            1. If there's a single original style and it's in supported execution styles, use it
            2. Otherwise, use the default execution style
            3. If no default execution style, use the default parameter style

        Preserves the original parameter style when possible, only converting
        when necessary for execution compatibility.
        """

        if len(original_styles) == 1 and config.supported_execution_parameter_styles is not None:
            original_style = next(iter(original_styles))
            if original_style in config.supported_execution_parameter_styles:
                return original_style

        return config.default_execution_parameter_style or config.default_parameter_style

    def _apply_type_wrapping(self, parameters: Any) -> Any:
        """Apply type wrapping using singledispatch for performance."""
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [_wrap_parameter_by_type(p) for p in parameters]
        if isinstance(parameters, Mapping):
            wrapped_dict = {}
            for k, v in parameters.items():
                wrapped_dict[k] = _wrap_parameter_by_type(v)
            return wrapped_dict
        return _wrap_parameter_by_type(parameters)

    def _apply_type_coercions(
        self, parameters: Any, type_coercion_map: "dict[type, Callable[[Any], Any]]", is_many: bool = False
    ) -> Any:
        """Apply database-specific type coercions.

        Args:
            parameters: Parameter values to coerce
            type_coercion_map: Type coercion mappings
            is_many: If True, parameters is a list of parameter sets for execute_many
        """

        def coerce_value(value: Any) -> Any:
            # Skip coercion for None values to preserve NULL semantics
            if value is None:
                return value

            if isinstance(value, TypedParameter):
                wrapped_value: Any = value.value
                # Skip coercion for None values even when wrapped
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
            """Coerce a single parameter set (dict, list, tuple, or scalar)."""
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


def is_iterable_parameters(obj: Any) -> bool:
    """Check if object is iterable parameters (not string/bytes).

    Args:
        obj: Object to check

    Returns:
        True if object is iterable parameters
    """
    return isinstance(obj, (list, tuple, set)) or (
        hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, Mapping))
    )


def wrap_with_type(value: Any, semantic_name: Optional[str] = None) -> Any:
    """Public API for type wrapping.

    Args:
        value: Value to potentially wrap
        semantic_name: Optional semantic name

    Returns:
        Original value or TypedParameter wrapper
    """
    return _wrap_parameter_by_type(value, semantic_name)
