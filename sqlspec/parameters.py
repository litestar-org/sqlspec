"""Consolidated parameter processing for SQLSpec.

This module provides centralized parameter handling with all core functionality
in a single file for better maintainability and performance.
"""

import re
from collections.abc import Mapping
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Final, Optional

from mypy_extensions import mypyc_attr

from sqlspec.utils.type_guards import is_iterable_parameters

if TYPE_CHECKING:
    from sqlglot import exp

    from sqlspec.typing import StatementParameters

__all__ = (
    "MAX_32BIT_INT",
    "ParameterConverter",
    "ParameterInfo",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "ParameterValidator",
    "TypedParameter",
)


# Core constants
MAX_32BIT_INT: Final[int] = 2147483647


class ParameterStyle(str, Enum):
    """Parameter style enumeration with string values."""

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

    def __str__(self) -> str:
        """String representation for better error messages."""
        return self.value


class ParameterInfo:
    """Immutable parameter information."""

    __slots__ = ("name", "ordinal", "placeholder_text", "position", "style")

    def __init__(
        self, name: Optional[str], style: ParameterStyle, position: int, ordinal: int, placeholder_text: str
    ) -> None:
        self.name = name
        self.style = style
        self.position = position
        self.ordinal = ordinal
        self.placeholder_text = placeholder_text

    def __eq__(self, other: object) -> bool:
        """Equality comparison for ParameterInfo objects."""
        if not isinstance(other, type(self)):
            return False
        return self.name == other.name and self.style == other.style and self.position == other.position

    def __repr__(self) -> str:
        """String representation compatible with dataclass.__repr__."""
        return f"{type(self).__name__}({', '.join([f'name={self.name!r}', f'ordinal={self.ordinal!r}', f'placeholder_text={self.placeholder_text!r}', f'position={self.position!r}', f'style={self.style!r}'])})"

    def __hash__(self) -> int:
        """Make ParameterInfo hashable for use in cache keys."""
        return hash((self.name, self.style, self.position))


class TypedParameter:
    """Internal container for parameter values with type metadata.

    This class preserves complete type information from SQL literals and user-provided
    parameters, enabling proper type coercion for each database adapter.

    Note:
        This is an internal class. Users never create TypedParameter objects directly.
        The system automatically wraps parameters with type information.
    """

    __slots__ = ("data_type", "semantic_name", "type_hint", "value")

    def __init__(
        self, value: Any, data_type: "exp.DataType", type_hint: str, semantic_name: Optional[str] = None
    ) -> None:
        self.value = value
        self.data_type = data_type
        self.type_hint = type_hint
        self.semantic_name = semantic_name

    def __hash__(self) -> int:
        """Make TypedParameter hashable for use in cache keys."""
        try:
            value_hash = hash(self.value) if not isinstance(self.value, (list, dict)) else hash(repr(self.value))
        except TypeError:
            value_hash = hash(repr(self.value))
        return hash((value_hash, self.type_hint, self.semantic_name))

    def __eq__(self, other: object) -> bool:
        """Equality comparison compatible with dataclass.__eq__."""
        if not isinstance(other, type(self)):
            return False
        return (
            self.value == other.value
            and self.data_type == other.data_type
            and self.type_hint == other.type_hint
            and self.semantic_name == other.semantic_name
        )

    def __repr__(self) -> str:
        """String representation compatible with dataclass.__repr__."""
        return f"{type(self).__name__}({', '.join([f'semantic_name={self.semantic_name!r}', f'data_type={self.data_type!r}', f'type_hint={self.type_hint!r}', f'value={self.value!r}'])})"


class ParameterStyleConfig:
    """Declarative configuration for a driver's parameter handling."""

    __slots__ = (
        "allow_mixed_parameter_styles",
        "default_execution_parameter_style",
        "default_parameter_style",
        "has_native_list_expansion",
        "needs_static_script_compilation",
        "output_transformer",
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
        output_transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None,
        needs_static_script_compilation: bool = True,
        allow_mixed_parameter_styles: bool = False,
        preserve_parameter_format: bool = False,
    ) -> None:
        """Initialize driver parameter configuration."""
        self.supported_parameter_styles = supported_parameter_styles or {default_parameter_style}
        self.default_parameter_style = default_parameter_style
        self.supported_execution_parameter_styles = supported_execution_parameter_styles
        self.default_execution_parameter_style = default_execution_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.output_transformer = output_transformer
        self.needs_static_script_compilation = needs_static_script_compilation
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles
        self.preserve_parameter_format = preserve_parameter_format

    def hash(self) -> int:
        """Generate hash for cache key generation."""
        return hash(
            (
                self.default_parameter_style.value if self.default_parameter_style else None,
                tuple(sorted(s.value for s in self.supported_parameter_styles))
                if self.supported_parameter_styles
                else (),
                tuple(sorted(s.value for s in self.supported_execution_parameter_styles))
                if self.supported_execution_parameter_styles
                else (),
                self.default_execution_parameter_style.value if self.default_execution_parameter_style else None,
                self.has_native_list_expansion,
                bool(self.output_transformer),
                self.needs_static_script_compilation,
                self.allow_mixed_parameter_styles,
                tuple(sorted(str(k) for k in self.type_coercion_map)) if self.type_coercion_map else (),
            )
        )


# Parameter extraction regex
_PARAMETER_REGEX: Final = re.compile(
    r"""
    # Literals and Comments (these should be matched first and skipped)
    (?P<dquote>"(?:[^"\\]|\\.)*") |                             # Group 1: Double-quoted strings
    (?P<squote>'(?:[^'\\]|\\.)*') |                             # Group 2: Single-quoted strings
    # Group 3: Dollar-quoted strings (e.g., $tag$...$tag$ or $$...$$)
    # Group 4 (dollar_quote_tag_inner) is the optional tag, back-referenced by \4
    (?P<dollar_quoted_string>\$(?P<dollar_quote_tag_inner>\w*)?\$[\s\S]*?\$\4\$) |
    (?P<line_comment>--[^\r\n]*) |                             # Group 5: Line comments
    (?P<block_comment>/\*(?:[^*]|\*(?!/))*\*/) |               # Group 6: Block comments
    # Specific non-parameter tokens that resemble parameters or contain parameter-like chars
    # These are matched to prevent them from being identified as parameters.
    (?P<pg_q_operator>\?\?|\?\||\?&) |                         # Group 7: PostgreSQL JSON operators ??, ?|, ?&
    (?P<pg_cast>::(?P<cast_type>\w+)) |                        # Group 8: PostgreSQL ::type casting (cast_type is Group 9)

    # Parameter Placeholders (order can matter if syntax overlaps)
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |          # Group 10: %(name)s (pyformat_name is Group 11)
    (?P<pyformat_pos>%s) |                                      # Group 12: %s
    # Oracle numeric parameters MUST come before named_colon to match :1, :2, etc.
    (?P<positional_colon>:(?P<colon_num>\d+)) |                  # Group 13: :1, :2 (colon_num is Group 14)
    (?P<named_colon>:(?P<colon_name>\w+)) |                     # Group 15: :name (colon_name is Group 16)
    (?P<named_at>@(?P<at_name>\w+)) |                           # Group 17: @name (at_name is Group 18)
    # Group 17: $name or $1 (dollar_param_name is Group 18)
    # Differentiation between $name and $1 is handled in Python code using isdigit()
    (?P<named_dollar_param>\$(?P<dollar_param_name>\w+)) |
    (?P<qmark>\?)                                              # Group 19: ? (now safer due to pg_q_operator rule above)
    """,
    re.VERBOSE | re.IGNORECASE | re.MULTILINE | re.DOTALL,
)


class ParameterValidator:
    """Validates and extracts SQL parameters with detailed information."""

    def __init__(self) -> None:
        """Initialize validator with caching support."""
        self._parameter_cache: dict[str, list[ParameterInfo]] = {}

    def extract_parameters(self, sql: str) -> list[ParameterInfo]:
        """Extract parameter information from SQL string."""
        if sql in self._parameter_cache:
            return self._parameter_cache[sql]

        parameters: list[ParameterInfo] = []
        ordinal = 0

        for match in _PARAMETER_REGEX.finditer(sql):
            if match.group("dquote") or match.group("squote") or match.group("dollar_quoted_string"):
                continue
            if match.group("line_comment") or match.group("block_comment"):
                continue
            if match.group("pg_q_operator") or match.group("pg_cast"):
                continue
            if match.group("qmark"):
                parameters.append(
                    ParameterInfo(
                        name=None,
                        style=ParameterStyle.QMARK,
                        position=match.start("qmark"),
                        ordinal=ordinal,
                        placeholder_text=match.group("qmark"),
                    )
                )
                ordinal += 1
            elif match.group("pyformat_pos"):
                parameters.append(
                    ParameterInfo(
                        name=None,
                        style=ParameterStyle.POSITIONAL_PYFORMAT,
                        position=match.start("pyformat_pos"),
                        ordinal=ordinal,
                        placeholder_text=match.group("pyformat_pos"),
                    )
                )
                ordinal += 1
            elif match.group("positional_colon"):
                name = match.group("colon_num")
                parameters.append(
                    ParameterInfo(
                        name=name,
                        style=ParameterStyle.POSITIONAL_COLON,
                        position=match.start("positional_colon"),
                        ordinal=ordinal,
                        placeholder_text=match.group("positional_colon"),
                    )
                )
                ordinal += 1
            elif match.group("named_colon"):
                name = match.group("colon_name")
                parameters.append(
                    ParameterInfo(
                        name=name,
                        style=ParameterStyle.NAMED_COLON,
                        position=match.start("named_colon"),
                        ordinal=ordinal,
                        placeholder_text=match.group("named_colon"),
                    )
                )
                ordinal += 1
            elif match.group("named_at"):
                name = match.group("at_name")
                parameters.append(
                    ParameterInfo(
                        name=name,
                        style=ParameterStyle.NAMED_AT,
                        position=match.start("named_at"),
                        ordinal=ordinal,
                        placeholder_text=match.group("named_at"),
                    )
                )
                ordinal += 1
            elif match.group("named_dollar_param"):
                name = match.group("dollar_param_name")
                style = ParameterStyle.NUMERIC if name.isdigit() else ParameterStyle.NAMED_DOLLAR
                parameters.append(
                    ParameterInfo(
                        name=name,
                        style=style,
                        position=match.start("named_dollar_param"),
                        ordinal=ordinal,
                        placeholder_text=match.group("named_dollar_param"),
                    )
                )
                ordinal += 1
            elif match.group("pyformat_named"):
                name = match.group("pyformat_name")
                parameters.append(
                    ParameterInfo(
                        name=name,
                        style=ParameterStyle.NAMED_PYFORMAT,
                        position=match.start("pyformat_named"),
                        ordinal=ordinal,
                        placeholder_text=match.group("pyformat_named"),
                    )
                )
                ordinal += 1

        self._parameter_cache[sql] = parameters
        return parameters

    def has_parameters(self, sql: str) -> bool:
        """Quick check if SQL contains any parameters."""
        return bool(self.extract_parameters(sql))

    def get_parameter_styles(self, sql: str) -> set[ParameterStyle]:
        """Get all parameter styles present in the SQL."""
        parameters = self.extract_parameters(sql)
        return {p.style for p in parameters}

    def count_parameters(self, sql: str) -> int:
        """Count the number of parameters in the SQL."""
        return len(self.extract_parameters(sql))

    def get_parameter_style(self, parameters: list[ParameterInfo]) -> ParameterStyle:
        """Determine the dominant parameter style from a list of parameters."""
        if not parameters:
            return ParameterStyle.NONE

        style_counts: dict[ParameterStyle, int] = {}
        for param in parameters:
            style_counts[param.style] = style_counts.get(param.style, 0) + 1

        precedence = {
            ParameterStyle.QMARK: 1,
            ParameterStyle.NUMERIC: 2,
            ParameterStyle.POSITIONAL_COLON: 3,
            ParameterStyle.POSITIONAL_PYFORMAT: 4,
            ParameterStyle.NAMED_AT: 5,
            ParameterStyle.NAMED_DOLLAR: 6,
            ParameterStyle.NAMED_COLON: 7,
            ParameterStyle.NAMED_PYFORMAT: 8,
            ParameterStyle.NONE: 0,
            ParameterStyle.STATIC: 0,
        }
        return max(style_counts.items(), key=lambda x: (x[1], precedence.get(x[0], 0)))[0]

    def get_sqlglot_incompatible_styles(self, dialect: Optional[str] = None) -> set[ParameterStyle]:
        """Get parameter styles incompatible with SQLGlot for a specific dialect.

        Args:
            dialect: The SQL dialect name (e.g., 'postgres', 'sqlite', 'mysql')

        Returns:
            Set of parameter styles that are incompatible with SQLGlot for the given dialect
        """
        # Base incompatible styles for most dialects
        base_incompatible = {
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.POSITIONAL_COLON,
        }

        # Dialect-specific overrides
        dialect_specific = {
            "sqlite": {
                # SQLite in SQLGlot supports pyformat styles
                ParameterStyle.POSITIONAL_COLON
            },
            "mysql": {
                # MySQL cannot parse %s placeholders due to modulo operator conflict
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.NAMED_PYFORMAT,
                ParameterStyle.POSITIONAL_COLON,
            },
            "postgres": base_incompatible,  # PostgreSQL dialect is strict
            "postgresql": base_incompatible,
            "oracle": base_incompatible,
            "bigquery": base_incompatible,
        }

        if dialect and dialect.lower() in dialect_specific:
            return dialect_specific[dialect.lower()]

        return base_incompatible

    def determine_parameter_input_type(self, parameters: list[ParameterInfo]) -> Optional[type]:
        """Determine expected parameter input type based on parameter styles."""
        if not parameters:
            return None

        styles = {p.style for p in parameters}

        named_styles = {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
        }
        return dict if any(style in named_styles for style in styles) else list

    def validate_parameters(
        self, param_info: list[ParameterInfo], provided_parameters: "StatementParameters", sql: str
    ) -> None:
        """Validate that provided parameters match the SQL parameters."""
        from sqlspec.exceptions import ExtraParameterError, MissingParameterError, ParameterStyleMismatchError

        if not param_info:
            if provided_parameters and provided_parameters not in ([], {}):
                msg = f"SQL has no parameters but {type(provided_parameters).__name__} was provided"
                raise ExtraParameterError(msg)
            return

        expected_type = self.determine_parameter_input_type(param_info)

        if expected_type is dict:
            if not isinstance(provided_parameters, (dict, Mapping)):
                msg = f"SQL expects named parameters (dict) but got {type(provided_parameters).__name__}"
                raise ParameterStyleMismatchError(msg)
        elif expected_type is list and isinstance(provided_parameters, (dict, Mapping)):
            msg = f"SQL expects positional parameters (list/tuple) but got {type(provided_parameters).__name__}"
            raise ParameterStyleMismatchError(msg)

        if expected_type is dict and isinstance(provided_parameters, (dict, Mapping)):
            required_names = {p.name for p in param_info if p.name}
            provided_names = set(provided_parameters.keys())

            if missing := required_names - provided_names:
                msg = f"Missing required parameters: {', '.join(sorted(missing))}"
                raise MissingParameterError(msg)

            if extra := provided_names - required_names:
                msg = f"Extra parameters provided: {', '.join(sorted(extra))}"
                raise ExtraParameterError(msg)

        elif is_iterable_parameters(provided_parameters) and not isinstance(provided_parameters, (str, bytes)):
            param_count = len(param_info)
            provided_count = len(list(provided_parameters))

            if provided_count < param_count:
                msg = f"SQL expects {param_count} parameters but only {provided_count} were provided"
                raise MissingParameterError(msg)
            if provided_count > param_count:
                msg = f"SQL expects {param_count} parameters but {provided_count} were provided"
                raise ExtraParameterError(msg)

        elif provided_parameters is not None:
            if len(param_info) != 1:
                msg = f"SQL expects {len(param_info)} parameters but a scalar value was provided"
                raise MissingParameterError(msg)


class ParameterConverter:
    """Parameter parameter conversion with caching and validation."""

    __slots__ = ("_format_converters", "_placeholder_generators", "_type_wrappers", "validator")

    def __init__(self) -> None:
        """Initialize converter with validator and performance optimizations."""
        self.validator = ParameterValidator()

        # Performance optimization: Hash map for O(1) placeholder generation
        self._placeholder_generators = {
            ParameterStyle.QMARK: lambda _i, _param: "?",
            ParameterStyle.NUMERIC: lambda i, _param: f"${i + 1}",
            ParameterStyle.POSITIONAL_PYFORMAT: lambda _i, _param: "%s",
            ParameterStyle.POSITIONAL_COLON: lambda i, _param: f":{i + 1}",
            ParameterStyle.NAMED_COLON: self._generate_named_colon_placeholder,
            ParameterStyle.NAMED_PYFORMAT: self._generate_named_pyformat_placeholder,
            ParameterStyle.NAMED_AT: self._generate_named_at_placeholder,
            ParameterStyle.NAMED_DOLLAR: self._generate_named_dollar_placeholder,
        }

        # Performance optimization: Hash map for O(1) format conversion
        self._format_converters = {
            ParameterStyle.POSITIONAL_COLON: self._convert_to_positional_colon_format,
            ParameterStyle.QMARK: self._convert_to_positional_format,
            ParameterStyle.NUMERIC: self._convert_to_positional_format,
            ParameterStyle.POSITIONAL_PYFORMAT: self._convert_to_positional_format,
            ParameterStyle.NAMED_COLON: self._convert_to_named_colon_format,
            ParameterStyle.NAMED_PYFORMAT: self._convert_to_named_pyformat_format,
        }

        # Performance optimization: Type dispatch for O(1) type wrapping
        self._type_wrappers = self._build_type_wrapper_dispatch()

    def convert_placeholders(
        self, sql: str, target_style: ParameterStyle, parameter_info: Optional[list[ParameterInfo]] = None
    ) -> str:
        """Convert SQL placeholders to a target style using O(1) hash map lookups."""
        parameter_info = parameter_info or self.validator.extract_parameters(sql)

        if not parameter_info:
            return sql

        result_parts = []
        current_pos = 0

        # O(1) lookup for placeholder generator instead of O(n) if/else chain
        placeholder_generator = self._placeholder_generators.get(target_style)
        if not placeholder_generator:
            # Fallback for unknown styles
            def fallback_generator(_i: int, param: ParameterInfo) -> str:
                return param.placeholder_text

            placeholder_generator = fallback_generator

        for i, param in enumerate(parameter_info):
            result_parts.append(sql[current_pos : param.position])
            placeholder = placeholder_generator(i, param)
            result_parts.append(placeholder)
            current_pos = param.position + len(param.placeholder_text)

        result_parts.append(sql[current_pos:])
        return "".join(result_parts)

    def needs_conversion(self, parameter_info: list[ParameterInfo], target_style: ParameterStyle) -> bool:
        """Check if parameter style conversion is needed."""
        if not parameter_info:
            return False

        detected_styles = {p.style for p in parameter_info}

        if target_style == ParameterStyle.NAMED_COLON and ParameterStyle.POSITIONAL_COLON in detected_styles:
            return False

        return target_style not in detected_styles

    def wrap_parameters_with_types(self, parameters: Any, param_info: list[ParameterInfo]) -> Any:
        """Wrap parameters with TypedParameter when type information is needed."""
        if isinstance(parameters, TypedParameter):
            return parameters

        if isinstance(parameters, dict):
            if any(isinstance(v, TypedParameter) for v in parameters.values()):
                return parameters
            return {key: self._wrap_single_parameter(value, key) for key, value in parameters.items()}

        if isinstance(parameters, (list, tuple)):
            if any(isinstance(v, TypedParameter) for v in parameters):
                return parameters

            return [
                self._wrap_single_parameter(
                    value, param_info[i].name if i < len(param_info) and param_info[i].name else None
                )
                for i, value in enumerate(parameters)
            ]
        return self._wrap_single_parameter(parameters, None)

    def _wrap_single_parameter(self, value: Any, semantic_name: Optional[str] = None) -> Any:
        """Wrap a single parameter value if it needs type information."""
        from datetime import date, datetime
        from decimal import Decimal

        import sqlglot.expressions as exp

        if isinstance(value, TypedParameter):
            return value
        if value is None:
            return TypedParameter(
                value=None, data_type=exp.DataType.build("NULL"), type_hint="null", semantic_name=semantic_name
            )

        if isinstance(value, bool):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("BOOLEAN"), type_hint="boolean", semantic_name=semantic_name
            )

        if isinstance(value, int) and abs(value) > MAX_32BIT_INT:
            return TypedParameter(
                value=value, data_type=exp.DataType.build("BIGINT"), type_hint="bigint", semantic_name=semantic_name
            )

        if isinstance(value, Decimal):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("DECIMAL"), type_hint="decimal", semantic_name=semantic_name
            )

        if isinstance(value, date) and not isinstance(value, datetime):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("DATE"), type_hint="date", semantic_name=semantic_name
            )

        if isinstance(value, datetime):
            return TypedParameter(
                value=value,
                data_type=exp.DataType.build("TIMESTAMP"),
                type_hint="timestamp",
                semantic_name=semantic_name,
            )

        if isinstance(value, (bytes, bytearray)):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("BINARY"), type_hint="binary", semantic_name=semantic_name
            )

        if isinstance(value, (list, tuple)) and not isinstance(value, str):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("ARRAY"), type_hint="array", semantic_name=semantic_name
            )

        if isinstance(value, dict):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("JSON"), type_hint="json", semantic_name=semantic_name
            )

        return value

    def _convert_to_sqlglot_compatible(self, sql: str, param_info: list[ParameterInfo]) -> str:
        """Convert SQL with SQLGlot-incompatible parameters to compatible named colon style.

        Simplified version that only returns the converted SQL since the state is not used.
        """
        if not param_info:
            return sql

        result_parts = []
        current_pos = 0

        for i, param in enumerate(param_info):
            result_parts.append(sql[current_pos : param.position])

            new_placeholder = f":param_{i}"
            result_parts.append(new_placeholder)
            current_pos = param.position + len(param.placeholder_text)

        result_parts.append(sql[current_pos:])
        return "".join(result_parts)

    def merge_parameters(
        self, parameters: Optional[Any], args: Optional[list[Any]], kwargs: Optional[dict[str, Any]]
    ) -> Any:
        """Merge parameters from different sources with precedence rules."""
        if parameters is not None:
            return parameters

        if kwargs:
            return kwargs

        if args is not None:
            return args if len(args) != 1 else args[0]
        return None

    def convert_mixed_parameters_to_dict(self, parameters: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert mixed parameter styles to a consistent dict format."""
        if not param_info:
            return {}

        if isinstance(parameters, dict):
            return parameters

        if not isinstance(parameters, (list, tuple)):
            return {"param_0": parameters} if parameters is not None else {}

        parameters_dict: dict[str, Any] = {}
        sorted_parameters = sorted(param_info, key=lambda p: p.position)

        for i, p_info in enumerate(sorted_parameters):
            if i < len(parameters):
                key = p_info.name if p_info.name is not None else f"param_{i}"
                if key in parameters_dict:
                    key = f"param_{i}"
                parameters_dict[key] = parameters[i]

        return parameters_dict

    def convert_placeholder_style(
        self, sql: str, parameters: Any, target_style: ParameterStyle, is_many: bool = False
    ) -> tuple[str, Any]:
        """Convert SQL and parameters to the requested placeholder style."""
        if is_many and isinstance(parameters, list) and parameters and isinstance(parameters[0], (list, tuple)):
            param_info = self.validator.extract_parameters(sql)
            if param_info:
                converted_sql = self.convert_placeholders(sql, target_style, param_info)
                return converted_sql, parameters
            return sql, parameters

        param_info = self.validator.extract_parameters(sql)
        if not param_info:
            return sql, parameters

        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, parameters, param_info)

        if all(p.style == target_style for p in param_info):
            converted_parameters = self._convert_parameters_format(parameters, param_info, target_style)
            return sql, converted_parameters

        converted_sql = self.convert_placeholders(sql, target_style, param_info)
        converted_parameters = self._convert_parameters_format(parameters, param_info, target_style)

        return converted_sql, converted_parameters

    def _embed_static_parameters(self, sql: str, parameters: Any, param_info: list[ParameterInfo]) -> tuple[str, None]:
        """Embed parameter values directly into SQL for STATIC style."""
        import sqlglot

        param_list: list[Any] = []
        if isinstance(parameters, dict):
            for p in param_info:
                if p.name and p.name in parameters:
                    param_list.append(parameters[p.name])
                elif f"param_{p.ordinal}" in parameters:
                    param_list.append(parameters[f"param_{p.ordinal}"])
                elif f"arg_{p.ordinal}" in parameters:
                    param_list.append(parameters[f"arg_{p.ordinal}"])
                else:
                    param_list.append(parameters.get(str(p.ordinal), None))
        elif isinstance(parameters, (list, tuple)):
            param_list = list(parameters)
        elif parameters is not None:
            param_list = [parameters]

        sorted_parameters = sorted(param_info, key=lambda p: p.position, reverse=True)

        for p in sorted_parameters:
            if p.ordinal < len(param_list):
                value = param_list[p.ordinal]

                if hasattr(value, "value"):
                    value = value.value
                if value is None:
                    literal_str = "NULL"
                elif isinstance(value, bool):
                    literal_str = "TRUE" if value else "FALSE"
                elif isinstance(value, str):
                    literal_expr = sqlglot.exp.Literal.string(value)
                    literal_str = literal_expr.sql()
                elif isinstance(value, (int, float)):
                    literal_expr = sqlglot.exp.Literal.number(value)
                    literal_str = literal_expr.sql()
                else:
                    literal_expr = sqlglot.exp.Literal.string(str(value))
                    literal_str = literal_expr.sql()

                start = p.position
                end = start + len(p.placeholder_text)
                sql = sql[:start] + literal_str + sql[end:]

        return sql, None

    def _convert_parameters_format(
        self, parameters: Any, param_info: list[ParameterInfo], target_style: ParameterStyle
    ) -> Any:
        """Convert parameters to the appropriate format for the target style using O(1) hash map lookup."""
        # O(1) lookup for format converter instead of O(n) if/else chain
        converter = self._format_converters.get(target_style)
        if converter:
            return converter(parameters, param_info)
        return parameters

    def _convert_to_positional_colon_format(self, parameters: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert parameters to positional colon format (Oracle :1, :2, :3)."""
        if isinstance(parameters, dict):
            return parameters
        if isinstance(parameters, (list, tuple)):
            return self._convert_list_to_colon_dict(parameters, param_info)
        if parameters is not None:
            return self._convert_single_value_to_colon_dict(parameters, param_info)
        return {}

    def _convert_to_positional_format(self, parameters: Any, param_info: list[ParameterInfo]) -> list[Any]:
        """Convert parameters to positional format (?, $1, %s)."""
        if isinstance(parameters, (list, tuple)):
            return list(parameters)
        if isinstance(parameters, dict):
            param_list = []

            param_values = list(parameters.values())

            for i, p in enumerate(param_info):
                if p.name and p.name in parameters:
                    param_list.append(parameters[p.name])
                elif f"param_{p.ordinal}" in parameters:
                    param_list.append(parameters[f"param_{p.ordinal}"])
                elif str(p.ordinal + 1) in parameters:
                    param_list.append(parameters[str(p.ordinal + 1)])
                elif i < len(param_values):
                    param_list.append(param_values[i])
                else:
                    param_list.append(None)
            return param_list
        if parameters is not None:
            return [parameters]
        return []

    def _convert_to_named_colon_format(self, parameters: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert parameters to named colon format (:name, :param_0)."""
        if isinstance(parameters, dict):
            return parameters
        if isinstance(parameters, (list, tuple)):
            result = {}
            for i, value in enumerate(parameters):
                param_name = None
                if i < len(param_info) and param_info[i].name:
                    param_name = param_info[i].name
                if param_name:
                    result[param_name] = value
                else:
                    result[f"param_{i}"] = value
            return result
        if parameters is not None:
            param_name = param_info[0].name if param_info and param_info[0].name else "param_0"
            return {param_name: parameters}
        return {}

    def _convert_to_named_pyformat_format(self, parameters: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert parameters to named pyformat format (%(name)s, %(param_0)s)."""
        return self._convert_to_named_colon_format(parameters, param_info)

    def _convert_list_to_colon_dict(
        self, parameters: "list[Any] | tuple[Any, ...]", param_info: list[ParameterInfo]
    ) -> dict[str, Any]:
        """Convert list/tuple parameters to colon-style dict format."""
        result_dict: dict[str, Any] = {}

        if param_info:
            has_numeric = any(p.style == ParameterStyle.NUMERIC for p in param_info)
            has_other_styles = any(p.style != ParameterStyle.NUMERIC for p in param_info)

            if has_numeric and has_other_styles:
                sorted_parameters = sorted(param_info, key=lambda p: p.position)
                for i, _ in enumerate(sorted_parameters):
                    if i < len(parameters):
                        result_dict[str(i + 1)] = parameters[i]
                return result_dict

            all_numeric = all(p.name and p.name.isdigit() for p in param_info)
            if all_numeric:
                for i, value in enumerate(parameters):
                    result_dict[str(i + 1)] = value
            else:
                for i, value in enumerate(parameters):
                    if i < len(param_info):
                        param_name = param_info[i].name or str(i + 1)
                        result_dict[param_name] = value
                    else:
                        result_dict[str(i + 1)] = value
        else:
            for i, value in enumerate(parameters):
                result_dict[str(i + 1)] = value

        return result_dict

    def _convert_single_value_to_colon_dict(self, parameters: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert single value parameter to colon-style dict format."""
        result_dict: dict[str, Any] = {}
        if param_info and param_info[0].name and param_info[0].name.isdigit():
            result_dict[param_info[0].name] = parameters
        else:
            result_dict["1"] = parameters
        return result_dict

    def _generate_named_colon_placeholder(self, i: int, param: ParameterInfo) -> str:
        """Generate named colon placeholder (:name or :param_N)."""
        if param.style in {
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.POSITIONAL_PYFORMAT,
        }:
            name = f"param_{i}"
        else:
            name = param.name or f"param_{i}"
        return f":{name}"

    def _generate_named_pyformat_placeholder(self, i: int, param: ParameterInfo) -> str:
        """Generate named pyformat placeholder (%(name)s or %(param_N)s)."""
        if param.style in {
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.POSITIONAL_PYFORMAT,
        }:
            name = f"param_{i}"
        else:
            name = param.name or f"param_{i}"
        return f"%({name})s"

    def _generate_named_at_placeholder(self, i: int, param: ParameterInfo) -> str:
        """Generate named at placeholder (@name or @param_N)."""
        if param.style in {
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.POSITIONAL_PYFORMAT,
        }:
            name = f"param_{i}"
        else:
            name = param.name or f"param_{i}"
        return f"@{name}"

    def _generate_named_dollar_placeholder(self, i: int, param: ParameterInfo) -> str:
        """Generate named dollar placeholder ($name or $param_N)."""
        if param.style in {
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.POSITIONAL_PYFORMAT,
        }:
            name = f"param_{i}"
        else:
            name = param.name or f"param_{i}"
        return f"${name}"

    def _build_type_wrapper_dispatch(self) -> dict[type, Any]:
        """Build type dispatch map for O(1) type wrapping decisions."""
        from datetime import date, datetime
        from decimal import Decimal

        return {
            type(None): self._wrap_none_type,
            bool: self._wrap_bool_type,
            int: self._wrap_int_type,
            Decimal: self._wrap_decimal_type,
            date: self._wrap_date_type,
            datetime: self._wrap_datetime_type,
            bytes: self._wrap_binary_type,
            bytearray: self._wrap_binary_type,
            list: self._wrap_array_type,
            tuple: self._wrap_array_type,
            dict: self._wrap_json_type,
        }

    def _wrap_none_type(self, _value: None, semantic_name: Optional[str]) -> TypedParameter:
        """Wrap None type value."""
        import sqlglot.expressions as exp

        return TypedParameter(
            value=None, data_type=exp.DataType.build("NULL"), type_hint="null", semantic_name=semantic_name
        )

    def _wrap_bool_type(self, value: bool, semantic_name: Optional[str]) -> TypedParameter:
        """Wrap boolean type value."""
        import sqlglot.expressions as exp

        return TypedParameter(
            value=value, data_type=exp.DataType.build("BOOLEAN"), type_hint="boolean", semantic_name=semantic_name
        )

    def _wrap_int_type(self, value: int, semantic_name: Optional[str]) -> Any:
        """Wrap integer type value, only if it's a bigint."""
        if abs(value) > MAX_32BIT_INT:
            import sqlglot.expressions as exp

            return TypedParameter(
                value=value, data_type=exp.DataType.build("BIGINT"), type_hint="bigint", semantic_name=semantic_name
            )
        return value  # Small integers don't need wrapping

    def _wrap_decimal_type(self, value: Any, semantic_name: Optional[str]) -> TypedParameter:
        """Wrap Decimal type value."""
        import sqlglot.expressions as exp

        return TypedParameter(
            value=value, data_type=exp.DataType.build("DECIMAL"), type_hint="decimal", semantic_name=semantic_name
        )

    def _wrap_date_type(self, value: Any, semantic_name: Optional[str]) -> Any:
        """Wrap date type value, but not datetime."""
        from datetime import datetime

        if not isinstance(value, datetime):  # Only wrap pure date, not datetime
            import sqlglot.expressions as exp

            return TypedParameter(
                value=value, data_type=exp.DataType.build("DATE"), type_hint="date", semantic_name=semantic_name
            )
        return self._wrap_datetime_type(value, semantic_name)

    def _wrap_datetime_type(self, value: Any, semantic_name: Optional[str]) -> TypedParameter:
        """Wrap datetime type value."""
        import sqlglot.expressions as exp

        return TypedParameter(
            value=value, data_type=exp.DataType.build("TIMESTAMP"), type_hint="timestamp", semantic_name=semantic_name
        )

    def _wrap_binary_type(self, value: Any, semantic_name: Optional[str]) -> TypedParameter:
        """Wrap binary type value (bytes/bytearray)."""
        import sqlglot.expressions as exp

        return TypedParameter(
            value=value, data_type=exp.DataType.build("BINARY"), type_hint="binary", semantic_name=semantic_name
        )

    def _wrap_array_type(self, value: Any, semantic_name: Optional[str]) -> Any:
        """Wrap array type value (list/tuple), but not strings."""
        if not isinstance(value, str):
            import sqlglot.expressions as exp

            return TypedParameter(
                value=value, data_type=exp.DataType.build("ARRAY"), type_hint="array", semantic_name=semantic_name
            )
        return value  # Strings don't need wrapping

    def _wrap_json_type(self, value: dict, semantic_name: Optional[str]) -> TypedParameter:
        """Wrap JSON type value (dict)."""
        import sqlglot.expressions as exp

        return TypedParameter(
            value=value, data_type=exp.DataType.build("JSON"), type_hint="json", semantic_name=semantic_name
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterProcessor:
    """Central parameter processing engine optimized for mypyc."""

    __slots__ = ("_cache", "_cache_size")

    DEFAULT_CACHE_SIZE: Final[int] = 1000

    def __init__(self) -> None:
        self._cache: dict[str, tuple[str, Any]] = {}
        self._cache_size = 0

    def process(
        self,
        sql: str,
        parameters: Any,
        config: ParameterStyleConfig,
        validator: ParameterValidator,
        converter: ParameterConverter,
        is_parsed: bool = True,
    ) -> tuple[str, Any]:
        """Process parameters with simplified transformation pipeline."""
        cache_key = f"{sql}:{hash(repr(parameters))}:{config.default_parameter_style}:{is_parsed}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        param_info = validator.extract_parameters(sql)
        needs_transformation = self._needs_transformation(param_info, config)

        if not needs_transformation and not config.type_coercion_map and not config.output_transformer:
            return sql, parameters

        processed_sql, processed_parameters = sql, parameters

        if is_parsed and not config.has_native_list_expansion:
            processed_sql, processed_parameters = self._expand_in_clauses(processed_sql, processed_parameters)

        if config.type_coercion_map:
            processed_parameters = self._apply_type_coercions(processed_parameters, config.type_coercion_map)

        if needs_transformation:
            processed_sql, processed_parameters = self._convert_to_execution_style(
                processed_sql, processed_parameters, param_info, config, converter
            )

        if config.output_transformer:
            processed_sql, processed_parameters = config.output_transformer(processed_sql, processed_parameters)

        if self._cache_size < self.DEFAULT_CACHE_SIZE:
            self._cache[cache_key] = (processed_sql, processed_parameters)
            self._cache_size += 1

        return processed_sql, processed_parameters

    def _needs_transformation(self, param_info: list[ParameterInfo], config: ParameterStyleConfig) -> bool:
        """Determine if parameter transformation is needed."""
        if not param_info:
            return False

        detected_styles = {p.style for p in param_info}
        target_style = config.default_parameter_style

        if target_style not in detected_styles:
            return True
        return len(detected_styles) > 1

    def _convert_to_execution_style(
        self,
        sql: str,
        parameters: Any,
        param_info: list[ParameterInfo],
        config: ParameterStyleConfig,
        converter: ParameterConverter,
    ) -> tuple[str, Any]:
        """Convert SQL and parameters to execution target style."""
        if config.supported_execution_parameter_styles is not None:
            detected_styles = {p.style for p in param_info}
            if detected_styles and detected_styles.intersection(config.supported_execution_parameter_styles):
                return sql, parameters
            execution_style = config.default_execution_parameter_style
        else:
            execution_style = config.default_parameter_style

        if execution_style is None:
            return sql, parameters

        converted_sql = converter.convert_placeholders(sql, execution_style, param_info)
        converted_parameters = self._adjust_parameters_for_style(parameters, param_info, execution_style)

        return converted_sql, converted_parameters

    def _expand_in_clauses(self, sql: str, parameters: Any) -> tuple[str, Any]:
        """Expand list parameters for IN clauses (only for parsed SQL)."""
        if not parameters:
            return sql, parameters

        if not isinstance(parameters, (list, tuple)):
            return sql, parameters

        validator = ParameterValidator()

        param_info = validator.extract_parameters(sql)
        if not param_info:
            return sql, parameters

        result_parts = []
        expanded_parameters = []
        current_pos = 0

        for param_idx, p_info in enumerate(param_info):
            result_parts.append(sql[current_pos : p_info.position])

            if param_idx < len(parameters) and isinstance(parameters[param_idx], (list, tuple)):
                before_param = sql[: p_info.position].rstrip()
                if before_param.upper().endswith(" IN"):
                    list_param = parameters[param_idx]
                    if list_param:
                        placeholders = ", ".join(["?"] * len(list_param))
                        result_parts.append(f"({placeholders})")
                        expanded_parameters.extend(list_param)
                    else:
                        result_parts.append("(NULL)")
                else:
                    result_parts.append(p_info.placeholder_text)
                    expanded_parameters.append(parameters[param_idx])
            else:
                result_parts.append(p_info.placeholder_text)
                if param_idx < len(parameters):
                    expanded_parameters.append(parameters[param_idx])

            current_pos = p_info.position + len(p_info.placeholder_text)
        result_parts.append(sql[current_pos:])

        return "".join(result_parts), expanded_parameters

    def _apply_type_coercions(self, parameters: Any, coercion_map: dict[type, Any]) -> Any:
        """Apply driver-specific type coercions."""
        if not parameters or not coercion_map:
            return parameters

        def coerce_value(value: Any) -> Any:
            """Coerce a single value based on type."""
            if isinstance(value, TypedParameter):
                inner_value = value.value
                for type_check, converter in coercion_map.items():
                    if isinstance(inner_value, type_check):
                        return converter(inner_value)
                return inner_value
            for type_check, converter in coercion_map.items():
                if isinstance(value, type_check):
                    return converter(value)
            return value

        if isinstance(parameters, dict):
            return {k: coerce_value(v) for k, v in parameters.items()}
        if isinstance(parameters, (list, tuple)):
            return [coerce_value(v) for v in parameters]
        return coerce_value(parameters)

    def _adjust_parameters_for_style(
        self, parameters: Any, param_info: list[ParameterInfo], target_style: ParameterStyle
    ) -> Any:
        """Adjust parameter format to match the target style."""
        if not param_info:
            return parameters

        expects_dict = target_style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
        }
        parameters_are_dict = isinstance(parameters, (dict, Mapping))
        parameters_are_sequence = is_iterable_parameters(parameters)

        if len(param_info) == 1 and not parameters_are_dict and not parameters_are_sequence:
            if expects_dict:
                p_info = param_info[0]
                if p_info.name:
                    return {p_info.name: parameters}
                return {f"param_{p_info.ordinal}": parameters}
            return [parameters]

        if expects_dict and parameters_are_dict:
            return parameters
        if not expects_dict and parameters_are_sequence:
            return parameters

        if not expects_dict and parameters_are_dict:
            parameters_values = list(parameters.values())
            return [
                parameters[p_info.name]
                if p_info.name and p_info.name in parameters
                else parameters[f"param_{p_info.ordinal}"]
                if f"param_{p_info.ordinal}" in parameters
                else parameters_values[p_info.ordinal]
                if p_info.ordinal < len(parameters_values)
                else None
                for p_info in param_info
            ]

        if expects_dict and parameters_are_sequence:
            return {p_info.name or f"param_{i}": value for i, (p_info, value) in enumerate(zip(param_info, parameters))}

        return parameters
