# ruff: noqa: RUF100, PLR0912, PLR0915, C901, PLR0911, PLR0914
"""High-performance SQL parameter conversion system.

This module provides bulletproof parameter handling for SQL statements,
supporting all major parameter styles with optimized performance.
"""

import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, Optional, Union

from typing_extensions import TypedDict

from sqlspec.exceptions import ExtraParameterError, MissingParameterError, ParameterStyleMismatchError
from sqlspec.typing import SQLParameterType

if TYPE_CHECKING:
    from sqlglot import exp

# Constants
MAX_32BIT_INT: Final[int] = 2147483647

__all__ = (
    "ConvertedParameters",
    "ParameterConverter",
    "ParameterInfo",
    "ParameterStyle",
    "ParameterStyleTransformationState",
    "ParameterValidator",
    "SQLParameterType",
    "TypedParameter",
)

logger = logging.getLogger("sqlspec.sql.parameters")

# Single comprehensive regex that captures all parameter types in one pass
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
        """String representation for better error messages.

        Returns:
            The enum value as a string.
        """
        return self.value


# Define SQLGlot incompatible styles after ParameterStyle enum
SQLGLOT_INCOMPATIBLE_STYLES: Final = {
    ParameterStyle.POSITIONAL_PYFORMAT,
    ParameterStyle.NAMED_PYFORMAT,
    ParameterStyle.POSITIONAL_COLON,
}


@dataclass
class ParameterInfo:
    """Immutable parameter information with optimal memory usage."""

    name: "Optional[str]"
    """Parameter name for named parameters, None for positional."""

    style: "ParameterStyle"
    """The parameter style."""

    position: int
    """Position in the SQL string (for error reporting)."""

    ordinal: int = field(compare=False)
    """Order of appearance in SQL (0-based)."""

    placeholder_text: str = field(compare=False)
    """The original text of the parameter."""


@dataclass
class TypedParameter:
    """Internal container for parameter values with type metadata.

    This class preserves complete type information from SQL literals and user-provided
    parameters, enabling proper type coercion for each database adapter.

    Note:
        This is an internal class. Users never create TypedParameter objects directly.
        The system automatically wraps parameters with type information.
    """

    value: Any
    """The actual parameter value."""

    sqlglot_type: "exp.DataType"
    """Full SQLGlot DataType instance with all type details."""

    type_hint: str
    """Simple string hint for adapter type coercion (e.g., 'integer', 'decimal', 'json')."""

    semantic_name: "Optional[str]" = None
    """Optional semantic name derived from SQL context (e.g., 'user_id', 'email')."""

    def __hash__(self) -> int:
        """Make TypedParameter hashable for use in cache keys.

        We hash based on the value and type_hint, which are the key attributes
        that affect SQL compilation and parameter handling.
        """
        if isinstance(self.value, (list, dict)):
            value_hash = hash(repr(self.value))
        else:
            try:
                value_hash = hash(self.value)
            except TypeError:
                value_hash = hash(repr(self.value))

        return hash((value_hash, self.type_hint, self.semantic_name))


class ParameterStyleInfo(TypedDict, total=False):
    """Information about SQL parameter style transformation."""

    was_converted: bool
    placeholder_map: dict[str, Union[str, int]]
    original_styles: list[ParameterStyle]


@dataclass
class ParameterStyleTransformationState:
    """Encapsulates all information about parameter style transformation.

    This class provides a single source of truth for parameter style conversions,
    making it easier to track and reverse transformations applied for SQLGlot compatibility.
    """

    was_transformed: bool = False
    """Whether parameter transformation was applied."""

    original_styles: list[ParameterStyle] = field(default_factory=list)
    """Original parameter style(s) detected in the SQL."""

    transformation_style: Optional[ParameterStyle] = None
    """Target style used for transformation (if transformed)."""

    placeholder_map: dict[str, Union[str, int]] = field(default_factory=dict)
    """Mapping from transformed names to original names/positions."""

    reverse_map: dict[Union[str, int], str] = field(default_factory=dict)
    """Reverse mapping for quick lookups."""

    original_param_info: list["ParameterInfo"] = field(default_factory=list)
    """Original parameter info before conversion."""

    def __post_init__(self) -> None:
        """Build reverse map if not provided."""
        if self.placeholder_map and not self.reverse_map:
            self.reverse_map = {v: k for k, v in self.placeholder_map.items()}


@dataclass
class ConvertedParameters:
    """Result of parameter conversion with clear structure."""

    transformed_sql: str
    """SQL after any necessary transformations."""

    parameter_info: list["ParameterInfo"]
    """Information about parameters found in the SQL."""

    merged_parameters: "SQLParameterType"
    """Parameters after merging from various sources."""

    conversion_state: ParameterStyleTransformationState
    """Complete conversion state for tracking conversions."""


@dataclass
class ParameterValidator:
    """Parameter validation."""

    def __post_init__(self) -> None:
        """Initialize validator."""
        self._parameter_cache: dict[str, list[ParameterInfo]] = {}

    @staticmethod
    def _create_parameter_info_from_match(match: "re.Match[str]", ordinal: int) -> "Optional[ParameterInfo]":
        if (
            match.group("dquote")
            or match.group("squote")
            or match.group("dollar_quoted_string")
            or match.group("line_comment")
            or match.group("block_comment")
            or match.group("pg_q_operator")
            or match.group("pg_cast")
        ):
            return None

        position = match.start()
        name: Optional[str] = None
        style: ParameterStyle

        if match.group("pyformat_named"):
            name = match.group("pyformat_name")
            style = ParameterStyle.NAMED_PYFORMAT
        elif match.group("pyformat_pos"):
            style = ParameterStyle.POSITIONAL_PYFORMAT
        elif match.group("positional_colon"):
            name = match.group("colon_num")
            style = ParameterStyle.POSITIONAL_COLON
        elif match.group("named_colon"):
            name = match.group("colon_name")
            style = ParameterStyle.NAMED_COLON
        elif match.group("named_at"):
            name = match.group("at_name")
            style = ParameterStyle.NAMED_AT
        elif match.group("named_dollar_param"):
            name_candidate = match.group("dollar_param_name")
            if not name_candidate.isdigit():
                name = name_candidate
                style = ParameterStyle.NAMED_DOLLAR
            else:
                name = name_candidate  # Keep the numeric value as name for NUMERIC style
                style = ParameterStyle.NUMERIC
        elif match.group("qmark"):
            style = ParameterStyle.QMARK
        else:
            logger.warning(
                "Unhandled SQL token pattern found by regex. Matched group: %s. Token: '%s'",
                match.lastgroup,
                match.group(0),
            )
            return None

        return ParameterInfo(name, style, position, ordinal, match.group(0))

    def extract_parameters(self, sql: str) -> "list[ParameterInfo]":
        """Extract all parameters from SQL with single-pass parsing.

        Args:
            sql: SQL string to analyze

        Returns:
            List of ParameterInfo objects in order of appearance
        """
        if sql in self._parameter_cache:
            return self._parameter_cache[sql]

        parameters: list[ParameterInfo] = []
        ordinal = 0
        for match in _PARAMETER_REGEX.finditer(sql):
            param_info = self._create_parameter_info_from_match(match, ordinal)
            if param_info:
                parameters.append(param_info)
                ordinal += 1

        self._parameter_cache[sql] = parameters
        return parameters

    @staticmethod
    def get_parameter_style(parameters_info: "list[ParameterInfo]") -> "ParameterStyle":
        """Determine overall parameter style from parameter list.

        This typically identifies the dominant style for user-facing messages or general classification.
        It differs from `determine_parameter_input_type` which is about expected Python type for params.

        Args:
            parameters_info: List of extracted parameters

        Returns:
            Overall parameter style
        """
        if not parameters_info:
            return ParameterStyle.NONE

        # Note: This logic prioritizes pyformat if present, then named, then positional.
        is_pyformat_named = any(p.style == ParameterStyle.NAMED_PYFORMAT for p in parameters_info)
        is_pyformat_positional = any(p.style == ParameterStyle.POSITIONAL_PYFORMAT for p in parameters_info)

        if is_pyformat_named:
            return ParameterStyle.NAMED_PYFORMAT
        if is_pyformat_positional:  # If only PYFORMAT_POSITIONAL and not PYFORMAT_NAMED
            return ParameterStyle.POSITIONAL_PYFORMAT

        # Simplified logic if not pyformat, checks for any named or any positional
        has_named = any(
            p.style
            in {
                ParameterStyle.NAMED_COLON,
                ParameterStyle.POSITIONAL_COLON,
                ParameterStyle.NAMED_AT,
                ParameterStyle.NAMED_DOLLAR,
            }
            for p in parameters_info
        )
        has_positional = any(p.style in {ParameterStyle.QMARK, ParameterStyle.NUMERIC} for p in parameters_info)

        # If mixed named and positional (non-pyformat), prefer named as dominant.
        # The choice of NAMED_COLON here is somewhat arbitrary if multiple named styles are mixed.
        if has_named:
            # Could refine to return the style of the first named param encountered, or most frequent.
            # For simplicity, returning a general named style like NAMED_COLON is often sufficient.
            # Or, more accurately, find the first named style:
            for p_style in (
                ParameterStyle.NAMED_COLON,
                ParameterStyle.POSITIONAL_COLON,
                ParameterStyle.NAMED_DOLLAR,
                ParameterStyle.NAMED_AT,
            ):
                if any(p.style == p_style for p in parameters_info):
                    return p_style
            return ParameterStyle.NAMED_COLON

        if has_positional:
            # Similarly, could choose QMARK or NUMERIC based on presence.
            if any(p.style == ParameterStyle.NUMERIC for p in parameters_info):
                return ParameterStyle.NUMERIC
            return ParameterStyle.QMARK  # Default positional

        return ParameterStyle.NONE  # Should not be reached if parameters_info is not empty

    @staticmethod
    def determine_parameter_input_type(parameters_info: "list[ParameterInfo]") -> "Optional[type]":
        """Determine if user-provided parameters should be a dict, list/tuple, or None.

        - If any parameter placeholder implies a name (e.g., :name, %(name)s), a dict is expected.
        - If all parameter placeholders are strictly positional (e.g., ?, %s, $1), a list/tuple is expected.
        - If no parameters, None is expected.

        Args:
            parameters_info: List of extracted ParameterInfo objects.

        Returns:
            `dict` if named parameters are expected, `list` if positional, `None` if no parameters.
        """
        if not parameters_info:
            return None

        if all(p.style == ParameterStyle.POSITIONAL_COLON for p in parameters_info):
            return list

        if any(
            p.name is not None and p.style not in {ParameterStyle.POSITIONAL_COLON, ParameterStyle.NUMERIC}
            for p in parameters_info
        ):  # True for NAMED styles and PYFORMAT_NAMED
            return dict
        # All parameters must have p.name is None or be positional styles (POSITIONAL_COLON, NUMERIC)
        if all(
            p.name is None or p.style in {ParameterStyle.POSITIONAL_COLON, ParameterStyle.NUMERIC}
            for p in parameters_info
        ):
            return list
        # This case implies a mix of parameters where some have names and some don't,
        # but not fitting the clear dict/list categories above.
        # Example: SQL like "SELECT :name, ?" - this is problematic and usually not supported directly.
        # Standard DBAPIs typically don't mix named and unnamed placeholders in the same query (outside pyformat).
        logger.warning(
            "Ambiguous parameter structure for determining input type. "
            "Query might contain a mix of named and unnamed styles not typically supported together."
        )
        # Defaulting to dict if any named param is found, as that's the more common requirement for mixed scenarios.
        # However, strict validation should ideally prevent such mixed styles from being valid.
        return dict  # Or raise an error for unsupported mixed styles.

    def validate_parameters(
        self,
        parameters_info: "list[ParameterInfo]",
        provided_params: "SQLParameterType",
        original_sql_for_error: "Optional[str]" = None,
    ) -> None:
        """Validate provided parameters against SQL requirements.

        Args:
            parameters_info: Extracted parameter info
            provided_params: Parameters provided by user
            original_sql_for_error: Original SQL for error context

        Raises:
            ParameterStyleMismatchError: When style doesn't match
        """
        expected_input_type = self.determine_parameter_input_type(parameters_info)

        # Allow creating SQL statements with placeholders but no parameters
        # This enables patterns like SQL("SELECT * FROM users WHERE id = ?").as_many([...])
        # Validation will happen later when parameters are actually provided
        if provided_params is None and parameters_info:
            # Don't raise an error, just return - validation will happen later
            return

        if (
            len(parameters_info) == 1
            and provided_params is not None
            and not isinstance(provided_params, (dict, list, tuple, Mapping))
            and (not isinstance(provided_params, Sequence) or isinstance(provided_params, (str, bytes)))
        ):
            return

        if expected_input_type is dict:
            if not isinstance(provided_params, Mapping):
                msg = (
                    f"SQL expects named parameters (dictionary/mapping), but received {type(provided_params).__name__}"
                )
                raise ParameterStyleMismatchError(msg, original_sql_for_error)
            self._validate_named_parameters(parameters_info, provided_params, original_sql_for_error)
        elif expected_input_type is list:
            if not isinstance(provided_params, Sequence) or isinstance(provided_params, (str, bytes)):
                msg = f"SQL expects positional parameters (list/tuple), but received {type(provided_params).__name__}"
                raise ParameterStyleMismatchError(msg, original_sql_for_error)
            self._validate_positional_parameters(parameters_info, provided_params, original_sql_for_error)
        elif expected_input_type is None and parameters_info:
            logger.error(
                "Parameter validation encountered an unexpected state: placeholders exist, "
                "but expected input type could not be determined. SQL: %s",
                original_sql_for_error,
            )
            msg = "Could not determine expected parameter type for the given SQL."
            raise ParameterStyleMismatchError(msg, original_sql_for_error)

    @staticmethod
    def _has_actual_params(params: SQLParameterType) -> bool:
        """Check if parameters contain actual values.

        Returns:
            True if parameters contain actual values.
        """
        if isinstance(params, (Mapping, Sequence)) and not isinstance(params, (str, bytes)):
            return bool(params)  # True for non-empty dict/list/tuple
        return params is not None  # True for scalar values other than None

    @staticmethod
    def _validate_named_parameters(
        parameters_info: "list[ParameterInfo]", provided_params: "Mapping[str, Any]", original_sql: "Optional[str]"
    ) -> None:
        """Validate named parameters.

        Raises:
            MissingParameterError: When required parameters are missing
            ExtraParameterError: When extra parameters are provided
        """
        required_names = {p.name for p in parameters_info if p.name is not None}
        provided_names = set(provided_params.keys())

        positional_count = sum(1 for p in parameters_info if p.name is None)
        expected_positional_names = {f"arg_{p.ordinal}" for p in parameters_info if p.name is None}
        if positional_count > 0 and required_names:
            all_expected_names = required_names | expected_positional_names

            missing = all_expected_names - provided_names
            if missing:
                msg = f"Missing required parameters: {sorted(missing)}"
                raise MissingParameterError(msg, original_sql)

            extra = provided_names - all_expected_names
            if extra:
                msg = f"Extra parameters provided: {sorted(extra)}"
                raise ExtraParameterError(msg, original_sql)
        else:
            missing = required_names - provided_names
            if missing:
                msg = f"Missing required named parameters: {sorted(missing)}"
                raise MissingParameterError(msg, original_sql)

            extra = provided_names - required_names
            if extra:
                msg = f"Extra parameters provided: {sorted(extra)}"
                raise ExtraParameterError(msg, original_sql)

    @staticmethod
    def _validate_positional_parameters(
        parameters_info: "list[ParameterInfo]", provided_params: "Sequence[Any]", original_sql: "Optional[str]"
    ) -> None:
        """Validate positional parameters.

        Raises:
            MissingParameterError: When required parameters are missing.
            ExtraParameterError: When extra parameters are provided.
        """
        expected_positional_params_count = sum(
            1
            for p in parameters_info
            if p.name is None or p.style in {ParameterStyle.POSITIONAL_COLON, ParameterStyle.NUMERIC}
        )
        actual_count = len(provided_params)

        if actual_count != expected_positional_params_count:
            if actual_count > expected_positional_params_count:
                msg = (
                    f"SQL requires {expected_positional_params_count} positional parameters "
                    f"but {actual_count} were provided."
                )
                raise ExtraParameterError(msg, original_sql)

            msg = (
                f"SQL requires {expected_positional_params_count} positional parameters "
                f"but {actual_count} were provided."
            )
            raise MissingParameterError(msg, original_sql)


@dataclass
class ParameterConverter:
    """Parameter parameter conversion with caching and validation."""

    def __init__(self) -> None:
        """Initialize converter with validator."""
        self.validator = ParameterValidator()

    @staticmethod
    def _transform_sql_for_parsing(
        original_sql: str, parameters_info: "list[ParameterInfo]"
    ) -> tuple[str, dict[str, Union[str, int]]]:
        """Transform SQL to use unique named placeholders for sqlglot parsing.

        Args:
            original_sql: The original SQL string.
            parameters_info: List of ParameterInfo objects for the SQL.
                             Assumed to be sorted by position as extracted.

        Returns:
            A tuple containing:
                - transformed_sql: SQL string with unique named placeholders (e.g., :param_0).
                - placeholder_map: Dictionary mapping new unique names to original names or ordinal index.
        """
        transformed_sql_parts = []
        placeholder_map: dict[str, Union[str, int]] = {}
        current_pos = 0
        for i, p_info in enumerate(parameters_info):
            transformed_sql_parts.append(original_sql[current_pos : p_info.position])

            unique_placeholder_name = f":param_{i}"
            map_key = f"param_{i}"

            if p_info.name:
                placeholder_map[map_key] = p_info.name
            else:
                placeholder_map[map_key] = p_info.ordinal

            transformed_sql_parts.append(unique_placeholder_name)
            current_pos = p_info.position + len(p_info.placeholder_text)

        transformed_sql_parts.append(original_sql[current_pos:])
        return "".join(transformed_sql_parts), placeholder_map

    def convert_placeholders(
        self, sql: str, target_style: "ParameterStyle", parameter_info: "Optional[list[ParameterInfo]]" = None
    ) -> str:
        """Convert SQL placeholders to a target style.

        Args:
            sql: The SQL string with placeholders
            target_style: The target parameter style to convert to
            parameter_info: Optional list of parameter info (will be extracted if not provided)

        Returns:
            SQL string with converted placeholders
        """
        if parameter_info is None:
            parameter_info = self.validator.extract_parameters(sql)

        if not parameter_info:
            return sql

        result_parts = []
        current_pos = 0

        for i, param in enumerate(parameter_info):
            result_parts.append(sql[current_pos : param.position])

            if target_style == ParameterStyle.QMARK:
                placeholder = "?"
            elif target_style == ParameterStyle.NUMERIC:
                placeholder = f"${i + 1}"
            elif target_style == ParameterStyle.POSITIONAL_PYFORMAT:
                placeholder = "%s"
            elif target_style == ParameterStyle.NAMED_COLON:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f":{name}"
            elif target_style == ParameterStyle.NAMED_PYFORMAT:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f"%({name})s"
            elif target_style == ParameterStyle.NAMED_AT:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f"@{name}"
            elif target_style == ParameterStyle.NAMED_DOLLAR:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f"${name}"
            elif target_style == ParameterStyle.POSITIONAL_COLON:
                placeholder = f":{i + 1}"
            else:
                placeholder = param.placeholder_text

            result_parts.append(placeholder)
            current_pos = param.position + len(param.placeholder_text)

        result_parts.append(sql[current_pos:])

        return "".join(result_parts)

    def convert_parameters(
        self,
        sql: str,
        parameters: "SQLParameterType" = None,
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        validate: bool = True,
    ) -> ConvertedParameters:
        """Convert and merge parameters, and transform SQL for parsing.

        Args:
            sql: SQL string to analyze
            parameters: Primary parameters
            args: Positional arguments (for compatibility)
            kwargs: Keyword arguments
            validate: Whether to validate parameters

        Returns:
            ConvertedParameters object with all conversion information
        """
        parameters_info = self.validator.extract_parameters(sql)

        needs_conversion = any(p.style in SQLGLOT_INCOMPATIBLE_STYLES for p in parameters_info)

        has_positional = any(p.name is None for p in parameters_info)
        has_named = any(p.name is not None for p in parameters_info)
        has_mixed_styles = has_positional and has_named

        if has_mixed_styles and args and kwargs and parameters is None:
            merged_params = self._merge_mixed_parameters(parameters_info, args, kwargs)
        else:
            merged_params = self.merge_parameters(parameters, args, kwargs)  # type: ignore[assignment]

        if validate:
            self.validator.validate_parameters(parameters_info, merged_params, sql)
        if needs_conversion:
            transformed_sql, placeholder_map = self._transform_sql_for_parsing(sql, parameters_info)
            conversion_state = ParameterStyleTransformationState(
                was_transformed=True,
                original_styles=list({p.style for p in parameters_info}),
                transformation_style=ParameterStyle.NAMED_COLON,
                placeholder_map=placeholder_map,
                original_param_info=parameters_info,
            )
        else:
            transformed_sql = sql
            conversion_state = ParameterStyleTransformationState(
                was_transformed=False,
                original_styles=list({p.style for p in parameters_info}),
                original_param_info=parameters_info,
            )

        return ConvertedParameters(
            transformed_sql=transformed_sql,
            parameter_info=parameters_info,
            merged_parameters=merged_params,
            conversion_state=conversion_state,
        )

    @staticmethod
    def _merge_mixed_parameters(
        parameters_info: "list[ParameterInfo]", args: "Sequence[Any]", kwargs: "Mapping[str, Any]"
    ) -> dict[str, Any]:
        """Merge args and kwargs for mixed parameter styles.

        Args:
            parameters_info: List of parameter information from SQL
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Dictionary with merged parameters
        """
        merged: dict[str, Any] = {}

        merged.update(kwargs)

        positional_count = 0
        for param_info in parameters_info:
            if param_info.name is None and positional_count < len(args):
                param_name = f"arg_{param_info.ordinal}"
                merged[param_name] = args[positional_count]
                positional_count += 1

        return merged

    @staticmethod
    def merge_parameters(
        parameters: "SQLParameterType", args: "Optional[Sequence[Any]]", kwargs: "Optional[Mapping[str, Any]]"
    ) -> "SQLParameterType":
        """Merge parameters from different sources with proper precedence.

        Precedence order (highest to lowest):
        1. parameters (primary source - always wins)
        2. kwargs (secondary source)
        3. args (only used if parameters is None and no kwargs)

        Returns:
            Merged parameters as a dictionary or list/tuple, or None.
        """
        # If parameters is provided, it takes precedence over everything
        if parameters is not None:
            return parameters

        if kwargs is not None:
            return dict(kwargs)  # Make a copy

        if args is not None:
            return list(args)  # Convert tuple of args to list for consistency and mutability if needed later

        return None

    @staticmethod
    def wrap_parameters_with_types(
        parameters: "SQLParameterType",
        parameters_info: "list[ParameterInfo]",  # noqa: ARG004
    ) -> "SQLParameterType":
        """Wrap user-provided parameters with TypedParameter objects when needed.

        This is called internally by the SQL processing pipeline after parameter
        extraction and merging. It preserves the original parameter structure
        while adding type information where beneficial.

        Args:
            parameters: User-provided parameters (dict, list, or scalar)
            parameters_info: Extracted parameter information from SQL

        Returns:
            Parameters with TypedParameter wrapping where appropriate
        """
        if parameters is None:
            return None

        # Import here to avoid circular imports
        from datetime import date, datetime, time
        from decimal import Decimal

        def infer_type_from_value(value: Any) -> tuple[str, "exp.DataType"]:
            """Infer SQL type hint and SQLGlot DataType from Python value."""
            # Import here to avoid issues
            from sqlglot import exp

            # None/NULL
            if value is None:
                return "null", exp.DataType.build("NULL")

            # Boolean
            if isinstance(value, bool):
                return "boolean", exp.DataType.build("BOOLEAN")

            # Integer types
            if isinstance(value, int) and not isinstance(value, bool):
                if abs(value) > MAX_32BIT_INT:
                    return "bigint", exp.DataType.build("BIGINT")
                return "integer", exp.DataType.build("INT")

            # Float/Decimal
            if isinstance(value, float):
                return "float", exp.DataType.build("FLOAT")
            if isinstance(value, Decimal):
                return "decimal", exp.DataType.build("DECIMAL")

            # Date/Time types
            if isinstance(value, datetime):
                return "timestamp", exp.DataType.build("TIMESTAMP")
            if isinstance(value, date):
                return "date", exp.DataType.build("DATE")
            if isinstance(value, time):
                return "time", exp.DataType.build("TIME")

            # JSON/Dict
            if isinstance(value, dict):
                return "json", exp.DataType.build("JSON")

            # Array/List
            if isinstance(value, (list, tuple)):
                return "array", exp.DataType.build("ARRAY")

            if isinstance(value, str):
                return "string", exp.DataType.build("VARCHAR")

            # Bytes
            if isinstance(value, bytes):
                return "binary", exp.DataType.build("BINARY")

            # Default fallback
            return "string", exp.DataType.build("VARCHAR")

        def wrap_value(value: Any, semantic_name: Optional[str] = None) -> Any:
            """Wrap a single value with TypedParameter if beneficial."""
            # Don't wrap if already a TypedParameter
            if hasattr(value, "__class__") and value.__class__.__name__ == "TypedParameter":
                return value

            # Don't wrap simple scalar types unless they need special handling
            if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                # For simple types, only wrap if we have special type needs
                # (e.g., bigint, decimal precision, etc.)
                if isinstance(value, int) and abs(value) > MAX_32BIT_INT:
                    # Wrap large integers as bigint
                    type_hint, sqlglot_type = infer_type_from_value(value)
                    return TypedParameter(
                        value=value, sqlglot_type=sqlglot_type, type_hint=type_hint, semantic_name=semantic_name
                    )
                # Otherwise, return unwrapped for performance
                return value

            # Wrap complex types and types needing special handling
            if isinstance(value, (datetime, date, time, Decimal, dict, list, tuple, bytes, bool, type(None))):
                type_hint, sqlglot_type = infer_type_from_value(value)
                return TypedParameter(
                    value=value, sqlglot_type=sqlglot_type, type_hint=type_hint, semantic_name=semantic_name
                )

            # Default: return unwrapped
            return value

        # Handle different parameter structures
        if isinstance(parameters, dict):
            # Wrap dict values selectively
            wrapped_dict = {}
            for key, value in parameters.items():
                wrapped_dict[key] = wrap_value(value, semantic_name=key)
            return wrapped_dict

        if isinstance(parameters, (list, tuple)):
            # Wrap list/tuple values selectively
            wrapped_list: list[Any] = []
            for i, value in enumerate(parameters):
                # Try to get semantic name from parameters_info if available
                semantic_name = None
                if parameters_info and i < len(parameters_info) and parameters_info[i].name:
                    semantic_name = parameters_info[i].name
                wrapped_list.append(wrap_value(value, semantic_name=semantic_name))
            return wrapped_list if isinstance(parameters, list) else tuple(wrapped_list)

        # Single scalar parameter
        semantic_name = None
        if parameters_info and parameters_info[0].name:
            semantic_name = parameters_info[0].name
        return wrap_value(parameters, semantic_name=semantic_name)

    def _convert_sql_placeholders(
        self, rendered_sql: str, final_parameter_info: "list[ParameterInfo]", target_style: "ParameterStyle"
    ) -> str:
        """Internal method to convert SQL from canonical format to target style.

        Args:
            rendered_sql: SQL with canonical placeholders (:param_N)
            final_parameter_info: Complete parameter info list
            target_style: Target parameter style

        Returns:
            SQL with target style placeholders
        """
        canonical_params = self.validator.extract_parameters(rendered_sql)

        # When we have more canonical parameters than final_parameter_info,
        # it's likely because the ParameterizeLiterals transformer added extra parameters.
        # We need to denormalize ALL parameters to ensure proper placeholder conversion.
        # The final_parameter_info only contains the original parameters, but we need
        # to handle all placeholders in the SQL (including those added by transformers).
        if len(canonical_params) > len(final_parameter_info):
            # Extend final_parameter_info to match canonical_params
            # Use the canonical param info for the extra parameters
            final_parameter_info = list(final_parameter_info)
            for i in range(len(final_parameter_info), len(canonical_params)):
                # Create a synthetic ParameterInfo for the extra parameter
                canonical = canonical_params[i]
                # Use the ordinal from the canonical parameter
                final_parameter_info.append(canonical)
        elif len(canonical_params) < len(final_parameter_info):
            from sqlspec.exceptions import SQLTransformationError

            msg = (
                f"Parameter count mismatch during deconversion. "
                f"Expected at least {len(final_parameter_info)} parameters, "
                f"found {len(canonical_params)} in SQL"
            )
            raise SQLTransformationError(msg)

        result_sql = rendered_sql

        for i in range(len(canonical_params) - 1, -1, -1):
            canonical = canonical_params[i]
            source_info = final_parameter_info[i]

            start = canonical.position
            end = start + len(canonical.placeholder_text)
            new_placeholder = self._get_placeholder_for_style(target_style, source_info)
            result_sql = result_sql[:start] + new_placeholder + result_sql[end:]

        return result_sql

    @staticmethod
    def _get_placeholder_for_style(target_style: "ParameterStyle", param_info: "ParameterInfo") -> str:
        """Generate placeholder text for a specific parameter style.

        Args:
            target_style: Target parameter style
            param_info: Parameter information

        Returns:
            Placeholder string for the target style
        """
        if target_style == ParameterStyle.QMARK:
            return "?"
        if target_style == ParameterStyle.NUMERIC:
            return f"${param_info.ordinal + 1}"
        if target_style == ParameterStyle.NAMED_COLON:
            return f":{param_info.name}" if param_info.name else f":arg_{param_info.ordinal}"
        if target_style == ParameterStyle.POSITIONAL_COLON:
            if param_info.style == ParameterStyle.POSITIONAL_COLON and param_info.name and param_info.name.isdigit():
                return f":{param_info.name}"
            return f":{param_info.ordinal + 1}"
        if target_style == ParameterStyle.NAMED_AT:
            return f"@{param_info.name}" if param_info.name else f"@arg_{param_info.ordinal}"
        if target_style == ParameterStyle.NAMED_DOLLAR:
            return f"${param_info.name}" if param_info.name else f"$arg_{param_info.ordinal}"
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            return f"%({param_info.name})s" if param_info.name else f"%(arg_{param_info.ordinal})s"
        if target_style == ParameterStyle.POSITIONAL_PYFORMAT:
            return "%s"
        return param_info.placeholder_text
