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
from typing import Any, Final, Optional, Union

from sqlspec.exceptions import (
    ExtraParameterError,
    MissingParameterError,
    ParameterStyleMismatchError,
)
from sqlspec.typing import SQLParameterType

__all__ = (
    "ParameterConverter",
    "ParameterInfo",
    "ParameterStyle",
    "ParameterValidator",
    "convert_parameters",
    "detect_parameter_style",
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

    # Parameter Placeholders (order can matter if syntax overlaps)
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |          # Group 8: %(name)s (pyformat_name is Group 9)
    (?P<pyformat_pos>%s) |                                      # Group 10: %s
    (?P<named_colon>:(?P<colon_name>\w+)) |                     # Group 11: :name (colon_name is Group 12)
    (?P<named_at>@(?P<at_name>\w+)) |                           # Group 13: @name (at_name is Group 14)
    # Group 15: $name or $1 (dollar_param_name is Group 16)
    # Differentiation between $name and $1 is handled in Python code using isdigit()
    (?P<named_dollar_param>\$(?P<dollar_param_name>\w+)) |
    (?P<qmark>\?)                                              # Group 17: ? (now safer due to pg_q_operator rule above)
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
    NAMED_AT = "named_at"
    NAMED_DOLLAR = "named_dollar"
    PYFORMAT_NAMED = "pyformat_named"
    PYFORMAT_POSITIONAL = "pyformat_positional"

    def __str__(self) -> str:
        """String representation for better error messages.

        Returns:
            The enum value as a string.
        """
        return self.value


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
        ):
            return None

        position = match.start()
        name: Optional[str] = None
        style: ParameterStyle

        if match.group("pyformat_named"):
            name = match.group("pyformat_name")
            style = ParameterStyle.PYFORMAT_NAMED
        elif match.group("pyformat_pos"):
            style = ParameterStyle.PYFORMAT_POSITIONAL
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

        Raises:
            AttributeError: If sql is None

        Returns:
            List of ParameterInfo objects in order of appearance
        """
        if sql is None:
            msg = "'NoneType' object has no attribute 'finditer'"
            raise AttributeError(msg)

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

        # Check for dominant styles
        # Note: This logic prioritizes pyformat if present, then named, then positional.
        is_pyformat_named = any(p.style == ParameterStyle.PYFORMAT_NAMED for p in parameters_info)
        is_pyformat_positional = any(p.style == ParameterStyle.PYFORMAT_POSITIONAL for p in parameters_info)

        if is_pyformat_named:
            return ParameterStyle.PYFORMAT_NAMED
        if is_pyformat_positional:  # If only PYFORMAT_POSITIONAL and not PYFORMAT_NAMED
            return ParameterStyle.PYFORMAT_POSITIONAL

        # Simplified logic if not pyformat, checks for any named or any positional
        has_named = any(
            p.style in {ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_AT, ParameterStyle.NAMED_DOLLAR}
            for p in parameters_info
        )
        has_positional = any(p.style in {ParameterStyle.QMARK, ParameterStyle.NUMERIC} for p in parameters_info)

        # If mixed named and positional (non-pyformat), prefer named as dominant.
        # The choice of NAMED_COLON here is somewhat arbitrary if multiple named styles are mixed.
        if has_named:
            # Could refine to return the style of the first named param encountered, or most frequent.
            # For simplicity, returning a general named style like NAMED_COLON is often sufficient.
            # Or, more accurately, find the first named style:
            for p_style in (ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_AT, ParameterStyle.NAMED_DOLLAR):
                if any(p.style == p_style for p in parameters_info):
                    return p_style
            return ParameterStyle.NAMED_COLON  # Fallback, though should be covered by 'any'

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
        if any(p.name is not None for p in parameters_info):  # True for NAMED styles and PYFORMAT_NAMED
            return dict
        # All parameters must have p.name is None (positional styles like QMARK, NUMERIC, PYFORMAT_POSITIONAL)
        if all(p.name is None for p in parameters_info):
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
            and not isinstance(provided_params, (dict, list, tuple, Mapping, Sequence))
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

        # Check for mixed parameter merging pattern: _arg_N for positional parameters
        positional_count = sum(1 for p in parameters_info if p.name is None)
        expected_positional_names = {f"_arg_{p.ordinal}" for p in parameters_info if p.name is None}

        # For mixed parameters, we expect both named and generated positional names
        if positional_count > 0 and required_names:
            # Mixed parameter style - accept both named params and _arg_N params
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
            # Pure named parameters - original logic
            missing = required_names - provided_names
            if missing:
                # Sort for consistent error messages
                msg = f"Missing required named parameters: {sorted(missing)}"
                raise MissingParameterError(msg, original_sql)

            extra = provided_names - required_names
            if extra:
                # Sort for consistent error messages
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
        # Filter for parameters that are truly positional (name is None)
        # This is important if parameters_info could contain mixed (which determine_parameter_input_type tries to handle)
        expected_positional_params_count = sum(1 for p in parameters_info if p.name is None)
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
                - transformed_sql: SQL string with unique named placeholders (e.g., :__param_0).
                - placeholder_map: Dictionary mapping new unique names to original names or ordinal index.
        """
        transformed_sql_parts = []
        placeholder_map: dict[str, Union[str, int]] = {}
        current_pos = 0
        # parameters_info is already sorted by position due to finditer order in extract_parameters.
        # No need for: sorted_params = sorted(parameters_info, key=lambda p: p.position)

        for i, p_info in enumerate(parameters_info):
            transformed_sql_parts.append(original_sql[current_pos : p_info.position])

            unique_placeholder_name = f":__param_{i}"
            map_key = f"__param_{i}"

            if p_info.name:  # For named parameters (e.g., :name, %(name)s, $name)
                placeholder_map[map_key] = p_info.name
            else:  # For positional parameters (e.g., ?, %s, $1)
                placeholder_map[map_key] = p_info.ordinal  # Store 0-based ordinal

            transformed_sql_parts.append(unique_placeholder_name)
            current_pos = p_info.position + len(p_info.placeholder_text)

        transformed_sql_parts.append(original_sql[current_pos:])
        return "".join(transformed_sql_parts), placeholder_map

    def convert_parameters(
        self,
        sql: str,
        parameters: "SQLParameterType" = None,
        args: "Optional[Sequence[Any]]" = None,
        kwargs: "Optional[Mapping[str, Any]]" = None,
        validate: bool = True,
    ) -> tuple[str, "list[ParameterInfo]", "SQLParameterType", "dict[str, Union[str, int]]"]:
        """Convert and merge parameters, and transform SQL for parsing.

        Args:
            sql: SQL string to analyze
            parameters: Primary parameters
            args: Positional arguments (for compatibility)
            kwargs: Keyword arguments
            validate: Whether to validate parameters

        Returns:
            Tuple of (transformed_sql, parameter_info_list, merged_parameters, placeholder_map)
        """
        parameters_info = self.validator.extract_parameters(sql)

        # Check if we have mixed parameter styles and both args and kwargs
        has_positional = any(p.name is None for p in parameters_info)
        has_named = any(p.name is not None for p in parameters_info)
        has_mixed_styles = has_positional and has_named

        if has_mixed_styles and args and kwargs and parameters is None:
            merged_params = self._merge_mixed_parameters(parameters_info, args, kwargs)
        else:
            merged_params = self.merge_parameters(parameters, args, kwargs)  # type: ignore[assignment]

        if validate:
            self.validator.validate_parameters(parameters_info, merged_params, sql)

        transformed_sql, placeholder_map = self._transform_sql_for_parsing(sql, parameters_info)

        return transformed_sql, parameters_info, merged_params, placeholder_map

    @staticmethod
    def _merge_mixed_parameters(
        parameters_info: "list[ParameterInfo]",
        args: "Sequence[Any]",
        kwargs: "Mapping[str, Any]",
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

        # Add named parameters from kwargs
        merged.update(kwargs)

        # Add positional parameters with generated names
        positional_count = 0
        for param_info in parameters_info:
            if param_info.name is None and positional_count < len(args):  # Positional parameter
                # Generate a name for the positional parameter using its ordinal
                param_name = f"_arg_{param_info.ordinal}"
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

        # No kwargs, consider args if parameters is None
        if args is not None:
            return list(args)  # Convert tuple of args to list for consistency and mutability if needed later

        # Return None if nothing provided
        return None


_validator = ParameterValidator()
_converter = ParameterConverter()


def detect_parameter_style(sql: str) -> "ParameterStyle":
    """Detect the parameter style of a SQL string.

    Args:
        sql: SQL string to analyze

    Returns:
        Detected parameter style

    This is a convenience function using the module-level validator.
    """
    parameters = _validator.extract_parameters(sql)
    return _validator.get_parameter_style(parameters)


def convert_parameters(
    sql: str,
    parameters: "SQLParameterType" = None,
    args: "Optional[Sequence[Any]]" = None,
    kwargs: "Optional[Mapping[str, Any]]" = None,
    validate: bool = True,
) -> tuple[str, "list[ParameterInfo]", "SQLParameterType", "dict[str, Union[str, int]]"]:
    """Convert parameters for a SQL statement and transform SQL for parsing.

    This is a convenience function using the module-level converter.

    Args:
        sql: SQL string to analyze
        parameters: Primary parameters
        args: Positional arguments
        kwargs: Keyword arguments
        validate: Whether to validate parameters

    Returns:
        Tuple of (transformed_sql, parameter_info_list, merged_parameters, placeholder_map)

    """
    return _converter.convert_parameters(sql, parameters, args, kwargs, validate)
