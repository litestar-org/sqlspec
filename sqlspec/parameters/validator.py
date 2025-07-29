"""Parameter validation and extraction logic.

This module contains the regex-based parameter extraction logic
that was previously in statement/parameters.py.
"""

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Final, Optional

from sqlspec.parameters.types import ParameterInfo, ParameterStyle

if TYPE_CHECKING:
    from sqlspec.typing import StatementParameters

__all__ = ("ParameterValidator",)


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


class ParameterValidator:
    """Validates and extracts SQL parameters with detailed information."""

    def __init__(self) -> None:
        """Initialize validator with caching support."""
        # Use a simple dict cache to ensure same object identity for same SQL
        self._parameter_cache: dict[str, list[ParameterInfo]] = {}

    def extract_parameters(self, sql: str) -> list[ParameterInfo]:
        """Extract parameter information from SQL string.

        Uses regex to find all parameter placeholders in the SQL string,
        including their position, style, and names (if applicable).

        Args:
            sql: SQL string to analyze

        Returns:
            List of ParameterInfo objects, sorted by position
        """
        # Check cache first
        if sql in self._parameter_cache:
            return self._parameter_cache[sql]

        parameters: list[ParameterInfo] = []
        ordinal = 0

        for match in _PARAMETER_REGEX.finditer(sql):
            # Skip literals and comments
            if match.group("dquote") or match.group("squote") or match.group("dollar_quoted_string"):
                continue
            if match.group("line_comment") or match.group("block_comment"):
                continue
            # Skip PostgreSQL-specific operators and casts
            if match.group("pg_q_operator") or match.group("pg_cast"):
                continue

            # Match parameter placeholders
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
                # PostgreSQL positional: $1, $2, etc. or named: $name
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

        # Cache the result before returning
        self._parameter_cache[sql] = parameters
        return parameters

    def has_parameters(self, sql: str) -> bool:
        """Quick check if SQL contains any parameters.

        Args:
            sql: SQL string to check

        Returns:
            True if SQL contains parameters
        """
        return bool(self.extract_parameters(sql))

    def get_parameter_styles(self, sql: str) -> set[ParameterStyle]:
        """Get all parameter styles present in the SQL.

        Args:
            sql: SQL string to analyze

        Returns:
            Set of parameter styles found
        """
        params = self.extract_parameters(sql)
        return {p.style for p in params}

    def count_parameters(self, sql: str) -> int:
        """Count the number of parameters in the SQL.

        Args:
            sql: SQL string to analyze

        Returns:
            Number of parameters found
        """
        return len(self.extract_parameters(sql))

    def get_parameter_style(self, parameters: list[ParameterInfo]) -> ParameterStyle:
        """Determine the dominant parameter style from a list of parameters.

        In case of ties, named styles take precedence over positional styles.

        Args:
            parameters: List of parameter information

        Returns:
            The dominant parameter style, or NONE if no parameters
        """
        if not parameters:
            return ParameterStyle.NONE

        # Count occurrences of each style
        style_counts: dict[ParameterStyle, int] = {}
        for param in parameters:
            style_counts[param.style] = style_counts.get(param.style, 0) + 1

        # Define precedence order (higher number = higher precedence)
        # Named styles take precedence over positional styles in ties
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

        # Return the style with highest count, with precedence as tiebreaker
        return max(style_counts.items(), key=lambda x: (x[1], precedence.get(x[0], 0)))[0]

    def determine_parameter_input_type(self, parameters: list[ParameterInfo]) -> Optional[type]:
        """Determine expected parameter input type based on parameter styles.

        Args:
            parameters: List of parameter information

        Returns:
            Expected type (list or dict), or None if no parameters
        """
        if not parameters:
            return None

        # Get all unique styles
        styles = {p.style for p in parameters}

        # Named parameters expect dict
        if any(
            style
            in {
                ParameterStyle.NAMED_COLON,
                ParameterStyle.NAMED_PYFORMAT,
                ParameterStyle.NAMED_AT,
                ParameterStyle.NAMED_DOLLAR,
            }
            for style in styles
        ):
            return dict

        # Positional parameters expect list
        return list

    def validate_parameters(
        self, param_info: list[ParameterInfo], provided_params: "StatementParameters", sql: str
    ) -> None:
        """Validate that provided parameters match the SQL parameters.

        Args:
            param_info: List of parameter information from SQL
            provided_params: Parameters provided by user
            sql: The SQL string (for error messages)

        Raises:
            ParameterStyleMismatchError: If parameter style doesn't match
            MissingParameterError: If required parameters are missing
            ExtraParameterError: If extra parameters are provided
        """
        from sqlspec.exceptions import ExtraParameterError, MissingParameterError, ParameterStyleMismatchError
        from sqlspec.utils.type_guards import is_iterable_parameters

        if not param_info:
            # No parameters expected
            if provided_params and provided_params not in ([], {}):
                msg = f"SQL has no parameters but {type(provided_params).__name__} was provided"
                raise ExtraParameterError(msg)
            return

        # Determine expected type
        expected_type = self.determine_parameter_input_type(param_info)

        # Check type mismatch
        if expected_type is dict:
            if not isinstance(provided_params, (dict, Mapping)):
                msg = f"SQL expects named parameters (dict) but got {type(provided_params).__name__}"
                raise ParameterStyleMismatchError(msg)
        elif expected_type is list and isinstance(provided_params, (dict, Mapping)):
            msg = f"SQL expects positional parameters (list/tuple) but got {type(provided_params).__name__}"
            raise ParameterStyleMismatchError(msg)

        # Check parameter count and names
        if expected_type is dict and isinstance(provided_params, (dict, Mapping)):
            # Check for missing named parameters
            required_names = {p.name for p in param_info if p.name}
            provided_names = set(provided_params.keys())

            missing = required_names - provided_names
            if missing:
                msg = f"Missing required parameters: {', '.join(sorted(missing))}"
                raise MissingParameterError(msg)

            # Check for extra parameters
            extra = provided_names - required_names
            if extra:
                msg = f"Extra parameters provided: {', '.join(sorted(extra))}"
                raise ExtraParameterError(msg)

        elif is_iterable_parameters(provided_params) and not isinstance(provided_params, (str, bytes)):
            # Check positional parameter count
            param_count = len(param_info)
            provided_count = len(list(provided_params))

            if provided_count < param_count:
                msg = f"SQL expects {param_count} parameters but only {provided_count} were provided"
                raise MissingParameterError(msg)
            if provided_count > param_count:
                msg = f"SQL expects {param_count} parameters but {provided_count} were provided"
                raise ExtraParameterError(msg)

        elif provided_params is not None:
            # Single scalar parameter
            if len(param_info) != 1:
                msg = f"SQL expects {len(param_info)} parameters but a scalar value was provided"
                raise MissingParameterError(msg)
