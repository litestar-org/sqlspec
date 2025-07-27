"""Core parameter types used throughout SQLSpec.

This module contains the fundamental types that were previously
scattered in statement/parameters.py.
"""

from enum import Enum
from typing import TYPE_CHECKING, Any, Final, Optional, Union

from typing_extensions import TypedDict

if TYPE_CHECKING:
    from sqlglot import exp

__all__ = (
    "ConvertedParameters",
    "ParameterInfo",
    "ParameterStyle",
    "ParameterStyleConversionState",
    "ParameterStyleInfo",
    "TypedParameter",
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


class ParameterInfo:
    """Immutable parameter information with optimal memory usage."""

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
        """Equality comparison (excludes ordinal and placeholder_text like dataclass compare=False)."""
        if not isinstance(other, type(self)):
            return False
        return self.name == other.name and self.style == other.style and self.position == other.position

    def __repr__(self) -> str:
        """String representation compatible with dataclass.__repr__."""
        # mypyc removes __slots__ at runtime, so we hardcode the fields
        field_strs = [
            f"name={self.name!r}",
            f"ordinal={self.ordinal!r}",
            f"placeholder_text={self.placeholder_text!r}",
            f"position={self.position!r}",
            f"style={self.style!r}",
        ]
        return f"{type(self).__name__}({', '.join(field_strs)})"

    def __hash__(self) -> int:
        """Make ParameterInfo hashable for use in cache keys.

        We hash based on the name, style, and position, which are the key attributes
        that affect SQL compilation and parameter handling.
        """
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
        field_strs = [
            f"semantic_name={self.semantic_name!r}",
            f"data_type={self.data_type!r}",
            f"type_hint={self.type_hint!r}",
            f"value={self.value!r}",
        ]
        return f"{type(self).__name__}({', '.join(field_strs)})"


# Constants
MAX_32BIT_INT: Final[int] = 2147483647

# Define SQLGlot incompatible styles after ParameterStyle enum
SQLGLOT_INCOMPATIBLE_STYLES: Final = {
    ParameterStyle.POSITIONAL_PYFORMAT,
    ParameterStyle.NAMED_PYFORMAT,
    ParameterStyle.POSITIONAL_COLON,
}


class ParameterStyleInfo(TypedDict, total=False):
    """Information about SQL parameter style transformation."""

    was_converted: bool
    placeholder_map: dict[str, Union[str, int]]
    original_styles: list[ParameterStyle]


class ParameterStyleConversionState:
    """Encapsulates all information about parameter style transformation.

    This class provides a single source of truth for parameter style conversions,
    making it easier to track and reverse transformations applied for SQLGlot compatibility.
    """

    __slots__ = (
        "original_param_info",
        "original_styles",
        "placeholder_map",
        "reverse_map",
        "transformation_style",
        "was_transformed",
    )

    def __hash__(self) -> int:
        """Hash based on transformation state and style."""
        return hash(
            (
                self.was_transformed,
                self.transformation_style,
                tuple(self.original_styles) if self.original_styles else None,
                tuple(sorted(self.placeholder_map.items())) if self.placeholder_map else None,
            )
        )

    def __init__(
        self,
        was_transformed: bool = False,
        original_styles: Optional[list[ParameterStyle]] = None,
        transformation_style: Optional[ParameterStyle] = None,
        placeholder_map: Optional[dict[str, Union[str, int]]] = None,
        reverse_map: Optional[dict[Union[str, int], str]] = None,
        original_param_info: Optional[list[ParameterInfo]] = None,
    ) -> None:
        self.was_transformed = was_transformed
        self.original_styles = original_styles or []
        self.transformation_style = transformation_style
        self.placeholder_map = placeholder_map or {}
        self.reverse_map = reverse_map or {}
        self.original_param_info = original_param_info or []

        # Build reverse map if not provided
        if self.placeholder_map and not self.reverse_map:
            self.reverse_map = {v: k for k, v in self.placeholder_map.items()}

    def __eq__(self, other: object) -> bool:
        """Equality comparison compatible with dataclass.__eq__."""
        if not isinstance(other, type(self)):
            return False
        return (
            self.was_transformed == other.was_transformed
            and self.original_styles == other.original_styles
            and self.transformation_style == other.transformation_style
            and self.placeholder_map == other.placeholder_map
            and self.reverse_map == other.reverse_map
            and self.original_param_info == other.original_param_info
        )

    def __repr__(self) -> str:
        """String representation compatible with dataclass.__repr__."""
        field_strs = [
            f"original_param_info={self.original_param_info!r}",
            f"original_styles={self.original_styles!r}",
            f"placeholder_map={self.placeholder_map!r}",
            f"reverse_map={self.reverse_map!r}",
            f"transformation_style={self.transformation_style!r}",
            f"was_transformed={self.was_transformed!r}",
        ]
        return f"{type(self).__name__}({', '.join(field_strs)})"


class ConvertedParameters:
    """Result of parameter conversion with clear structure."""

    __slots__ = ("conversion_state", "merged_parameters", "parameter_info", "transformed_sql")

    def __hash__(self) -> int:
        """Hash based on transformed SQL and conversion state."""
        return hash(
            (
                self.transformed_sql,
                self.conversion_state,
                tuple(param.placeholder_text for param in self.parameter_info),
            )
        )

    def __init__(
        self,
        transformed_sql: str,
        parameter_info: list[ParameterInfo],
        merged_parameters: Any,  # SQLParameterType
        conversion_state: ParameterStyleConversionState,
    ) -> None:
        self.transformed_sql = transformed_sql
        self.parameter_info = parameter_info
        self.merged_parameters = merged_parameters
        self.conversion_state = conversion_state

    def __eq__(self, other: object) -> bool:
        """Equality comparison compatible with dataclass.__eq__."""
        if not isinstance(other, type(self)):
            return False
        return (
            self.transformed_sql == other.transformed_sql
            and self.parameter_info == other.parameter_info
            and self.merged_parameters == other.merged_parameters
            and self.conversion_state == other.conversion_state
        )

    def __repr__(self) -> str:
        """String representation compatible with dataclass.__repr__."""
        field_strs = [
            f"conversion_state={self.conversion_state!r}",
            f"merged_parameters={self.merged_parameters!r}",
            f"parameter_info={self.parameter_info!r}",
            f"transformed_sql={self.transformed_sql!r}",
        ]
        return f"{type(self).__name__}({', '.join(field_strs)})"
