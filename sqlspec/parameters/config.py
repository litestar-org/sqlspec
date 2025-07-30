"""Parameter configuration for database drivers."""

from typing import Any, Callable, Optional

from sqlspec.parameters.types import ParameterStyle

__all__ = ("ParameterStyleConfig",)


class ParameterStyleConfig:
    """Declarative configuration for a driver's parameter handling."""

    __slots__ = (
        "default_parameter_style",
        "execution_target_style",
        "force_style_conversion",
        "has_native_list_expansion",
        "output_transformer",
        "supported_parameter_styles",
        "type_coercion_map",
    )

    def __init__(
        self,
        default_parameter_style: ParameterStyle,
        type_coercion_map: Optional[dict[type, Callable[[Any], Any]]] = None,
        has_native_list_expansion: bool = False,
        output_transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None,
        supported_parameter_styles: Optional[list[ParameterStyle]] = None,
        force_style_conversion: bool = False,
        execution_target_style: Optional[ParameterStyle] = None,
    ) -> None:
        """Initialize driver parameter configuration.

        Args:
            default_parameter_style: The preferred parameter style for this driver
            type_coercion_map: Type coercion map for driver-specific conversions
            has_native_list_expansion: Does the driver handle list expansion for IN clauses natively?
            output_transformer: Optional additional parameter transformations
            supported_parameter_styles: List of parameter styles the driver natively supports
            force_style_conversion: Force conversion even if style is supported (e.g., psycopg)
            execution_target_style: Final parameter style for database execution (defaults to default_parameter_style)
        """

        if supported_parameter_styles is None:
            supported_parameter_styles = [default_parameter_style]

        # Ensure default is in supported list
        if default_parameter_style not in supported_parameter_styles:
            supported_parameter_styles = [default_parameter_style, *supported_parameter_styles]

        self.supported_parameter_styles = supported_parameter_styles
        self.default_parameter_style = default_parameter_style
        self.execution_target_style = execution_target_style or default_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.force_style_conversion = force_style_conversion
        self.output_transformer = output_transformer
