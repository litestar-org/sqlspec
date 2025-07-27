"""Parameter configuration for database drivers."""

from typing import Any, Callable, Optional

from sqlspec.parameters.types import ParameterStyle

__all__ = ("DriverParameterConfig",)


class DriverParameterConfig:
    """Declarative configuration for a driver's parameter handling.

    This replaces 50-100 lines of imperative parameter handling code
    per driver with a simple declarative configuration.
    """

    __slots__ = (
        "default_parameter_style",
        "force_style_conversion",
        "has_native_list_expansion",
        "output_transformer",
        # Backward compatibility
        "paramstyle",
        "supported_parameter_styles",
        "type_coercion_map",
    )

    def __init__(
        self,
        paramstyle: Optional[ParameterStyle] = None,  # For backward compatibility
        type_coercion_map: Optional[dict[type, Callable[[Any], Any]]] = None,
        has_native_list_expansion: bool = False,
        output_transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None,
        # New enhanced parameters
        supported_parameter_styles: Optional[list[ParameterStyle]] = None,
        default_parameter_style: Optional[ParameterStyle] = None,
        force_style_conversion: bool = False,
    ) -> None:
        """Initialize driver parameter configuration.

        Args:
            paramstyle: DEPRECATED - Use default_parameter_style instead
            type_coercion_map: Type coercion map for driver-specific conversions
            has_native_list_expansion: Does the driver handle list expansion for IN clauses natively?
            output_transformer: Optional additional parameter transformations
            supported_parameter_styles: List of parameter styles the driver natively supports
            default_parameter_style: The preferred parameter style for this driver
            force_style_conversion: Force conversion even if style is supported (e.g., psycopg)
        """
        # Handle backward compatibility
        if paramstyle is not None and default_parameter_style is None:
            default_parameter_style = paramstyle

        if paramstyle is not None and supported_parameter_styles is None:
            supported_parameter_styles = [paramstyle]

        # Validate we have the required parameters
        if default_parameter_style is None:
            msg = "Either paramstyle or default_parameter_style must be provided"
            raise ValueError(msg)

        if supported_parameter_styles is None:
            supported_parameter_styles = [default_parameter_style]

        # Ensure default is in supported list
        if default_parameter_style not in supported_parameter_styles:
            supported_parameter_styles = [default_parameter_style, *supported_parameter_styles]

        self.supported_parameter_styles = supported_parameter_styles
        self.default_parameter_style = default_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.force_style_conversion = force_style_conversion
        self.output_transformer = output_transformer
