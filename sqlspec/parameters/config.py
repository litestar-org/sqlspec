"""Parameter configuration for database drivers."""

from typing import Any, Callable, Optional

from sqlspec.parameters.types import ParameterStyle

__all__ = ("ParameterStyleConfig",)


class ParameterStyleConfig:
    """Declarative configuration for a driver's parameter handling."""

    __slots__ = (
        "default_parameter_style",
        "execution_target_style",
        "has_native_list_expansion",
        "output_transformer",
        "supported_parameter_styles",
        "type_coercion_map",
    )

    def __init__(
        self,
        default_parameter_style: ParameterStyle,
        supported_parameter_styles: Optional[set[ParameterStyle]] = None,
        execution_target_style: Optional[ParameterStyle] = None,
        type_coercion_map: Optional[dict[type, Callable[[Any], Any]]] = None,
        has_native_list_expansion: bool = False,
        output_transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None,
    ) -> None:
        """Initialize driver parameter configuration."""

        if supported_parameter_styles is None:
            supported_parameter_styles = {default_parameter_style}

        self.supported_parameter_styles = supported_parameter_styles
        self.default_parameter_style = default_parameter_style
        self.execution_target_style = execution_target_style or default_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.output_transformer = output_transformer
