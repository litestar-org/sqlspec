"""Parameter configuration for database drivers."""

from typing import Any, Callable, Optional

from sqlspec.parameters.types import ParameterStyle

__all__ = ("ParameterStyleConfig",)


class ParameterStyleConfig:
    """Declarative configuration for a driver's parameter handling."""

    __slots__ = (
        "allow_mixed_parameter_styles",
        "default_parameter_style",
        "execution_parameter_style",
        "has_native_list_expansion",
        "needs_static_script_compilation",
        "output_transformer",
        "supported_parameter_styles",
        "type_coercion_map",
    )

    def __init__(
        self,
        default_parameter_style: ParameterStyle,
        supported_parameter_styles: Optional[set[ParameterStyle]] = None,
        execution_parameter_style: Optional[ParameterStyle] = None,
        type_coercion_map: Optional[dict[type, Callable[[Any], Any]]] = None,
        has_native_list_expansion: bool = False,
        output_transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None,
        needs_static_script_compilation: bool = True,
        allow_mixed_parameter_styles: bool = False,
    ) -> None:
        """Initialize driver parameter configuration."""

        if supported_parameter_styles is None:
            supported_parameter_styles = {default_parameter_style}

        self.supported_parameter_styles = supported_parameter_styles
        self.default_parameter_style = default_parameter_style
        self.execution_parameter_style = execution_parameter_style or default_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.output_transformer = output_transformer
        self.needs_static_script_compilation = needs_static_script_compilation
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles
