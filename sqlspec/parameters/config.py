"""Parameter configuration for database drivers."""

from typing import Any, Callable, Optional

from sqlspec.parameters.types import ParameterStyle

__all__ = ("ParameterStyleConfig",)


class ParameterStyleConfig:
    """Declarative configuration for a driver's parameter handling."""

    __slots__ = (
        "allow_mixed_parameter_styles",
        "default_execution_parameter_style",
        "default_parameter_style",
        "has_native_list_expansion",
        "needs_static_script_compilation",
        "output_transformer",
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
    ) -> None:
        """Initialize driver parameter configuration.

        Args:
            default_parameter_style: The default parameter style for the driver
            supported_parameter_styles: Set of parameter styles supported by the driver
            supported_execution_parameter_styles: Set of execution parameter styles supported
            default_execution_parameter_style: Default execution parameter style
            type_coercion_map: Mapping of types to their coercion functions
            has_native_list_expansion: Whether the driver supports native list expansion
            output_transformer: Function to transform output parameters
            needs_static_script_compilation: Whether scripts need static compilation
            allow_mixed_parameter_styles: Whether mixed parameter styles are allowed
        """

        self.supported_parameter_styles = supported_parameter_styles or {default_parameter_style}
        self.default_parameter_style = default_parameter_style
        self.supported_execution_parameter_styles = supported_execution_parameter_styles
        self.default_execution_parameter_style = default_execution_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.output_transformer = output_transformer
        self.needs_static_script_compilation = needs_static_script_compilation
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles

    def hash(self) -> int:
        """Generate hash for cache key generation.

        This method creates a deterministic hash of the parameter configuration
        for use in cache keys, ensuring different parameter configurations
        don't share cache entries.
        """
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
