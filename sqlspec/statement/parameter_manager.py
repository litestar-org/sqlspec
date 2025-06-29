"""Parameter management for SQL objects."""

from typing import Any, Optional

from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle

__all__ = ("ParameterManager",)


class ParameterManager:
    """Manages parameter processing and conversion for SQL objects."""

    def __init__(
        self,
        parameters: "Optional[tuple[Any, ...]]" = None,
        kwargs: "Optional[dict[str, Any]]" = None,
        converter: "Optional[ParameterConverter]" = None,
    ) -> None:
        self.converter = converter or ParameterConverter()
        self.named_params: dict[str, Any] = {}
        self.filters: list[StatementFilter] = []
        self._positional_parameters = parameters or ()
        self._named_parameters = kwargs or {}

        # Process initial parameters
        if parameters:
            # Parameters is a tuple of values, process each one
            for i, param in enumerate(parameters):
                self.named_params[f"pos_param_{i}"] = param
        if kwargs:
            self.process_parameters(**kwargs)

    def process_parameters(self, *parameters: Any, **kwargs: Any) -> None:
        """Process positional parameters and kwargs into named parameters."""
        # Process positional parameters, converting them to named parameters
        for i, param in enumerate(parameters):
            if isinstance(param, StatementFilter):
                self.filters.append(param)
                pos_params, named_params = param.extract_parameters()
                # Convert positional params from filter to named params
                for j, p_param in enumerate(pos_params):
                    self.named_params[f"pos_param_{i}_{j}"] = p_param
                self.named_params.update(named_params)
            elif isinstance(param, (list, tuple)):
                for j, p_param in enumerate(param):
                    self.named_params[f"pos_param_{i}_{j}"] = p_param
            elif isinstance(param, dict):
                self.named_params.update(param)
            else:
                self.named_params[f"pos_param_{i}"] = param

        # Handle 'parameters' kwarg if present
        if "parameters" in kwargs:
            param_value = kwargs.pop("parameters")
            if isinstance(param_value, (list, tuple)):
                for i, p_param in enumerate(param_value):
                    self.named_params[f"kw_pos_param_{i}"] = p_param
            elif isinstance(param_value, dict):
                self.named_params.update(param_value)
            else:
                self.named_params["kw_single_param"] = param_value

        # Add remaining kwargs as named parameters
        for key, value in kwargs.items():
            if not key.startswith("_"):
                self.named_params[key] = value

    def get_compiled_parameters(self, param_info: list[Any], target_style: ParameterStyle) -> Any:
        """Compile internal named parameters into the target style."""
        if target_style == ParameterStyle.POSITIONAL_COLON:
            return self._convert_to_positional_colon_format(self.named_params, param_info)
        if target_style in {ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_PYFORMAT}:
            return self._convert_to_positional_format(self.named_params, param_info)
        if target_style == ParameterStyle.NAMED_COLON:
            return self._convert_to_named_colon_format(self.named_params, param_info)
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            return self._convert_to_named_pyformat_format(self.named_params, param_info)
        return self.named_params

    def copy_from(self, other: "ParameterManager") -> None:
        """Copy parameters and filters from another parameter manager."""
        self.named_params.update(other.named_params)
        self.filters.extend(other.filters)

    def add_named_parameter(self, name: str, value: Any) -> None:
        """Add a named parameter."""
        self.named_params[name] = value

    def get_unique_parameter_name(
        self, base_name: str, namespace: Optional[str] = None, preserve_original: bool = False
    ) -> str:
        """Generate a unique parameter name."""
        all_param_names = set(self.named_params.keys())
        candidate = f"{namespace}_{base_name}" if namespace else base_name

        if preserve_original and candidate not in all_param_names:
            return candidate

        if candidate not in all_param_names:
            return candidate

        counter = 1
        while True:
            new_candidate = f"{candidate}_{counter}"
            if new_candidate not in all_param_names:
                return new_candidate
            counter += 1

    def _convert_to_positional_format(self, params: dict[str, Any], param_info: list[Any]) -> list[Any]:
        """Convert to positional format (list).

        This is used for parameter styles like QMARK (?), NUMERIC ($1), and POSITIONAL_PYFORMAT (%s).
        """
        if not param_info:
            return list(params.values())

        result = []
        for i, info in enumerate(param_info):
            # First try the actual parameter name
            if info.name and info.name in params:
                result.append(params[info.name])
            # Then try generated positional names
            elif f"pos_param_{i}" in params:
                result.append(params[f"pos_param_{i}"])
            elif f"kw_pos_param_{i}" in params:
                result.append(params[f"kw_pos_param_{i}"])
            elif f"_arg_{i}" in params:
                result.append(params[f"_arg_{i}"])
            else:
                # If not found, append None (will cause error later in validation)
                result.append(None)
        return result

    def _convert_to_positional_colon_format(self, params: dict[str, Any], param_info: list[Any]) -> list[Any]:
        """Convert to positional colon format (Oracle :1, :2 style).

        Oracle's positional parameters are 1-indexed, but we still return a 0-indexed list.
        The driver will handle the 1-based indexing when building the SQL.
        """
        return self._convert_to_positional_format(params, param_info)

    def _convert_to_named_colon_format(self, params: dict[str, Any], param_info: list[Any]) -> dict[str, Any]:
        """Convert to named colon format (:name style).

        This format expects a dictionary with parameter names as keys.
        We need to ensure all placeholders have corresponding values.
        """
        result = {}

        # For each parameter in the SQL, find its value
        for info in param_info:
            if info.name:
                # Named parameter - look for it in params
                if info.name in params:
                    result[info.name] = params[info.name]
                else:
                    # Check if it's a generated name that needs mapping
                    for key, value in params.items():
                        if key.endswith(f"_{info.ordinal}") or key == f"_arg_{info.ordinal}":
                            result[info.name] = value
                            break
            else:
                # Positional parameter converted to named - use generated name
                gen_name = f"_arg_{info.ordinal}"
                if f"pos_param_{info.ordinal}" in params:
                    result[gen_name] = params[f"pos_param_{info.ordinal}"]
                elif f"kw_pos_param_{info.ordinal}" in params:
                    result[gen_name] = params[f"kw_pos_param_{info.ordinal}"]
                elif gen_name in params:
                    result[gen_name] = params[gen_name]

        # Include any extra named parameters that might be needed
        for key, value in params.items():
            if not key.startswith(("pos_param_", "kw_pos_param_", "_arg_")) and key not in result:
                result[key] = value

        return result

    def _convert_to_named_pyformat_format(self, params: dict[str, Any], param_info: list[Any]) -> dict[str, Any]:
        """Convert to named pyformat format (%(name)s style).

        This is similar to named colon format but uses Python string formatting syntax.
        """
        # Same logic as named colon format - pyformat also expects a dictionary
        return self._convert_to_named_colon_format(params, param_info)

    @property
    def positional_parameters(self) -> tuple[Any, ...]:
        """Get the original positional parameters."""
        return self._positional_parameters

    @property
    def named_parameters(self) -> dict[str, Any]:
        """Get the combined named parameters."""
        return self.named_params

    def get_parameter_info(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Get parameter information in the legacy format.

        This method provides backward compatibility for code expecting
        the old parameter_info format.

        Returns:
            Tuple of (positional_parameters, named_parameters)
        """
        return (self._positional_parameters, self._named_parameters)
