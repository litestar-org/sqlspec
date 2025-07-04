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
        if parameters:
            for i, param in enumerate(parameters):
                self.named_params[f"pos_param_{i}"] = param
        if kwargs:
            self.process_parameters(**kwargs)

    def process_parameters(self, *parameters: Any, **kwargs: Any) -> None:
        """Process positional parameters and kwargs into named parameters."""
        for i, param in enumerate(parameters):
            if isinstance(param, StatementFilter):
                self.filters.append(param)
                pos_params, named_params = param.extract_parameters()
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
        if "parameters" in kwargs:
            param_value = kwargs.pop("parameters")
            if isinstance(param_value, (list, tuple)):
                for i, p_param in enumerate(param_value):
                    self.named_params[f"kw_pos_param_{i}"] = p_param
            elif isinstance(param_value, dict):
                self.named_params.update(param_value)
            else:
                self.named_params["kw_single_param"] = param_value

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
        self._named_parameters[name] = value

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
            if info.name and info.name in params:
                result.append(params[info.name])
            elif f"pos_param_{i}" in params:
                result.append(params[f"pos_param_{i}"])
            elif f"kw_pos_param_{i}" in params:
                result.append(params[f"kw_pos_param_{i}"])
            elif f"arg_{i}" in params:
                result.append(params[f"arg_{i}"])
            else:
                result.append(None)
        return result

    def _convert_to_positional_colon_format(self, params: dict[str, Any], param_info: list[Any]) -> dict[str, Any]:
        """Convert to positional colon format (Oracle :1, :2 style).

        Oracle's positional parameters are 1-indexed and are accessed by string keys.
        Returns a dict with string keys "1", "2", etc.
        """
        digit_keys = {k: v for k, v in params.items() if k.isdigit()}
        if (
            digit_keys
            and param_info
            and all(hasattr(info, "style") and info.style == ParameterStyle.POSITIONAL_COLON for info in param_info)
        ):
            required_nums = {info.name for info in param_info if hasattr(info, "name")}
            if required_nums.issubset(digit_keys.keys()):
                return digit_keys

        # This handles cases like :0, :1, :3 (with gaps) where we should preserve the actual numbers
        if param_info and all(
            hasattr(info, "style")
            and info.style == ParameterStyle.POSITIONAL_COLON
            and hasattr(info, "name")
            and info.name.isdigit()
            for info in param_info
        ):
            result = {}
            positional_values = self._convert_to_positional_format(params, param_info)
            for i, value in enumerate(positional_values):
                if value is not None:
                    numeric_key = str(i)
                    if any(info.name == numeric_key for info in param_info):
                        result[numeric_key] = value
                    else:
                        result[str(i + 1)] = value

            return result

        positional_list = self._convert_to_positional_format(params, param_info)
        return {str(i + 1): value for i, value in enumerate(positional_list)}

    def _convert_to_named_colon_format(self, params: dict[str, Any], param_info: list[Any]) -> dict[str, Any]:
        """Convert to named colon format (:name style).

        This format expects a dictionary with parameter names as keys.
        We need to ensure all placeholders have corresponding values.
        """
        result = {}
        for info in param_info:
            if info.name:
                if info.name in params:
                    result[info.name] = params[info.name]
                else:
                    for key, value in params.items():
                        if key.endswith(f"_{info.ordinal}") or key == f"arg_{info.ordinal}":
                            result[info.name] = value
                            break
            else:
                gen_name = f"arg_{info.ordinal}"
                if f"pos_param_{info.ordinal}" in params:
                    result[gen_name] = params[f"pos_param_{info.ordinal}"]
                elif f"kw_pos_param_{info.ordinal}" in params:
                    result[gen_name] = params[f"kw_pos_param_{info.ordinal}"]
                elif gen_name in params:
                    result[gen_name] = params[gen_name]
        for key, value in params.items():
            if not key.startswith(("pos_param_", "kw_pos_param_", "arg_")) and key not in result:
                result[key] = value

        return result

    def _convert_to_named_pyformat_format(self, params: dict[str, Any], param_info: list[Any]) -> dict[str, Any]:
        """Convert to named pyformat format (%(name)s style).

        This is similar to named colon format but uses Python string formatting syntax.
        """
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
