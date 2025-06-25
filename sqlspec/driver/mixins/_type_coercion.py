"""Type coercion mixin for database drivers.

This module provides a mixin that all database drivers use to handle
TypedParameter objects and perform appropriate type conversions.
"""

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    from sqlspec.typing import SQLParameterType

__all__ = ("TypeCoercionMixin",)


class TypeCoercionMixin:
    """Mixin providing type coercion for database drivers.

    This mixin is used by all database drivers to handle TypedParameter objects
    and convert values to database-specific types.
    """

    __slots__ = ()

    def _process_parameters(self, parameters: "SQLParameterType") -> "SQLParameterType":
        """Process parameters, extracting values from TypedParameter objects.

        This method is called by drivers before executing SQL to handle
        TypedParameter objects and perform necessary type conversions.

        Args:
            parameters: Raw parameters that may contain TypedParameter objects

        Returns:
            Processed parameters with TypedParameter values extracted and converted
        """
        if parameters is None:
            return None

        if isinstance(parameters, dict):
            return self._process_dict_parameters(parameters)
        if isinstance(parameters, (list, tuple)):
            return self._process_sequence_parameters(parameters)
        # Single scalar parameter
        return self._coerce_parameter_type(parameters)

    def _process_dict_parameters(self, params: dict[str, Any]) -> dict[str, Any]:
        """Process dictionary parameters."""
        result = {}
        for key, value in params.items():
            result[key] = self._coerce_parameter_type(value)
        return result

    def _process_sequence_parameters(self, params: Union[list, tuple]) -> Union[list, tuple]:
        """Process list/tuple parameters."""
        result = [self._coerce_parameter_type(p) for p in params]
        return tuple(result) if isinstance(params, tuple) else result

    def _coerce_parameter_type(self, param: Any) -> Any:
        """Coerce a single parameter to the appropriate database type.

        This method checks if the parameter is a TypedParameter and extracts
        its value, then applies driver-specific type conversions.

        Args:
            param: Parameter value or TypedParameter object

        Returns:
            Coerced parameter value suitable for the database
        """
        # Check if it's a TypedParameter
        if hasattr(param, "__class__") and param.__class__.__name__ == "TypedParameter":
            # Extract value and type hint
            value = param.value
            type_hint = param.type_hint

            # Apply driver-specific coercion based on type hint
            return self._apply_type_coercion(value, type_hint)
        # Regular parameter - apply default coercion
        return self._apply_type_coercion(param, None)

    def _apply_type_coercion(self, value: Any, type_hint: Optional[str]) -> Any:
        """Apply driver-specific type coercion.

        This method should be overridden by each driver to implement
        database-specific type conversions.

        Args:
            value: The value to coerce
            type_hint: Optional type hint from TypedParameter

        Returns:
            Coerced value
        """
        # Default implementation - override in specific drivers
        # This base implementation handles common cases

        if value is None:
            return None

        # Use type hint if available
        if type_hint:
            if type_hint == "boolean":
                return self._coerce_boolean(value)
            if type_hint == "decimal":
                return self._coerce_decimal(value)
            if type_hint == "json":
                return self._coerce_json(value)
            if type_hint.startswith("array"):
                return self._coerce_array(value)

        # Default: return value as-is
        return value

    def _coerce_boolean(self, value: Any) -> Any:
        """Coerce boolean values. Override in drivers without native boolean support."""
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """Coerce decimal values. Override for specific decimal handling."""
        if isinstance(value, str):
            return Decimal(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """Coerce JSON values. Override for databases needing JSON strings."""
        return value

    def _coerce_array(self, value: Any) -> Any:
        """Coerce array values. Override for databases without native array support."""
        return value
