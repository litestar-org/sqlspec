"""Consolidated parameter processing utilities for database drivers.

This module provides centralized parameter handling logic to avoid duplication
across sync and async driver implementations.
"""

from typing import TYPE_CHECKING, Any, Optional, Union

from sqlspec.statement.filters import StatementFilter
from sqlspec.utils.type_guards import is_sync_transaction_capable

if TYPE_CHECKING:
    from sqlspec.typing import StatementParameters

__all__ = (
    "convert_parameter_sequence",
    "convert_parameters_to_positional",
    "process_execute_many_parameters",
    "separate_filters_and_parameters",
    "should_use_transaction",
)


def separate_filters_and_parameters(
    parameters: "tuple[Union[StatementParameters, StatementFilter], ...]",
) -> "tuple[list[StatementFilter], list[Any]]":
    """Separate filters from parameters in a mixed parameter tuple.

    Args:
        parameters: Mixed tuple of parameters and filters

    Returns:
        Tuple of (filters, parameters) lists
    """

    filters: list[StatementFilter] = []
    param_values: list[Any] = []

    for param in parameters:
        if isinstance(param, StatementFilter):
            filters.append(param)
        else:
            param_values.append(param)

    return filters, param_values


def process_execute_many_parameters(
    parameters: "tuple[Union[StatementParameters, StatementFilter], ...]",
) -> "tuple[list[StatementFilter], Optional[list[Any]]]":
    """Process parameters for execute_many operations.

    Args:
        parameters: Mixed tuple of parameters and filters

    Returns:
        Tuple of (filters, parameter_sequence)
    """
    filters, param_values = separate_filters_and_parameters(parameters)

    # Use first parameter as the sequence for execute_many
    param_sequence = param_values[0] if param_values else None

    # Normalize the parameter sequence
    param_sequence = convert_parameter_sequence(param_sequence)

    return filters, param_sequence


def convert_parameter_sequence(params: Any) -> Optional[list[Any]]:
    """Normalize a parameter sequence to a list format.

    Args:
        params: Parameter sequence in various formats

    Returns:
        converted list of parameters or None
    """
    if params is None:
        return None

    if isinstance(params, list):
        return params

    if isinstance(params, tuple):
        return list(params)

    # Check if it's iterable (but not string or dict)
    # Use duck typing to check for iterable protocol
    try:
        iter(params)
        if not isinstance(params, (str, dict)):
            return list(params)
    except TypeError:
        pass

    # Single parameter, wrap in list
    return [params]


def convert_parameters_to_positional(params: "dict[str, Any]", parameter_info: "list[Any]") -> list[Any]:
    """Convert named parameters to positional based on SQL order.

    Args:
        params: Dictionary of named parameters
        parameter_info: List of parameter info from SQL parsing

    Returns:
        List of positional parameters
    """
    if not params:
        return []

    # Handle param_0, param_1, etc. pattern
    if all(key.startswith("param_") for key in params):
        return [params[f"param_{i}"] for i in range(len(params))]

    # Convert based on parameter info order
    # Check for name attribute using getattr with default
    result = []
    for info in parameter_info:
        param_name = getattr(info, "name", None)
        if param_name is not None:
            result.append(params.get(param_name, None))
    return result


def should_use_transaction(connection: Any, auto_commit: bool = True) -> bool:
    """Determine if a transaction should be used.

    Args:
        connection: Database connection object
        auto_commit: Whether auto-commit is enabled

    Returns:
        True if transaction capabilities are available and should be used
    """
    return False if auto_commit else is_sync_transaction_capable(connection)
