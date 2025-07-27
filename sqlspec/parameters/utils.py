"""Utility functions for parameter processing."""

from typing import TYPE_CHECKING, Union

from sqlspec.statement.filters import StatementFilter

if TYPE_CHECKING:
    from sqlspec.typing import StatementParameters

__all__ = ("process_execute_many_parameters",)


def process_execute_many_parameters(
    parameters: tuple[Union["StatementParameters", "StatementFilter"], ...],
) -> tuple[list["StatementFilter"], list["StatementParameters"]]:
    """Process execute_many parameters by separating filters from parameters.

    This function splits the input parameters into two lists:
    - StatementFilter objects (e.g., WHERE clauses, ORDER BY, etc.)
    - Regular parameter values (dict, list, tuple, or scalar values)

    Args:
        parameters: Tuple of parameters that can be either filters or regular parameters

    Returns:
        Tuple of (filters, param_sequence) where:
        - filters: List of StatementFilter objects
        - param_sequence: List of regular parameter values
    """

    filters: list[StatementFilter] = []
    param_sequence: list[StatementParameters] = []

    # Special handling for execute_many: if the first parameter is a list of
    # tuples/dicts/lists (typical execute_many pattern), use it as the param_sequence
    if len(parameters) == 1 and isinstance(parameters[0], list):
        # Check if it's a list of parameter sets (typical execute_many usage)
        first_param = parameters[0]
        if first_param and all(isinstance(p, (tuple, list, dict)) for p in first_param):
            # This is the typical execute_many pattern: driver.execute_many(sql, [params1, params2, ...])
            param_sequence = first_param
            return filters, param_sequence

    # Otherwise, process normally
    for param in parameters:
        if isinstance(param, StatementFilter):
            filters.append(param)
        else:
            param_sequence.append(param)

    return filters, param_sequence
