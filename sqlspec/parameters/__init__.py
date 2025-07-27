"""Parameter processing infrastructure for SQLSpec.

This module provides centralized parameter handling to eliminate duplication
across database drivers while maintaining high performance through MyPyC optimization.
"""

from sqlspec.parameters.config import DriverParameterConfig
from sqlspec.parameters.converter import ParameterConverter
from sqlspec.parameters.core import ParameterProcessor
from sqlspec.parameters.types import (
    MAX_32BIT_INT,
    SQLGLOT_INCOMPATIBLE_STYLES,
    ConvertedParameters,
    ParameterInfo,
    ParameterStyle,
    ParameterStyleConversionState,
    ParameterStyleInfo,
    TypedParameter,
)
from sqlspec.parameters.utils import process_execute_many_parameters
from sqlspec.parameters.validator import ParameterValidator

# Legacy import for backward compatibility
SQLParameterType = TypedParameter

__all__ = (
    "MAX_32BIT_INT",
    "SQLGLOT_INCOMPATIBLE_STYLES",
    "ConvertedParameters",
    "DriverParameterConfig",
    "ParameterConverter",
    "ParameterInfo",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConversionState",
    "ParameterStyleInfo",
    "ParameterValidator",
    "TypedParameter",
    "process_execute_many_parameters",
    # Legacy
    "SQLParameterType",
)
