"""Parameter processing infrastructure for SQLSpec.

This module provides centralized parameter handling to eliminate duplication
across database drivers while maintaining high performance through MyPyC optimization.
"""

from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.parameters.converter import ParameterConverter
from sqlspec.parameters.core import ParameterProcessor
from sqlspec.parameters.types import (
    MAX_32BIT_INT,
    SQLGLOT_INCOMPATIBLE_STYLES,
    ConvertedParameters,
    ParameterInfo,
    ParameterStyle,
    ParameterStyleConversionState,
)
from sqlspec.parameters.validator import ParameterValidator

__all__ = (
    "MAX_32BIT_INT",
    "SQLGLOT_INCOMPATIBLE_STYLES",
    "ConvertedParameters",
    "ParameterConverter",
    "ParameterInfo",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "ParameterStyleConversionState",
    "ParameterValidator",
)
