"""SQL Analysis Pipeline Components.

This module provides analysis components that can extract metadata and insights
from SQL statements as part of the processing pipeline.
"""

from sqlspec.statement.pipelines.analyzers.statement_analyzer import (
    StatementAnalysis,
    StatementAnalyzer,
)

__all__ = (
    "StatementAnalysis",
    "StatementAnalyzer",
)
