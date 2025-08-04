"""PostgreSQL COPY operation pipeline steps for asyncpg driver."""

import sqlglot.expressions as exp

from sqlspec.statement.pipeline import SQLTransformContext
from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = ("postgres_copy_pipeline_step",)


def postgres_copy_pipeline_step(context: SQLTransformContext) -> SQLTransformContext:
    """Transform PostgreSQL COPY operations for consistent handling across drivers.

    Detects COPY expressions, preserves original SQL text, extracts parameters,
    and stores metadata for both psycopg and asyncpg drivers.

    Args:
        context: SQL transformation context

    Returns:
        Modified context with COPY operation metadata
    """
    if not _is_copy_expression(context.current_expression):
        return context

    copy_sql = str(context.current_expression)
    if "COPY INTO" in copy_sql.upper():
        copy_sql = copy_sql.replace("COPY INTO", "COPY").replace("copy into", "copy")

    context.metadata["postgres_copy_operation"] = True
    context.metadata["copy_operation"] = True
    context.metadata["original_sql"] = copy_sql

    if context.parameters:
        context.metadata["copy_data"] = context.parameters
        context.parameters = {}

    logger.debug("PostgreSQL COPY pipeline step: Detected COPY operation, preserved original SQL")

    return context


def _is_copy_expression(expression: exp.Expression) -> bool:
    """Check for PostgreSQL COPY operations.

    Handles various ways SQLGlot might parse COPY statements including
    standard Copy expressions, Command expressions, and Anonymous expressions.

    Args:
        expression: SQLGlot expression to check

    Returns:
        True if expression represents a COPY operation
    """
    if isinstance(expression, exp.Copy):
        return True

    if isinstance(expression, (exp.Command, exp.Anonymous)):
        sql_text = str(expression).strip().upper()
        return sql_text.startswith("COPY ")

    return False
