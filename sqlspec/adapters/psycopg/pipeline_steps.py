"""PostgreSQL COPY operation pipeline steps for psycopg driver."""

import sqlglot.expressions as exp

from sqlspec.statement.pipeline import SQLTransformContext
from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = ("postgres_copy_pipeline_step",)


def postgres_copy_pipeline_step(context: SQLTransformContext) -> SQLTransformContext:
    """Transform PostgreSQL COPY operations for consistent handling across drivers.

    This unified pipeline step handles COPY operations for both psycopg and asyncpg:
    1. Detects COPY expressions comprehensively (including SQLGlot parsing edge cases)
    2. Preserves original SQL text (fixes SQLGlot's COPY INTO transformation)
    3. Extracts parameters in a single pass
    4. Stores metadata consistently for both drivers

    Follows the process-once design principle by handling all transformations
    in a single pass through the pipeline.
    """
    # Check if this is a COPY operation using comprehensive detection
    if not _is_copy_expression(context.current_expression):
        return context

    # Preserve original SQL text to handle SQLGlot transformation issues
    copy_sql = str(context.current_expression)
    if "COPY INTO" in copy_sql.upper():
        # Remove the incorrect "INTO" keyword that SQLGlot adds
        copy_sql = copy_sql.replace("COPY INTO", "COPY").replace("copy into", "copy")

    # Store consistent metadata for both drivers with postgres prefix
    context.metadata["postgres_copy_operation"] = True
    context.metadata["postgres_copy_original_sql"] = copy_sql

    # Process parameters in single pass - move all to metadata
    if context.parameters:
        context.metadata["postgres_copy_data"] = context.parameters
        # Clear parameters to prevent SQL parameter processing
        context.parameters = {}

    logger.debug("PostgreSQL COPY pipeline step: Detected COPY operation, preserved original SQL")

    return context


def _is_copy_expression(expression: exp.Expression) -> bool:
    """Comprehensive check for PostgreSQL COPY operations.

    Handles various ways SQLGlot might parse COPY statements:
    - Standard exp.Copy expressions
    - Command expressions for complex COPY syntax
    - Anonymous expressions for edge cases
    """
    if isinstance(expression, exp.Copy):
        return True

    # Check for COPY statements parsed as Command or Anonymous expressions
    if isinstance(expression, (exp.Command, exp.Anonymous)):
        sql_text = str(expression).strip().upper()
        return sql_text.startswith("COPY ")

    return False
