"""BigQuery execute_many to script conversion pipeline steps."""

import sqlglot

from sqlspec.statement.pipeline import SQLTransformContext
from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = ("bigquery_execute_many_to_script_step",)


def bigquery_execute_many_to_script_step(context: SQLTransformContext) -> SQLTransformContext:
    """Convert execute_many operations to script execution for BigQuery.

    BigQuery doesn't support native executemany operations, so this pipeline step
    converts is_many=True statements into script statements that can be executed
    sequentially. This step:

    1. Detects when a statement is marked for execute_many (is_many=True) via driver_adapter
    2. Converts the parameters from list of parameter sets to individual statements
    3. Marks the result as a script for proper handling
    4. Uses SQLGlot to generate individual parameterized statements

    This respects the single-parse rule by working with the already-parsed expression.
    """
    # Check if this is an execute_many operation by metadata

    # Check if this is an execute_many operation by looking for is_many metadata
    # or checking if we have multiple parameter sets in the metadata
    is_many_operation = context.metadata.get("bigquery_is_many", False) or context.metadata.get(
        "execute_many_operation", False
    )

    if not is_many_operation:
        return context

    # Check if we have multiple parameter sets
    original_parameters = context.metadata.get("execute_many_parameters")
    if not original_parameters:
        return context

    try:
        # Generate individual statements for each parameter set
        script_statements = []

        for _ in original_parameters:
            # Create a copy of the expression for this parameter set
            stmt_expression = context.current_expression.copy()

            # For BigQuery, we need to substitute parameters into the SQL
            # Generate SQL with parameter placeholders
            dialect = context.dialect or "bigquery"
            sql_str = sqlglot.transpile(str(stmt_expression), read=dialect, write=dialect)[0]

            # Store the statement with parameters for later processing
            script_statements.append(sql_str)

        # Create script SQL by joining statements
        script_sql = ";\n".join(script_statements) + ";"

        # Parse the script as a new expression
        script_expression = sqlglot.parse_one(script_sql, dialect=context.dialect or "bigquery")

        # Update context for script execution
        context.current_expression = script_expression
        context.parameters = original_parameters  # Store all parameter sets for script execution

        # Mark as script and store metadata
        context.metadata["bigquery_execute_many_converted"] = True
        context.metadata["bigquery_original_param_sets"] = len(original_parameters)
        context.metadata["bigquery_script_statements"] = len(script_statements)
        context.metadata["is_script"] = True

    except Exception:
        logger.exception("BigQuery execute_many step failed")
        # On failure, let the original execute_many handling proceed
        return context

    return context
