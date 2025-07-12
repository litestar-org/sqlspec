"""Psycopg-specific pipeline steps for SQL processing."""

import sqlglot.expressions as exp

from sqlspec.statement.pipeline import SQLTransformContext
from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = ("psycopg_copy_transform_step",)


def psycopg_copy_transform_step(context: SQLTransformContext) -> SQLTransformContext:
    """Extract parameters from COPY commands for psycopg driver.

    This pipeline step detects COPY FROM STDIN and COPY TO STDOUT commands
    and properly routes the data to bypass parameter validation.

    The step works at the AST level to detect COPY commands and
    marks them in the context metadata for special handling by the driver.
    """
    expression = context.current_expression
    if isinstance(expression, exp.Copy):
        is_copy_from_stdin = False
        is_copy_to_stdout = False

        files = expression.args.get("files", [])
        for file_expr in files:
            file_str = str(file_expr).upper()
            if file_str == "STDIN":
                is_copy_from_stdin = True
            elif file_str == "STDOUT":
                is_copy_to_stdout = True

        # If this is a COPY FROM STDIN or TO STDOUT, handle the data
        if is_copy_from_stdin or is_copy_to_stdout:
            copy_data = None
            if context.parameters:
                param_keys = sorted(context.parameters.keys())
                if param_keys:
                    for key in param_keys:
                        if key.startswith(("_pos_", "pos_param_")):
                            copy_data = context.parameters[key]
                            # Remove the parameter from context so it doesn't go through validation
                            del context.parameters[key]
                            break

                    # If no positional parameter found, try the first parameter
                    if copy_data is None and param_keys:
                        first_key = param_keys[0]
                        copy_data = context.parameters[first_key]
                        del context.parameters[first_key]

            # Store COPY metadata in context for driver to use
            context.metadata["is_copy_command"] = True
            context.metadata["is_copy_from_stdin"] = is_copy_from_stdin
            context.metadata["is_copy_to_stdout"] = is_copy_to_stdout
            if copy_data is not None:
                context.metadata["copy_data"] = copy_data

            logger.debug(
                "Psycopg COPY transformer: Detected COPY command (FROM STDIN: %s, TO STDOUT: %s)",
                is_copy_from_stdin,
                is_copy_to_stdout,
            )

    return context
