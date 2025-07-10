"""Psycopg-specific AST transformers for handling COPY commands."""

from typing import Optional

from sqlglot import exp

from sqlspec.protocols import ProcessorProtocol
from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("PsycopgCopyTransformer",)


class PsycopgCopyTransformer(ProcessorProtocol):
    """Transformer to handle PostgreSQL COPY commands for psycopg driver.

    This transformer detects COPY FROM STDIN and COPY TO STDOUT commands
    and properly routes the data to bypass parameter validation.

    The transformer works at the AST level to detect COPY commands and
    marks them in the context for special handling by the driver.
    """

    def __init__(self) -> None:
        self.is_copy_command = False
        self.is_copy_from_stdin = False
        self.is_copy_to_stdout = False
        self.copy_data = None

    def process(self, expression: Optional[exp.Expression], context: SQLProcessingContext) -> Optional[exp.Expression]:
        """Process the SQL expression to detect and handle COPY commands."""
        if not expression:
            return expression

        # Check if this is a COPY command
        if isinstance(expression, exp.Copy):
            self.is_copy_command = True

            # Check for FROM STDIN or TO STDOUT
            files = expression.args.get("files", [])
            for file_expr in files:
                file_str = str(file_expr).upper()
                if file_str == "STDIN":
                    self.is_copy_from_stdin = True
                elif file_str == "STDOUT":
                    self.is_copy_to_stdout = True

            # If this is a COPY FROM STDIN or TO STDOUT, handle the data
            if self.is_copy_from_stdin or self.is_copy_to_stdout:
                # Extract the data from parameters
                params = context.merged_parameters
                if params:
                    # For COPY commands, the data is passed as the first parameter
                    if isinstance(params, (list, tuple)) and len(params) > 0:
                        self.copy_data = params[0]
                        # Clear the parameters so they don't go through validation
                        context.merged_parameters = None
                    elif isinstance(params, dict):
                        # For dict params, we don't expect COPY data
                        pass

                # Store COPY metadata in context for driver to use
                context.metadata["is_copy_command"] = True
                context.metadata["is_copy_from_stdin"] = self.is_copy_from_stdin
                context.metadata["is_copy_to_stdout"] = self.is_copy_to_stdout
                if self.copy_data is not None:
                    context.metadata["copy_data"] = self.copy_data

        return expression
