"""ADBC-specific AST transformers for handling driver limitations."""

from typing import Optional

from sqlglot import exp

from sqlspec.protocols import ProcessorProtocol
from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("AdbcPostgresTransformer",)


class AdbcPostgresTransformer(ProcessorProtocol):
    """Transformer to handle ADBC PostgreSQL driver limitations.

    This transformer addresses specific issues with the ADBC PostgreSQL driver:
    1. Empty parameter lists in executemany() causing "no parameter $1" errors
    2. NULL parameters causing "Can't map Arrow type 'na' to Postgres type" errors

    The transformer works at the AST level to properly handle these edge cases.
    """

    def __init__(self) -> None:
        self.has_placeholders = False
        self.all_params_null = False
        self.is_empty_params = False
        self.has_null_params = False
        self.null_param_indices: list[int] = []

    def process(self, expression: Optional[exp.Expression], context: SQLProcessingContext) -> Optional[exp.Expression]:
        """Process the SQL expression to handle ADBC limitations."""
        if not expression:
            return expression

        # Check if we have an empty parameter list for executemany
        # Look at the merged_parameters in the context
        params = context.merged_parameters

        # For execute_many, check if we have an empty list
        if isinstance(params, list) and len(params) == 0:
            self.is_empty_params = True

        # Check for NULL parameters
        if params:
            if isinstance(params, (list, tuple)):
                # Track which parameters are NULL
                self.null_param_indices = [i for i, p in enumerate(params) if p is None]
                self.has_null_params = len(self.null_param_indices) > 0
                self.all_params_null = len(self.null_param_indices) == len(params)

                # For ADBC PostgreSQL, we need to replace NULL parameters with literals
                # and remove them from the parameter list
                if self.has_null_params:
                    # Create new parameter list without NULLs
                    new_params = [p for p in params if p is not None]
                    context.merged_parameters = new_params

            elif isinstance(params, dict):
                # For dict parameters, track which ones are NULL
                null_keys = [k for k, v in params.items() if v is None]
                self.has_null_params = len(null_keys) > 0
                self.all_params_null = len(null_keys) == len(params)

                if self.has_null_params:
                    # Remove NULL parameters from dict
                    context.merged_parameters = {k: v for k, v in params.items() if v is not None}

        # Transform the AST if needed
        if self.is_empty_params:
            # For empty parameters, we should skip transformation and let the driver handle it
            # The driver already has logic to return empty result for empty params
            return expression

        if self.has_null_params:
            # Transform placeholders to NULL literals where needed
            self._parameter_index = 0  # Track current parameter position
            return expression.transform(self._transform_node)

        return expression

    def _transform_node(self, node: exp.Expression) -> exp.Expression:
        """Transform individual AST nodes."""
        # Handle parameter nodes (e.g., $1, $2, etc. in PostgreSQL)
        if isinstance(node, exp.Parameter):
            # Access the parameter value directly from the AST node
            # The 'this' attribute contains a Literal node, whose 'this' contains the actual value
            if node.this and isinstance(node.this, exp.Literal):
                try:
                    param_index = int(node.this.this) - 1  # Convert to 0-based index
                    # Check if this parameter should be NULL
                    if param_index in self.null_param_indices:
                        return exp.Null()
                    # Renumber the parameter based on how many NULLs came before it
                    nulls_before = sum(1 for idx in self.null_param_indices if idx < param_index)
                    new_index = param_index - nulls_before + 1  # Convert back to 1-based
                    return exp.Parameter(this=exp.Literal.number(new_index))
                except (ValueError, IndexError):
                    pass

        # Handle placeholder nodes for other dialects
        elif isinstance(node, exp.Placeholder):
            # For placeholders, we need to track position
            if self._parameter_index in self.null_param_indices:
                self._parameter_index += 1
                return exp.Null()
            self._parameter_index += 1

        return node
