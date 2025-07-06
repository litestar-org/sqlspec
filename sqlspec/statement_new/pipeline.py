"""SQL Statement Processing Pipeline."""
from typing import Optional

import sqlglot

from sqlspec.exceptions import RiskLevel
from sqlspec.statement_new.protocols import ProcessorPhase, SQLProcessingContext, SQLProcessor, ValidationError


class SQLPipeline:
    """A simplified SQL processing pipeline that supports execution phases."""

    def __init__(self, processors: Optional[list[SQLProcessor]] = None) -> None:
        self.processors = sorted(processors, key=lambda p: p.phase.value) if processors else []

    def process(self, context: SQLProcessingContext) -> SQLProcessingContext:
        """Run SQL through all processors in phase order."""
        if context.current_expression is None:
            try:
                context.current_expression = sqlglot.parse_one(context.initial_sql_string, read=context.dialect)
            except Exception as e:
                # Check if it's an empty expression error
                error_msg = str(e)
                if "No expression was parsed" in error_msg:
                    error_msg = "No SQL expression"
                else:
                    error_msg = f"SQL Parsing Error: {e}"

                error = ValidationError(
                    message=error_msg,
                    code="parsing-error",
                    risk_level=RiskLevel.CRITICAL,
                    processor="SQLPipeline",
                    expression=None,
                )
                context.validation_errors.append(error)
                return context

        for processor in self.processors:
            # Short-circuit on validation errors before running non-validation processors
            if context.has_errors and processor.phase != ProcessorPhase.VALIDATE:
                break

            try:
                context = processor.process(context)
            except Exception as e:
                # Handle processor exceptions gracefully
                error = ValidationError(
                    message=f"Processing failed: {e}",
                    code="processor-error",
                    risk_level=RiskLevel.CRITICAL,
                    processor=processor.__class__.__name__,
                    expression=context.current_expression,
                )
                context.validation_errors.append(error)
                # Stop processing on processor errors
                break

        return context
