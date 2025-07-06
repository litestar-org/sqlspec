"""Expression simplifier transformer."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, cast

from sqlglot.optimizer import simplify

from sqlspec.exceptions import RiskLevel
from sqlspec.statement_new.protocols import (
    ProcessorPhase,
    SQLProcessingContext,
    SQLProcessor,
    TransformationLog,
    ValidationError,
)

if TYPE_CHECKING:
    from sqlglot import exp

__all__ = ("ExpressionSimplifier", "SimplificationConfig")


@dataclass
class SimplificationConfig:
    """Configuration for expression simplification."""

    enable_literal_folding: bool = True
    enable_boolean_optimization: bool = True
    enable_connector_optimization: bool = True
    enable_equality_normalization: bool = True
    enable_complement_removal: bool = True


class ExpressionSimplifier(SQLProcessor):
    """Advanced expression optimization using SQLGlot's simplification engine."""

    phase = ProcessorPhase.TRANSFORM

    def __init__(self, enabled: bool = True, config: Optional[SimplificationConfig] = None) -> None:
        self.enabled = enabled
        self.config = config or SimplificationConfig()

    def process(self, context: "SQLProcessingContext") -> "SQLProcessingContext":
        if not self.enabled or context.current_expression is None:
            return context

        original_sql = context.current_expression.sql(dialect=context.dialect)

        try:
            simplified = simplify.simplify(
                context.current_expression.copy(),
                constant_propagation=self.config.enable_literal_folding,
                dialect=context.dialect,
            )
        except Exception as e:
            error = ValidationError(
                message=f"Expression simplification failed: {e}",
                code="simplification-failed",
                risk_level=RiskLevel.LOW,
                processor=self.__class__.__name__,
                expression=context.current_expression,
            )
            context.validation_errors.append(error)
            return context
        else:
            simplified_sql = simplified.sql(dialect=context.dialect)
            chars_saved = len(original_sql) - len(simplified_sql)

            if original_sql != simplified_sql:
                log = TransformationLog(
                    description=f"Simplified expression (saved {chars_saved} chars)",
                    processor=self.__class__.__name__,
                    before=original_sql,
                    after=simplified_sql,
                )
                context.transformations.append(log)

            context.metadata[self.__class__.__name__] = {
                "simplified": original_sql != simplified_sql,
                "chars_saved": chars_saved,
            }
            context.current_expression = cast("exp.Expression", simplified)
            return context
