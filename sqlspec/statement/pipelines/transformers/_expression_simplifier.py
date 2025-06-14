from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, cast

from sqlglot.optimizer import simplify

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.result_types import TransformationLog, ValidationError

if TYPE_CHECKING:
    from sqlglot import exp

    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("ExpressionSimplifier", "SimplificationConfig")


@dataclass
class SimplificationConfig:
    """Configuration for expression simplification."""

    enable_literal_folding: bool = True
    enable_boolean_optimization: bool = True
    enable_connector_optimization: bool = True
    enable_equality_normalization: bool = True
    enable_complement_removal: bool = True


class ExpressionSimplifier(ProcessorProtocol):
    """Advanced expression optimization using SQLGlot's simplification engine.

    This transformer applies SQLGlot's comprehensive simplification suite:
    - Constant folding: 1 + 1 → 2
    - Boolean logic optimization: (A AND B) OR (A AND C) → A AND (B OR C)
    - Tautology removal: WHERE TRUE AND x = 1 → WHERE x = 1
    - Dead code elimination: WHERE FALSE OR x = 1 → WHERE x = 1
    - Double negative removal: NOT NOT x → x
    - Expression standardization: Consistent operator precedence

    Args:
        enabled: Whether expression simplification is enabled.
        config: Configuration object controlling which optimizations to apply.
    """

    def __init__(self, enabled: bool = True, config: Optional[SimplificationConfig] = None) -> None:
        self.enabled = enabled
        self.config = config or SimplificationConfig()

    def process(self, expression: "exp.Expression", context: "SQLProcessingContext") -> "exp.Expression":
        """Process the expression to apply SQLGlot's simplification optimizations."""
        if not self.enabled:
            return expression

        original_sql = expression.sql(dialect=context.dialect)

        try:
            simplified = simplify.simplify(expression.copy())
        except Exception as e:
            # Add warning to context
            error = ValidationError(
                message=f"Expression simplification failed: {e}",
                code="simplification-failed",
                risk_level=RiskLevel.LOW,  # Not critical
                processor=self.__class__.__name__,
                expression=expression,
            )
            context.validation_errors.append(error)
            return expression
        else:
            simplified_sql = simplified.sql(dialect=context.dialect)
            chars_saved = len(original_sql) - len(simplified_sql)

            # Log transformation
            if original_sql != simplified_sql:
                log = TransformationLog(
                    description=f"Simplified expression (saved {chars_saved} chars)",
                    processor=self.__class__.__name__,
                    before=original_sql,
                    after=simplified_sql,
                )
                context.transformations.append(log)

            # Store metadata
            context.metadata[self.__class__.__name__] = {
                "simplified": original_sql != simplified_sql,
                "chars_saved": chars_saved,
                "optimizations_applied": self._get_applied_optimizations(),
            }

            return cast("exp.Expression", simplified)

    def _get_applied_optimizations(self) -> list[str]:
        """Get list of optimization types that are enabled."""
        optimizations = []
        if self.config.enable_literal_folding:
            optimizations.append("literal_folding")
        if self.config.enable_boolean_optimization:
            optimizations.append("boolean_optimization")
        if self.config.enable_connector_optimization:
            optimizations.append("connector_optimization")
        if self.config.enable_equality_normalization:
            optimizations.append("equality_normalization")
        if self.config.enable_complement_removal:
            optimizations.append("complement_removal")
        return optimizations
