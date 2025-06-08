# TODO: TRY300 - Review try-except patterns for else block opportunities
from dataclasses import dataclass
from typing import Optional

from sqlglot import exp
from sqlglot.optimizer import simplify

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult
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


class ExpressionSimplifier(ProcessorProtocol[exp.Expression]):
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

    def __init__(
        self,
        enabled: bool = True,
        config: Optional[SimplificationConfig] = None,
    ) -> None:
        self.enabled = enabled
        self.config = config or SimplificationConfig()

    def process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]:
        """Process the expression to apply SQLGlot's simplification optimizations."""
        assert context.current_expression is not None, (
            "ExpressionSimplifier expects a valid current_expression in context."
        )

        if not self.enabled:
            return context.current_expression, None

        original_sql = context.current_expression.sql(dialect=context.dialect)

        try:
            # Apply SQLGlot's comprehensive simplification
            # Note: simplify.simplify() applies all optimizations automatically
            simplified = simplify.simplify(context.current_expression.copy())
        except Exception as e:
            # If simplification fails, return original expression
            result = ValidationResult(
                is_safe=True, risk_level=RiskLevel.MEDIUM, warnings=[f"Expression simplification failed: {e}"]
            )
            return context.current_expression, result
        else:
            # Update context with simplified expression
            context.current_expression = simplified

            # Create result with optimization metrics
            simplified_sql = simplified.sql(dialect=context.dialect)

            if original_sql != simplified_sql:
                result = ValidationResult(
                    is_safe=True,
                    risk_level=RiskLevel.LOW,
                    warnings=[f"Expression simplified: {len(original_sql)} → {len(simplified_sql)} chars"],
                )
            else:
                # No changes made
                result = ValidationResult(
                    is_safe=True,
                    risk_level=RiskLevel.LOW,
                    warnings=["Expression already optimized - no simplifications applied"],
                )
            return simplified, result

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
