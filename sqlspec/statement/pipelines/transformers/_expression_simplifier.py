from typing import TYPE_CHECKING, Optional, cast

from sqlglot.optimizer import simplify

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines import ProcessorProtocol, TransformationLog, ValidationError

if TYPE_CHECKING:
    from sqlglot import exp

    from sqlspec.statement.pipelines import SQLProcessingContext

__all__ = ("ExpressionSimplifier", "SimplificationConfig")


class SimplificationConfig:
    """Configuration for expression simplification."""

    __slots__ = (
        "enable_boolean_optimization",
        "enable_complement_removal",
        "enable_connector_optimization",
        "enable_equality_conversion",
        "enable_literal_folding",
    )

    def __init__(
        self,
        enable_literal_folding: bool = True,
        enable_boolean_optimization: bool = True,
        enable_connector_optimization: bool = True,
        enable_equality_conversion: bool = True,
        enable_complement_removal: bool = True,
    ) -> None:
        self.enable_literal_folding = enable_literal_folding
        self.enable_boolean_optimization = enable_boolean_optimization
        self.enable_connector_optimization = enable_connector_optimization
        self.enable_equality_conversion = enable_equality_conversion
        self.enable_complement_removal = enable_complement_removal


class ExpressionSimplifier(ProcessorProtocol):
    """Advanced expression optimization using SQLGlot's simplification engine."""

    def __init__(self, enabled: bool = True, config: Optional[SimplificationConfig] = None) -> None:
        self.enabled = enabled
        self.config = config or SimplificationConfig()

    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        if not self.enabled or expression is None:
            return expression

        original_sql = expression.sql(dialect=context.dialect)

        try:
            simplified = simplify.simplify(
                expression.copy(), constant_propagation=self.config.enable_literal_folding, dialect=context.dialect
            )
        except Exception as e:
            error = ValidationError(
                message=f"Expression simplification failed: {e}",
                code="simplification-failed",
                risk_level=RiskLevel.LOW,
                processor=self.__class__.__name__,
                expression=expression,
            )
            context.validation_errors.append(error)
            return expression
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

            optimizations = []
            if self.config.enable_literal_folding:
                optimizations.append("literal_folding")
            if self.config.enable_boolean_optimization:
                optimizations.append("boolean_optimization")
            if self.config.enable_connector_optimization:
                optimizations.append("connector_optimization")
            if self.config.enable_equality_conversion:
                optimizations.append("equality_conversion")
            if self.config.enable_complement_removal:
                optimizations.append("complement_removal")

            context.metadata[self.__class__.__name__] = {
                "simplified": original_sql != simplified_sql,
                "chars_saved": chars_saved,
                "optimizations_applied": optimizations,
            }

            return cast("exp.Expression", simplified)
