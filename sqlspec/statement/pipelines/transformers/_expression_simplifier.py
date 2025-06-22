from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlglot import exp
from sqlglot.optimizer import simplify

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.result_types import TransformationLog, ValidationError

if TYPE_CHECKING:
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

    def process(self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext") -> "Optional[exp.Expression]":
        """Process the expression to apply SQLGlot's simplification optimizations."""
        if not self.enabled or expression is None:
            return expression

        original_sql = expression.sql(dialect=context.dialect)

        # Extract placeholder info before simplification
        placeholders_before = []
        if context.merged_parameters:
            placeholders_before = self._extract_placeholder_info(expression)

        try:
            simplified = simplify.simplify(
                expression.copy(), constant_propagation=self.config.enable_literal_folding, dialect=context.dialect
            )
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

                # If we have parameters and SQL changed, check for parameter reordering
                if context.merged_parameters and placeholders_before:
                    placeholders_after = self._extract_placeholder_info(simplified)

                    # Create parameter position mapping if placeholders were reordered
                    if len(placeholders_after) == len(placeholders_before):
                        parameter_mapping = self._create_parameter_mapping(placeholders_before, placeholders_after)

                        # Store mapping in context metadata for later use
                        if parameter_mapping and any(
                            new_pos != old_pos for new_pos, old_pos in parameter_mapping.items()
                        ):
                            context.metadata["parameter_position_mapping"] = parameter_mapping

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

    @staticmethod
    def _extract_placeholder_info(expression: "exp.Expression") -> list[dict[str, Any]]:
        """Extract information about placeholder positions in an expression.

        Returns:
            List of placeholder info dicts with position, comparison context, etc.
        """
        placeholders = []

        for node in expression.walk():
            if isinstance(node, exp.Placeholder):
                # Get comparison context for the placeholder
                parent = node.parent
                comparison_info = None

                if isinstance(parent, (exp.GTE, exp.GT, exp.LTE, exp.LT, exp.EQ, exp.NEQ)):
                    # Get the column being compared
                    left = parent.this
                    right = parent.expression

                    # Determine which side the placeholder is on
                    if node == right:
                        side = "right"
                        column = left
                    else:
                        side = "left"
                        column = right

                    if isinstance(column, exp.Column):
                        comparison_info = {"column": column.name, "operator": parent.__class__.__name__, "side": side}

                placeholder_info = {"node": node, "parent": parent, "comparison_info": comparison_info}
                placeholders.append(placeholder_info)

        return placeholders

    @staticmethod
    def _create_parameter_mapping(
        placeholders_before: list[dict[str, Any]], placeholders_after: list[dict[str, Any]]
    ) -> dict[int, int]:
        """Create a mapping of parameter positions from transformed SQL back to original positions.

        Args:
            placeholders_before: Placeholder info from original expression
            placeholders_after: Placeholder info from transformed expression

        Returns:
            Dict mapping new positions to original positions
        """
        mapping = {}

        # For each placeholder in the transformed expression
        for new_pos, ph_after in enumerate(placeholders_after):
            after_info = ph_after["comparison_info"]

            if after_info:
                # Find matching placeholder in original based on comparison context
                for old_pos, ph_before in enumerate(placeholders_before):
                    before_info = ph_before["comparison_info"]

                    if before_info and before_info["column"] == after_info["column"]:
                        # Check if operators were swapped (e.g., >= became <=)
                        if (
                            before_info["operator"] == "GTE"
                            and after_info["operator"] == "LTE"
                            and before_info["side"] == after_info["side"]
                        ):
                            # This is the upper bound parameter that was moved
                            # Find the original position of the upper bound (<=)
                            for orig_pos, orig_ph in enumerate(placeholders_before):
                                orig_info = orig_ph["comparison_info"]
                                if (
                                    orig_info
                                    and orig_info["column"] == after_info["column"]
                                    and orig_info["operator"] == "LTE"
                                ):
                                    mapping[new_pos] = orig_pos
                                    break
                        elif (
                            before_info["operator"] == "LTE"
                            and after_info["operator"] == "GTE"
                            and before_info["side"] == after_info["side"]
                        ):
                            # This is the lower bound parameter that was moved
                            # Find the original position of the lower bound (>=)
                            for orig_pos, orig_ph in enumerate(placeholders_before):
                                orig_info = orig_ph["comparison_info"]
                                if (
                                    orig_info
                                    and orig_info["column"] == after_info["column"]
                                    and orig_info["operator"] == "GTE"
                                ):
                                    mapping[new_pos] = orig_pos
                                    break
                        elif before_info["operator"] == after_info["operator"]:
                            # Same operator, direct mapping
                            mapping[new_pos] = old_pos
                            break
            # No comparison context, try to map by position
            elif new_pos < len(placeholders_before):
                mapping[new_pos] = new_pos

        return mapping
