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

    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
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
            # Check for both Placeholder and Parameter nodes (sqlglot parses $1 as Parameter)
            if isinstance(node, (exp.Placeholder, exp.Parameter)):
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

                # Extract the placeholder index from its text
                placeholder_text = str(node)
                placeholder_index = None

                # Handle different formats: "$1", "@1", ":1", etc.
                if placeholder_text.startswith("$") and placeholder_text[1:].isdigit():
                    # PostgreSQL style: $1, $2, etc. (1-based)
                    placeholder_index = int(placeholder_text[1:]) - 1
                elif placeholder_text.startswith("@") and placeholder_text[1:].isdigit():
                    # sqlglot internal representation: @1, @2, etc. (1-based)
                    placeholder_index = int(placeholder_text[1:]) - 1
                elif placeholder_text.startswith(":") and placeholder_text[1:].isdigit():
                    # Oracle style: :1, :2, etc. (1-based)
                    placeholder_index = int(placeholder_text[1:]) - 1

                placeholder_info = {
                    "node": node,
                    "parent": parent,
                    "comparison_info": comparison_info,
                    "index": placeholder_index,
                }
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

        # For simplicity, if we have placeholder indices, use them directly
        # This handles numeric placeholders like $1, $2
        if all(ph.get("index") is not None for ph in placeholders_before + placeholders_after):
            for new_pos, ph_after in enumerate(placeholders_after):
                # The placeholder index tells us which original parameter this refers to
                original_index = ph_after["index"]
                if original_index is not None:
                    mapping[new_pos] = original_index
            return mapping

        # For more complex cases, we need to match based on comparison context
        # Map placeholders based on their comparison context and column
        for new_pos, ph_after in enumerate(placeholders_after):
            after_info = ph_after["comparison_info"]

            if after_info:
                # For flipped comparisons (e.g., "value >= $1" becomes "$1 <= value")
                # we need to match based on the semantic meaning, not just the operator

                # First, try to find exact match based on column and operator meaning
                for old_pos, ph_before in enumerate(placeholders_before):
                    before_info = ph_before["comparison_info"]

                    if before_info and before_info["column"] == after_info["column"]:
                        # Check if this is a flipped comparison
                        # "value >= X" is semantically equivalent to "X <= value"
                        # "value <= X" is semantically equivalent to "X >= value"

                        before_op = before_info["operator"]
                        after_op = after_info["operator"]
                        before_side = before_info["side"]
                        after_side = after_info["side"]

                        # If sides are different, operators might be flipped
                        if before_side != after_side:
                            # Map flipped operators
                            op_flip_map = {
                                ("GTE", "right", "LTE", "left"): True,  # value >= X -> X <= value
                                ("LTE", "right", "GTE", "left"): True,  # value <= X -> X >= value
                                ("GT", "right", "LT", "left"): True,  # value > X -> X < value
                                ("LT", "right", "GT", "left"): True,  # value < X -> X > value
                            }

                            if op_flip_map.get((before_op, before_side, after_op, after_side)):
                                mapping[new_pos] = old_pos
                                break
                        # Same side, same operator - direct match
                        elif before_op == after_op:
                            mapping[new_pos] = old_pos
                            break

            # If no comparison context or no match found, try to map by position
            if new_pos not in mapping and new_pos < len(placeholders_before):
                mapping[new_pos] = new_pos

        return mapping
