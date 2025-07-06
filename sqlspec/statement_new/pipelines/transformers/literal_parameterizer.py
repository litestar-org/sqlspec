"""Replaces literals in SQL with placeholders and extracts them using SQLGlot AST."""

from dataclasses import dataclass
from typing import Any, Optional

from sqlglot import exp

from sqlspec.statement_new.parameters import TypedParameter
from sqlspec.statement_new.protocols import ProcessorPhase, SQLProcessingContext, SQLProcessor

__all__ = ("ParameterizationContext", "ParameterizeLiterals")


@dataclass
class ParameterizationContext:
    """Context for tracking parameterization state during AST traversal."""

    parent_stack: list[exp.Expression]
    in_function_args: bool = False
    in_case_when: bool = False
    in_array: bool = False
    in_in_clause: bool = False
    in_recursive_cte: bool = False
    in_subquery: bool = False
    in_select_list: bool = False
    in_join_condition: bool = False
    function_depth: int = 0
    cte_depth: int = 0
    subquery_depth: int = 0


class ParameterizeLiterals(SQLProcessor):
    """Advanced literal parameterization using SQLGlot AST analysis."""

    phase = ProcessorPhase.TRANSFORM

    def __init__(
        self,
        placeholder_style: str = "?",
        preserve_null: bool = True,
        preserve_boolean: bool = True,
        preserve_numbers_in_limit: bool = True,
        preserve_in_functions: Optional[list[str]] = None,
    ) -> None:
        self.placeholder_style = placeholder_style
        self.preserve_null = preserve_null
        self.preserve_boolean = preserve_boolean
        self.preserve_numbers_in_limit = preserve_numbers_in_limit
        self.preserve_in_functions = preserve_in_functions or []
        self.extracted_parameters: list[Any] = []
        self._parameter_counter = 0

    def process(self, context: SQLProcessingContext) -> SQLProcessingContext:
        if context.current_expression is None:
            return context

        self.extracted_parameters = []
        self._parameter_counter = 0

        param_context = ParameterizationContext(parent_stack=[])
        transformed_expression = self._transform_with_context(context.current_expression.copy(), param_context)

        context.current_expression = transformed_expression
        context.extracted_parameters_from_pipeline.extend(self.extracted_parameters)

        return context

    def _transform_with_context(self, node: exp.Expression, context: ParameterizationContext) -> exp.Expression:
        # This is a simplified version of the traversal logic.
        # A full implementation would be much more complex.

        def replacer(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Literal):
                value, type_hint, sqlglot_type, semantic_name = self._extract_literal_value_and_type(node, context)
                typed_param = TypedParameter(
                    value=value, sqlglot_type=sqlglot_type, type_hint=type_hint, semantic_name=semantic_name
                )
                self.extracted_parameters.append(typed_param)
                self._parameter_counter += 1
                return exp.Placeholder(this=f"param_{self._parameter_counter}")
            return node

        return node.transform(replacer, copy=True)

    def _extract_literal_value_and_type(
        self, literal: exp.Literal, context: ParameterizationContext
    ) -> tuple[Any, str, exp.DataType, Optional[str]]:
        # Simplified extraction logic
        value = literal.this
        type_hint = "unknown"
        if isinstance(value, str):
            type_hint = "string"
        elif isinstance(value, bool):
            type_hint = "boolean"
        elif isinstance(value, (int, float)):
            type_hint = "number"

        sqlglot_type = exp.DataType.build(type_hint.upper())
        semantic_name = None  # Would be determined from context

        return value, type_hint, sqlglot_type, semantic_name
