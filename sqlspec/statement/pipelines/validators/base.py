# ruff: noqa: PLR6301
# Base class for validators
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.results import ProcessorResult, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("BaseValidator",)


class BaseValidator(ProcessorProtocol[exp.Expression], ABC):
    """Base class for all validators."""

    def process(self, context: "SQLProcessingContext") -> "ProcessorResult":
        """Process the SQL context through this validator.

        Args:
            context: The SQL processing context

        Returns:
            A ProcessorResult containing the outcome of the validation.
        """
        # Call the abstract process method that returns ProcessorResult
        result = self._process_internal(context)

        # Return in the expected tuple format
        if result.expression is None:
            result.expression = context.current_expression or exp.Placeholder()

        return result

    @abstractmethod
    def _process_internal(self, context: "SQLProcessingContext") -> ProcessorResult:
        """Internal process method to be implemented by subclasses.

        Args:
            context: The SQL processing context

        Returns:
            ProcessorResult with validation findings
        """
        raise NotImplementedError

    def _create_result(
        self,
        expression: "Optional[exp.Expression]",
        is_safe: bool,
        risk_level: RiskLevel,
        issues: "Optional[list[str]]" = None,
        warnings: "Optional[list[str]]" = None,
        metadata: "Optional[dict[str, Any]]" = None,
    ) -> ProcessorResult:
        """Helper to create a ProcessorResult with ValidationResult.

        Args:
            expression: The expression (usually unchanged for validators)
            is_safe: Whether the SQL is safe
            risk_level: The risk level
            issues: List of issues found
            warnings: List of warnings
            metadata: Additional metadata

        Returns:
            ProcessorResult with validation information
        """
        validation_result = ValidationResult(
            is_safe=is_safe,
            risk_level=risk_level,
            issues=issues,
            warnings=warnings,
        )

        return ProcessorResult(
            expression=expression,
            validation_result=validation_result,
            metadata=metadata,
        )
