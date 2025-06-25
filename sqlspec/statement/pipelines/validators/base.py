# Base class for validators
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.result_types import ValidationError

if TYPE_CHECKING:
    from sqlglot import exp

    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("BaseValidator",)


class BaseValidator(ProcessorProtocol, ABC):
    """Base class for all validators."""

    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        """Process the SQL expression through this validator.

        Args:
            expression: The SQL expression to validate.
            context: The SQL processing context.

        Returns:
            The expression unchanged (validators don't transform).
        """
        if expression is None:
            return None
        self.validate(expression, context)
        return expression

    @abstractmethod
    def validate(self, expression: "exp.Expression", context: "SQLProcessingContext") -> None:
        """Validate the expression and add any errors to the context.

        Args:
            expression: The SQL expression to validate.
            context: The SQL processing context.
        """
        raise NotImplementedError

    def add_error(
        self,
        context: "SQLProcessingContext",
        message: str,
        code: str,
        risk_level: RiskLevel,
        expression: "exp.Expression | None" = None,
    ) -> None:
        """Helper to add a validation error to the context.

        Args:
            context: The SQL processing context.
            message: The error message.
            code: The error code.
            risk_level: The risk level.
            expression: The specific expression with the error (optional).
        """
        error = ValidationError(
            message=message, code=code, risk_level=risk_level, processor=self.__class__.__name__, expression=expression
        )
        context.validation_errors.append(error)
