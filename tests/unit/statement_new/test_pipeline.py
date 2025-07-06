"""Tests for SQL processing pipeline in statement_new."""

import unittest

from sqlglot import parse_one

from sqlspec.exceptions import RiskLevel
from sqlspec.statement_new.config import SQLConfig
from sqlspec.statement_new.pipeline import SQLPipeline
from sqlspec.statement_new.protocols import ProcessorPhase, SQLProcessingContext, SQLProcessor, ValidationError


class MockValidator(SQLProcessor):
    """Mock validator for testing."""

    phase = ProcessorPhase.VALIDATE

    def __init__(self, should_error: bool = False) -> None:
        self.should_error = should_error

    def process(self, context: SQLProcessingContext) -> SQLProcessingContext:
        if self.should_error:
            error = ValidationError(
                message="Mock validation error",
                code="mock-error",
                risk_level=RiskLevel.HIGH,
                processor="MockValidator",
                expression=context.current_expression,
            )
            context.validation_errors.append(error)
        return context


class MockTransformer(SQLProcessor):
    """Mock transformer for testing."""

    phase = ProcessorPhase.TRANSFORM

    def process(self, context: SQLProcessingContext) -> SQLProcessingContext:
        # Add metadata to track that this transformer ran
        context.metadata["MockTransformer"] = {"ran": True}
        return context


class TestSQLPipeline(unittest.TestCase):
    """Test the SQLPipeline class."""

    def test_pipeline_creation_empty(self) -> None:
        """Test creating an empty pipeline."""
        pipeline = SQLPipeline()
        self.assertEqual(len(pipeline.processors), 0)

    def test_pipeline_creation_with_processors(self) -> None:
        """Test creating a pipeline with processors."""
        validator = MockValidator()
        transformer = MockTransformer()
        pipeline = SQLPipeline([transformer, validator])

        # Should be sorted by phase
        self.assertEqual(len(pipeline.processors), 2)
        self.assertIsInstance(pipeline.processors[0], MockValidator)
        self.assertIsInstance(pipeline.processors[1], MockTransformer)

    def test_pipeline_process_no_expression(self) -> None:
        """Test processing with no SQL expression."""
        pipeline = SQLPipeline([MockValidator()])
        context = SQLProcessingContext(initial_sql_string="", dialect=None, config=SQLConfig())

        processed = pipeline.process(context)

        # Should have an error about missing expression
        self.assertTrue(processed.has_errors)
        self.assertEqual(len(processed.validation_errors), 1)
        self.assertIn("No SQL expression", processed.validation_errors[0].message)

    def test_pipeline_process_with_expression(self) -> None:
        """Test processing with a valid SQL expression."""
        pipeline = SQLPipeline([MockTransformer()])
        context = SQLProcessingContext(initial_sql_string="SELECT * FROM users", dialect=None, config=SQLConfig())
        context.current_expression = parse_one("SELECT * FROM users")

        processed = pipeline.process(context)

        # Should have no errors
        self.assertFalse(processed.has_errors)
        # Transformer should have run
        self.assertTrue(processed.metadata["MockTransformer"]["ran"])

    def test_pipeline_short_circuit_on_validation_error(self) -> None:
        """Test that pipeline short-circuits on validation errors."""
        validator = MockValidator(should_error=True)
        transformer = MockTransformer()
        pipeline = SQLPipeline([validator, transformer])

        context = SQLProcessingContext(initial_sql_string="SELECT * FROM users", dialect=None, config=SQLConfig())
        context.current_expression = parse_one("SELECT * FROM users")

        processed = pipeline.process(context)

        # Should have validation error
        self.assertTrue(processed.has_errors)
        # Transformer should NOT have run
        self.assertNotIn("MockTransformer", processed.metadata)

    def test_pipeline_processor_exception_handling(self) -> None:
        """Test that processor exceptions are handled gracefully."""

        class BrokenProcessor(SQLProcessor):
            phase = ProcessorPhase.TRANSFORM

            def process(self, context: SQLProcessingContext) -> SQLProcessingContext:
                raise RuntimeError("Processor broke!")

        pipeline = SQLPipeline([BrokenProcessor()])
        context = SQLProcessingContext(initial_sql_string="SELECT * FROM users", dialect=None, config=SQLConfig())
        context.current_expression = parse_one("SELECT * FROM users")

        processed = pipeline.process(context)

        # Should have error from exception
        self.assertTrue(processed.has_errors)
        self.assertIn("Processing failed", processed.validation_errors[0].message)
        self.assertIn("Processor broke!", processed.validation_errors[0].message)


class TestSQLProcessingContext(unittest.TestCase):
    """Test the SQLProcessingContext class."""

    def test_context_creation(self) -> None:
        """Test creating a processing context."""
        context = SQLProcessingContext(
            initial_sql_string="SELECT * FROM users",
            dialect="postgres",
            config=SQLConfig(),
            initial_parameters={"id": 1},
        )

        self.assertEqual(context.initial_sql_string, "SELECT * FROM users")
        self.assertEqual(context.dialect, "postgres")
        self.assertIsInstance(context.config, SQLConfig)
        self.assertEqual(context.initial_parameters, {"id": 1})
        self.assertIsNone(context.current_expression)
        self.assertEqual(len(context.validation_errors), 0)
        self.assertFalse(context.has_errors)

    def test_context_has_errors(self) -> None:
        """Test the has_errors property."""
        context = SQLProcessingContext(initial_sql_string="", dialect=None, config=SQLConfig())

        self.assertFalse(context.has_errors)

        # Add an error
        error = ValidationError(
            message="Test error", code="test", risk_level=RiskLevel.LOW, processor="Test", expression=None
        )
        context.validation_errors.append(error)

        self.assertTrue(context.has_errors)

    def test_context_highest_risk_level(self) -> None:
        """Test the highest_risk_level property."""
        context = SQLProcessingContext(initial_sql_string="", dialect=None, config=SQLConfig())

        # Add errors with different risk levels
        context.validation_errors.append(
            ValidationError(message="Low risk", code="low", risk_level=RiskLevel.LOW, processor="Test", expression=None)
        )
        context.validation_errors.append(
            ValidationError(
                message="High risk", code="high", risk_level=RiskLevel.HIGH, processor="Test", expression=None
            )
        )
        context.validation_errors.append(
            ValidationError(
                message="Medium risk", code="medium", risk_level=RiskLevel.MEDIUM, processor="Test", expression=None
            )
        )

        self.assertEqual(context.highest_risk_level, RiskLevel.HIGH)


if __name__ == "__main__":
    unittest.main()
