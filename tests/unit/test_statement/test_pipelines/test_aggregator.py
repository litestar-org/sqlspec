"""Unit tests for the Result Aggregator."""

import pytest

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.aggregator import AggregatedResults, ResultAggregator
from sqlspec.statement.pipelines.validators.base import ProcessorResult


class TestAggregatedResults:
    """Test the AggregatedResults class."""

    def test_initialization(self) -> None:
        """Test aggregated results initialization."""
        results = AggregatedResults()
        assert results.overall_risk_level == RiskLevel.SKIP
        assert results.is_safe is True
        assert results.all_issues == []
        assert results.all_warnings == []
        assert results.was_transformed is False
        assert results.has_analysis is False

    def test_add_processor_result(self) -> None:
        """Test adding processor results."""
        results = AggregatedResults()
        processor_result = ProcessorResult(metadata={"test": "data", "performance_issues": [{"issue": "slow query"}]})

        results.add_processor_result("test_processor", processor_result)

        assert results.total_processors_run == 1
        assert len(results.performance_issues) == 1
        assert results.component_metadata["test_processor"]["test"] == "data"

    def test_mark_processor_failed(self) -> None:
        """Test marking processor as failed."""
        results = AggregatedResults()

        results.mark_processor_failed("test_processor", "Test error")

        assert "test_processor" in results.failed_processors
        assert "test_processor_error" in results.component_metadata
        assert results.component_metadata["test_processor_error"]["error"] == "Test error"

    def test_get_summary(self) -> None:
        """Test getting summary of results."""
        results = AggregatedResults()
        results.overall_risk_level = RiskLevel.MEDIUM
        results.was_transformed = True
        results.transformations_applied = ["test_transform"]

        summary = results.get_summary()

        assert summary["overall_risk_level"] == "MEDIUM"
        assert summary["was_transformed"] is True
        assert summary["transformations_count"] == 1

    def test_get_recommendations(self) -> None:
        """Test getting recommendations."""
        results = AggregatedResults()
        results.performance_issues = [{"recommendation": "Add index"}]
        results.security_issues = [{"recommendation": "Use parameterized queries"}]

        recommendations = results.get_recommendations()

        assert "Add index" in recommendations
        assert "Use parameterized queries" in recommendations
        assert len(recommendations) == 2

    def test_to_dict(self) -> None:
        """Test converting to dictionary."""
        results = AggregatedResults()
        results.overall_risk_level = RiskLevel.LOW
        results.table_count = 2

        result_dict = results.to_dict()

        assert "summary" in result_dict
        assert "risk_assessment" in result_dict
        assert "analysis" in result_dict
        assert result_dict["analysis"]["table_count"] == 2


class TestResultAggregator:
    """Test the ResultAggregator class."""

    @pytest.fixture
    def aggregator(self) -> ResultAggregator:
        """Create a result aggregator instance."""
        return ResultAggregator()

    def test_initialization(self, aggregator: ResultAggregator) -> None:
        """Test aggregator initialization."""
        assert aggregator.results.overall_risk_level == RiskLevel.SKIP
        assert aggregator.results.total_processors_run == 0

    def test_aggregate_processor_results(self, aggregator: ResultAggregator) -> None:
        """Test aggregating multiple processor results."""
        result1 = ProcessorResult(metadata={"test1": "data1"})
        result2 = ProcessorResult(metadata={"test2": "data2"})

        results = [
            ("processor1", result1),
            ("processor2", result2),
        ]

        aggregated = aggregator.aggregate_processor_results(results)

        assert aggregated.total_processors_run == 2
        assert "processor1" in aggregated.component_metadata
        assert "processor2" in aggregated.component_metadata

    def test_add_timing_info(self, aggregator: ResultAggregator) -> None:
        """Test adding timing information."""
        aggregator.add_timing_info(123.45)

        assert aggregator.results.processing_time_ms == 123.45

    def test_reset(self, aggregator: ResultAggregator) -> None:
        """Test resetting the aggregator."""
        # Add some data
        aggregator.add_timing_info(100.0)
        aggregator.results.total_processors_run = 5

        # Reset
        aggregator.reset()

        # Should be back to initial state
        assert aggregator.results.processing_time_ms == 0.0
        assert aggregator.results.total_processors_run == 0

    def test_merge_with(self, aggregator: ResultAggregator) -> None:
        """Test merging with another aggregated result."""
        # Setup first aggregator
        aggregator.add_timing_info(50.0)
        aggregator.results.total_processors_run = 2
        aggregator.results.all_issues = ["issue1"]

        # Setup second result to merge
        other_results = AggregatedResults()
        other_results.processing_time_ms = 75.0
        other_results.total_processors_run = 3
        other_results.all_issues = ["issue2"]
        other_results.overall_risk_level = RiskLevel.HIGH

        # Merge
        aggregator.merge_with(other_results)

        # Check merged results
        assert aggregator.results.processing_time_ms == 125.0  # 50 + 75
        assert aggregator.results.total_processors_run == 5  # 2 + 3
        assert len(aggregator.results.all_issues) == 2  # Both issues
        assert aggregator.results.overall_risk_level == RiskLevel.HIGH  # Highest level


class TestResultAggregatorIntegration:
    """Test aggregator integration scenarios."""

    def test_complex_aggregation_scenario(self) -> None:
        """Test a complex aggregation scenario with multiple components."""
        aggregator = ResultAggregator()

        # Add validation result
        validation_result = ProcessorResult(
            metadata={
                "validation_type": "security",
                "security_issues": [{"type": "sql_injection", "recommendation": "Use parameters"}],
                "performance_issues": [{"type": "missing_index", "recommendation": "Add index on user_id"}],
            }
        )

        # Add analysis result
        analysis_result = ProcessorResult(
            metadata={"analysis_type": "statement", "tables": ["users", "orders"], "complexity": "medium"}
        )

        # Add transformation result
        transform_result = ProcessorResult(
            metadata={"transformation_type": "parameterize", "parameters_extracted": 3, "comments_removed": 2}
        )

        # Aggregate all results
        results = [
            ("SecurityValidator", validation_result),
            ("StatementAnalyzer", analysis_result),
            ("ParameterTransformer", transform_result),
        ]

        final_results = aggregator.aggregate_processor_results(results)
        aggregator.add_timing_info(89.5)

        # Verify aggregation
        assert final_results.total_processors_run == 3
        assert len(final_results.security_issues) == 1
        assert len(final_results.performance_issues) == 1
        assert final_results.processing_time_ms == 89.5

        # Test summary
        summary = final_results.get_summary()
        assert summary["processors_run"] == 3
        assert summary["security_issues_count"] == 1
        assert summary["performance_issues_count"] == 1

        # Test recommendations
        recommendations = final_results.get_recommendations()
        assert "Use parameters" in recommendations
        assert "Add index on user_id" in recommendations
