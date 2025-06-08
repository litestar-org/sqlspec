"""Unit tests for the Result Aggregator."""

import pytest

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.aggregator import ProcessorResult, ResultAggregator, ValidationIssue


class TestResultAggregator:
    """Test the ResultAggregator class."""

    @pytest.fixture
    def aggregator(self):
        """Create a result aggregator instance."""
        return ResultAggregator()

    def test_initialization(self, aggregator) -> None:
        """Test aggregator initialization."""
        assert aggregator.analysis_results == []
        assert aggregator.validation_results == []
        assert aggregator.transformation_results == []
        assert aggregator.overall_risk_level == RiskLevel.SKIP

    def test_add_analysis_result(self, aggregator) -> None:
        """Test adding analysis results."""
        result = ProcessorResult(processor_name="TestAnalyzer", success=True, data={"tables": ["users", "orders"]})

        aggregator.add_analysis_result(result)

        assert len(aggregator.analysis_results) == 1
        assert aggregator.analysis_results[0] == result

    def test_add_validation_result_skip(self, aggregator) -> None:
        """Test adding validation result with SKIP risk level."""
        from sqlspec.statement.pipelines import ValidationResult

        result = ValidationResult(processor_name="TestValidator", risk_level=RiskLevel.SKIP, issues=[])

        aggregator.add_validation_result(result)

        assert len(aggregator.validation_results) == 1
        assert aggregator.overall_risk_level == RiskLevel.SKIP

    def test_add_validation_result_low_risk(self, aggregator) -> None:
        """Test adding validation result with LOW risk level."""
        from sqlspec.statement.pipelines import ValidationResult

        issue = ValidationIssue(
            severity=RiskLevel.LOW, message="Minor performance concern", line_number=1, column_number=0, code="PERF001"
        )

        result = ValidationResult(processor_name="PerformanceValidator", risk_level=RiskLevel.LOW, issues=[issue])

        aggregator.add_validation_result(result)

        assert len(aggregator.validation_results) == 1
        assert aggregator.overall_risk_level == RiskLevel.LOW

    def test_risk_level_escalation(self, aggregator) -> None:
        """Test that risk level escalates to highest level."""
        from sqlspec.statement.pipelines import ValidationResult

        # Add LOW risk
        low_result = ValidationResult(
            processor_name="Validator1",
            risk_level=RiskLevel.LOW,
            issues=[ValidationIssue(RiskLevel.LOW, "Low risk", 1, 0, "LOW001")],
        )
        aggregator.add_validation_result(low_result)
        assert aggregator.overall_risk_level == RiskLevel.LOW

        # Add MEDIUM risk - should escalate
        medium_result = ValidationResult(
            processor_name="Validator2",
            risk_level=RiskLevel.MEDIUM,
            issues=[ValidationIssue(RiskLevel.MEDIUM, "Medium risk", 2, 0, "MED001")],
        )
        aggregator.add_validation_result(medium_result)
        assert aggregator.overall_risk_level == RiskLevel.MEDIUM

        # Add HIGH risk - should escalate
        high_result = ValidationResult(
            processor_name="Validator3",
            risk_level=RiskLevel.HIGH,
            issues=[ValidationIssue(RiskLevel.HIGH, "High risk", 3, 0, "HIGH001")],
        )
        aggregator.add_validation_result(high_result)
        assert aggregator.overall_risk_level == RiskLevel.HIGH

        # Add another LOW risk - should remain HIGH
        aggregator.add_validation_result(low_result)
        assert aggregator.overall_risk_level == RiskLevel.HIGH

    def test_add_transformation_result(self, aggregator) -> None:
        """Test adding transformation results."""
        result = ProcessorResult(
            processor_name="ParameterTransformer",
            success=True,
            data={
                "original_sql": "SELECT * FROM users WHERE id = 1",
                "transformed_sql": "SELECT * FROM users WHERE id = ?",
                "extracted_params": [1],
            },
        )

        aggregator.add_transformation_result(result)

        assert len(aggregator.transformation_results) == 1
        assert aggregator.transformation_results[0] == result

    def test_get_all_issues(self, aggregator) -> None:
        """Test retrieving all validation issues."""
        from sqlspec.statement.pipelines import ValidationResult

        # Add multiple validation results with issues
        issues1 = [
            ValidationIssue(RiskLevel.LOW, "Issue 1", 1, 0, "CODE1"),
            ValidationIssue(RiskLevel.MEDIUM, "Issue 2", 2, 0, "CODE2"),
        ]
        result1 = ValidationResult("Validator1", RiskLevel.MEDIUM, issues1)

        issues2 = [ValidationIssue(RiskLevel.HIGH, "Issue 3", 3, 0, "CODE3")]
        result2 = ValidationResult("Validator2", RiskLevel.HIGH, issues2)

        aggregator.add_validation_result(result1)
        aggregator.add_validation_result(result2)

        all_issues = aggregator.get_all_issues()

        assert len(all_issues) == 3
        assert all_issues[0].message == "Issue 1"
        assert all_issues[1].message == "Issue 2"
        assert all_issues[2].message == "Issue 3"

    def test_get_issues_by_severity(self, aggregator) -> None:
        """Test filtering issues by severity."""
        from sqlspec.statement.pipelines import ValidationResult

        issues = [
            ValidationIssue(RiskLevel.LOW, "Low issue", 1, 0, "LOW1"),
            ValidationIssue(RiskLevel.MEDIUM, "Medium issue", 2, 0, "MED1"),
            ValidationIssue(RiskLevel.HIGH, "High issue", 3, 0, "HIGH1"),
            ValidationIssue(RiskLevel.LOW, "Another low", 4, 0, "LOW2"),
        ]
        result = ValidationResult("MultiValidator", RiskLevel.HIGH, issues)
        aggregator.add_validation_result(result)

        # Get only HIGH severity issues
        high_issues = aggregator.get_issues_by_severity(RiskLevel.HIGH)
        assert len(high_issues) == 1
        assert high_issues[0].message == "High issue"

        # Get only LOW severity issues
        low_issues = aggregator.get_issues_by_severity(RiskLevel.LOW)
        assert len(low_issues) == 2
        assert low_issues[0].message == "Low issue"
        assert low_issues[1].message == "Another low"

    def test_to_dict(self, aggregator) -> None:
        """Test serialization to dictionary."""
        from sqlspec.statement.pipelines import ValidationResult

        # Add various results
        analysis_result = ProcessorResult("Analyzer", True, {"tables": ["users"]})
        validation_result = ValidationResult(
            "Validator", RiskLevel.LOW, [ValidationIssue(RiskLevel.LOW, "Test issue", 1, 0, "TEST001")]
        )
        transform_result = ProcessorResult("Transformer", True, {"sql": "SELECT 1"})

        aggregator.add_analysis_result(analysis_result)
        aggregator.add_validation_result(validation_result)
        aggregator.add_transformation_result(transform_result)

        result_dict = aggregator.to_dict()

        assert "overall_risk_level" in result_dict
        assert result_dict["overall_risk_level"] == "low"
        assert "analysis_results" in result_dict
        assert len(result_dict["analysis_results"]) == 1
        assert "validation_results" in result_dict
        assert len(result_dict["validation_results"]) == 1
        assert "transformation_results" in result_dict
        assert len(result_dict["transformation_results"]) == 1
        assert "all_issues" in result_dict
        assert len(result_dict["all_issues"]) == 1

    def test_empty_aggregator_to_dict(self, aggregator) -> None:
        """Test serialization of empty aggregator."""
        result_dict = aggregator.to_dict()

        assert result_dict["overall_risk_level"] == "skip"
        assert result_dict["analysis_results"] == []
        assert result_dict["validation_results"] == []
        assert result_dict["transformation_results"] == []
        assert result_dict["all_issues"] == []

    def test_multiple_validators_same_risk_level(self, aggregator) -> None:
        """Test aggregating results from multiple validators with same risk level."""
        from sqlspec.statement.pipelines import ValidationResult

        # Multiple validators all finding MEDIUM risk issues
        for i in range(3):
            result = ValidationResult(
                f"Validator{i}",
                RiskLevel.MEDIUM,
                [ValidationIssue(RiskLevel.MEDIUM, f"Issue from validator {i}", i + 1, 0, f"CODE{i}")],
            )
            aggregator.add_validation_result(result)

        assert len(aggregator.validation_results) == 3
        assert aggregator.overall_risk_level == RiskLevel.MEDIUM
        assert len(aggregator.get_all_issues()) == 3

    def test_mixed_processor_results(self, aggregator) -> None:
        """Test handling mixed success/failure processor results."""
        # Successful analysis
        aggregator.add_analysis_result(ProcessorResult("SuccessAnalyzer", True, {"result": "data"}))

        # Failed analysis
        aggregator.add_analysis_result(ProcessorResult("FailedAnalyzer", False, {"error": "Analysis failed"}))

        # Successful transformation
        aggregator.add_transformation_result(ProcessorResult("Transformer", True, {"transformed": True}))

        assert len(aggregator.analysis_results) == 2
        assert aggregator.analysis_results[0].success is True
        assert aggregator.analysis_results[1].success is False
        assert len(aggregator.transformation_results) == 1
