"""Unit tests for SQL class helper methods introduced in the refactoring."""

from unittest.mock import Mock, patch

import pytest

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement.filters import LimitOffsetFilter
from sqlspec.statement.parameters import ParameterInfo, ParameterStyle
from sqlspec.statement.pipelines import ProcessingResult, ValidationIssue, ValidationResult
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.sql import SQL, SQLConfig


class TestSQLHelperMethods:
    """Test the refactored SQL helper methods."""

    @pytest.fixture
    def config(self):
        """Create a default SQL config."""
        return SQLConfig()

    @pytest.fixture
    def sql_instance(self, config):
        """Create a SQL instance."""
        return SQL("SELECT * FROM users WHERE id = ?", parameters=(1,), config=config)

    def test_prepare_processing_context(self, sql_instance) -> None:
        """Test _prepare_processing_context method."""
        context = sql_instance._prepare_processing_context()

        assert isinstance(context, SQLProcessingContext)
        assert context.initial_sql_string == "SELECT * FROM users WHERE id = ?"
        assert context.initial_parameters == (1,)
        assert context.config == sql_instance._config
        assert context.current_expression is None
        assert context.extracted_parameters_from_pipeline == []

    def test_detect_placeholders_with_placeholders(self, sql_instance) -> None:
        """Test _detect_placeholders when SQL has placeholders."""
        context = sql_instance._prepare_processing_context()

        with patch.object(sql_instance._config.parameter_validator, "extract_parameters") as mock_extract:
            mock_extract.return_value = [
                ParameterInfo(name="param_0", style=ParameterStyle.QMARK, position=0)
            ]

            sql_instance._detect_placeholders(context, "SELECT * FROM users WHERE id = ?")

            assert context.input_sql_had_placeholders is True
            assert sql_instance._config.input_sql_had_placeholders is True

    def test_detect_placeholders_without_placeholders(self, sql_instance) -> None:
        """Test _detect_placeholders when SQL has no placeholders."""
        sql_no_params = SQL("SELECT * FROM users")
        context = sql_no_params._prepare_processing_context()

        with patch.object(sql_no_params._config.parameter_validator, "extract_parameters") as mock_extract:
            mock_extract.return_value = []

            sql_no_params._detect_placeholders(context, "SELECT * FROM users")

            assert context.input_sql_had_placeholders is False

    def test_process_parameters_with_dict(self, config) -> None:
        """Test _process_parameters with dictionary parameters."""
        sql_dict = SQL("SELECT * FROM users WHERE id = :id", parameters={"id": 1}, config=config)
        context = sql_dict._prepare_processing_context()

        processed_params = sql_dict._process_parameters(context)

        assert processed_params == {"id": 1}

    def test_process_parameters_with_list(self, sql_instance) -> None:
        """Test _process_parameters with list parameters."""
        context = sql_instance._prepare_processing_context()

        processed_params = sql_instance._process_parameters(context)

        assert processed_params == [1]

    def test_process_parameters_with_tuple(self, sql_instance) -> None:
        """Test _process_parameters with tuple parameters."""
        context = sql_instance._prepare_processing_context()

        processed_params = sql_instance._process_parameters(context)

        assert processed_params == [1]  # Tuples are converted to lists

    def test_parse_initial_expression_enabled(self, sql_instance) -> None:
        """Test _parse_initial_expression when parsing is enabled."""
        with patch("sqlglot.parse_one") as mock_parse:
            mock_exp = Mock()
            mock_parse.return_value = mock_exp

            result = sql_instance._parse_initial_expression("SELECT 1", SQLConfig())

            assert result == mock_exp
            mock_parse.assert_called_once_with("SELECT 1", dialect=None)

    def test_parse_initial_expression_disabled(self, sql_instance) -> None:
        """Test _parse_initial_expression when parsing is disabled."""
        config = SQLConfig(enable_parsing=False)

        result = sql_instance._parse_initial_expression("SELECT 1", config)

        assert result is None

    def test_execute_pipeline(self, sql_instance) -> None:
        """Test _execute_pipeline method."""
        context = sql_instance._prepare_processing_context()
        context.current_expression = Mock()

        mock_pipeline = Mock()
        mock_result = ProcessingResult(
            sql="SELECT * FROM users WHERE id = ?",
            merged_parameters=[1],
            dialect="duckdb"
        )
        mock_pipeline.process.return_value = mock_result

        with patch.object(sql_instance._config, "get_statement_pipeline", return_value=mock_pipeline):
            result = sql_instance._execute_pipeline(context)

            assert result == mock_result
            mock_pipeline.process.assert_called_once()

    def test_merge_extracted_parameters_dict(self, config) -> None:
        """Test _merge_extracted_parameters with dict parameters."""
        sql_dict = SQL("SELECT * FROM users", config=config)
        context = sql_dict._prepare_processing_context()
        context.extracted_parameters_from_pipeline = [1, 2, 3]

        merged = sql_dict._merge_extracted_parameters({"existing": "value"}, context)

        assert merged == {
            "existing": "value",
            "param_0": 1,
            "param_1": 2,
            "param_2": 3
        }

    def test_merge_extracted_parameters_list(self, sql_instance) -> None:
        """Test _merge_extracted_parameters with list parameters."""
        context = sql_instance._prepare_processing_context()
        context.extracted_parameters_from_pipeline = [4, 5]

        merged = sql_instance._merge_extracted_parameters([1, 2, 3], context)

        assert merged == [1, 2, 3, 4, 5]

    def test_merge_extracted_parameters_none(self, sql_instance) -> None:
        """Test _merge_extracted_parameters with None parameters."""
        context = sql_instance._prepare_processing_context()
        context.extracted_parameters_from_pipeline = [1, 2]

        merged = sql_instance._merge_extracted_parameters(None, context)

        assert merged == [1, 2]

    def test_run_validation_only(self, sql_instance) -> None:
        """Test _run_validation_only method."""
        context = sql_instance._prepare_processing_context()
        context.current_expression = Mock()

        mock_validator = Mock()
        mock_validation_result = ValidationResult(
            processor_name="TestValidator",
            risk_level=RiskLevel.LOW,
            issues=[ValidationIssue(RiskLevel.LOW, "Test issue", 1, 0, "TEST001")]
        )
        mock_validator.validate.return_value = mock_validation_result

        with patch.object(sql_instance._config, "get_validators", return_value=[mock_validator]):
            result = sql_instance._run_validation_only(context, "SELECT 1")

            assert result.sql == "SELECT 1"
            assert result.merged_parameters == []
            assert result.validation_results == [mock_validation_result]

    def test_create_disabled_result(self, sql_instance) -> None:
        """Test _create_disabled_result method."""
        result = sql_instance._create_disabled_result("SELECT 1", {"id": 1})

        assert result.sql == "SELECT 1"
        assert result.merged_parameters == {"id": 1}
        assert result.dialect == "duckdb"  # default dialect
        assert result.validation_results == []
        assert result.transformation_results == []

    def test_build_processed_state_safe(self, sql_instance) -> None:
        """Test _build_processed_state with safe SQL."""
        pipeline_result = ProcessingResult(
            sql="SELECT * FROM users WHERE id = ?",
            merged_parameters=[1],
            dialect="postgres",
            validation_results=[]
        )

        state = sql_instance._build_processed_state(
            pipeline_result,
            SQLConfig(strict_mode=True)
        )

        assert state["_is_safe"] is True
        assert state["_processed_sql"] == "SELECT * FROM users WHERE id = ?"
        assert state["_final_parameters"] == [1]
        assert state["_validation_results"] == []

    def test_build_processed_state_unsafe_strict(self, sql_instance) -> None:
        """Test _build_processed_state with unsafe SQL in strict mode."""
        validation_result = ValidationResult(
            processor_name="SecurityValidator",
            risk_level=RiskLevel.HIGH,
            issues=[ValidationIssue(RiskLevel.HIGH, "SQL injection detected", 1, 0, "SEC001")]
        )

        pipeline_result = ProcessingResult(
            sql="SELECT * FROM users WHERE id = ?",
            merged_parameters=[1],
            dialect="postgres",
            validation_results=[validation_result]
        )

        with pytest.raises(SQLValidationError) as exc_info:
            sql_instance._build_processed_state(
                pipeline_result,
                SQLConfig(strict_mode=True)
            )

        assert "SQL validation failed" in str(exc_info.value)

    def test_build_processed_state_unsafe_non_strict(self, sql_instance) -> None:
        """Test _build_processed_state with unsafe SQL in non-strict mode."""
        validation_result = ValidationResult(
            processor_name="SecurityValidator",
            risk_level=RiskLevel.HIGH,
            issues=[ValidationIssue(RiskLevel.HIGH, "SQL injection detected", 1, 0, "SEC001")]
        )

        pipeline_result = ProcessingResult(
            sql="SELECT * FROM users WHERE id = ?",
            merged_parameters=[1],
            dialect="postgres",
            validation_results=[validation_result]
        )

        state = sql_instance._build_processed_state(
            pipeline_result,
            SQLConfig(strict_mode=False)
        )

        assert state["_is_safe"] is False
        assert state["_processed_sql"] == "SELECT * FROM users WHERE id = ?"
        assert state["_final_parameters"] == [1]
        assert len(state["_validation_results"]) == 1

    def test_ensure_processed_full_flow(self, config) -> None:
        """Test the full _ensure_processed flow."""
        sql = SQL("SELECT * FROM users WHERE name = 'test'", config=config)

        # Mock the pipeline
        mock_pipeline = Mock()
        mock_result = ProcessingResult(
            sql="SELECT * FROM users WHERE name = ?",
            merged_parameters=["test"],
            dialect="duckdb"
        )
        mock_pipeline.process.return_value = mock_result

        with patch.object(config, "get_statement_pipeline", return_value=mock_pipeline):
            # Access a property that triggers processing
            processed_sql = sql.sql

            assert processed_sql == "SELECT * FROM users WHERE name = ?"
            assert sql._final_parameters == ["test"]
            assert sql._processed is True

    def test_ensure_processed_with_filters(self, config) -> None:
        """Test _ensure_processed with filters applied."""
        sql = SQL("SELECT * FROM users", config=config)
        filtered_sql = sql.apply_filters(LimitOffsetFilter(limit=10))

        # The filtered SQL should process with the filter applied
        assert "LIMIT 10" in filtered_sql.sql
        assert filtered_sql._processed is True
