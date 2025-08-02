"""Tests for analysis pipeline functionality."""

from datetime import datetime
from decimal import Decimal

from sqlglot import exp

from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.parameters.types import TypedParameter
from sqlspec.statement.pipeline import (
    SQLTransformContext,
    metadata_extraction_step,
    parameter_analysis_step,
    returns_rows_analysis_step,
)
from sqlspec.statement.sql import SQL, StatementConfig


class TestAnalysisPipeline:
    """Test the analysis pipeline steps."""

    def test_metadata_extraction_step_select(self) -> None:
        """Test metadata extraction for SELECT statements."""
        expression = exp.select("id", "name").from_("users").where(exp.column("active").eq(1))
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = metadata_extraction_step(context)

        metadata = result.metadata["analysis_metadata"]
        assert metadata["operation_type"] == "SELECT"
        assert "users" in metadata["tables"]
        assert "id" in metadata["columns"]
        assert "name" in metadata["columns"]
        assert "active" in metadata["columns"]
        assert result.metadata["metadata_extracted"] is True

    def test_metadata_extraction_step_insert(self) -> None:
        """Test metadata extraction for INSERT statements."""
        from sqlglot import parse_one

        # Use parse_one for proper INSERT expression
        expression = parse_one(
            "INSERT INTO users (name, email) VALUES ('test', 'test@example.com')", dialect="postgres"
        )
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = metadata_extraction_step(context)

        metadata = result.metadata["analysis_metadata"]
        assert metadata["operation_type"] == "INSERT"
        assert "users" in metadata["tables"]
        assert "name" in metadata["columns"]
        assert "email" in metadata["columns"]
        assert result.metadata["metadata_extracted"] is True

    def test_metadata_extraction_step_with_joins(self) -> None:
        """Test metadata extraction with JOIN operations."""
        expression = (
            exp.select("u.name", "o.total")
            .from_("users u")
            .join("orders o", on="u.id = o.user_id")
            .join("payments p", on="o.id = p.order_id", join_type="left")
        )
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = metadata_extraction_step(context)

        metadata = result.metadata["analysis_metadata"]
        assert metadata["operation_type"] == "SELECT"
        assert "users" in metadata["tables"]
        assert "orders" in metadata["tables"]
        assert "payments" in metadata["tables"]
        assert "LEFT" in metadata["joins"]
        assert result.metadata["metadata_extracted"] is True

    def test_metadata_extraction_step_error_handling(self) -> None:
        """Test metadata extraction error handling."""
        # Anonymous expressions are handled gracefully, so test with None to force error
        context = SQLTransformContext(
            current_expression=None,  # type: ignore[arg-type]
            original_expression=None,  # type: ignore[arg-type]
            dialect="postgres",
        )

        result = metadata_extraction_step(context)

        # Should handle errors gracefully
        assert result.metadata["metadata_extracted"] is False
        assert "metadata_extraction_error" in result.metadata

    def test_returns_rows_analysis_select(self) -> None:
        """Test returns rows analysis for SELECT statements."""
        expression = exp.select("*").from_("users")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = returns_rows_analysis_step(context)

        assert result.metadata["returns_rows"] is True
        assert result.metadata["returns_rows_analyzed"] is True

    def test_returns_rows_analysis_insert_without_returning(self) -> None:
        """Test returns rows analysis for INSERT without RETURNING."""
        from sqlglot import parse_one

        expression = parse_one("INSERT INTO users (name) VALUES ('test')", dialect="postgres")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = returns_rows_analysis_step(context)

        assert result.metadata["returns_rows"] is False
        assert result.metadata["returns_rows_analyzed"] is True

    def test_returns_rows_analysis_insert_with_returning(self) -> None:
        """Test returns rows analysis for INSERT with RETURNING."""
        from sqlglot import parse_one

        expression = parse_one("INSERT INTO users (name) VALUES ('test') RETURNING id", dialect="postgres")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = returns_rows_analysis_step(context)

        assert result.metadata["returns_rows"] is True
        assert result.metadata["returns_rows_analyzed"] is True

    def test_returns_rows_analysis_update_with_returning(self) -> None:
        """Test returns rows analysis for UPDATE with RETURNING."""
        from sqlglot import parse_one

        expression = parse_one("UPDATE users SET active = false WHERE id = 1 RETURNING id", dialect="postgres")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = returns_rows_analysis_step(context)

        assert result.metadata["returns_rows"] is True
        assert result.metadata["returns_rows_analyzed"] is True

    def test_returns_rows_analysis_anonymous_select(self) -> None:
        """Test returns rows analysis for anonymous SELECT."""
        expression = exp.Anonymous(this="SELECT 1")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = returns_rows_analysis_step(context)

        assert result.metadata["returns_rows"] is True
        assert result.metadata["returns_rows_analyzed"] is True

    def test_returns_rows_analysis_anonymous_show(self) -> None:
        """Test returns rows analysis for anonymous SHOW statement."""
        expression = exp.Anonymous(this="SHOW TABLES")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = returns_rows_analysis_step(context)

        assert result.metadata["returns_rows"] is True
        assert result.metadata["returns_rows_analyzed"] is True

    def test_returns_rows_analysis_anonymous_create(self) -> None:
        """Test returns rows analysis for anonymous CREATE statement."""
        expression = exp.Anonymous(this="CREATE TABLE test (id INT)")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = returns_rows_analysis_step(context)

        assert result.metadata["returns_rows"] is False
        assert result.metadata["returns_rows_analyzed"] is True

    def test_parameter_analysis_dict_parameters(self) -> None:
        """Test parameter analysis with dict parameters."""
        expression = exp.select("*").from_("users").where(exp.column("id").eq(exp.Placeholder(this="user_id")))
        context = SQLTransformContext(
            current_expression=expression,
            original_expression=expression,
            parameters={
                "user_id": TypedParameter(value=123, data_type=exp.DataType.build("INTEGER"), type_hint="integer")
            },
            dialect="postgres",
        )

        result = parameter_analysis_step(context)

        analysis = result.metadata["parameter_analysis"]
        assert analysis["parameter_count"] == 1
        assert analysis["has_named_parameters"] is True
        assert analysis["has_positional_parameters"] is False
        assert "int" in analysis["parameter_types"]  # TypedParameter.value type is used
        assert analysis["placeholder_count"] == 1
        assert result.metadata["parameter_analyzed"] is True

    def test_parameter_analysis_list_parameters(self) -> None:
        """Test parameter analysis with list parameters."""
        expression = exp.select("*").from_("users").where(exp.column("id").eq(exp.Placeholder(this="?")))
        context = SQLTransformContext(
            current_expression=expression,
            original_expression=expression,
            parameters=[123, "test", datetime(2024, 1, 1)],
            dialect="postgres",
        )

        result = parameter_analysis_step(context)

        analysis = result.metadata["parameter_analysis"]
        assert analysis["parameter_count"] == 3
        assert analysis["has_named_parameters"] is False
        assert analysis["has_positional_parameters"] is True
        assert "int" in analysis["parameter_types"]
        assert "str" in analysis["parameter_types"]
        assert "datetime" in analysis["parameter_types"]
        assert result.metadata["parameter_analyzed"] is True

    def test_parameter_analysis_typed_parameters(self) -> None:
        """Test parameter analysis with TypedParameter objects."""
        expression = exp.select("*").from_("users")
        context = SQLTransformContext(
            current_expression=expression,
            original_expression=expression,
            parameters={
                "balance": TypedParameter(
                    value=Decimal("100.50"), data_type=exp.DataType.build("DECIMAL"), type_hint="decimal"
                ),
                "active": TypedParameter(value=True, data_type=exp.DataType.build("BOOLEAN"), type_hint="boolean"),
            },
            dialect="postgres",
        )

        result = parameter_analysis_step(context)

        analysis = result.metadata["parameter_analysis"]
        assert analysis["parameter_count"] == 2
        assert analysis["has_named_parameters"] is True
        assert "Decimal" in analysis["parameter_types"]
        assert "bool" in analysis["parameter_types"]
        assert result.metadata["parameter_analyzed"] is True

    def test_parameter_analysis_no_parameters(self) -> None:
        """Test parameter analysis with no parameters."""
        expression = exp.select("*").from_("users")
        context = SQLTransformContext(current_expression=expression, original_expression=expression, dialect="postgres")

        result = parameter_analysis_step(context)

        analysis = result.metadata["parameter_analysis"]
        assert analysis["parameter_count"] == 0
        assert analysis["has_named_parameters"] is False
        assert analysis["has_positional_parameters"] is False
        assert len(analysis["parameter_types"]) == 0
        assert analysis["placeholder_count"] == 0
        assert result.metadata["parameter_analyzed"] is True

    def test_parameter_analysis_error_handling(self) -> None:
        """Test parameter analysis handles various parameter types gracefully."""
        # The parameter analysis function is robust and handles string parameters
        expression = exp.select("*").from_("users")
        context = SQLTransformContext(
            current_expression=expression,
            original_expression=expression,
            parameters="invalid_parameter_type",  # type: ignore[arg-type]
            dialect="postgres",
        )

        result = parameter_analysis_step(context)

        # Should handle gracefully - string parameters don't cause errors
        assert result.metadata["parameter_analyzed"] is True
        assert "parameter_analysis" in result.metadata


class TestAnalysisPipelineIntegration:
    """Test analysis pipeline integration with SQL objects."""

    def test_sql_with_analysis_enabled(self) -> None:
        """Test SQL with analysis pipeline enabled."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_analysis=True)

        sql = SQL(
            "SELECT u.name, COUNT(*) as order_count FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = 1 GROUP BY u.name",
            statement_config=config,
        )

        # Access sql to trigger pipeline processing
        _ = sql.sql

        # Check that analysis metadata was generated
        assert sql._processed_state is not None
        metadata = sql._processed_state.analysis_results

        # Metadata extraction
        assert "analysis_metadata" in metadata
        analysis_metadata = metadata["analysis_metadata"]
        assert analysis_metadata["operation_type"] == "SELECT"
        assert "users" in analysis_metadata["tables"]
        assert "orders" in analysis_metadata["tables"]
        assert "name" in analysis_metadata["columns"]

        # Returns rows analysis
        assert metadata["returns_rows"] is True
        assert metadata["returns_rows_analyzed"] is True

        # Parameter analysis
        assert "parameter_analysis" in metadata
        param_analysis = metadata["parameter_analysis"]
        assert param_analysis["parameter_count"] >= 0  # May have parameterized literals

    def test_sql_with_analysis_disabled(self) -> None:
        """Test SQL with analysis pipeline disabled."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_analysis=False)

        sql = SQL("SELECT * FROM users WHERE active = 1", statement_config=config)

        # Access sql to trigger pipeline processing
        _ = sql.sql

        # Check that analysis metadata was not generated
        assert sql._processed_state is not None
        metadata = sql._processed_state.analysis_results

        # Analysis steps should not have run
        assert "analysis_metadata" not in metadata
        assert "returns_rows" not in metadata
        assert "parameter_analysis" not in metadata

    def test_sql_with_parameterized_literals_and_analysis(self) -> None:
        """Test SQL with literal parameterization and analysis enabled."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_analysis=True)

        sql = SQL("SELECT * FROM users WHERE age > 18 AND active = true", statement_config=config)

        # Access sql to trigger pipeline processing
        compiled_sql, params = sql.compile()

        # Should have parameterized literals
        assert ":param_" in compiled_sql or "?" in compiled_sql  # Either named or qmark format
        assert len(params) > 0

        # Check that parameter analysis captured the parameterized literals
        assert sql._processed_state is not None
        metadata = sql._processed_state.analysis_results
        param_analysis = metadata["parameter_analysis"]
        assert param_analysis["parameter_count"] > 0
        assert param_analysis["has_named_parameters"] is True  # Internal format uses named

    def test_sql_insert_with_returning_analysis(self) -> None:
        """Test SQL INSERT with RETURNING analysis."""
        param_config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
        config = StatementConfig(parameter_config=param_config, enable_analysis=True)

        sql = SQL(
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id", statement_config=config
        )

        # Access sql to trigger pipeline processing
        _ = sql.sql

        # Check returns rows analysis
        assert sql._processed_state is not None
        metadata = sql._processed_state.analysis_results
        assert metadata["returns_rows"] is True
        assert metadata["returns_rows_analyzed"] is True

        # Check metadata extraction
        analysis_metadata = metadata["analysis_metadata"]
        assert analysis_metadata["operation_type"] == "INSERT"
        assert "users" in analysis_metadata["tables"]
