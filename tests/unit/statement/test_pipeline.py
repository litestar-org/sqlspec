"""Tests for the single-pass pipeline architecture."""

import sqlglot.expressions as exp

from sqlspec.statement.pipeline import (
    SQLTransformContext,
    compose_pipeline,
    normalize_step,
    optimize_step,
    parameterize_literals_step,
    validate_step,
)


class TestSQLTransformContext:
    """Test the SQLTransformContext dataclass."""

    def test_context_initialization(self):
        """Test basic context initialization."""
        expr = exp.Select().select("*").from_("users")
        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr,
            dialect="postgres"
        )

        assert context.current_expression == expr
        assert context.original_expression == expr
        assert context.dialect == "postgres"
        assert context.parameters == {}
        assert context.metadata == {}

    def test_merged_parameters_mysql_sqlite(self):
        """Test parameter merging for MySQL/SQLite (positional)."""
        expr = exp.Select()
        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr,
            dialect="mysql",
            parameters={"param_0": "value1", "param_1": "value2", "param_2": "value3"}
        )

        # Should return positional list in sorted order
        merged = context.merged_parameters
        assert merged == ["value1", "value2", "value3"]

    def test_merged_parameters_postgres(self):
        """Test parameter merging for other dialects (named)."""
        expr = exp.Select()
        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr,
            dialect="postgres",
            parameters={"name": "John", "age": 30}
        )

        # Should return dict as-is
        assert context.merged_parameters == {"name": "John", "age": 30}


class TestPipelineComposition:
    """Test pipeline composition functionality."""

    def test_compose_pipeline_empty(self):
        """Test composing empty pipeline."""
        pipeline = compose_pipeline([])

        expr = exp.Select()
        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = pipeline(context)
        assert result == context

    def test_compose_pipeline_single_step(self):
        """Test composing single-step pipeline."""
        def test_step(context: SQLTransformContext) -> SQLTransformContext:
            context.metadata["test"] = True
            return context

        pipeline = compose_pipeline([test_step])

        expr = exp.Select()
        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = pipeline(context)
        assert result.metadata["test"] is True

    def test_compose_pipeline_multiple_steps(self):
        """Test composing multi-step pipeline."""
        def step1(context: SQLTransformContext) -> SQLTransformContext:
            context.metadata["step1"] = True
            return context

        def step2(context: SQLTransformContext) -> SQLTransformContext:
            context.metadata["step2"] = True
            return context

        def step3(context: SQLTransformContext) -> SQLTransformContext:
            context.metadata["step3"] = True
            return context

        pipeline = compose_pipeline([step1, step2, step3])

        expr = exp.Select()
        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = pipeline(context)
        assert result.metadata["step1"] is True
        assert result.metadata["step2"] is True
        assert result.metadata["step3"] is True


class TestNormalizeStep:
    """Test the normalize step."""

    def test_normalize_step_passthrough(self):
        """Test that normalize step passes through context unchanged (for now)."""
        expr = exp.Select()
        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = normalize_step(context)
        assert result == context


class TestParameterizeLiteralsStep:
    """Test the parameterize literals step."""

    def test_parameterize_simple_literal(self):
        """Test parameterizing a simple string literal."""
        # Create SQL with literal: SELECT * FROM users WHERE name = 'John'
        expr = exp.Select().select("*").from_("users").where("name = 'John'")

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = parameterize_literals_step(context)

        # Check that literal was replaced with placeholder
        assert "param_0" in result.parameters
        assert result.parameters["param_0"] == "John"
        assert result.metadata["literals_parameterized"] is True
        assert result.metadata["parameter_count"] == 1

        # Check that the expression now has a placeholder
        sql = result.current_expression.sql()
        assert "param_0" in sql
        assert "'John'" not in sql

    def test_parameterize_multiple_literals(self):
        """Test parameterizing multiple literals."""
        # SELECT * FROM users WHERE name = 'John' AND age = 30
        expr = (exp.Select()
                .select("*")
                .from_("users")
                .where("name = 'John' AND age = 30"))

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = parameterize_literals_step(context)

        # Check parameters
        assert len(result.parameters) == 2
        assert result.parameters["param_0"] == "John"
        assert result.parameters["param_1"] == "30"  # Numbers are stored as strings in literals
        assert result.metadata["parameter_count"] == 2

    def test_parameterize_no_literals(self):
        """Test parameterizing SQL with no literals."""
        # SELECT * FROM users
        expr = exp.Select().select("*").from_("users")

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = parameterize_literals_step(context)

        # No parameters should be added
        assert len(result.parameters) == 0
        assert result.metadata["parameter_count"] == 0


class TestOptimizeStep:
    """Test the optimize step."""

    def test_optimize_simple_expression(self):
        """Test optimizing a simple expression."""
        # Create expression with redundant condition: WHERE 1 = 1 AND name = 'John'
        expr = (exp.Select()
                .select("*")
                .from_("users")
                .where("1 = 1 AND name = 'John'"))

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = optimize_step(context)

        # Should be optimized
        assert result.metadata["optimized"] is True

        # The tautology might be simplified away
        sql = result.current_expression.sql()
        assert "name" in sql  # The real condition should remain


class TestValidateStep:
    """Test the validate step."""

    def test_validate_clean_sql(self):
        """Test validating clean SQL."""
        expr = exp.Select().select("*").from_("users")

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = validate_step(context)

        assert result.metadata["validated"] is True
        assert result.metadata["validation_issues"] == []
        assert result.metadata["validation_warnings"] == []

    def test_validate_suspicious_function(self):
        """Test detecting suspicious functions."""
        # SELECT SLEEP(5) FROM users
        expr = exp.Select().select(exp.func("SLEEP", 5)).from_("users")

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = validate_step(context)

        assert result.metadata["validated"] is True
        assert len(result.metadata["validation_issues"]) > 0
        assert "sleep" in result.metadata["validation_issues"][0].lower()

    def test_validate_tautology(self):
        """Test detecting tautologies."""
        # WHERE 'a' = 'a'
        expr = exp.Select().select("*").from_("users").where("'a' = 'a'")

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = validate_step(context)

        assert result.metadata["validated"] is True
        assert len(result.metadata["validation_warnings"]) > 0
        assert "tautology" in result.metadata["validation_warnings"][0].lower()

    def test_validate_union_injection_pattern(self):
        """Test detecting potential UNION injection patterns."""
        # Create a UNION with many NULLs (suspicious pattern)
        union_select = exp.Select().select(
            exp.Null(), exp.Null(), exp.Null(), exp.Null(), exp.Null()
        )
        main_select = exp.Select().select("*").from_("users")
        expr = exp.Union(this=main_select, expression=union_select)

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr
        )

        result = validate_step(context)

        assert result.metadata["validated"] is True
        assert len(result.metadata["validation_warnings"]) > 0
        assert "union" in result.metadata["validation_warnings"][0].lower()


class TestFullPipeline:
    """Test full pipeline execution."""

    def test_full_pipeline_execution(self):
        """Test running a full pipeline with all steps."""
        # Create SQL with literals and potential issues
        expr = (exp.Select()
                .select("*")
                .from_("users")
                .where("name = 'John' AND 1 = 1"))

        context = SQLTransformContext(
            current_expression=expr,
            original_expression=expr,
            dialect="postgres"
        )

        # Create and run full pipeline
        pipeline = compose_pipeline([
            normalize_step,
            parameterize_literals_step,
            optimize_step,
            validate_step
        ])

        result = pipeline(context)

        # Check that all steps ran
        assert result.metadata["literals_parameterized"] is True
        assert result.metadata["optimized"] is True
        assert result.metadata["validated"] is True

        # Check parameters were extracted
        assert "param_0" in result.parameters
        assert result.parameters["param_0"] == "John"

        # Check that parameters were properly handled
        # The tautology (1=1) might have been optimized away by the optimize step
        # which is correct behavior - we shouldn't warn about optimized-away tautologies
