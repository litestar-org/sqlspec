"""Regression tests for the shared statement pipeline registry."""

from unittest.mock import patch

import pytest
from sqlglot import exp

from sqlspec.core import ParameterProfile
from sqlspec.core.compiler import CompiledSQL
from sqlspec.core.pipeline import compile_with_pipeline, reset_statement_pipeline_cache
from sqlspec.core.statement import get_default_config

pytestmark = pytest.mark.xdist_group("core")


def test_compile_with_pipeline_passes_expression() -> None:
    """Ensure pipeline forwards expressions to the SQL processor."""
    config = get_default_config()
    expression = exp.select("*").from_("users")

    reset_statement_pipeline_cache()
    with patch("sqlspec.core.pipeline.SQLProcessor.compile") as mock_compile:
        mock_compile.return_value = CompiledSQL(
            compiled_sql="SELECT * FROM users",
            execution_parameters=[],
            operation_type="SELECT",
            expression=expression,
            parameter_profile=ParameterProfile.empty(),
        )

        _ = compile_with_pipeline(config, "SELECT * FROM users", [], expression=expression)

        _, kwargs = mock_compile.call_args
        assert kwargs["expression"] is expression
