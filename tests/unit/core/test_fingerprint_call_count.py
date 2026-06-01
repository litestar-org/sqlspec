# pyright: reportPrivateUsage = false
"""Regression tests for structural fingerprint reuse in the shared pipeline."""

from typing import Any
from unittest.mock import patch

import pytest

from sqlspec.core.parameters._processor import structural_fingerprint, value_fingerprint
from sqlspec.core.parameters._types import ParameterStyle, ParameterStyleConfig
from sqlspec.core.pipeline import reset_statement_pipeline_cache
from sqlspec.core.statement import SQL, StatementConfig


@pytest.fixture(autouse=True)
def reset_pipeline() -> None:
    reset_statement_pipeline_cache()


def _make_config(*, needs_static_script_compilation: bool = False) -> StatementConfig:
    return StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NAMED_COLON,
            supported_parameter_styles={ParameterStyle.NAMED_COLON},
            default_execution_parameter_style=ParameterStyle.NAMED_COLON,
            supported_execution_parameter_styles={ParameterStyle.NAMED_COLON},
            needs_static_script_compilation=needs_static_script_compilation,
        )
    )


def test_structural_fingerprint_called_once_per_fresh_compile() -> None:
    config = _make_config()
    statement = SQL("SELECT * FROM users WHERE id = :id", statement_config=config, id=1)
    call_count = 0

    def counting_fingerprint(parameters: Any, is_many: bool = False) -> Any:
        nonlocal call_count
        call_count += 1
        return structural_fingerprint(parameters, is_many=is_many)

    with patch("sqlspec.core.statement.structural_fingerprint", side_effect=counting_fingerprint):
        with patch("sqlspec.core.compiler.structural_fingerprint", side_effect=counting_fingerprint):
            statement.compile()

    assert call_count == 1


def test_static_script_path_does_not_forward_structural_fingerprint() -> None:
    config = _make_config(needs_static_script_compilation=True)
    statement = SQL("SELECT :id", statement_config=config, id=1)
    value_fingerprint_calls = 0

    def counting_value_fingerprint(parameters: Any) -> Any:
        nonlocal value_fingerprint_calls
        value_fingerprint_calls += 1
        return value_fingerprint(parameters)

    with patch("sqlspec.core.compiler.value_fingerprint", side_effect=counting_value_fingerprint):
        statement.compile()

    assert value_fingerprint_calls >= 1
