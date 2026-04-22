"""Shared fixtures for adapter testing."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig

if TYPE_CHECKING:
    pass


__all__ = ("sample_sql_statement", "sample_statement_config")


@pytest.fixture
def sample_statement_config() -> StatementConfig:
    """Sample statement configuration for testing."""
    return StatementConfig(
        dialect="sqlite",
        enable_caching=False,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
            default_execution_parameter_style=ParameterStyle.QMARK,
            supported_execution_parameter_styles={ParameterStyle.QMARK},
        ),
    )


@pytest.fixture
def sample_sql_statement(sample_statement_config: StatementConfig) -> SQL:
    """Sample SQL statement for testing."""
    return SQL("SELECT * FROM users WHERE id = ?", 1, statement_config=sample_statement_config)
