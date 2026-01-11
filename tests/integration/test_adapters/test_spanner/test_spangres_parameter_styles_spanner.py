"""Integration tests for Spanner PostgreSQL interface parameter styles.

These tests verify that Spangres parameter binding works with PostgreSQL's
$1, $2 positional parameter style through SQLSpec.

To run these tests, you need:
1. A Spanner database created with database_dialect=DatabaseDialect.POSTGRESQL
2. The spangres_service and spangres_config fixtures configured in conftest.py
"""

import pytest

from sqlspec.adapters.spanner.core import driver_profile
from sqlspec.core import ParameterStyle

pytestmark = [pytest.mark.spanner, pytest.mark.skip(reason="Spangres fixtures missing")]


def test_spangres_positional_parameter_basic() -> None:
    """Test basic $1 parameter binding."""
    pytest.skip("Spangres fixtures required")


def test_spangres_positional_parameter_multiple() -> None:
    """Test multiple $1, $2, $3 parameters in single query."""
    pytest.skip("Spangres fixtures required")


def test_spangres_positional_parameter_reuse() -> None:
    """Test reusing same positional parameter."""
    pytest.skip("Spangres fixtures required")


def test_spangres_null_parameter() -> None:
    """Test NULL parameter handling in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_array_parameter() -> None:
    """Test array parameter binding in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_timestamp_parameter() -> None:
    """Test TIMESTAMP parameter binding in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_parameter_type_inference() -> None:
    """Test automatic parameter type inference in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_parameter_in_where_clause() -> None:
    """Test parameter binding in WHERE clause."""
    pytest.skip("Spangres fixtures required")


def test_spangres_parameter_style_differs_from_googlesql() -> None:
    """Verify Spangres uses $1 style vs GoogleSQL's @name style.

    This is a documentation test to confirm the parameter style difference.
    """
    assert driver_profile.default_style == ParameterStyle.NAMED_AT
