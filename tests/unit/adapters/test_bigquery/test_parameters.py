"""Unit tests for BigQuery parameter handling utilities."""

import pytest

from sqlspec.adapters.bigquery.core import create_parameters
from sqlspec.exceptions import SQLSpecError


def test_create_parameters_requires_named_parameters() -> None:
    """Positional parameters should raise to avoid silent no-op behaviour."""

    with pytest.raises(SQLSpecError, match="requires named parameters"):
        create_parameters([1, 2, 3], json_serializer=lambda value: value)
