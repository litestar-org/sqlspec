"""Unit tests for BigQuery parameter handling utilities."""

import pytest

from sqlspec.adapters.bigquery.driver import _create_bq_parameters
from sqlspec.exceptions import SQLSpecError


def test_create_bq_parameters_requires_named_parameters() -> None:
    """Positional parameters should raise to avoid silent no-op behaviour."""

    with pytest.raises(SQLSpecError, match="requires named parameters"):
        _create_bq_parameters([1, 2, 3], json_serializer=lambda value: value)
