"""Integration tests for sqlspec adapters."""

import pytest

pytestmark = [pytest.mark.xdist_group("duckdb"), pytest.mark.duckdb, pytest.mark.duckdb_driver]
