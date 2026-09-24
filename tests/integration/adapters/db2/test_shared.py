"""Shared IBM Db2 integration contracts."""

import os

import pytest

from tests.integration.adapters._shared import install_shared_tests

DB2_INTEGRATION_ENABLED = bool(
    os.environ.get("DB2_HOST") or os.environ.get("SQLSPEC_ENABLE_DB2_INTEGRATION_TESTS") == "1"
)

pytestmark = [
    pytest.mark.db2,
    pytest.mark.xdist_group("db2"),
    pytest.mark.skipif(
        not DB2_INTEGRATION_ENABLED,
        reason="Db2 integration tests require a live database; set DB2_HOST or SQLSPEC_ENABLE_DB2_INTEGRATION_TESTS=1",
    ),
]

install_shared_tests(globals(), "db2")
