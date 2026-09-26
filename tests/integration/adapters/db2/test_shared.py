"""Shared IBM Db2 integration contracts."""

import pytest

from tests.integration.adapters._shared import install_shared_tests

pytestmark = [pytest.mark.db2, pytest.mark.xdist_group("db2")]

install_shared_tests(globals(), "db2")
