"""Unit tests for the driver lifecycle (pooling / connection-hook) contract wiring."""

import pytest

from tests.integration.adapters.contracts._cases import DRIVER_CASES, get_driver_case

POOLING_CASES = ("sqlite-sync", "duckdb-sync", "aiosqlite-async")
CONNECTION_HOOK_CASES = ("sqlite-sync", "duckdb-sync", "aiosqlite-async")


@pytest.mark.parametrize("case_id", POOLING_CASES)
def test_pooling_case_declares_factory(case_id: str) -> None:
    """A case that opts into the pooling contract must provide a config factory fixture."""
    case = get_driver_case(case_id)
    assert case.supports_pooling
    assert case.config_factory_fixture is not None


@pytest.mark.parametrize("case_id", CONNECTION_HOOK_CASES)
def test_connection_hook_case_declares_factory(case_id: str) -> None:
    """A case that opts into the connection-hook contract must provide a config factory fixture."""
    case = get_driver_case(case_id)
    assert case.supports_connection_hook
    assert case.config_factory_fixture is not None


def test_lifecycle_flags_require_config_factory() -> None:
    """No case may declare pooling/connection-hook support without a config factory (no untestable claim)."""
    for case in DRIVER_CASES:
        if case.supports_pooling or case.supports_connection_hook:
            assert case.config_factory_fixture is not None, (
                f"{case.id} declares lifecycle support without a config_factory_fixture"
            )
