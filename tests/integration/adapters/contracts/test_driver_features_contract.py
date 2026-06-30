"""Shared driver-feature contracts: folded driver-specific behaviors (COPY, SET-variable, native types)."""

from tests.integration.adapters.contracts._cases import DriverCaseContext, get_driver_case
from tests.integration.adapters.contracts.behaviors import (
    assert_async_driver_features_contract,
    assert_sync_driver_features_contract,
    validate_extra_assertions,
)


def test_sync_driver_features_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync drivers run folded driver-feature proofs opted in via extra_assertions."""
    assert_sync_driver_features_contract(sync_driver_case.driver, sync_driver_case.case)


async def test_async_driver_features_contract(async_driver_case: DriverCaseContext) -> None:
    """Async drivers run folded driver-feature proofs opted in via extra_assertions."""
    await assert_async_driver_features_contract(async_driver_case.driver, async_driver_case.case)


def test_bigquery_contract_declares_job_control_assertion() -> None:
    """BigQuery keeps job-control wiring covered in the shared contract matrix."""
    case = get_driver_case("bigquery-sync")

    assert "driver_features:bigquery_job_controls" in case.extra_assertions


def test_oracle_contract_declares_batch_error_assertions() -> None:
    """Oracle keeps batch-errors metadata covered in the shared contract matrix."""
    sync_case = get_driver_case("oracledb-sync")
    async_case = get_driver_case("oracledb-async")

    assert "driver_features:oracle_batch_errors" in sync_case.extra_assertions
    assert "driver_features:oracle_batch_errors" in async_case.extra_assertions
    validate_extra_assertions(sync_case)
    validate_extra_assertions(async_case)


def test_spanner_contract_status_defers_session_controls_explicitly() -> None:
    """Spanner remains deferred in the shared matrix until a safe active contract is available."""
    case = get_driver_case("spanner-sync")

    assert case.integration_status == "deferred"
    assert case.reason is not None
    assert "session controls" in case.reason
