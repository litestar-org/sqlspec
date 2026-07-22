"""Shared driver-feature contracts: registry scaffolds, source tripwires, and folded driver-specific behaviors."""

import importlib
from collections.abc import Sequence
from typing import cast

import pytest
from _pytest.mark.structures import ParameterSet

from tests.integration.adapters._shared._cases import (
    ASYNC_DRIVER_CASES,
    DRIVER_CASES,
    SYNC_DRIVER_CASES,
    DriverCase,
    DriverCaseContext,
    get_driver_case,
)
from tests.integration.adapters._shared._driver_type_system import (
    DRIVER_FEATURE_CONTRACT_GROUPS,
    SOURCE_EQUIVALENCE_CASES,
    SourceEquivalenceCase,
    assert_source_equivalent,
    get_driver_feature_consumed_keys,
    get_driver_feature_keys,
)
from tests.integration.adapters._shared.behaviors import (
    AsyncConfigFactory,
    SyncConfigFactory,
    assert_async_custom_type_adapters_contract,
    assert_async_driver_feature_parity_contract,
    assert_async_driver_feature_row_format_contract,
    assert_async_driver_features_contract,
    assert_sync_custom_type_adapters_contract,
    assert_sync_driver_feature_parity_contract,
    assert_sync_driver_feature_row_format_contract,
    assert_sync_driver_features_contract,
    validate_extra_assertions,
)

_DRIVER_TYPE_SYSTEM_MODULE = "tests.integration.adapters._shared._driver_type_system"


def test_driver_type_system_registry_module_exports_contract_groups() -> None:
    """The driver-type-system registry module is importable and exposes the expected contract groups."""
    module = importlib.import_module(_DRIVER_TYPE_SYSTEM_MODULE)

    assert hasattr(module, "DRIVER_FEATURE_CONTRACT_GROUPS")
    assert hasattr(module, "DRIVER_FEATURE_CONTRACT_ENABLED")
    assert hasattr(module, "DRIVER_FEATURE_CONSUMED_KEYS")
    assert hasattr(module, "SOURCE_EQUIVALENCE_CASES")
    assert hasattr(module, "SOURCE_EQUIVALENCE_ALLOWLIST")
    assert set(module.DRIVER_FEATURE_CONTRACT_GROUPS) == {
        "feature_honesty",
        "enable_false_semantics",
        "parity",
        "row_format",
    }
    assert module.DRIVER_FEATURE_CONTRACT_ENABLED == {
        group_name: group.enabled for group_name, group in module.DRIVER_FEATURE_CONTRACT_GROUPS.items()
    }
    assert set(module.DRIVER_FEATURE_CONSUMED_KEYS) == {case.adapter for case in DRIVER_CASES}
    assert {case.group for case in module.SOURCE_EQUIVALENCE_CASES} == {"mysql_four_way", "sqlite_pair", "mssql_pair"}


def _disabled_first_params(group_name: str, cases: Sequence[DriverCase]) -> tuple[ParameterSet, ...]:
    group = DRIVER_FEATURE_CONTRACT_GROUPS[group_name]
    params: list[ParameterSet] = []
    for case in cases:
        marks = list(case.marks)
        if not group.enabled or case.adapter not in group.enabled_adapters:
            marks.append(
                pytest.mark.skip(
                    reason=(
                        f"{group_name} scaffold is disabled until the adapter type-system registry is populated "
                        f"for {case.adapter}"
                    )
                )
            )
        params.append(pytest.param(case, id=case.id, marks=tuple(marks)))
    return tuple(params)


def _assert_driver_feature_honesty(adapter: str) -> None:
    declared = set(get_driver_feature_keys(adapter))
    consumed = set(get_driver_feature_consumed_keys(adapter))
    missing = sorted(declared - consumed)

    assert consumed, f"{adapter} has no consumed DriverFeatures registry entry"
    assert not missing, f"{adapter} declares unconsumed DriverFeatures keys: {missing!r}"


@pytest.mark.parametrize("adapter", DRIVER_FEATURE_CONTRACT_GROUPS["feature_honesty"].enabled_adapters)
def test_driver_feature_honesty_registry_covers_enabled_adapters(adapter: str) -> None:
    """Every feature-honesty-enabled adapter has an explicit consumed-key registry entry."""
    _assert_driver_feature_honesty(adapter)


@pytest.mark.parametrize("sync_driver_case", _disabled_first_params("feature_honesty", SYNC_DRIVER_CASES))
def test_sync_driver_feature_honesty_contract(sync_driver_case: DriverCase) -> None:
    """Sync DriverFeatures TypedDict keys are tracked as consumed after adapter migration."""
    _assert_driver_feature_honesty(sync_driver_case.adapter)


@pytest.mark.parametrize("async_driver_case", _disabled_first_params("feature_honesty", ASYNC_DRIVER_CASES))
async def test_async_driver_feature_honesty_contract(async_driver_case: DriverCase) -> None:
    """Async DriverFeatures TypedDict keys are tracked as consumed after adapter migration."""
    _assert_driver_feature_honesty(async_driver_case.adapter)


@pytest.mark.parametrize(
    "sync_driver_case",
    _disabled_first_params("enable_false_semantics", (get_driver_case("sqlite-sync"),)),
    indirect=True,
)
def test_sync_driver_feature_semantics_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync enable-false semantics stays inert unless the feature is explicitly enabled."""
    assert sync_driver_case.make_config is not None
    assert_sync_custom_type_adapters_contract(
        cast("SyncConfigFactory", sync_driver_case.make_config), sync_driver_case.case
    )


@pytest.mark.parametrize(
    "async_driver_case",
    _disabled_first_params("enable_false_semantics", (get_driver_case("aiosqlite-async"),)),
    indirect=True,
)
async def test_async_driver_feature_semantics_contract(async_driver_case: DriverCaseContext) -> None:
    """Async enable-false semantics stays inert unless the feature is explicitly enabled."""
    assert async_driver_case.make_config is not None
    await assert_async_custom_type_adapters_contract(
        cast("AsyncConfigFactory", async_driver_case.make_config), async_driver_case.case
    )


@pytest.mark.parametrize(
    "sync_driver_case",
    _disabled_first_params(
        "parity",
        tuple(
            case for case in SYNC_DRIVER_CASES if case.supports_arrow_streaming or case.supports_native_row_streaming
        ),
    ),
    indirect=True,
)
def test_sync_driver_feature_parity_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync buffered, streamed, and Arrow values stay equal for a canonical fixture."""
    assert_sync_driver_feature_parity_contract(sync_driver_case.driver, sync_driver_case.case)


@pytest.mark.parametrize(
    "async_driver_case",
    _disabled_first_params(
        "parity",
        tuple(
            case for case in ASYNC_DRIVER_CASES if case.supports_arrow_streaming or case.supports_native_row_streaming
        ),
    ),
    indirect=True,
)
async def test_async_driver_feature_parity_contract(async_driver_case: DriverCaseContext) -> None:
    """Async buffered, streamed, and Arrow values stay equal for a canonical fixture."""
    await assert_async_driver_feature_parity_contract(async_driver_case.driver, async_driver_case.case)


@pytest.mark.parametrize("sync_driver_case", _disabled_first_params("row_format", SYNC_DRIVER_CASES), indirect=True)
def test_sync_driver_feature_row_format_contract(sync_driver_case: DriverCaseContext) -> None:
    """Sync row-format tags match the actual configured row shape."""
    make_config = cast("SyncConfigFactory | None", sync_driver_case.make_config)
    assert_sync_driver_feature_row_format_contract(make_config, sync_driver_case.case)


@pytest.mark.parametrize("async_driver_case", _disabled_first_params("row_format", ASYNC_DRIVER_CASES), indirect=True)
async def test_async_driver_feature_row_format_contract(async_driver_case: DriverCaseContext) -> None:
    """Async row-format tags match the actual configured row shape."""
    make_config = cast("AsyncConfigFactory | None", async_driver_case.make_config)
    await assert_async_driver_feature_row_format_contract(make_config, async_driver_case.case)


@pytest.mark.parametrize("source_equivalence_case", SOURCE_EQUIVALENCE_CASES, ids=lambda case: case.case_id)
def test_driver_feature_source_equivalence(source_equivalence_case: SourceEquivalenceCase) -> None:
    """Duplicate helper implementations stay source-equivalent across adapter families."""
    assert_source_equivalent(source_equivalence_case)


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


def test_oracle_contract_declares_plsql_assertions() -> None:
    """Oracle keeps PL/SQL script execution covered in the shared contract matrix."""
    sync_case = get_driver_case("oracledb-sync")
    async_case = get_driver_case("oracledb-async")

    assert "driver_features:oracle_plsql" in sync_case.extra_assertions
    assert "driver_features:oracle_plsql" in async_case.extra_assertions
    validate_extra_assertions(sync_case)
    validate_extra_assertions(async_case)


def test_oracle_contract_declares_lob_fetch_matrix_assertions() -> None:
    """Oracle LOB fetch behavior stays in the shared contract matrix."""
    sync_case = get_driver_case("oracledb-sync")
    async_case = get_driver_case("oracledb-async")

    assert "oracle_lob_fetch:matrix" in sync_case.extra_assertions
    assert "oracle_lob_fetch:matrix" in async_case.extra_assertions
    validate_extra_assertions(sync_case)
    validate_extra_assertions(async_case)


def test_spanner_contract_status_defers_session_controls_explicitly() -> None:
    """Spanner remains deferred in the shared matrix until a safe active contract is available."""
    case = get_driver_case("spanner-sync")

    assert case.integration_status == "deferred"
    assert case.reason is not None
    assert "session controls" in case.reason
