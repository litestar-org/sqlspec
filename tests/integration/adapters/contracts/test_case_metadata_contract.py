"""Contract metadata guardrails for adapter case records."""

from typing import cast

from _pytest.mark.structures import ParameterSet

from tests.integration.adapters.contracts._adk_cases import AdkStoreCase, adk_store_params_with
from tests.integration.adapters.contracts._cases import (
    ACTIVE_DRIVER_CASES,
    ASYNC_LIFECYCLE_DRIVER_PARAMS,
    LIFECYCLE_CAPABILITIES,
    SYNC_LIFECYCLE_DRIVER_PARAMS,
    DriverCase,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters.contracts._inputs import (
    PARAMETER_STYLE_EXECUTE_MANY_PARAMS,
    PARAMETER_STYLE_EXECUTE_PARAMS,
    PARAMETER_STYLE_PARAMS,
    ParameterStyleCase,
)


def _driver_cases(params: tuple[ParameterSet, ...]) -> tuple[DriverCase, ...]:
    return tuple(cast("DriverCase", param.values[0]) for param in params)


def _adk_cases(params: tuple[ParameterSet, ...]) -> tuple[AdkStoreCase, ...]:
    return tuple(cast("AdkStoreCase", param.values[0]) for param in params)


def _parameter_style_cases(params: tuple[ParameterSet, ...]) -> tuple[ParameterStyleCase, ...]:
    return tuple(cast("ParameterStyleCase", param.values[0]) for param in params)


def test_active_driver_cases_do_not_use_string_deviations() -> None:
    """Active cases use typed capability metadata instead of broad string deviations."""
    cases_with_deviations = {case.id: case.deviations for case in ACTIVE_DRIVER_CASES if case.deviations}
    assert cases_with_deviations == {}


def test_lifecycle_params_are_capability_filtered() -> None:
    """Lifecycle contract params collect only cases that can run at least one lifecycle behavior."""
    sync_cases = _driver_cases(SYNC_LIFECYCLE_DRIVER_PARAMS)
    async_cases = _driver_cases(ASYNC_LIFECYCLE_DRIVER_PARAMS)
    assert all(any(getattr(case, name) for name in LIFECYCLE_CAPABILITIES) for case in sync_cases)
    assert all(any(getattr(case, name) for name in LIFECYCLE_CAPABILITIES) for case in async_cases)


def test_capability_params_match_requested_capability() -> None:
    """Capability-filtered params collect only cases that declare the requested capability."""
    for capability_name in (
        *LIFECYCLE_CAPABILITIES,
        "supports_arrow",
        "supports_exception_translation",
        "supports_execute_many",
        "supports_for_update",
        "supports_grouped_subquery",
        "supports_native_bulk_ingest",
        "supports_native_metadata",
        "supports_storage_bridge",
    ):
        sync_cases = _driver_cases(sync_driver_params_with(capability_name))
        async_cases = _driver_cases(async_driver_params_with(capability_name))
        assert all(getattr(case, capability_name) for case in sync_cases)
        assert all(getattr(case, capability_name) for case in async_cases)


def test_adk_capability_params_match_requested_capability() -> None:
    """ADK capability-filtered params collect only cases that declare the requested capability."""
    cases = _adk_cases(adk_store_params_with("supports_atomic_state_update"))
    assert all(case.supports_atomic_state_update for case in cases)


def test_parameter_style_params_are_method_filtered() -> None:
    """Parameter-style params separate execute and execute_many cases before driver selection."""
    execute_cases = _parameter_style_cases(PARAMETER_STYLE_EXECUTE_PARAMS)
    execute_many_cases = _parameter_style_cases(PARAMETER_STYLE_EXECUTE_MANY_PARAMS)
    all_cases = _parameter_style_cases(PARAMETER_STYLE_PARAMS)
    assert all(case.method == "execute" for case in execute_cases)
    assert all(case.method == "execute_many" for case in execute_many_cases)
    assert {case.id for case in execute_cases} | {case.id for case in execute_many_cases} == {
        case.id for case in all_cases
    }
