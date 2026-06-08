"""Unit tests for the additive contract capability flags on DriverCase."""

import pytest

from tests.integration.adapters.contracts._cases import DriverCase

NEW_CAPABILITY_FLAGS = (
    "supports_lob",
    "supports_native_array_codec",
    "supports_json_native",
    "supports_merge",
    "supports_copy",
    "supports_pooling",
    "supports_multi_schema_migrations",
    "supports_data_dictionary",
)


@pytest.fixture
def bare_case() -> DriverCase:
    return DriverCase(id="bare", fixture_name="f", adapter="a", dialect="sqlite", mode="sync")


@pytest.mark.parametrize("flag", NEW_CAPABILITY_FLAGS)
def test_new_capability_flag_defaults_false(bare_case: DriverCase, flag: str) -> None:
    """Every new capability flag is additive and opt-in (defaults False), so opting into nothing is a no-op."""
    assert getattr(bare_case, flag) is False


def test_existing_vector_flag_still_present(bare_case: DriverCase) -> None:
    """The pre-existing supports_vector flag is retained alongside the new flags."""
    assert bare_case.supports_vector is False


def test_new_capability_flags_opt_in_independently() -> None:
    """Each new capability can be explicitly enabled on a case."""
    case = DriverCase(
        id="opt",
        fixture_name="f",
        adapter="a",
        dialect="sqlite",
        mode="sync",
        supports_lob=True,
        supports_native_array_codec=True,
        supports_json_native=True,
        supports_merge=True,
        supports_copy=True,
        supports_pooling=True,
        supports_multi_schema_migrations=True,
        supports_data_dictionary=True,
    )
    for flag in NEW_CAPABILITY_FLAGS:
        assert getattr(case, flag) is True
