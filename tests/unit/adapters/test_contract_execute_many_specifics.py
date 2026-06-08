"""Unit tests for the folded execute_many driver-specific proofs in the contract harness."""

import pytest

from tests.integration.adapters.contracts import behaviors
from tests.integration.adapters.contracts._cases import DRIVER_CASES, get_driver_case
from tests.integration.adapters.contracts.behaviors import EXECUTE_MANY_SPECIFICS_SCOPE, validate_extra_assertions

SYNC_PROOF_KEYS = ("execute_many_specifics:postgres", "execute_many_specifics:duckdb")
ASYNC_PROOF_KEYS = ("execute_many_specifics:postgres",)

OPTED_IN = {
    "psycopg-sync": "execute_many_specifics:postgres",
    "asyncpg-async": "execute_many_specifics:postgres",
    "duckdb-sync": "execute_many_specifics:duckdb",
}


@pytest.mark.parametrize("key", SYNC_PROOF_KEYS)
def test_sync_execute_many_proof_registered(key: str) -> None:
    """Each dialect's sync execute_many-specifics proof is registered under the scope."""
    assert behaviors._SYNC_EXTRA_ASSERTIONS[key][0] == EXECUTE_MANY_SPECIFICS_SCOPE


@pytest.mark.parametrize("key", ASYNC_PROOF_KEYS)
def test_async_execute_many_proof_registered(key: str) -> None:
    """Each dialect's async execute_many-specifics proof is registered under the scope."""
    assert behaviors._ASYNC_EXTRA_ASSERTIONS[key][0] == EXECUTE_MANY_SPECIFICS_SCOPE


@pytest.mark.parametrize(("case_id", "key"), list(OPTED_IN.items()))
def test_case_opts_into_execute_many_proof(case_id: str, key: str) -> None:
    """The adapters whose per-adapter execute_many files are removed opt into the folded proof."""
    assert key in get_driver_case(case_id).extra_assertions


def test_every_case_extra_assertion_key_is_registered() -> None:
    """No driver case declares an extra_assertions key registered nowhere (no silent coverage loss)."""
    for case in DRIVER_CASES:
        validate_extra_assertions(case)
