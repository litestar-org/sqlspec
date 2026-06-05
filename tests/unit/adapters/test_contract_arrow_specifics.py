"""Unit tests for the folded Arrow driver-specific proofs in the contract harness."""

import pytest

from tests.integration.adapters.contracts import behaviors
from tests.integration.adapters.contracts._cases import DRIVER_CASES, get_driver_case
from tests.integration.adapters.contracts.behaviors import ARROW_SPECIFICS_SCOPE, validate_extra_assertions

SYNC_PROOF_KEYS = ("arrow_specifics:duckdb", "arrow_specifics:postgres")
ASYNC_PROOF_KEYS = ("arrow_specifics:sqlite", "arrow_specifics:mysql", "arrow_specifics:postgres", "arrow_specifics:oracle")

OPTED_IN = {
    "aiosqlite-async": "arrow_specifics:sqlite",
    "aiomysql-async": "arrow_specifics:mysql",
    "asyncmy-async": "arrow_specifics:mysql",
    "duckdb-sync": "arrow_specifics:duckdb",
    "psycopg-sync": "arrow_specifics:postgres",
    "psycopg-async": "arrow_specifics:postgres",
    "asyncpg-async": "arrow_specifics:postgres",
    "psqlpy-async": "arrow_specifics:postgres",
    "oracledb-async": "arrow_specifics:oracle",
}


@pytest.mark.parametrize("key", SYNC_PROOF_KEYS)
def test_sync_arrow_proof_registered(key: str) -> None:
    """Each dialect's sync Arrow-specifics proof is registered under the arrow_specifics scope."""
    assert behaviors._SYNC_EXTRA_ASSERTIONS[key][0] == ARROW_SPECIFICS_SCOPE


@pytest.mark.parametrize("key", ASYNC_PROOF_KEYS)
def test_async_arrow_proof_registered(key: str) -> None:
    """Each dialect's async Arrow-specifics proof is registered under the arrow_specifics scope."""
    assert behaviors._ASYNC_EXTRA_ASSERTIONS[key][0] == ARROW_SPECIFICS_SCOPE


@pytest.mark.parametrize(("case_id", "key"), list(OPTED_IN.items()))
def test_case_opts_into_arrow_proof(case_id: str, key: str) -> None:
    """The adapters whose per-adapter Arrow files are removed opt into the folded proof."""
    assert key in get_driver_case(case_id).extra_assertions


def test_every_case_extra_assertion_key_is_registered() -> None:
    """No driver case declares an extra_assertions key registered nowhere (no silent coverage loss)."""
    for case in DRIVER_CASES:
        validate_extra_assertions(case)
