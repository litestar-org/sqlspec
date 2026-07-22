"""Unit tests for the folded EXPLAIN-modifier proofs in the contract harness."""

import pytest

from tests.integration.adapters._shared import behaviors
from tests.integration.adapters._shared._cases import DRIVER_CASES, get_driver_case
from tests.integration.adapters._shared.behaviors import EXPLAIN_MODIFIERS_SCOPE, validate_extra_assertions

SYNC_PROOF_KEYS = (
    "explain_modifiers:postgres",
    "explain_modifiers:mysql",
    "explain_modifiers:duckdb",
    "explain_modifiers:oracle",
)
ASYNC_PROOF_KEYS = ("explain_modifiers:postgres", "explain_modifiers:mysql", "explain_modifiers:oracle")

OPTED_IN = {
    "duckdb-sync": "explain_modifiers:duckdb",
    "psycopg-sync": "explain_modifiers:postgres",
    "psycopg-async": "explain_modifiers:postgres",
    "asyncpg-async": "explain_modifiers:postgres",
    "psqlpy-async": "explain_modifiers:postgres",
    "aiomysql-async": "explain_modifiers:mysql",
    "asyncmy-async": "explain_modifiers:mysql",
    "oracledb-sync": "explain_modifiers:oracle",
    "oracledb-async": "explain_modifiers:oracle",
}


@pytest.mark.parametrize("key", SYNC_PROOF_KEYS)
def test_sync_modifier_proof_registered(key: str) -> None:
    """Each dialect's sync EXPLAIN-modifier proof is registered under the explain_modifiers scope."""
    assert behaviors._SYNC_EXTRA_ASSERTIONS[key][0] == EXPLAIN_MODIFIERS_SCOPE


@pytest.mark.parametrize("key", ASYNC_PROOF_KEYS)
def test_async_modifier_proof_registered(key: str) -> None:
    """Each dialect's async EXPLAIN-modifier proof is registered under the explain_modifiers scope."""
    assert behaviors._ASYNC_EXTRA_ASSERTIONS[key][0] == EXPLAIN_MODIFIERS_SCOPE


@pytest.mark.parametrize(("case_id", "key"), list(OPTED_IN.items()))
def test_case_opts_into_modifier_proof(case_id: str, key: str) -> None:
    """The adapters whose per-adapter EXPLAIN files are removed opt into the folded proof."""
    assert key in get_driver_case(case_id).extra_assertions


def test_every_case_extra_assertion_key_is_registered() -> None:
    """No driver case declares an extra_assertions key registered nowhere (no silent coverage loss)."""
    for case in DRIVER_CASES:
        validate_extra_assertions(case)
