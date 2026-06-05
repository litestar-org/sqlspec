"""Unit tests for the folded parameter-codec driver-specific proofs in the contract harness."""

import pytest

from tests.integration.adapters.contracts import behaviors
from tests.integration.adapters.contracts._cases import DRIVER_CASES, get_driver_case
from tests.integration.adapters.contracts.behaviors import PARAM_CODECS_SCOPE, validate_extra_assertions

SYNC_PROOF_KEYS = (
    "param_codecs:psycopg",
    "param_codecs:duckdb",
    "param_codecs:cockroach_psycopg",
    "param_codecs:mysql",
    "param_codecs:oracle",
)
ASYNC_PROOF_KEYS = (
    "param_codecs:asyncpg",
    "param_codecs:psqlpy",
    "param_codecs:psycopg",
    "param_codecs:cockroach_asyncpg",
    "param_codecs:cockroach_psycopg",
    "param_codecs:mysql",
    "param_codecs:oracle",
)

OPTED_IN = {
    "asyncpg-async": "param_codecs:asyncpg",
    "psqlpy-async": "param_codecs:psqlpy",
    "psycopg-sync": "param_codecs:psycopg",
    "psycopg-async": "param_codecs:psycopg",
    "duckdb-sync": "param_codecs:duckdb",
    "cockroach-asyncpg-async": "param_codecs:cockroach_asyncpg",
    "cockroach-psycopg-sync": "param_codecs:cockroach_psycopg",
    "cockroach-psycopg-async": "param_codecs:cockroach_psycopg",
    "mysqlconnector-sync": "param_codecs:mysql",
    "pymysql-sync": "param_codecs:mysql",
    "aiomysql-async": "param_codecs:mysql",
    "asyncmy-async": "param_codecs:mysql",
    "mysqlconnector-async": "param_codecs:mysql",
    "oracledb-sync": "param_codecs:oracle",
    "oracledb-async": "param_codecs:oracle",
}


@pytest.mark.parametrize("key", SYNC_PROOF_KEYS)
def test_sync_param_codecs_proof_registered(key: str) -> None:
    """Each dialect's sync parameter-codec proof is registered under the scope."""
    assert behaviors._SYNC_EXTRA_ASSERTIONS[key][0] == PARAM_CODECS_SCOPE


@pytest.mark.parametrize("key", ASYNC_PROOF_KEYS)
def test_async_param_codecs_proof_registered(key: str) -> None:
    """Each dialect's async parameter-codec proof is registered under the scope."""
    assert behaviors._ASYNC_EXTRA_ASSERTIONS[key][0] == PARAM_CODECS_SCOPE


@pytest.mark.parametrize(("case_id", "key"), list(OPTED_IN.items()))
def test_case_opts_into_param_codecs_proof(case_id: str, key: str) -> None:
    """The adapters whose per-adapter parameter_variants files are removed opt into the folded proof."""
    assert key in get_driver_case(case_id).extra_assertions


def test_every_case_extra_assertion_key_is_registered() -> None:
    """No driver case declares an extra_assertions key registered nowhere (no silent coverage loss)."""
    for case in DRIVER_CASES:
        validate_extra_assertions(case)
