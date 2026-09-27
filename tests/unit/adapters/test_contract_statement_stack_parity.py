"""Unit coverage for native-vs-fallback statement-stack parity registration."""

import pytest

from tests.integration.adapters._shared import behaviors
from tests.integration.adapters._shared._cases import DRIVER_CASES, get_driver_case
from tests.integration.adapters._shared.behaviors import STATEMENT_STACK_SCOPE, validate_extra_assertions

PARITY_PROOF_KEY = "statement_stack:native_fallback_parity"
OPTED_IN_CASE_IDS = ("psycopg-sync", "asyncpg-async", "psycopg-async", "oracledb-async")


def test_sync_statement_stack_parity_proof_registered() -> None:
    """The sync parity proof is registered under the statement-stack scope."""
    assert behaviors._SYNC_EXTRA_ASSERTIONS[PARITY_PROOF_KEY][0] == STATEMENT_STACK_SCOPE


def test_async_statement_stack_parity_proof_registered() -> None:
    """The async parity proof is registered under the statement-stack scope."""
    assert behaviors._ASYNC_EXTRA_ASSERTIONS[PARITY_PROOF_KEY][0] == STATEMENT_STACK_SCOPE


@pytest.mark.parametrize("case_id", OPTED_IN_CASE_IDS)
def test_native_stack_case_opts_into_parity_proof(case_id: str) -> None:
    """Every active native-stack adapter opts into the shared parity proof."""
    assert PARITY_PROOF_KEY in get_driver_case(case_id).extra_assertions


def test_every_case_extra_assertion_key_is_registered() -> None:
    """No driver case declares an unregistered proof key."""
    for case in DRIVER_CASES:
        validate_extra_assertions(case)
