"""Proof that the additive extra_assertions hook is wired into a real contract behavior.

The driver-basics contract is the canonical proof site: it runs for every adapter,
so a no-op proof key opted into by SQLite demonstrates the hook end-to-end without a
separate per-adapter file.
"""

from dataclasses import replace

from tests.integration.adapters.contracts import behaviors
from tests.integration.adapters.contracts._cases import get_driver_case
from tests.integration.adapters.contracts.behaviors import (
    DRIVER_BASICS_PROOF_KEY,
    DRIVER_BASICS_SCOPE,
    assert_sync_driver_basics_contract,
    register_sync_extra_assertion,
)


def test_noop_proof_key_registered_in_both_registries() -> None:
    """The demonstration no-op proof key is registered for sync and async basics."""
    assert behaviors._SYNC_EXTRA_ASSERTIONS[DRIVER_BASICS_PROOF_KEY][0] == DRIVER_BASICS_SCOPE
    assert behaviors._ASYNC_EXTRA_ASSERTIONS[DRIVER_BASICS_PROOF_KEY][0] == DRIVER_BASICS_SCOPE


def test_sqlite_case_opts_into_noop_proof() -> None:
    """The wired SQLite case carries the no-op proof key, exercising the hook in the live matrix."""
    assert DRIVER_BASICS_PROOF_KEY in get_driver_case("sqlite-sync").extra_assertions
    assert DRIVER_BASICS_PROOF_KEY in get_driver_case("aiosqlite-async").extra_assertions


def test_basics_contract_dispatches_extra_assertions(contract_sqlite_driver: object) -> None:
    """Running the sync basics contract dispatches proofs owned by the driver-basics scope."""
    ran: list[object] = []
    key = "driver_basics:spy-proof"
    register_sync_extra_assertion(key, DRIVER_BASICS_SCOPE, lambda driver, case: ran.append(driver))
    try:
        case = replace(get_driver_case("sqlite-sync"), extra_assertions=(key,))
        assert_sync_driver_basics_contract(contract_sqlite_driver, case)
        assert ran == [contract_sqlite_driver]
    finally:
        behaviors._SYNC_EXTRA_ASSERTIONS.pop(key, None)
