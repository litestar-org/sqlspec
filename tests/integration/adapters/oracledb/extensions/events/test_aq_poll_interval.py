"""Regression test for Oracle AQ backend poll_interval handling."""

import time
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import SQLSpec
from sqlspec.adapters.oracledb import OracleSyncConfig

pytestmark = pytest.mark.xdist_group("oracle")

_AQ_WAIT_SECONDS = 10
_POLL_INTERVAL = 0.5
_LATENCY_TOLERANCE = 4.0


@pytest.fixture
def oracle_aq_poll_config(
    provision_classic_aq: "Callable[..., AbstractContextManager[None]]", oracle_23ai_service: OracleService
) -> Generator[OracleSyncConfig, None, None]:
    """Provision Oracle config with a high aq_wait_seconds and a live AQ queue."""

    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        },
        extension_config={"events": {"backend": "advanced_queue", "aq_wait_seconds": _AQ_WAIT_SECONDS}},
    )

    with provision_classic_aq():
        try:
            yield config
        finally:
            config.close_pool()


def test_oracle_aq_dequeue_honors_caller_poll_interval(oracle_aq_poll_config: OracleSyncConfig) -> None:
    """dequeue() should return within poll_interval seconds, not aq_wait_seconds.

    The current backend passes settings.aq_wait_seconds straight to options.wait, ignoring
    the caller's poll_interval. A caller asking for a 0.5s heartbeat should not block for
    10s. Once the redesign respects min(poll_interval, aq_wait_seconds), this becomes
    cheap.
    """

    spec = SQLSpec()
    spec.add_config(oracle_aq_poll_config)
    channel = spec.event_channel(oracle_aq_poll_config)

    start = time.monotonic()
    # Pull one timeout cycle — there are no enqueued messages so dequeue should return None
    # promptly. We rely on the channel's internal poll loop returning to the caller within
    # roughly poll_interval (the loop continues on None, so we'd see this in latency between
    # iterations if dequeue blocked for aq_wait_seconds instead).
    backend = channel._backend  # pyright: ignore[reportPrivateUsage]
    elapsed_first = _measure_single_dequeue(backend, "aq_poll_chan", _POLL_INTERVAL)

    elapsed_total = time.monotonic() - start
    assert elapsed_first <= _LATENCY_TOLERANCE, (
        f"dequeue blocked for {elapsed_first:.2f}s with poll_interval={_POLL_INTERVAL}s "
        f"(aq_wait_seconds={_AQ_WAIT_SECONDS}) — poll_interval was ignored"
    )
    assert elapsed_total <= _LATENCY_TOLERANCE + 1.0


def _measure_single_dequeue(backend: object, channel: str, poll_interval: float) -> float:
    """Time a single backend.dequeue() call directly to expose poll_interval drift."""
    start = time.monotonic()
    backend.dequeue(channel, poll_interval)  # type: ignore[attr-defined]
    return time.monotonic() - start
