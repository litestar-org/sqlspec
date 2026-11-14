import asyncio
from collections.abc import AsyncGenerator, Awaitable, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, cast

import pytest

from sqlspec.config import NoPoolAsyncConfig, NoPoolSyncConfig, ReadReplicaGroup
from sqlspec.exceptions import (
    DatabaseConnectionError,
    ImproperConfigurationError,
    OperationalError,
    ReplicaUnavailableError,
)


class SyncEndpoint(NoPoolSyncConfig[Any, Any]):
    __slots__ = ("events",)

    def __init__(self, **kwargs: Any) -> None:
        self.events: list[object] = []
        super().__init__(**kwargs)

    def close_pool(self) -> None:
        self.events.append("close")
        if self.connection_config.get("fail_close"):
            raise OSError("close failed")

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Generator[Any]:
        self.events.append(("enter", kwargs))
        if self.connection_config.get("fail"):
            raise OSError("endpoint unavailable")
        try:
            yield self.connection_config.get("dsn", "endpoint")
        except BaseException as exc:
            self.events.append(type(exc))
            raise
        finally:
            self.events.append("exit")


class AsyncEndpoint(NoPoolAsyncConfig[Any, Any]):
    __slots__ = ("events",)

    def __init__(self, **kwargs: Any) -> None:
        self.events: list[object] = []
        super().__init__(**kwargs)

    async def close_pool(self) -> None:
        self.events.append("close")
        if self.connection_config.get("fail_close"):
            raise OSError("close failed")

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncGenerator[Any]:
        self.events.append(("enter", kwargs))
        if self.connection_config.get("fail"):
            raise OSError("endpoint unavailable")
        try:
            yield self.connection_config.get("dsn", "endpoint")
        except BaseException as exc:
            self.events.append(type(exc))
            raise
        finally:
            self.events.append("exit")


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
async def test_group_failover_is_limited_to_checkout(endpoint_type: Any) -> None:
    failed = endpoint_type(connection_config={"fail": True})
    ready = endpoint_type(connection_config={"dsn": "ready"})
    group = ReadReplicaGroup([failed, ready], failure_threshold=1, fallback_to_primary=False)
    context = group.provide_session(tenant="acme", bypass_tenant=False)
    with pytest.raises(ValueError, match="caller failure"):
        if group.is_async:
            async with context as session:
                assert session == "ready"
                raise ValueError("caller failure")
        else:
            with context as session:
                assert session == "ready"
                raise ValueError("caller failure")
    assert len(failed.events) == 1
    assert ready.events == [
        ("enter", {"tenant": "acme", "bypass_tenant": False, "statement_config": None}),
        ValueError,
        "exit",
    ]
    assert not group.breaker.is_available(0)


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
async def test_group_rejects_stale_replica_without_poisoning_health(endpoint_type: Any) -> None:
    stale = endpoint_type(connection_config={"dsn": "stale"})
    ready = endpoint_type(connection_config={"dsn": "ready"})
    group = ReadReplicaGroup([stale, ready], causal_validator=lambda session, token: session == token)
    context = group.provide_session(causal_token="ready")
    if group.is_async:
        async with context as session:
            assert session == "ready"
    else:
        with context as session:
            assert session == "ready"
    assert stale.events[-1] == "exit"
    assert group.breaker.is_available(0)


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
async def test_group_falls_back_to_primary_or_reports_exhaustion(endpoint_type: Any) -> None:
    failed = endpoint_type(connection_config={"fail": True})
    primary = endpoint_type(connection_config={"dsn": "primary"})
    group = ReadReplicaGroup([failed], failure_threshold=1)
    context = group.provide_session(primary=primary)
    if group.is_async:
        async with context as session:
            assert session == "primary"
    else:
        with context as session:
            assert session == "primary"
    with pytest.raises(ReplicaUnavailableError):
        if group.is_async:
            async with group.provide_session():
                pytest.fail("No eligible endpoint can enter")
        else:
            with group.provide_session():
                pytest.fail("No eligible endpoint can enter")


async def test_async_validator_cancellation_unwinds_lease_and_releases_probe() -> None:
    started = asyncio.Event()
    wait = asyncio.Event()

    async def validator(session: Any, token: str) -> bool:
        started.set()
        await wait.wait()
        return True

    endpoint = AsyncEndpoint()
    now = [0.0]
    group = ReadReplicaGroup(
        [endpoint], causal_validator=validator, failure_threshold=1, recovery_seconds=1, clock=lambda: now[0]
    )
    group.breaker.record_failure(0)
    now[0] = 2.0

    async def checkout() -> None:
        async with group.provide_session(causal_token="after-write"):
            pytest.fail("Validation has not completed")

    task = asyncio.create_task(checkout())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert endpoint.events[-2:] == [asyncio.CancelledError, "exit"]
    assert group.breaker.try_acquire_probe(0)
    group.breaker.release_probe(0)


def test_sync_group_rejects_async_validator_and_cleans_up() -> None:
    async def validator(session: Any, token: str) -> bool:
        return True

    endpoint = SyncEndpoint()
    group = ReadReplicaGroup([endpoint], causal_validator=validator)
    with pytest.raises(ImproperConfigurationError, match="async causal validator"):
        with group.provide_session(causal_token="token"):
            pytest.fail("A sync group cannot await validation")
    assert endpoint.events[-2:] == [ImproperConfigurationError, "exit"]


@pytest.mark.parametrize("configs", [[], [SyncEndpoint(), AsyncEndpoint()], ["not a config"]])
def test_group_rejects_invalid_topologies(configs: Any) -> None:
    with pytest.raises(ImproperConfigurationError):
        ReadReplicaGroup(configs)


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
async def test_validator_error_unwinds_actual_exception_without_retry(endpoint_type: Any) -> None:
    first = endpoint_type()
    second = endpoint_type()

    def validator(session: Any, token: str) -> bool:
        raise ValueError("validator failed")

    group = ReadReplicaGroup([first, second], causal_validator=validator)
    with pytest.raises(ValueError, match="validator failed"):
        if group.is_async:
            async with group.provide_session(causal_token="token"):
                pytest.fail("Validator errors cannot enter the caller")
        else:
            with group.provide_session(causal_token="token"):
                pytest.fail("Validator errors cannot enter the caller")
    assert first.events[-2:] == [ValueError, "exit"]
    assert second.events == []
    assert group.breaker.is_available(0)


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
async def test_close_attempts_every_replica_before_propagating_error(endpoint_type: Any) -> None:
    failed = endpoint_type(connection_config={"fail_close": True})
    ready = endpoint_type()
    group = ReadReplicaGroup([failed, ready])
    with pytest.raises(OSError, match="close failed"):
        if group.is_async:
            await cast("Awaitable[None]", group.close_pools())
        else:
            group.close_pools()
    assert failed.events == ["close"]
    assert ready.events == ["close"]


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
async def test_no_pool_primary_cascades_replica_create_and_close(endpoint_type: Any) -> None:
    first = endpoint_type(connection_config={"fail_close": True})
    second = endpoint_type()
    primary = endpoint_type(replicas=[first, second])
    if primary.is_async:
        await primary.create_pool()
        with pytest.raises(OSError, match="close failed"):
            await NoPoolAsyncConfig.close_pool(primary)
    else:
        primary.create_pool()
        with pytest.raises(OSError, match="close failed"):
            NoPoolSyncConfig.close_pool(primary)
    assert first.events == ["close"]
    assert second.events == ["close"]


class _StrictSyncPrimary(SyncEndpoint):
    __slots__ = ()

    def _allow_replica_fallback(self, _error: Exception) -> bool:
        return False


class _StrictAsyncPrimary(AsyncEndpoint):
    __slots__ = ()

    def _allow_replica_fallback(self, _error: Exception) -> bool:
        return False


@pytest.mark.parametrize(
    ("endpoint_type", "primary_type"), [(SyncEndpoint, _StrictSyncPrimary), (AsyncEndpoint, _StrictAsyncPrimary)]
)
async def test_primary_may_refuse_checkout_fallback_without_losing_original_error(
    endpoint_type: Any, primary_type: Any
) -> None:
    failed = endpoint_type(connection_config={"fail": True})
    primary = primary_type(connection_config={"dsn": "primary"}, replicas=[failed])
    with pytest.raises(OSError, match="endpoint unavailable"):
        if primary.is_async:
            async with primary.provide_read_session():
                pytest.fail("failed checkout must not yield")
        else:
            with primary.provide_read_session():
                pytest.fail("failed checkout must not yield")
    assert primary.events == []


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
@pytest.mark.parametrize("exc_type", [DatabaseConnectionError, OperationalError, OSError, TimeoutError])
async def test_causal_validator_transient_failure_fails_over_and_trips_breaker(
    endpoint_type: Any, exc_type: type[Exception]
) -> None:
    failing = endpoint_type(connection_config={"dsn": "failing"})
    ready = endpoint_type(connection_config={"dsn": "ready"})

    def sync_validator(session: Any, _token: str) -> bool:
        if session == "failing":
            raise exc_type("replica unreachable during causal check")
        return True

    async def async_validator(session: Any, _token: str) -> bool:
        if session == "failing":
            raise exc_type("replica unreachable during causal check")
        return True

    group = ReadReplicaGroup(
        [failing, ready],
        failure_threshold=1,
        fallback_to_primary=False,
        causal_validator=async_validator if endpoint_type is AsyncEndpoint else sync_validator,
    )
    context = group.provide_session(causal_token="lsn-10")
    if group.is_async:
        async with context as session:
            assert session == "ready"
    else:
        with context as session:
            assert session == "ready"
    assert failing.events[-2:] == [exc_type, "exit"]
    assert not group.breaker.is_available(0)
    assert group.breaker.is_available(1)


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
@pytest.mark.parametrize("fallback", [False, True])
async def test_causal_validator_exhaustion_falls_back_or_raises_with_cause(endpoint_type: Any, fallback: bool) -> None:
    failing = endpoint_type(connection_config={"dsn": "failing"})
    primary = endpoint_type(connection_config={"dsn": "primary"})
    causal_error = OperationalError("replication check failed")

    def validator(_session: Any, _token: str) -> bool:
        raise causal_error

    group = ReadReplicaGroup([failing], failure_threshold=1, fallback_to_primary=fallback, causal_validator=validator)
    if fallback:
        context = group.provide_session(primary=primary, causal_token="lsn-11")
        if group.is_async:
            async with context as session:
                assert session == "primary"
        else:
            with context as session:
                assert session == "primary"
    else:
        with pytest.raises(ReplicaUnavailableError) as failure:
            if group.is_async:
                async with group.provide_session(primary=primary, causal_token="lsn-11"):
                    pytest.fail("exhausted causal failover must not yield")
            else:
                with group.provide_session(primary=primary, causal_token="lsn-11"):
                    pytest.fail("exhausted causal failover must not yield")
        assert failure.value.__cause__ is causal_error
    assert failing.events[-2:] == [OperationalError, "exit"]
    assert not group.breaker.is_available(0)


@pytest.mark.parametrize("endpoint_type", [SyncEndpoint, AsyncEndpoint])
async def test_causal_validator_failure_reopens_half_open_probe(endpoint_type: Any) -> None:
    now = [0.0]
    endpoint = endpoint_type(connection_config={"dsn": "probe"})

    def validator(_session: Any, _token: str) -> bool:
        raise TimeoutError("probe timed out")

    group = ReadReplicaGroup(
        [endpoint],
        failure_threshold=1,
        recovery_seconds=5.0,
        fallback_to_primary=False,
        causal_validator=validator,
        clock=lambda: now[0],
    )
    group.breaker.record_failure(0)
    assert not group.breaker.is_available(0)
    now[0] = 6.0
    assert group.breaker.is_available(0)
    with pytest.raises(ReplicaUnavailableError) as failure:
        if group.is_async:
            async with group.provide_session(causal_token="lsn-12"):
                pytest.fail("failed half-open probe must not yield")
        else:
            with group.provide_session(causal_token="lsn-12"):
                pytest.fail("failed half-open probe must not yield")
    assert isinstance(failure.value.__cause__, TimeoutError)
    assert not group.breaker.is_available(0)
    now[0] = 12.0
    assert group.breaker.try_acquire_probe(0)
    group.breaker.release_probe(0)


@pytest.mark.parametrize(
    ("endpoint_type", "primary_type"), [(SyncEndpoint, _StrictSyncPrimary), (AsyncEndpoint, _StrictAsyncPrimary)]
)
async def test_causal_validator_failure_respects_primary_fallback_policy(endpoint_type: Any, primary_type: Any) -> None:
    failing = endpoint_type(connection_config={"dsn": "failing"})
    primary = primary_type(connection_config={"dsn": "primary"})

    def validator(_session: Any, _token: str) -> bool:
        raise DatabaseConnectionError("lag query disconnected")

    group = ReadReplicaGroup([failing], failure_threshold=1, causal_validator=validator)
    with pytest.raises(DatabaseConnectionError, match="lag query disconnected"):
        if group.is_async:
            async with group.provide_session(primary=primary, causal_token="lsn-13"):
                pytest.fail("strict primary must reject fallback")
        else:
            with group.provide_session(primary=primary, causal_token="lsn-13"):
                pytest.fail("strict primary must reject fallback")
    assert primary.events == []
