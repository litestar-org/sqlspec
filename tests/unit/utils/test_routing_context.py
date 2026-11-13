"""Routing scope isolation and ASGI hint precedence."""

import asyncio
from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from sqlspec import sign_write_pin, verify_write_pin
from sqlspec.exceptions import ImproperConfigurationError, RoutingContextError
from sqlspec.utils.routing import RoutingContext, RoutingContextPayload, RoutingEntry, SQLSpecRoutingMiddleware


@pytest.mark.parametrize(
    "kwargs",
    [
        {"mode": "invalid"},
        {"mode": "primary", "bind_group": "a"},
        {"bind_group": ""},
        {"bind_group": "reporting\r\nx-injected: 1"},
        {"bind_group": "a" * 257},
        {"causal_token": ""},
        {"causal_token": "lsn\x00evil"},
        {"causal_token": "t" * 257},
        {"max_staleness_seconds": -1},
        {"max_staleness_seconds": float("nan")},
    ],
)
def test_invalid_routing_directives(kwargs: dict[str, Any]) -> None:
    with pytest.raises(RoutingContextError):
        RoutingEntry(**kwargs)


def test_nested_scopes_and_write_pin_preserve_metadata() -> None:
    assert RoutingContext.mark_write("billing") is False
    assert RoutingContext.current() is None
    with RoutingContext.replica(bind_group="reporting", causal_token="a", max_staleness_seconds=2):
        outer = cast(RoutingContextPayload, RoutingContext.current())
        with RoutingContext.primary("billing"):
            assert cast(RoutingEntry, RoutingContext.get_entry("billing")).mode == "primary"
            assert cast(RoutingEntry, RoutingContext.get_entry("other")).bind_group == "reporting"
        assert RoutingContext.current() is outer
        RoutingContext.mark_write("orders", causal_token="b")
        entry = cast(RoutingEntry, RoutingContext.get_entry("orders"))
        assert (
            entry.mode,
            entry.pinned_by_write,
            entry.bind_group,
            entry.causal_token,
            entry.max_staleness_seconds,
        ) == ("primary", True, "reporting", "b", 2)
        assert cast(RoutingEntry, outer.get("orders")).mode == "replica"
    assert RoutingContext.current() is None


async def test_child_task_write_does_not_pin_parent() -> None:
    async with RoutingContext.auto():

        async def child() -> str:
            RoutingContext.mark_write()
            await asyncio.sleep(0)
            return cast(RoutingEntry, RoutingContext.get_entry()).mode

        assert await asyncio.create_task(child()) == "primary"
        assert cast(RoutingEntry, RoutingContext.get_entry()).mode == "auto"
    assert RoutingContext.current() is None


def test_payload_and_entry_are_immutable_snapshots() -> None:
    entry = RoutingEntry("replica")
    entries = {"a": entry}
    payload = RoutingContextPayload(entries)
    entries.clear()
    assert payload.get("a") is entry
    with pytest.raises(TypeError):
        cast(Any, payload.entries)["b"] = entry
    with pytest.raises(FrozenInstanceError):
        cast(Any, entry).mode = "primary"


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ([], ("auto", None, None)),
        ([(b"x-sqlspec-routing", b"primary")], ("primary", None, None)),
        ([(b"x-sqlspec-routing", b"replica:reporting")], ("replica", "reporting", None)),
        ([(b"cookie", b"wp=lsn-10")], ("primary", None, "lsn-10")),
        ([(b"cookie", b"wp=1"), (b"x-sqlspec-routing", b"replica")], ("replica", None, None)),
        ([(b"x-sqlspec-routing", b"bogus")], ("auto", None, None)),
        ([(b"x-sqlspec-routing", b"replica:" + b"a" * 257)], ("auto", None, None)),
        ([(b"x-sqlspec-routing", b"replica:bad\x7fgroup")], ("auto", None, None)),
        ([(b"cookie", b"wp=" + b"t" * 257)], ("auto", None, None)),
    ],
)
async def test_middleware_hints_and_reset(
    headers: list[tuple[bytes, bytes]], expected: tuple[str, str | None, str | None]
) -> None:
    observed: list[tuple[str, str | None, str | None]] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        entry = RoutingContext.get_entry() or RoutingEntry()
        observed.append((entry.mode, entry.bind_group, entry.causal_token))
        RoutingContext.mark_write(causal_token="after-write")
        await asyncio.sleep(0)
        assert cast(RoutingEntry, RoutingContext.get_entry()).causal_token == "after-write"

    async def receive() -> dict[str, Any]:
        return {"type": "http.request"}

    async def send(message: Any) -> None:
        pass

    await SQLSpecRoutingMiddleware(app, write_pin_cookie="wp")({"type": "http", "headers": headers}, receive, send)
    assert observed == [expected]
    assert RoutingContext.current() is None


async def test_middleware_async_extractor_exception_reset_and_lifespan() -> None:
    seen: list[RoutingEntry | None] = []

    async def extractor(scope: Any) -> RoutingEntry:
        return RoutingEntry("replica", bind_group="custom")

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append(RoutingContext.get_entry())
        raise RuntimeError("handler failed")

    async def receive() -> dict[str, Any]:
        return {}

    async def send(message: Any) -> None:
        pass

    middleware = SQLSpecRoutingMiddleware(app, extractor=extractor, strict_headers=True)
    with pytest.raises(RoutingContextError):
        await middleware({"type": "http", "headers": [(b"x-sqlspec-routing", b"bad")]}, receive, send)
    assert seen == []
    with pytest.raises(RuntimeError, match="handler failed"):
        await middleware({"type": "websocket", "headers": []}, receive, send)
    assert cast(RoutingEntry, seen[0]).bind_group == "custom"
    assert RoutingContext.current() is None
    with pytest.raises(RuntimeError):
        await middleware({"type": "lifespan"}, receive, send)
    assert seen[1] is None


def test_signed_write_pin_roundtrip_expiry_and_bind_isolation() -> None:
    now = 1_700_000_000.0
    signed = sign_write_pin("top-secret", ttl_seconds=5.0, bind_key="orders", causal_token="lsn-42", clock=lambda: now)
    assert verify_write_pin(signed, "top-secret", bind_key="orders", clock=lambda: now + 4.9) == (True, "lsn-42")
    assert verify_write_pin(signed, "top-secret", bind_key="orders", clock=lambda: now + 5.1) == (False, None)
    assert verify_write_pin(signed, "wrong-secret", bind_key="orders", clock=lambda: now) == (False, None)
    assert verify_write_pin(signed, "top-secret", bind_key="billing", clock=lambda: now) == (False, None)
    assert verify_write_pin(signed, "top-secret", bind_key=None, clock=lambda: now) == (False, None)

    plain_pin = sign_write_pin(b"bytes-secret", ttl_seconds=2.0, clock=lambda: now)
    assert verify_write_pin(plain_pin, b"bytes-secret", clock=lambda: now + 1.0) == (True, None)
    assert verify_write_pin("1", "top-secret", clock=lambda: now) == (False, None)
    assert verify_write_pin("x" * 1025, "top-secret", clock=lambda: now) == (False, None)


@pytest.mark.parametrize(
    ("secret", "kwargs", "exc_type"),
    [
        ("", {}, ImproperConfigurationError),
        ("ok", {"ttl_seconds": 0.0}, ImproperConfigurationError),
        ("ok", {"ttl_seconds": float("nan")}, ImproperConfigurationError),
        ("ok", {"causal_token": "bad\r\ntoken"}, RoutingContextError),
        ("ok", {"bind_key": "bad\x00bind"}, RoutingContextError),
    ],
)
def test_sign_write_pin_rejects_invalid_inputs(secret: str, kwargs: dict[str, Any], exc_type: type[Exception]) -> None:
    with pytest.raises(exc_type):
        sign_write_pin(secret, **kwargs)


@pytest.mark.parametrize(
    ("routing_header", "headers", "expected"),
    [
        (None, [(b"x-sqlspec-routing", b"primary")], ("auto", None, None)),
        (None, [(b"cookie", b"wp=1")], ("auto", None, None)),
        (None, [(b"cookie", b"wp=lsn-unsigned")], ("auto", None, None)),
        ("x-sqlspec-routing", [(b"x-sqlspec-routing", b"replica:analytics")], ("replica", "analytics", None)),
    ],
)
async def test_middleware_signed_cookie_and_disabled_header(
    routing_header: str | None, headers: list[tuple[bytes, bytes]], expected: tuple[str, str | None, str | None]
) -> None:
    observed: list[tuple[str, str | None, str | None]] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        entry = RoutingContext.get_entry("orders") or RoutingEntry()
        observed.append((entry.mode, entry.bind_group, entry.causal_token))

    async def receive() -> dict[str, Any]:
        return {"type": "http.request"}

    async def send(message: Any) -> None:
        pass

    middleware = SQLSpecRoutingMiddleware(
        app, bind_key="orders", routing_header=routing_header, write_pin_cookie="wp", routing_secret="cookie-secret"
    )
    await middleware({"type": "http", "headers": headers}, receive, send)
    assert observed == [expected]

    signed = sign_write_pin("cookie-secret", bind_key="orders", causal_token="lsn-77")
    observed.clear()
    await middleware({"type": "http", "headers": [(b"cookie", f"wp={signed}".encode("ascii"))]}, receive, send)
    assert observed == [("primary", None, "lsn-77")]
