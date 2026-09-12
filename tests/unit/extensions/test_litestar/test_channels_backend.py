"""Unit tests for the Litestar channels backend payload budget and metrics."""

import base64
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest

from sqlspec.extensions.events import MAX_NOTIFY_BYTES, encode_notify_payload, measure_notify_payload
from sqlspec.extensions.litestar.channels import SQLSpecChannelsBackend

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.extensions.events import AsyncEventChannel


class _StubEventChannel:
    """Minimal event channel exposing only what the backend uses."""

    def __init__(self, backend_name: str, metrics: "dict[str, float] | None" = None) -> None:
        self.backend_name = backend_name
        self._metrics = metrics or {}
        self.published: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []

    def metrics_snapshot(self) -> "dict[str, float]":
        return dict(self._metrics)

    async def publish_many(self, events: "Sequence[tuple[str, dict[str, Any], dict[str, Any] | None]]") -> list[str]:
        self.published.extend(events)
        return [uuid4().hex for _ in events]

    async def shutdown(self) -> None:
        return None


def _backend(backend_name: str, metrics: "dict[str, float] | None" = None) -> SQLSpecChannelsBackend:
    return SQLSpecChannelsBackend(cast("AsyncEventChannel", _StubEventChannel(backend_name, metrics)))


def _wrapped(data: bytes) -> "dict[str, Any]":
    return {"data_b64": base64.b64encode(data).decode("ascii")}


def test_measure_matches_encoded_envelope() -> None:
    """measure() returns the size of the envelope publish() would send."""
    backend = _backend("notify")
    data = b"x" * 100

    assert backend.measure(data) == measure_notify_payload(_wrapped(data), None)
    assert backend.measure(data) == len(encode_notify_payload(uuid4().hex, _wrapped(data), None).encode("utf-8"))


def test_fits_boundary() -> None:
    """fits() flips at the first payload whose envelope exceeds the NOTIFY budget."""
    backend = _backend("notify")
    size = next(n for n in range(MAX_NOTIFY_BYTES) if backend.measure(b"x" * n) > MAX_NOTIFY_BYTES)

    assert backend.notify_budget == MAX_NOTIFY_BYTES
    assert backend.measure(b"x" * (size - 1)) <= MAX_NOTIFY_BYTES
    assert backend.fits(b"x" * (size - 1)) is True
    assert backend.fits(b"x" * size) is False


@pytest.mark.parametrize("backend_name", ["poll_queue", "notify_queue", "aq", "txeventq"])
def test_fits_non_notify_backends_always_true(backend_name: str) -> None:
    """Backends that do not carry payloads in NOTIFY have no payload budget."""
    backend = _backend(backend_name)

    assert backend.notify_budget is None
    assert backend.fits(b"x" * (MAX_NOTIFY_BYTES * 2)) is True


def test_backend_metrics_snapshot_merges_channel() -> None:
    """metrics_snapshot() merges channel metrics with the backend queue counters."""
    channel_metrics = {"AsyncpgConfig.events.publish": 3.0, "AsyncpgConfig.events.ack": 2.0}
    backend = _backend("notify", channel_metrics)

    snapshot = backend.metrics_snapshot()

    assert snapshot == {**channel_metrics, "channels.output_queue_depth": 0.0, "channels.dropped_messages": 0.0}
    assert all(isinstance(value, float) for value in snapshot.values())
