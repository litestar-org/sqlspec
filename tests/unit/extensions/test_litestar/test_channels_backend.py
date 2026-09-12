"""Unit tests for the Litestar channels backend payload budget and metrics."""

from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest

from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events import MAX_NOTIFY_BYTES, encode_notify_payload
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


async def test_measure_matches_published_event_envelope() -> None:
    """measure() matches the notify envelope encoded from the event publish() hands to the channel."""
    stub = _StubEventChannel("notify")
    backend = SQLSpecChannelsBackend(cast("AsyncEventChannel", stub))
    data = b"payload bytes" * 40

    await backend.publish(data, ["c"])

    _, payload, metadata = stub.published[0]
    assert len(encode_notify_payload(uuid4().hex, payload, metadata).encode("utf-8")) == backend.measure(data)


async def test_fits_agrees_with_notify_encoding_at_boundary() -> None:
    """Encoding the published event raises exactly when fits() is False."""
    stub = _StubEventChannel("notify")
    backend = SQLSpecChannelsBackend(cast("AsyncEventChannel", stub))
    size = next(n for n in range(MAX_NOTIFY_BYTES) if backend.measure(b"x" * n) > MAX_NOTIFY_BYTES)

    await backend.publish(b"x" * (size - 1), ["c"])
    await backend.publish(b"x" * size, ["c"])

    _, fitting_payload, fitting_metadata = stub.published[0]
    _, oversized_payload, oversized_metadata = stub.published[1]
    assert backend.notify_budget == MAX_NOTIFY_BYTES
    assert backend.fits(b"x" * (size - 1)) is True
    encode_notify_payload(uuid4().hex, fitting_payload, fitting_metadata)
    assert backend.fits(b"x" * size) is False
    with pytest.raises(EventChannelError):
        encode_notify_payload(uuid4().hex, oversized_payload, oversized_metadata)


@pytest.mark.parametrize("backend_name", ["poll_queue", "notify_queue", "aq", "txeventq"])
def test_fits_non_notify_backends_always_true(backend_name: str) -> None:
    """Backends that do not carry payloads in NOTIFY have no payload budget."""
    backend = _backend(backend_name)

    assert backend.notify_budget is None
    assert backend.fits(b"x" * (MAX_NOTIFY_BYTES * 2)) is True


def test_backend_metrics_snapshot_merges_channel() -> None:
    """metrics_snapshot() keeps the channel's metrics and adds the backend queue counters."""
    channel_metrics = {"AsyncpgConfig.events.publish.native": 3.0, "AsyncpgConfig.events.ack": 2.0}
    backend = _backend("notify", channel_metrics)

    snapshot = backend.metrics_snapshot()

    assert {key: snapshot[key] for key in channel_metrics} == channel_metrics
    assert set(snapshot) == {*channel_metrics, "channels.output_queue_depth", "channels.dropped_message_count"}
