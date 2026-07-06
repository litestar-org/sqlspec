from datetime import datetime, timezone
from uuid import UUID

from sqlspec.adapters.spanner.type_converter import coerce_params_for_spanner, spanner_json, spanner_to_uuid
from sqlspec.core import TypedParameter


def test_spanner_to_uuid_converts_bytes() -> None:
    uuid_value = UUID("550e8400-e29b-41d4-a716-446655440000")

    assert spanner_to_uuid(uuid_value.bytes) == uuid_value


def test_spanner_json_uses_native_json_object() -> None:
    result = spanner_json({"key": "value"})

    assert result == {"key": "value"}


def test_coerce_params_unwraps_typed_datetime_parameter() -> None:
    timestamp = datetime(2026, 7, 4, 22, 9, 0, tzinfo=timezone.utc)
    params = {"available_at": TypedParameter(timestamp, datetime)}

    coerced = coerce_params_for_spanner(params)

    assert coerced == {"available_at": timestamp}
