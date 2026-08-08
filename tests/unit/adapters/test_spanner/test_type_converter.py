import base64
from datetime import date, datetime, timezone
from typing import Any, cast
from uuid import UUID

import uuid_utils
from google.cloud.spanner_v1.data_types import JsonObject

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


def test_coerce_params_preserves_driver_ready_parameters() -> None:
    timestamp = datetime(2026, 7, 4, 22, 9, 0, tzinfo=timezone.utc)
    array = ["alpha", "beta"]
    payload = cast("Any", JsonObject)({"key": "value"})
    params = {
        "id": 1,
        "name": "alpha",
        "enabled": True,
        "score": 2.5,
        "missing": None,
        "day": date(2026, 7, 4),
        "available_at": timestamp,
        "tags": array,
        "payload": payload,
    }

    coerced = coerce_params_for_spanner(params)

    assert coerced is not None
    assert coerced is params
    assert coerced["tags"] is array
    assert coerced["payload"] is payload


def test_coerce_params_preserves_empty_parameter_mapping() -> None:
    params: dict[str, object] = {}

    assert coerce_params_for_spanner(params) is params


def test_coerce_params_copies_only_when_values_require_conversion() -> None:
    stdlib_uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
    utils_uuid = uuid_utils.UUID(str(stdlib_uuid))
    binary = b"binary"
    naive_timestamp = datetime(2026, 7, 4, 22, 9, 0)
    typed_timestamp = datetime(2026, 7, 5, 22, 9, 0, tzinfo=timezone.utc)
    plain_array = ["alpha", "beta"]
    params = {
        "stdlib_uuid": stdlib_uuid,
        "utils_uuid": utils_uuid,
        "binary": binary,
        "naive_timestamp": naive_timestamp,
        "typed_timestamp": TypedParameter(typed_timestamp, datetime),
        "payload": {"key": "value"},
        "tuple_array": ("alpha", "beta"),
        "json_array": [{"key": "value"}],
        "plain_array": plain_array,
    }

    coerced = coerce_params_for_spanner(params)

    assert coerced is not params
    assert coerced is not None
    assert coerced["stdlib_uuid"] == str(stdlib_uuid)
    assert coerced["utils_uuid"] == str(stdlib_uuid)
    assert coerced["binary"] == base64.b64encode(binary)
    assert coerced["naive_timestamp"] == naive_timestamp.replace(tzinfo=timezone.utc)
    assert coerced["typed_timestamp"] is typed_timestamp
    assert isinstance(coerced["payload"], JsonObject)
    assert coerced["payload"] == {"key": "value"}
    assert coerced["tuple_array"] == ["alpha", "beta"]
    assert isinstance(coerced["json_array"], JsonObject)
    assert cast("Any", coerced["json_array"]).serialize() == '[{"key":"value"}]'
    assert coerced["plain_array"] is plain_array
    assert params["stdlib_uuid"] is stdlib_uuid
    assert params["utils_uuid"] is utils_uuid
    assert params["binary"] is binary
    assert params["naive_timestamp"] is naive_timestamp
    assert isinstance(params["typed_timestamp"], TypedParameter)
    assert params["payload"] == {"key": "value"}
    assert params["tuple_array"] == ("alpha", "beta")
    assert params["json_array"] == [{"key": "value"}]
    assert params["plain_array"] is plain_array


def test_spanner_uuid_coercion_and_param_type_inference() -> None:
    """Test Spanner UUID parameter coercion to string and param_types inference."""
    from google.cloud.spanner_v1 import param_types

    from sqlspec.adapters.spanner.type_converter import infer_spanner_param_types

    stdlib_uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
    utils_uuid = uuid_utils.UUID("550e8400-e29b-41d4-a716-446655440000")

    params = {"stdlib_id": stdlib_uuid, "utils_id": utils_uuid}

    coerced = coerce_params_for_spanner(params, enable_uuid_conversion=True)
    assert coerced is not None
    assert coerced["stdlib_id"] == "550e8400-e29b-41d4-a716-446655440000"
    assert coerced["utils_id"] == "550e8400-e29b-41d4-a716-446655440000"

    inferred = infer_spanner_param_types(coerced)
    assert inferred["stdlib_id"] == param_types.STRING
    assert inferred["utils_id"] == param_types.STRING


def test_spanner_uuid_conversion_disabled() -> None:
    """Test that enable_uuid_conversion=False preserves original UUID instances."""
    stdlib_uuid = UUID("550e8400-e29b-41d4-a716-446655440000")
    utils_uuid = uuid_utils.UUID("550e8400-e29b-41d4-a716-446655440000")

    params = {"stdlib_id": stdlib_uuid, "utils_id": utils_uuid}

    coerced = coerce_params_for_spanner(params, enable_uuid_conversion=False)
    assert coerced is params
    assert coerced["stdlib_id"] is stdlib_uuid
    assert coerced["utils_id"] is utils_uuid
