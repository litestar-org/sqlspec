"""Unit tests for Spanner JsonObject direct unwrapping optimization."""

from unittest.mock import MagicMock

from google.cloud.spanner_v1.data_types import JsonObject

from sqlspec.adapters.spanner.core import _convert_json_row_value
from sqlspec.utils.serializers import from_json


class MonitoredJsonObject(JsonObject):
    """JsonObject subclass that tracks calls to serialize()."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.serialize_called = False

    def serialize(self) -> str | None:
        self.serialize_called = True
        return super().serialize()


def test_convert_json_row_value_unwraps_without_serialize() -> None:
    """Verify that default deserializer unwraps JsonObject directly without calling serialize()."""
    obj = MonitoredJsonObject({"key": "val", "nested": [1, 2]})
    res = _convert_json_row_value(obj, json_deserializer=from_json)
    assert res == {"key": "val", "nested": [1, 2]}
    assert not obj.serialize_called


def test_convert_json_row_value_calls_serialize_for_custom_deserializer() -> None:
    """Verify that a custom string deserializer invokes serialize()."""
    obj = MonitoredJsonObject({"key": "val"})
    custom_deserializer = MagicMock(return_value={"custom": True})
    res = _convert_json_row_value(obj, json_deserializer=custom_deserializer)
    assert res == {"custom": True}
    assert obj.serialize_called
    custom_deserializer.assert_called_once_with('{"key":"val"}')


def test_convert_json_row_value_null_json() -> None:
    """Verify that null JsonObject unwraps directly to None."""
    obj = MonitoredJsonObject(None)
    res = _convert_json_row_value(obj, json_deserializer=from_json)
    assert res is None
    assert not obj.serialize_called


def test_convert_json_row_value_array_json() -> None:
    """Verify that array JsonObject unwraps directly to a list."""
    obj = MonitoredJsonObject([1, 2, 3])
    res = _convert_json_row_value(obj, json_deserializer=from_json)
    assert res == [1, 2, 3]
    assert not obj.serialize_called
