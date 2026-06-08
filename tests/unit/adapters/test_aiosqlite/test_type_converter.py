"""Unit tests for the aiosqlite-local SQLite type converter copy."""

import json
from unittest.mock import MagicMock, patch


def test_import_is_from_aiosqlite_not_sqlite() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import register_type_handlers

    assert callable(register_type_handlers)


def test_aiosqlite_module_is_independent_of_sqlite_module() -> None:
    import sqlspec.adapters.aiosqlite.type_converter as aio_mod
    import sqlspec.adapters.sqlite.type_converter as sql_mod

    assert aio_mod is not sql_mod


def test_json_adapter_dict_default_serializer() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import json_adapter

    data = {"key": "value", "count": 42}
    result = json_adapter(data)

    assert isinstance(result, str)
    assert json.loads(result) == data


def test_json_adapter_list_default_serializer() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import json_adapter

    data = [1, 2, 3, "four"]
    result = json_adapter(data)

    assert isinstance(result, str)
    assert json.loads(result) == data


def test_json_adapter_custom_serializer() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import json_adapter

    called_with = []

    def custom_serializer(value: object) -> str:
        called_with.append(value)
        return json.dumps(value)

    data = {"x": 1}
    result = json_adapter(data, serializer=custom_serializer)

    assert result == json.dumps(data)
    assert called_with == [data]


def test_json_converter_default_deserializer() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import json_converter

    data = {"key": "value"}
    result = json_converter(json.dumps(data).encode("utf-8"))

    assert result == data


def test_register_type_handlers_calls_sqlite3_apis() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import register_type_handlers

    with patch("sqlite3.register_adapter") as mock_adapter, patch("sqlite3.register_converter") as mock_converter:
        register_type_handlers()

    assert mock_adapter.call_count == 2
    mock_converter.assert_called_once()


def test_register_type_handlers_with_custom_serializers() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import register_type_handlers

    def custom_serializer(value: object) -> str:
        return json.dumps(value)

    def custom_deserializer(value: str) -> object:
        return json.loads(value)

    with patch("sqlite3.register_adapter"), patch("sqlite3.register_converter"):
        register_type_handlers(json_serializer=custom_serializer, json_deserializer=custom_deserializer)


def test_unregister_type_handlers_is_noop() -> None:
    from sqlspec.adapters.aiosqlite.type_converter import unregister_type_handlers

    unregister_type_handlers()


def test_aiosqlite_config_uses_aiosqlite_type_converter() -> None:
    import sqlspec.adapters.aiosqlite.config as config_mod
    import sqlspec.adapters.sqlite.type_converter as sqlite_type_converter

    aio_mock = MagicMock()
    sqlite_mock = MagicMock()

    with (
        patch.object(config_mod, "register_type_handlers", aio_mock),
        patch.object(sqlite_type_converter, "register_type_handlers", sqlite_mock),
    ):
        config = config_mod.AiosqliteConfig(driver_features={"enable_custom_adapters": True})
        config._register_type_adapters()  # pyright: ignore[reportPrivateUsage]

    aio_mock.assert_called_once()
    sqlite_mock.assert_not_called()
