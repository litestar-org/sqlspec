"""Unit tests for SQLite custom type handlers."""

import json
from unittest.mock import patch


def test_json_adapter_dict_default_serializer() -> None:
    """Test JSON adapter with dict using default json.dumps."""
    from sqlspec.adapters.sqlite.type_converter import json_adapter

    data = {"key": "value", "count": 42}
    result = json_adapter(data)

    assert isinstance(result, str)
    assert json.loads(result) == data


def test_json_adapter_list_default_serializer() -> None:
    """Test JSON adapter with list using default json.dumps."""
    from sqlspec.adapters.sqlite.type_converter import json_adapter

    data = [1, 2, 3, "four"]
    result = json_adapter(data)

    assert isinstance(result, str)
    assert json.loads(result) == data


def test_register_type_handlers_default() -> None:
    """Test register_type_handlers registers adapters and converters."""
    from sqlspec.adapters.sqlite.type_converter import register_type_handlers

    with patch("sqlite3.register_adapter") as mock_adapter, patch("sqlite3.register_converter") as mock_converter:
        register_type_handlers()

        assert mock_adapter.call_count == 2
        mock_converter.assert_called_once()


def test_unregister_type_handlers_is_noop() -> None:
    """Test unregister_type_handlers executes without error."""
    from sqlspec.adapters.sqlite.type_converter import unregister_type_handlers

    unregister_type_handlers()
