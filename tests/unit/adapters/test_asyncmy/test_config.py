"""Asyncmy configuration tests covering statement config builders."""

from typing import Any

import pytest

from sqlspec.adapters.asyncmy._typing import AsyncmyDictCursor
from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.adapters.asyncmy.core import build_statement_config
from sqlspec.exceptions import ImproperConfigurationError


def test_build_default_statement_config_custom_serializers() -> None:
    """Custom serializers should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    def deserializer(_: str) -> object:
        return {"value": "deserialized"}

    statement_config = build_statement_config(json_serializer=serializer, json_deserializer=deserializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_asyncmy_config_applies_driver_feature_serializers() -> None:
    """Driver features should mutate the Asyncmy statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    def deserializer(_: str) -> object:
        return {"feature": True}

    config = AsyncmyConfig(driver_features={"json_serializer": serializer, "json_deserializer": deserializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_asyncmy_connection_params_type_modern_driver_options() -> None:
    """Typed connection params should include current asyncmy connection options."""
    from sqlspec.adapters.asyncmy.config import AsyncmyConnectionParams

    expected_options = {
        "auth_plugin_map",
        "binary_prefix",
        "client_flag",
        "conv",
        "cursor_cls",
        "db",
        "max_allowed_packet",
        "program_name",
        "read_timeout",
        "server_public_key",
        "use_unicode",
        "write_timeout",
    }

    assert expected_options <= set(AsyncmyConnectionParams.__annotations__)
    assert "cursor_class" in AsyncmyConnectionParams.__annotations__


def test_asyncmy_cursor_cls_is_canonical_and_cursor_class_is_compatibility_alias() -> None:
    """The legacy cursor_class key should normalize to upstream cursor_cls."""
    config = AsyncmyConfig(connection_config={"cursor_class": AsyncmyDictCursor})

    assert config.connection_config["cursor_cls"] is AsyncmyDictCursor
    assert "cursor_class" not in config.connection_config


def test_asyncmy_cursor_cls_and_cursor_class_conflict_raises() -> None:
    """Conflicting cursor aliases should not silently choose one."""
    with pytest.raises(ImproperConfigurationError, match="cursor_cls"):
        AsyncmyConfig(connection_config={"cursor_cls": object, "cursor_class": AsyncmyDictCursor})


def test_asyncmy_local_infile_requires_explicit_security_gate() -> None:
    """LOAD DATA LOCAL INFILE should stay disabled unless separately gated."""
    with pytest.raises(ImproperConfigurationError, match="allow_local_infile=True"):
        AsyncmyConfig(connection_config={"local_infile": True})

    config = AsyncmyConfig(connection_config={"allow_local_infile": True, "local_infile": True})

    assert config.connection_config["local_infile"] is True
    assert "allow_local_infile" not in config.connection_config


def test_asyncmy_rejects_local_infile_bulk_load_feature() -> None:
    """Asyncmy exposes local_infile, but its LOAD DATA LOCAL INFILE protocol path is not usable."""
    with pytest.raises(ImproperConfigurationError, match="asyncmy does not currently support"):
        AsyncmyConfig(
            connection_config={"local_infile": True, "allow_local_infile": True},
            driver_features={"enable_local_infile_bulk_load": True},
        )


async def test_asyncmy_create_pool_normalizes_connection_and_pool_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool-only and SQLSpec-only settings should not leak into asyncmy connection kwargs."""
    import sqlspec.adapters.asyncmy.config as config_mod

    captured_connection_kwargs: dict[str, Any] = {}
    captured_pool_kwargs: dict[str, Any] = {}
    auth_plugin_map = {"mysql_clear_password": object}
    conv = {str: str}
    ssl_config = {"ca": "/tmp/ca.pem", "check_hostname": True}

    async def fake_create_pool(
        *, minsize: int = 1, maxsize: int = 10, echo: bool = False, pool_recycle: int = 3600, **kwargs: Any
    ) -> object:
        captured_pool_kwargs.update({
            "minsize": minsize,
            "maxsize": maxsize,
            "echo": echo,
            "pool_recycle": pool_recycle,
        })
        captured_connection_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(config_mod.asyncmy, "create_pool", fake_create_pool)

    config = AsyncmyConfig(
        connection_config={
            "allow_local_infile": True,
            "auth_plugin_map": auth_plugin_map,
            "charset": "utf8mb4",
            "conv": conv,
            "cursor_class": AsyncmyDictCursor,
            "db": "legacy_db",
            "echo": True,
            "local_infile": True,
            "maxsize": 8,
            "minsize": 2,
            "pool_recycle": 120,
            "read_timeout": 5.5,
            "ssl": ssl_config,
            "write_timeout": 6.5,
        }
    )

    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

    assert captured_pool_kwargs == {"minsize": 2, "maxsize": 8, "echo": True, "pool_recycle": 120}
    assert captured_connection_kwargs["auth_plugin_map"] is auth_plugin_map
    assert captured_connection_kwargs["charset"] == "utf8mb4"
    assert captured_connection_kwargs["conv"] is conv
    assert captured_connection_kwargs["cursor_cls"] is AsyncmyDictCursor
    assert captured_connection_kwargs["db"] == "legacy_db"
    assert captured_connection_kwargs["local_infile"] is True
    assert captured_connection_kwargs["read_timeout"] == 5.5
    assert captured_connection_kwargs["ssl"] is ssl_config
    assert "allow_local_infile" not in captured_connection_kwargs
    assert "cursor_class" not in captured_connection_kwargs
    assert "minsize" not in captured_connection_kwargs
    assert "maxsize" not in captured_connection_kwargs
    assert "pool_recycle" not in captured_connection_kwargs
    assert "write_timeout" not in captured_connection_kwargs


def test_asyncmy_typing_all_exports_pool_and_dict_cursor() -> None:
    """AsyncmyPool and AsyncmyDictCursor must be present in _typing.__all__."""
    from sqlspec.adapters.asyncmy import _typing

    assert "AsyncmyPool" in _typing.__all__
    assert "AsyncmyDictCursor" in _typing.__all__


def test_asyncmy_typing_pool_is_importable() -> None:
    """AsyncmyPool must be importable from sqlspec.adapters.asyncmy._typing."""
    from sqlspec.adapters.asyncmy._typing import AsyncmyPool

    assert AsyncmyPool is not None


def test_asyncmy_typing_dict_cursor_is_importable() -> None:
    """AsyncmyDictCursor must be importable from sqlspec.adapters.asyncmy._typing."""
    from sqlspec.adapters.asyncmy._typing import AsyncmyDictCursor

    assert AsyncmyDictCursor is not None


def test_asyncmy_config_imports_pool_from_typing_not_vendor() -> None:
    """AsyncmyPool in config module must be the same object as _typing.AsyncmyPool."""
    import sqlspec.adapters.asyncmy.config as config_mod
    from sqlspec.adapters.asyncmy._typing import AsyncmyPool

    assert config_mod.AsyncmyPool is AsyncmyPool


def test_asyncmy_config_no_direct_vendor_type_imports() -> None:
    """config.py must not expose raw vendor type names at module level."""
    import sqlspec.adapters.asyncmy.config as config_mod

    assert not hasattr(config_mod, "Pool")
    assert not hasattr(config_mod, "Cursor")
    assert not hasattr(config_mod, "DictCursor")


def test_asyncmy_provide_pool_return_annotation_is_asyncmy_pool() -> None:
    """provide_pool must be annotated with AsyncmyPool, not the raw Pool alias."""
    from sqlspec.adapters.asyncmy.config import AsyncmyConfig

    assert AsyncmyConfig.provide_pool.__annotations__.get("return") == "AsyncmyPool"


def test_driver_profile_name_matches_registry_key() -> None:
    """driver_profile.name must equal the registry key 'asyncmy'."""
    from sqlspec.adapters.asyncmy.core import driver_profile

    assert driver_profile.name == "asyncmy"
