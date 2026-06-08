"""Asyncmy configuration tests covering statement config builders."""

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.adapters.asyncmy.core import build_statement_config


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
