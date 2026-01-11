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
