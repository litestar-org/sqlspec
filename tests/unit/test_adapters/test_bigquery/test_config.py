"""BigQuery configuration tests covering statement config builders."""

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.core import build_statement_config


def test_build_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_bigquery_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the BigQuery statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    config = BigQueryConfig(driver_features={"json_serializer": serializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
