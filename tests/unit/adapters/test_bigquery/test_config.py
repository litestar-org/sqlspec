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


def test_bigquery_config_wires_query_timeout_ms_to_default_job_config() -> None:
    """query_timeout_ms on connection_config reaches QueryJobConfig.job_timeout_ms (existing behaviour)."""
    config = BigQueryConfig(connection_config={"project": "p", "dataset_id": "d", "query_timeout_ms": 12345})
    default_job_config = config.connection_config["default_query_job_config"]
    assert int(default_job_config.job_timeout_ms) == 12345


def test_bigquery_config_wires_job_timeout_ms_to_default_job_config() -> None:
    """job_timeout_ms on connection_config reaches QueryJobConfig.job_timeout_ms (regression for #473)."""
    config = BigQueryConfig(connection_config={"project": "p", "dataset_id": "d", "job_timeout_ms": 30000})
    default_job_config = config.connection_config["default_query_job_config"]
    assert int(default_job_config.job_timeout_ms) == 30000


def test_bigquery_config_job_timeout_ms_overrides_query_timeout_ms() -> None:
    """When both are set, job_timeout_ms wins (applied after query_timeout_ms)."""
    config = BigQueryConfig(
        connection_config={"project": "p", "dataset_id": "d", "query_timeout_ms": 1000, "job_timeout_ms": 30000}
    )
    default_job_config = config.connection_config["default_query_job_config"]
    assert int(default_job_config.job_timeout_ms) == 30000
