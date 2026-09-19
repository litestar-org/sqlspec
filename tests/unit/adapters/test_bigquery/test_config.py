"""BigQuery configuration tests covering statement config builders."""

import uuid
from types import SimpleNamespace
from typing import Any, cast

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig
from pytest import MonkeyPatch

from sqlspec.adapters.bigquery.config import (
    BigQueryConfig,
    BigQueryConnectionParams,
    BigQueryDriverFeatures,
    build_connection_config,
)
from sqlspec.adapters.bigquery.core import apply_driver_features, build_statement_config


class _RecordingBigQueryClient:
    instances: list["_RecordingBigQueryClient"] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.__class__.instances.append(self)


def test_build_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_bigquery_config_applies_driver_feature_serializer() -> None:
    """Driver features should preserve the serializer for BigQuery parameter construction."""

    def serializer(_: object) -> str:
        return "feature"

    config = BigQueryConfig(driver_features={"json_serializer": serializer})

    assert config.driver_features["json_serializer"] is serializer


def test_bigquery_driver_features_honor_uuid_conversion_flag() -> None:
    """The BigQuery UUID conversion flag should control UUID parameter coercion."""
    enabled_config, enabled_features = apply_driver_features(build_statement_config(), {"enable_uuid_conversion": True})
    disabled_config, disabled_features = apply_driver_features(
        build_statement_config(), {"enable_uuid_conversion": False}
    )

    assert enabled_features["enable_uuid_conversion"] is True
    assert disabled_features["enable_uuid_conversion"] is False
    assert uuid.UUID in enabled_config.parameter_config.type_coercion_map
    assert uuid.UUID not in disabled_config.parameter_config.type_coercion_map


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


def test_bigquery_config_routes_current_client_level_settings(monkeypatch: MonkeyPatch) -> None:
    """Current BigQuery Client constructor settings should reach client creation."""
    _RecordingBigQueryClient.instances.clear()
    monkeypatch.setattr(BigQueryConfig, "connection_type", _RecordingBigQueryClient)
    query_job_config = QueryJobConfig()
    load_job_config = LoadJobConfig()

    config = BigQueryConfig(
        connection_config={
            "project": "p",
            "location": "US",
            "default_query_job_config": query_job_config,
            "default_load_job_config": load_job_config,
            "default_job_creation_mode": "JOB_CREATION_OPTIONAL",
        }
    )

    connection = cast(_RecordingBigQueryClient, config.create_connection())

    assert connection.kwargs == {
        "project": "p",
        "location": "US",
        "default_query_job_config": query_job_config,
        "default_load_job_config": load_job_config,
        "default_job_creation_mode": "JOB_CREATION_OPTIONAL",
    }


def test_bigquery_config_typed_surfaces_do_not_advertise_inert_settings() -> None:
    """Typed public settings should only include options that SQLSpec routes."""
    assert "credentials_path" not in BigQueryConnectionParams.__annotations__
    assert "use_query_and_wait" in BigQueryDriverFeatures.__annotations__
    assert "enable_storage_write_api" in BigQueryDriverFeatures.__annotations__
    assert "enable_storage_write_api" not in BigQueryConnectionParams.__annotations__
    assert "on_job_start" not in BigQueryDriverFeatures.__annotations__
    assert "on_job_complete" not in BigQueryDriverFeatures.__annotations__


def test_qualified_dataset_id_is_preserved_without_a_project() -> None:
    """An already-qualified dataset must not be dropped for want of a project."""
    config = BigQueryConfig(connection_config={"dataset_id": "other-project.analytics"})

    assert config.connection_config["default_query_job_config"].default_dataset.dataset_id == "analytics"


def test_unqualified_dataset_id_without_a_project_does_not_raise() -> None:
    """BigQuery rejects an unqualified dataset, so it is left unset rather than crashing."""
    config = BigQueryConfig(connection_config={"dataset_id": "analytics"})

    assert config.connection_config["default_query_job_config"].default_dataset is None


def test_unqualified_dataset_id_is_resolved_from_the_client_project() -> None:
    """Once the client exists its project qualifies the dataset."""
    config = BigQueryConfig(connection_config={"dataset_id": "analytics"})
    config._qualify_default_dataset(cast("Any", SimpleNamespace(project="discovered")))
    default_dataset = config.connection_config["default_query_job_config"].default_dataset

    assert default_dataset.project == "discovered"
    assert default_dataset.dataset_id == "analytics"


def test_dataset_id_is_qualified_with_the_configured_project() -> None:
    config = BigQueryConfig(connection_config={"project": "acme", "dataset_id": "analytics"})
    default_dataset = config.connection_config["default_query_job_config"].default_dataset

    assert default_dataset.project == "acme"
    assert default_dataset.dataset_id == "analytics"


def test_build_connection_config_normalizes_aliases() -> None:
    """Project and dataset aliases should map to canonical keys."""
    cfg = build_connection_config({"project_id": "p-123", "dataset": "d-456"})
    assert "project_id" not in cfg
    assert "dataset" not in cfg
    assert cfg["project"] == "p-123"
    assert cfg["dataset_id"] == "d-456"


def test_build_connection_config_canonical_keys_override_aliases() -> None:
    """Canonical parameter names should take precedence over aliases."""
    cfg = build_connection_config({
        "project": "canonical-p",
        "project_id": "alias-p",
        "dataset_id": "canonical-d",
        "dataset": "alias-d",
    })
    assert cfg["project"] == "canonical-p"
    assert cfg["dataset_id"] == "canonical-d"


def test_build_connection_config_database_and_db_aliases() -> None:
    """Database and db aliases should map to dataset_id."""
    cfg_db = build_connection_config({"project": "p", "database": "db-1"})
    assert cfg_db["dataset_id"] == "db-1"
    assert "database" not in cfg_db

    cfg_short_db = build_connection_config({"project": "p", "db": "db-2"})
    assert cfg_short_db["dataset_id"] == "db-2"
    assert "db" not in cfg_short_db


def test_bigquery_config_uses_project_id_and_dataset_alias() -> None:
    """BigQueryConfig should accept project_id and dataset aliases and configure jobs."""
    config = BigQueryConfig(connection_config={"project_id": "my-proj", "dataset": "my-dataset"})
    assert config.connection_config["project"] == "my-proj"
    assert config.connection_config["dataset_id"] == "my-dataset"
    default_dataset = config.connection_config["default_query_job_config"].default_dataset
    assert default_dataset.project == "my-proj"
    assert default_dataset.dataset_id == "my-dataset"
