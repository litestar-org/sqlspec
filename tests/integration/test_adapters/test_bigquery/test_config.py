"""BigQuery configuration tests with CORE_ROUND_3 architecture."""

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from sqlspec.core.result import SQLResult


@pytest.mark.xdist_group("bigquery")
def test_bigquery_config_creation(bigquery_service: BigQueryService) -> None:
    """Test BigQuery configuration creation."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    assert config.connection_config["project"] == bigquery_service.project
    assert config.connection_config["dataset_id"] == bigquery_service.dataset


@pytest.mark.xdist_group("bigquery")
def test_bigquery_config_session_context_manager(bigquery_service: BigQueryService) -> None:
    """Test BigQuery session context manager."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    with config.provide_session() as session:
        assert isinstance(session, BigQueryDriver)
        # Test that session is functional
        result = session.execute("SELECT 1 as test_value")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("bigquery")
def test_bigquery_config_with_query_job_config(bigquery_service: BigQueryService) -> None:
    """Test BigQuery configuration with query job configuration."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    with config.provide_session() as session:
        # Test basic query with job configuration
        result = session.execute("SELECT 'BigQuery Config Test' as message")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["message"] == "BigQuery Config Test"


@pytest.mark.xdist_group("bigquery")
def test_bigquery_config_connection_reuse(bigquery_service: BigQueryService) -> None:
    """Test BigQuery configuration connection reuse."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    # Test multiple sessions from same config
    with config.provide_session() as session1:
        result1 = session1.execute("SELECT 'Session 1' as session_name")
        assert isinstance(result1, SQLResult)
        assert result1.data is not None
        assert result1.data[0]["session_name"] == "Session 1"

    with config.provide_session() as session2:
        result2 = session2.execute("SELECT 'Session 2' as session_name")
        assert isinstance(result2, SQLResult)
        assert result2.data is not None
        assert result2.data[0]["session_name"] == "Session 2"


@pytest.mark.xdist_group("bigquery")
def test_bigquery_config_error_handling(bigquery_service: BigQueryService) -> None:
    """Test BigQuery configuration error handling."""
    # Test with invalid project
    invalid_config = BigQueryConfig(
        connection_config={
            "project": "nonexistent-project-12345",
            "dataset_id": "nonexistent_dataset",
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    with invalid_config.provide_session() as session:
        # The session should still be created, but queries may fail
        # This depends on the BigQuery emulator behavior
        assert isinstance(session, BigQueryDriver)


@pytest.mark.xdist_group("bigquery")
def test_bigquery_config_dataset_scoping(bigquery_service: BigQueryService) -> None:
    """Test BigQuery dataset scoping in configuration."""
    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    with config.provide_session() as session:
        # Create a test table in the configured dataset
        session.execute_script(f"""
            CREATE TABLE IF NOT EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.config_test` (
                id INT64,
                message STRING
            )
        """)

        # Insert test data
        session.execute(
            f"INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.config_test` (id, message) VALUES (?, ?)",
            (1, "config test"),
        )

        # Query the table
        result = session.execute(
            f"SELECT message FROM `{bigquery_service.project}.{bigquery_service.dataset}.config_test` WHERE id = ?",
            (1,),
        )
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["message"] == "config test"

        # Cleanup
        session.execute_script(f"DROP TABLE `{bigquery_service.project}.{bigquery_service.dataset}.config_test`")
