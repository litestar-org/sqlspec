"""Clientless multi-cloud object storage integration fixtures for Floci GCS, Floci AWS S3, and Azurite."""

import importlib
from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.docker.floci import FLOCI_CONTAINER_PORTS, FlociGCPService

from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from pytest_databases.docker.azure_blob import AzureBlobService
    from pytest_databases.docker.floci import FlociAWSService

    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.storage.backends.fsspec import FSSpecBackend
    from sqlspec.storage.backends.obstore import ObStoreBackend
else:
    FSSpecBackend = import_optional_attr("sqlspec.storage.backends.fsspec", "FSSpecBackend")
    ObStoreBackend = import_optional_attr("sqlspec.storage.backends.obstore", "ObStoreBackend")

FLOCI_GCP_GATEWAY_PORT = FLOCI_CONTAINER_PORTS["gcp"]

__all__ = (
    "FLOCI_GCP_GATEWAY_PORT",
    "azurite_fsspec",
    "azurite_obstore",
    "floci_aws_s3_bucket_name",
    "floci_aws_s3_obstore",
    "floci_gcs_bucket_name",
    "floci_gcs_fsspec",
    "floci_gcs_obstore",
    "floci_gcs_obstore_kwargs",
)


def floci_gcs_obstore_kwargs(floci_gcp_service: "FlociGCPService") -> "dict[str, Any]":
    """Build ObStoreBackend keyword arguments for the Floci GCP Storage emulator without host ADC lookup."""
    return {
        "base_url": floci_gcp_service.endpoint_url,
        "bearer_token": "anon",
        "skip_signature": True,
        "client_options": {"allow_http": True},
    }


@pytest.fixture(scope="session")
def floci_gcs_bucket_name(floci_gcp_service: "FlociGCPService", floci_resource_prefix: str) -> str:
    """Ensure and return a worker-isolated GCS bucket name on the Floci GCP emulator."""
    bucket_name = f"{floci_resource_prefix}gcs"
    floci_gcp_service.ensure_bucket(bucket_name)
    return bucket_name


@pytest.fixture
def floci_gcs_obstore(floci_gcp_service: "FlociGCPService", floci_gcs_bucket_name: str) -> "ObjectStoreProtocol":
    """Provide an ObStoreBackend connected to the Floci GCP Storage emulator."""
    pytest.importorskip("obstore")
    return ObStoreBackend(f"gs://{floci_gcs_bucket_name}", **floci_gcs_obstore_kwargs(floci_gcp_service))


@pytest.fixture
def floci_gcs_fsspec(
    floci_gcp_service: "FlociGCPService", floci_gcs_bucket_name: str, monkeypatch: pytest.MonkeyPatch
) -> "ObjectStoreProtocol":
    """Provide an FSSpecBackend connected to the Floci GCP Storage emulator."""
    pytest.importorskip("fsspec")
    pytest.importorskip("gcsfs")
    monkeypatch.setenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false")
    core_gcs_fs: Any = import_optional_attr("gcsfs.core", "GCSFileSystem")
    fsspec_registry_mod: Any = importlib.import_module("fsspec.registry")
    if core_gcs_fs is not None:
        monkeypatch.setitem(fsspec_registry_mod._registry, "gs", core_gcs_fs)
        monkeypatch.setitem(fsspec_registry_mod._registry, "gcs", core_gcs_fs)
    return FSSpecBackend(f"gs://{floci_gcs_bucket_name}", token="anon", endpoint_url=floci_gcp_service.endpoint_url)


@pytest.fixture(scope="session")
def floci_aws_s3_bucket_name(floci_aws_service: "FlociAWSService", floci_resource_prefix: str) -> str:
    """Ensure and return a worker-isolated S3 bucket name on the Floci AWS emulator."""
    bucket_name = f"{floci_resource_prefix}s3"
    floci_aws_service.ensure_bucket(bucket_name)
    return bucket_name


@pytest.fixture
def floci_aws_s3_obstore(floci_aws_service: "FlociAWSService", floci_aws_s3_bucket_name: str) -> "ObjectStoreProtocol":
    """Provide an ObStoreBackend connected to the Floci AWS S3 emulator."""
    pytest.importorskip("obstore")
    return ObStoreBackend(
        f"s3://{floci_aws_s3_bucket_name}",
        aws_endpoint=floci_aws_service.endpoint_url,
        aws_access_key_id=floci_aws_service.access_key,
        aws_secret_access_key=floci_aws_service.secret_key,
        aws_region=floci_aws_service.region,
        aws_virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )


@pytest.fixture
def azurite_obstore(
    azure_blob_service: "AzureBlobService", azure_blob_default_container_name: str
) -> "ObjectStoreProtocol":
    """Provide an ObStoreBackend connected to the Azurite Blob Storage emulator."""
    pytest.importorskip("obstore")
    return ObStoreBackend(
        f"az://{azure_blob_default_container_name}",
        account_name=azure_blob_service.account_name,
        account_key=azure_blob_service.account_key,
        endpoint=azure_blob_service.account_url,
        client_options={"allow_http": True},
    )


@pytest.fixture
def azurite_fsspec(
    azure_blob_service: "AzureBlobService", azure_blob_default_container_name: str
) -> "ObjectStoreProtocol":
    """Provide an FSSpecBackend connected to the Azurite Blob Storage emulator."""
    pytest.importorskip("fsspec")
    pytest.importorskip("adlfs")
    return FSSpecBackend(
        f"az://{azure_blob_default_container_name}",
        account_name=azure_blob_service.account_name,
        account_key=azure_blob_service.account_key,
        connection_string=azure_blob_service.connection_string,
    )
