"""Native storage eligibility preserves the selected backend's configuration."""

from typing import Any

import pytest

from sqlspec.adapters.duckdb.core import _native_storage_eligible, _resolve_native_storage_target
from sqlspec.storage import StorageRegistry, SyncStoragePipeline


@pytest.mark.parametrize("scheme", ["gcss", "memory", "custom+storage"])
@pytest.mark.parametrize("write", [True, False])
def test_configured_filesystem_preserves_uri_without_python_backend(
    scheme: str, write: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    def reject_resolution(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("A configured DuckDB filesystem must not require a Python storage backend")

    monkeypatch.setattr(StorageRegistry, "get", reject_resolution)
    uri = f"{scheme}://bucket/prefix/file.parquet"
    target = _resolve_native_storage_target(
        SyncStoragePipeline(), uri, {"_duckdb_storage_protocols": frozenset({scheme})}, write=write
    )
    assert target is not None
    assert target.uri == uri


@pytest.mark.parametrize("options", [{}, {"custom_option": True}])
def test_registered_filesystem_alias_preserves_backend_options(options: dict[str, Any]) -> None:
    registry = StorageRegistry()
    registry.register_alias("reports", "memory://", backend="fsspec", base_path="bucket/prefix", **options)
    target = _resolve_native_storage_target(
        SyncStoragePipeline(registry=registry),
        "alias://reports/file.parquet",
        {"_duckdb_storage_protocols": frozenset({"memory", "alias"})},
        write=True,
    )
    if options:
        assert target is None
    else:
        assert target is not None
        assert target.uri == "memory://bucket/prefix/file.parquet"


@pytest.fixture
def storage_settings() -> dict[str, Any]:
    return {
        "_duckdb_storage_extensions": frozenset({"httpfs"}),
        "_duckdb_storage_secrets": (
            {
                "secret_type": "s3",
                "name": "local_store",
                "scope": "s3://bucket/",
                "value": {
                    "key_id": "local-key",
                    "secret": "local-secret",
                    "region": "us-east-1",
                    "endpoint": "127.0.0.1:9000",
                    "url_style": "path",
                    "use_ssl": False,
                },
            },
        ),
    }


@pytest.fixture
def backend_options() -> dict[str, Any]:
    return {
        "aws_access_key_id": "local-key",
        "aws_secret_access_key": "local-secret",
        "aws_region": "us-east-1",
        "aws_endpoint": "http://127.0.0.1:9000",
        "aws_virtual_hosted_style_request": False,
        "allow_http": True,
    }


@pytest.mark.parametrize("write", [True, False])
def test_matching_local_provider_is_eligible(
    storage_settings: dict[str, Any], backend_options: dict[str, Any], write: bool
) -> None:
    assert _native_storage_eligible(
        "s3://bucket/prefix/file.parquet", "s3", backend_options, storage_settings, write=write
    )


@pytest.mark.parametrize("scheme", ["s3", "gs", "gcs", "r2", "az", "azure", "abfss"])
def test_configured_cloud_uri_resolves_offline_without_changing_spelling(scheme: str) -> None:
    provider = {"gs": "gcs", "gcs": "gcs", "az": "azure", "abfss": "azure"}.get(scheme, scheme)
    settings = {
        "_duckdb_storage_extensions": frozenset({"httpfs", "azure"}),
        "_duckdb_storage_secrets": (
            {
                "secret_type": provider,
                "value": {"connection_string": "DefaultEndpointsProtocol=https;AccountName=example;AccountKey=ZmFrZQ=="}
                if provider == "azure"
                else {},
            },
        ),
    }
    uri = f"{scheme}://bucket/prefix/file.parquet"
    target = _resolve_native_storage_target(SyncStoragePipeline(registry=StorageRegistry()), uri, settings, write=True)
    assert target is not None
    assert target.uri == uri
    assert target.protocol == scheme


def test_alias_named_gcs_retains_its_actual_backend(
    storage_settings: dict[str, Any], backend_options: dict[str, Any]
) -> None:
    registry = StorageRegistry()
    options = {key: value for key, value in backend_options.items() if key != "allow_http"}
    options["client_options"] = {"allow_http": True}
    registry.register_alias("gcs", "s3://bucket/prefix", **options)
    target = _resolve_native_storage_target(
        SyncStoragePipeline(registry=registry), "alias://gcs/file.parquet", storage_settings, write=True
    )
    assert target is not None
    assert target.uri == "s3://bucket/prefix/file.parquet"
    assert target.protocol == "s3"


@pytest.mark.parametrize("destination", ["s3://bucket/a?b", "s3://bucket/a#b", "file:///tmp/a", "local.parquet"])
def test_unrepresentable_object_names_and_local_paths_fall_back(
    storage_settings: dict[str, Any], destination: str
) -> None:
    assert (
        _resolve_native_storage_target(
            SyncStoragePipeline(registry=StorageRegistry()), destination, storage_settings, write=True
        )
        is None
    )


@pytest.mark.parametrize("protocol", ["file", "memory", "ftp", "unknown", ""])
def test_unsupported_protocol_uses_arrow(
    storage_settings: dict[str, Any], backend_options: dict[str, Any], protocol: str
) -> None:
    assert not _native_storage_eligible(
        "s3://bucket/file.parquet", protocol, backend_options, storage_settings, write=False
    )


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("aws_access_key_id", "different-key"),
        ("aws_secret_access_key", "different-secret"),
        ("aws_region", "eu-west-1"),
        ("aws_endpoint", "http://127.0.0.1:9001"),
        ("aws_virtual_hosted_style_request", True),
        ("allow_http", False),
        ("request_timeout", 1),
        ("aws_skip_signature", True),
    ],
)
def test_changed_or_unrepresented_backend_option_uses_arrow(
    storage_settings: dict[str, Any], backend_options: dict[str, Any], key: str, value: Any
) -> None:
    backend_options[key] = value
    assert not _native_storage_eligible("s3://bucket/file.parquet", "s3", backend_options, storage_settings, write=True)


def test_opaque_backend_uses_arrow(storage_settings: dict[str, Any]) -> None:
    assert not _native_storage_eligible("s3://bucket/file.parquet", "s3", None, storage_settings, write=False)


@pytest.mark.parametrize(
    ("protocol", "secret_type", "extension"),
    [
        ("s3", "s3", "httpfs"),
        ("gs", "gcs", "httpfs"),
        ("gcs", "gcs", "httpfs"),
        ("r2", "r2", "httpfs"),
        ("az", "azure", "azure"),
        ("azure", "azure", "azure"),
        ("abfss", "azure", "azure"),
    ],
)
@pytest.mark.parametrize("provider", ["config", "credential_chain"])
@pytest.mark.parametrize("write", [False, True])
def test_configured_cloud_provider_without_conflicting_backend_options(
    protocol: str, secret_type: str, extension: str, provider: str, write: bool
) -> None:
    settings = {
        "_duckdb_storage_extensions": frozenset({extension}),
        "_duckdb_storage_secrets": (
            {"secret_type": secret_type, "name": "cloud", "provider": provider, "scope": f"{protocol}://bucket/"},
        ),
    }
    uri = f"{protocol}://bucket/file.parquet"
    assert _native_storage_eligible(uri, protocol, {}, settings, write=write)
    assert not _native_storage_eligible(uri, protocol, {"unknown_option": True}, settings, write=write)


def test_gcs_oauth_does_not_match_native_hmac() -> None:
    settings = {
        "_duckdb_storage_extensions": frozenset({"httpfs"}),
        "_duckdb_storage_secrets": (
            {"secret_type": "gcs", "name": "gcs", "value": {"key_id": "hmac-id", "secret": "hmac-secret"}},
        ),
    }
    assert not _native_storage_eligible(
        "gs://bucket/file.parquet", "gs", {"service_account_key": "oauth-json"}, settings, write=False
    )


def test_azure_account_key_matches_connection_string() -> None:
    settings = {
        "_duckdb_storage_extensions": frozenset({"azure"}),
        "_duckdb_storage_secrets": (
            {
                "secret_type": "azure",
                "name": "azure",
                "value": {
                    "connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key==;EndpointSuffix=core.windows.net"
                },
            },
        ),
    }
    options = {"azure_storage_account_name": "test", "azure_storage_access_key": "key=="}
    assert _native_storage_eligible("az://bucket/file.parquet", "az", options, settings, write=True)
    options["azure_storage_access_key"] = "other=="
    assert not _native_storage_eligible("az://bucket/file.parquet", "az", options, settings, write=True)


def test_extension_intent_does_not_imply_loaded(backend_options: dict[str, Any]) -> None:
    assert not _native_storage_eligible(
        "s3://bucket/file.parquet", "s3", backend_options, {"extensions": [{"name": "httpfs"}]}, write=False
    )


def test_secret_scope_must_match(storage_settings: dict[str, Any], backend_options: dict[str, Any]) -> None:
    assert not _native_storage_eligible(
        "s3://other-bucket/file.parquet", "s3", backend_options, storage_settings, write=True
    )


def test_more_specific_secret_controls_credentials(
    storage_settings: dict[str, Any], backend_options: dict[str, Any]
) -> None:
    general = storage_settings["_duckdb_storage_secrets"][0]
    specific = {**general, "name": "specific", "scope": "s3://bucket/private/", "value": {"key_id": "other"}}
    storage_settings["_duckdb_storage_secrets"] += (specific,)
    assert not _native_storage_eligible(
        "s3://bucket/private/file.parquet", "s3", backend_options, storage_settings, write=False
    )


@pytest.mark.parametrize("protocol", ["http", "https"])
def test_public_http_reads_only(storage_settings: dict[str, Any], protocol: str) -> None:
    uri = f"{protocol}://127.0.0.1/file.parquet"
    assert _native_storage_eligible(uri, protocol, {}, storage_settings, write=False)
    assert not _native_storage_eligible(uri, protocol, {}, storage_settings, write=True)
    assert not _native_storage_eligible(
        uri, protocol, {"headers": {"Authorization": "x"}}, storage_settings, write=False
    )


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        42,
        "localhost:9000",
        "ftp://localhost",
        "https://host/prefix",
        "https://user:pass@host",
        "https://host?query=1",
    ],
)
def test_unsupported_endpoint_semantics_use_arrow(
    storage_settings: dict[str, Any], backend_options: dict[str, Any], endpoint: Any
) -> None:
    backend_options["aws_endpoint"] = endpoint
    assert not _native_storage_eligible(
        "s3://bucket/file.parquet", "s3", backend_options, storage_settings, write=False
    )


@pytest.mark.parametrize(
    "extra",
    [
        {"access_key_id": "conflicting-key"},
        {"client_options": {"allow_http": False}},
        {"client_options": {"proxy_url": "https://proxy"}},
        {"client_options": "opaque"},
    ],
)
def test_conflicting_aliases_or_custom_client_options_use_arrow(
    storage_settings: dict[str, Any], backend_options: dict[str, Any], extra: dict[str, Any]
) -> None:
    backend_options.update(extra)
    assert not _native_storage_eligible(
        "s3://bucket/file.parquet", "s3", backend_options, storage_settings, write=False
    )


def test_ambiguous_same_scope_secrets_use_arrow(
    storage_settings: dict[str, Any], backend_options: dict[str, Any]
) -> None:
    storage_settings["_duckdb_storage_secrets"] *= 2
    assert not _native_storage_eligible(
        "s3://bucket/file.parquet", "s3", backend_options, storage_settings, write=False
    )


def test_static_backend_keys_cannot_override_native_credential_chain(
    storage_settings: dict[str, Any], backend_options: dict[str, Any]
) -> None:
    storage_settings["_duckdb_storage_secrets"][0]["provider"] = "credential_chain"
    assert not _native_storage_eligible(
        "s3://bucket/file.parquet", "s3", backend_options, storage_settings, write=False
    )


@pytest.mark.parametrize(
    "connection_string",
    [
        "AccountName=test;AccountKey=key==;AccountName=other",
        "invalid",
        "AccountName=test;AccountKey=key==;BlobEndpoint=https://other",
    ],
)
def test_azure_custom_or_ambiguous_connection_strings_use_arrow(connection_string: str) -> None:
    settings = {
        "_duckdb_storage_extensions": frozenset({"azure"}),
        "_duckdb_storage_secrets": (
            {"secret_type": "azure", "name": "azure", "value": {"connection_string": connection_string}},
        ),
    }
    assert not _native_storage_eligible(
        "az://bucket/file.parquet", "az", {"account_name": "test", "access_key": "key=="}, settings, write=False
    )


def test_equal_azure_connection_strings_are_eligible() -> None:
    value = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key=="
    settings = {
        "_duckdb_storage_extensions": frozenset({"azure"}),
        "_duckdb_storage_secrets": ({"secret_type": "azure", "name": "azure", "value": {"connection_string": value}},),
    }
    assert _native_storage_eligible(
        "az://bucket/file.parquet", "az", {"connection_string": value}, settings, write=True
    )


@pytest.mark.parametrize(
    ("protocol", "provider_type", "endpoint", "extra"),
    [
        ("gs", "gcs", "https://storage.googleapis.com", {}),
        ("r2", "r2", "https://account.r2.cloudflarestorage.com", {"account_id": "account"}),
    ],
)
def test_known_cloud_endpoint_defaults_match_explicit_backend_options(
    protocol: str, provider_type: str, endpoint: str, extra: dict[str, str]
) -> None:
    settings = {
        "_duckdb_storage_extensions": frozenset({"httpfs"}),
        "_duckdb_storage_secrets": (
            {"secret_type": provider_type, "name": "cloud", "value": {"key_id": "id", "secret": "key", **extra}},
        ),
    }
    options = {"access_key_id": "id", "secret_access_key": "key", "endpoint": endpoint}
    assert _native_storage_eligible(f"{protocol}://bucket/file.parquet", protocol, options, settings, write=True)


def test_default_s3_endpoint_still_preserves_explicit_addressing_style() -> None:
    settings = {
        "_duckdb_storage_extensions": frozenset({"httpfs"}),
        "_duckdb_storage_secrets": (
            {"secret_type": "s3", "name": "cloud", "value": {"key_id": "id", "secret": "key", "url_style": "path"}},
        ),
    }
    options = {"access_key_id": "id", "secret_access_key": "key", "virtual_hosted_style_request": True}
    assert not _native_storage_eligible("s3://bucket/file.parquet", "s3", options, settings, write=True)
