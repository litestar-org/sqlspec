"""Clientless multi-cloud object storage integration fixtures for Floci GCS, Floci AWS S3, and Azurite."""

import contextlib
import email.utils
import http.client
import http.server
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any, ClassVar

import pytest
from pytest_databases.docker.floci import FlociGCPService
from pytest_databases.helpers import get_xdist_worker_num

from sqlspec.utils.module_loader import import_optional, import_optional_attr
from sqlspec.utils.serializers import from_json
from tests.fixtures.rustfs import ensure_s3_bucket

os.environ.setdefault("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false")

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_databases import ContainerService
    from pytest_databases.docker.azure_blob import AzureBlobService
    from pytest_databases.docker.floci import FlociAWSService
    from pytest_databases.types import ServiceContainer, XdistIsolationLevel

    from sqlspec.protocols import ObjectStoreProtocol
    from sqlspec.storage.backends.fsspec import FSSpecBackend
    from sqlspec.storage.backends.obstore import ObStoreBackend
else:
    FSSpecBackend = import_optional_attr("sqlspec.storage.backends.fsspec", "FSSpecBackend")
    ObStoreBackend = import_optional_attr("sqlspec.storage.backends.obstore", "ObStoreBackend")

fsspec_module: Any = import_optional("fsspec")
CoreGCSFileSystem: Any = import_optional_attr("gcsfs.core", "GCSFileSystem")

FLOCI_GCP_GATEWAY_PORT = 4588

__all__ = (
    "FLOCI_GCP_GATEWAY_PORT",
    "azurite_fsspec",
    "azurite_obstore",
    "floci_aws_s3_bucket_name",
    "floci_aws_s3_obstore",
    "floci_gcp_service",
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
def floci_gcp_service(
    container_service: "ContainerService",
    floci_gcp_image: str,
    floci_gcp_project: str,
    floci_resource_prefix: str,
    xdist_floci_isolation_level: "XdistIsolationLevel",
) -> "Generator[FlociGCPService, None, None]":
    """Start and yield a ready Floci GCP emulator service container on gateway port 4588."""
    worker_num = get_xdist_worker_num()
    container_name = (
        f"floci_gcp_{worker_num}" if worker_num is not None and xdist_floci_isolation_level == "server" else "floci_gcp"
    )
    transient = worker_num is not None and xdist_floci_isolation_level == "server"
    with (
        container_service.run(
            image=floci_gcp_image,
            name=container_name,
            container_port=FLOCI_GCP_GATEWAY_PORT,
            check=_is_floci_gcp_ready,
            timeout=60,
            pause=0.25,
            transient=transient,
        ) as service,
        _provide_floci_gcs_header_proxy(service.host, service.port) as proxy_port,
    ):
        yield FlociGCPService(
            container=service.container,
            host="127.0.0.1",
            port=proxy_port,
            endpoint_url=f"http://127.0.0.1:{proxy_port}",
            resource_prefix=floci_resource_prefix,
            project=floci_gcp_project,
        )


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
def floci_gcs_fsspec(floci_gcp_service: "FlociGCPService", floci_gcs_bucket_name: str) -> "ObjectStoreProtocol":
    """Provide an FSSpecBackend connected to the Floci GCP Storage emulator."""
    pytest.importorskip("fsspec")
    pytest.importorskip("gcsfs")
    if fsspec_module is not None and CoreGCSFileSystem is not None:
        fsspec_module.register_implementation("gs", CoreGCSFileSystem, clobber=True)
        fsspec_module.register_implementation("gcs", CoreGCSFileSystem, clobber=True)
    return FSSpecBackend(f"gs://{floci_gcs_bucket_name}", token="anon", endpoint_url=floci_gcp_service.endpoint_url)


@pytest.fixture(scope="session")
def floci_aws_s3_bucket_name(floci_aws_service: "FlociAWSService", floci_resource_prefix: str) -> str:
    """Ensure and return a worker-isolated S3 bucket name on the Floci AWS emulator."""
    bucket_name = f"{floci_resource_prefix}s3"
    return ensure_s3_bucket(
        floci_aws_service.endpoint_url,
        bucket_name,
        access_key=floci_aws_service.access_key,
        secret_key=floci_aws_service.secret_key,
        region=floci_aws_service.region,
    )


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


def _is_floci_gcp_ready(service: "ServiceContainer") -> bool:
    """Return True when the Floci GCP HTTP health endpoint responds with HTTP 200."""
    for path in ("/_floci-gcp/health", "/_floci/health", "/_localstack/health"):
        request = urllib.request.Request(f"http://{service.host}:{service.port}{path}", method="GET")
        try:
            with urllib.request.urlopen(request, timeout=2.0) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, TimeoutError, OSError):
            continue
    return False


class _FlociGCSHeaderProxyHandler(http.server.BaseHTTPRequestHandler):
    """Forward HTTP requests to Floci GCP while completing ETag and Last-Modified XML headers for obstore."""

    upstream_host: ClassVar[str] = "127.0.0.1"
    upstream_port: ClassVar[int] = FLOCI_GCP_GATEWAY_PORT

    def do_GET(self) -> None:
        """Forward GET requests."""
        self._forward_request()

    def do_HEAD(self) -> None:
        """Forward HEAD requests."""
        self._forward_request()

    def do_POST(self) -> None:
        """Forward POST requests."""
        self._forward_request()

    def do_PUT(self) -> None:
        """Forward PUT requests."""
        self._forward_request()

    def do_DELETE(self) -> None:
        """Forward DELETE requests."""
        self._forward_request()

    def do_PATCH(self) -> None:
        """Forward PATCH requests."""
        self._forward_request()

    def log_message(self, format: str, *args: object) -> None:
        """Suppress per-request stderr logging during test runs."""
        del format, args

    def _forward_request(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(content_length) if content_length > 0 else None
        upstream_method, upstream_path, forwarded_headers = self._translate_request()
        connection = http.client.HTTPConnection(self.upstream_host, self.upstream_port, timeout=30)
        try:
            connection.request(upstream_method, upstream_path, body=request_body, headers=forwarded_headers)
            upstream_response = connection.getresponse()
            response_body = upstream_response.read()
            response_headers = list(upstream_response.getheaders())
            status_code = upstream_response.status
        finally:
            connection.close()

        lower_header_names = {key.lower() for key, _ in response_headers}
        self.send_response(status_code)
        for key, value in response_headers:
            if key.lower() in {"connection", "content-length", "date", "server", "transfer-encoding"}:
                continue
            self.send_header(key, value)

        if 200 <= status_code < 300:
            if "etag" not in lower_header_names:
                self.send_header("ETag", self._extract_etag(response_body))
            if "last-modified" not in lower_header_names:
                self.send_header("Last-Modified", self._format_last_modified(response_headers))

        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(response_body)

    def _translate_request(self) -> "tuple[str, str, dict[str, str]]":
        excluded_headers = {"connection", "transfer-encoding"}
        copy_source = self.headers.get("x-goog-copy-source")
        if self.command == "PUT" and copy_source is not None:
            src_parts = urllib.parse.unquote(copy_source.lstrip("/")).split("/", 1)
            dest_parts = urllib.parse.unquote(urllib.parse.urlsplit(self.path).path.lstrip("/")).split("/", 1)
            if len(src_parts) == 2 and len(dest_parts) == 2:
                src_bucket, src_key = src_parts
                dest_bucket, dest_key = dest_parts
                copy_path = (
                    f"/storage/v1/b/{urllib.parse.quote(src_bucket, safe='')}"
                    f"/o/{urllib.parse.quote(src_key, safe='')}"
                    f"/copyTo/b/{urllib.parse.quote(dest_bucket, safe='')}"
                    f"/o/{urllib.parse.quote(dest_key, safe='')}"
                )
                excluded_headers = {"connection", "content-length", "transfer-encoding", "x-goog-copy-source"}
                headers = {key: val for key, val in self.headers.items() if key.lower() not in excluded_headers}
                headers["Content-Length"] = "0"
                return "POST", copy_path, headers
        headers = {key: val for key, val in self.headers.items() if key.lower() not in excluded_headers}
        return self.command, self.path, headers

    @staticmethod
    def _extract_etag(response_body: bytes) -> str:
        if response_body.startswith(b"{"):
            with contextlib.suppress(Exception):
                parsed = from_json(response_body.decode("utf-8", errors="replace"))
                if isinstance(parsed, dict) and isinstance(parsed.get("etag"), str):
                    return f'"{parsed["etag"]}"'
        return '"floci"'

    @staticmethod
    def _format_last_modified(response_headers: "list[tuple[str, str]]") -> str:
        generation = next((val for key, val in response_headers if key.lower() == "x-goog-generation"), None)
        timestamp = int(generation) / 1000.0 if generation is not None and generation.isdigit() else time.time()
        return email.utils.formatdate(timestamp, usegmt=True)


@contextlib.contextmanager
def _provide_floci_gcs_header_proxy(upstream_host: str, upstream_port: int) -> "Generator[int, None, None]":
    """Run a background ThreadingHTTPServer normalizing Floci GCP XML headers for obstore compatibility."""
    handler_cls = type(
        "_BoundFlociGCSHeaderProxyHandler",
        (_FlociGCSHeaderProxyHandler,),
        {"upstream_host": upstream_host, "upstream_port": upstream_port},
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    proxy_port = int(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, name="floci-gcs-header-proxy", daemon=True)
    thread.start()
    try:
        yield proxy_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
