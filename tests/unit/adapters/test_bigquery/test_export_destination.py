"""Native BigQuery export URI and format contracts."""

from typing import Any

import pytest

from sqlspec.adapters.bigquery.core import _build_export_uri, _resolve_export_format
from sqlspec.exceptions import ImproperConfigurationError


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("gs://bucket/out.parquet", "gs://bucket/out-*.parquet"),
        ("gcs://bucket/out.parquet", "gs://bucket/out-*.parquet"),
        ("gs://bucket/out", "gs://bucket/out-*"),
        ("gs://bucket/prefix/", "gs://bucket/prefix/part-*.parquet"),
        ("gs://bucket", "gs://bucket/part-*.parquet"),
        ("gs://bucket/", "gs://bucket/part-*.parquet"),
        ("gs://bucket/out-*.parquet", "gs://bucket/out-*.parquet"),
        ("gs://bucket/*", "gs://bucket/*"),
        ("gs://bucket/prefix/a*b.parquet", "gs://bucket/prefix/a*b.parquet"),
        ("gs://bucket/out.tar.gz", "gs://bucket/out.tar-*.gz"),
        ("gs://bucket/.hidden", "gs://bucket/.hidden-*"),
        ("gs://bucket/out.", "gs://bucket/out.-*"),
        ("s3://bucket/out.csv", "s3://bucket/out-*.csv"),
        (
            "azure://account.blob.core.windows.net/container/out.jsonl",
            "azure://account.blob.core.windows.net/container/out-*.jsonl",
        ),
        (
            "azure://account.blob.core.windows.net/container/",
            "azure://account.blob.core.windows.net/container/part-*.parquet",
        ),
        (
            "azure://account.blob.core.windows.net/container",
            "azure://account.blob.core.windows.net/container/part-*.parquet",
        ),
    ],
)
def test_export_uri_has_one_leaf_wildcard(uri: str, expected: str) -> None:
    assert _build_export_uri(uri) == expected
    assert expected.count("*") == 1
    assert "*" in expected.rsplit("/", 1)[-1]


@pytest.mark.parametrize(
    ("format_hint", "native_format", "extension"),
    [
        (None, "PARQUET", "parquet"),
        ("parquet", "PARQUET", "parquet"),
        ("csv", "CSV", "csv"),
        ("json", "JSON", "jsonl"),
        ("jsonl", "JSON", "jsonl"),
    ],
)
def test_export_format_and_directory_extension(format_hint: Any, native_format: str, extension: str) -> None:
    assert _resolve_export_format(format_hint) == native_format
    assert _build_export_uri("gs://bucket/prefix/", format_hint) == f"gs://bucket/prefix/part-*.{extension}"


def test_export_format_does_not_infer_encoding_from_filename() -> None:
    assert _resolve_export_format(None) == "PARQUET"
    assert _build_export_uri("gs://bucket/out.csv") == "gs://bucket/out-*.csv"
    assert _build_export_uri("gs://bucket/out.parquet", "csv") == "gs://bucket/out-*.parquet"


@pytest.mark.parametrize("format_hint", ["arrow-ipc", "avro", "unknown"])
def test_unsupported_native_export_format_selects_fallback(format_hint: Any) -> None:
    assert _resolve_export_format(format_hint) is None
    with pytest.raises(ImproperConfigurationError, match="format"):
        _build_export_uri("gs://bucket/", format_hint)


@pytest.mark.parametrize(
    "uri",
    [
        "",
        "gs:///out.parquet",
        "gs://*/out.parquet",
        "gs://bucket/pre*/out.parquet",
        "gs://bucket/a**.parquet",
        "gs://bucket/*/",
        "gs://bucket/out?token=value",
        "gs://bucket/out?",
        "gs://bucket/out#fragment",
        "gs://bucket/out#",
        "gs://bucket/out'file.parquet",
        'gs://bucket/out"file.parquet',
        "gs://bucket/out\\file.parquet",
        "gs://bucket/out\nfile.parquet",
        "gs://bucket/out\tfile.parquet",
        "gs://bucket/out\x00file.parquet",
        "gs://user@bucket/out.parquet",
        "gs://bucket:443/out.parquet",
        "file:///tmp/out.parquet",
        "az://container/out.parquet",
        "abfss://container/out.parquet",
        "azure://container/out.parquet",
        "azure://account.blob.core.windows.net/",
        "azure://account.blob.core.windows.net",
        "azure://account.blob.core.windows.net/con*ainer/out.parquet",
        "gs://bucket/out\x7ffile.parquet",
    ],
)
def test_export_uri_rejects_invalid_or_unsafe_destination(uri: str) -> None:
    with pytest.raises(ImproperConfigurationError):
        _build_export_uri(uri)
