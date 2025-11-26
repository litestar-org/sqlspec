from pathlib import Path

import pytest

from docs.examples.usage.usage_sql_files_1 import create_loader

__all__ = ("test_cloud_storage_loading", )


@pytest.mark.skipif(reason="need to find a way to mock cloud storage")
def test_cloud_storage_loading(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)
    # start-example
    # S3
    loader.load_sql("s3://my-bucket/sql/users.sql")

    # Google Cloud Storage
    loader.load_sql("gs://my-bucket/sql/users.sql")

    # Azure Blob Storage
    loader.load_sql("az://my-container/sql/users.sql")

    # HTTP/HTTPS
    loader.load_sql("https://example.com/queries/users.sql")
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
