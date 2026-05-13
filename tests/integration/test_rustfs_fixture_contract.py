"""Tests for the RustFS-backed S3 integration fixture contract."""

from pathlib import Path

import tomli

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ROOT_CONFTEST = PROJECT_ROOT / "tests" / "conftest.py"
INTEGRATION_CONFTEST = PROJECT_ROOT / "tests" / "integration" / "conftest.py"
LIVE_TEST_PATHS = (
    ROOT_CONFTEST,
    INTEGRATION_CONFTEST,
    PROJECT_ROOT / "tests" / "integration" / "storage",
    PROJECT_ROOT / "tests" / "integration" / "adapters" / "_storage_bridge_helpers.py",
    PROJECT_ROOT / "tests" / "integration" / "adapters" / "contracts" / "test_storage_bridge.py",
)
STALE_MINIO_MARKERS = (
    "from minio import",
    "MinioService",
    "minio_client",
    "minio_default_bucket_name",
    "pytest_databases.docker.minio",
    "register_minio_alias",
)


def _read_live_test_text() -> str:
    parts: list[str] = []
    for path in LIVE_TEST_PATHS:
        if path.is_dir():
            parts.extend(child.read_text(encoding="utf-8") for child in sorted(path.rglob("*.py")))
        else:
            parts.append(path.read_text(encoding="utf-8"))
    return "\n".join(parts)


def test_pytest_databases_test_extra_uses_rustfs_capable_release() -> None:
    pyproject = tomli.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    test_deps = pyproject["dependency-groups"]["test"]
    pytest_databases_deps = [dep for dep in test_deps if dep.startswith("pytest-databases")]

    assert pytest_databases_deps == ["pytest-databases[postgres,oracle,bigquery,spanner]>=0.18.0"]


def test_live_tests_use_rustfs_fixture_instead_of_minio_client() -> None:
    live_test_text = _read_live_test_text()
    root_conftest_text = ROOT_CONFTEST.read_text(encoding="utf-8")

    assert "rustfs" not in root_conftest_text.lower()
    assert "pytest_databases.docker.rustfs" in live_test_text
    assert "rustfs_service" in live_test_text
    assert "rustfs_default_bucket_name" in live_test_text
    for marker in STALE_MINIO_MARKERS:
        assert marker not in live_test_text
