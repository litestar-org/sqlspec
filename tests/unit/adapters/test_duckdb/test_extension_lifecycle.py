"""Call-count tests for DuckDB extension install/load lifecycle.

These tests pin the install-vs-load contract without touching the network or
relying on timing. A spy connection records every install/load call so we can
assert exact call counts:

* name-only ``{"name": X}`` => LOAD-ONLY (install never called)
* explicit install (``install=True`` / ``force_install`` / version / repository
  / repository_url) => install runs once per pool per signature
* ``load_extension`` runs per physical connection
* best-effort by default; ``required=True`` raises
"""

import logging
import threading
from typing import Any

import pytest

from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool

pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")


class _SpyConnection:
    """Fake DuckDB connection recording install/load calls into shared logs."""

    def __init__(
        self,
        install_log: "list[tuple[str, dict[str, Any]]]",
        load_log: "list[str]",
        *,
        fail_install: bool = False,
        fail_load: bool = False,
    ) -> None:
        self._install_log = install_log
        self._load_log = load_log
        self._fail_install = fail_install
        self._fail_load = fail_load

    def install_extension(self, extension: str, **kwargs: Any) -> None:
        self._install_log.append((extension, kwargs))
        if self._fail_install:
            msg = "install failed"
            raise RuntimeError(msg)

    def load_extension(self, extension: str) -> None:
        if self._fail_load:
            msg = "load failed"
            raise RuntimeError(msg)
        self._load_log.append(extension)

    def execute(self, sql: str, parameters: Any = None) -> "_SpyConnection":
        return self

    def fetchone(self) -> "tuple[Any, ...] | None":
        return None

    def cursor(self) -> "_SpyConnection":
        return self

    def close(self) -> None:
        pass


def _spy_connect(
    monkeypatch: pytest.MonkeyPatch, *, fail_install: bool = False, fail_load: bool = False
) -> "tuple[list[tuple[str, dict[str, Any]]], list[str]]":
    """Patch ``duckdb.connect`` to return spies sharing install/load logs."""
    install_log: list[tuple[str, dict[str, Any]]] = []
    load_log: list[str] = []

    def fake_connect(**_: Any) -> _SpyConnection:
        return _SpyConnection(install_log, load_log, fail_install=fail_install, fail_load=fail_load)

    monkeypatch.setattr("sqlspec.adapters.duckdb.pool.duckdb.connect", fake_connect)
    return install_log, load_log


def test_name_only_extension_never_installs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test A: name-only extensions LOAD only, never install."""
    install_log, load_log = _spy_connect(monkeypatch)
    pool = DuckDBConnectionPool({"database": ":memory:"}, extensions=[{"name": "postgres"}])

    pool._create_connection()

    assert install_log == []
    assert load_log == ["postgres"]


def test_explicit_install_runs_once_across_sessions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Test B: explicit install runs once per pool even across reconnects."""
    install_log, load_log = _spy_connect(monkeypatch)
    pool = DuckDBConnectionPool(
        {"database": str(tmp_path / "assessment.db")}, extensions=[{"name": "postgres", "install": True}]
    )

    for _ in range(3):
        pool._create_connection()

    assert len(install_log) == 1
    assert load_log == ["postgres", "postgres", "postgres"]


def test_concurrent_sessions_do_not_multiply_installs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test C: 8 concurrent connection builds install exactly once."""
    install_log, load_log = _spy_connect(monkeypatch)
    pool = DuckDBConnectionPool({"database": ":memory:"}, extensions=[{"name": "postgres", "install": True}])

    threads = [threading.Thread(target=pool._create_connection) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(install_log) == 1
    assert len(load_log) == 8


@pytest.mark.parametrize(
    "extension",
    [{"name": "h3", "version": "1.0"}, {"name": "h3", "repository": "community"}, {"name": "h3", "repository_url": "https://ext.example.test"}],
)
def test_version_repository_imply_install(monkeypatch: pytest.MonkeyPatch, extension: "dict[str, Any]") -> None:
    """Test D: version/repository/repository_url imply an explicit install."""
    install_log, load_log = _spy_connect(monkeypatch)
    pool = DuckDBConnectionPool({"database": ":memory:"}, extensions=[extension])

    pool._create_connection()

    assert len(install_log) == 1
    assert install_log[0][0] == "h3"
    assert load_log == ["h3"]


def test_load_failure_is_best_effort_by_default(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Test E (load): a failing LOAD is swallowed with a WARNING by default."""
    _spy_connect(monkeypatch, fail_load=True)
    pool = DuckDBConnectionPool({"database": ":memory:"}, extensions=[{"name": "postgres"}])

    with caplog.at_level(logging.WARNING):
        pool._create_connection()

    assert any("load" in record.message and record.levelno == logging.WARNING for record in caplog.records)


def test_load_failure_raises_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test E (load): a failing LOAD raises when required=True."""
    _spy_connect(monkeypatch, fail_load=True)
    pool = DuckDBConnectionPool({"database": ":memory:"}, extensions=[{"name": "postgres", "required": True}])

    with pytest.raises(RuntimeError, match="load failed"):
        pool._create_connection()


def test_install_failure_is_best_effort_by_default(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Test E (install): a failing INSTALL is swallowed with a WARNING by default."""
    _spy_connect(monkeypatch, fail_install=True)
    pool = DuckDBConnectionPool({"database": ":memory:"}, extensions=[{"name": "postgres", "install": True}])

    with caplog.at_level(logging.WARNING):
        pool._create_connection()

    assert any("install" in record.message and record.levelno == logging.WARNING for record in caplog.records)


def test_install_failure_raises_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test E (install): a failing INSTALL raises when required=True."""
    _spy_connect(monkeypatch, fail_install=True)
    pool = DuckDBConnectionPool(
        {"database": ":memory:"}, extensions=[{"name": "postgres", "install": True, "required": True}]
    )

    with pytest.raises(RuntimeError, match="install failed"):
        pool._create_connection()
