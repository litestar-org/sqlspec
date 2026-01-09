"""Unit tests for observability common helpers."""

import builtins
import hashlib
import sys
from types import ModuleType

from sqlspec.observability import compute_sql_hash, get_trace_context, resolve_db_system


def test_resolve_db_system_asyncpg() -> None:
    assert resolve_db_system("AsyncpgDriver") == "postgresql"


def test_resolve_db_system_sqlite() -> None:
    assert resolve_db_system("SqliteDriver") == "sqlite"
    assert resolve_db_system("AiosqliteDriver") == "sqlite"


def test_resolve_db_system_unknown() -> None:
    assert resolve_db_system("UnknownDriver") == "other_sql"


def test_compute_sql_hash() -> None:
    sql = "SELECT 1"
    expected = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]
    assert compute_sql_hash(sql) == expected


def test_get_trace_context_without_otel(monkeypatch) -> None:
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "opentelemetry":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert get_trace_context() == (None, None)


def test_get_trace_context_with_otel(monkeypatch) -> None:
    opentelemetry_module = ModuleType("opentelemetry")
    trace_module = ModuleType("opentelemetry.trace")

    class FakeSpanContext:
        is_valid = True
        trace_id = int("0" * 31 + "1", 16)
        span_id = int("0" * 15 + "2", 16)

    class FakeSpan:
        def is_recording(self) -> bool:
            return True

        def get_span_context(self) -> "FakeSpanContext":
            return FakeSpanContext()

    def get_current_span() -> "FakeSpan":
        return FakeSpan()

    setattr(trace_module, "get_current_span", get_current_span)
    setattr(opentelemetry_module, "trace", trace_module)

    monkeypatch.setitem(sys.modules, "opentelemetry", opentelemetry_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace_module)

    trace_id, span_id = get_trace_context()
    assert trace_id == "00000000000000000000000000000001"
    assert span_id == "0000000000000002"
