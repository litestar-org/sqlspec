"""Unit coverage for Oracle JSON benchmark scenario registration."""

from collections.abc import Callable

import pytest
from tools.scripts import bench


def test_oracle_json_scenarios_are_registered() -> None:
    """Every Oracle JSON benchmark resolves to its public callable."""
    expected: dict[tuple[str, str, str], Callable[[], None]] = {
        ("sqlspec_native_json", "oracle", "json_write"): bench.sqlspec_oracle_native_json_write,
        ("sqlspec_serialized_json", "oracle", "json_write"): bench.sqlspec_oracle_serialized_json_write,
        ("sqlspec", "oracle", "json_read"): bench.sqlspec_oracle_json_read,
    }

    for key, scenario in expected.items():
        assert bench.SCENARIO_REGISTRY[key] is scenario
        assert (key[0], key[2]) in bench.ORACLE_EXTENDED_SCENARIOS


def test_oracle_json_rows_distinguish_native_and_serialized_payloads() -> None:
    """Native and serialized writes use equivalent payloads with distinct bind types."""
    native_rows = bench._oracle_json_rows(serialized=False)
    serialized_rows = bench._oracle_json_rows(serialized=True)

    assert len(native_rows) == len(serialized_rows) == bench.ORACLE_JSON_ROWS
    assert isinstance(native_rows[0][1], dict)
    assert isinstance(serialized_rows[0][1], str)
    assert native_rows[0][1] == bench.ORACLE_JSON_PAYLOAD


def test_oracle_json_public_wrappers_delegate(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Public scenarios select the intended write/read runner mode."""
    calls: list[tuple[str, bool | None]] = []

    def fake_write(*, serialized: bool) -> None:
        calls.append(("write", serialized))

    def fake_read() -> None:
        calls.append(("read", None))

    monkeypatch.setattr(bench, "_run_sqlspec_oracle_json_write", fake_write)
    monkeypatch.setattr(bench, "_run_sqlspec_oracle_json_read", fake_read)

    bench.sqlspec_oracle_native_json_write()
    bench.sqlspec_oracle_serialized_json_write()
    bench.sqlspec_oracle_json_read()

    assert calls == [("write", False), ("write", True), ("read", None)]
