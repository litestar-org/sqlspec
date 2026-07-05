"""Service-backed smoke tests for Oracle benchmark scenarios."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner
from pytest_databases.docker.oracle import OracleService
from tools.scripts import bench

pytestmark = pytest.mark.xdist_group("oracle")


ORACLE_LOB_SCENARIOS = {
    ("raw", "oracle", "lob_fetch_1k"): "raw_oracle_lob_fetch_1k",
    ("raw", "oracle", "lob_fetch_100k"): "raw_oracle_lob_fetch_100k",
    ("sqlspec", "oracle", "lob_fetch_1k"): "sqlspec_oracle_lob_fetch_1k",
    ("sqlspec", "oracle", "lob_fetch_100k"): "sqlspec_oracle_lob_fetch_100k",
    ("sqlspec_fetch_lobs_true", "oracle", "lob_fetch_1k"): "sqlspec_oracle_lob_fetch_fetch_lobs_true_1k",
    ("sqlspec_fetch_lobs_true", "oracle", "lob_fetch_100k"): "sqlspec_oracle_lob_fetch_fetch_lobs_true_100k",
    ("sqlspec_async", "oracle", "lob_fetch_1k"): "sqlspec_oracle_lob_fetch_async_1k",
    ("sqlspec_async", "oracle", "lob_fetch_100k"): "sqlspec_oracle_lob_fetch_async_100k",
    ("sqlspec_async_fetch_lobs_true", "oracle", "lob_fetch_1k"): "sqlspec_oracle_lob_fetch_async_fetch_lobs_true_1k",
    (
        "sqlspec_async_fetch_lobs_true",
        "oracle",
        "lob_fetch_100k",
    ): "sqlspec_oracle_lob_fetch_async_fetch_lobs_true_100k",
}


def _configure_oracle_bench_env(monkeypatch: pytest.MonkeyPatch, oracle_service: OracleService) -> None:
    monkeypatch.setenv("SQLSPEC_BENCH_ORACLE_HOST", oracle_service.host)
    monkeypatch.setenv("SQLSPEC_BENCH_ORACLE_PORT", str(oracle_service.port))
    monkeypatch.setenv("SQLSPEC_BENCH_ORACLE_SERVICE_NAME", oracle_service.service_name)
    monkeypatch.setenv("SQLSPEC_BENCH_ORACLE_USER", oracle_service.user)
    monkeypatch.setenv("SQLSPEC_BENCH_ORACLE_PASSWORD", oracle_service.password)
    monkeypatch.setattr(bench, "ORACLE_LOB_ROWS", 2)
    monkeypatch.setattr(bench, "ORACLE_LOB_PAYLOAD_SIZES", {"1k": 128, "100k": 256})


def test_oracle_lob_benchmark_scenarios_are_registered() -> None:
    for key, function_name in ORACLE_LOB_SCENARIOS.items():
        assert bench.SCENARIO_REGISTRY[key] is getattr(bench, function_name)
        assert function_name in bench.__all__


def test_oracle_lob_benchmark_scenarios_run_with_service_config(
    monkeypatch: pytest.MonkeyPatch, oracle_23ai_service: OracleService
) -> None:
    _configure_oracle_bench_env(monkeypatch, oracle_23ai_service)

    errors: list[str] = []
    results = bench.run_extended_benchmark("oracle", errors, iterations=1, warmup=0)

    assert errors == []
    assert {(result["library"], result["driver"], result["scenario"]) for result in results} == {
        (bench._benchmark_library_label(library), driver, scenario)
        for library, driver, scenario in ORACLE_LOB_SCENARIOS
    }
    assert len(results) == len(ORACLE_LOB_SCENARIOS)


def test_oracle_extended_cli_runs_without_core_missing_scenario_errors(
    monkeypatch: pytest.MonkeyPatch, oracle_23ai_service: OracleService, tmp_path: Path
) -> None:
    _configure_oracle_bench_env(monkeypatch, oracle_23ai_service)
    json_output = tmp_path / "oracle-bench.json"

    result = CliRunner().invoke(
        bench.main,
        ["--driver", "oracle", "--extended", "--iterations", "1", "--warmup", "0", "--json-output", str(json_output)],
    )

    assert result.exit_code == 0
    assert "No implementation" not in result.output
    payload = json.loads(json_output.read_text())
    assert {result["scenario"] for result in payload["results"]} == {"lob_fetch_1k", "lob_fetch_100k"}


def test_oracle_lob_benchmark_connection_config_requires_service_safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "SQLSPEC_BENCH_ORACLE_HOST",
        "SQLSPEC_BENCH_ORACLE_PORT",
        "SQLSPEC_BENCH_ORACLE_SERVICE_NAME",
        "SQLSPEC_BENCH_ORACLE_USER",
        "SQLSPEC_BENCH_ORACLE_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError, match="SQLSPEC_BENCH_ORACLE_HOST"):
        bench._oracle_connection_config_from_env()
