"""Fresh-process startup measurements. No SQLSpec or third-party parent imports.

Each phase is sequential and cumulative_ms includes all feature phases. Temporary
fixtures, harness imports, metadata inspection and cleanup are outside that timer.
Warm scenarios report a separate per-operation measurement after first use.
CLI help alone measures a complete process (including interpreter startup).
"""

# ruff: noqa: T201, S607

import argparse
import contextlib
import hashlib
import importlib.machinery
import io
import json
import math
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
CONFIG = "from sqlspec.adapters.sqlite import SqliteConfig; config = SqliteConfig(connection_config={'database': ':memory:'})"
QUERY = "with config.provide_session() as session:\n    assert session.execute('SELECT 1 AS value').data[0][0] == 1"
SERIALIZER = "from sqlspec.utils.serializers._json import encode_json"
SCENARIOS: dict[str, list[tuple[str, str]]] = {
    "bare": [("import", "import sqlspec")],
    "metadata": [("metadata", "import sqlspec; version = sqlspec.__version__")],
    "sqlspec": [("import", "from sqlspec import SQLSpec"), ("construct", "manager = SQLSpec()")],
    "sql": [("import", "from sqlspec import SQL"), ("construct", "statement = SQL('SELECT 1')")],
    "sqlite-import": [("import", "from sqlspec.adapters.sqlite import SqliteConfig")],
    "sqlite-config": [("config", CONFIG)],
    "sqlite-query": [
        ("config", CONFIG + "; from sqlspec import SQLSpec; manager = SQLSpec(); manager.add_config(config)"),
        ("first_query", QUERY),
    ],
    "aiosqlite-query": [
        ("import", "import asyncio; from sqlspec.adapters.aiosqlite import AiosqliteConfig"),
        (
            "first_query",
            "config = AiosqliteConfig(connection_config={'database': ':memory:'})\nasync def run():\n    async with config.provide_session() as session:\n        assert (await session.execute('SELECT 1 AS value')).data[0][0] == 1\nloop = asyncio.new_event_loop(); loop.run_until_complete(run())",
        ),
    ],
    "builder-query": [
        ("config", CONFIG),
        (
            "first_query",
            "from sqlspec import sql\nstatement = sql.select('1 AS value'); statement.enable_optimization = False\nwith config.provide_session() as session:\n    assert session.execute(statement).data[0][0] == 1",
        ),
    ],
    "optimized-query": [
        ("config", CONFIG),
        (
            "first_query",
            "from sqlspec import sql\nstatement = sql.select('1 AS value')\nwith config.provide_session() as session:\n    assert session.execute(statement).data[0][0] == 1",
        ),
    ],
    "primitive-json": [("import", SERIALIZER), ("first_encode", "encoded = encode_json({'value': 1})")],
    "asyncpg-uuid": [
        ("import", SERIALIZER),
        (
            "first_encode",
            "from asyncpg.pgproto.pgproto import UUID\nencoded = encode_json(UUID('12345678-1234-5678-1234-567812345678'))",
        ),
    ],
    "migration-loader": [("config", CONFIG), ("first_loader", "loader = config.get_migration_loader()")],
    "migration-commands": [("config", CONFIG), ("first_commands", "commands = config.get_migration_commands()")],
    "migration-run": [
        (
            "config",
            "from sqlspec.adapters.sqlite import SqliteConfig\nconfig = SqliteConfig(connection_config={'database': ':memory:'}, migration_config={'script_location': fixture_root})",
        ),
        (
            "first_migration",
            "commands = config.get_migration_commands(); commands.upgrade(); assert commands.current() == '0001'",
        ),
    ],
    "event-schema": [
        ("config", CONFIG),
        (
            "first_schema",
            "from sqlspec.adapters.sqlite.events import SqliteEventQueueStore\nstore = SqliteEventQueueStore(config)\nwith config.provide_session() as session:\n    store.prepare_schema_sync(session)\n    store.reconcile_schema_sync(session)",
        ),
    ],
    "cli-help": [
        (
            "cli_feature",
            "from sqlspec.cli import get_sqlspec_group; get_sqlspec_group()(['--help'], standalone_mode=False)",
        )
    ],
    "litestar-openapi": [
        ("config", CONFIG),
        (
            "first_openapi",
            "from litestar import Litestar, get\nfrom sqlspec import SQLSpec\nfrom sqlspec.extensions.litestar import SQLSpecPlugin\nmanager = SQLSpec(); manager.add_config(config)\n@get('/items')\nasync def items() -> list[dict[str, int]]:\n    return [{'value': 1}]\napp = Litestar(route_handlers=[items], plugins=[SQLSpecPlugin(sqlspec=manager)])\nassert app.openapi_schema.paths",
        ),
    ],
    "querybuilder-import": [("import", "from sqlspec.builder import QueryBuilder")],
    "migration-schema-import": [("import", "from sqlspec.migrations.schema import SchemaTarget")],
    "migration-tracker-import": [("import", "from sqlspec.migrations.tracker import SyncMigrationTracker")],
    "event-hints-import": [("import", "from sqlspec.extensions.events import EventRuntimeHints")],
    "warm-query": [("config", CONFIG), ("first_query", QUERY)],
    "warm-json": [("import", SERIALIZER), ("first_encode", "encoded = encode_json({'value': 1})")],
}
OPTIONAL = {
    "aiosqlite-query": "aiosqlite",
    "asyncpg-uuid": "asyncpg",
    "litestar-openapi": "litestar",
    "cli-help": "rich_click",
}


def manifest() -> dict[str, Any]:
    import importlib.metadata

    dependencies = {
        dist.metadata["Name"].lower(): dist.version
        for dist in importlib.metadata.distributions()
        if dist.metadata["Name"].lower() != "sqlspec"
    }
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "dependencies": dependencies,
    }


def child(scenario: str, root: str | None) -> dict[str, Any]:
    if root:
        sys.path.insert(0, root)
    namespace: dict[str, Any] = {"__name__": "__main__"}
    phases: dict[str, float] = {}
    before = set(sys.modules)
    if "sqlspec" in before:
        raise RuntimeError("SQLSpec was imported before the timer")
    with tempfile.TemporaryDirectory(prefix="sqlspec-bench-") as fixture:
        Path(fixture, "0001_bench.sql").write_text(
            "-- name: migrate-0001-up\nCREATE TABLE bench (id INTEGER PRIMARY KEY);\n-- name: migrate-0001-down\nDROP TABLE bench;\n"
        )
        namespace["fixture_root"] = fixture
        compiled = [(name, compile(code, "<benchmark>", "exec")) for name, code in SCENARIOS[scenario]]
        started = time.perf_counter_ns()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                for name, code in compiled:
                    phase_start = time.perf_counter_ns()
                    exec(code, namespace)  # noqa: S102
                    phases[name] = (time.perf_counter_ns() - phase_start) / 1e6
            elapsed = (time.perf_counter_ns() - started) / 1e6
            modules = sorted(set(sys.modules) - before)
            result: dict[str, Any] = {
                "status": "ok",
                "cumulative_ms": elapsed,
                "phases_ms": phases,
                "modules": modules,
                "module_count": len(modules),
                "preloaded_modules": sorted(before),
            }
            if scenario.startswith("warm-"):
                code = compile(QUERY if scenario == "warm-query" else "encode_json({'value': 1})", "<warm>", "exec")
                warm_start = time.perf_counter_ns()
                for _ in range(1000):
                    exec(code, namespace)  # noqa: S102
                result["warm_operation_ms"] = (time.perf_counter_ns() - warm_start) / 1e9
                result["warm_iterations"] = 1000
        except ModuleNotFoundError as exc:
            if scenario not in OPTIONAL or (exc.name or "").split(".")[0] != OPTIONAL[scenario]:
                raise
            result = {"status": "skipped", "reason": str(exc)}
        finally:
            config = namespace.get("config")
            if config is not None:
                closed = config.close_pool()
                if hasattr(closed, "__await__"):
                    import asyncio

                    loop = namespace.get("loop")
                    if loop is not None:
                        loop.run_until_complete(closed)
                        loop.close()
                    else:
                        asyncio.run(closed)
    module = sys.modules.get("sqlspec")
    origin = str(Path(module.__file__).resolve()) if module else None
    if origin and root and not Path(origin).is_relative_to(Path(root)):
        raise ValueError(f"Unexpected source origin: {origin}")
    if origin and not root and "site-packages" not in Path(origin).parts:
        raise ValueError(f"Expected installed wheel origin: {origin}")
    result.update(
        origin=origin,
        executable=sys.executable,
        environment=manifest(),
        sqlspec_version=getattr(module, "__version__", None),
        compiled_modules={
            name: item.__file__
            for name, item in sys.modules.copy().items()
            if name.startswith("sqlspec")
            and any(
                str(getattr(item, "__file__", "")).endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES
            )
        },
    )
    if root and result["compiled_modules"]:
        raise ValueError("Source mode loaded compiled SQLSpec modules; use a clean source checkout")
    return result


def sample(python: str, root: str | None, scenario: str) -> dict[str, Any]:
    command = [python, "-I", str(Path(__file__).resolve()), "--child", scenario]
    if root:
        command.extend(["--source-root", root])
    with tempfile.TemporaryDirectory(prefix="sqlspec-bench-cwd-") as cwd:
        process = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)
        if process.returncode:
            raise RuntimeError(process.stderr)
        result = json.loads(process.stdout)
        if scenario == "cli-help" and result["status"] == "ok":
            prefix = f"import sys; sys.path.insert(0, {root!r}); " if root else ""
            cli = [
                python,
                "-I",
                "-c",
                prefix + "from sqlspec.cli import get_sqlspec_group; get_sqlspec_group()(['--help'])",
            ]
            started = time.perf_counter_ns()
            subprocess.run(cli, cwd=cwd, capture_output=True, text=True, check=True)
            result.update(
                feature_cumulative_ms=result["cumulative_ms"],
                cumulative_ms=(time.perf_counter_ns() - started) / 1e6,
                timing_boundary="complete_cli_process",
            )
    for value in [
        result.get("cumulative_ms", 0),
        *result.get("phases_ms", {}).values(),
        result.get("warm_operation_ms", 0),
    ]:
        if not math.isfinite(value) or value < 0:
            raise ValueError("Nonfinite or negative timing")
    return result


def summarize(rows: list[dict[str, Any]], field: str) -> dict[str, float]:
    values = sorted(row[field] for row in rows if row["status"] == "ok" and field in row)
    if not values:
        return {}
    median = statistics.median(values)
    return {
        "p50": median,
        "p95": values[math.ceil(len(values) * 0.95) - 1],
        "mad": statistics.median(abs(value - median) for value in values),
    }


def provenance(python: str, root: str | None) -> dict[str, Any]:
    commit = subprocess.check_output(["git", "-C", root, "rev-parse", "HEAD"], text=True).strip() if root else None
    return {
        "python": python,
        "source_root": root,
        "isolated": root is None,
        "commit": commit,
        "git_status": subprocess.check_output(["git", "-C", root, "status", "--porcelain"], text=True)
        if root
        else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--source-root")
    parser.add_argument("--isolated", action="store_true")
    parser.add_argument("--baseline-python")
    parser.add_argument("--baseline-source-root")
    parser.add_argument("--baseline-isolated", action="store_true")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    parser.add_argument("--samples", type=int, default=31)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--batches", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--list-scenarios", action="store_true")
    parser.add_argument("--child", choices=SCENARIOS, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(args.child, args.source_root)))
        return
    if args.list_scenarios:
        print("\n".join(SCENARIOS))
        return
    if args.samples <= 0 or args.warmups < 0 or args.batches <= 0:
        parser.error("samples and batches must be positive; warmups must be nonnegative")
    if bool(args.source_root) == args.isolated:
        parser.error("require exactly one of --source-root or --isolated")
    if args.baseline_python:
        if bool(args.baseline_source_root) == args.baseline_isolated:
            parser.error("baseline requires exactly one source-root or isolated mode")
    elif args.baseline_source_root or args.baseline_isolated:
        parser.error("baseline mode requires --baseline-python")
    sides = {
        "candidate": provenance(
            str(Path(args.python).absolute()), str(Path(args.source_root).resolve()) if args.source_root else None
        )
    }
    if args.baseline_python:
        sides["baseline"] = provenance(
            str(Path(args.baseline_python).absolute()),
            str(Path(args.baseline_source_root).resolve()) if args.baseline_source_root else None,
        )
    scenarios = args.scenario or list(SCENARIOS)
    rows: list[dict[str, Any]] = []
    environments: dict[str, Any] = {}
    for batch in range(args.batches):
        for scenario in scenarios:
            for pair in range(-args.warmups, args.samples):
                order = list(sides) if pair % 2 else list(reversed(sides))
                for position, side in enumerate(order):
                    result = sample(sides[side]["python"], sides[side]["source_root"], scenario)
                    environment = result.pop("environment")
                    if environments and environment != next(iter(environments.values())):
                        parser.error("Python/platform/dependency environment mismatch")
                    environments[side] = environment
                    if pair >= 0:
                        rows.append(dict(result, batch=batch, pair=pair, order=position, side=side, scenario=scenario))
            print(f"batch {batch + 1}: {scenario}", file=sys.stderr, flush=True)
    report = {
        "schema_version": SCHEMA_VERSION,
        "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scenarios": scenarios,
        "provenance": sides,
        "environments": environments,
        "samples": rows,
        "summary": {
            side: {
                str(batch): {
                    scenario: {
                        field: summarize(
                            [
                                row
                                for row in rows
                                if row["side"] == side and row["batch"] == batch and row["scenario"] == scenario
                            ],
                            field,
                        )
                        for field in ("cumulative_ms", "warm_operation_ms")
                    }
                    for scenario in scenarios
                }
                for batch in range(args.batches)
            }
            for side in sides
        },
    }
    if args.compare:
        previous = json.loads(args.compare.read_text())
        if (
            previous.get("schema_version") != SCHEMA_VERSION
            or previous.get("scenarios") != scenarios
            or previous.get("environments", {}).get("candidate") != environments["candidate"]
        ):
            parser.error("comparison scenario/schema/environment mismatch")
        report["historical_comparison"] = {
            "report_only": True,
            "path": str(args.compare),
            "summary": previous["summary"],
        }
    encoded = json.dumps(report, indent=2, allow_nan=False)
    if args.output:
        args.output.write_text(encoded + "\n")
    else:
        print(encoded)


if __name__ == "__main__":
    main()
