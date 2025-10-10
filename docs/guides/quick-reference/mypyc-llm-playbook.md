# MyPyC LLM Playbook

Primer for agents compiling SQLSpec hot paths with MyPyC. Optimize build steps, target selection, and troubleshooting for minimal iteration time.

## When to Reach for MyPyC

- Tight loops in core query preparation (`sqlspec/core`, `sqlspec/driver`) show up in profiles.
- Serialization or parameter-munging utilities run on every request and allocate heavily.
- Adapter-specific helpers need C-level speed but must stay in Python for maintainability.
- Release builds require compiled wheels for distribution or benchmarking parity.

## Speed-First Habits

- Profile first with `py-spy`, `cProfile`, or `perf` and capture call counts before enabling MyPyC.
- Gate compilation via `HATCH_BUILD_HOOKS_ENABLE=1` so editable installs stay fast when you do not need C extensions.
- Keep functions under 75 lines and favor primitive types to minimize boxed operations in generated C.
- Store include/exclude globs in `pyproject.toml` once; reuse them in scripts instead of ad-hoc file lists.
- Use `uv pip install -e . --force-reinstall` after codegen changes to avoid stale `.so` binaries lingering in site-packages.

## Core Command Patterns

- `HATCH_BUILD_HOOKS_ENABLE=1 uv sync --all-extras --dev` compiles in editable mode with project extras.
- `HATCH_BUILD_HOOKS_ENABLE=1 uv build --wheel` emits a compiled distribution artifact for CI.
- `uv run mypyc path/to/module.py` quickly compiles a single module during experiments.
- `uv run pytest -n 2 --dist=loadgroup tests` validates compiled modules stay ABI compatible.
- `python -c "import sqlspec.core.statement; print(sqlspec.core.statement.__file__)"` confirms `.so` imports.

## Project Integration Hooks

- Include only hot modules via `[tool.hatch.build.targets.wheel.hooks.mypyc.include]`; keep configuration modules interpreted for flexibility.
- Mirror the default `mypy-args` from `pyproject.toml` when invoking mypycify to ensure identical type checking.
- Respect adapters’ optional dependencies—mark features behind `require-runtime-features = ["performance"]` so builds fail fast when extras missing.
- Use `sqlspec.utils.benchmarking` helpers for consistent before/after comparisons in docs or PR summaries.
- Document any new `@mypyc_attr` usages in code so future agents understand subclassing constraints.

## Common Pitfalls

- Compiling modules with dynamic imports or monkey patches; MyPyC cannot optimize them and may break behavior.
- Leaving `copy=True` semantics in builder mutations; MyPyC still pays for deep copies even after compilation.
- Failing to clean build artifacts (`rm -rf build dist .mypy_cache`) before timing—stale caches skew results.
- Mixing typed and untyped code within the same hot module; untyped functions drag the whole module back to Python API paths.
- Forgetting to run interpreted tests after compilation; some regressions only appear when falling back to pure Python.

## Retrieval Targets

- Documentation home: <https://mypyc.readthedocs.io/en/latest/>
- Getting started and prerequisites: <https://mypyc.readthedocs.io/en/latest/getting_started.html>
- Build hook reference: <https://mypyc.readthedocs.io/en/latest/build.html>
- Native class best practices: <https://mypyc.readthedocs.io/en/latest/native_classes.html>
- Debugging guide: <https://mypyc.readthedocs.io/en/latest/debugging.html>
- Source repository: <https://github.com/python/mypy>

## Ship Checklist

- Hot modules confirmed via profiling before toggling MyPyC include globs.
- `pyproject.toml` include/exclude kept in sync with new modules.
- Tests run in both interpreted (default `uv run pytest`) and compiled modes.
- Benchmark or perf notes captured when changes rely on MyPyC gains.
- Docs updated (this playbook, performance guide) whenever workflow changes.
