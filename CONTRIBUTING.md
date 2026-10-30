# Contribution Guide

## Setting Up the Environment

1. Run `make install-uv` to install [uv](https://docs.astral.sh/uv/) if not already installed.
2. Run `make install` to create the virtual environment and install all development dependencies.

## Code Contributions

### Workflow

1. [Fork](https://github.com/litestar-org/sqlspec/fork) the [sqlspec repository](https://github.com/litestar-org/sqlspec).
2. Clone your fork locally with `git`.
3. [Set up the environment](#setting-up-the-environment).
4. Create a dedicated feature or bugfix branch.
5. Make your code changes and write tests.
6. Run `make lint` to run code formatters, Prek hooks, type checkers, `slotscheck`, and workflow audits. Always run this before committing; project setup intentionally does not install Git hook shims.
7. Commit your changes following the [Conventional Commit format](https://www.conventionalcommits.org) (for example, `fix(#100): resolve connection pooling leak` or `feat: add database adapter feature`).
8. Push the changes to your fork.
9. Open a [pull request](https://docs.github.com/en/pull-requests) with a descriptive title indicating what it changes and the corresponding issue number if applicable.

!!! tip
    Pull requests and commits all follow the [Conventional Commit format](https://www.conventionalcommits.org).

## Guidelines for Writing Code

- **Python Version & Typing**:
  - Python `3.11+` baseline (`>=3.11, <4.0`).
  - All code must be fully typed (enforced via [mypy](https://mypy.readthedocs.io/en/stable/) and [pyright](https://microsoft.github.io/pyright/)). Avoid `Any` whenever possible.
  - Use PEP 585 built-in collection types (`dict`, `list`, `tuple`, `set`) rather than their `typing` module equivalents.
  - Use PEP 604 union types (`T | None`), never `Optional[T]` or `Union[T, None]`.
  - Package modules must **not** import `from __future__ import annotations` (enforced via Ruff rule `TID251`).

- **Comments & Documentation**:
  - **Never use inline `#` comments in Python code**. If code requires explanation, document the rationale in the class or function docstring.
  - Use Google-style docstrings for public classes and functions with structured `Args:`, `Returns:`, and `Raises:` blocks, and document every field on public `TypedDict` and `msgspec.Struct` schemas with inline PEP 257 attribute docstrings.
  - Explain *why* something is done in a specific way, not merely *what* the code does.

- **Naming Conventions**:
  - Functions, methods, and variables: `snake_case`.
  - Classes, exceptions, and protocols: `PascalCase`.
  - Constants and module-level singletons: `SCREAMING_SNAKE_CASE`.

- **SQL Safety**:
  - SQL queries must **always** use parameterized placeholders (`:param`, `?`, `$1`, or `@param`).
  - Never use f-strings, string concatenation, or `%`/`.format()` formatting to construct SQL statements with dynamic inputs.

- **Linting & Code Quality**:
  - Formatting and linting are handled via Ruff and Prek.
  - `make lint` applies Ruff fixes and formatting, runs every Prek hook, checks types and slots, and audits workflows with `zizmor`.
  - Dependency resolution uses a seven-day cooldown by default, with Litestar ecosystem packages exempt so coordinated releases remain testable.

### Logging

- Logger names must follow the `sqlspec.<module>` hierarchy.
- Always obtain loggers via `sqlspec.utils.logging.get_logger` to ensure correlation and redaction filters are attached.
- Use static event names in structured logs and include context fields instead of dynamic message strings.

### Writing and Running Tests

- **Test Style**: Use `pytest` function-based tests (`test_*` or `async def test_*`). Do not use test classes.
- **Test Placement**:
  - Put behavior shared by adapters in the contract suite (`tests/integration/adapters/<database>/test_shared.py`).
  - Keep vendor-only cases in that adapter's test folder (`tests/integration/adapters/<database>/<driver>/`).
  - Use unit tests (`tests/unit/`) for code that does not need a database (parsing, config normalization, parameter formatting).
  - Review the [test placement guide](https://github.com/litestar-org/sqlspec/blob/main/tests/README.md) for detailed fixture conventions.
- **Integration Resources**:
  - Integration tests use `pytest-databases` fixtures to provision and manage database containers automatically.
  - The IBM Db2 tests start the Db2 Community Edition 11.5.9 image as a privileged container, so Docker must allow privileged containers.
- **Coverage Requirements**:
  - The repository-wide coverage floor is 76%.
  - Every new commit must keep changed-code coverage at or above **90%**.
  - Detect and eliminate N+1 queries for result mapping and list operations.

Run the smallest relevant test file first, then run the repository gates:

```bash
make lint
make type-check
make test
make coverage
```

### Mypyc and Performance Gates

SQLSpec keeps a compiled surface for hot paths. If a change touches `pyproject.toml` mypyc includes or excludes, `tools/scripts/bench*.py`, `tools/scripts/mypyc_*.py`, compiled `sqlspec/core` or `sqlspec/driver` modules, storage registry/pipeline code, data dictionary registry code, or adapter `core.py` / `type_converter.py` files, run the focused gates below before opening a pull request:

```bash
make install-compiled && make test
uv run python tools/scripts/mypyc_inventory.py
```

For pull requests that change build hooks, wheel workflows, or compiled import boundaries, also run:

```bash
make build-performance
uv run python tools/scripts/mypyc_smoke.py
```

Benchmark claims need current artifacts rather than estimates:

```bash
uv run python tools/scripts/bench.py --json-output /tmp/sqlspec-bench.json
uv run python tools/scripts/bench_gate.py --json-output /tmp/sqlspec-bench-gate.json
uv run python tools/scripts/bench_subsystems.py \
    --json-output /tmp/sqlspec-bench-subsystems.json
```

## Project Documentation

Documentation is authored in Markdown under `docs/`, configured via `zensical.toml`, and built with [Zensical Free](https://zensical.org/) (`zensical >= 0.0.71`, MIT) and `mkdocstrings-python` (Griffe static AST analysis).

### Authoring Guidelines and Runnable Examples

- **Page Budgets**: Keep conceptual guides and tutorials focused on one topic (`<= 12` viewports) and Reference pages scoped to one primary class or cohesive symbol group (`<= 120 KB` HTML, `<= 14` `"On this page"` TOC items).
- **Standalone Tested Examples**: Place runnable code examples under `docs/examples/**/*.py` as standalone zero-comment modules exposing a `def main()` entrypoint and `# --8<-- [start:example]` / `# --8<-- [end:example]` snippet markers. Include them in Markdown pages via `pymdownx.snippets` (`--8<-- "docs/examples/...:example"`) inside braced SuperFences blocks:
  ````markdown
  ```{ .python title="docs/examples/quickstart/first_query.py" }
  &#45;&#45;8&lt;&#45;&#45; "docs/examples/quickstart/first_query.py:example"
  ```
  ````
  Every example module is executed automatically by `pytest tests/unit/docs/test_examples.py`.
- **CLI Reference Generation**: When modifying Click commands in `sqlspec/cli.py`, regenerate `docs/reference/cli.md` with `uv run python -m tools.docs.generate_cli_docs --output docs/reference/cli.md`.

### Building and Serving Docs Locally

```bash
make docs-serve
make docs
```

`make docs` runs `zensical build --clean`, generates `site/llms.txt`, `site/llms-full.txt`, and legacy `.html` redirect stubs (`tools/docs/postbuild.py`), and validates HTML budgets, anchors, and docstring hygiene (`tools/docs/verify_docs_artifacts.py`).

### CLI Demo Recordings

SQLSpec uses [VHS](https://github.com/charmbracelet/vhs) to record terminal demos as GIF files from `docs/_tapes/*.tape` into `docs/_static/demos/`:

```bash
make docs-demos
make docs-all
```

## Release Process

Releases follow [Semantic Versioning](https://semver.org/) and [PEP 440](https://peps.python.org/pep-0440/). Release preparation is automated via `Makefile` targets:

```bash
make release bump=patch
make pre-release version=0.70.0-alpha.1
```

See the [SQLSpec Releases](/releases/) guide for versioning schemes, pre-release cadences, deprecation policies, and the automated CI publishing pipeline.
