Contribution Guide
==================

Setting up the environment
--------------------------

1. Run ``make install-uv`` to install `uv <https://docs.astral.sh/uv/>`_ if not already installed.
2. Run ``make install`` to create the virtual environment and install all development dependencies.

Code contributions
------------------

Workflow
++++++++

1. `Fork <https://github.com/litestar-org/sqlspec/fork>`_ the `sqlspec repository <https://github.com/litestar-org/sqlspec>`_.
2. Clone your fork locally with git.
3. `Set up the environment <#setting-up-the-environment>`_.
4. Create a dedicated feature or bugfix branch.
5. Make your code changes and write tests.
6. Run ``make lint`` to run code formatters, Prek hooks, type checkers, slotscheck, and workflow audits. Always run this before committing; project setup intentionally does not install Git hook shims.
7. Commit your changes following the `Conventional Commit format <https://www.conventionalcommits.org>`_ (e.g. ``fix(#100): resolve connection pooling leak`` or ``feat: add database adapter feature``).
8. Push the changes to your fork.
9. Open a `pull request <https://docs.github.com/en/pull-requests>`_. Give the pull request a descriptive title indicating what it changes. Include the corresponding issue number if applicable (e.g., ``fix(#100): ...``).

.. tip:: Pull requests and commits all need to follow the
    `Conventional Commit format <https://www.conventionalcommits.org>`_.

Guidelines for writing code
---------------------------

- **Python Version & Typing**:
  - Python 3.10+ baseline.
  - All code must be fully typed (enforced via `mypy <https://mypy.readthedocs.io/en/stable/>`_ and `pyright <https://microsoft.github.io/pyright/>`_). Avoid ``Any`` whenever possible.
  - Use PEP 585 built-in collection types (``dict``, ``list``, ``tuple``, ``set``) rather than their ``typing`` module equivalents.
  - Use PEP 604 union types (``T | None``), never ``Optional[T]`` or ``Union[T, None]``.
  - Package modules must **not** import ``from __future__ import annotations``. Prek checks this using an AST-based local hook.

- **Comments & Documentation**:
  - **Never use in-line comments in Python code**. If code requires explanation, document the rationale in the class or function docstring.
  - Use Google-style docstrings for public classes and functions with structured ``Args:``, ``Returns:``, and ``Raises:`` blocks.
  - Explain *why* something is done in a specific way, not merely *what* the code does.

- **Naming Conventions**:
  - Functions, methods, and variables: ``snake_case``.
  - Classes, exceptions, and protocols: ``PascalCase``.
  - Constants and module-level singletons: ``SCREAMING_SNAKE_CASE``.

- **SQL Safety**:
  - SQL queries must **always** use parameterized placeholders (``:param`` or ``?``).
  - Never use f-strings, string concatenation, or ``%``/``.format()`` formatting to construct SQL statements with dynamic inputs.

- **Linting & Code Quality**:
  - Formatting and linting are handled via Ruff and Prek.
  - ``make lint`` applies Ruff fixes and formatting, runs every Prek hook, checks types and slots, and audits workflows with zizmor.
  - Dependency resolution uses a seven-day cooldown by default, with Litestar ecosystem packages exempt so coordinated releases remain testable.

Logging
+++++++

- Logger names must follow the ``sqlspec.<module>`` hierarchy.
- Always obtain loggers via ``sqlspec.utils.logging.get_logger`` to ensure filters are attached.
- Use static event names in structured logs and include context fields instead of dynamic message strings.

Writing and running tests
+++++++++++++++++++++++++

- **Test Style**: Use pytest function-based tests (``test_*`` or ``async def test_*``). Do not use test classes.
- **Test Placement**:
  - Put behavior shared by adapters in the contract suite (`tests/integration/adapters/<database>/test_shared.py`).
  - Keep vendor-only cases in that adapter's test folder (`tests/integration/adapters/<database>/<driver>/`).
  - Use unit tests (`tests/unit/`) for code that does not need a database (parsing, config normalization, parameter formatting).
  - Review the `test placement guide <https://github.com/litestar-org/sqlspec/blob/main/tests/README.md>`_ for detailed fixture conventions.
- **Integration Resources**:
  - Integration tests use ``pytest-databases`` fixtures.
  - Run database containers locally before running integration tests if testing locally (e.g. via ``make infra-up``).
- **Coverage Requirements**:
  - The repository-wide coverage floor is temporarily 76%.
  - Every new commit must keep changed-code coverage at or above **90%**.
  - Detect and eliminate N+1 queries for result mapping and list operations.

Run the smallest relevant test file first, then run the repository gates:

.. code-block:: console

   make lint
   make type-check
   make test
   make coverage

Mypyc and performance gates
+++++++++++++++++++++++++++

SQLSpec keeps a narrow compiled surface for hot paths. If a change touches
``pyproject.toml`` mypyc includes or excludes, ``tools/scripts/bench*.py``,
``tools/scripts/mypyc_*.py``, compiled ``sqlspec/core`` or ``sqlspec/driver``
modules, storage registry/pipeline code, data dictionary registry code, or
adapter ``core.py`` / ``type_converter.py`` files, run the focused gates below
before opening a pull request:

.. code-block:: console

   make install-compiled && make test
   uv run python tools/scripts/mypyc_inventory.py

``make install-compiled`` compiles the full mypyc include set (catching compile
errors in any compiled module), and the test suite automatically skips cases
that cannot run against a compiled build.

For pull requests that change build hooks, wheel workflows, or compiled import
boundaries, also run:

.. code-block:: console

   make build-performance
   uv run python tools/scripts/mypyc_smoke.py

Benchmark claims need current artifacts rather than estimates. Use JSON output
when capturing baselines for review:

.. code-block:: console

   uv run python tools/scripts/bench.py --json-output /tmp/sqlspec-bench.json
   uv run python tools/scripts/bench_gate.py --json-output /tmp/sqlspec-bench-gate.json
   uv run python tools/scripts/bench_subsystems.py --json-output /tmp/sqlspec-bench-subsystems.json

CI gate ownership:

- Pull requests always run lint, mypy, pyright, slotscheck, docs, and the Python
  test matrix through ``.github/workflows/ci.yml``.
- Pull requests that touch build configuration run
  ``.github/workflows/test-build.yml``. The default pull-request path builds a
  subset mypyc wheel matrix; maintainers can dispatch the full architecture
  matrix when release confidence is needed.
- Releases run ``.github/workflows/publish.yml`` with standard wheels, mypyc
  wheels, PGO on Linux and macOS, and mypyc smoke imports before publishing.
- ``.github/workflows/pgo-validate.yml`` is manual Linux PGO validation. It is
  useful for build-hook changes but is not required for every pull request.
- Optional services and container-backed adapter benchmarks remain manual unless
  their owning PR explicitly opts into those dependencies.

Project documentation
---------------------

The documentation is located in the ``/docs`` directory and is written in
`ReST <https://docutils.sourceforge.io/rst.html>`_ and built with
`Sphinx <https://www.sphinx-doc.org/en/master/>`_. If you're unfamiliar with either,
the `ReStructuredText primer <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>`_
and `Sphinx quickstart <https://www.sphinx-doc.org/en/master/usage/quickstart.html>`_ are
recommended reads.

Running the docs locally
++++++++++++++++++++++++

You can serve the documentation locally with live reloading via ``make docs-serve``, or
build the static HTML output via ``make docs``.

CLI demo recordings
+++++++++++++++++++

SQLSpec uses `VHS <https://github.com/charmbracelet/vhs>`_ to record terminal demos as GIF files
that are embedded in the documentation.

**Requirements:** VHS, ffmpeg, ttyd

**Installation:**

.. code-block:: console

   go install github.com/charmbracelet/vhs@latest

**Recording demos:**

.. code-block:: console

   make docs-demos

This will process every ``.tape`` file in ``docs/_tapes/`` and write GIF output to
``docs/_static/demos/``.

**Creating a new tape:**

1. Create a new ``.tape`` file in ``docs/_tapes/``.
2. Use the standard header (see existing tapes for examples). All tapes should use
   the ``Catppuccin Mocha`` theme, font size 14, and 1000x600 dimensions.
3. Use ``Hide``/``Show`` commands to hide setup steps like virtual environment activation.
4. Include generous ``Sleep`` durations after commands that produce output.
5. Run ``make docs-demos`` to generate the GIF.
6. Reference the GIF in your documentation with an ``.. image::`` directive pointing to
   ``/_static/demos/<name>.gif``.

**Building docs with demos:**

.. code-block:: console

   make docs-all

Release process
---------------

Releases follow `Semantic Versioning <https://semver.org/>`_ and `PEP 440 <https://peps.python.org/pep-0440/>`_.
Release preparation is automated via the repository `Makefile`:

.. code-block:: console

   # Stable releases (patch, minor, major)
   make release bump=patch

   # Pre-releases
   make pre-release version=0.63.0-alpha.1

See the full :doc:`releases` guide for detailed information on versioning schemes, pre-release cadences, deprecation policies, and the automated CI publishing pipeline.
