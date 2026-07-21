Contribution guide
==================

Setting up the environment
--------------------------

1. Run ``make install-uv`` to install `uv <https://docs.astral.sh/uv/>`_ if not already installed
1. Run ``make install`` to install all dependencies and pre-commit hooks


Code contributions
------------------

Workflow
++++++++

1. `Fork <https://github.com/litestar-org/sqlspec/fork>`_ the `sqlspec repository <https://github.com/litestar-org/sqlspec>`_
2. Clone your fork locally with git
3. `Set up the environment <#setting-up-the-environment>`_
4. Make your changes
5. Run ``make lint`` to run linters and formatters. This step is optional and will be executed
   automatically by git before you make a commit, but you may want to run it manually in order to apply fixes
6. Commit your changes to git
7. Push the changes to your fork
8. Open a `pull request <https://docs.github.com/en/pull-requests>`_. Give the pull request a descriptive title
   indicating what it changes. If it has a corresponding open issue, the issue number should be included in the title as
   well. For example a pull request that fixes issue ``bug: Increased stack size making it impossible to find needle #100``
   could be titled ``fix(#100): Make needles easier to find by applying fire to haystack``

.. tip:: Pull requests and commits all need to follow the
    `Conventional Commit format <https://www.conventionalcommits.org>`_

Guidelines for writing code
----------------------------

- All code should be fully `typed <https://peps.python.org/pep-0484/>`_. This is enforced via
  `mypy <https://mypy.readthedocs.io/en/stable/>`_.
- All code should be tested. This is enforced via `pytest <https://docs.pytest.org/en/stable/>`_.
- All code should be properly formatted. This is enforced via `black <https://black.readthedocs.io/en/stable/>`_ and `Ruff <https://docs.astral.sh/ruff/>`_.

Logging
++++++++

- Logger names must follow the ``sqlspec.<module>`` hierarchy.
- Always obtain loggers via ``sqlspec.utils.logging.get_logger`` to ensure filters are attached.
- Use static event names in structured logs and include context fields instead of dynamic message strings.

Writing and running tests
+++++++++++++++++++++++++

Put behavior shared by adapters in the contract suite. Keep vendor-only cases
in that adapter's test folder. Use unit tests for code that does not need a
database. The `test placement guide
<https://github.com/litestar-org/sqlspec/blob/main/tests/README.md>`_ explains
where tests and fixtures belong. It also lists the checks to run.

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

``make install-compiled`` compiles the full mypyc include set (so it catches
compile errors in any compiled module), and the test suite automatically skips
the cases that cannot run against a compiled build.

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

The documentation is located in the ``/docs`` directory and is `ReST <https://docutils.sourceforge.io/rst.html>`_ and
`Sphinx <https://www.sphinx-doc.org/en/master/>`_. If you're unfamiliar with any of those,
`ReStructuredText primer <https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>`_ and
`Sphinx quickstart <https://www.sphinx-doc.org/en/master/usage/quickstart.html>`_ are recommended reads.

Running the docs locally
++++++++++++++++++++++++

You can serve the documentation with ``make docs-serve``, or build them with ``make docs``.

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

Creating a new release
----------------------

1. Increment the version in `pyproject.toml <https://github.com/litestar-org/sqlspec/blob/main/pyproject.toml>`_.
    .. note:: The version should follow `semantic versioning <https://semver.org/>`_ and `PEP 440 <https://peps.python.org/pep-0440/>`_.
2. `Draft a new release <https://github.com/litestar-org/sqlspec/releases/new>`_ on GitHub

   * Use ``vMAJOR.MINOR.PATCH`` (e.g. ``v1.2.3``) as both the tag and release title
   * Fill in the release description. You can use the "Generate release notes" function to get a draft for this
3. Commit your changes and push to ``main``
4. Publish the release
5. Go to `Actions <https://github.com/litestar-org/sqlspec/actions>`_ and approve the release workflow
6. Check that the workflow runs successfully
