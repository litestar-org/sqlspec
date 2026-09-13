================
SQLSpec Releases
================

SQLSpec follows structured release cadences, transparent versioning policies, and automated build pipelines to ensure reliability across all supported database drivers and compiled extensions.

Version Numbering
-----------------

SQLSpec strictly adheres to `Semantic Versioning (SemVer 2.0.0) <https://semver.org/>`_ and `PEP 440 <https://peps.python.org/pep-0440/>`_ using the ``<major>.<minor>.<patch>`` scheme:

**Major (X.0.0)**
    Introduces backwards-incompatible API changes or major architectural redesigns. Breaking changes are announced in advance with deprecation warnings in prior minor releases whenever feasible.

**Minor (0.Y.0 or X.Y.0)**
    Adds new functionality, adapters, query builder capabilities, or optimizations in a backwards-compatible manner.

**Patch (0.Y.Z or X.Y.Z)**
    Includes backwards-compatible bug fixes, driver compatibility corrections, performance enhancements, and documentation revisions.

Pre-release Versions
--------------------

Prior to a new major or significant minor release, SQLSpec publishes preview releases using the format ``<major>.<minor>.<patch>-<stage>.<number>``:

- **Alpha (``alpha.N``)**: Early developer preview for testing new features, public API changes, and experimental adapter implementations. APIs may evolve before stabilization.
- **Beta (``beta.N``)**: Feature-complete preview. Core APIs are frozen; testing focuses on edge cases, multi-database contract conformance, and integration stability.
- **Release Candidate (``rc.N``)**: Final validation build under a complete feature freeze. Only critical regression fixes are accepted before the official release.

Long-term Support (LTS) & Maintenance
-------------------------------------

Major releases are supported according to our project maintenance lifecycle:

Supported Versions
++++++++++++++++++

At any given time, the Litestar organization actively maintains:

- The current major release series (e.g., ``1.x``).
- The previous major release series (for critical security and regression fixes).
- Any designated LTS releases.

Bug fixes are applied directly to the active development branch and backported to supported release branches based on severity and impact.

Deprecation Policy
------------------

When an existing API, parameter, or driver feature is scheduled for retirement:

1. A deprecation notice and runtime warning (such as ``DeprecationWarning``) is introduced in a **minor** release.
2. The deprecated feature remains functional throughout the remainder of that major release series.
3. The feature is completely removed in the subsequent **major** release.

Release Workflow & Automation
-----------------------------

SQLSpec automates release versioning and package distribution using repository tooling and GitHub Actions.

Local Release Preparation
+++++++++++++++++++++++++

Maintainers prepare releases via dedicated `Makefile` targets that coordinate clean builds, documentation validation, version increments, and lockfile updates:

.. code-block:: console

   # Prepare a stable patch release (or bump=minor, bump=major)
   make release bump=patch

   # Prepare a pre-release
   make pre-release version=0.63.0-alpha.1

These targets use `bump-my-version` to update the canonical version in `pyproject.toml`, rebuild the distribution artifacts, update lockfiles, and generate git tags.

CI/CD Publishing Pipeline
+++++++++++++++++++++++++

When a release tag (``v*``) is pushed to the repository, ``.github/workflows/publish.yml`` automatically executes:

1. **Standard Distributions**: Builds standard source distribution (``sdist``) and pure Python wheel packages.
2. **Compiled Mypyc Wheels**: Compiles high-performance C-extensions via `hatch-mypyc` across a matrix of operating systems (Linux, macOS, Windows) and Python versions (3.10 through 3.14).
3. **Profile-Guided Optimization (PGO)**: Compiles Linux and macOS binary wheels using execution profile data to optimize critical statement translation and dispatch paths.
4. **Smoke Testing**: Validates binary wheel imports across supported architectures before publishing to `PyPI <https://pypi.org/project/sqlspec/>`_.

See Also
--------

- :doc:`changelog` for release notes, detailed change lists, and migration instructions.
- :doc:`contribution-guide` for development workflow, testing, and contribution standards.
