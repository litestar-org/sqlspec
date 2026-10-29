# SQLSpec Releases

SQLSpec follows structured release cadences, transparent versioning policies, and automated build pipelines across all supported database drivers and compiled C-extension wheels.

## Version Numbering

SQLSpec adheres to [Semantic Versioning (SemVer 2.0.0)](https://semver.org/) and [PEP 440](https://peps.python.org/pep-0440/) using the `<major>.<minor>.<patch>` scheme:

- **Major (`X.0.0`)**: Introduces backwards-incompatible API changes or major architectural redesigns. Breaking changes are announced in advance with deprecation warnings in prior minor releases whenever feasible.
- **Minor (`0.Y.0` or `X.Y.0`)**: Adds new functionality, database adapters, query builder capabilities, or performance optimizations in a backwards-compatible manner.
- **Patch (`0.Y.Z` or `X.Y.Z`)**: Includes backwards-compatible bug fixes, driver compatibility corrections, performance refinements, and documentation updates.

## Pre-Release Versions

Prior to a new major or significant minor release, SQLSpec publishes preview builds using `<major>.<minor>.<patch>-<stage>.<number>`:

- **Alpha (`alpha.N`)**: Early developer preview for testing new features, public API additions, and experimental adapter implementations.
- **Beta (`beta.N`)**: Feature-complete preview. Core APIs are frozen; testing focuses on edge cases, multi-database contract conformance, and integration stability.
- **Release Candidate (`rc.N`)**: Final validation build under a complete feature freeze. Only critical regression fixes are accepted before the official release.

## Maintenance and Deprecation Policy

At any given time, the Litestar organization actively maintains:

- The current release series.
- The previous release series for critical security and regression fixes.

When an existing API, parameter, or driver feature is scheduled for retirement:

1. A deprecation notice and runtime warning (`DeprecationWarning`) is introduced in a **minor** release.
2. The deprecated feature remains functional throughout the remainder of that major release series.
3. The feature is removed in the subsequent **major** release.

## Release Workflow and CI/CD Pipeline

### Local Release Preparation

Maintainers prepare releases via `Makefile` targets powered by `bump-my-version`:

```bash
make release bump=patch
make pre-release version=0.70.0-alpha.1
```

### Automated Publishing (`.github/workflows/publish.yml`)

When a GitHub Release is published, CI automatically executes:

1. **Source and Pure-Python Distributions**: Builds standard `sdist` and pure-Python wheel packages.
2. **Compiled Mypyc Wheels**: Compiles C-extensions via `hatch-mypyc` across Linux, macOS, and Windows for Python `3.11` through `3.15`.
3. **Profile-Guided Optimization (PGO)**: Compiles Linux binary wheels using execution profile data (GCC) to optimize statement compilation and driver dispatch hot paths.
4. **Smoke Verification**: Runs `tools/scripts/mypyc_smoke.py` across compiled wheel artifacts before publishing to [PyPI](https://pypi.org/project/sqlspec/).

## See Also

- [Changelog](changelog.md) — Release notes, detailed change lists, and upgrade instructions.
- [Contribution Guide](contributing/index.md) — Development workflow, testing standards, and documentation authoring.
