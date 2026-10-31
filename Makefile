SHELL := /bin/bash

# =============================================================================
# Configuration and Environment Variables
# =============================================================================

.DEFAULT_GOAL := help
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory
MYPY_WORKERS ?= 2
UNIT_WORKERS ?= 4
INTEGRATION_WORKERS ?= 2
SHARD_POSTGRES_COCKROACH := tests/integration/adapters/postgres tests/integration/adapters/cockroach tests/integration/extensions tests/integration/observability
SHARD_MYSQL_MARIADB_EMBEDDED := tests/integration/adapters/mysql tests/integration/adapters/sqlite tests/integration/adapters/duckdb tests/integration/cli tests/integration/migrations
SHARD_ORACLE := tests/integration/adapters/oracle
SHARD_MSSQL_DB2 := tests/integration/adapters/mssql tests/integration/adapters/db2
SHARD_CLOUD_ADBC_STORAGE := tests/integration/adapters/bigquery tests/integration/adapters/spanner tests/integration/adapters/gizmosql tests/integration/storage tests/integration/adapters/snowflake

# -----------------------------------------------------------------------------
# Display Formatting and Colors
# -----------------------------------------------------------------------------
BLUE := $(shell printf "\033[1;34m")
GREEN := $(shell printf "\033[1;32m")
RED := $(shell printf "\033[1;31m")
YELLOW := $(shell printf "\033[1;33m")
NC := $(shell printf "\033[0m")
INFO := $(shell printf "$(BLUE)ℹ$(NC)")
OK := $(shell printf "$(GREEN)✓$(NC)")
WARN := $(shell printf "$(YELLOW)⚠$(NC)")
ERROR := $(shell printf "$(RED)✖$(NC)")

# =============================================================================
# Help and Documentation
# =============================================================================

.PHONY: help
help:                                               ## Display this help text for Makefile
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# =============================================================================
# Installation and Environment Setup
# =============================================================================

.PHONY: install-uv
install-uv:                                         ## Install latest version of uv
	@echo "${INFO} Installing uv..."
	@curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
	@echo "${OK} UV installed successfully"

.PHONY: install
install: destroy clean                              ## Install the project and dependencies
	@echo "${INFO} Starting fresh installation..."
	@uv python pin 3.11 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@uv sync --all-extras --no-extra mypyc --dev
	@echo "${OK} Installation complete! 🎉"

.PHONY: install-compiled
install-compiled: destroy clean                  ## Install with mypyc compilation for performance
	@echo "${INFO} Starting fresh installation with mypyc compilation..."
	@uv python pin 3.11 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@echo "${INFO} Installing in editable mode with mypyc compilation..."
	@uv sync --all-extras --dev
	@HATCH_BUILD_HOOKS_ENABLE=1 uv pip install -e .
	@echo "${OK} Performance installation complete! 🚀"
	@echo "${INFO} Verifying compilation..."
	@find sqlspec -name "*.so" | wc -l | xargs -I {} echo "${OK} Compiled {} modules"

.PHONY: destroy
destroy:                                            ## Destroy the virtual environment
	@echo "${INFO} Destroying virtual environment... 🗑️"
	@uv run --no-sync prek cache clean >/dev/null 2>&1 || true
	@rm -rf .venv
	@find sqlspec \( -name '*.so' -o -name '*.c' \) -delete >/dev/null 2>&1 || true
	@echo "${OK} Virtual environment destroyed 🗑️"

# =============================================================================
# Dependency Management
# =============================================================================

.PHONY: upgrade
upgrade:                                            ## Upgrade all dependencies to latest stable versions
	@echo "${INFO} Updating all dependencies... 🔄"
	@uv lock --upgrade
	@echo "${OK} Dependencies updated 🔄"
	@uv run prek update --cooldown-days 7
	@echo "${OK} Updated Prek hooks 🔄"
	@uv lock >/dev/null 2>&1

.PHONY: lock
lock:                                              ## Rebuild lockfiles from scratch
	@echo "${INFO} Rebuilding lockfiles... 🔄"
	@uv lock --upgrade >/dev/null 2>&1
	@echo "${OK} Lockfiles updated"

# =============================================================================
# Build and Release
# =============================================================================

.PHONY: build
build:                                             ## Build the package
	@echo "${INFO} Building package... 📦"
	@uv build >/dev/null 2>&1
	@echo "${OK} Package build complete"

.PHONY: build-performance
build-performance:                                 ## Build package with mypyc compilation
	@echo "${INFO} Building package with mypyc compilation... 📦"
	@HATCH_BUILD_HOOKS_ENABLE=1 uv build >/dev/null 2>&1
	@echo "${OK} Performance package build complete 🚀"


.PHONY: release
release:                                           ## Bump version and create release tag
	@echo "${INFO} Preparing for release... 📦"
	@make docs
	@make clean
	@make build
	@uv run bump-my-version bump --current-version "$$(uv version --short)" $(bump)
	@uv lock --upgrade-package sqlspec >/dev/null 2>&1
	@echo "${OK} Release complete 🎉"

.PHONY: pre-release
pre-release:                                       ## Start a pre-release: make pre-release version=0.33.0-alpha.1
	@if [ -z "$(version)" ]; then \
		echo "${ERROR} Usage: make pre-release version=X.Y.Z-alpha.N"; \
		echo ""; \
		echo "Pre-release workflow:"; \
		echo "  1. Start alpha:     make pre-release version=0.33.0-alpha.1"; \
		echo "  2. Next alpha:      make pre-release version=0.33.0-alpha.2"; \
		echo "  3. Move to beta:    make pre-release version=0.33.0-beta.1"; \
		echo "  4. Move to rc:      make pre-release version=0.33.0-rc.1"; \
		echo "  5. Final release:   make release bump=patch (from rc) OR bump=minor (from stable)"; \
		exit 1; \
	fi
	@echo "${INFO} Preparing pre-release $(version)... 🧪"
	@make clean
	@make build
	@uv run bump-my-version bump --current-version "$$(uv version --short)" --new-version $(version) pre
	@uv lock --upgrade-package sqlspec >/dev/null 2>&1
	@echo "${OK} Pre-release $(version) complete 🧪"
	@echo ""
	@echo "${INFO} Next steps:"
	@echo "  1. Push: git push origin HEAD"
	@echo "  2. Create a GitHub pre-release: gh release create v$(version) --prerelease --generate-notes --title 'v$(version)'"
	@echo "  3. This will publish to PyPI with pre-release tags"

# =============================================================================
# Cleaning and Maintenance
# =============================================================================

.PHONY: clean
clean:                                              ## Cleanup temporary build artifacts
	@echo "${INFO} Cleaning working directory... 🧹"
	@rm -rf .pytest_cache .ruff_cache .hypothesis build/ -rf dist/ .eggs/ .coverage coverage.xml coverage.json htmlcov/ .pytest_cache tests/.pytest_cache tests/**/.pytest_cache .mypy_cache .unasyncd_cache/ .auto_pytabs_cache >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -name '*.egg-info' -exec rm -rf {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -type f -name '*.egg' -exec rm -f {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -name '*.pyc' -exec rm -f {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -name '*.pyo' -exec rm -f {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -name '*~' -exec rm -f {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -type d -name '__pycache__' -exec rm -rf {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -name '.ipynb_checkpoints' -exec rm -rf {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -name '*.so' -exec rm -f {} + >/dev/null 2>&1
	@find . \( -path ./.venv -o -path ./.git \) -prune -o -name '*.c' -exec rm -f {} + >/dev/null 2>&1
	@echo "${OK} Working directory cleaned"
	$(MAKE) docs-clean

# =============================================================================
# Testing and Quality Checks
# =============================================================================

.PHONY: test
test: test-unit test-integration                     ## Run the tests
	@echo "${OK} Tests complete ✨"

.PHONY: test-unit
test-unit:                                          ## Run unit tests
	@echo "${INFO} Running unit tests... 🧪"
	@uv run pytest -n $(UNIT_WORKERS) --dist=loadgroup tests/unit
	@echo "${OK} Unit tests complete ✨"

.PHONY: test-integration
test-integration: test-integration-postgres-cockroach test-integration-mysql-mariadb-embedded test-integration-oracle test-integration-mssql-db2 test-integration-cloud-adbc-storage ## Run integration tests shard by shard
	@echo "${OK} Integration tests complete ✨"

.PHONY: test-integration-postgres-cockroach
test-integration-postgres-cockroach:                ## Run Postgres and CockroachDB integration shard
	@echo "${INFO} Running postgres-cockroach integration shard... 🧪"
	@uv run pytest -n $(INTEGRATION_WORKERS) --dist=loadgroup $(SHARD_POSTGRES_COCKROACH)
	@echo "${OK} postgres-cockroach integration shard complete ✨"

.PHONY: test-integration-mysql-mariadb-embedded
test-integration-mysql-mariadb-embedded:            ## Run MySQL, MariaDB, and embedded integration shard
	@echo "${INFO} Running mysql-mariadb-embedded integration shard... 🧪"
	@uv run pytest -n $(INTEGRATION_WORKERS) --dist=loadgroup $(SHARD_MYSQL_MARIADB_EMBEDDED)
	@echo "${OK} mysql-mariadb-embedded integration shard complete ✨"

.PHONY: test-integration-oracle
test-integration-oracle:                            ## Run Oracle integration shard
	@echo "${INFO} Running oracle integration shard... 🧪"
	@uv run pytest -n $(INTEGRATION_WORKERS) --dist=loadgroup $(SHARD_ORACLE)
	@echo "${OK} oracle integration shard complete ✨"

.PHONY: test-integration-mssql-db2
test-integration-mssql-db2:                         ## Run SQL Server and Db2 integration shard
	@echo "${INFO} Running mssql-db2 integration shard... 🧪"
	@uv run pytest -n $(INTEGRATION_WORKERS) --dist=loadgroup $(SHARD_MSSQL_DB2)
	@echo "${OK} mssql-db2 integration shard complete ✨"

.PHONY: test-integration-cloud-adbc-storage
test-integration-cloud-adbc-storage:                ## Run BigQuery, Spanner, GizmoSQL, Snowflake, and storage shard
	@echo "${INFO} Running cloud-adbc-storage integration shard... 🧪"
	@uv run pytest -n $(INTEGRATION_WORKERS) --dist=loadgroup --run-bigquery-tests --run-spanner-tests $(SHARD_CLOUD_ADBC_STORAGE)
	@echo "${OK} cloud-adbc-storage integration shard complete ✨"

.PHONY: test-all
test-all: test				                         ## Run all tests
	@echo "${INFO} All tests executed successfully ✨"

.PHONY: coverage
coverage:                                           ## Run tests with coverage report
	@echo "${INFO} Running tests with coverage... 📊"
	@uv run coverage erase
	@uv run pytest --cov --cov-report= --cov-fail-under=0 -n $(UNIT_WORKERS) --dist=loadgroup --quiet \
		tests/unit
	@uv run pytest --cov --cov-append --cov-report= --cov-fail-under=0 -n $(INTEGRATION_WORKERS) --dist=loadgroup --quiet \
		$(SHARD_POSTGRES_COCKROACH)
	@uv run pytest --cov --cov-append --cov-report= --cov-fail-under=0 -n $(INTEGRATION_WORKERS) --dist=loadgroup --quiet \
		$(SHARD_MYSQL_MARIADB_EMBEDDED)
	@uv run pytest --cov --cov-append --cov-report= --cov-fail-under=0 -n $(INTEGRATION_WORKERS) --dist=loadgroup --quiet \
		$(SHARD_ORACLE)
	@uv run pytest --cov --cov-append --cov-report= --cov-fail-under=0 -n $(INTEGRATION_WORKERS) --dist=loadgroup --quiet \
		$(SHARD_MSSQL_DB2)
	@uv run pytest --cov --cov-append --cov-report= --cov-fail-under=0 -n $(INTEGRATION_WORKERS) --dist=loadgroup --run-bigquery-tests --run-spanner-tests --quiet \
		$(SHARD_CLOUD_ADBC_STORAGE)
	@uv run coverage report --fail-under=0
	@uv run coverage html >/dev/null 2>&1
	@uv run coverage xml >/dev/null 2>&1
	@echo "${OK} Coverage report generated ✨"

# -----------------------------------------------------------------------------
# Type Checking
# -----------------------------------------------------------------------------

.PHONY: mypy
mypy:                                               ## Run mypy
	@echo "${INFO} Running mypy with $(MYPY_WORKERS) workers... 🔍"
	@uv run mypy -n $(MYPY_WORKERS) --no-incremental --no-warn-unused-configs --show-traceback --no-error-summary -p sqlspec
	@echo "${OK} Mypy checks passed ✨"

.PHONY: dmypy
dmypy:                                              ## Run mypy daemon
	@echo "${INFO} Running mypy daemon... 🔍"
	@uv run dmypy run
	@echo "${OK} Mypy daemon checks passed ✨"

.PHONY: mypy-parallel
mypy-parallel: mypy                                 ## Run mypy parallel checking

.PHONY: pyright
pyright:                                            ## Run pyright
	@echo "${INFO} Running pyright... 🔍"
	@PYRIGHT_PYTHON_FORCE_VERSION=latest uv run pyright
	@echo "${OK} Pyright checks passed ✨"

.PHONY: type-check
type-check: mypy pyright                            ## Run all type checking

# -----------------------------------------------------------------------------
# Linting and Formatting
# -----------------------------------------------------------------------------

.PHONY: prek
prek:                                              ## Run Prek hooks
	@echo "${INFO} Running Prek checks... 🔎"
	@uv run prek run --color=always --all-files
	@echo "${OK} Prek checks passed ✨"

.PHONY: slotscheck
slotscheck:                                        ## Run slotscheck
	@echo "${INFO} Running slotscheck... 🔍"
	@PYTHONWARNINGS="ignore:::google.adk.features._feature_decorator" uv run slotscheck sqlspec/
	@echo "${OK} Slotscheck complete ✨"

.PHONY: fix
fix:                                               ## Run code formatters
	@echo "${INFO} Running code formatters... 🔧"
	@uv run ruff check --fix --unsafe-fixes
	@uv run ruff format
	@echo "${OK} Code formatting complete ✨"

.PHONY: zizmor
zizmor:                                            ## Audit GitHub Actions workflows
	@echo "${INFO} Auditing GitHub Actions workflows... 🔐"
	@uv run --locked zizmor --strict-collection .github/workflows
	@echo "${OK} GitHub Actions audit passed ✨"

.PHONY: lint
lint: fix prek type-check slotscheck zizmor         ## Run all linting checks
	@echo "${OK} All linting checks passed ✨"

.PHONY: check-all
check-all: lint coverage                            ## Run all checks (lint, test with coverage)
	@echo "${OK} All checks passed successfully ✨"

# =============================================================================
# Documentation
# =============================================================================

.PHONY: docs-clean
docs-clean:                                        ## Clean documentation build
	@echo "${INFO} Cleaning documentation build assets... 🧹"
	@rm -rf site docs/_build .cache
	@echo "${OK} Documentation assets cleaned"

.PHONY: docs-serve
docs-serve: docs-clean                             ## Serve documentation locally
	@echo "${INFO} Starting documentation server... 📚"
	@uv run --no-sync zensical serve -a localhost:8002

.PHONY: docs
docs: docs-clean                                   ## Build documentation
	@echo "${INFO} Building documentation... 📝"
	@uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts sqlspec
	@uv run --no-sync python -m tools.docs.generate_cli_docs --check
	@uv run --no-sync zensical build --clean
	@uv run --no-sync python -m tools.docs.postbuild --config zensical.toml --docs-dir docs --site-dir site
	@uv run --no-sync python -m tools.docs.verify_docs_artifacts --site-dir site
	@echo "${OK} Documentation built successfully"

.PHONY: docs-check
docs-check: docs                                   ## Build and verify documentation artifacts

.PHONY: docs-linkcheck
docs-linkcheck:                                    ## Check documentation links and anchors
	@echo "${INFO} Checking documentation links... 🔗"
	@uv run --no-sync python -m tools.docs.verify_docs_artifacts --site-dir site
	@echo "${OK} Link check complete"

.PHONY: docs-linkcheck-full
docs-linkcheck-full: docs-linkcheck                ## Run full documentation link check

.PHONY: docs-demos
docs-demos:                                        ## Generate CLI demo GIFs from VHS tapes
	@if ! command -v vhs >/dev/null 2>&1; then \
		echo "${WARN} VHS not installed. Install: go install github.com/charmbracelet/vhs@latest"; \
		exit 0; \
	fi
	@echo "${INFO} Generating CLI demo GIFs... 🎬"
	@mkdir -p docs/_static/demos
	@VHS_NO_SANDBOX=true; \
	for tape in docs/_tapes/*.tape; do \
		[ -f "$$tape" ] || continue; \
		echo "  Recording: $$tape"; \
		vhs "$$tape" || true; \
	done
	@echo "${OK} Demo GIFs generated"

.PHONY: docs-all
docs-all: docs-demos docs                         ## Generate demos then build documentation

# =============================================================================
# Benchmarks and Performance
# =============================================================================

.PHONY: bench
bench:                                              ## Run core benchmarks (sqlite)
	@echo "${INFO} Running core benchmarks... 🏎️"
	@uv run python tools/scripts/bench.py
	@echo "${OK} Benchmarks complete"

.PHONY: bench-extended
bench-extended:                                     ## Run extended benchmarks (all scenarios)
	@echo "${INFO} Running extended benchmarks... 🏎️"
	@uv run python tools/scripts/bench.py --extended
	@echo "${OK} Extended benchmarks complete"

.PHONY: bench-profile
bench-profile:                                      ## Run benchmarks with cProfile profiling
	@echo "${INFO} Running profiled benchmarks... 🔬"
	@uv run python tools/scripts/bench.py --profile
	@echo "${OK} Profiled benchmarks complete. Profiles saved to tools/scripts/profiles/"

.PHONY: bench-gate
bench-gate:                                         ## Run performance regression gate
	@echo "${INFO} Running performance gate... 🚦"
	@uv run python tools/scripts/bench_gate.py
	@echo "${OK} Performance gate complete"

.PHONY: bench-gate-adapters
DRIVERS ?= duckdb aiosqlite
bench-gate-adapters:                                ## Compare selected adapter benchmarks with committed baselines
	@mkdir -p /tmp/sqlspec-bench
	@failed_drivers=""; \
	for driver in ${DRIVERS}; do \
		baseline="tools/perf_baselines/$${driver}.json"; \
		run_args=$$(uv run --no-sync python tools/scripts/bench_compare.py "$${baseline}" --print-run-args); \
		if ! uv run --no-sync python tools/scripts/bench.py --driver "$${driver}" --extended $${run_args} --json-output "/tmp/sqlspec-bench/$${driver}.json"; then \
			failed_drivers="$${failed_drivers} $${driver}"; \
			continue; \
		fi; \
		if ! uv run --no-sync python tools/scripts/bench_compare.py "$${baseline}" "/tmp/sqlspec-bench/$${driver}.json" --mode "$${BENCHMARK_MODE:-ratio}" --threshold "$${BENCHMARK_THRESHOLD:-30}"; then \
			failed_drivers="$${failed_drivers} $${driver}"; \
		fi; \
	done; \
	if [ -n "$${failed_drivers}" ]; then \
		echo "Adapter benchmark gate failed for:$${failed_drivers}"; \
		exit 1; \
	fi

.PHONY: bench-subsystems
bench-subsystems:                                   ## Run subsystem micro-benchmarks
	@echo "${INFO} Running subsystem micro-benchmarks... 🔬"
	@uv run python tools/scripts/bench_subsystems.py
	@echo "${OK} Subsystem benchmarks complete"

.PHONY: bench-analyze
bench-analyze:                                      ## Analyze saved profile data
	@echo "${INFO} Analyzing benchmark profiles... 📊"
	@uv run python tools/scripts/analyze_profile.py tools/scripts/profiles/
	@echo "${OK} Profile analysis complete"

# =============================================================================
# PGO (Profile-Guided Optimization)
# =============================================================================

PGO_DIR := /tmp/sqlspec-pgo
PGO_BUILD_DIR := /tmp/sqlspec-mypyc-build

.PHONY: pgo-verify
pgo-verify:                                         ## Verify CFLAGS reach the C compiler through hatch-mypyc
	@echo "${INFO} Testing CFLAGS passthrough to C compiler..."
	@CFLAGS="-DPGO_CANARY=1" uv run python -c "\
	from distutils.sysconfig import customize_compiler; \
	from distutils.ccompiler import new_compiler; \
	cc = new_compiler(); customize_compiler(cc); \
	cmd = ' '.join(cc.compiler_so); \
	assert 'PGO_CANARY' in cmd, f'CFLAGS not found in: {cmd}'; \
	print('CFLAGS reach compiler: ' + cmd)" 2>/dev/null
	@echo "${OK} CFLAGS passthrough verified"

.PHONY: pgo-local
pgo-local:                                          ## Run full three-stage PGO build locally
	@echo "${INFO} Running PGO three-stage build..."
	@rm -rf $(PGO_DIR) $(PGO_BUILD_DIR) dist/
	@mkdir -p $(PGO_DIR) $(PGO_BUILD_DIR)
	@echo "${INFO} Stage 1: Building instrumented wheel..."
	@CFLAGS="-fprofile-generate=$(PGO_DIR)" \
		HATCH_MYPYC_BUILD_DIR=$(PGO_BUILD_DIR) \
		HATCH_BUILD_HOOKS_ENABLE=1 uv build --wheel >/dev/null 2>&1
	@echo "${OK} Instrumented wheel built"
	@echo "${INFO} Stage 2: Running training workload..."
	@uv pip install dist/*.whl --force-reinstall --no-deps >/dev/null 2>&1
	@.venv/bin/python tools/scripts/pgo_training.py
	@echo "${OK} Training complete"
	@rm -rf dist/ $(PGO_BUILD_DIR)/build $(PGO_BUILD_DIR)/tmp
	@echo "${INFO} Stage 3: Building PGO-optimized wheel..."
	@CFLAGS="-fprofile-use=$(PGO_DIR) -fprofile-correction -Wno-error=missing-profile -Wno-error=coverage-mismatch" \
		HATCH_MYPYC_BUILD_DIR=$(PGO_BUILD_DIR) \
		HATCH_BUILD_HOOKS_ENABLE=1 uv build --wheel >/dev/null 2>&1
	@echo "${OK} PGO-optimized wheel built: $$(ls dist/*.whl)"
	@uv pip install dist/*.whl --force-reinstall --no-deps >/dev/null 2>&1
	@echo "${OK} PGO build complete 🚀"

# =============================================================================
# End of Makefile
# =============================================================================
