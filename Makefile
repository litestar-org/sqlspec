SHELL := /bin/bash

# =============================================================================
# Configuration and Environment Variables
# =============================================================================

.DEFAULT_GOAL := help
.ONESHELL:
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory

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
install: destroy clean                              ## Install the project, dependencies, and pre-commit
	@echo "${INFO} Starting fresh installation..."
	@uv python pin 3.10 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@uv sync --all-extras --dev
	@echo "${OK} Installation complete! 🎉"

.PHONY: install-compiled
install-compiled: destroy clean                  ## Install with mypyc compilation for performance
	@echo "${INFO} Starting fresh installation with mypyc compilation..."
	@uv python pin 3.10 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@echo "${INFO} Installing in editable mode with mypyc compilation..."
	@HATCH_BUILD_HOOKS_ENABLE=1 uv pip install -e .
	@uv sync --all-extras --dev
	@echo "${OK} Performance installation complete! 🚀"
	@echo "${INFO} Verifying compilation..."
	@find sqlspec -name "*.so" | wc -l | xargs -I {} echo "${OK} Compiled {} modules"

.PHONY: destroy
destroy:                                            ## Destroy the virtual environment
	@echo "${INFO} Destroying virtual environment... 🗑️"
	@uv run pre-commit clean >/dev/null 2>&1
	@rm -rf .venv
	@echo "${OK} Virtual environment destroyed 🗑️"

# =============================================================================
# Dependency Management
# =============================================================================

.PHONY: upgrade
upgrade:                                            ## Upgrade all dependencies to latest stable versions
	@echo "${INFO} Updating all dependencies... 🔄"
	@uv lock --upgrade
	@echo "${OK} Dependencies updated 🔄"
	@uv run pre-commit autoupdate
	@echo "${OK} Updated Pre-commit hooks 🔄"

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

.PHONY: test-mypyc
test-mypyc:                                        ## Test mypyc compilation on individual modules
	@echo "${INFO} Testing mypyc compilation... 🔧"
	@uv run mypyc --check-untyped-defs sqlspec/utils/statement_hashing.py
	@uv run mypyc --check-untyped-defs sqlspec/utils/text.py
	@uv run mypyc --check-untyped-defs sqlspec/utils/sync_tools.py
	@uv run mypyc --check-untyped-defs sqlspec/statement/cache.py
	@echo "${OK} Mypyc compilation tests passed ✨"


.PHONY: release
release:                                           ## Bump version and create release tag
	@echo "${INFO} Preparing for release... 📦"
	@make docs
	@make clean
	@make build
	@uv run bump-my-version bump $(bump)
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
	@uv run bump-my-version bump --new-version $(version) pre
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
test:                                               ## Run the tests
	@echo "${INFO} Running test cases... 🧪"
	@uv run pytest -n 2 --dist=loadgroup tests
	@echo "${OK} Tests complete ✨"

.PHONY: test-all
test-all: tests				                        ## Run all tests
	@echo "${INFO} All tests executed successfully ✨"

.PHONY: coverage
coverage:                                           ## Run tests with coverage report
	@echo "${INFO} Running tests with coverage... 📊"
	@uv run pytest --cov -n 2 --dist=loadgroup --quiet
	@uv run coverage html >/dev/null 2>&1
	@uv run coverage xml >/dev/null 2>&1
	@echo "${OK} Coverage report generated ✨"

# -----------------------------------------------------------------------------
# Type Checking
# -----------------------------------------------------------------------------

.PHONY: mypy
mypy:                                               ## Run mypy
	@echo "${INFO} Running mypy... 🔍"
	@uv run dmypy run
	@echo "${OK} Mypy checks passed ✨"

.PHONY: pyright
pyright:                                            ## Run pyright
	@echo "${INFO} Running pyright... 🔍"
	@uv run pyright
	@echo "${OK} Pyright checks passed ✨"

.PHONY: type-check
type-check: mypy pyright                            ## Run all type checking

# -----------------------------------------------------------------------------
# Linting and Formatting
# -----------------------------------------------------------------------------

.PHONY: pre-commit
pre-commit:                                        ## Run pre-commit hooks
	@echo "${INFO} Running pre-commit checks... 🔎"
	@uv run pre-commit run --color=always --all-files
	@echo "${OK} Pre-commit checks passed ✨"

.PHONY: slotscheck
slotscheck:                                        ## Run slotscheck
	@echo "${INFO} Running slotscheck... 🔍"
	@uv run slotscheck sqlspec/
	@echo "${OK} Slotscheck complete ✨"

.PHONY: fix
fix:                                               ## Run code formatters
	@echo "${INFO} Running code formatters... 🔧"
	@uv run ruff check --fix --unsafe-fixes
	@echo "${OK} Code formatting complete ✨"

.PHONY: lint
lint: fix pre-commit type-check slotscheck             ## Run all linting checks
	@echo "${OK} All linting checks passed ✨"

.PHONY: check-all
check-all: lint test-all coverage                  ## Run all checks (lint, test, coverage)
	@echo "${OK} All checks passed successfully ✨"

# =============================================================================
# Documentation
# =============================================================================

.PHONY: docs-clean
docs-clean:                                        ## Clean documentation build
	@echo "${INFO} Cleaning documentation build assets... 🧹"
	@rm -rf docs/_build
	@echo "${OK} Documentation assets cleaned"

.PHONY: docs-serve
docs-serve: docs-clean                             ## Serve documentation locally
	@echo "${INFO} Starting documentation server... 📚"
	@uv run sphinx-autobuild docs docs/_build/ -j 1 --watch sqlspec --watch docs --watch tests --watch CONTRIBUTING.rst --port 8002

.PHONY: docs
docs: docs-clean                                   ## Build documentation
	@echo "${INFO} Building documentation... 📝"
	@PYTHONWARNINGS="ignore::FutureWarning" uv run sphinx-build -M html docs docs/_build/ -E -a -j 1 -W --keep-going
	@echo "${OK} Documentation built successfully"

.PHONY: docs-linkcheck
docs-linkcheck:                                    ## Check documentation links
	@echo "${INFO} Checking documentation links... 🔗"
	@uv run sphinx-build -b linkcheck ./docs ./docs/_build -D linkcheck_ignore='http://.*','https://.*'
	@echo "${OK} Link check complete"

.PHONY: docs-linkcheck-full
docs-linkcheck-full:                               ## Run full documentation link check
	@echo "${INFO} Running full link check... 🔗"
	@uv run sphinx-build -b linkcheck ./docs ./docs/_build -D linkcheck_anchors=0
	@echo "${OK} Full link check complete"

# =============================================================================
# Development Infrastructure
# =============================================================================

.PHONY: infra-up
infra-up:                                              ## Start development infrastructure (databases, storage)
	@echo "${INFO} Starting development infrastructure..."
	@./tools/local-infra.sh up
	@echo "${OK} Development infrastructure ready ✨"

.PHONY: infra-down
infra-down:                                            ## Stop development infrastructure
	@echo "${INFO} Stopping development infrastructure..."
	@./tools/local-infra.sh down --quiet
	@echo "${OK} Development infrastructure stopped"

.PHONY: infra-status
infra-status:                                          ## Show development infrastructure status
	@./tools/local-infra.sh status

.PHONY: infra-cleanup
infra-cleanup:                                         ## Clean up development infrastructure
	@echo "${WARN} This will remove all development containers and volumes"
	@./tools/local-infra.sh cleanup

.PHONY: infra-postgres
infra-postgres:                                        ## Start only PostgreSQL
	@echo "${INFO} Starting PostgreSQL..."
	@./tools/local-infra.sh up postgres --quiet
	@echo "${OK} PostgreSQL ready on port 5433"

.PHONY: infra-oracle
infra-oracle:                                          ## Start only Oracle
	@echo "${INFO} Starting Oracle..."
	@./tools/local-infra.sh up oracle --quiet
	@echo "${OK} Oracle ready on port 1522"

.PHONY: infra-mysql
infra-mysql:                                           ## Start only MySQL
	@echo "${INFO} Starting MySQL..."
	@./tools/local-infra.sh up mysql --quiet
	@echo "${OK} MySQL ready on port 3307"

# =============================================================================
# End of Makefile
# =============================================================================
