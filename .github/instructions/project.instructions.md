---
applyTo: '**/*.py'
---
# Foundational Rules for AI Agent Code Generation (sqlspec Project)

## Project Setup & Dependencies

1. **Package Management**:
    * Use `uv` for all Python package management.
    * Declare all project dependencies, including development dependencies, in the `pyproject.toml` file. Ensure versions are pinned or appropriately ranged.
    * After adding or updating dependencies in `pyproject.toml`, run `uv pip compile pyproject.toml -o requirements.txt` (or the relevant `uv` command for your workflow if not using `requirements.txt` directly) and then `uv sync` to update the environment.

2. **Virtual Environment**:
    * Always work within the project\'s virtual environment (likely `.venv`, managed by `uv`).

3. **Prefix Command***:
    * Use the `uv` command prefix for all Python commands to ensure they run within the correct virtual environment. For example, use `uv run python` instead of just `python`.

## Code Structure & Style

3. **Directory Structure**:
    * Core library code resides in the `sqlspec/` directory.
    * Organize code logically within subdirectories (e.g., `adapters/`, `utils/`, `service/`, `sql/`).
    * New database adapters should be placed in `sqlspec/adapters/` following the established pattern for existing adapters.

4. **Modularity**:
    * Strive for small, focused modules and functions.
    * Avoid circular dependencies between modules.

6. **Type Hinting**:
    * Use type hints for all function signatures and variable declarations.
    * Ensure code passes `mypy` checks, strict checks in Pyright and all RUFF checks should be addressed or suppressed. This is critical.

7. **Linting and Formatting (Specifics)**:
    * **Ruff**: The primary linter and formatter is Ruff.
        * Ensure all code passes `ruff` checks (it will auto-fix where possible).
        * Ensure all code is formatted by `ruff-format`.
    * **Flake8 Dunder All**:
        * ensure `__all__` is defined and uses a tuple.
    * **Slotscheck**:
        * For classes (excluding those in `docs` or `.github`), consider using `__slots__` to reduce memory footprint, especially if many instances of the class are expected. `slotscheck` will verify this.
    * **Other Pre-commit Hooks**:
        * `check-ast`: Ensures the code is valid Python AST.
        * `check-case-conflict`: Checks for files that would conflict on case-insensitive filesystems.
        * `check-toml`: Validates TOML files (like `pyproject.toml`).
        * `debug-statements`: Prevents committing `pdb` or other debug statements.
        * `end-of-file-fixer`: Ensures files end with a single newline.
        * `mixed-line-ending`: Enforces consistent line endings.
        * `trailing-whitespace`: Removes trailing whitespace.
        * `codespell`: Checks for common misspellings in code and documentation.

## Testing

8. **Test Location**:
    * **Unit Tests**: Place unit tests in `tests/unit/`. Mirror the `sqlspec/` directory structure where appropriate (e.g., unit tests for `sqlspec/adapters/foo.py` should be in `tests/unit/test_adapters/test_foo.py`).
    * **Integration Tests**: Place integration tests in `tests/integration/`. These tests should interact with actual database services or external systems.

9. **Test Coverage**:
    * Write tests for all new code. Aim for high test coverage.
    * Ensure new tests pass before submitting code.
    * Use `pytest` as the test runner.

10. **Test Fixtures**:
    * Utilize `pytest` fixtures for test setup and teardown, especially for database connections or shared resources. Look for existing fixtures in `tests/fixtures/` or within test modules.
    * Design tests to be as parameterized and complete as possible, covering various scenarios and edge cases.


## Documentation

11. **Docstrings**:
    * Write clear and concise docstrings for all public modules, classes, functions, and methods, following a consistent style (e.g., Google or NumPy style, check existing docs for prevalence).
    * Explain the purpose, arguments, and return values.
    * Use type hints in docstrings where applicable.
    * Exceptions must be documented and described in the docstrings.
    * Args and Kwargs should be documented in the docstring, especially for public APIs.
    * Use google style docstring directives for parameters and return values in docstrings, especially for public APIs.
    * Do not document the return type of it is None.  Only document the return type if it is not None.
    * Use raises directive to document exceptions that a function may raise.

12. **Project Documentation**:
    * Update or add documentation in the `docs/` directory as needed. This likely uses Sphinx.
    * For new features or modules, create corresponding `.rst` or `.md` files in the appropriate `docs/` subdirectory (e.g., `docs/usage/`, `docs/reference/`).

## Common Patterns & Best Practices

13. **Configuration**:
    * For components requiring configuration (like database adapters), follow existing patterns. For example, `sqlspec/adapters/bigquery/config/` and `sqlspec/adapters/psycopg/config/` suggest a pattern of dedicated config modules.

14. **Error Handling**:
    * Implement robust error handling. Define custom exceptions where appropriate.  Errors are defined in `sqlspec/exceptions.py`.
    * Assume that there will be a single wrapper for all exceptions, so ensure that exceptions are raised with sufficient context to be useful.  Do not needlessly catch and re-raise exceptions unless adding context or handling specific cases.
    * Use `logging` for error reporting and debugging. Ensure that logs are meaningful and provide context.  Do not user f-string in log messages and do not use `print` statements for debugging or logging.

15. **Avoid**:
    * Global variables where possible.
    * Overly complex comprehensions or one-liners that sacrifice readability.
    * Directly committing secrets or sensitive credentials. Use environment variables or configuration files (ignored by git) for such data.
    * Do not needlessly assign variables for the sake of assignment or type hinting.  Only assign variables when they are used or needed for clarity.
    * If the variable is only use once, please do not assign it to a variable.  Use the value directly in the expression or function call.

## Pre-commit Hooks & CI

16. **Pre-commit**:
    * Ensure pre-commit hooks pass before pushing changes. Install (`pre-commit install --hook-type commit-msg --hook-type pre-commit`) and use the hooks defined in `.pre-commit-config.yaml`.
    * This will enforce linting, formatting, type checking, and commit message conventions.

17. **Workflow Adherence**:
    * Follow contribution guidelines, if any (e.g., `CONTRIBUTING.rst`).
    * Be mindful of GitHub Actions workflows in `.github/workflows/` which might run checks on pull requests.

18. **Commit Messages**:
    * Commit messages must follow the Conventional Commits specification (enforced by `conventional-pre-commit`).

19. **Documentation Linting**:
    * Sphinx documentation (`.rst` files, etc.) will be linted by `sphinx-lint`. Ensure there are no linting errors.
