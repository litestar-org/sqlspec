# Testing Guide for SQLSpec

This guide outlines the testing procedures for the `sqlspec` project, leveraging `pytest-databases` for integration testing against real database instances.

## Running Tests

The project uses `make` for convenience.

- **Run all tests:**

    ```bash
    make test
    ```

- **Run tests with coverage:**

    ```bash
    make coverage
    ```

- **Run a specific test file:**

    ```bash
    uv run pytest tests/path/to/test.py
    ```

- **Run a single test function:**

    ```bash
    uv run pytest tests/path/to/test.py::test_name
    ```

## Linting and Type Checking

Before committing, ensure all quality checks pass.

- **Run all linting and type checking:**

    ```bash
    make lint
    ```

- **Auto-fix linting issues:**

    ```bash
    make fix
    ```

- **Run MyPy:**

    ```bash
    make mypy
    ```

## Integration Testing with Databases

`sqlspec` uses `pytest-databases` to manage Docker containers for integration tests. To run tests that require a database, you first need to start the infrastructure.

- **Start database containers:**

    ```bash
    make infra-up
    ```

- **Stop database containers:**

    ```bash
    make infra-down
    ```

### Oracle Database

- The Oracle tests use the official Oracle database container images.
- The driver supports both **thin** and **thick** client modes. When implementing changes, ensure both modes are supported if possible.
- For **Autonomous Database** connectivity, additional configuration for wallets and connection strings will be required. Refer to the official `oracledb` documentation for details on connecting to OCI, Google Cloud, etc.
