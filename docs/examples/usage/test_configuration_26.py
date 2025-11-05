"""Test configuration example: Best practice - Tune pool sizes."""

MIN_POOL_SIZE_CPU = 5
MAX_POOL_SIZE_CPU = 10
MIN_IO_BOUND_POOL_SIZE = 20
MAX_IO_BOUND_POOL_SIZE = 50


def test_tune_pool_sizes_best_practice() -> None:
    """Test pool sizing best practices for different workloads."""

    # CPU-bound workload - smaller pool
    cpu_bound_pool_config = {"min_size": MIN_POOL_SIZE_CPU, "max_size": MAX_POOL_SIZE_CPU}
    assert cpu_bound_pool_config["min_size"] == MIN_POOL_SIZE_CPU
    assert cpu_bound_pool_config["max_size"] == MAX_POOL_SIZE_CPU

    # I/O-bound workload - larger pool
    io_bound_pool_config = {"min_size": MIN_IO_BOUND_POOL_SIZE, "max_size": MAX_IO_BOUND_POOL_SIZE}
    assert io_bound_pool_config["min_size"] == MIN_IO_BOUND_POOL_SIZE
    assert io_bound_pool_config["max_size"] == MAX_IO_BOUND_POOL_SIZE


def test_disable_security_checks_best_practice() -> None:
    """Test disabling security checks when necessary."""

    from sqlspec.core.statement import StatementConfig

    # Example: Disabling security checks for trusted internal queries
    statement_config = StatementConfig(
        dialect="postgres",
        enable_validation=False,  # Skip security checks
    )
    assert statement_config.enable_validation is False
