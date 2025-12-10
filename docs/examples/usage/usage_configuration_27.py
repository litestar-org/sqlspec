"""Test configuration example: Best practice - Tune pool sizes."""

__all__ = ("test_tune_pool_sizes_best_practice",)


MIN_POOL_SIZE_CPU = 5
MAX_POOL_SIZE_CPU = 10
MIN_IO_BOUND_POOL_SIZE = 20
MAX_IO_BOUND_POOL_SIZE = 50


def test_tune_pool_sizes_best_practice() -> None:
    """Test pool sizing best practices for different workloads."""

    # start-example
    # CPU-bound workload - smaller pool
    cpu_bound_connection_config = {"min_size": 5, "max_size": 10}
    # end-example
    assert cpu_bound_connection_config["min_size"] == 5
    assert cpu_bound_connection_config["max_size"] == 10

    # I/O-bound workload - larger pool
    io_bound_connection_config = {"min_size": 20, "max_size": 50}
    assert io_bound_connection_config["min_size"] == 20
    assert io_bound_connection_config["max_size"] == 50
