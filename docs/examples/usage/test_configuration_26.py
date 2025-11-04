"""Test configuration example: Best practice - Tune pool sizes."""


def test_tune_pool_sizes_best_practice() -> None:
    """Test pool sizing best practices for different workloads."""

    # CPU-bound workload - smaller pool
    cpu_bound_pool_config = {"min_size": 5, "max_size": 10}
    assert cpu_bound_pool_config["min_size"] == 5
    assert cpu_bound_pool_config["max_size"] == 10

    # I/O-bound workload - larger pool
    io_bound_pool_config = {"min_size": 20, "max_size": 50}
    assert io_bound_pool_config["min_size"] == 20
    assert io_bound_pool_config["max_size"] == 50
