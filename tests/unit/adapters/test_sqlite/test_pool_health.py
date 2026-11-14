def test_serialized_writer_leases_allow_nested_use_and_release_threads(tmp_path) -> None:
    import threading

    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": str(tmp_path / "serialized.db")}, replicas=True)
    pool = config.provide_pool()
    started = threading.Event()
    acquired = threading.Event()

    def writer() -> None:
        started.set()
        with pool.get_connection():
            acquired.set()

    with pool.get_connection() as outer:
        with pool.get_connection() as inner:
            assert inner is outer
        thread = threading.Thread(target=writer)
        thread.start()
        assert started.wait(5)
        assert acquired.wait(0.05) is False
    assert acquired.wait(5)
    thread.join(5)
    config.close_pool()
