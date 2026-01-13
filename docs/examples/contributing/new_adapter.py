from __future__ import annotations

from typing import Any, NoReturn

__all__ = ("test_new_adapter",)


def test_new_adapter() -> None:
    # start-example
    from sqlspec.driver import SyncDriverAdapterBase
    from sqlspec.exceptions import SQLSpecError

    class ExampleDriver(SyncDriverAdapterBase):
        def begin(self) -> None:
            raise NotImplementedError

        def commit(self) -> None:
            raise NotImplementedError

        def rollback(self) -> None:
            raise NotImplementedError

        def with_cursor(self, connection: Any) -> NoReturn:
            raise NotImplementedError

        def handle_database_exceptions(self) -> NoReturn:
            msg = "Replace with adapter-specific handler"
            raise SQLSpecError(msg)

        def _connection_in_transaction(self) -> bool:
            return False

    # end-example

    assert ExampleDriver._connection_in_transaction is not None
