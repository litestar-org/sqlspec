from pathlib import Path
from typing import Any

__all__ = ("test_example_26",)


def test_example_26(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example26.db"
    config = SqliteConfig(
        pool_config={
            "database": database.name,
            "timeout": 5.0,
            "check_same_thread": False,
            "cached_statements": 100,
            "uri": False,
        }
    )
    with db.provide_session(config) as session:
        session.execute(
            """CREATE TABLE if not exists products(id integer primary key autoincrement, category_id int, price real, stock int)"""
        )

        # start-example
        # Good use case: dynamic filtering
        def search_products(
            category: Any | None = None, min_price: Any | None = None, in_stock: Any | None = None
        ) -> Any:
            query = sql.select("*").from_("products")
            params = []

            if category:
                query = query.where("category_id = ?")
                params.append(category)

            if min_price:
                query = query.where("price >= ?")
                params.append(min_price)

            if in_stock:
                query = query.where("stock > 0")

            return session.execute(query, *params)

        # end-example
