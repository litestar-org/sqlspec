def test_example_26():
    # start-example
    from sqlspec import sql

    # Good use case: dynamic filtering
    def search_products(category=None, min_price=None, in_stock=None):
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
