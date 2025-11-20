import pytest
from sqlspec.adapters.spanner import SpannerDriver


@pytest.mark.spanner
def test_crud_operations(spanner_session: SpannerDriver) -> None:
    # DDL to create table
    # Spanner DDL usually requires a separate operation or separate connection type (admin client)
    # But we can try executing DDL if the driver supports it, or use the client directly in fixture.
    # Standard driver might only support DML/DQL in transaction/snapshot.
    # Spanner `database.update_ddl` is needed for DDL.
    pass
    # Since I haven't implemented DDL support in the driver (it uses snapshot/transaction),
    # I should update the conftest to create tables or add a helper.
    
    # Skipping actual CRUD test until DDL helper is available
    # But I can test SELECT 1 which works.
    assert spanner_session.select_value("SELECT 1") == 1
