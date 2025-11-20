import pytest
from sqlspec.adapters.spanner import SpannerDriver


@pytest.mark.spanner
def test_connection_pooling(spanner_session: SpannerDriver) -> None:
    """Test that we can acquire a session and execute a simple query."""
    result = spanner_session.select_value("SELECT 1")
    assert result == 1


@pytest.mark.spanner
def test_session_management(spanner_config) -> None:
    """Test session lifecycle."""
    with spanner_config.provide_session() as session:
        assert session.select_value("SELECT 1") == 1
