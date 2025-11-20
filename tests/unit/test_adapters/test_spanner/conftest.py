import sys
from unittest.mock import MagicMock

# Mock google.cloud.spanner_v1 if not present
try:
    import google.cloud.spanner_v1
except ImportError:
    mock_google = MagicMock()
    mock_cloud = MagicMock()
    mock_spanner_v1 = MagicMock()
    
    # Setup module structure
    sys.modules["google"] = mock_google
    sys.modules["google.cloud"] = mock_cloud
    sys.modules["google.cloud.spanner_v1"] = mock_spanner_v1
    sys.modules["google.cloud.spanner_v1.database"] = MagicMock()
    sys.modules["google.cloud.spanner_v1.pool"] = MagicMock()
    sys.modules["google.cloud.spanner_v1.snapshot"] = MagicMock()
    sys.modules["google.cloud.spanner_v1.transaction"] = MagicMock()
    sys.modules["google.cloud.spanner_v1.streamed"] = MagicMock()
    
    # Mock param_types
    mock_param_types = MagicMock()
    mock_param_types.STRING = "STRING"
    mock_param_types.INT64 = "INT64"
    mock_param_types.FLOAT64 = "FLOAT64"
    mock_param_types.BOOL = "BOOL"
    mock_param_types.BYTES = "BYTES"
    mock_param_types.Array = MagicMock(return_value="ARRAY")
    sys.modules["google.cloud.spanner_v1.param_types"] = mock_param_types
    
    mock_spanner_v1.param_types = mock_param_types

    # Mock exceptions
    mock_exceptions = MagicMock()
    # We need exception classes to be types, not just mocks, for isinstance checks
    class AlreadyExists(Exception): pass
    class NotFound(Exception): pass
    class InvalidArgument(Exception): pass
    class PermissionDenied(Exception): pass
    class ServiceUnavailable(Exception): pass
    class TooManyRequests(Exception): pass
    class GoogleAPICallError(Exception): pass
    
    mock_api_core_exceptions = MagicMock()
    mock_api_core_exceptions.AlreadyExists = AlreadyExists
    mock_api_core_exceptions.NotFound = NotFound
    mock_api_core_exceptions.InvalidArgument = InvalidArgument
    mock_api_core_exceptions.PermissionDenied = PermissionDenied
    mock_api_core_exceptions.ServiceUnavailable = ServiceUnavailable
    mock_api_core_exceptions.TooManyRequests = TooManyRequests
    mock_api_core_exceptions.GoogleAPICallError = GoogleAPICallError
    
    sys.modules["google.api_core"] = MagicMock()
    sys.modules["google.api_core.exceptions"] = mock_api_core_exceptions
    
    # Mock auth
    sys.modules["google.auth"] = MagicMock()
    sys.modules["google.auth.credentials"] = MagicMock()

    # Attach to parent modules
    mock_google.cloud = mock_cloud
    mock_cloud.spanner_v1 = mock_spanner_v1