# Flask Extension

```python
from sqlspec.extensions.flask import FlaskConfigState, SQLSpecAsyncService, SQLSpecPlugin, SQLSpecSyncService
```

Flask WSGI extension managing `flask.g` request-scoped sessions, automatic commit/rollback teardown hooks, and background event-loop portal bridging for async adapters. See the [Flask Integration Guide](../../integrations/flask.md) for usage examples.

## Plugin & Configuration State

::: sqlspec.extensions.flask.extension.SQLSpecPlugin
    options:
      heading_level: 3

::: sqlspec.extensions.flask._state.FlaskConfigState
    options:
      heading_level: 3
