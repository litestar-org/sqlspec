# Routing APIs

```python
from sqlspec.config import ReadReplicaGroup
from sqlspec.core.routing import RandomSelector, ReplicaHealthBreaker, ReplicaSpec, RoundRobinSelector
from sqlspec.utils.routing import RoutingContext, RoutingContextPayload, RoutingEntry, RoutingScope, SQLSpecRoutingMiddleware
```

Use [Read Replicas](../learn/recipes/read-replicas.md) for setup and [Replica Operations](../learn/recipes/read-replicas-operations.md) for freshness and failure policy. Context scopes select intent; database roles and policies still govern access and write permission.

## Routing Context and ASGI Middleware

::: sqlspec.utils.routing.RoutingEntry
    options:
      heading_level: 4
      members: false

::: sqlspec.utils.routing.RoutingContextPayload
    options:
      heading_level: 4
      members: false

::: sqlspec.utils.routing.RoutingScope
    options:
      heading_level: 4
      members: false

::: sqlspec.utils.routing.RoutingContext
    options:
      heading_level: 4
      members: [current, get_entry, mark_write, scope, primary, replica, auto, set, reset, clear]

::: sqlspec.utils.routing.SQLSpecRoutingMiddleware
    options:
      heading_level: 4
      members: false

## Replica Specifications, Circuit Breakers, and Selectors

::: sqlspec.core.routing.ReplicaSpec
    options:
      heading_level: 4
      members: false

::: sqlspec.core.routing.ReplicaHealthBreaker
    options:
      heading_level: 4
      members: false

::: sqlspec.core.routing.RoundRobinSelector
    options:
      heading_level: 4
      members: false

::: sqlspec.core.routing.RandomSelector
    options:
      heading_level: 4
      members: false

