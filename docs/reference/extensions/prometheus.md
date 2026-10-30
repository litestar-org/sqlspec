# Prometheus Extension

```python
from sqlspec.extensions.prometheus import (
    PrometheusStatementObserver,
    enable_metrics,
)
```

Prometheus statement observer and configuration helper exporting statement counters and duration histograms via [`ObservabilityConfig`](../observability.md). See the [Observability Guide](../../learn/observability.md) for usage examples.

## Statement Observer & Configuration Helper

::: sqlspec.extensions.prometheus._observer.PrometheusStatementObserver
    options:
      heading_level: 3

::: sqlspec.extensions.prometheus._observer.enable_metrics
    options:
      heading_level: 3
