## [REF-005] Driver Protocol Architecture

**DECISION**: Layered protocol with abstract methods for driver implementations.

**IMPLEMENTATION**:

- **Protocol classes**: Define public API (`execute`, `execute_many`, `execute_script`)
- **Abstract methods**: Driver-specific implementations (`_execute_statement`, `_wrap_*_result`)
- **Instrumentation mixins**: Provide telemetry capabilities
- **Common attributes**: Shared functionality and setup

**USER BENEFIT**:

- Consistent API across all database drivers
- Automatic instrumentation for all drivers
- Type safety through protocol compliance

**INHERITANCE HIERARCHY**:

```
SyncDriverAdapterProtocol
├── CommonDriverAttributes (connection management, instrumentation setup)
├── SyncInstrumentationMixin (telemetry capabilities)
└── Abstract methods for driver implementation

PsycopgSyncDriver
├── Inherits from SyncDriverAdapterProtocol
└── Implements: _execute_statement, _wrap_select_result, _wrap_execute_result
```

**KEY POINTS FOR DOCS**:

- Users interact with protocol methods, never abstract methods
- All drivers get instrumentation automatically
- Driver implementations focus on database-specific logic
- Protocol handles statement building, type conversion orchestration

---
