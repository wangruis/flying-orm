# Repository Scope Alignment Design

## Goal

Repository must preserve the complete `DataScope` object instead of extracting only its SQL condition. Tenant metadata, `FieldScope`, `TimeScope`, and custom business terms must behave exactly as they do through `ReactiveFormClient` and `SyncFormClient`.

## Design

- Repository keeps entity mapping, annotation fallback, optimistic locking, and logical-delete behavior.
- Every Repository overload that accepts `DataScope` passes that scope to the matching FormClient overload. Repository must not rebuild or partially interpret scope internals.
- A FormClient select/page operation resolves its effective default-plus-explicit scope once, then uses that same snapshot for field trimming and WHERE construction.
- Typed select/page scope overloads are added to Reactive/Sync FormClient so Repository does not have to map scoped rows through a weaker API.
- Scoped update/delete variants resolve logical-delete and optimistic-lock behavior first, then call FormClient with the original scope.
- Scoped physical delete calls the explicit FormClient physical-delete API. It never routes through ordinary delete.
- No device, organization, sharing, or alarm-specific type is added. Those remain examples of composing generic conditions, custom terms, tenant scope, field scope, and time scope.

## Safety Rules

- Default scope and explicit scope continue to combine with AND.
- Field read/write restrictions must survive Repository typed mapping.
- Tenant-enabled forms must still reject Repository operations without a trusted tenant scope.
- OR business conditions remain grouped before scope conditions are added.
- Reactive execution remains non-blocking; Sync Repository remains a blocking facade over the same reactive path.

## Verification

- A typed Repository select with `FieldScope` must select only readable columns.
- A Repository update with a non-writable field must fail before SQL.
- A tenant-enabled Repository operation must preserve tenant metadata, not only the tenant SQL condition.
- Reactive and Sync Repository must render the same scoped SQL.
- Scoped physical delete must render `delete from`, even when the form declares logical delete.
