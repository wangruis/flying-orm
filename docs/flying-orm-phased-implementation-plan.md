# flying-orm Phased Implementation Plan

> 本文是 V1 的历史实施计划，已完成项不再改写。V2.0.0 的 JDBC/R2DBC 双执行内核、统一事务、统一接入和日志规划
> 以 [`v2.0.0-roadmap.md`](v2.0.0-roadmap.md) 为唯一执行清单；本文中“仅 R2DBC 内核”的表述只描述 V1 决策。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build flying-orm as a compact, high-performance ORM for dynamic forms using a new optimized internal architecture.

**Architecture:** Start with a 3+1 module layout: `flying-orm-core`, `flying-orm-rdb`, `flying-orm-testkit`, and `flying-orm-benchmark`. Keep SQL modeling, dialect rendering, metadata indexing, and parameter binding centralized so hot paths can be measured and optimized.

**Tech Stack:** Java 21, Maven, JUnit Jupiter, JMH, Reactor, R2DBC, H2, and fixed-version Docker certification environments. Traditional JDBC-style support is a blocking API bridge over R2DBC, not a separate JDBC executor.

## Current Status

Phases 0 through 12.6 are complete, including the V1 core, post-release enhancements, real-database certification, concurrency verification, performance investigation, and the `v1.0.0` GitHub source release. The historical MySQL single-row auto-commit P99 gate remains recorded as not passed because the skipped Linux or independent-disk cross-check was not performed; the diagnosis work itself is complete.

The five agreed post-release enhancements are complete: keyset pagination, compensating data migration, formal Oracle/SQL Server support, cache metric export, and driver-value/column-alias adaptation. The formal NVD-backed audit was explicitly skipped; this records scope and does not claim a vulnerability-scan pass. Phase 13 remains an optional upper-layer project and must not add Spring dependencies to this repository.

## Global Constraints

- Historical third-party API compatibility is not required.
- The full roadmap must cover the capabilities required by dynamic-form applications.
- Performance, high concurrency, high throughput, and stability are primary design goals.
- Light SQL and heavy Java is a primary design rule: business behavior should be expressed through Java DSL, metadata, structured conditions, and type-safe runtime models.
- Dynamic forms and dynamic table structure maintenance are first-class use cases.
- Parameter-driven dynamic conditions and custom term ids such as `user-in-org` are P0 capabilities.
- R2DBC/Reactor is the execution kernel and must be truly non-blocking, not a reactive wrapper around JDBC.
- Traditional JDBC-style APIs are synchronous facades over the R2DBC kernel; do not design a parallel `DataSource`/`java.sql.Connection` runtime.
- Initial module count must stay small.
- Source project code should be treated as feature baseline and benchmark target, not copied wholesale.
- Every performance claim must have a benchmark or profiling result.
- Runtime users should primarily depend on `flying-orm-rdb`.
- Spring Boot auto-configuration belongs to an upper service or the sibling example project; it must not be added to flying-orm core modules.
- During early scaffolding, use lightweight compile/package smoke checks instead of exhaustive tests; add focused contract tests when production behavior is introduced.

## Current Benchmark Entry Points

- `flying-orm-benchmark` now has first-pass JMH entry points for batch insert/upsert planning, frontend structured-condition compilation, and SQL where rendering.
- These benchmarks are measurement hooks only. They do not count as a performance claim until run in a controlled environment and published with complete parameters.
- Batch insert/upsert plans now precompute column-to-parameter indexes, so each row no longer builds a temporary field map just to align parameters.
- `BenchmarkRunner` now gives JMH a stable project entry point, writes JSON results under `target/benchmark-results` by default, and can pin threads, per-iteration duration, throughput, average-time, or sample-time mode for comparable runs.

---

## Phase 0: Source Baseline and Product Definition

**Goal:** Turn the V1 capability scope and performance targets into measurable acceptance criteria.

**Deliverables:**

- `docs/source-feature-matrix.md`
- `docs/performance-baseline-plan.md`
- Initial benchmark scenarios for source project
- Initial flying-orm API examples

**Tasks:**

- [x] Inventory source features by package and test coverage.
- [x] Group features into MVP, post-MVP, advanced, and compatibility categories.
- [x] Identify source hot paths from graph and benchmark candidates.
- [x] Write target flying-orm usage examples for query, insert, update, delete, and metadata.
- [x] Decide Maven coordinates and Java package prefix.
- [x] Confirm the release boundary: publish source on GitHub without adding a project license, Maven repository publication, or artifact signing.

**Acceptance Criteria:**

- Every major V1 feature area has an entry in the matrix.
- MVP scope is explicitly separated from full roadmap scope.
- Benchmark targets include SQL generation, metadata lookup, parameter binding, batch write planning, and result mapping.

## Phase 1: Project Skeleton

**Goal:** Create a compact Maven project with enough structure to support TDD and benchmarks.

**Modules:**

- `flying-orm-core`
- `flying-orm-rdb`
- `flying-orm-testkit`
- `flying-orm-benchmark`

**Tasks:**

- [x] Create parent Maven project with Java 21 compiler settings.
- [x] Add core, rdb, testkit, and benchmark modules.
- [x] Configure unit test execution.
- [x] Configure JMH benchmark module.
- [x] Keep formatting review-based for V1; no mandatory checkstyle or formatter was added.
- [x] Add README with project goal and current release status.

**Acceptance Criteria:**

- `mvn -DskipTests package` runs successfully as the initial scaffold smoke check.
- `mvn -pl flying-orm-benchmark -DskipTests package` builds the benchmark jar.
- Empty modules have clear package roots.

## Phase 2: Core Metadata and Type Model

**Goal:** Build the read-heavy metadata foundation with indexed lookup and thread-safe publication.

**Core Types:**

- `DatabaseMetadata`
- `SchemaMetadata`
- `TableMetadata`
- `ColumnMetadata`
- `IndexMetadata`
- `DataType`
- `ValueCodec`
- `FeatureRegistry`

**Tasks:**

- [x] Write tests for table lookup by normalized name.
- [x] Write tests for column lookup by normalized name.
- [x] Write tests for primary key and index metadata.
- [x] Implement immutable metadata builders.
- [x] Implement O(1) lookup maps.
- [x] Implement feature registry lookup by id and type.
- [x] Implement first-pass `ValueCodecRegistry` for enum, boolean, number, and Java time conversion.
- [x] Route dynamic Map, batch, and repository write parameters through `ValueCodecRegistry.write()`.
- [x] Add a JMH benchmark for repeated metadata lookup and cache invalidation.

**Acceptance Criteria:**

- Metadata lookup is thread-safe after build.
- Repeated table and column lookup benchmark is at least 2x faster than source baseline.
- Missing table/column errors are explicit and deterministic.

## Phase 3: Condition DSL and Structured Input Model

**Goal:** Define a small, safe structured-condition model, with custom term ids as a first-class extension point.

**Core Types:**

- `ConditionNode`
- `ConditionGroup`
- `TermCondition`
- `LogicalOperator`
- `ConditionValueShape`
- `TermRegistry`
- `StructuredConditionInput`
- `StructuredConditionCompiler`
- `ParameterConditionCompiler`

**Tasks:**

- [x] Write usage examples as tests for query conditions.
- [x] Write tests for custom term ids such as `user-in-org`.
- [x] Write tests for parameter-driven condition groups.
- [x] Write tests for nested `and` and `or` groups.
- [x] Write tests for null handling.
- [x] Write tests for unsupported operator handling.
- [x] Implement immutable condition model.
- [x] Keep query, update, and delete builders in the RDB facade while sharing the same immutable condition model.
- [x] Route rendered condition parameters through `ValueCodecRegistry.write()`.
- [x] Add benchmark for frontend structured-condition compilation.
- [x] Let Reactive/Sync form clients plug a structured-condition resolver for business terms and JSON inputs.
- [x] Add composite structured-condition resolver/customizer chain so multiple frontend condition extensions can work together.
- [x] Add structured-condition resolver presets for common JSON + business-term combinations.
- [x] Keep structured-condition resolver presets as pure Java APIs; framework integration belongs to an upper layer or separate adapter.
- [x] Unify condition-value normalization across Java DSL, parameter-driven conditions, frontend structured conditions, tenant scopes, and time scopes; every custom term has one explicit value shape, and unknown terms default to scalar validation.

**Acceptance Criteria:**

- DSL can express common conditions: eq, ne, gt, gte, lt, lte, like, in, between, is null, not null.
- DSL can express business terms such as `where("userId", "user-in-org", orgId)` without embedding SQL.
- Conditions remain structured until SQL planning.
- Unsupported terms fail before SQL execution unless explicitly configured otherwise.

## Phase 4: Parameterized SQL Renderer

**Goal:** Render structured conditions directly into deterministic SQL fragments and requests without maintaining a second command/AST/planner hierarchy.

**Core Types:**

- `SqlRenderer`
- `SqlRenderContext`
- `SqlFragment`
- `SqlTermHandler`
- `SqlTermRegistry`
- `SqlBindMarkerStyle`
- `SqlRequest`

**Tasks:**

- [x] Write SQL rendering tests for simple select.
- [x] Write SQL rendering tests for nested where conditions.
- [x] Write SQL rendering tests for insert.
- [x] Write SQL rendering tests for update.
- [x] Write SQL rendering tests for delete.
- [x] Implement direct condition-to-SQL rendering through registered term handlers.
- [x] Validate and quote identifiers through the selected RDB dialect.
- [x] Keep canonical and native bind-marker origins explicit on every SQL request.
- [x] Add benchmark for SQL rendering throughput.

**Acceptance Criteria:**

- Values are represented as parameter slots, not string-inlined.
- Renderer output is deterministic.
- SQL rendering benchmark is at least 30% faster than source baseline for equivalent scenarios.

## Phase 5: R2DBC Executor and H2 Integration

**Goal:** Execute generated SQL through R2DBC with non-blocking connection handling and low-allocation parameter binding.

**RDB Types:**

- `DatabaseClient`
- `ReactiveDatabaseClient`
- `ReactiveSqlExecutor`
- `R2dbcSqlExecutor`
- `Row`
- `RowMapper`
- `ExecutionResult`

**Tasks:**

- [x] Write H2 integration test for insert and query.
- [x] Write H2 integration test for update.
- [x] Write H2 integration test for delete.
- [x] Write tests for generated key behavior.
- [x] Implement the unified client builder.
- [x] Implement R2DBC executor.
- [x] Implement parameter binding from `ParameterSlot`.
- [x] Implement map/row result mapping.
- [x] Add executor overhead benchmark where database latency is minimized.

**Acceptance Criteria:**

- H2 integration tests pass for query, insert, update, and delete.
- R2DBC executor does not depend on global mutable state.
- Parameter binding rejects unsafe native value injection by default.

## Phase 6: DDL MVP

**Goal:** Support basic table creation and alteration for H2/Common.

**Types:**

- `CreateTableCommand`
- `AlterTableCommand`
- `ColumnDefinition`
- `CreateIndexCommand`
- `DdlOperator`

**Tasks:**

- [x] Write rendering test for create table.
- [x] Write rendering test for add column.
- [x] Write rendering test for create index.
- [x] Write H2 integration test for create table then insert/query.
- [x] Write H2 R2DBC metadata reader test for existing table to `DynamicForm`.
- [x] Implement DDL AST.
- [x] Implement H2/Common DDL renderer.
- [x] Implement H2 R2DBC metadata reader for table columns, primary key, type arguments, and comments.
- [x] Implement H2 R2DBC table metadata reader for normal indexes and unique indexes.
- [x] Implement H2 R2DBC table metadata reader for foreign keys.
- [x] Implement first-pass MySQL and PostgreSQL information_schema metadata readers.
- [x] Implement first-pass Oracle and SQL Server metadata readers for table columns, primary keys, type arguments, and comments.
- [x] Implement first-pass MySQL, PostgreSQL, Oracle, and SQL Server index metadata SQL contracts.
- [x] Implement first-pass MySQL, PostgreSQL, Oracle, and SQL Server foreign-key metadata SQL contracts.
- [x] Implement sync metadata reader facade over the reactive metadata reader.
- [x] Expose metadata reading through `DatabaseOperator.metadata()` and `SyncDatabaseOperator.metadata()`.
- [x] Provide pure Java reactive and sync metadata reader factories; framework auto-configuration is out of this project.
- [x] Add `operator.ddl().createOrAlter(...)` entry point with conservative metadata-driven create/add-column/add-index behavior.
- [x] Add createOrAlter migration plan/result APIs so upper layers can see executed SQL and skipped risky changes.
- [x] Add explicit createOrAlter migration options; default SAFE stays conservative, reviewed plans can opt into column change, column drop, index drop, and index rebuild.
- [x] Add structured primary-key-change plan details with old keys, new keys, and suggested manual steps; execution remains manual.
- [x] Add first-pass foreign-key metadata and migration-plan differences for add/drop/change; execution remains manual.
- [x] Expose first-pass foreign-key targets through `operator.ddl().createOrAlter(...).addForeignKey(...)` and the sync facade.

**Acceptance Criteria:**

- MVP can create a table, add columns, create indexes, and use the result with DML.
- DDL type mapping goes through dialect type mapping, not hardcoded scattered strings.

## Phase 7: Batch Write and Upsert Planning

**Goal:** Add high-throughput batch insert and prepare the design for dialect-specific upsert.

**Types:**

- `BatchInsertCommand`
- `BatchSqlPlan`
- `ColumnLayout`
- `UpsertCommand`

**Tasks:**

- [x] Write benchmark for batch insert plan creation.
- [x] Write H2 R2DBC integration test for batch insert.
- [x] Implement column layout precomputation for dynamic form batch rows.
- [x] Execute reactive batches through one R2DBC Statement with multiple parameter groups.
- [x] Implement compact parameter slot layout for batch rows.
- [x] Implement H2-compatible upsert if H2 dialect support is chosen for MVP+.
- [x] Establish repeatable JMH baselines for batch planning throughput and allocation.

**Acceptance Criteria:**

- Batch request creation is at least 30% faster than source baseline.
- Batch layout does not rebuild column metadata per row.

## Phase 8: Production Dialects

**Goal:** Add production dialect rendering after H2 is stable, starting with MySQL, PostgreSQL, Oracle, and SQL Server.

**Tasks:**

- [x] Add MySQL identifier quoting, pagination, type mapping, and upsert rendering.
- [x] Add PostgreSQL identifier quoting, pagination, type mapping, and upsert rendering.
- [x] Add first-pass Oracle identifier quoting, pagination, type mapping, and upsert rendering.
- [x] Add first-pass SQL Server identifier quoting, pagination, type mapping, and upsert rendering.
- [x] Add explicit external R2DBC compatibility entry for MySQL, skipped unless URL is configured.
- [x] Add explicit external R2DBC compatibility entry for PostgreSQL, skipped unless URL is configured.
- [x] Add explicit external metadata compatibility entry for MySQL and PostgreSQL, skipped unless URL is configured.
- [x] Add opt-in MySQL/PostgreSQL test-driver profiles and external batch optimistic-lock smoke scenarios.
- [x] Add first-pass column comment rendering for PostgreSQL, Oracle, and SQL Server.
- [x] Add fixed-version Docker certification and real-driver tests for MySQL.
- [x] Add fixed-version Docker certification and real-driver tests for PostgreSQL.
- [x] Add opt-in R2DBC compatibility smoke entry for Oracle; real target-version certification remains pending.
- [x] Add opt-in R2DBC compatibility smoke entry for SQL Server, including driver bind-marker adaptation; real target-version certification remains pending.
- [x] Add first-pass dialect SQL contract tests through `flying-orm-testkit` shared compatibility cases.
- [x] Add reusable `flying-orm-testkit` reactive smoke scenario for later real-database compatibility tests.
- [x] Add exception translation basics for MySQL and PostgreSQL common SQLState/error-code cases.

**Acceptance Criteria:**

- Query, insert, update, delete, batch insert, and upsert work on supported production dialects after their compatibility tests are enabled.
- Dialect differences are isolated in dialect packages.
- Core command model does not branch directly on database product names.

## Phase 9: Repository and Mapping

**Goal:** Add ergonomic object mapping without compromising hot-path performance.

**Types:**

- `Repository<T, ID>`
- `EntityMetadata`
- `PropertyAccessor`
- `MappingPlan`
- `EntityRowMapper`

**Tasks:**

- [x] Write tests for record mapping.
- [x] Write tests for JavaBean mapping.
- [x] Write tests for map/row mapping.
- [x] Implement mapping metadata.
- [x] Implement precompiled property access plan.
- [x] Route record and Bean row mapping through `ValueCodecRegistry` for safe target-type conversion.
- [x] Implement repository query/save/delete basics.
- [x] Reuse reactive batch insert/upsert from repository entities.
- [x] Add JMH entry point for entity mapping allocation and throughput.
- [x] Establish repeatable JMH baselines for entity mapping allocation and throughput.
- [x] Add single-row optimistic locking for dynamic form update/delete, including explicit version field options, version condition injection, version increment/assignment, stable conflict exception, and Repository integration.
- [x] Add batch optimistic locking and automatic version-field discovery.

**Acceptance Criteria:**

- Result mapping avoids repeated reflection in hot paths.
- Repository remains optional; lower-level `DatabaseClient` stays first-class.

## Phase 6: R2DBC Execution Contract

**Goal:** Establish the non-blocking R2DBC/Reactor execution contract early, so later SQL planning and result mapping are designed around one execution kernel.

**Types:**

- `ReactiveDatabaseClient`
- `ReactiveSqlExecutor`
- `ReactiveSqlRequest`
- `ReactiveResultMapper`

**Tasks:**

- [x] Add Reactor and R2DBC dependency boundaries in `flying-orm-rdb`.
- [x] Define reactive execution interfaces without JDBC coupling.
- [x] Write contract tests for non-blocking API shape where practical.
- [x] Ensure SQL planning output can feed the R2DBC executor and synchronous bridge.

**Acceptance Criteria:**

- Reactive contracts do not depend on JDBC types.
- Reactive path uses Reactor types directly.
- No blocking bridge is introduced.

## Phase 10: Traditional Blocking Bridge

**Goal:** Add traditional JDBC-style synchronous APIs after the R2DBC path is stable and benchmarked.

**Types:**

- `SyncDatabaseClient`
- `BlockingSqlExecutor`
- `SyncFormClient`
- `SyncRepository`

**Tasks:**

- [x] Write blocking bridge test for query.
- [x] Write blocking bridge test for insert/update/delete.
- [x] Implement sync facade over `ReactiveSqlExecutor`.
- [x] Implement sync form client over `ReactiveFormClient`.
- [x] Add explicit timeout policy for `.block(...)`.
- [x] Verify no `DataSource`/`java.sql.Connection` path is introduced.

**Acceptance Criteria:**

- No blocking bridge is used inside reactive executor.
- Reactive API has explicit lifecycle and error semantics.
- R2DBC implementation reuses SQL planning and rendering from core.

## Phase 11: Advanced Database Features

**Goal:** Cover source project's advanced database features after the main ORM surface is stable.

**Feature Areas:**

- JSON query terms
- Arrays
- PostgreSQL vector
- JPA annotation parser
- Oracle advanced types and behavior
- SQL Server advanced types and behavior
- Advanced exception translation

**Tasks:**

- [x] Add feature matrix entries and behavior tests for each advanced feature.
- [x] Implement first-pass JSON operation AST and MySQL/PostgreSQL renderers for path equality, containment, and path existence.
- [x] Add first-pass JSON logical type placeholders for built-in schema dialects.
- [x] Add first-pass MySQL/PostgreSQL JSON text equality term extension entry.
- [x] Add first-pass frontend structured JSON text equality input bridge for MySQL/PostgreSQL.
- [x] Add field-aware JSON value codec for dynamic-form writes, batch/upsert plans, dynamic Map reads, and entity Map/List/JsonNode mapping.
- [x] Add PostgreSQL JSONB parameter expressions without making collection conditions behave like JSON values.
- [x] Verify H2 JSON insert, batch upsert, and structured readback through the real R2DBC driver.
- [x] Add opt-in MySQL/PostgreSQL JSON round-trip smoke entries; real execution still requires external R2DBC URLs.
- [x] Add materialized BLOB/CLOB codecs for byte[], ByteBuffer, CharSequence, dynamic Map reads, and entity mapping; verify insert/upsert/readback with H2 R2DBC.
- [x] Add fully reactive R2DBC Blob/Clob stream consumption without blocking row mapping; enforce per-field byte/character limits, timeout, cancellation, and unused-handle discard rules.
- [x] Add opt-in MySQL/PostgreSQL BLOB/CLOB round-trip and size-limit smoke entries; real execution still requires external R2DBC URLs.
- [x] Implement PostgreSQL array codec, metadata recovery, operation AST, frontend structured input, and dialect renderers.
- [x] Implement PostgreSQL vector type and function support.
- [x] Implement optional JPA metadata parser.
- [x] Expand Oracle and SQL Server compatibility beyond first-pass SQL rendering: version boundaries, pagination rules, sequences, identity columns, database-specific column changes, batch merge/upsert, scalar binding/reading, and error-code contracts are covered without claiming real-database certification.
- [x] Expand common exception translation for MySQL, PostgreSQL, Oracle, and SQL Server: constraints, deadlocks, lock waits, cancellation, wrapped driver errors, and batch failure kinds.

**Acceptance Criteria:**

- Each advanced feature has a source-equivalent behavior test.
- Unsupported dialect-feature combinations fail explicitly.
- Advanced features do not pollute the common hot path.

## Phase 12: Hardening and Release

**Goal:** Make flying-orm stable enough for real usage.

**Tasks:**

- [x] Run full unit test suite.
- [x] Run H2/MySQL/PostgreSQL integration tests.
- [x] Run benchmark suite and publish comparison report.
- [x] Run concurrency stress tests for metadata, plan cache, and executors.
- [x] Replace the lightweight metadata cache with a direct Caffeine dependency for enterprise-grade high-concurrency caching, including TTL, size eviction, invalidation, and cache metrics hooks.
- [x] Add first-pass metadata cache benchmark/stress entry against the Caffeine implementation.
- [x] Validate metadata and cache behavior through JMH cache stress, real-database metadata reads, and sustained real-database workloads.
- [x] Verify optimistic-lock conflicts under real-driver concurrency and report conflicts as observable outcomes instead of ordinary success.
- [x] Add the batch optimistic-lock execution foundation: exact row-count checking, input-offset conflicts, ATOMIC rollback, INDEPENDENT chunk isolation, and optimistic-lock observation categories.
- [x] Expose batch optimistic update through FormClient and Repository without weakening DataScope, tenant, logic-delete, or field-write protection.
- [x] Review public API names and package boundaries.
- [x] Write migration and upgrade guidance for applications adopting flying-orm concepts and APIs.
- [x] Write release notes with supported databases and known limitations.
- [x] Record the explicit decision to skip the formal NVD-backed vulnerability audit without reporting it as passed.

**Acceptance Criteria:**

- Feature matrix has no unclassified source features.
- Benchmarks meet or explain performance targets.
- Public API is documented with examples.
- Runtime dependency graph is acceptable for production use.

## Phase 12.5: Tenant Isolation and DataScope Kernel

**Goal:** Add a framework-neutral data access safety layer before upper Spring integration, so tenant isolation and business data scopes are enforced by flying-orm SQL planning/execution paths instead of being left to every service method.

**Scope Boundary:**

- This phase belongs to `flying-orm-core` and `flying-orm-rdb`; it must not introduce Spring dependencies.
- flying-orm does not become an RBAC/ABAC permission system. Upper services still decide who the user is, which tenant they belong to, and which organizations/devices/resources they can access.
- flying-orm only accepts already-computed scope objects and safely merges them into dynamic-form DML, structured conditions, Repository, and Operator queries.
- Initial support targets shared database + tenant column. Schema-per-tenant, datasource-per-tenant, and database-native RLS are later enhancements.
- Time-series databases are out of scope. Alarm events stored in the business database are in scope as ordinary business event tables.

**Tasks:**

- [x] Add `TenantStrategy` with `NONE`, `AUTO`, and `MANUAL` semantics for dynamic forms.
- [x] Add `TenantScope`, `DataScope`, and `FieldScope` models without Spring or security-framework dependencies; upper layers resolve the current context and pass an immutable `DataScope` snapshot into the ORM.
- [x] Add first-class `DataScope` presets for all data, current organization and children, current organization only, and self-only data; these presets work without tenants and also compose with `TenantScope` for SaaS.
- [x] Add parameterized `TimeScope` with half-open, closed, start-only, and end-only windows; compose it through `DataScope.time(...)` without adding RDB or dialect branches.
- [x] Merge scope conditions with user conditions for select/page/count/update/delete through the existing structured condition and SQL renderer path.
- [x] Support default scope injection for `ReactiveFormClient`, `SyncFormClient`, `DatabaseOperator`, and `FlyingOrmClients`; explicit per-call scope keeps narrowing the default scope instead of replacing it.
- [x] Apply `FieldScope` to readable-column trimming and writable-field protection before SQL rendering.
- [x] Preserve complete scope semantics through Reactive/Sync Repository typed reads, writes, optimistic locking, logical deletion, and explicit physical deletion; Repository no longer extracts only the scope condition.
- [x] Auto-fill or validate tenant fields for insert and batch write when a form declares tenant isolation.
- [x] Prevent frontend structured conditions from spoofing tenant fields, logic-delete fields, or fields excluded by server-provided `FieldScope`; failures keep the existing stable error code and input path.
- [x] Add execution-time guardrails for scoped forms: missing required scope should fail before SQL execution.
- [x] Expose stable scope error codes for missing tenant scope, tenant-value conflicts, and field access failures while keeping `IllegalArgumentException` compatibility.
- [x] Align direct `DatabaseOperator` projection failures with FormClient scope errors, and expose condition, scope, and database failures through one `OrmErrorReport` shape.
- [x] Keep authorization extension generic: upper services can turn any ownership, hierarchy, sharing, or visibility result into ordinary `DataScope`, `TimeScope`, `FieldScope`, or a registered custom term without adding business-specific scope types to the kernel.
- [x] Document generic business-scope collaboration for device ownership, tenant authorization, user sharing, alarm visibility, and time range without adding business-specific models to the kernel.

**Acceptance Criteria:**

- Forms without tenant configuration keep existing behavior.
- Tenant-enabled forms cannot query, update, delete, insert, or batch-write across tenants unless a caller explicitly uses a cross-tenant scope.
- DataScope, FieldScope, and TimeScope combine with ordinary dynamic conditions using parameter binding, not SQL string concatenation. `DataScope.all()` means full data for the current boundary: full table in a non-tenant system, current tenant data when a tenant scope is present, and all tenants only when an explicit cross-tenant scope is used.
- R2DBC and sync bridge users see the same scope behavior.
- Spring Boot adapters can provide tenant/user context later without changing flying-orm core APIs.

Type handling note: logical `OFFSET_TIME` uses native `TIME WITH TIME ZONE` on H2/PostgreSQL. MySQL, Oracle, and SQL Server use a text fallback so the UTC offset is never silently discarded; single writes, batch writes, and dynamic Map reads share this rule.

## Phase 12.6: V1 Post-release Enhancements

**Goal:** Finish the agreed code contracts that sit behind the V1 release line, while keeping real-database certification and release packaging in their final dedicated batches.

**Tasks:**

- [x] Add PostgreSQL Vector value validation, R2DBC binding/reading, structured distance conditions, and nearest-neighbor ordering.
- [x] Add a server-registered SQL template escape hatch with bound values, separately declared identifier slots, exact parameter checks, and no frontend SQL input.
- [x] Extend optional JPA annotation reading for transient fields, generated identity/sequence values, enum storage, and LOB fields without adding a runtime JPA dependency.
- [x] Add pure Java entity mapping events outside the shared reflection-plan cache.
- [x] Add Reactor-context database routing, schema sessions, and PostgreSQL RLS session settings with cleanup before pooled connections are returned.
- [x] Add reviewed migration results with reverse structure SQL, explicit rollback gaps, and online-DDL enforcement modes.
- [x] Certify pgvector, schema switching, RLS cleanup, and reviewed DDL against the final real-database matrix.

**Acceptance Criteria:**

- New APIs remain framework-neutral and keep the main project free of Spring dependencies.
- SQL values are bound; vector operators, template identifiers, schemas, and RLS setting names cannot become arbitrary SQL fragments.
- Routing applies to every executor path because it sits below clients and operators at `ConnectionFactory` level.
- Rollback output never claims deleted row data can be recreated from DDL alone.
- `REQUIRE_ONLINE` refuses potentially blocking SQL instead of silently running a normal alter.

## Phase 12.7: Post-release Enterprise Enhancements

**Goal:** Finish the agreed non-blocking enhancements without changing the V1 release result.

**Tasks:**

- [x] Add composite keyset pagination with stable mixed-direction sorting, next-cursor results, no count query, and a synchronous bridge.
- [x] Add parameterized data-migration plans and reverse-order compensation with explicit `ROLLED_BACK` / `ROLLBACK_FAILED` outcomes.
- [x] Promote the certified Oracle Free 23.26.0 and SQL Server 2022 CU22 GDR1 contracts from Preview to formal support while retaining driver limitations.
- [x] Export Caffeine metadata-cache snapshots through stable metric names without adding a monitoring-framework dependency.
- [x] Add driver-value adapters, explicit row aliases, and qualified/quoted column-label normalization.

**Acceptance Criteria:**

- Cursor pagination never builds an offset proportional to page depth and requires stable server-approved sort fields.
- Data compensation never claims that structure-only DDL can restore deleted row values.
- Formal database support names exact certified versions and keeps observable driver limitations public.
- Monitoring and special-driver integration remain optional, immutable, and safe for concurrent use.

## Phase 13: Upper-layer Spring Boot Adapter

**Goal:** Provide out-of-the-box application integration outside the flying-orm core project.

**Scope Boundary:**

- This phase is implemented in an upper service or the sibling example project, not in `flying-orm-core`, `flying-orm-rdb`, `flying-orm-testkit`, or `flying-orm-benchmark`.
- The adapter depends on `flying-orm-rdb` and uses its pure Java factories.
- Dynamic datasource management, application transactions, configuration binding, and lifecycle belong to the upper layer.

**Tasks:**

- [ ] Auto-configure `RdbDialect` from the available `ConnectionFactory` or explicit configuration.
- [ ] Auto-configure `SqlRenderer` with default terms and user-provided term packages.
- [ ] Auto-configure `ReactiveSqlExecutor`, `ReactiveFormClient`, `ReactiveSchemaClient`, and `ReactiveFormMetadataReader`.
- [ ] Auto-configure `SyncSqlExecutor`, `SyncFormClient`, `DatabaseOperator`, and `SyncDatabaseOperator`.
- [x] Expose batch transaction outcomes so upper services can react to `BatchWriteResult`, `BatchChunkResult`, `BatchWriteException.result()`, and stable `BatchChunkResult.Failure.kind()` values.
- [ ] Keep the adapter optional so non-Spring and plain Java users do not inherit framework dependencies.
- [x] Create a sibling `flying-orm-example` project to verify upper-layer usage through Maven dependencies and public APIs.
- [x] Add a Spring Boot configuration example showing JSON structured conditions, `user-in-org`, and injected `ReactiveFormClient` usage.

**Acceptance Criteria:**

- A Spring Boot application can use flying-orm by adding the adapter and a R2DBC `ConnectionFactory`.
- No Spring Boot dependency appears in flying-orm core modules.
- Manual pure Java assembly remains fully supported and documented.

## Milestone Summary

| Milestone | Scope | Expected Result |
| --- | --- | --- |
| M0 | Baseline and design | Clear feature and benchmark target |
| M1 | Skeleton | Buildable multi-module project |
| M2 | Core metadata/DSL | Structured model and indexed metadata |
| M3 | SQL renderer | Fast H2/Common SQL generation |
| M4 | R2DBC/H2 | Usable MVP ORM path |
| M5 | DDL/batch/upsert | Practical write support |
| M6 | MySQL/PostgreSQL | Production dialect foundation |
| M7 | Repository/Blocking bridge | Higher-level API and traditional synchronous facade |
| M8 | Advanced features | Full source feature coverage |
| M9 | Data isolation | Tenant/DataScope safety kernel |
| M10 | Hardening | Release candidate |

## Recommended First Execution Slice

Start with Phases 0 through 5 only.

This produces a small but real ORM:

- compact architecture
- structured DSL
- indexed metadata
- SQL AST and renderer
- R2DBC execution
- H2 integration tests
- first benchmark comparisons

After that slice passes tests and benchmark review, expand to DDL, batch, MySQL, PostgreSQL, and the traditional blocking bridge.
