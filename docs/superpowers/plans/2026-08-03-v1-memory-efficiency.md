# V1.0.0 Memory Efficiency Implementation Plan

**实施状态（2026-08-03）：已完成。** 下方复选项保留为当时的执行步骤，不再作为实时进度；
最终实现已经通过聚焦契约测试、公开 API 基线更新和全模块编译，外部真实数据库验证按项目总计划留到最终认证阶段。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-row Map allocation with `DynamicRow`, make batch buffering strictly bounded, and place all long-lived caches under one bounded and observable policy before V1.0.0 API freeze.

**Architecture:** `flying-orm-rdb` owns compact result rows, R2DBC row extraction, batch memory budgets, and Caffeine-backed cache regions. `flying-orm-core` remains dependency-free. Existing public Map query signatures are intentionally replaced because V1.0.0 is still being optimized.

**Tech Stack:** Java 21, Reactor 3.8, R2DBC SPI 1.0, Caffeine 3.2, JUnit 5, Reactor Test, Maven 3.9.16, JMH.

## Global Constraints

- Work only in `D:\new_code\flying-orm`.
- Keep `flying-orm-core` and `flying-orm-rdb` free of Spring dependencies.
- Use `D:\apache-maven-3.9.16\bin\mvn.cmd` and `D:\MavenRepository`.
- Keep comments natural, direct, and detailed enough to explain ownership, concurrency, cancellation, and memory boundaries.
- Remove obsolete imports, variables, factories, and Map-only compatibility code touched by the change.
- Do not run external MySQL, PostgreSQL, Oracle, or SQL Server tests in this implementation batch.
- Update the V1.0.0 API baseline only after all focused tests pass and the public changes are reviewed.

---

### Task 1: Compact DynamicRow Model

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/result/DynamicRow.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/result/RowLayout.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/result/DuplicateColumnLabelException.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/result/DynamicRowTest.java`

**Interfaces:**
- Produces: immutable `DynamicRow implements Map<String,Object>` with `value(int)`, `columnName(int)`, `columnCount()`, typed `get`, and `toMap()`.
- Produces: result-scoped `RowLayout` shared by all rows from one R2DBC Result.

- [ ] Write tests for ordered Map reads, null values, typed reads, immutable mutators, array access, materialization, shared layout, and duplicate labels.
- [ ] Run `mvn -pl flying-orm-rdb -am -Dtest=DynamicRowTest -Dsurefire.failIfNoSpecifiedTests=false test` and verify compilation fails because the result types do not exist.
- [ ] Implement adaptive RowLayout lookup: linear scan through 8 columns, shared immutable index above 8 columns.
- [ ] Implement DynamicRow read-only views without copying the values array.
- [ ] Rerun `DynamicRowTest` and verify it passes.

### Task 2: R2DBC and Mapping Pipeline

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/ReactiveSqlExecutor.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutor.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/ObservedReactiveSqlExecutor.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/DefaultOptionsReactiveSqlExecutor.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/mapping/RowMapper.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/mapping/MappingPlan.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutorTest.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/mapping/RowMapperExtensionTest.java`

**Interfaces:**
- Consumes: `DynamicRow`, `RowLayout`.
- Produces: `ReactiveSqlExecutor.query(SqlRequest)` as `Flux<DynamicRow>`.
- Produces: `RowMapper<T>.map(DynamicRow)` with alias indexes instead of renamed Maps.

- [ ] Change focused tests to require DynamicRow and shared row layouts; run them and verify type/behavior failures.
- [ ] Move R2DBC row extraction to one RowLayout per Result and one `Object[]` per row.
- [ ] Update executor decorators without materializing Maps.
- [ ] Update MappingPlan and alias mapping to consume DynamicRow directly.
- [ ] Run executor and mapping tests until green.

### Task 3: Form, Operator, Native SQL, and Testkit Result Types

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/SyncFormClient.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/CountResultReader.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/QueryOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/SyncQueryOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/NativeSqlOperator.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/SyncNativeSqlOperator.java`
- Modify: affected repository, migration, metadata, isolation, benchmark, and testkit callers found by compilation.
- Test: `ReactiveFormClientTest`, `SyncFormClientTest`, `DatabaseOperatorTest`, `NativeSqlOperatorTest`.

**Interfaces:**
- Produces: select/page/cursor/native SQL defaults using `DynamicRow` end to end.
- Keeps: write-side `Map<String,Object>` inputs for dynamic form values.

- [ ] Change focused tests to assert DynamicRow result types and no LinkedHashMap decoding copy; run and verify failures.
- [ ] Replace read-side Map generic signatures throughout form and operator APIs.
- [ ] Decode JSON, arrays, vectors, temporal values, and LOBs into value arrays before publishing DynamicRow.
- [ ] Keep write-side Maps unchanged and remove obsolete read-side copies/imports.
- [ ] Compile `flying-orm-rdb`, testkit, and benchmark; use compiler errors to finish every caller.
- [ ] Run the focused form/operator tests until green.

### Task 4: Batch Memory Limits

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchMemoryLimits.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchMemoryBudget.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchMemoryLimitExceededException.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteOptions.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteRequest.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/bootstrap/FlyingOrmClients.java`
- Test: `BatchWriteOptionsTest`, `R2dbcBatchWriterTest`, `H2R2dbcBatchIntegrationTest`.

**Interfaces:**
- Produces: default `maxRows=100000`, `maxBufferedBytes=32 MiB`, `maxResultChunks=4096`.
- Produces: client hard limits for chunk size, concurrency, rows, bytes, and result details.

- [ ] Write failing tests for defaults, hard-limit rejection, byte estimates, permit release, single-row oversize, and result-detail preflight.
- [ ] Run the focused batch tests and verify expected failures.
- [ ] Extend immutable options and builder defaults without unlimited zero values.
- [ ] Add deterministic value weighting and per-subscription byte accounting.
- [ ] Set Reactor chunk execution prefetch to 1 and release budget on success, error, and cancellation.
- [ ] Make List inputs shallow-copy only the outer list and snapshot rows lazily as they enter a chunk.
- [ ] Run focused batch tests until green.

### Task 5: Streaming RECEIPT Recovery

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/BatchPayloadHasher.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/BatchReceiptStore.java`
- Test: `R2dbcBatchWriterTest`, `BatchPayloadHasherTest`, `H2R2dbcBatchIntegrationTest`.

**Interfaces:**
- Produces: incremental payload hash API that never requires `List<BatchChunk>`.
- Keeps: ATOMIC receipt reservation, completion, idempotent replay, UNKNOWN token, and mismatch detection.

- [ ] Write a failing contract test proving receipt mode consumes a large Publisher incrementally and never requests the complete source before starting execution.
- [ ] Add replay tests for equal and conflicting payloads.
- [ ] Remove `chunks(request).collectList()` and hash rows incrementally while chunks execute.
- [ ] For a completed receipt, consume only for hash comparison and skip writes.
- [ ] Run recovery and H2 batch tests until green.

### Task 6: Unified Bounded Cache Policy

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/cache/CacheRegionPolicy.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/cache/OrmCachePolicy.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/cache/OrmCacheSnapshot.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/cache/BoundedCacheRegion.java`
- Modify: metadata cache options, snapshots, reader factory, and `FlyingOrmClients`.
- Modify: `MappingPlan` codec-registry child cache.
- Test: new cache policy tests plus existing metadata cache tests.

**Interfaces:**
- Produces: Caffeine maximum-weight regions with per-entry rejection, expiry, W-TinyLFU eviction, and framework-free snapshots.
- Produces: metadata explicit, SQL plan bounded-enabled, condition-plan disabled defaults.

- [ ] Write failing tests for default policies, weight bounds, oversized rejection, hit rate, eviction weight, expiry, and clear operations.
- [ ] Implement the generic bounded cache region in rdb only.
- [ ] Adapt metadata caches to weighted policies and preserve precise table invalidation.
- [ ] Replace MappingPlan's unbounded codec map with a bounded or weak-identity child cache and default-registry fast path.
- [ ] Run cache and mapping tests until green.

### Task 7: SQL and Condition Structural Plan Caches

**Files:**
- Create focused plan/key types under `flying-orm-rdb/src/main/java/com/flying/orm/rdb/plan/`.
- Modify: `FormDataSqlRenderer`, structured-condition resolver/compiler adapter, and `FlyingOrmClients`.
- Test: form renderer, structured condition, cache safety, and DDL invalidation tests.

**Interfaces:**
- Produces: keys containing dialect, operation, form fingerprint, projection, sort/page/scope shapes, and registry version but never values.
- Produces: cached plans containing SQL structure and value-slot extractors, never request trees or request values.

- [ ] Write failing tests showing equal shapes with different values share a plan and values never leak.
- [ ] Write failing tests for high-cardinality bounded eviction and DDL table-specific invalidation.
- [ ] Implement SQL plan cache with the bounded region.
- [ ] Implement optional condition-shape cache, disabled by default.
- [ ] Wire invalidation and statistics through FlyingOrmClients.
- [ ] Run focused renderer/condition/cache tests until green.

### Task 8: API, Documentation, Benchmarks, and Final Verification

**Files:**
- Modify: public API baseline and API closure tests.
- Modify: target API examples, V1 roadmap/checklist/release notes, and V1.0.1 roadmap.
- Modify: JMH benchmarks for DynamicRow, batch planning, mapping, and cache regions.

**Interfaces:**
- Produces: reviewed V1.0.0 API baseline matching final bytecode.
- Produces: allocation and throughput comparisons for DynamicRow versus LinkedHashMap.

- [ ] Add benchmark sources and compile benchmark module.
- [ ] Scan all main/test/docs code for stale Map result signatures and removed cache/batch APIs.
- [ ] Run core+rdb focused and full local tests, then testkit local tests; external database tests remain skipped.
- [ ] Generate and review the V1.0.0 API baseline only after tests are green.
- [ ] Run Javadoc and package builds for core, rdb, testkit, and benchmark.
- [ ] Run `git diff --check`, unused import/variable scans, and repository status review.
- [ ] Batch commit implementation and documentation with natural Chinese commit messages.
