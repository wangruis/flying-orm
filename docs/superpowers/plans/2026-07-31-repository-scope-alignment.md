# Repository Scope Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Reactive/Sync Repository preserve full DataScope semantics across typed reads, writes, logical deletion, optimistic locking, and physical deletion.

**Architecture:** FormClient remains the only layer that merges default and explicit scopes. Repository performs entity-specific mapping and annotation fallback, then forwards the original scope without extracting its condition.

**Tech Stack:** Java 21, Reactor, Maven, JUnit 5, R2DBC-facing FormClient APIs.

## Global Constraints

- Main project remains framework-neutral and contains no Spring dependencies.
- Add no business-specific scope types.
- Keep R2DBC execution non-blocking and Sync APIs as a bridge over the same path.
- Use focused tests and one batch commit after the complete feature is verified.

---

### Task 1: Prove Complete Scope Preservation

**Files:**
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/repository/ReactiveFormRepositoryTest.java`

**Interfaces:**
- Consumes: existing `ReactiveFormRepository` and `DataScope` APIs.
- Produces: failing tests for typed field trimming, write protection, tenant enforcement, and physical deletion.

- [ ] Add a typed select test whose explicit `FieldScope` only allows a subset of entity columns.
- [ ] Add an update test whose explicit `FieldScope` rejects a non-writable entity field before SQL execution.
- [ ] Add a tenant-enabled form test showing that Repository must preserve trusted tenant metadata.
- [ ] Add a scoped physical-delete test for a form with logical deletion.
- [ ] Run `mvn -pl flying-orm-rdb -am "-Dtest=ReactiveFormRepositoryTest" "-Dsurefire.failIfNoSpecifiedTests=false" test` and confirm the new tests fail for the expected missing behavior.

### Task 2: Add Typed FormClient Scope Overloads

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/SyncFormClient.java`

**Interfaces:**
- Produces: typed `select` and `page` overloads accepting `DataScope`, with optional `SqlExecutionOptions`.

- [ ] Add Reactive typed select overloads that map the existing scoped map query.
- [ ] Add Reactive typed page overloads that map the existing scoped map page.
- [ ] Add matching Sync facade overloads.
- [ ] Keep scope merging inside the existing FormClient scoped map methods.
- [ ] Resolve the effective scope once per select/page so field trimming and WHERE construction use the same provider snapshot.

### Task 3: Forward Complete Scope Through Repository

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/repository/ReactiveFormRepository.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/repository/SyncFormRepository.java`

**Interfaces:**
- Consumes: Task 2 typed FormClient scope overloads.
- Produces: complete scope forwarding for select, page, update, delete, optimistic-lock variants, and physical delete.

- [ ] Replace condition-only scope splicing with direct FormClient scope calls.
- [ ] Add missing `DataScope + OptimisticLockOptions` overloads where the FormClient already supports the combination.
- [ ] Add scoped physical-delete overloads and route them to FormClient physical delete.
- [ ] Remove obsolete `ConditionGroups` imports and private condition-splicing helpers.
- [ ] Run `ReactiveFormRepositoryTest` and confirm all new and existing tests pass.

### Task 4: Documentation And Batch Verification

**Files:**
- Modify: `docs/requirements/index.md`
- Modify: `docs/source-feature-matrix.md`
- Modify: `docs/flying-orm-phased-implementation-plan.md`
- Modify: `docs/target-api-examples.md`

- [ ] Document that Repository forwards complete scope and does not understand business-specific authorization models.
- [ ] Run focused Repository, FormClient, DataScope, logic-delete, and optimistic-lock tests.
- [ ] Run `mvn -DskipTests package`.
- [ ] Run `git diff --check` and remove unused imports, variables, and obsolete helpers.
- [ ] Commit and push the complete batch once.
