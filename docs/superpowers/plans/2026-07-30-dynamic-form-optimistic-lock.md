# Dynamic Form Optimistic Lock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit single-row optimistic locking for dynamic form update/delete.

**Architecture:** Keep optimistic locking as an explicit DML option, not a default behavior. Render version conditions through the existing condition/renderer path, execute through existing `rowsUpdated`, and translate `0` affected rows into a stable conflict exception at the form client and Repository layer.

**Tech Stack:** Java 21, Reactor, R2DBC execution facade, JUnit 6.1.1.

## Global Constraints

- `flying-orm` main project must not depend on Spring or contain Spring code.
- Do not change existing update/delete behavior unless optimistic locking is explicitly passed.
- Keep the first version single-row only; no batch optimistic lock in this task.
- Keep tests focused and light.
- Remove unused imports and variables after edits.

---

## File Structure

- Create `flying-orm-rdb/src/main/java/com/flying/orm/rdb/lock/OptimisticLockMode.java`
- Create `flying-orm-rdb/src/main/java/com/flying/orm/rdb/lock/OptimisticLockOptions.java`
- Create `flying-orm-rdb/src/main/java/com/flying/orm/rdb/lock/OptimisticLockConflictException.java`
- Modify `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormDataSqlRenderer.java`
- Modify `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- Modify `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/SyncFormClient.java`
- Modify `flying-orm-rdb/src/main/java/com/flying/orm/rdb/repository/ReactiveFormRepository.java`
- Modify `flying-orm-rdb/src/main/java/com/flying/orm/rdb/repository/SyncFormRepository.java`
- Test `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/FormDataSqlRendererTest.java`
- Test `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveFormClientTest.java`
- Test `flying-orm-rdb/src/test/java/com/flying/orm/rdb/repository/ReactiveFormRepositoryTest.java`

---

### Task 1: Lock Option Model

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/lock/OptimisticLockMode.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/lock/OptimisticLockOptions.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/lock/OptimisticLockConflictException.java`

**Interfaces:**
- Produces: `OptimisticLockOptions.increment(String field, Object expectedValue)`
- Produces: `OptimisticLockOptions.assign(String field, Object expectedValue, Object nextValue)`
- Produces: `OptimisticLockConflictException(String table, String field, Object expectedValue)`

### Task 2: SQL Rendering

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormDataSqlRenderer.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/FormDataSqlRendererTest.java`

**Interfaces:**
- Consumes: `OptimisticLockOptions`
- Produces: update/delete render overloads that append `where lockField = ?`

### Task 3: Reactive and Sync Form Client

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/SyncFormClient.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveFormClientTest.java`

**Interfaces:**
- Produces: `update(..., OptimisticLockOptions lock, SqlExecutionOptions options)`
- Produces: `delete(..., OptimisticLockOptions lock, SqlExecutionOptions options)`
- Produces: `OptimisticLockConflictException` when affected rows are 0

### Task 4: Repository Entry Points

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/repository/ReactiveFormRepository.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/repository/SyncFormRepository.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/repository/ReactiveFormRepositoryTest.java`

**Interfaces:**
- Produces: Repository update/delete overloads accepting `OptimisticLockOptions`

### Task 5: Docs and Verification

**Files:**
- Modify: `docs/requirements/index.md`
- Modify: `docs/source-feature-matrix.md`
- Modify: `docs/flying-orm-phased-implementation-plan.md`

**Verification:**
- `mvn -pl flying-orm-rdb -am -DskipTests compile`
- `mvn -pl flying-orm-rdb "-Dtest=FormDataSqlRendererTest,ReactiveFormClientTest,ReactiveFormRepositoryTest" test`

## Self-Review

- Spec coverage: option model, SQL rendering, form client, Repository, docs, tests are covered.
- Scope is single-row update/delete only.
- No Spring dependency is introduced.
