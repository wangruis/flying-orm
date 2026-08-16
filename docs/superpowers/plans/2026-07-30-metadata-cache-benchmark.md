# Metadata Cache Benchmark Implementation Plan

> 历史计划：其中 `MetadataCacheOptions` API 已被当前 `CacheRegionPolicy` 统一权重缓存设计取代；当前实现与用法以需求索引和 V2.0.0 文档为准。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add runnable Caffeine metadata-cache benchmark coverage and a small concurrency safety test.

**Architecture:** Keep production cache APIs unchanged. Add one JMH class in `flying-orm-benchmark` that uses an in-memory metadata reader, and one focused rdb unit test that proves concurrent same-key reads share the cached `Mono`.

**Tech Stack:** Java 21, Reactor, Caffeine 3.2.4, JMH 1.37, JUnit 6.1.1.

## Global Constraints

- `flying-orm` main project must not depend on Spring or contain Spring code.
- Keep tests light; do not add real database pressure tests in this task.
- Keep comments natural and clear.
- Remove unused imports and variables after edits.
- Preserve the public `MetadataCacheOptions`, `MetadataCacheInvalidator`, `ReactiveFormMetadataCache`, and `ReactiveFormMetadataReaders.cached(...)` entry points.

---

## File Structure

- Create `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/MetadataCacheBenchmark.java`
  - Owns all metadata-cache JMH scenarios.
  - Contains a small in-memory `ReactiveFormMetadataReader` delegate.

- Modify `flying-orm-rdb/src/test/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReaderTest.java`
  - Adds one focused concurrent same-key read test.

- Modify `docs/source-feature-matrix.md`
  - Marks metadata cache benchmark entry as available.

- Modify `docs/requirements/index.md`
  - Updates R-016 progress.

- Modify `docs/requirements/index.md`
  - Marks focused benchmark/stress entry as done for the first pass.

---

### Task 1: Add Metadata Cache JMH Benchmark

**Files:**
- Create: `flying-orm-benchmark/src/main/java/com/flying/orm/benchmark/MetadataCacheBenchmark.java`

**Interfaces:**
- Consumes: `ReactiveFormMetadataReaders.cached(ReactiveFormMetadataReader, MetadataCacheOptions)`
- Consumes: `ReactiveFormMetadataReader.readForm(String, String)` and `MetadataCacheInvalidator.invalidate(String)`
- Produces: JMH methods `hotFormRead()`, `manyTableRead()`, and `readWithInvalidation()`

- [ ] **Step 1: Create benchmark class**

Create `MetadataCacheBenchmark` with this shape:

```java
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MetadataCacheBenchmark {

    @Param({"128"})
    public int tableCount;

    private ReactiveFormMetadataCache cache;
    private CountingMetadataReader delegate;
    private AtomicInteger cursor;

    @Setup
    public void setUp() {
        delegate = new CountingMetadataReader();
        cache = ReactiveFormMetadataReaders.cached(
                delegate,
                new MetadataCacheOptions(Duration.ofMinutes(5), tableCount));
        cursor = new AtomicInteger();
    }
}
```

- [ ] **Step 2: Add hot-form benchmark**

Add:

```java
@Benchmark
public DynamicForm hotFormRead() {
    return cache.readForm("users", "Users").block();
}
```

- [ ] **Step 3: Add many-table benchmark**

Add:

```java
@Benchmark
public DynamicForm manyTableRead() {
    int index = Math.floorMod(cursor.getAndIncrement(), tableCount);
    return cache.readForm("form_" + index, "table_" + index).block();
}
```

- [ ] **Step 4: Add read-with-invalidation benchmark**

Add:

```java
@Benchmark
public DynamicForm readWithInvalidation() {
    int index = Math.floorMod(cursor.getAndIncrement(), tableCount);
    String table = "table_" + index;
    if (index % 32 == 0) {
        cache.invalidate(table);
    }
    return cache.readForm("form_" + index, table).block();
}
```

- [ ] **Step 5: Add counting delegate**

Use a private static delegate:

```java
private static final class CountingMetadataReader implements ReactiveFormMetadataReader {

    private final AtomicInteger formReads = new AtomicInteger();

    @Override
    public Mono<DynamicForm> readForm(String formId, String table) {
        return Mono.fromSupplier(() -> DynamicForm.builder(formId, table)
                                                  .addField(DynamicField.primaryKey(
                                                          "id_" + formReads.incrementAndGet(),
                                                          "BIGINT"))
                                                  .build());
    }
}
```

Also implement `readForm(String, String, String)` and `readTable(...)` variants if the interface requires them.

- [ ] **Step 6: Compile benchmark module**

Run:

```bash
mvn -pl flying-orm-benchmark -am -DskipTests compile
```

Expected: build success.

---

### Task 2: Add Focused Concurrent Cache Test

**Files:**
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReaderTest.java`

**Interfaces:**
- Consumes: `ReactiveFormMetadataCache.readForm(String, String)`
- Produces: JUnit test `sharesSingleLoadForConcurrentSameKeyReads()`

- [ ] **Step 1: Add imports**

Add imports for:

```java
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
```

- [ ] **Step 2: Add test**

Add:

```java
@Test
void sharesSingleLoadForConcurrentSameKeyReads() throws InterruptedException {
    CountingMetadataReader delegate = new CountingMetadataReader();
    ReactiveFormMetadataCache reader = ReactiveFormMetadataReaders.cached(delegate);
    CountDownLatch start = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>();
    List<DynamicForm> results = new ArrayList<>();

    for (int i = 0; i < 16; i++) {
        Thread thread = new Thread(() -> {
            try {
                start.await();
                DynamicForm form = reader.readForm("users", "Users").block();
                synchronized (results) {
                    results.add(form);
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        });
        threads.add(thread);
        thread.start();
    }

    start.countDown();
    for (Thread thread : threads) {
        thread.join();
    }

    assertEquals(16, results.size());
    assertEquals(1, delegate.formReads());
}
```

- [ ] **Step 3: Run focused rdb test**

Run:

```bash
mvn -pl flying-orm-rdb -Dtest=CachedReactiveFormMetadataReaderTest test
```

Expected: all tests in the class pass.

---

### Task 3: Sync Docs

**Files:**
- Modify: `docs/source-feature-matrix.md`
- Modify: `docs/requirements/index.md`
- Modify: `docs/requirements/index.md`

**Interfaces:**
- Produces: docs that mention metadata cache benchmark entry exists.

- [ ] **Step 1: Update source feature matrix**

Change metadata cache performance candidate to mention `MetadataCacheBenchmark`.

- [ ] **Step 2: Update R-016**

Add that Caffeine cache now has benchmark entry and focused concurrent same-key test.

- [ ] **Step 3: Update phased plan**

Mark first-pass metadata cache benchmark/stress task as complete, leaving real database pressure tests for later.

- [ ] **Step 4: Verify text**

Run:

```bash
rg -n "MetadataCacheBenchmark|Caffeine|并发" docs/source-feature-matrix.md docs/requirements/index.md
```

Expected: the new benchmark and remaining follow-up are visible.

---

### Task 4: Final Verification

**Files:**
- No new files.

**Interfaces:**
- Produces: verified build and focused tests.

- [ ] **Step 1: Compile rdb and benchmark**

Run:

```bash
mvn -pl flying-orm-rdb,flying-orm-benchmark -am -DskipTests compile
```

Expected: build success.

- [ ] **Step 2: Run focused tests**

Run:

```bash
mvn -pl flying-orm-rdb -Dtest=CachedReactiveFormMetadataReaderTest test
```

Expected: test success.

- [ ] **Step 3: Optional short JMH smoke**

If time and local environment allow:

```bash
mvn -pl flying-orm-benchmark -am -DskipTests package
java -cp flying-orm-benchmark/target/flying-orm-benchmark-1.0.0.jar com.flying.orm.benchmark.BenchmarkRunner --include ".*MetadataCacheBenchmark.hotFormRead" --warmup 1 --measurement 1 --forks 1
```

Expected: a JSON result appears under `flying-orm-benchmark/target/benchmark-results`.

---

## Self-Review

- Spec coverage: hot read, many table read, read with invalidation, lightweight concurrent test, and docs sync are all mapped to tasks.
- Placeholder scan: no TBD or TODO placeholders.
- Type consistency: benchmark methods use the public `ReactiveFormMetadataCache`, `ReactiveFormMetadataReader`, `DynamicForm`, and `MetadataCacheOptions` names.
