# Reactive Batch Chunking And Transaction Implementation Plan

> 历史实施计划。批量的 ATOMIC/INDEPENDENT/UNKNOWN 契约继续有效，但阻塞桥方案已被 V2.0.0
> 的原生 JDBC 同步内核替代；本文中的桥接 API 不是当前公共入口。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为动态表单批量插入加入默认整批原子事务、显式独立分片、真实响应式背压、传统 JDBC 风格阻塞桥接入口，以及可选的 UNKNOWN 回执恢复。

**Architecture:** `ReactiveFormClient` 是主线客户端，从首行编译一次 `BatchInsertPlan`，把参数行流交给 R2DBC 批量执行契约；`R2dbcBatchWriter` 负责真实非阻塞的有界分片、连接、事务和结果状态。传统 JDBC 风格入口后续改为 `Sync/Blocking` 桥接层，内部调用同一个 R2DBC/Reactor 内核并阻塞等待结果，不再规划 `DataSource`/`java.sql.Connection` 双内核。回执模式通过同事务唯一记录确认提交并支持幂等重放。现有 Maven 模块保持不变。

**Tech Stack:** Java 21、Reactor、R2DBC SPI、JUnit 5、Reactor Test、R2DBC H2。传统 JDBC 风格只指同步 API 体验，不引入 JDBC 执行内核作为正式路线；flying-orm 本体不依赖应用框架，也不包含应用框架自动配置。

## Global Constraints

- 只支持当前已规划数据库：MySQL、PostgreSQL、H2；Oracle 和 SQL Server 留后续，OpenGauss 不支持。
- 默认模式必须是 `ATOMIC`，任一分片失败时整批回滚。
- `INDEPENDENT` 必须显式开启，逐分片结果按完成顺序发出。
- 不增加 Maven 模块，不引入新的运行时依赖。
- 默认 `chunkSize=500`、`concurrency=1`，原子模式禁止分片并发。
- SQL 在单次调用中只编译一次；数据按分片保留，不能先收集整批。
- R2DBC 内核统一持有 `BatchWriteOptions`、`BatchWriteResult`、`BatchChunkResult`、条件 AST、SQL 渲染器、方言和参数顺序；同步桥接层只复用这些契约。
- R2DBC 路径不能使用 JDBC、`DataSource`、`block()` 或线程等待；传统 JDBC 风格入口允许在最外层阻塞等待 R2DBC 结果，但不能创建另一套 JDBC 事务语义。
- 注释和 Javadoc 使用自然中文；公开类型和公开方法保留 `@author wangr`、日期和版本。
- 测试只覆盖关键契约和真实事务边界，不展开参数组合矩阵。
- 当前目录不是 Git 仓库；每个任务使用“检查点”代替虚构的 commit 步骤。

## File Map

### 新增公开模型

- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteOptions.java`：提交模式、分片大小、并发、限制和回执配置。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchChunkResult.java`：单个分片的最终状态、错误和恢复令牌。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteResult.java`：整次调用的汇总结果。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteException.java`：携带已有结果的终止异常。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchResolution.java`：UNKNOWN 确认结果。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteRequest.java`：执行器消费的 SQL 计划和参数行流。

### 新增内部组件

- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/BatchInsertPlan.java`：保存一次调用内复用的字段布局和 SQL。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`：执行 ATOMIC 和 INDEPENDENT。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/SyncFormClient.java`：正式同步表单入口，提供传统调用习惯的阻塞桥接能力。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/sync/SyncSqlExecutor.java`：正式同步 SQL 执行契约。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/sync/R2dbcSyncSqlExecutor.java`：包内的 R2DBC/Reactor 同步阻塞桥接实现，使用方统一从 `SyncSqlExecutor.bridge(...)` 进入。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/BatchPayloadHasher.java`：生成稳定摘要。
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/BatchReceiptStore.java`：回执占位、完成和查询。

### 修改现有文件

- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormDataSqlRenderer.java`
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/ReactiveSqlExecutor.java`
- `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutor.java`
- `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/FormDataSqlRendererTest.java`
- `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveFormClientTest.java`
- `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutorTest.java`
- `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/H2R2dbcBatchIntegrationTest.java`
- `docs/requirements/2026-07-23-reactive-batch-chunking-transaction-design.md`
- `docs/requirements/index.md`

### 新增回执建表文档

- `docs/requirements/sql/batch-receipt-h2.sql`
- `docs/requirements/sql/batch-receipt-mysql.sql`
- `docs/requirements/sql/batch-receipt-postgresql.sql`

---

### Task 1: 批量配置和结果模型

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteOptions.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchChunkResult.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteResult.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteException.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchResolution.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/batch/BatchWriteOptionsTest.java`

**Interfaces:**
- Produces: `BatchWriteOptions.defaults()`, `atomic(int)`, `independent(int)`, `independent(int,int)`。
- Produces: `BatchChunkResult.Status` = `COMMITTED | ROLLED_BACK | FAILED | UNKNOWN`。
- Produces: `BatchWriteResult.Status` = `COMMITTED | PARTIAL | ROLLED_BACK | UNKNOWN`。
- Produces: `BatchChunkResult.RecoveryToken` 和 `BatchResolution`。

- [x] **Step 1: 写一个聚焦失败测试**

```java
@Test
void defaultsToAtomicAndRejectsUnsafeValues() {
    BatchWriteOptions defaults = BatchWriteOptions.defaults();

    assertEquals(BatchWriteOptions.Mode.ATOMIC, defaults.mode());
    assertEquals(500, defaults.chunkSize());
    assertEquals(1, defaults.concurrency());
    assertEquals(BatchWriteOptions.RecoveryMode.NONE, defaults.recovery().mode());
    assertThrows(IllegalArgumentException.class, () -> BatchWriteOptions.atomic(0));
    assertThrows(IllegalArgumentException.class, () -> BatchWriteOptions.independent(100, 0));
}
```

- [x] **Step 2: 运行测试并确认因类型不存在而失败**

Run:

```powershell
mvn -pl flying-orm-rdb -am -Dtest=BatchWriteOptionsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `COMPILATION ERROR`，提示 `BatchWriteOptions` 不存在。

- [x] **Step 3: 实现紧凑模型和不变量**

`BatchWriteOptions` 使用下面的公开形状；`withMaxRows`、`withTimeout` 和 `withReceipt` 都返回新实例，不修改原对象。

```java
public record BatchWriteOptions(Mode mode,
                                int chunkSize,
                                int concurrency,
                                long maxRows,
                                Duration timeout,
                                Recovery recovery) {
    public static final int DEFAULT_CHUNK_SIZE = 500;

    public enum Mode { ATOMIC, INDEPENDENT }

    public enum RecoveryMode { NONE, RECEIPT }

    public record Recovery(RecoveryMode mode,
                           String operationId,
                           String receiptTable,
                           Duration confirmTimeout) {
        public static Recovery none() {
            return new Recovery(RecoveryMode.NONE, "", "flying_orm_batch_receipt", Duration.ZERO);
        }
    }

    public static BatchWriteOptions defaults() {
        return atomic(DEFAULT_CHUNK_SIZE);
    }

    public static BatchWriteOptions atomic(int chunkSize) {
        return new BatchWriteOptions(
                Mode.ATOMIC, chunkSize, 1, 0, Duration.ZERO, Duration.ZERO, Recovery.none());
    }

    public static BatchWriteOptions independent(int chunkSize, int concurrency) {
        return new BatchWriteOptions(
                Mode.INDEPENDENT, chunkSize, concurrency, 0, Duration.ZERO, Duration.ZERO, Recovery.none());
    }

    public static BatchWriteOptions independent(int chunkSize) {
        return independent(chunkSize, 1);
    }
}
```

`BatchChunkResult.Failure` 保存异常类名、消息、SQL state 和错误码；不保存 SQL 参数。所有结果列表在构造时使用 `List.copyOf`。

恢复令牌作为 `BatchChunkResult` 的嵌套记录，避免再增加一个顶层类型：

```java
public record RecoveryToken(String operationId,
                            int chunkIndex,
                            String receiptTable,
                            String planHash,
                            String payloadHash) {
}
```

- [x] **Step 4: 补充结果汇总测试并运行**

```java
@Test
void summarizesIndependentFailuresAsPartial() {
    BatchWriteResult result = BatchWriteResult.from(
            BatchWriteOptions.Mode.INDEPENDENT,
            List.of(committedChunk(0, 2), failedChunk(1, 2)));

    assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
    assertEquals(4, result.inputCount());
    assertEquals(2, result.affectedRows());
}
```

Expected: 两个测试通过。

- [x] **Step 5: 检查点**

确认公开类型都有自然中文 Javadoc、空值校验和不可变集合，没有引入新依赖。

---

### Task 2: 单次编译的动态表单批量计划

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteRequest.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/BatchInsertPlan.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormDataSqlRenderer.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/FormDataSqlRendererTest.java`

**Interfaces:**
- Consumes: `BatchWriteOptions`。
- Produces: `FormDataSqlRenderer.insertPlan(DynamicForm, Map<String,Object>)`。
- Produces: `BatchInsertPlan.parameters(Map<String,Object>, long)` 和 `request(Publisher<Object[]>, BatchWriteOptions)`。

- [x] **Step 1: 写字段布局复用测试**

```java
@Test
void compilesOneInsertPlanAndMapsLaterRowsByLayout() {
    BatchInsertPlan plan = renderer.insertPlan(form(), orderedMap("id", 1L, "name", "A", "age", 20));

    assertEquals("insert into Users (id, name, age) values (?, ?, ?)", plan.sql());
    assertArrayEquals(new Object[]{2L, "B", 21},
                      plan.parameters(orderedMap("age", 21, "name", "B", "id", 2L), 1));
    assertThrows(IllegalArgumentException.class,
                 () -> plan.parameters(orderedMap("id", 3L, "name", "C"), 2));
}
```

- [x] **Step 2: 运行测试并确认失败**

Run:

```powershell
mvn -pl flying-orm-rdb -am -Dtest=FormDataSqlRendererTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: `BatchInsertPlan` 或 `insertPlan` 不存在导致编译失败。

- [x] **Step 3: 实现执行计划**

`BatchWriteRequest` 使用下面的执行器契约，只传递一行一组的参数数组，不提前保存整批：

```java
public record BatchWriteRequest(String sql,
                                int parameterCount,
                                List<Class<?>> parameterTypes,
                                SqlBindMarkerStyle bindMarkerStyle,
                                Publisher<Object[]> rows,
                                BatchWriteOptions options) {
}
```

`BatchInsertPlan` 保持包内可见，持有渲染器、`DynamicForm`、SQL、不可变字段布局和参数类型。参数类型按逻辑类型映射常用 Java 类型，未知类型回退 `Object.class`。`FormDataSqlRenderer.parametersForLayout(...)` 从私有方法改为包内可见，仍只有同包执行计划调用。

```java
record BatchInsertPlan(FormDataSqlRenderer renderer,
                       DynamicForm form,
                       String sql,
                       List<DynamicField> layout,
                       List<Class<?>> parameterTypes) {
    Object[] parameters(Map<String, Object> row, long rowIndex) {
        List<Object> values = renderer.parametersForLayout(form, layout, row, rowIndex);
        return values.toArray(Object[]::new);
    }

    BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options) {
        return new BatchWriteRequest(sql,
                                     layout.size(),
                                     parameterTypes,
                                     SqlBindMarkerStyle.CANONICAL,
                                     rows,
                                     options);
    }
}
```

`FormDataSqlRenderer.insertBatch(...)` 调用 `insertPlan(...)` 和 `plan.parameters(...)`，List 便捷输入也直接生成统一的 `BatchWriteRequest`，避免字段校验和执行语义分成两套。

- [x] **Step 4: 运行渲染器测试**

Expected: 原有批量字段顺序、字段不一致和重复规范化字段测试继续通过，新计划测试通过。

- [x] **Step 5: 检查点**

确认 SQL 只在 `insertPlan` 生成一次，后续参数行不再创建 SQL 文本。旧 JDBC 同步批量路径只保留为临时验证，不再继续扩展；R2DBC 后续 Task 3 消费 `BatchWriteRequest(Publisher<Object[]>)`。后续 `Sync/Blocking` 桥接层直接复用 R2DBC 内核的 `BatchWriteOptions` 和结果模型。

---

### Task 3: 客户端响应式入口和执行契约

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/ReactiveSqlExecutor.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/ReactiveFormClient.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/form/ReactiveFormClientTest.java`

**Interfaces:**
- Consumes: `BatchWriteRequest`、`BatchWriteResult`、`BatchChunkResult`。
- Produces: `ReactiveSqlExecutor.writeBatch(...)`、`writeBatchChunks(...)`、`resolveUnknown(...)`。
- Produces: `ReactiveFormClient.insertBatch(...)` 三个入口。

- [x] **Step 1: 写客户端委托测试**

```java
@Test
void defaultsPublisherBatchToAtomicWithoutCollectingAllRows() {
    Flux<Map<String, Object>> rows = Flux.just(orderedMap("id", 1L, "name", "A"),
                                               orderedMap("name", "B", "id", 2L));

    StepVerifier.create(client.insertBatch(form(), rows))
                .assertNext(result -> assertEquals(BatchWriteOptions.Mode.ATOMIC, result.mode()))
                .verifyComplete();

    assertEquals(BatchWriteOptions.Mode.ATOMIC, executor.writeRequest().options().mode());
}
```

- [x] **Step 2: 运行测试并确认新入口缺失**

Expected: 编译失败，提示 `insertBatch(DynamicForm, Publisher)` 或 `writeBatch` 不存在。

- [x] **Step 3: 扩展执行契约**

```java
default Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
    Objects.requireNonNull(request, "batch write request must not be null");
    return Mono.error(new UnsupportedOperationException("reactive sql executor does not support chunked batch writes"));
}

default Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
    Objects.requireNonNull(request, "batch write request must not be null");
    return Flux.error(new UnsupportedOperationException("reactive sql executor does not support independent batch writes"));
}

default Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
    Objects.requireNonNull(token, "batch recovery token must not be null");
    return Mono.error(new UnsupportedOperationException("reactive sql executor does not support batch recovery"));
}
```

- [x] **Step 4: 实现客户端首行切换**

```java
public Mono<BatchWriteResult> insertBatch(DynamicForm form,
                                          Publisher<Map<String, Object>> rows,
                                          BatchWriteOptions options) {
    Flux<Map<String, Object>> source = Flux.from(Objects.requireNonNull(rows, "batch rows must not be null"));
    return source.switchOnFirst((signal, replay) -> {
        if (signal.isOnError()) {
            return Mono.error(Objects.requireNonNull(signal.getThrowable()));
        }
        if (!signal.hasValue()) {
            return Mono.just(BatchWriteResult.empty(options.mode()));
        }
        BatchInsertPlan plan = renderer.insertPlan(form, signal.get());
        Flux<Object[]> parameters = replay.index().map(indexed -> plan.parameters(indexed.getT2(), indexed.getT1()));
        return executor.writeBatch(plan.request(parameters, options));
    }).single();
}
```

`List` 重载委托给 `Flux.fromIterable(rows)`；无配置重载使用 `BatchWriteOptions.defaults()`；`insertBatchChunks` 先检查模式必须为 `INDEPENDENT`，然后调用 `writeBatchChunks`。

- [x] **Step 5: 运行 `ReactiveFormClientTest`**

Expected: 新入口测试通过。当前为降低破坏面，原 `List<Map<...>> -> Mono<Long>` 批量入口暂时保留，新增 `Publisher<Map<...>> -> Mono<BatchWriteResult>` 和 `insertBatchChunks(...)` 走新 `BatchWriteRequest` 契约；真正 ATOMIC 事务执行在 Task 4 接入 `R2dbcBatchWriter` 后替换默认 unsupported 行为。

---

### Task 4: ATOMIC 分片事务执行

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutor.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/H2R2dbcBatchIntegrationTest.java`

**Interfaces:**
- Consumes: `BatchWriteRequest` 参数行流。
- Produces: `R2dbcBatchWriter.write(...)` 和 `R2dbcSqlExecutor.writeBatch(...)`。

- [x] **Step 1: 写原子事务契约测试**

```java
@Test
void atomicBatchUsesOneConnectionAndRollsBackWhenLaterChunkFails() {
    BatchWriteRequest request = request(Flux.just(row(1), row(2), row(2)), BatchWriteOptions.atomic(2));
    connection.failStatementNumber(2);

    StepVerifier.create(executor.writeBatch(request))
                .expectErrorSatisfies(error -> {
                    BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                    assertEquals(BatchWriteResult.Status.ROLLED_BACK, batchError.result().status());
                })
                .verify();

    assertEquals(1, connection.beginCount());
    assertEquals(0, connection.commitCount());
    assertEquals(1, connection.rollbackCount());
    assertEquals(1, connection.closeCount());
}
```

- [x] **Step 2: 运行测试并确认执行器尚未支持**

Expected: `UnsupportedOperationException` 或新方法未实现导致失败。

- [x] **Step 3: 实现有界分片和事务资源状态**

`R2dbcBatchWriter` 先执行 `Flux.from(request.rows()).buffer(request.options().chunkSize())`，再用 `switchOnFirst` 确保空输入不申请连接。ATOMIC 使用一个连接和顺序 `concatMap`。

```java
Mono<BatchWriteResult> writeAtomic(BatchWriteRequest request) {
    Flux<List<Object[]>> chunks = guardedRows(request).buffer(request.options().chunkSize());
    return chunks.switchOnFirst((signal, replay) -> {
        if (signal.isOnError()) {
            return Mono.error(Objects.requireNonNull(signal.getThrowable()));
        }
        if (!signal.hasValue()) {
            return Mono.just(BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC));
        }
        TransactionState state = new TransactionState();
        return Mono.usingWhen(Mono.from(connectionFactory.create()),
                              connection -> begin(connection, state)
                                      .thenMany(replay.index().concatMap(indexed -> executeChunk(
                                              connection, request, indexed.getT1().intValue(), indexed.getT2())))
                                      .collectList()
                                      .flatMap(results -> commit(connection, state)
                                              .thenReturn(BatchWriteResult.committed(request.options().mode(), results))),
                              connection -> close(connection),
                              (connection, error) -> cleanupAfterError(connection, state),
                              connection -> rollbackThenClose(connection, state));
    }).single();
}
```

`TransactionState` 至少区分 `ACTIVE`、`COMMITTING`、`COMMITTED`。执行错误时回滚并把失败分片标成 `FAILED`、之前成功执行的分片标成 `ROLLED_BACK`；提交确认丢失时不谎报回滚，返回带 `UNKNOWN` 结果的 `BatchWriteException`。

- [x] **Step 4: 接入 SQL 适配和绑定**

把 `sqlForDriver(...)` 和数组绑定提取为 `R2dbcSqlExecutor` 包内可复用方法。每个分片创建一个 `Statement`，分片内用 `Statement.add()`；空值优先使用 `BatchWriteRequest.parameterTypes()`，未知类型仍回退 `Object.class`。

- [x] **Step 5: 运行执行器测试**

Expected: 原有单 Statement 批处理测试通过；ATOMIC 测试确认一个连接、一次事务、失败回滚和一次关闭。

---

### Task 5: INDEPENDENT 完成顺序和有界并发

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutor.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/H2R2dbcBatchIntegrationTest.java`

**Interfaces:**
- Consumes: `BatchWriteOptions.Mode.INDEPENDENT`。
- Produces: `Flux<BatchChunkResult> writeChunks(...)`，结果按完成顺序发出。

- [x] **Step 1: 写部分成功和并发上限测试**

```java
@Test
void independentBatchContinuesAfterFailedChunkAndBoundsConnections() {
    BatchWriteOptions options = BatchWriteOptions.independent(2, 2);
    factory.failChunk(1);

    StepVerifier.create(executor.writeBatchChunks(request(sixRows(), options)).collectList())
                .assertNext(results -> {
                    assertEquals(3, results.size());
                    assertEquals(BatchChunkResult.Status.FAILED, result(results, 1).status());
                    assertEquals(BatchChunkResult.Status.COMMITTED, result(results, 2).status());
                })
                .verifyComplete();

    assertTrue(factory.maxActiveConnections() <= 2);
}
```

- [x] **Step 2: 运行测试并确认失败**

Expected: 独立入口仍返回 unsupported 或失败后提前终止。

- [x] **Step 3: 实现分片独立事务**

```java
Flux<BatchChunkResult> writeIndependent(BatchWriteRequest request) {
    return guardedRows(request)
            .buffer(request.options().chunkSize())
            .index()
            .flatMap(indexed -> executeIndependentChunk(request,
                                                        indexed.getT1().intValue(),
                                                        indexed.getT2()),
                     request.options().concurrency(),
                     1);
}
```

每个 `executeIndependentChunk` 单独创建连接和事务。Statement 或数据约束失败且回滚成功时返回 `FAILED`，继续后续分片；连接创建失败属于全局故障，终止流；提交确认丢失返回 `UNKNOWN` 分片，不自动重试。

- [x] **Step 4: 实现汇总入口**

`writeBatch` 在 INDEPENDENT 下调用 `writeIndependent(request).collectList()`，按 `chunkIndex` 排序后调用 `BatchWriteResult.from(...)`。直接 `Flux` 保持完成顺序，不排序。

- [x] **Step 5: 运行执行器测试**

Expected: 中间分片失败后后续分片仍提交；汇总状态为 `PARTIAL`。并发上限后续用专门连接工厂测试补充，当前先用真实 H2 验证事务事实。

---

### Task 6: 任务限制、取消和错误报告

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutorTest.java`

**Interfaces:**
- Consumes: `maxRows`、`timeout`。
- Produces: 带已有分片结果的 `BatchWriteException`。

- [x] **Step 1: 写超限和取消测试**

进度：已补 `ATOMIC + maxRows`、`INDEPENDENT + maxRows` 和任务超时的真实 H2 用例。前者覆盖“默认原子模式超限必须整批回滚，并且结果能看到触发超限的那一行”；后者覆盖“前面分片已提交，后面被全局限制拦停时，上层仍能拿到已有结果”。取消清理通过事务状态对象实现，测试保持在关键路径。

```java
@Test
void atomicBatchRollsBackWhenRowLimitIsExceeded() {
    BatchWriteOptions options = BatchWriteOptions.atomic(2).withMaxRows(2);

    StepVerifier.create(executor.writeBatch(request(Flux.just(row(1), row(2), row(3)), options)))
                .expectError(BatchWriteException.class)
                .verify();

    assertEquals(1, connection.rollbackCount());
    assertEquals(1, connection.closeCount());
}
```

- [x] **Step 2: 运行测试并确认限制未生效**

Expected: 当前实现错误提交第三行或返回普通异常。

进度：`INDEPENDENT` 新用例先失败为普通 `IllegalArgumentException`，证明汇总入口会丢掉已有分片结果；`ATOMIC` 新用例先失败在 `inputCount` 统计缺失，证明失败结果没有计入触发超限的行。

- [x] **Step 3: 在参数行进入 buffer 前检查行数**

```java
Flux<Object[]> guardedRows(BatchWriteRequest request) {
    Flux<Object[]> rows = Flux.from(request.rows())
                              .index()
                              .handle((indexed, sink) -> {
                                  long maxRows = request.options().maxRows();
                                  if (maxRows > 0 && indexed.getT1() >= maxRows) {
                                      sink.error(limitExceeded(indexed.getT1() + 1));
                                  } else {
                                      sink.next(indexed.getT2());
                                  }
                              });
    Duration timeout = request.options().timeout();
    return timeout.isZero() ? rows : rows.timeout(timeout);
}
```

把超限、上游错误和超时统一包装成 `BatchWriteException`。ATOMIC 错误和取消走回滚；INDEPENDENT 保留已提交结果，停止申请新连接，并把已有结果放进异常。

进度：`INDEPENDENT` 汇总入口已记录已确认分片；`maxRows` 超限会合成一个失败分片，并通过 `BatchWriteException.result()` 返回 `PARTIAL` 汇总。`ATOMIC` 超限也会把已执行分片标为 `ROLLED_BACK`、把触发超限的行标为 `FAILED`，汇总为 `ROLLED_BACK`。超时会包装为 `BatchWriteException`，避免上层只看到裸异常。

- [x] **Step 4: 增加提交阶段取消保护**

提交开始后由 `TransactionState` 决定清理动作：`ACTIVE` 可以回滚；`COMMITTING` 不能把取消假装成已回滚，提交无明确确认时产生 `UNKNOWN`。

- [x] **Step 5: 运行限制和资源测试**

Expected: 超限、超时、错误和取消路径都只关闭一次连接，ATOMIC 不留下已提交行。

---

### Task 7: 回执摘要、建表 SQL 和确认入口

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/BatchPayloadHasher.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/BatchReceiptStore.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcSqlExecutor.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/BatchPayloadHasherTest.java`
- Create: `docs/requirements/sql/batch-receipt-h2.sql`
- Create: `docs/requirements/sql/batch-receipt-mysql.sql`
- Create: `docs/requirements/sql/batch-receipt-postgresql.sql`

**Interfaces:**
- Consumes: `BatchWriteOptions.RecoveryMode.RECEIPT`。
- Produces: 稳定 `planHash`、`payloadHash`、事务内回执和 `resolveUnknown(...)`。

- [x] **Step 1: 写确定性摘要测试**

```java
@Test
void hashesSameTypedValuesDeterministicallyAndKeepsTypesDistinct() {
    String first = hasher.hash(List.of(new Object[]{1L, "A", null}));
    String second = hasher.hash(List.of(new Object[]{1L, "A", null}));
    String integer = hasher.hash(List.of(new Object[]{1, "A", null}));

    assertEquals(first, second);
    assertNotEquals(first, integer);
}
```

- [x] **Step 2: 运行测试并确认摘要器不存在**

Expected: 编译失败，提示 `BatchPayloadHasher` 不存在。

- [x] **Step 3: 实现规范化 SHA-256 编码**

每个值写入类型标签、长度和值：`null`、字符串、布尔、整数、`BigDecimal`、浮点十六进制、UUID、日期时间、`byte[]`、`ByteBuffer` 和 R2DBC `Parameter`。未知对象抛出明确异常，不使用 `toString()` 或默认 `hashCode()`。

```java
void update(MessageDigest digest, Object value) {
    if (value == null) {
        put(digest, "NULL", new byte[0]);
    } else if (value instanceof CharSequence text) {
        put(digest, "TEXT", text.toString().getBytes(StandardCharsets.UTF_8));
    } else if (value instanceof Long number) {
        put(digest, "INT64", ByteBuffer.allocate(Long.BYTES).putLong(number).array());
    } else if (value instanceof byte[] bytes) {
        put(digest, "BYTES", bytes);
    } else {
        updateKnownScalar(digest, value);
    }
}
```

- [x] **Step 4: 实现回执存储**

`BatchReceiptStore` 在调用方连接和事务内执行：

```java
Mono<Void> reserve(Connection connection, RecoveryKey key, String planHash);
Mono<Void> complete(Connection connection, RecoveryKey key, String payloadHash, long rowCount, long affectedRows);
Mono<Receipt> find(RecoveryKey key);
```

三个建表文件都使用 `(operation_id, chunk_index)` 主键，并包含 `plan_hash`、`payload_hash`、`row_count`、`affected_rows`、`created_at`。DML 参数标记继续经过现有 PostgreSQL 适配。

- [x] **Step 5: 接入确认入口并运行测试**

`R2dbcSqlExecutor.resolveUnknown(token)` 委托回执存储并只返回 `COMMITTED` 或 `UNKNOWN`；一次查不到不能返回已回滚。运行 `BatchPayloadHasherTest` 和一个回执查询执行器测试。

---

### Task 8: 回执幂等重放和真实 H2 事务验证

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/R2dbcBatchWriter.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/H2R2dbcBatchIntegrationTest.java`

**Interfaces:**
- Consumes: `operationId`、回执唯一键和参数摘要。
- Produces: UNKNOWN 后使用相同操作编号安全重放。

- [x] **Step 1: 写 ATOMIC 真实回滚测试**

创建 H2 表后，以 `chunkSize=2` 插入三行，第三行重复主键。通过 `StepVerifier` 断言 `BatchWriteException.result().status()` 为 `ROLLED_BACK`，随后查询 `count(*)` 必须为 `0`。

- [x] **Step 2: 写 INDEPENDENT 真实部分成功测试**

使用三片数据，让中间分片重复主键。断言分片状态依次包含 `COMMITTED`、`FAILED`、`COMMITTED`，数据库只保存前后两个成功分片的数据。

- [x] **Step 3: 写提交确认丢失后的幂等重放测试**

用测试连接代理让第一次 `commitTransaction()` 先真实提交，再向客户端返回错误。第一次结果必须是 `UNKNOWN` 并携带恢复令牌；相同 `operationId` 和相同数据再次执行时读取回执，不新增重复行，最终返回 `COMMITTED`。

- [x] **Step 4: 实现回执事务流程**

RECEIPT 模式在事务开始时 `reserve`，执行业务分片时计算摘要，提交前 `complete`。相同 `operationId`、计划和参数再次执行时先读取已提交回执，一致时直接重建 `COMMITTED` 结果；独立分片提交确认丢失时也会携带分片自己的恢复令牌。

- [x] **Step 5: 运行四个关键验证**

Run:

```powershell
mvn -pl flying-orm-rdb -am -Dtest=H2R2dbcBatchIntegrationTest,R2dbcSqlExecutorTest,BatchPayloadHasherTest,ReactiveFormClientTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: 所有指定测试通过；H2 中原子失败为零行、独立模式部分提交、回执重放无重复。

---

### Task 9: 文档、索引和打包检查

**Files:**
- Modify: `docs/requirements/2026-07-23-reactive-batch-chunking-transaction-design.md`
- Modify: `docs/requirements/2026-07-23-reactive-dynamic-form-batch-insert.md`
- Modify: `docs/requirements/index.md`

**Interfaces:**
- Consumes: 已通过验证的最终 API 和状态语义。
- Produces: 可直接查阅的功能状态、回执部署说明和后续基准入口。

- [x] **Step 1: 更新设计状态和需求索引**

把设计文档状态改为“已实现”，在 R-011 下记录默认 ATOMIC、显式 INDEPENDENT、回执恢复和三个数据库建表脚本。原批量插入文档的“下一步”移除已完成的分片与事务项。

- [x] **Step 2: 运行完整打包**

Run:

```powershell
mvn -DskipTests package
```

Expected: 五个 Maven 模块全部 `BUILD SUCCESS`。

- [x] **Step 3: 重新索引代码图**

对 `D:\new_code\flying-orm` 运行 codebase-memory `moderate` 索引，项目名继续使用 `flying-orm-current`。确认新批量类型、客户端入口和执行器方法都能被 `search_graph` 找到。

- [x] **Step 4: 最终检查点**

确认没有新增 Maven 模块、没有 OpenGauss 代码、运行时依赖未增加、文档和代码状态一致，并记录未执行的 JMH 基准为下一阶段工作。

## Plan Self-Review

- Spec coverage: ATOMIC、INDEPENDENT、完成顺序、有界并发、背压、限制、取消、UNKNOWN、回执和文档都有对应任务。
- Placeholder scan: 每个任务都有明确文件、接口、失败验证、实现动作和通过标准，不存在占位步骤。
- Type consistency: 客户端、执行器和恢复入口统一使用 `BatchWriteRequest`、`BatchWriteResult`、`BatchChunkResult.RecoveryToken`。
- Scope: 自适应分片、upsert、自动重试、跨调用计划缓存、主键回填和性能比例声明都没有进入本轮实现。
