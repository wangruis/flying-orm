package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.param.ParameterConditionCompiler;
import com.flying.orm.core.param.ParameterConditionPackage;
import com.flying.orm.core.param.ParameterConditionSpec;
import com.flying.orm.core.sql.render.RelationTermPackage;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.scope.TimeScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.UpsertDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.SqlLargeObjectLimitExceededException;
import com.flying.orm.rdb.json.JsonStructuredConditions;
import com.flying.orm.rdb.json.JsonTermHandlers;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.SchemaDialect;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证动态表单响应式客户端把 CRUD 操作委托给真正的响应式 SQL 执行器。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class ReactiveFormClientTest {

    /** Form 计划必须把数据库生成主键的物理列名交给原生 R2DBC 内部协作。 */
    @Test
    void passesGeneratedKeyColumnToNativeReactiveExecutor() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm generated = DynamicForm.builder("device", "device")
                                           .addField(DynamicField.primaryKey("id", "BIGINT")
                                                                 .withGeneration(ValueGeneration.identity()))
                                           .addField(DynamicField.of("profile", "JSON"))
                                           .build();

        StepVerifier.create(client.insertReturningKeys(WriteSpec.insert(generated, Map.of("profile", "{}"))))
                    .assertNext(result -> assertEquals(1L, result.affectedRows()))
                    .verifyComplete();

        assertEquals("id", executor.generatedKeyColumn);
    }

    @Test
    void immutableSpecsShareTheSameSafeReactivePipelineAndClientDefaults() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlExecutionOptions defaults = SqlExecutionOptions.maxRows(9).withTimeout(Duration.ofSeconds(3));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                      .withDefaultExecutionOptions(defaults);
        ConditionGroup where = ConditionGroup.and().where("id", "=", "u1").build();

        StepVerifier.create(client.select(QuerySpec.of(form(), where)))
                    .expectNextCount(1)
                    .verifyComplete();
        StepVerifier.create(client.update(WriteSpec.update(form(), orderedMap("name", "新名字"), where)))
                    .expectNext(1L)
                    .verifyComplete();
        StepVerifier.create(client.writeBatch(BatchSpec.insert(
                            form(), Flux.just(orderedMap("id", "u2", "name", "乙")))))
                    .expectNextMatches(result -> result.inputCount() == 1L)
                    .verifyComplete();

        assertEquals(List.of(defaults, defaults), executor.options());
        assertBatchOptionsPreserved(BatchWriteOptions.defaults(), executor.writeRequest().options());
        assertThrows(IllegalArgumentException.class,
                     () -> client.insert(WriteSpec.update(form(), orderedMap("name", "错误入口"), where)));
    }

    /** 验证公开写入规格在构造和访问边界都冻结数组字段，后续执行不会被外部改写。 */
    @Test
    void snapshotsArrayValuesAtTheWriteSpecBoundary() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm binaryForm = DynamicForm.builder("binaryForm", "BinaryRows")
                                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                                            .addField(DynamicField.of("payload", "BINARY"))
                                            .build();
        byte[] payload = new byte[]{1, 2, 3};
        WriteSpec spec = WriteSpec.insert(binaryForm, orderedMap("id", 1L, "payload", payload));

        payload[0] = 9;
        ((byte[]) spec.values().get("payload"))[1] = 8;

        StepVerifier.create(client.insert(spec)).expectNext(1L).verifyComplete();

        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) executor.request().parameters().get(1));
    }

    /** 写入规格必须冻结直接数组值的完整数组图，避免冷执行前改写内部节点。 */
    @Test
    void snapshotsNestedArrayGraphsAtTheWriteSpecBoundary() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm arrayForm = DynamicForm.builder("arrayForm", "ArrayRows")
                                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                                            .addField(DynamicField.of("payload", "ARRAY"))
                                            .build();
        byte[][] payload = new byte[][]{{1, 2, 3}};
        WriteSpec spec = WriteSpec.insert(arrayForm, orderedMap("id", 1L, "payload", payload));

        payload[0][0] = 9;
        ((byte[][]) spec.values().get("payload"))[0][1] = 8;

        StepVerifier.create(client.insert(spec)).expectNext(1L).verifyComplete();

        assertArrayEquals(new byte[]{1, 2, 3}, ((byte[][]) executor.request().parameters().get(1))[0]);
    }

    @Test
    void querySpecPageKeepsPageSortWhenSpecDoesNotOverrideIt() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        FormDataSqlRenderer sqlServerRenderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(),
                RdbDialect.sqlServer());
        ReactiveFormClient client = ReactiveFormClient.create(executor, sqlServerRenderer);

        StepVerifier.create(client.page(QuerySpec.of(form(), ConditionGroup.and().build()),
                                        PageQuery.of(1, 10, PageSort.asc("id"))))
                    .expectNextCount(1)
                    .verifyComplete();

        assertTrue(executor.requests.stream().anyMatch(request -> request.sql().contains("order by")
                && request.sql().contains("offset ? rows fetch next ? rows only")));
    }

    @Test
    void querySpecPageCompilesStructuredConditionsForCountAndRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        QuerySpec spec = QuerySpec.structured(
                form(), StructuredConditionInput.term("name", "=", "王"));

        StepVerifier.create(client.page(spec, PageQuery.of(1, 10)))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals(2, executor.requests.size());
        assertTrue(executor.requests.stream().allMatch(request -> request.sql().contains("name = ?")));
        assertTrue(executor.requests.stream().allMatch(request -> request.parameters().contains("王")));
    }

    @Test
    void querySpecCursorPageCompilesStructuredConditions() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        QuerySpec spec = QuerySpec.structured(
                form(), StructuredConditionInput.term("name", "=", "王"));

        StepVerifier.create(client.cursorPage(spec, CursorPageQuery.first(10, CursorSort.asc("id"))))
                    .expectNextCount(1)
                    .verifyComplete();

        assertTrue(executor.request().sql().contains("name = ?"));
        assertTrue(executor.request().parameters().contains("王"));
    }

    @Test
    void cursorPageDecodesDynamicFormFields() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows(Map.of("id", "u1", "profile", "{\"name\":\"Alice\"}"));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = DynamicForm.builder("profiles", "Profiles")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();

        StepVerifier.create(client.cursorPage(
                            QuerySpec.of(form, ConditionGroup.and().build()),
                            CursorPageQuery.first(10, CursorSort.asc("id"))))
                    .assertNext(page -> assertEquals(Map.of("name", "Alice"),
                                                     page.rows().getFirst().get("profile")))
                    .verifyComplete();
    }

    /**
     * 验证动态表单 CRUD 都通过 Flux/Mono 执行链路完成。
     */
    @Test
    void executesDynamicFormCrudWithReactiveSqlExecutor() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = form();

        StepVerifier.create(client.operations().batchInserts.insert(form, orderedMap("id", "u1", "name", "王")))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("insert into Users (id, name) values (?, ?)", executor.request().sql());

        StepVerifier.create(client.operations().queries.select(form, ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNext(DynamicRow.copyOf(Map.of("id", "u1", "name", "王")))
                    .verifyComplete();
        assertEquals("select id, name from Users where id = ?", executor.request().sql());

        StepVerifier.create(client.operations().writes.update(form,
                                          orderedMap("name", "新名字"),
                                          ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update Users set name = ? where id = ?", executor.request().sql());

        StepVerifier.create(client.operations().deletes.delete(form, ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("delete from Users where id = ?", executor.request().sql());
    }

    @Test
    void decodesJsonFieldsInDynamicFormRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows(Map.of("id", "u1",
                                  "profile", "{\"name\":\"Alice\",\"roles\":[\"admin\"]}"));
        DynamicForm form = DynamicForm.builder("profileForm", "Profiles")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("profile", "JSON"))
                                      .build();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());

        StepVerifier.create(client.operations().queries.select(form, ConditionGroup.and().build()))
                    .assertNext(row -> assertEquals(Map.of("name", "Alice", "roles", List.of("admin")),
                                                    row.get("profile")))
                    .verifyComplete();
    }

    /**
     * 验证 List 批量新增也走统一的结构化批量执行契约。
     */
    @Test
    void insertsDynamicFormRowsAsOneBatchRequest() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());

        StepVerifier.create(client.operations().batchInserts.insertBatch(form(),
                                               List.of(orderedMap("id", "u1", "name", "王"),
                                                       orderedMap("name", "李", "id", "u2"))))
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(2L, result.affectedRows());
                    })
                    .verifyComplete();

        assertEquals("insert into Users (id, name) values (?, ?)", executor.writeRequest().sql());
        assertEquals(2, executor.capturedParameterRows().size());
    }

    /** List 便利入口在返回冷流前冻结外层列表和每行 Map，订阅时看到的是调用瞬间的字段快照。 */
    @Test
    void snapshotsBatchListAndRowsBeforeSubscription() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        Map<String, Object> row = orderedMap("id", "u1", "name", "王");
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row);

        Mono<BatchWriteResult> result = client.operations().batchInserts.insertBatch(form(), rows, BatchWriteOptions.atomic(10));
        rows.clear();
        row.put("name", "订阅前篡改");

        StepVerifier.create(result).expectNextCount(1).verifyComplete();

        assertBatchOptionsPreserved(BatchWriteOptions.atomic(10), executor.writeRequest().options());
        assertArrayEquals(new Object[]{"u1", "王"}, executor.capturedParameterRows().getFirst());
    }

    /** 验证 List 批量便利入口冻结数组字段，使冷订阅前改写源数组不影响实际绑定。 */
    @Test
    void snapshotsBatchArrayValuesBeforeColdSubscription() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm binaryForm = DynamicForm.builder("binaryForm", "BinaryRows")
                                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                                            .addField(DynamicField.of("payload", "BINARY"))
                                            .build();
        byte[] payload = new byte[]{1, 2, 3};

        Mono<BatchWriteResult> result = client.operations().batchInserts.insertBatch(
                binaryForm, List.of(orderedMap("id", 1L, "payload", payload)), BatchWriteOptions.atomic(10));
        payload[0] = 9;

        StepVerifier.create(result).expectNextCount(1).verifyComplete();

        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) executor.capturedParameterRows().getFirst()[1]);
    }

    /** List 批量便利入口必须在返回冷 Mono 前冻结直接数组值的完整数组图。 */
    @Test
    void snapshotsNestedBatchArrayGraphsBeforeColdSubscription() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm arrayForm = DynamicForm.builder("arrayForm", "ArrayRows")
                                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                                            .addField(DynamicField.of("payload", "ARRAY"))
                                            .build();
        byte[][] payload = new byte[][]{{1, 2, 3}};

        Mono<BatchWriteResult> result = client.operations().batchInserts.insertBatch(
                arrayForm, List.of(orderedMap("id", 1L, "payload", payload)), BatchWriteOptions.atomic(10));
        payload[0][0] = 9;

        StepVerifier.create(result).expectNextCount(1).verifyComplete();

        assertArrayEquals(new byte[]{1, 2, 3},
                          ((byte[][]) executor.capturedParameterRows().getFirst()[1])[0]);
    }

    /** 即使批次为空，明显错误的表单参数也不能被空结果悄悄掩盖。 */
    @Test
    void validatesFormBeforeReturningAnEmptyBatchResult() {
        ReactiveFormClient client = ReactiveFormClient.create(new RecordingSqlExecutor(), renderer());

        NullPointerException error = assertThrows(
                NullPointerException.class,
                () -> client.operations().batchInserts.insertBatch(null, java.util.Collections.singletonList(null)));

        assertEquals("dynamic form must not be null", error.getMessage());
    }

    /**
     * 验证 List upsert 与 Publisher 使用同一套批量结果和执行保护。
     */
    @Test
    void upsertsDynamicFormRowsAsOneBatchRequest() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2()));

        StepVerifier.create(client.operations().batchInserts.upsertBatch(form(),
                                               List.of(orderedMap("id", "u1", "name", "王"),
                                                       orderedMap("name", "李", "id", "u2"))))
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(2L, result.affectedRows());
                    })
                    .verifyComplete();

        assertEquals("merge into Users (id, name) key (id) values (?, ?)", executor.writeRequest().sql());
        assertEquals(2, executor.capturedParameterRows().size());
    }

    /**
     * 验证 Publisher 批量入口默认走 ATOMIC，并把参数流交给新的批量执行契约。
     */
    @Test
    void defaultsPublisherBatchToAtomicWithoutCollectingAllRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        Flux<Map<String, Object>> rows = Flux.just(orderedMap("id", "u1", "name", "王"),
                                                   orderedMap("name", "李", "id", "u2"));

        StepVerifier.create(client.operations().batchInserts.insertBatch(form(), rows))
                    .assertNext(result -> {
                        assertEquals(BatchWriteOptions.Mode.ATOMIC, result.mode());
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(2, result.affectedRows());
                    })
                    .verifyComplete();

        assertEquals(BatchWriteOptions.Mode.ATOMIC, executor.writeRequest().options().mode());
        assertEquals("insert into Users (id, name) values (?, ?)", executor.writeRequest().sql());
        assertEquals(2, executor.capturedParameterRows().size());
        assertArrayEquals(new Object[]{"u1", "王"}, executor.capturedParameterRows().get(0));
        assertArrayEquals(new Object[]{"u2", "李"}, executor.capturedParameterRows().get(1));
    }

    /** 输入首行等待由上游负责，不能提前消费连接可用后才开始的批量 SQL 时限。 */
    @Test
    void delayedBatchInsertInputDoesNotConsumeSqlTimeout() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        BatchWriteOptions options = BatchWriteOptions.atomic(1).withTimeout(Duration.ofMillis(10));

        StepVerifier.withVirtualTime(() -> client.operations().batchInserts.insertBatch(
                            form(), Mono.delay(Duration.ofMillis(20))
                                        .map(ignored -> orderedMap("id", "u1", "name", "王")), options))
                    .thenAwait(Duration.ofMillis(20))
                    .assertNext(result -> assertEquals(BatchWriteResult.Status.COMMITTED, result.status()))
                    .verifyComplete();

        assertEquals(options, executor.writeRequest().options());
    }

    /** Form 规划不能缩短连接可用后才由执行器启动的完整事务 SQL 时限。 */
    @Test
    void passesOriginalTransactionTimeoutToCustomBatchExecutor() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.keepBatchOpen = true;
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        BatchWriteOptions options = BatchWriteOptions.atomic(1).withTimeout(Duration.ofMillis(100));

        StepVerifier.withVirtualTime(() -> client.operations().batchInserts.insertBatch(
                            form(),
                            Mono.delay(Duration.ofMillis(90))
                                .map(ignored -> orderedMap("id", "u1", "name", "王")),
                            options))
                    .thenAwait(Duration.ofMillis(91))
                    .then(() -> assertEquals(options, executor.writeRequest().options()))
                    .thenCancel()
                    .verify();
    }

    /** 乐观锁批量更新等待首条输入时也不能提前消费事务 SQL 时限。 */
    @Test
    void delayedBatchUpdateInputDoesNotConsumeSqlTimeout() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        BatchWriteOptions options = BatchWriteOptions.atomic(1).withTimeout(Duration.ofMillis(10));
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                orderedMap("name", "Alice"),
                ConditionGroup.and().where("id", "=", "u1").build(),
                OptimisticLockOptions.increment("version", 1));

        StepVerifier.withVirtualTime(() -> client.operations().batchUpdates.updateBatch(
                            logicDeletedVersionedForm(),
                            Mono.delay(Duration.ofMillis(20)).map(ignored -> update), options))
                    .thenAwait(Duration.ofMillis(20))
                    .assertNext(result -> assertEquals(BatchWriteResult.Status.COMMITTED, result.status()))
                    .verifyComplete();

        assertEquals(options, executor.writeRequest().options());
    }

    /** INDEPENDENT 分片入口等待首行时同样不占用连接可用后的 SQL 时限。 */
    @Test
    void delayedBatchChunksInputDoesNotConsumeSqlTimeout() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        BatchWriteOptions options = BatchWriteOptions.independent(1).withTimeout(Duration.ofMillis(10));

        StepVerifier.withVirtualTime(() -> client.operations().batchInserts.insertBatchChunks(
                            form(), Mono.delay(Duration.ofMillis(20))
                                        .map(ignored -> orderedMap("id", "u1", "name", "王")), options))
                    .thenAwait(Duration.ofMillis(20))
                    .assertNext(chunk -> assertEquals(BatchChunkResult.Status.COMMITTED, chunk.status()))
                    .verifyComplete();

        assertEquals(options, executor.writeRequest().options());
    }

    /** timeout=0 明确交给外部边界，等待首行时不得暗中恢复 ORM 定时器。 */
    @Test
    void zeroBatchTimeoutLeavesTheFirstInputSignalToTheExternalBoundary() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());

        StepVerifier.withVirtualTime(() -> client.operations().batchInserts.insertBatch(
                            form(), Flux.never(), BatchWriteOptions.unlimitedAtomic(1)))
                    .thenAwait(Duration.ofSeconds(6))
                    .thenCancel()
                    .verify();

        assertNull(executor.writeRequest());
    }

    /**
     * 验证启动期设置的批量保护会被无 options 入口使用，并且套数据范围后不会丢失。
     */
    @Test
    void usesConfiguredDefaultBatchWriteOptions() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        BatchWriteOptions options = BatchWriteOptions.atomic(7)
                                                     .withMaxRows(99)
                                                     .withTimeout(Duration.ofSeconds(4));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                      .withDefaultBatchWriteOptions(options)
                                                      .withDefaultDataScope(DataScope.none());

        StepVerifier.create(client.operations().batchInserts.insertBatch(form(),
                                               Flux.just(orderedMap("id", "u1", "name", "王"))))
                    .expectNextCount(1)
                    .verifyComplete();

        assertBatchOptionsPreserved(options, executor.writeRequest().options());

        BatchWriteOptions explicit = BatchWriteOptions.independent(3, 1);
        StepVerifier.create(client.operations().batchInserts.insertBatch(form(),
                                               Flux.just(orderedMap("id", "u2", "name", "李")),
                                               explicit))
                    .expectNextCount(1)
                    .verifyComplete();

        assertBatchOptionsPreserved(explicit, executor.writeRequest().options());
    }

    /**
     * 验证响应式 upsert 默认走 ATOMIC，并且不需要先收集完整数据集。
     */
    @Test
    void defaultsPublisherUpsertBatchToAtomicWithoutCollectingAllRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2()));
        Flux<Map<String, Object>> rows = Flux.just(orderedMap("id", "u1", "name", "王"),
                                                   orderedMap("name", "李", "id", "u2"));

        StepVerifier.create(client.operations().batchInserts.upsertBatch(form(), rows))
                    .assertNext(result -> {
                        assertEquals(BatchWriteOptions.Mode.ATOMIC, result.mode());
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(2, result.affectedRows());
                    })
                    .verifyComplete();

        assertEquals(BatchWriteOptions.Mode.ATOMIC, executor.writeRequest().options().mode());
        assertEquals("merge into Users (id, name) key (id) values (?, ?)", executor.writeRequest().sql());
        assertEquals(2, executor.capturedParameterRows().size());
        assertArrayEquals(new Object[]{"u1", "王"}, executor.capturedParameterRows().get(0));
        assertArrayEquals(new Object[]{"u2", "李"}, executor.capturedParameterRows().get(1));
    }

    @Test
    void batchesOptimisticUpdatesWithImmutableScopeAndExactRowChecks() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.where(
                                                             ConditionGroup.and()
                                                                           .where("name", "=", "allowed")
                                                                           .build()));
        Flux<BatchOptimisticUpdate> updates = Flux.just(
                new BatchOptimisticUpdate(orderedMap("name", "Alice"),
                                          ConditionGroup.and().where("id", "=", "u1").build(),
                                          OptimisticLockOptions.increment("version", 1)),
                new BatchOptimisticUpdate(orderedMap("name", "Bob"),
                                          ConditionGroup.and().where("id", "=", "u2").build(),
                                          OptimisticLockOptions.increment("version", 2)));

        StepVerifier.create(client.operations().batchUpdates.updateBatch(logicDeletedVersionedForm(), updates))
                    .assertNext(result -> assertEquals(BatchWriteOptions.Mode.ATOMIC, result.mode()))
                    .verifyComplete();

        assertEquals(BatchRowCountPolicy.EXACTLY_ONE, executor.writeRequest().rowCountPolicy());
        assertEquals("update Users set name = ?, version = version + 1 "
                             + "where id = ? and name = ? and deleted = ? and version = ?",
                     executor.writeRequest().sql());
        assertArrayEquals(new Object[]{"Alice", "u1", "allowed", 0, 1},
                          executor.capturedParameterRows().get(0));
        assertArrayEquals(new Object[]{"Bob", "u2", "allowed", 0, 2},
                          executor.capturedParameterRows().get(1));
    }

    @Test
    void autoTenantScopeFillsPublisherBatchRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"));

        StepVerifier.create(client.operations().batchInserts.insertBatch(autoTenantForm(),
                                               Flux.just(orderedMap("id", "u1", "name", "王"),
                                                         orderedMap("id", "u2", "name", "李"))))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("insert into Users (id, name, tenant_id) values (?, ?, ?)", executor.writeRequest().sql());
        assertArrayEquals(new Object[]{"u1", "王", "t1"}, executor.capturedParameterRows().get(0));
        assertArrayEquals(new Object[]{"u2", "李", "t1"}, executor.capturedParameterRows().get(1));
    }

    @Test
    void batchSpecUsesRequestTenantScopeForInsertAndUpsert() {
        DataScope tenant = DataScope.tenant("tenant_id", "t1");

        RecordingSqlExecutor insertExecutor = new RecordingSqlExecutor();
        ReactiveFormClient insertClient = ReactiveFormClient.create(insertExecutor, renderer());
        StepVerifier.create(insertClient.writeBatch(
                            BatchSpec.insert(autoTenantForm(), Flux.just(orderedMap("id", "u1", "name", "王")))
                                     .withScope(tenant)))
                    .expectNextCount(1)
                    .verifyComplete();
        assertArrayEquals(new Object[]{"u1", "王", "t1"},
                          insertExecutor.capturedParameterRows().getFirst());

        RecordingSqlExecutor upsertExecutor = new RecordingSqlExecutor();
        ReactiveFormClient upsertClient = ReactiveFormClient.create(upsertExecutor, renderer());
        StepVerifier.create(upsertClient.writeBatch(
                            BatchSpec.upsert(autoTenantForm(), Flux.just(orderedMap("id", "u2", "name", "李")))
                                     .withScope(tenant)))
                    .expectNextCount(1)
                    .verifyComplete();
        assertArrayEquals(new Object[]{"u2", "李", "t1"},
                          upsertExecutor.capturedParameterRows().getFirst());
    }

    @Test
    void autoTenantWriteRejectsMissingTenantScope() {
        ReactiveFormClient client = ReactiveFormClient.create(new RecordingSqlExecutor(), renderer());

        ScopeAccessException error = assertThrows(
                ScopeAccessException.class,
                () -> client.operations().batchInserts.insert(autoTenantForm(), orderedMap("id", "u1", "name", "王")));

        assertEquals(ScopeErrorCode.TENANT_SCOPE_REQUIRED, error.code());
        assertEquals("userForm", error.formId());
        assertEquals("tenant_id", error.field());
    }

    @Test
    void manualTenantWriteRejectsDifferentTenantValue() {
        ReactiveFormClient client = ReactiveFormClient.create(new RecordingSqlExecutor(), renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"));

        ScopeAccessException error = assertThrows(
                ScopeAccessException.class,
                () -> client.operations().batchInserts.insert(manualTenantForm(),
                                    orderedMap("id", "u1", "name", "王", "tenant_id", "t2")));

        assertEquals(ScopeErrorCode.TENANT_VALUE_MISMATCH, error.code());
        assertEquals("tenant_id", error.field());
    }

    @Test
    void tenantUpdateRejectsMovingRowsToAnotherTenant() {
        ReactiveFormClient client = ReactiveFormClient.create(new RecordingSqlExecutor(), renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"));

        ScopeAccessException error = assertThrows(
                ScopeAccessException.class,
                () -> client.operations().writes.update(autoTenantForm(),
                                    orderedMap("name", "王", "tenant_id", "t2"),
                                    ConditionGroup.and().where("id", "=", "u1").build()));

        assertEquals(ScopeErrorCode.TENANT_VALUE_MISMATCH, error.code());
        assertEquals("tenant_id", error.field());
    }

    /**
     * 验证独立分片入口只接受显式 INDEPENDENT，并逐分片返回执行器结果。
     */
    @Test
    void delegatesIndependentPublisherBatchChunksToExecutor() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        BatchWriteOptions options = BatchWriteOptions.independent(1);

        StepVerifier.create(client.operations().batchInserts.insertBatchChunks(form(),
                                                     Flux.just(orderedMap("id", "u1", "name", "王"),
                                                               orderedMap("id", "u2", "name", "李")),
                                                     options))
                    .assertNext(result -> assertEquals(BatchChunkResult.Status.COMMITTED, result.status()))
                    .assertNext(result -> assertEquals(BatchChunkResult.Status.COMMITTED, result.status()))
                    .verifyComplete();

        assertEquals(BatchWriteOptions.Mode.INDEPENDENT, executor.writeRequest().options().mode());
    }

    /**
     * 验证动态表单查询可以直接接收请求参数，并通过参数编译器生成动态条件。
     */
    @Test
    void selectsDynamicFormRowsWithParameterDrivenConditions() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, parameterRenderer());
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.of("name",
                                                                                                      "name",
                                                                                                      "like"))
                                                                        .add(ParameterConditionSpec.of("orgId",
                                                                                                      "userId",
                                                                                                      "user-in-org"))
                                                                        .build();

        StepVerifier.create(client.operations().structuredQueries.select(form(), compiler, Map.of("name", "王%", "orgId", "org-1")))
                    .expectNext(DynamicRow.copyOf(Map.of("id", "u1", "name", "王")))
                    .verifyComplete();

        assertEquals("select id, name from Users where name like ? and exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?)",
                     executor.request().sql());
        assertEquals(List.of("王%", "org-1"), executor.request().parameters());
    }

    /**
     * 验证动态表单查询可以直接接收参数条件包，并与关系 SQL term 包组合完成响应式查询。
     */
    @Test
    void selectsDynamicFormRowsWithParameterConditionPackage() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, organizationRenderer());

        StepVerifier.create(client.operations().structuredQueries.select(form(),
                                          userOrganizationParameters(),
                                          Map.of("orgIds",
                                                 List.of("org-1", "org-2"),
                                                 "excludeOrgIds",
                                                 "org-3")))
                    .expectNext(DynamicRow.copyOf(Map.of("id", "u1", "name", "王")))
                    .verifyComplete();

        assertEquals("select id, name from Users where exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id in (?, ?)) "
                             + "and not exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?)",
                     executor.request().sql());
        assertEquals(List.of("org-1", "org-2", "org-3"), executor.request().parameters());
    }

    @Test
    void selectsDynamicFormRowsWithConfiguredStructuredConditionResolver() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, jsonRenderer())
                                                      .withStructuredConditionResolver(JsonStructuredConditions.standard());
        StructuredConditionInput input = StructuredConditionInput.term("profile",
                                                                       "json-path-eq",
                                                                       Map.of("key", "name", "value", "Alice"));

        StepVerifier.create(client.operations().structuredQueries.select(profileForm(), input))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("select `id`, `profile` from `Profiles` where json_extract(`profile`, ?) = cast(? as json)",
                     executor.request().sql());
        assertEquals(List.of("$.name", "\"Alice\""), executor.request().parameters());
    }

    @Test
    void rejectsExcessiveConditionDepthBeforeCallingCustomResolver() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, jsonRenderer())
                                                      .withStructuredConditionResolver(
                                                              (form, input, policy) -> ConditionGroup.and().build());
        StructuredConditionInput input = StructuredConditionInput.term("profile", "eq", "value");
        for (int depth = 0; depth < 10; depth++) {
            input = StructuredConditionInput.and(input);
        }
        StructuredConditionInput excessiveInput = input;

        StructuredConditionException error = assertThrows(StructuredConditionException.class,
                                                           () -> client.operations().structuredQueries.select(profileForm(), excessiveInput));

        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, error.code());
    }

    @Test
    void selectsDynamicFormRowsWithCompositeStructuredConditionResolver() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, jsonAndBusinessRenderer())
                                                      .withStructuredConditionResolver(StructuredConditionResolver.composite(
                                                              JsonStructuredConditions.standard(),
                                                              StructuredConditionCustomizer.allowOperator(
                                                                      "user-in-org")));
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("profile", "json-path-eq", Map.of("key", "name", "value", "Alice")),
                StructuredConditionInput.term("userId", "user-in-org", "org-1"));

        StepVerifier.create(client.operations().structuredQueries.select(profileWithUserForm(), input))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("select `id`, `profile`, `userId` from `Profiles` where "
                             + "json_extract(`profile`, ?) = cast(? as json) and exists "
                             + "(select 1 from org_user ou where ou.user_id = `userId` and ou.org_id = ?)",
                     executor.request().sql());
        assertEquals(List.of("$.name", "\"Alice\"", "org-1"), executor.request().parameters());
    }

    @Test
    void structuredRelationTermReusesCollectionShapeFromSqlRenderer() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, organizationRenderer())
                                                      .withStructuredConditionResolver(
                                                              StructuredConditionResolvers.allowOperators(
                                                                      "user-in-org"));

        StepVerifier.create(client.operations().structuredQueries.select(profileWithUserForm(),
                                          StructuredConditionInput.term("userId",
                                                                        "user-in-org",
                                                                        List.of("org-1", "org-2"))))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("select id, profile, userId from Profiles where exists (select 1 from org_user ou "
                             + "where ou.user_id = userId and ou.org_id in (?, ?))",
                     executor.request().sql());
        assertEquals(List.of("org-1", "org-2"), executor.request().parameters());
    }

    /**
     * 验证动态表单分页查询会先查询总数，再查询当前页数据，并返回响应式分页结果。
     */
    @Test
    void pagesDynamicFormRowsWithParameterConditionPackage() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, organizationRenderer());

        StepVerifier.create(client.operations().parameterPages.page(form(),
                                        userOrganizationParameters(),
                                        Map.of("orgIds", List.of("org-1", "org-2")),
                                        PageQuery.of(2, 2, PageSort.asc("id"))))
                    .assertNext(page -> {
                        assertEquals(2, page.rows().size());
                        assertEquals(3L, page.total());
                        assertEquals(2, page.page());
                        assertEquals(2, page.size());
                        assertEquals(2L, page.totalPages());
                    })
                    .verifyComplete();

        assertEquals("select count(*) as total from Users where exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id in (?, ?))",
                     executor.requests().get(0).sql());
        assertEquals("select id, name from Users where exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id in (?, ?)) order by id asc limit ? offset ?",
                     executor.requests().get(1).sql());
        assertEquals(List.of("org-1", "org-2", 2, 2L), executor.requests().get(1).parameters());
    }

    /** 数据库驱动返回超出 long 的 COUNT 时必须明确失败，不能截断后伪造一个分页总数。 */
    @Test
    void rejectsPageTotalOutsideLongRange() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.countTotal = new java.math.BigInteger("9223372036854775808");
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());

        StepVerifier.create(client.operations().plainPages.page(form(), ConditionGroup.and().build(), PageQuery.of(1, 10)))
                    .expectErrorMatches(error -> error instanceof IllegalArgumentException
                            && error.getMessage().contains("exact long integer"))
                    .verify();
    }

    /**
     * 验证动态表单响应式客户端可以直接接收 RDB 方言，并使用其中的分页方言。
     */
    @Test
    void pagesDynamicFormRowsWithRdbDialect() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        RdbDialect dialect = RdbDialect.of("offset-fetch-test",
                                           SchemaDialect.standard(),
                                           PaginationDialect.offsetFetch(),
                                           UpsertDialect.h2());
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), dialect));

        StepVerifier.create(client.operations().plainPages.page(form(),
                                        ConditionGroup.and()
                                                      .where("name", "=", "王")
                                                      .build(),
                                        PageQuery.of(2, 2, PageSort.asc("id"))))
                    .assertNext(page -> {
                        assertEquals(2, page.rows().size());
                        assertEquals(3L, page.total());
                    })
                    .verifyComplete();

        assertEquals("select id, name from Users where name = ? order by id asc offset ? rows fetch next ? rows only",
                     executor.requests().get(1).sql());
        assertEquals(List.of("王", 2L, 2), executor.requests().get(1).parameters());
    }

    /**
     * 表单查询、更新和删除可以显式带执行保护，不用绕到底层 SQL executor。
     */
    @Test
    void passesExecutionOptionsToSelectUpdateAndDelete() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(100);

        StepVerifier.create(client.operations().queries.select(form(), ConditionGroup.and().where("id", "=", "u1").build(), options))
                    .expectNextCount(1)
                    .verifyComplete();
        assertSame(options, executor.options().get(0));

        StepVerifier.create(client.operations().writes.update(form(),
                                          orderedMap("name", "新名字"),
                                          ConditionGroup.and().where("id", "=", "u1").build(),
                                          options))
                    .expectNext(1L)
                    .verifyComplete();
        assertSame(options, executor.options().get(1));

        StepVerifier.create(client.operations().deletes.delete(form(), ConditionGroup.and().where("id", "=", "u1").build(), options))
                    .expectNext(1L)
                    .verifyComplete();
        assertSame(options, executor.options().get(2));
    }

    @Test
    void appliesLargeObjectLimitWhileDecodingFormRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows(Map.of("id", "a1",
                                  "payload", Blob.from(Flux.just(ByteBuffer.wrap(new byte[]{1, 2, 3})))));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = DynamicForm.builder("attachment", "Attachments")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .build();
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(1).withMaxLargeObjectBytes(2);

        StepVerifier.create(client.operations().queries.select(form, ConditionGroup.and().build(), options))
                    .expectError(SqlLargeObjectLimitExceededException.class)
                    .verify();
        assertSame(options, executor.options().getFirst());
    }

    @Test
    void appliesDefaultLargeObjectLimitWhileDecodingFormRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows(Map.of("id", "a1",
                                  "payload", Blob.from(Flux.just(ByteBuffer.wrap(new byte[]{1, 2, 3})))));
        SqlExecutionOptions defaults = SqlExecutionOptions.maxRows(1).withMaxLargeObjectBytes(2);
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                      .withDefaultExecutionOptions(defaults);
        DynamicForm form = DynamicForm.builder("attachment", "Attachments")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .build();

        StepVerifier.create(client.operations().queries.select(form, ConditionGroup.and().build()))
                    .expectError(SqlLargeObjectLimitExceededException.class)
                    .verify();
        assertSame(defaults, executor.options().getFirst());
    }

    @Test
    void limitsTotalMemoryAfterLargeObjectsAreDecoded() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows(
                Map.of("id", "a1", "payload", blobWithBytes(40)),
                Map.of("id", "a2", "payload", blobWithBytes(40)));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = DynamicForm.builder("attachment", "Attachments")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .build();
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(10)
                                                           .withMaxResultBytes(200)
                                                           .withMaxLargeObjectBytes(100);

        StepVerifier.create(client.operations().queries.select(form, ConditionGroup.and().build(), options))
                    .expectNextMatches(row -> ((byte[]) row.get("payload")).length == 40)
                    .expectError(SqlResultMemoryLimitExceededException.class)
                    .verify();
    }

    private static Blob blobWithBytes(int length) {
        return Blob.from(Flux.just(ByteBuffer.wrap(new byte[length])));
    }

    /** 带乐观锁的便捷重载也必须继承客户端默认保护，不能偷偷改成 unlimited。 */
    @Test
    void optimisticWriteOverloadsKeepDefaultExecutionOptions() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        SqlExecutionOptions defaults = SqlExecutionOptions.maxRows(1).withTimeout(Duration.ofSeconds(2));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                      .withDefaultExecutionOptions(defaults);
        ConditionGroup where = ConditionGroup.and().where("id", "=", "u1").build();
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 3);

        StepVerifier.create(client.operations().writes.update(versionedForm(), orderedMap("name", "新名字"), where, lock))
                    .expectNext(1L)
                    .verifyComplete();
        StepVerifier.create(client.operations().deletes.delete(versionedForm(), where, lock))
                    .expectNext(1L)
                    .verifyComplete();
        StepVerifier.create(client.operations().deletes.physicalDelete(versionedForm(), where, lock))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals(List.of(defaults, defaults, defaults), executor.options());
    }

    @Test
    void dynamicFormLogicDeleteProtectsReadsAndWrites() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = logicDeletedForm();
        ConditionGroup where = ConditionGroup.and().where("id", "=", "u1").build();

        StepVerifier.create(client.operations().queries.select(form, where))
                    .expectNextCount(1)
                    .verifyComplete();
        assertEquals("select id, name, deleted from Users where id = ? and deleted = ?", executor.request().sql());
        assertEquals(List.of("u1", 0), executor.request().parameters());

        StepVerifier.create(client.operations().writes.update(form, orderedMap("name", "新名字"), where))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update Users set name = ? where id = ? and deleted = ?", executor.request().sql());
        assertEquals(List.of("新名字", "u1", 0), executor.request().parameters());

        StepVerifier.create(client.operations().deletes.delete(form, where))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update Users set deleted = ? where id = ? and deleted = ?", executor.request().sql());
        assertEquals(List.of(1, "u1", 0), executor.request().parameters());
    }

    @Test
    void dataScopeNarrowsDynamicFormOperations() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = scopedLogicDeletedForm();
        ConditionGroup where = ConditionGroup.and().where("id", "=", "u1").build();
        DataScope scope = DataScope.tenant("tenant_id", "t1");

        StepVerifier.create(client.operations().queries.select(form, where, scope))
                    .expectNextCount(1)
                    .verifyComplete();
        assertEquals("select id, name, tenant_id, deleted from Users where id = ? and tenant_id = ? and deleted = ?",
                     executor.request().sql());
        assertEquals(List.of("u1", "t1", 0), executor.request().parameters());

        StepVerifier.create(client.operations().deletes.delete(form, where, scope))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update Users set deleted = ? where id = ? and tenant_id = ? and deleted = ?",
                     executor.request().sql());
        assertEquals(List.of(1, "u1", "t1", 0), executor.request().parameters());
    }

    @Test
    void tenantFormRejectsReadAndPhysicalDeleteWithoutTenantScope() {
        ReactiveFormClient client = ReactiveFormClient.create(new RecordingSqlExecutor(), renderer());
        ConditionGroup where = ConditionGroup.and().where("id", "=", "u1").build();

        assertThrows(IllegalArgumentException.class, () -> client.operations().queries.select(autoTenantForm(), where));
        assertThrows(IllegalArgumentException.class, () -> client.operations().deletes.physicalDelete(autoTenantForm(), where));
    }

    @Test
    void rejectsFrontendStructuredConditionsOnServerProtectedFields() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1")
                                                                                   .withFields(FieldScope.readable("id", "name")));
        DynamicForm form = autoTenantLogicDeletedForm();

        assertStructuredFieldRejected(client, form, "tenant_id");
        assertStructuredFieldRejected(client, form, "deleted");
        assertStructuredFieldRejected(client, form, "internal_note");
        assertEquals(List.of(), executor.requests());
    }

    /** 两个字段白名单没有交集时，FormClient 也必须拒绝全部前端字段，不能在策略转换时重新放开。 */
    @Test
    void disjointFieldScopesDenyEveryFrontendConditionField() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        DataScope scope = DataScope.none().withFields(FieldScope.readable("id"))
                                   .and(DataScope.none().withFields(FieldScope.readable("name")));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer()).withDefaultDataScope(scope);

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> client.operations().structuredQueries.select(scopedForm(), StructuredConditionInput.term("id", "eq", "u1")));

        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, error.code());
        assertEquals(List.of(), executor.requests());
    }

    @Test
    void defaultDataScopeNarrowsDynamicFormOperations() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"));
        DynamicForm form = scopedForm();
        ConditionGroup where = ConditionGroup.and().where("id", "=", "u1").build();

        StepVerifier.create(client.operations().queries.select(form, where))
                    .expectNextCount(1)
                    .verifyComplete();
        assertEquals("select id, name, tenant_id, org_id from Users where id = ? and tenant_id = ?",
                     executor.request().sql());
        assertEquals(List.of("u1", "t1"), executor.request().parameters());

        StepVerifier.create(client.operations().deletes.physicalDelete(form, where))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("delete from Users where id = ? and tenant_id = ?", executor.request().sql());
        assertEquals(List.of("u1", "t1"), executor.request().parameters());
    }

    @Test
    void frameworkScopesCannotReplaceTheCallerWritePredicate() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"));
        ConditionGroup emptyBusinessWhere = ConditionGroup.and().build();

        assertThrows(IllegalArgumentException.class,
                     () -> client.operations().writes.update(autoTenantLogicDeletedForm(), orderedMap("name", "new-name"),
                                         emptyBusinessWhere));
        assertThrows(IllegalArgumentException.class,
                     () -> client.operations().deletes.delete(autoTenantLogicDeletedForm(), emptyBusinessWhere));
        assertThrows(IllegalArgumentException.class,
                     () -> client.operations().deletes.physicalDelete(autoTenantLogicDeletedForm(), emptyBusinessWhere));
        assertEquals(List.of(), executor.requests());
    }

    /** 重复挂默认范围只能继续 AND，后一次配置不能把已经存在的租户范围替换掉。 */
    @Test
    void repeatedDefaultDataScopeCanOnlyNarrowFormClientAccess() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"))
                                                     .withDefaultDataScope(DataScope.where(
                                                             ConditionGroup.and()
                                                                           .where("org_id", "=", "o1")
                                                                           .build()));

        StepVerifier.create(client.operations().queries.select(scopedForm(),
                                          ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("select id, name, tenant_id, org_id from Users "
                             + "where id = ? and tenant_id = ? and org_id = ?",
                     executor.request().sql());
        assertEquals(List.of("u1", "t1", "o1"), executor.request().parameters());
    }

    @Test
    void explicitDataScopeIsMergedWithDefaultDataScope() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.tenant("tenant_id", "t1"));
        DynamicForm form = scopedForm();
        ConditionGroup where = ConditionGroup.and().where("id", "=", "u1").build();
        DataScope organizationScope = DataScope.where(ConditionGroup.and().where("org_id", "=", "o1").build());

        StepVerifier.create(client.operations().queries.select(form, where, organizationScope))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("select id, name, tenant_id, org_id from Users where id = ? and tenant_id = ? and org_id = ?",
                     executor.request().sql());
        assertEquals(List.of("u1", "t1", "o1"), executor.request().parameters());
    }

    @Test
    void tenantTimeAndFieldScopesReachDynamicFormSqlTogether() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        LocalDateTime start = LocalDateTime.of(2026, 7, 1, 0, 0);
        LocalDateTime end = LocalDateTime.of(2026, 8, 1, 0, 0);
        DataScope scope = DataScope.tenant("tenant_id", "t1")
                                   .and(DataScope.time(TimeScope.between("created_at", start, end)))
                                   .withFields(FieldScope.readable("id", "name", "created_at"));
        ReactiveFormClient client = ReactiveFormClient.create(executor, parameterRenderer())
                                                     .withDefaultDataScope(scope);

        StepVerifier.create(client.operations().queries.select(timeScopedForm(),
                                          ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("select id, name, created_at from Users where id = ? and tenant_id = ? and created_at >= ? and created_at < ?",
                     executor.request().sql());
        assertEquals(List.of("u1", "t1", start, end), executor.request().parameters());
    }

    @Test
    void fieldScopeNarrowsReadableColumns() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.none()
                                                                                   .withFields(FieldScope.readable("id",
                                                                                                                   "name")));

        StepVerifier.create(client.operations().queries.select(scopedForm(), ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNextCount(1)
                    .verifyComplete();

        assertEquals("select id, name from Users where id = ?", executor.request().sql());
        assertEquals(List.of("u1"), executor.request().parameters());
    }

    /** QuerySpec 的排序和分组也属于读取边界，不可借 FieldScope 隐藏字段形成旁路。 */
    @Test
    void querySpecSortsAndGroupsCannotBypassReactiveFieldScope() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.none()
                                                                                   .withFields(
                                                                                           FieldScope.readable(
                                                                                                   "id", "name")));
        DynamicForm form = scopedForm();
        QuerySpec hiddenSort = QuerySpec.of(form, ConditionGroup.and().build())
                                        .withSorts(List.of(PageSort.asc("internal_note")));
        QuerySpec hiddenGroup = QuerySpec.of(form, ConditionGroup.and().build())
                                         .withProjection(List.of("id"), List.of("internal_note"));

        assertThrows(IllegalArgumentException.class, () -> client.select(hiddenSort));
        assertThrows(IllegalArgumentException.class, () -> client.select(hiddenGroup));
        assertThrows(IllegalArgumentException.class,
                     () -> client.page(
                             QuerySpec.of(form, ConditionGroup.and().build()),
                             PageQuery.of(1, 10, PageSort.asc("internal_note"))));
        assertThrows(IllegalArgumentException.class,
                     () -> client.cursorPage(
                             QuerySpec.of(form, ConditionGroup.and().build()),
                             CursorPageQuery.first(10, CursorSort.asc("internal_note"))));
        assertEquals(List.of(), executor.requests());
    }

    @Test
    void fieldScopeRejectsNonWritableValues() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer())
                                                     .withDefaultDataScope(DataScope.none()
                                                                                   .withFields(FieldScope.writable("name")));
        DynamicForm form = scopedForm();

        StepVerifier.create(client.operations().writes.update(form,
                                          orderedMap("name", "Alice"),
                                          ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update Users set name = ? where id = ?", executor.request().sql());

        ScopeAccessException insertError = assertThrows(
                ScopeAccessException.class,
                () -> client.operations().batchInserts.insert(form, orderedMap("id", "u2", "name", "Bob")));
        assertEquals(ScopeErrorCode.FIELD_NOT_WRITABLE, insertError.code());
        assertEquals("id", insertError.field());

        ScopeAccessException updateError = assertThrows(
                ScopeAccessException.class,
                () -> client.operations().writes.update(form,
                                    orderedMap("tenant_id", "t2"),
                                    ConditionGroup.and().where("id", "=", "u1").build()));
        assertEquals(ScopeErrorCode.FIELD_NOT_WRITABLE, updateError.code());
        assertEquals("tenant_id", updateError.field());
    }

    @Test
    void dynamicFormPhysicalDeleteIsExplicitEscapeHatch() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());

        StepVerifier.create(client.operations().deletes.physicalDelete(logicDeletedForm(),
                                                  ConditionGroup.and().where("id", "=", "u1").build()))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("delete from Users where id = ?", executor.request().sql());
        assertEquals(List.of("u1"), executor.request().parameters());
    }

    @Test
    void dynamicFormLogicalDeleteCanUseOptimisticLock() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 3);

        StepVerifier.create(client.operations().deletes.delete(logicDeletedVersionedForm(),
                                          ConditionGroup.and().where("id", "=", "u1").build(),
                                          lock))
                    .expectNext(1L)
                    .verifyComplete();

        assertEquals("update Users set deleted = ?, version = version + 1 where id = ? and deleted = ? and version = ?",
                     executor.request().sql());
        assertEquals(List.of(1, "u1", 0, 3), executor.request().parameters());
    }

    @Test
    void updatesAndDeletesWithOptimisticLock() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 3);

        StepVerifier.create(client.operations().writes.update(versionedForm(),
                                          orderedMap("name", "新名字"),
                                          ConditionGroup.and().where("id", "=", "u1").build(),
                                          lock))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("update Users set name = ?, version = version + 1 where id = ? and version = ?",
                     executor.request().sql());
        assertEquals(List.of("新名字", "u1", 3), executor.request().parameters());

        StepVerifier.create(client.operations().deletes.delete(versionedForm(),
                                          ConditionGroup.and().where("id", "=", "u1").build(),
                                          lock))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals("delete from Users where id = ? and version = ?", executor.request().sql());
        assertEquals(List.of("u1", 3), executor.request().parameters());
    }

    @Test
    void turnsZeroRowsIntoOptimisticLockConflict() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.rowsUpdated = 0L;
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 3);

        StepVerifier.create(client.operations().writes.update(versionedForm(),
                                          orderedMap("name", "新名字"),
                                          ConditionGroup.and().where("id", "=", "u1").build(),
                                          lock))
                    .expectErrorSatisfies(error -> {
                        OptimisticLockConflictException conflict = (OptimisticLockConflictException) error;
                        assertEquals("Users", conflict.table());
                        assertEquals("version", conflict.field());
                        assertEquals(3, conflict.expectedValue());
                    })
                    .verify();
    }

    /**
     * 分页查询的 count 和当前页查询都要用同一套执行保护。
     */
    @Test
    void passesExecutionOptionsToPageCountAndRows() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(20);

        StepVerifier.create(client.operations().plainPages.page(form(),
                                        ConditionGroup.and().where("name", "=", "王").build(),
                                        PageQuery.of(1, 2),
                                        options))
                    .assertNext(page -> assertEquals(2, page.rows().size()))
                    .verifyComplete();

        assertEquals(2, executor.options().size());
        assertSame(options, executor.options().get(0));
        assertSame(options, executor.options().get(1));
    }

    @Test
    void decodesArrayFieldsIntoStableLists() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows(Map.of("id", 1L, "tags", new String[]{"alpha", "beta"}));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = DynamicForm.builder("arrayForm", "ArrayRecords")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("tags", "VARCHAR[]"))
                                      .build();

        StepVerifier.create(client.operations().queries.select(form, ConditionGroup.and().build()))
                    .assertNext(row -> assertEquals(List.of("alpha", "beta"), row.get("tags")))
                    .verifyComplete();
    }

    @Test
    void decodesOffsetTimeFieldsIntoStableJavaValues() {
        RecordingSqlExecutor executor = new RecordingSqlExecutor();
        executor.queryRows(Map.of("id", 1L, "meeting_time", "13:40+08:00"));
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        DynamicForm form = DynamicForm.builder("schedule", "Schedules")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("meeting_time", "OFFSET_TIME"))
                                      .build();

        StepVerifier.create(client.operations().queries.select(form, ConditionGroup.and().build()))
                    .assertNext(row -> assertEquals(OffsetTime.parse("13:40+08:00"), row.get("meeting_time")))
                    .verifyComplete();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder()
                                                     .addTerm(SqlTermHandler.equalsTo())
                                                     .build(),
                                          RdbDialect.h2());
    }

    private static FormDataSqlRenderer parameterRenderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder()
                                                     .addDefaultTerms()
                                                     .addTerm(SqlTermHandler.of("user-in-org",
                                                                               (term, context) -> SqlFragment.of(
                                                                                       "exists (select 1 from org_user ou where ou.user_id = "
                                                                                               + context.identifier(term.field())
                                                                                               + " and ou.org_id = ?)",
                                                                                       term.value())))
                                                     .build(),
                                          RdbDialect.h2());
    }

    private static FormDataSqlRenderer organizationRenderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder()
                                                     .addDefaultTerms()
                                                     .addTermPackage(userOrganizationTerms())
                                                     .build(),
                                          RdbDialect.h2());
    }

    private static ParameterConditionPackage userOrganizationParameters() {
        return ParameterConditionPackage.of(
                "user-organization",
                ParameterConditionSpec.of("orgIds", "userId", "user-in-org"),
                ParameterConditionSpec.of("excludeOrgIds", "userId", "user-not-in-org"));
    }

    private static com.flying.orm.core.sql.render.SqlTermPackage userOrganizationTerms() {
        return RelationTermPackage.of("user-organization",
                                      "org_user",
                                      "ou",
                                      "user_id",
                                      "org_id",
                                      "user-in-org",
                                      "user-not-in-org");
    }

    private static FormDataSqlRenderer jsonRenderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder()
                                                     .addTermPackage(JsonTermHandlers.mysql())
                                                     .build(),
                                          RdbDialect.mysql());
    }

    private static FormDataSqlRenderer jsonAndBusinessRenderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder()
                                                     .addTermPackage(JsonTermHandlers.mysql())
                                                     .addTerm(SqlTermHandler.of("user-in-org",
                                                                               (term, context) -> SqlFragment.of(
                                                                                       "exists (select 1 from org_user ou where ou.user_id = "
                                                                                               + context.identifier(term.field())
                                                                                               + " and ou.org_id = ?)",
                                                                                       term.value())))
                                                     .build(),
                                          RdbDialect.mysql());
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder()
                          .addTerm(SqlTermHandler.equalsTo())
                          .build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .build();
    }

    private static DynamicForm versionedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("version", "INTEGER"))
                          .build();
    }

    private static DynamicForm logicDeletedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("deleted", "INTEGER"))
                          .logicDelete("deleted", 0, 1)
                          .build();
    }

    private static DynamicForm scopedLogicDeletedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .addField(DynamicField.of("deleted", "INTEGER"))
                          .logicDelete("deleted", 0, 1)
                          .build();
    }

    private static DynamicForm scopedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .addField(DynamicField.of("org_id", "VARCHAR"))
                          .build();
    }

    private static DynamicForm timeScopedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .addField(DynamicField.of("created_at", "TIMESTAMP"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .build();
    }

    private static DynamicForm autoTenantForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .build();
    }

    private static DynamicForm autoTenantLogicDeletedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .addField(DynamicField.of("deleted", "INTEGER"))
                          .addField(DynamicField.of("internal_note", "VARCHAR"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .logicDelete("deleted", 0, 1)
                          .build();
    }

    private static void assertStructuredFieldRejected(ReactiveFormClient client, DynamicForm form, String field) {
        StructuredConditionException error = assertThrows(StructuredConditionException.class,
                                                           () -> client.operations().structuredQueries.select(form,
                                                                               StructuredConditionInput.and(
                                                                                       StructuredConditionInput.term(field,
                                                                                                                     "eq",
                                                                                                                     "value"))));
        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, error.code());
        assertEquals("conditions[0].field", error.path());
    }

    private static DynamicForm manualTenantForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("tenant_id", "VARCHAR"))
                          .tenant("tenant_id", TenantStrategy.MANUAL)
                          .build();
    }

    private static DynamicForm logicDeletedVersionedForm() {
        return DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("deleted", "INTEGER"))
                          .addField(DynamicField.of("version", "INTEGER"))
                          .logicDelete("deleted", 0, 1)
                          .build();
    }

    private static DynamicForm profileForm() {
        return DynamicForm.builder("profileForm", "Profiles")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("profile", "JSON"))
                          .build();
    }

    private static DynamicForm profileWithUserForm() {
        return DynamicForm.builder("profileForm", "Profiles")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("profile", "JSON"))
                          .addField(DynamicField.of("userId", "BIGINT"))
                          .build();
    }

    /** Form 编排不能改写任何批量执行选项，SQL 时限由连接可用后的执行器负责启动。 */
    private static void assertBatchOptionsPreserved(BatchWriteOptions expected, BatchWriteOptions actual) {
        assertEquals(expected, actual);
    }

    private static Map<String, Object> orderedMap(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }

    private static final class RecordingSqlExecutor implements ReactiveSqlExecutor {

        private SqlRequest request;

        private BatchWriteRequest writeRequest;

        private final List<Object[]> capturedParameterRows = new ArrayList<>();

        private final List<SqlRequest> requests = new ArrayList<>();

        private final List<SqlExecutionOptions> options = new ArrayList<>();

        private long rowsUpdated = 1L;

        private Object countTotal = 3L;

        private List<Map<String, Object>> queryRows;

        private boolean keepBatchOpen;

        private String generatedKeyColumn;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            this.request = request;
            this.requests.add(request);
            if (request.sql().startsWith("select count(*)")) {
                return Flux.just(DynamicRow.copyOf(Map.of("total", countTotal)));
            }
            if (queryRows != null) {
                return Flux.fromIterable(queryRows).map(DynamicRow::copyOf);
            }
            if (request.sql().contains(" limit ? offset ?")) {
                return Flux.just(DynamicRow.copyOf(Map.of("id", "u1", "name", "王")),
                                 DynamicRow.copyOf(Map.of("id", "u2", "name", "王二")));
            }
            if (request.sql().contains(" offset ? rows fetch next ? rows only")) {
                return Flux.just(DynamicRow.copyOf(Map.of("id", "u1", "name", "王")),
                                 DynamicRow.copyOf(Map.of("id", "u2", "name", "王二")));
            }
            return Flux.just(DynamicRow.copyOf(Map.of("id", "u1", "name", "王")));
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            this.options.add(options);
            return query(request);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            this.request = request;
            this.requests.add(request);
            return Mono.just(rowsUpdated);
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            this.options.add(options);
            return rowsUpdated(request);
        }

        @Override
        public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return rowsUpdated(request, options).map(rows -> new SqlWriteResult(rows, List.of()));
        }

        @Override
        public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request,
                                                             SqlExecutionOptions options,
                                                             String generatedKeyColumn) {
            this.generatedKeyColumn = generatedKeyColumn;
            return rowsUpdatedReturningKeys(request, options);
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            this.writeRequest = request;
            if (keepBatchOpen) {
                return Mono.never();
            }
            return Flux.from(request.rows())
                       .doOnNext(capturedParameterRows::add)
                       .count()
                       .map(count -> BatchWriteResult.from(request.options().mode(),
                                                           List.of(BatchChunkResult.committed(0, 0, count.intValue(),
                                                                                              count))));
        }

        @Override
        public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            this.writeRequest = request;
            return Flux.from(request.rows())
                       .doOnNext(capturedParameterRows::add)
                       .index()
                       .map(indexed -> BatchChunkResult.committed(indexed.getT1().intValue(),
                                                                  indexed.getT1(),
                                                                  1,
                                                                  1));
        }

        @Override
        public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
            return Mono.just(BatchResolution.unknown(token));
        }

        private SqlRequest request() {
            return request;
        }

        private List<SqlRequest> requests() {
            return Collections.unmodifiableList(requests);
        }

        private List<SqlExecutionOptions> options() {
            return Collections.unmodifiableList(options);
        }

        private BatchWriteRequest writeRequest() {
            return writeRequest;
        }

        private List<Object[]> capturedParameterRows() {
            return Collections.unmodifiableList(capturedParameterRows);
        }

        @SafeVarargs
        private final void queryRows(Map<String, Object>... rows) {
            queryRows = List.of(rows);
        }
    }
}
