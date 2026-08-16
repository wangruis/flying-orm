package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证同步表单批量只订阅一次输入，并把首行和剩余参数完整交给 JDBC 执行契约。 */
class NativeSyncFormBatchOperationsTest {

    @Test
    void plansFromFirstRowWithoutLosingOrResubscribingIt() {
        RecordingBatchExecutor executor = new RecordingBatchExecutor();
        NativeSyncFormBatchOperations operations = operations(executor);
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Map<String, Object>> rows = Flux.just(
                row(1L, "first"), row(2L, "second"))
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet());

        BatchWriteResult result = operations.writeBatch(
                BatchSpec.insert(form(), rows).withOptions(BatchWriteOptions.atomic(2)));

        assertEquals(1, subscriptions.get());
        assertEquals("insert into device (id, name) values (?, ?)", executor.request.sql());
        assertEquals(List.of(List.of(1L, "first"), List.of(2L, "second")), executor.parameters);
        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
    }

    @Test
    void delayedFirstRowDoesNotConsumeSqlTransactionTimeout() {
        RecordingBatchExecutor executor = new RecordingBatchExecutor();
        BatchWriteOptions options = BatchWriteOptions.atomic(2).withTimeout(Duration.ofMillis(10));

        operations(executor).writeBatch(BatchSpec.insert(
                                                  form(),
                                                  Flux.just(row(1L, "first"))
                                                      .delaySubscription(Duration.ofMillis(50)))
                                                  .withOptions(options));

        assertEquals(options, executor.request.options());
    }

    /** 同步批量入口必须与单条及响应式入口共享字段加密和盲索引派生。 */
    @Test
    void protectsEverySyncBatchInsertRow() {
        RecordingBatchExecutor executor = new RecordingBatchExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            NativeSyncFormBatchOperations operations = new NativeSyncFormBatchOperations(
                    executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                    DataScope.tenant("tenant_id", "tenant-a"), BatchWriteOptions.defaults());

            BatchWriteResult result = operations.writeBatch(BatchSpec.insert(
                    protectedForm(), Flux.just(Map.<String, Object>of("contact", "13800138000"),
                                               Map.<String, Object>of("contact", "13900139000"))));

            assertEquals(2L, result.affectedRows());
            assertTrue(executor.request.sql().contains("__fop_e_"));
            assertTrue(executor.request.sql().contains("__fop_s4_"));
            for (List<Object> row : executor.parameters) {
                assertInstanceOf(byte[].class, row.get(0));
                assertEquals(32, assertInstanceOf(byte[].class, row.get(1)).length);
                assertEquals(32, assertInstanceOf(byte[].class, row.get(2)).length);
                assertEquals("tenant-a", row.get(3));
            }
        }
    }

    /** 同步批量更新与响应式链共享 SET 加密和 WHERE 盲索引改写。 */
    @Test
    void protectsSyncBatchUpdateValuesAndConditions() {
        RecordingBatchExecutor executor = new RecordingBatchExecutor();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2())
                                                           .withProtectedFields(
                                                                   ProtectedFieldRuntime.create(keys));
            NativeSyncFormBatchOperations operations = new NativeSyncFormBatchOperations(
                    executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                    DataScope.tenant("tenant_id", "tenant-a"), BatchWriteOptions.defaults());
            BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                    Map.of("contact", "13900139000"),
                    ConditionGroup.and().add(ProtectedConditions.exact("contact", "13800138000")).build(),
                    OptimisticLockOptions.increment("version", 1));

            BatchWriteResult result = operations.writeBatch(
                    BatchSpec.update(protectedForm(), Flux.just(update)));

            assertEquals(1L, result.affectedRows());
            assertTrue(executor.request.sql().contains("__fop_e_"));
            assertTrue(executor.request.sql().contains("__fop_s4_"));
            for (Object value : executor.parameters.getFirst()) {
                if (value instanceof String text) {
                    assertEquals("tenant-a", text);
                }
            }
        }
    }

    private static NativeSyncFormBatchOperations operations(SyncBatchExecutor executor) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        return new NativeSyncFormBatchOperations(
                executor, renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(), BatchWriteOptions.defaults());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("device", "device")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .build();
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.of("tenant_id", "VARCHAR").withNullable(false))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .addField(DynamicField.of("version", "INTEGER"))
                          .tenant("tenant_id", TenantStrategy.AUTO)
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.EXACT,
                                                                                 EncryptedSearchMode.SUFFIX)
                                                                         .normalizer("digits")
                                                                         .suffixLengths(4)
                                                                         .build())
                          .build();
    }

    private static Map<String, Object> row(long id, String name) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }

    private static final class RecordingBatchExecutor implements SyncBatchExecutor {
        private BatchWriteRequest request;
        private List<List<Object>> parameters;

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            this.request = request;
            this.parameters = Flux.from(request.rows())
                                  .map(values -> Arrays.stream(values).toList())
                                  .collectList()
                                  .block();
            return BatchWriteResult.from(request.options().mode(), List.of(
                    BatchChunkResult.committed(0, 0L, parameters.size(), parameters.size())));
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            return writeBatch(request).chunks();
        }
    }
}
