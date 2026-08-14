package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证实体批量生成键在 Form 与 Repository 之间的传播、偏移和早失败边界。 */
class RepositoryBatchGeneratedKeyContractTest {

    @Test
    void reactiveInsertBackfillsGeneratedKeysByInputOffset() {
        RecordingReactiveExecutor executor = new RecordingReactiveExecutor();
        ReactiveFormRepository<AutoDevice> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()), form(), AutoDevice.class);
        AutoDevice first = new AutoDevice("first");
        AutoDevice second = new AutoDevice("second");

        StepVerifier.create(repository.insertBatch(List.of(first, second)))
                    .assertNext(result -> assertEquals(2L, result.affectedRows()))
                    .verifyComplete();

        assertEquals(100L, first.getDeviceId());
        assertEquals(101L, second.getDeviceId());
        assertTrue(executor.request.generatedKeys().required());
        assertEquals("device_id", executor.request.generatedKeys().columnName());
    }

    @Test
    void syncInsertBackfillsGeneratedKeysByInputOffset() {
        RecordingSyncBatchExecutor executor = new RecordingSyncBatchExecutor();
        SyncFormRepository<AutoDevice> repository = SyncFormRepository.create(
                SyncFormClient.create(new UnusedSyncSqlExecutor(), executor, renderer()),
                form(), AutoDevice.class);
        AutoDevice first = new AutoDevice("first");
        AutoDevice second = new AutoDevice("second");

        BatchWriteResult result = repository.insertBatch(List.of(first, second));

        assertEquals(2L, result.affectedRows());
        assertEquals(200L, first.getDeviceId());
        assertEquals(201L, second.getDeviceId());
        assertTrue(executor.request.generatedKeys().required());
        assertEquals("device_id", executor.request.generatedKeys().columnName());
    }

    @Test
    void reactiveAtomicRollbackRestoresGeneratedKeys() {
        RecordingReactiveExecutor executor = new RecordingReactiveExecutor(false);
        ReactiveFormRepository<AutoDevice> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()), form(), AutoDevice.class);
        AutoDevice entity = new AutoDevice("rolled-back");

        StepVerifier.create(repository.insertBatch(List.of(entity)))
                    .assertNext(result -> assertEquals(BatchWriteResult.Status.ROLLED_BACK, result.status()))
                    .verifyComplete();

        assertEquals(null, entity.getDeviceId());
    }

    @Test
    void syncAtomicRollbackRestoresGeneratedKeys() {
        RecordingSyncBatchExecutor executor = new RecordingSyncBatchExecutor(false);
        SyncFormRepository<AutoDevice> repository = SyncFormRepository.create(
                SyncFormClient.create(new UnusedSyncSqlExecutor(), executor, renderer()),
                form(), AutoDevice.class);
        AutoDevice entity = new AutoDevice("rolled-back");

        BatchWriteResult result = repository.insertBatch(List.of(entity));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, result.status());
        assertEquals(null, entity.getDeviceId());
    }

    /** JDBC 执行器在已回填键后抛普通 Error 时，协调器仍必须撤销未确认实体键并原样传播该错误。 */
    @Test
    void syncBatchErrorAfterGeneratedKeyRestoresEntityKey() {
        AssertionError failure = new AssertionError("batch executor error");
        FailingAfterGeneratedKeySyncBatchExecutor executor = new FailingAfterGeneratedKeySyncBatchExecutor(failure);
        SyncFormRepository<AutoDevice> repository = SyncFormRepository.create(
                SyncFormClient.create(new UnusedSyncSqlExecutor(), executor, renderer()), form(), AutoDevice.class);
        AutoDevice entity = new AutoDevice("error-after-key");

        AssertionError observed = assertThrows(AssertionError.class,
                                                () -> repository.insertBatch(List.of(entity)));

        assertSame(failure, observed);
        assertEquals(null, entity.getDeviceId());
    }

    /** INDEPENDENT 分片入口发生同一失败时，也必须走完整的生成键撤销路径。 */
    @Test
    void syncBatchChunkErrorAfterGeneratedKeyRestoresEntityKey() {
        AssertionError failure = new AssertionError("batch chunk executor error");
        FailingAfterGeneratedKeySyncBatchExecutor executor = new FailingAfterGeneratedKeySyncBatchExecutor(failure);
        SyncFormRepository<AutoDevice> repository = SyncFormRepository.create(
                SyncFormClient.create(new UnusedSyncSqlExecutor(), executor, renderer()), form(), AutoDevice.class);
        AutoDevice entity = new AutoDevice("error-after-key");

        AssertionError observed = assertThrows(AssertionError.class,
                                                () -> repository.insertBatchChunks(
                                                        Flux.just(entity), BatchWriteOptions.independent(1)));

        assertSame(failure, observed);
        assertEquals(null, entity.getDeviceId());
    }

    /** 恢复回填键本身含 VME 时，不能让普通执行 Error 覆盖该致命失败或形成异常图环。 */
    @Test
    void syncBatchPromotesRestoreVirtualMachineErrorAfterExecutorError() {
        AssertionError primary = new AssertionError("batch executor error");
        OutOfMemoryError cleanup = new OutOfMemoryError("generated key restore error");
        FailingAfterGeneratedKeySyncBatchExecutor executor = new FailingAfterGeneratedKeySyncBatchExecutor(primary);
        SyncFormRepository<FailingRestoreAutoDevice> repository = SyncFormRepository.create(
                SyncFormClient.create(new UnusedSyncSqlExecutor(), executor, renderer()),
                form(),
                FailingRestoreAutoDevice.class);
        FailingRestoreAutoDevice entity = new FailingRestoreAutoDevice("error-after-key", cleanup);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> repository.insertBatch(List.of(entity)));

        assertSame(cleanup, observed);
        assertTrue(reaches(cleanup, primary));
        assertFalse(reaches(primary, cleanup));
    }

    @Test
    void syncExternalCompletionHonoursCancellationAndInvalidDemand() {
        RecordingSyncBatchExecutor executor = new RecordingSyncBatchExecutor(true, true);
        SyncFormRepository<AutoDevice> repository = SyncFormRepository.create(
                SyncFormClient.create(new UnusedSyncSqlExecutor(), executor, renderer()),
                form(), AutoDevice.class);
        AutoDevice entity = new AutoDevice("external");

        BatchWriteResult result = repository.insertBatch(List.of(entity));
        BatchWriteResult rolledBack = rolledBack(executor.request, 1);

        assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
        assertEquals(200L, entity.getDeviceId());
        executor.request.completion().afterCompletion(rolledBack).subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) { subscription.cancel(); }
            @Override public void onNext(Void ignored) { }
            @Override public void onError(Throwable error) { throw new AssertionError(error); }
            @Override public void onComplete() { throw new AssertionError("cancelled completion must stay silent"); }
        });
        assertEquals(200L, entity.getDeviceId());

        AtomicInteger errors = new AtomicInteger();
        executor.request.completion().afterCompletion(rolledBack).subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) {
                subscription.request(0);
                subscription.request(-1);
            }
            @Override public void onNext(Void ignored) { }
            @Override public void onError(Throwable error) { errors.incrementAndGet(); }
            @Override public void onComplete() { throw new AssertionError("invalid demand must not complete"); }
        });
        assertEquals(1, errors.get());
        assertEquals(200L, entity.getDeviceId());

        StepVerifier.create(Mono.from(executor.request.completion().afterCompletion(rolledBack))).verifyComplete();
        assertEquals(null, entity.getDeviceId());
    }

    @Test
    void reactiveAutoUpsertFailsBeforeInputSubscriptionAndExecutor() {
        RecordingReactiveExecutor executor = new RecordingReactiveExecutor();
        ReactiveFormRepository<AutoDevice> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()), form(), AutoDevice.class);
        AtomicBoolean subscribed = new AtomicBoolean();
        Publisher<AutoDevice> input = Flux.just(new AutoDevice("first"))
                                          .doOnSubscribe(ignored -> subscribed.set(true));

        StepVerifier.create(repository.upsertBatch(input, BatchWriteOptions.defaults()))
                    .expectError(MappingException.class)
                    .verify();

        assertFalse(subscribed.get());
        assertFalse(executor.invoked);
    }

    @Test
    void syncAutoUpsertFailsBeforeInputSubscriptionAndExecutor() {
        RecordingSyncBatchExecutor executor = new RecordingSyncBatchExecutor();
        SyncFormRepository<AutoDevice> repository = SyncFormRepository.create(
                SyncFormClient.create(new UnusedSyncSqlExecutor(), executor, renderer()),
                form(), AutoDevice.class);
        AtomicBoolean subscribed = new AtomicBoolean();
        Publisher<AutoDevice> input = Flux.just(new AutoDevice("first"))
                                          .doOnSubscribe(ignored -> subscribed.set(true));

        assertThrows(MappingException.class,
                     () -> repository.upsertBatch(input, BatchWriteOptions.defaults()));

        assertFalse(subscribed.get());
        assertFalse(executor.invoked);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("deviceForm", "device")
                          .addField(DynamicField.primaryKey("device_id", "BIGINT"))
                          .addField(DynamicField.of("device_name", "VARCHAR"))
                          .build();
    }

    private static BatchWriteResult committed(BatchWriteRequest request, int count) {
        return BatchWriteResult.from(request.options().mode(),
                                     List.of(BatchChunkResult.committed(0, 0L, count, count)));
    }

    private static BatchWriteResult rolledBack(BatchWriteRequest request, int count) {
        return BatchWriteResult.from(request.options().mode(),
                                     List.of(BatchChunkResult.rolledBack(0, 0L, count)));
    }

    @TableName("device")
    private static final class AutoDevice {

        @TableId(value = "device_id", type = IdType.AUTO)
        private Long deviceId;

        @TableField("device_name")
        private String name;

        private AutoDevice(String name) {
            this.name = name;
        }

        public Long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(Long deviceId) {
            this.deviceId = deviceId;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("device")
    private static final class FailingRestoreAutoDevice {

        @TableId(value = "device_id", type = IdType.AUTO)
        private Long deviceId;

        @TableField("device_name")
        private String name;

        @TableField(exist = false)
        private final OutOfMemoryError restoreFailure;

        private FailingRestoreAutoDevice(String name, OutOfMemoryError restoreFailure) {
            this.name = name;
            this.restoreFailure = restoreFailure;
        }

        public Long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(Long deviceId) {
            this.deviceId = deviceId;
            if (deviceId == null) {
                throw restoreFailure;
            }
        }

        public String getName() {
            return name;
        }
    }

    private static final class RecordingReactiveExecutor implements ReactiveSqlExecutor {

        private final boolean commit;
        private BatchWriteRequest request;
        private boolean invoked;

        private RecordingReactiveExecutor() {
            this(true);
        }

        private RecordingReactiveExecutor(boolean commit) {
            this.commit = commit;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.just(0L);
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            this.request = request;
            this.invoked = true;
            return Flux.from(request.rows())
                       .index()
                       .doOnNext(row -> request.generatedKeys().accept(
                               row.getT1(), DynamicRow.copyOf(Map.of("device_id", 100L + row.getT1()))))
                       .count()
                       .map(count -> commit
                               ? committed(request, count.intValue())
                               : rolledBack(request, count.intValue()));
        }
    }

    private static final class RecordingSyncBatchExecutor implements SyncBatchExecutor {

        private final boolean commit;
        private final boolean enlist;
        private BatchWriteRequest request;
        private boolean invoked;

        private RecordingSyncBatchExecutor() {
            this(true, false);
        }

        private RecordingSyncBatchExecutor(boolean commit) {
            this(commit, false);
        }

        private RecordingSyncBatchExecutor(boolean commit, boolean enlist) {
            this.commit = commit;
            this.enlist = enlist;
        }

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            this.request = request;
            this.invoked = true;
            List<Object[]> rows = Flux.from(request.rows()).collectList().block();
            for (int index = 0; index < rows.size(); index++) {
                request.generatedKeys().accept(
                        index, DynamicRow.copyOf(Map.of("device_id", 200L + index)));
            }
            if (enlist) {
                BatchChunkResult chunk = new BatchChunkResult(
                        0, 0L, rows.size(), 0L, BatchChunkResult.Status.ENLISTED,
                        null, null, List.of());
                return BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(chunk));
            }
            return commit ? committed(request, rows.size()) : rolledBack(request, rows.size());
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            return writeBatch(request).chunks();
        }
    }

    /** 真实消费 rows 并接受生成键后再失败，专门暴露同步协调器的未确认状态清理边界。 */
    private static final class FailingAfterGeneratedKeySyncBatchExecutor implements SyncBatchExecutor {

        private final AssertionError failure;

        private FailingAfterGeneratedKeySyncBatchExecutor(AssertionError failure) {
            this.failure = failure;
        }

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            List<Object[]> rows = Flux.from(request.rows()).collectList().block();
            request.generatedKeys().accept(0L, DynamicRow.copyOf(Map.of("device_id", 300L)));
            assertEquals(1, rows.size());
            throw failure;
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            List<Object[]> rows = Flux.from(request.rows()).collectList().block();
            request.generatedKeys().accept(0L, DynamicRow.copyOf(Map.of("device_id", 300L)));
            assertEquals(1, rows.size());
            throw failure;
        }
    }

    private static final class UnusedSyncSqlExecutor implements SyncSqlExecutor {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new AssertionError("batch contract test must not execute a query");
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("batch contract test must not execute a single-row write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                       com.flying.orm.rdb.execution.SqlExecutionOptions options) {
            throw new AssertionError("batch contract test must not execute a single-row generated-key write");
        }
    }

    private static boolean reaches(Throwable root, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }
}
