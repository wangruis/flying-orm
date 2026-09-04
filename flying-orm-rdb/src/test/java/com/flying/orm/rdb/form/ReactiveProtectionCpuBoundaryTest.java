package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactiveProtectionCpuBoundaryTest {

    private static final String DRIVER_THREAD = "simulated-r2dbc-driver";

    @Test
    void enabledProtectionMovesJcaWorkOffTheDriverThread() throws Exception {
        RecordingStringCodec codec = new RecordingStringCodec();
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(codec);
            protection.prepareWrite(
                    protectedForm(), Map.of("id", 0L, "secret", "entropy-warmup"),
                    DataScope.none(), codecs);
            codec.writeThreads.clear();
            ReactiveFormClient client = client(executor, protection, codec);

            executeOnDriver(() -> client.writeBatch(BatchSpec.insert(
                    protectedForm(), Flux.just(Map.of("id", 7L, "secret", "classified")))).block());

            assertFalse(codec.writeThreads.isEmpty(), "the protected value codec must run before AES encryption");
            assertTrue(codec.writeThreads.stream().noneMatch(name -> name.startsWith(DRIVER_THREAD)),
                       "protected encoding and the immediately following JCA work must leave the driver thread");
        }
    }

    @Test
    void disabledProtectionSchedulesNoWorkerTask() throws Exception {
        AtomicInteger scheduledTasks = new AtomicInteger();
        String hook = getClass().getName() + ".disabled";
        Schedulers.onScheduleHook(hook, task -> {
            scheduledTasks.incrementAndGet();
            return task;
        });
        try {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            ReactiveFormClient client = client(executor, ProtectedFieldRuntime.withoutKeys(), null);

            executeOnDriver(() -> client.writeBatch(BatchSpec.insert(
                    ordinaryForm(), Flux.just(Map.of("id", 7L, "note", "plain")))).block());

            assertEquals(0, scheduledTasks.get(),
                         "plain CRUD must not create a protection scheduler task");
            assertTrue(executor.consumptionThread.get().startsWith(DRIVER_THREAD));
        } finally {
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    @Test
    void enabledProtectionMovesResultDecryptionOffTheDriverThread() throws Exception {
        RecordingStringCodec codec = new RecordingStringCodec();
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        List<String> maskThreads = new CopyOnWriteArrayList<>();
        MaskingPolicyRegistry policies = MaskingPolicyRegistry.standard().with("recording", (value, definition) -> {
            maskThreads.add(Thread.currentThread().getName());
            return value;
        });
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]),
                ProtectedValueNormalizerRegistry.standard(), policies)) {
            ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(codec);
            Map<String, Object> stored = protection.prepareWrite(
                    protectedMaskedForm(), Map.of("id", 7L, "secret", "classified"),
                    DataScope.none(), codecs).values();
            executor.queryRows = Flux.just(DynamicRow.copyOf(stored));
            ReactiveFormClient client = client(executor, protection, codec);

            executeOnDriver(() -> client.select(QuerySpec.of(
                    protectedMaskedForm(), com.flying.orm.core.condition.ConditionGroup.and().build()))
                    .collectList().block());

            assertFalse(maskThreads.isEmpty(), "the decrypted value must reach the configured masking policy");
            assertTrue(maskThreads.stream().noneMatch(name -> name.startsWith(DRIVER_THREAD)),
                       "protected result decoding must leave the driver signal thread");
        }
    }

    @Test
    void ordinaryProjectionOnAProtectedFormSchedulesNoWorkerTask() throws Exception {
        AtomicInteger scheduledTasks = new AtomicInteger();
        String hook = getClass().getName() + ".ordinary-projection";
        Schedulers.onScheduleHook(hook, task -> {
            scheduledTasks.incrementAndGet();
            return task;
        });
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            executor.queryRows = Flux.just(DynamicRow.copyOf(Map.of("id", 7L)));
            ReactiveFormClient client = client(executor, protection, null);
            QuerySpec query = QuerySpec.of(
                    protectedForm(), com.flying.orm.core.condition.ConditionGroup.and().build())
                    .withProjection(List.of("id"), List.of());

            executeOnDriver(() -> client.select(query).collectList().block());

            assertEquals(0, scheduledTasks.get(),
                         "an ordinary projection must not create a protection scheduler task");
        } finally {
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    @Test
    void singleProtectedWritePlanningLeavesTheDriverThread() throws Exception {
        RecordingStringCodec codec = new RecordingStringCodec();
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            protection.prepareWrite(
                    protectedForm(), Map.of("id", 0L, "secret", "entropy-warmup"),
                    DataScope.none(), ValueCodecRegistry.standard().withFirst(codec));
            codec.writeThreads.clear();
            ReactiveFormClient client = client(executor, protection, codec);

            executeOnDriver(() -> client.insert(WriteSpec.insert(
                    protectedForm(), Map.of("id", 7L, "secret", "classified"))).block());

            assertFalse(codec.writeThreads.isEmpty(), "the protected single write must invoke its value codec");
            assertTrue(codec.writeThreads.stream().noneMatch(name -> name.startsWith(DRIVER_THREAD)),
                       "single-write encoding and JCA planning must leave the driver thread");
        }
    }

    @Test
    void protectedExactAndContainsPlanningLeaveTheDriverThread() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        ProtectedValueNormalizerRegistry normalizers = ProtectedValueNormalizerRegistry.standard()
                .with("recording", value -> {
                    normalizationThreads.add(Thread.currentThread().getName());
                    return value;
                });
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), normalizers,
                MaskingPolicyRegistry.standard())) {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            ReactiveFormClient client = client(executor, protection, null);
            DynamicForm form = protectedSearchForm();
            QuerySpec exact = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "classified")).build());
            QuerySpec contains = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "ass")).build());

            executeOnDriver(() -> {
                client.select(exact).collectList().block();
                client.select(contains).collectList().block();
            });

            assertFalse(normalizationThreads.isEmpty(), "protected search planning must normalize its value");
            assertTrue(normalizationThreads.stream().noneMatch(name -> name.startsWith(DRIVER_THREAD)),
                       "exact HMAC and contains-token planning must leave the driver thread");
        }
    }

    @Test
    void ordinarySingleCrudSchedulesNoWorkerTask() throws Exception {
        AtomicInteger scheduledTasks = new AtomicInteger();
        String hook = getClass().getName() + ".ordinary-single";
        Schedulers.onScheduleHook(hook, task -> {
            scheduledTasks.incrementAndGet();
            return task;
        });
        try {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            ReactiveFormClient client = client(executor, ProtectedFieldRuntime.withoutKeys(), null);
            DynamicForm form = ordinaryForm();

            executeOnDriver(() -> {
                client.insert(WriteSpec.insert(form, Map.of("id", 7L, "note", "plain"))).block();
                client.select(QuerySpec.of(form, ConditionGroup.and().build())).collectList().block();
            });

            assertEquals(0, scheduledTasks.get(),
                         "ordinary single CRUD must not create a protection scheduler task");
        } finally {
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    @Test
    void protectedJoinConditionPlanningLeavesTheDriverThread() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        ProtectedValueNormalizerRegistry normalizers = ProtectedValueNormalizerRegistry.standard()
                .with("recording", value -> {
                    normalizationThreads.add(Thread.currentThread().getName());
                    return value;
                });
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), normalizers,
                MaskingPolicyRegistry.standard())) {
            ReactiveFormClient client = client(new CapturingBatchExecutor(), protection, null);
            DynamicForm form = protectedSearchForm();
            JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
            JoinSource root = builder.root();
            JoinQuerySpec query = builder.where(root, ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "classified")).build())
                    .select(root, "id")
                    .build();

            executeOnDriver(() -> client.selectJoin(query).collectList().block());

            assertFalse(normalizationThreads.isEmpty(), "protected join planning must normalize its value");
            assertTrue(normalizationThreads.stream().noneMatch(name -> name.startsWith(DRIVER_THREAD)),
                       "protected join HMAC planning must leave the driver thread");
        }
    }

    @Test
    void protectedBatchUpdateScopeIsPlannedOnceOffTheDriverThread() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        ProtectedValueNormalizerRegistry normalizers = recordingNormalizers(normalizationThreads);
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), normalizers,
                MaskingPolicyRegistry.standard())) {
            ReactiveFormClient client = client(new CapturingBatchExecutor(), protection, null);
            DynamicForm form = protectedBatchUpdateForm();

            executeOnDriver(() -> client.writeBatch(protectedBatchUpdate(form, BatchWriteOptions.atomic(2))).block());

            assertAll(
                    () -> assertEquals(1, normalizationThreads.size(),
                                       "the fixed protected Scope must be planned once per batch subscription"),
                    () -> assertTrue(normalizationThreads.stream()
                                                         .noneMatch(name -> name.startsWith(DRIVER_THREAD)),
                                     "protected Scope HMAC planning must leave the driver thread"));
        }
    }

    @Test
    void protectedBatchUpdateChunkScopeIsPlannedOnceOffTheDriverThread() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        ProtectedValueNormalizerRegistry normalizers = recordingNormalizers(normalizationThreads);
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), normalizers,
                MaskingPolicyRegistry.standard())) {
            ReactiveFormClient client = client(new CapturingBatchExecutor(), protection, null);
            DynamicForm form = protectedBatchUpdateForm();

            executeOnDriver(() -> client.writeBatchChunks(
                    protectedBatchUpdate(form, BatchWriteOptions.independent(2))).collectList().block());

            assertAll(
                    () -> assertEquals(1, normalizationThreads.size(),
                                       "the fixed protected Scope must be planned once per chunked batch subscription"),
                    () -> assertTrue(normalizationThreads.stream()
                                                         .noneMatch(name -> name.startsWith(DRIVER_THREAD)),
                                     "chunked protected Scope HMAC planning must leave the driver thread"));
        }
    }

    @Test
    void ordinaryWideWriteSkipsKeyTraversalWhenNoFieldsAreEncrypted() {
        CountingMap values = countingValues(256);

        assertFalse(ReactiveProtectionCpuBoundary.writesEncryptedField(ordinaryWideForm(), values));
        assertEquals(0, values.keyVisits(),
                     "a form without encrypted metadata must not inspect ordinary write keys");
    }

    @Test
    void ordinaryWideConditionNeedsNoEncryptedMatch() {
        ConditionGroup.Builder builder = ConditionGroup.and();
        for (int index = 0; index < 256; index++) {
            builder.where("column_" + index, "=", index);
        }

        assertFalse(ReactiveProtectionCpuBoundary.usesEncryptedCondition(ordinaryWideForm(), builder.build()));
    }

    @Test
    void encryptedFieldDetectionRemainsReachable() {
        DynamicForm form = protectedForm();

        assertAll(
                () -> assertTrue(ReactiveProtectionCpuBoundary.writesEncryptedField(
                        form, Map.of("id", 7L, "secret", "classified"))),
                () -> assertTrue(ReactiveProtectionCpuBoundary.usesEncryptedCondition(
                        form, ConditionGroup.and().where("secret", "=", "classified").build())));
    }

    private static ProtectedValueNormalizerRegistry recordingNormalizers(List<String> threads) {
        return ProtectedValueNormalizerRegistry.standard().with("recording", value -> {
            threads.add(Thread.currentThread().getName());
            return value;
        });
    }

    private static BatchSpec protectedBatchUpdate(DynamicForm form, BatchWriteOptions options) {
        BatchOptimisticUpdate first = new BatchOptimisticUpdate(
                Map.of("note", "first"),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", 0L));
        BatchOptimisticUpdate second = new BatchOptimisticUpdate(
                Map.of("note", "second"),
                ConditionGroup.and().where("id", "=", 2L).build(),
                OptimisticLockOptions.increment("version", 0L));
        DataScope protectedScope = DataScope.where(ConditionGroup.and()
                .add(ProtectedConditions.exact("secret", "classified"))
                .build());
        return BatchSpec.update(form, Flux.just(first, second))
                        .withScope(protectedScope)
                        .withOptions(options);
    }

    private static ReactiveFormClient client(CapturingBatchExecutor executor,
                                             ProtectedFieldRuntime protection,
                                             RecordingStringCodec codec) {
        ValueCodecRegistry codecs = codec == null
                ? ValueCodecRegistry.standard() : ValueCodecRegistry.standard().withFirst(codec);
        SqlRenderer conditions = SqlRenderer.builder().valueCodecs(codecs).addDefaultTerms().build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql())
                                                          .withProtectedFields(protection);
        return ReactiveFormClient.create(executor, renderer);
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("protected-cpu-boundary", "protected_cpu_boundary")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .encrypted("secret", EncryptedFieldDefinition.builder().build())
                          .build();
    }

    private static DynamicForm ordinaryForm() {
        return DynamicForm.builder("plain-cpu-boundary", "plain_cpu_boundary")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("note", "VARCHAR"))
                          .build();
    }

    private static DynamicForm ordinaryWideForm() {
        DynamicForm.Builder builder = DynamicForm.builder("plain-cpu-boundary-wide", "plain_cpu_boundary_wide");
        for (int index = 0; index < 256; index++) {
            builder.addField(DynamicField.of("column_" + index, "INTEGER"));
        }
        return builder.build();
    }

    private static CountingMap countingValues(int width) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < width; index++) {
            values.put("column_" + index, index);
        }
        return new CountingMap(values);
    }

    private static DynamicForm protectedSearchForm() {
        return DynamicForm.builder("protected-cpu-boundary-search", "protected_cpu_boundary_search")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .encrypted("secret", EncryptedFieldDefinition.builder()
                                  .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.CONTAINS)
                                  .normalizer("recording")
                                  .build())
                          .build();
    }

    private static DynamicForm protectedMaskedForm() {
        return DynamicForm.builder("protected-cpu-boundary-read", "protected_cpu_boundary_read")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .encrypted("secret", EncryptedFieldDefinition.builder().build())
                          .masked("secret", MaskedFieldDefinition.builder("recording").build())
                          .build();
    }

    private static DynamicForm protectedBatchUpdateForm() {
        return DynamicForm.builder("protected-cpu-boundary-update", "protected_cpu_boundary_update")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("note", "VARCHAR"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .addField(DynamicField.of("version", "BIGINT"))
                          .encrypted("secret", EncryptedFieldDefinition.builder()
                                  .searchModes(EncryptedSearchMode.EXACT)
                                  .normalizer("recording")
                                  .build())
                          .build();
    }

    private static void executeOnDriver(Runnable action) throws Exception {
        ExecutorService driver = Executors.newSingleThreadExecutor(
                task -> new Thread(task, DRIVER_THREAD));
        try {
            Future<?> execution = driver.submit(action);
            execution.get(5, TimeUnit.SECONDS);
        } finally {
            driver.shutdownNow();
        }
    }

    private static final class RecordingStringCodec implements ValueCodec {

        private final List<String> writeThreads = new CopyOnWriteArrayList<>();

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public Object write(Object value) {
            writeThreads.add(Thread.currentThread().getName());
            return value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }

    private static final class CountingMap extends AbstractMap<String, Object> {

        private final Map<String, Object> values;

        private final AtomicInteger keyVisits = new AtomicInteger();

        private CountingMap(Map<String, Object> values) {
            this.values = Map.copyOf(values);
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return values.entrySet();
        }

        @Override
        public Set<String> keySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<String> iterator() {
                    Iterator<String> keys = values.keySet().iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return keys.hasNext();
                        }

                        @Override
                        public String next() {
                            keyVisits.incrementAndGet();
                            return keys.next();
                        }
                    };
                }

                @Override
                public int size() {
                    return values.size();
                }
            };
        }

        private int keyVisits() {
            return keyVisits.get();
        }
    }

    private static final class CapturingBatchExecutor implements ReactiveSqlExecutor {

        private final AtomicReference<String> consumptionThread = new AtomicReference<>();

        private Flux<DynamicRow> queryRows = Flux.empty();

        @Override
        public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
            return queryRows;
        }

        @Override
        public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
            return Mono.just(1L);
        }

        @Override
        public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
            return Mono.just(new SqlWriteResult(1L, List.of()));
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            return consume(request);
        }

        @Override
        public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
            return consume(request);
        }

        @Override
        public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            return consumeChunk(request);
        }

        @Override
        public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
            return consumeChunk(request);
        }

        private Mono<BatchWriteResult> consume(BatchWriteRequest request) {
            return Flux.from(request.rows())
                       .doOnNext(ignored -> consumptionThread.set(Thread.currentThread().getName()))
                       .then(Mono.fromSupplier(() -> BatchWriteResult.empty(request.options().mode())));
        }

        private Flux<BatchChunkResult> consumeChunk(BatchWriteRequest request) {
            return Flux.from(request.rows())
                       .doOnNext(ignored -> consumptionThread.set(Thread.currentThread().getName()))
                       .count()
                       .map(count -> BatchChunkResult.committed(0, 0L, count.intValue(), count))
                       .flux();
        }
    }
}
