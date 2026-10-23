package com.flying.orm.rdb.form;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
    void lockingKeysetSharesProtectedPlanningAndKeepsEachSubscriptionCold() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(normalizationThreads),
                MaskingPolicyRegistry.standard())) {
            ReactiveFormClient client = client(new CapturingBatchExecutor(), protection, null);
            QuerySpec query = QuerySpec.of(protectedSearchForm(), ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "classified")).build()).withLock(ReadLock.update());
            KeysetPageQuery page = KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST));
            for (ReactiveFormClient configured : List.of(client, client.withFieldUsePolicy(
                    com.flying.orm.core.scope.FieldUsePolicy.unrestricted()))) {
                normalizationThreads.clear();
                Mono<?> pending = configured.lockingRead(query, page);
                assertTrue(normalizationThreads.isEmpty(), "constructing a query must not encode protected values");
                executeOnDriver(pending::block);
                executeOnDriver(pending::block);
                assertEquals(2, normalizationThreads.size(), "each subscription must prepare its own predicate once");
                assertTrue(normalizationThreads.stream().noneMatch(thread -> thread.startsWith(DRIVER_THREAD)),
                        "locking keyset must use the same protected CPU boundary as ordinary keyset");
            }
        }
    }

    @Test
    void countPlansProtectedPredicatesOnceOffTheDriverThread() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(normalizationThreads),
                MaskingPolicyRegistry.standard())) {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            executor.queryRows = Flux.just(DynamicRow.copyOf(Map.of("total", 3_000_000_000L)));
            ReactiveFormClient client = client(executor, protection, null);
            QuerySpec spec = QuerySpec.of(protectedSearchForm(), ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "classified")).build());
            executeOnDriver(() -> assertEquals(3_000_000_000L, client.count(spec).block()));
            executeOnDriver(() -> assertEquals(3_000_000_000L, client.withFieldUsePolicy(
                    com.flying.orm.core.scope.FieldUsePolicy.builder()
                            .allow("secret", com.flying.orm.core.scope.FieldUse.FILTER).build()).count(spec).block()));
            assertEquals(2, normalizationThreads.size());
            assertTrue(normalizationThreads.stream().noneMatch(thread -> thread.startsWith(DRIVER_THREAD)));
        }
    }

    @Test
    void firstPlansProtectedPredicatesOffTheDriverThread() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(normalizationThreads),
                MaskingPolicyRegistry.standard())) {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            executor.queryRows = Flux.empty();
            ReactiveFormClient client = client(executor, protection, null);
            QuerySpec spec = QuerySpec.of(protectedSearchForm(), ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "classified")).build());
            executeOnDriver(() -> client.first(spec).block());
            executeOnDriver(() -> client.withFieldUsePolicy(
                    com.flying.orm.core.scope.FieldUsePolicy.unrestricted()).first(spec).block());
            assertEquals(2, normalizationThreads.size());
            assertTrue(normalizationThreads.stream().noneMatch(thread -> thread.startsWith(DRIVER_THREAD)));
        }
    }

    @Test
    void aggregatePagePlansProtectedPredicateOnceOffTheDriverThread() throws Exception {
        List<String> normalizationThreads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(normalizationThreads),
                MaskingPolicyRegistry.standard())) {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            executor.queryRows = Flux.just(DynamicRow.copyOf(Map.of("total", 0L)));
            ReactiveFormClient client = client(executor, protection, null);
            QuerySpec query = QuerySpec.of(protectedSearchForm(), ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "classified")).build());
            com.flying.orm.rdb.aggregate.AggregateSpec report = com.flying.orm.rdb.aggregate.AggregateSpec.builder(query)
                    .aggregate(com.flying.orm.rdb.aggregate.AggregateExpression.count("id", "total")).build();

            executeOnDriver(() -> client.aggregatePage(report,
                    com.flying.orm.core.page.PageQuery.of(1, 20)).block());

            assertEquals(1, normalizationThreads.size(), "count and page must share the protected query plan");
            assertFalse(normalizationThreads.getFirst().startsWith(DRIVER_THREAD));
        }
    }

    @Test
    void enabledProtectionMovesJcaWorkOffTheDriverThread() throws Exception {
        RecordingStringCodec codec = new RecordingStringCodec();
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(codec);
            DynamicForm form = protectedForm();
            protection.writeOperation(form, ProtectedFormLayout.physical(form), DataScope.none(), codecs)
                    .prepare(Map.of("id", 0L, "secret", "entropy-warmup"));
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
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void protectedBatchSnapshotsMutableRowsBeforeParallelBoundary() throws Exception {
        verifyMutableRowSnapshot(false, false);
        verifyMutableRowSnapshot(true, false);
        verifyMutableRowSnapshot(false, true);
        verifyMutableRowSnapshot(true, true);
    }

    @Test
    void protectedBatchSnapshotsEachEmittedRowOnce() {
        CountingObjectNode payload = new CountingObjectNode();
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            ReactiveFormClient client = client(executor, protection, null);

            client.writeBatch(BatchSpec.insert(protectedJsonForm(),
                    Flux.just(Map.of("id", 7L, "secret", "classified", "payload", payload)))
                    .withOptions(BatchWriteOptions.of(2))).block();

            assertEquals(1, payload.deepCopies(),
                         "the protected row must be snapshotted once before the CPU boundary");
        }
    }

    private static void verifyMutableRowSnapshot(boolean upsert, boolean chunks) throws Exception {
        ValueRecordingStringCodec codec = new ValueRecordingStringCodec();
        CapturingBatchExecutor executor = new CapturingBatchExecutor();
        BoundaryMutatingMap mutable = new BoundaryMutatingMap();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            DynamicForm form = protectedForm();
            protection.writeOperation(form, ProtectedFormLayout.physical(form), DataScope.none(), ValueCodecRegistry.standard())
                    .prepare(Map.of("id", 0L, "secret", "entropy-warmup"));
            ReactiveFormClient client = client(executor, protection, codec);
            AtomicInteger emitted = new AtomicInteger();
            Flux<Map<String, Object>> rows = Flux.generate(sink -> {
                switch (emitted.getAndIncrement()) {
                    case 0 -> sink.next(Map.of("id", 1L, "secret", "first"));
                    case 1 -> {
                        mutable.beginEmission();
                        try {
                            sink.next(mutable);
                        } finally {
                            mutable.finishEmission();
                        }
                    }
                    default -> sink.complete();
                }
            });
            BatchWriteOptions options = chunks
                    ? BatchWriteOptions.of(2) : BatchWriteOptions.of(2);
            BatchSpec spec = (upsert ? BatchSpec.upsert(protectedForm(), rows)
                    : BatchSpec.insert(protectedForm(), rows)).withOptions(options);

            executeOnDriver(() -> {
                if (chunks) {
                    client.writeBatch(spec).block();
                } else {
                    client.writeBatch(spec).block();
                }
            });

            assertEquals(List.of("first", "second-original"), codec.writeValues,
                         "each mutable row must be frozen before crossing the protection scheduler boundary");
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
            QuerySpec plainQuery = QuerySpec.of(ordinaryForm(), ConditionGroup.and().build());
            KeysetPageQuery page = KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST));
            executeOnDriver(() -> client.keysetPage(plainQuery, page).block());
            executeOnDriver(() -> client.keysetPage(plainQuery, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))).block());
            executeOnDriver(() -> client.lockingRead(plainQuery.withLock(ReadLock.update()), page).block());

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
            DynamicForm form = protectedMaskedForm();
            Map<String, Object> stored = protection.writeOperation(
                    form, ProtectedFormLayout.physical(form), DataScope.none(), codecs)
                    .prepare(Map.of("id", 7L, "secret", "classified")).values();
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
            DynamicForm form = protectedForm();
            protection.writeOperation(form, ProtectedFormLayout.physical(form),
                    DataScope.none(), ValueCodecRegistry.standard().withFirst(codec))
                    .prepare(Map.of("id", 0L, "secret", "entropy-warmup"));
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
                JoinQuerySpec.Builder query = JoinQuerySpec.builder(form);
                client.selectJoin(query.where(ConditionGroup.and().where("id", "=", 7L).build())
                        .select(query.root(), "id").build()).collectList().block();
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
    void qualifiedWhereAndOnProtectedValuesLeaveTheDriverThread() throws Exception {
        List<String> threads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(threads),
                MaskingPolicyRegistry.standard())) {
            ReactiveFormClient client = client(new CapturingBatchExecutor(), protection, null);
            for (boolean on : List.of(false, true)) {
                JoinQuerySpec.Builder query = JoinQuerySpec.builder(ordinaryForm());
                query.relation("protected", com.flying.orm.core.join.QueryRelation.of(
                        protectedSearchForm(), "id", "id"));
                ConditionGroup condition = ConditionGroup.and().add(ProtectedConditions.exact(
                        on ? "secret" : "protected.secret", "classified")).build();
                if (on) {
                    query.andOn(query.related("protected"),
                            com.flying.orm.core.join.JoinCondition.where(query.related("protected"), condition));
                } else {
                    query.where(condition);
                }
                JoinQuerySpec spec = query.select(query.root(), "id").build();
                executeOnDriver(() -> client.selectJoin(spec).collectList().block());
                assertEquals(1, threads.size(), "each condition must be normalized once");
                assertFalse(threads.getFirst().startsWith(DRIVER_THREAD),
                        on ? "ON protection must leave the driver thread" : "qualified WHERE protection must leave the driver thread");
                threads.clear();
            }
        }
    }

    @Test
    void ordinaryAndLockingQueryPlanRelatedProtectedConditionOffDriverThreadOnce() throws Exception {
        List<String> threads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(threads),
                MaskingPolicyRegistry.standard())) {
            ReactiveFormClient client = client(new CapturingBatchExecutor(), protection, null);
            DynamicForm form = DynamicForm.builder("ordinary", "ordinary")
                    .addField(DynamicField.primaryKey("id", "BIGINT"))
                    .relation("protected", com.flying.orm.core.join.QueryRelation.of(
                            protectedSearchForm(), "id", "id")).build();
            QuerySpec query = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.exact("protected.secret", "classified")).build());
            executeOnDriver(() -> client.select(query).collectList().block());
            assertEquals(1, threads.size());
            assertFalse(threads.getFirst().startsWith(DRIVER_THREAD));
            threads.clear();
            executeOnDriver(() -> client.lockingRead(query.withLock(com.flying.orm.rdb.lock.ReadLock.update()))
                    .collectList().block());
            assertEquals(1, threads.size());
            assertFalse(threads.getFirst().startsWith(DRIVER_THREAD));
            KeysetPageQuery page = KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST));
            DynamicForm cursorForm = DynamicForm.builder("ordinary", "ordinary")
                    .addField(DynamicField.primaryKey("rootId", "BIGINT"))
                    .relation("protected", com.flying.orm.core.join.QueryRelation.of(protectedSearchForm(), "rootId", "id")
                            .joinType(com.flying.orm.core.join.JoinType.INNER)).build();
            QuerySpec cursorQuery = QuerySpec.of(cursorForm, query.where())
                    .withProjection(List.of("rootId", "protected.id"), List.of());
            for (boolean governed : List.of(false, true)) {
                ReactiveFormClient configured = governed ? client.withQueryShapeLimits(
                        com.flying.orm.core.condition.QueryShapeLimits.defaults().withMaxSortCount(4)) : client;
                threads.clear();
                Mono<?> cursor = configured.keysetPage(cursorQuery, KeysetPageQuery.first(2, KeysetSort.asc("rootId", com.flying.orm.core.page.NullOrder.LAST)));
                assertTrue(threads.isEmpty(), "related cursor planning must be cold");
                executeOnDriver(cursor::block);
                executeOnDriver(cursor::block);
                assertEquals(2, threads.size());
                assertTrue(threads.stream().noneMatch(thread -> thread.startsWith(DRIVER_THREAD)));
                for (boolean locked : List.of(false, true)) {
                    threads.clear();
                    Mono<?> pending = locked ? configured.lockingRead(query.withLock(ReadLock.update()), page)
                            : configured.keysetPage(query, page);
                    assertTrue(threads.isEmpty(), "related keyset planning must be cold");
                    executeOnDriver(pending::block);
                    executeOnDriver(pending::block);
                    assertEquals(2, threads.size());
                    assertTrue(threads.stream().noneMatch(thread -> thread.startsWith(DRIVER_THREAD)));
                }
            }
        }
    }

    @Test
    void joinedAggregatePlansNonRootProtectedConditionOffDriverThreadOnce() throws Exception {
        List<String> threads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(threads),
                MaskingPolicyRegistry.standard())) {
            CapturingBatchExecutor executor = new CapturingBatchExecutor();
            executor.queryRows = Flux.just(DynamicRow.copyOf(Map.of("total", 0L)));
            ReactiveFormClient client = client(executor, protection, null);
            JoinQuerySpec.Builder input = JoinQuerySpec.builder(ordinaryForm());
            JoinSource secret = input.join(com.flying.orm.core.join.JoinType.LEFT, protectedSearchForm(), input.root(), "id", "id");
            input.where(secret, ConditionGroup.and().add(ProtectedConditions.exact("secret", "classified")).build());
            com.flying.orm.rdb.aggregate.AggregateSpec report = com.flying.orm.rdb.aggregate.AggregateSpec.builder(input.buildRelation())
                    .aggregate(com.flying.orm.rdb.aggregate.AggregateExpression.countRows("rows")).build();
            executeOnDriver(() -> client.aggregatePage(report, com.flying.orm.core.page.PageQuery.of(1, 20)).block());
            assertEquals(1, threads.size());
            assertFalse(threads.getFirst().startsWith(DRIVER_THREAD));
        }
    }

    @Test
    void laterUpdateLayoutRunsProtectedWorkOffTheDriverThread() throws Exception {
        List<String> threads = new CopyOnWriteArrayList<>();
        try (ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), recordingNormalizers(threads),
                MaskingPolicyRegistry.standard())) {
            ReactiveFormClient client = client(new CapturingBatchExecutor(), protection, null);
            BatchSpec batch = BatchSpec.update(protectedBatchUpdateForm(), Flux.just(
                    new BatchUpdate(Map.of("note", "ordinary"), ConditionGroup.and().where("id", "=", 1L).build()),
                    new BatchUpdate(Map.of("secret", "classified"), ConditionGroup.and().where("id", "=", 2L).build())))
                    .withOptions(BatchWriteOptions.of(2));
            executeOnDriver(() -> client.writeBatch(batch).block());
            assertFalse(threads.isEmpty());
            assertTrue(threads.stream().noneMatch(thread -> thread.startsWith(DRIVER_THREAD)));
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

            executeOnDriver(() -> client.writeBatch(protectedBatchUpdate(form, BatchWriteOptions.of(2))).block());

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

            executeOnDriver(() -> client.writeBatch(
                    protectedBatchUpdate(form, BatchWriteOptions.of(2))).block());

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
        BatchUpdate first = new BatchUpdate(
                Map.of("note", "first"),
                ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", 0L));
        BatchUpdate second = new BatchUpdate(
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
                                             ValueCodec codec) {
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

    private static DynamicForm protectedJsonForm() {
        return DynamicForm.builder("protected-list-snapshot", "protected_list_snapshot")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .addField(DynamicField.of("payload", "JSON"))
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

    private static final class ValueRecordingStringCodec implements ValueCodec {

        private final List<String> writeValues = new CopyOnWriteArrayList<>();

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public Object write(Object value) {
            writeValues.add((String) value);
            return value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }

    private static final class BoundaryMutatingMap extends AbstractMap<String, Object> {

        private final Map<String, Object> values = new LinkedHashMap<>(Map.of(
                "id", 2L, "secret", "second-original"));

        private final ThreadLocal<Boolean> emitting = new ThreadLocal<>();

        private final CountDownLatch mutationCompleted = new CountDownLatch(1);

        @Override
        public Set<Entry<String, Object>> entrySet() {
            if (Boolean.TRUE.equals(emitting.get())) {
                return new LinkedHashMap<>(values).entrySet();
            }
            try {
                assertTrue(mutationCompleted.await(5, TimeUnit.SECONDS),
                           "the source must mutate the row after onNext returns");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while awaiting the source mutation", interrupted);
            }
            return values.entrySet();
        }

        private void beginEmission() {
            emitting.set(Boolean.TRUE);
        }

        private void finishEmission() {
            emitting.remove();
            values.put("secret", "second-mutated");
            mutationCompleted.countDown();
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

    private static final class CountingObjectNode extends ObjectNode {

        private final AtomicInteger deepCopies = new AtomicInteger();

        private CountingObjectNode() {
            super(JsonNodeFactory.instance);
        }

        @Override
        public ObjectNode deepCopy() {
            deepCopies.incrementAndGet();
            return this;
        }

        private int deepCopies() {
            return deepCopies.get();
        }
    }

    private static final class CapturingBatchExecutor implements ReactiveSqlExecutor {

        @Override
        public boolean supportsBatchRowLayouts() { return true; }

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
        public Mono<SqlWriteResult> protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
            return Mono.just(new SqlWriteResult(1L, List.of()));
        }

        @Override
        public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
            return consume(request);
        }

        @Override
        public Mono<BatchExecutionEvidence> writeProtectedBatch(BatchWriteRequest request, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
            return consume(request);
        }



        private Mono<BatchExecutionEvidence> consume(BatchWriteRequest request) {
            return Flux.from(request.rows())
                       .doOnNext(ignored -> consumptionThread.set(Thread.currentThread().getName()))
                       .then(Mono.fromSupplier(() -> com.flying.orm.rdb.repository.BatchEvidenceFixtures.successful(0)));
        }

    }
}
