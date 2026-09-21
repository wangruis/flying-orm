package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchUpsertScopeBoundaryTest {

    @TestFactory
    Stream<DynamicTest> allPublicChannelsBindTheEffectiveTargetScopeOnEveryRow() {
        return dialects().flatMap(dialect -> Stream.of(false, true).flatMap(reactive ->
                Stream.of("default", "explicit", "tenant", "merged").map(source -> DynamicTest.dynamicTest(
                        dialect.name() + " reactive=" + reactive + " scope=" + source, () -> {
                            String expectedSql = null;
                            for (String channel : List.of("result", "evidence")) {
                                CapturingWriter capture = new CapturingWriter();
                                AtomicInteger subscriptions = new AtomicInteger();
                                DataScope initial = source.equals("explicit") ? DataScope.none()
                                        : source.equals("tenant") ? DataScope.tenant("org_id", "scope-A") : rowScope();
                                DataScope explicit = source.equals("explicit") ? rowScope()
                                        : source.equals("merged") ? DataScope.where(ConditionGroup.and()
                                                .where("name", "=", "scope-old-name").build()) : DataScope.all();
                                BatchSpec spec = BatchSpec.upsert(form(), Flux.defer(() -> {
                                    subscriptions.incrementAndGet();
                                    return Flux.just(row(7L), row(8L));
                                })).withScope(explicit).withOptions(channel.equals("chunks")
                                        ? BatchWriteOptions.of(1) : BatchWriteOptions.defaults());

                                execute(capture, dialect, initial, spec, reactive, channel);

                                assertEquals(1, subscriptions.get());
                                assertEquals(1, capture.executions);
                                List<Object> first = source.equals("merged")
                                        ? List.of(7L, "scope-A", "changed", "scope-A", "scope-old-name")
                                        : List.of(7L, "scope-A", "changed", "scope-A");
                                List<Object> second = source.equals("merged")
                                        ? List.of(8L, "scope-A", "changed", "scope-A", "scope-old-name")
                                        : List.of(8L, "scope-A", "changed", "scope-A");
                                assertEquals(List.of(first, second), capture.rows);
                                assertEquals(first.size(), capture.parameterCount);
                                assertEquals(first.size(), capture.parameterTypes.size());
                                assertTrue(capture.parameterTypes.subList(3, first.size()).stream()
                                        .allMatch(type -> type == String.class));
                                String sql = capture.sql.toLowerCase(Locale.ROOT);
                                assertTrue(sql.contains("case") || sql.contains("if("), sql);
                                assertFalse(sql.contains("scope-a"), sql);
                                assertFalse(sql.contains("scope-old-name"), sql);
                                if (expectedSql == null) expectedSql = capture.sql;
                                else assertEquals(expectedSql, capture.sql, "result/evidence/chunks share the plan");
                            }
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> unscopedAndFieldOnlyUpsertsStillCompileAndDeliverTheirRows() {
        return dialects().flatMap(dialect -> Stream.of(false, true).flatMap(reactive ->
                Stream.of(false, true).map(fieldsOnly -> DynamicTest.dynamicTest(
                        dialect.name() + " reactive=" + reactive + " fields=" + fieldsOnly, () -> {
                            CapturingWriter capture = new CapturingWriter();
                            DataScope scope = fieldsOnly
                                    ? DataScope.none().withFields(FieldScope.writable("id", "org_id", "name"))
                                    : DataScope.none();
                            execute(capture, dialect, DataScope.none(),
                                    BatchSpec.upsert(form(), Flux.just(row(7L))).withScope(scope), reactive, "result");

                            assertEquals(1, capture.executions);
                            assertEquals(List.of(List.of(7L, "scope-A", "changed")), capture.rows);
                            assertEquals(3, capture.parameterCount);
                            String sql = capture.sql.toLowerCase(Locale.ROOT);
                            assertTrue(sql.contains("on conflict") || sql.contains("on duplicate key")
                                    || sql.startsWith("merge into"), sql);
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> keyOnlyTargetScopeNeverCreatesABusinessUpdate() {
        DynamicForm keys = DynamicForm.builder("pure_keys", "pure_keys")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        return dialects().flatMap(dialect -> Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                dialect.name() + " reactive=" + reactive, () -> {
                    CapturingWriter capture = new CapturingWriter();
                    DataScope scope = DataScope.where(ConditionGroup.and().where("id", ">", 0L).build());
                    execute(capture, dialect, scope, BatchSpec.upsert(keys, Flux.just(Map.of("id", 7L))),
                            reactive, "result");

                    String sql = capture.sql.toLowerCase(Locale.ROOT);
                    if (dialect.name().equals("postgresql")) {
                        assertTrue(sql.contains("do nothing"), sql);
                        assertFalse(sql.contains("do update"), sql);
                    } else if (!dialect.name().equals("mysql")) {
                        assertFalse(sql.contains("when matched"), sql);
                        assertFalse(sql.contains("update set"), sql);
                    } else {
                        assertFalse(sql.contains(" = values("), sql);
                    }
                    assertEquals(List.of(List.of(7L)), capture.rows, "no unused Scope binds for a no-op target");
                    assertEquals(1, capture.parameterCount);
                })));
    }

    private static void execute(CapturingWriter capture, RdbDialect dialect, DataScope defaults,
                                BatchSpec spec, boolean reactive, String channel) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        if (reactive) {
            ReactiveFormClient client = ReactiveFormClient.create(capture.reactive(), renderer)
                    .withDefaultDataScope(defaults);
            switch (channel) {
                case "result" -> client.writeBatch(spec).block();
                case "evidence" -> client.writeBatchEvidence(spec).block();
                default -> throw new AssertionError(channel);
            }
        } else {
            SyncFormClient client = SyncFormClient.create(unusedSqlExecutor(), capture.sync(), renderer)
                    .withDefaultDataScope(defaults);
            switch (channel) {
                case "result" -> client.writeBatch(spec);
                case "evidence" -> client.writeBatchEvidence(spec);
                default -> throw new AssertionError(channel);
            }
        }
    }

    private static Stream<RdbDialect> dialects() {
        return Stream.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.h2(),
                RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static DataScope rowScope() {
        return DataScope.where(ConditionGroup.and().where("org_id", "=", "scope-A").build());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("scoped_rows", "scoped_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("org_id", "VARCHAR"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("org_id", "scope-A");
        row.put("name", "changed");
        return row;
    }

    private static SyncSqlExecutor unusedSqlExecutor() {
        return (SyncSqlExecutor) Proxy.newProxyInstance(SyncSqlExecutor.class.getClassLoader(),
                new Class<?>[]{SyncSqlExecutor.class}, (proxy, method, args) -> {
                    throw new AssertionError("unexpected single SQL execution");
                });
    }

    private static final class CapturingWriter {
        private int executions;
        private String sql;
        private int parameterCount;
        private List<Class<?>> parameterTypes;
        private List<List<Object>> rows;

        private Mono<Void> accept(BatchWriteRequest request) {
            executions++;
            sql = request.sql();
            parameterCount = request.parameterCount();
            parameterTypes = request.parameterTypes();
            return Flux.from(request.rows()).map(Arrays::asList).collectList().doOnNext(values -> rows = values).then();
        }


        private BatchExecutionEvidence evidence(BatchWriteRequest request) {
            return com.flying.orm.rdb.repository.BatchEvidenceFixtures.successful(rows.size());
        }

        private SyncBatchExecutor sync() {
            return new SyncBatchExecutor() {
                @Override
                public BatchExecutionEvidence writeBatch(BatchWriteRequest request, java.util.function.LongConsumer rowCompleted) {
                    accept(request).block();
                    return evidence(request);
                }

                @Override
                public BatchExecutionEvidence writeBatchEvidence(BatchWriteRequest request) {
                    accept(request).block();
                    return evidence(request);
                }

            };
        }

        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.error(new AssertionError("unexpected query"));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("unexpected single write"));
                }

                @Override
                public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
                    return accept(request).then(Mono.fromSupplier(() ->
                            evidence(request)));
                }

                @Override
                public Mono<BatchExecutionEvidence> writeBatchEvidence(BatchWriteRequest request) {
                    return accept(request).then(Mono.fromSupplier(() -> evidence(request)));
                }

            };
        }
    }
}
