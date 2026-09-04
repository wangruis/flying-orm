package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeysetFieldUseAndProtectionTest {

    @Test
    void callerSortAndInternalTieBreakerRequireDistinctGrantsBeforeExecution() {
        AtomicInteger executions = new AtomicInteger();
        SyncFormClient client = client(executions)
                .withFieldUsePolicy(FieldUsePolicy.builder()
                                                  .visibility("payload", FieldVisibility.FULL)
                                                  .allow("created_at", FieldUse.SORT)
                                                  .build());
        QuerySpec query = QuerySpec.of(form(), ConditionGroup.and().build())
                                   .withProjection(List.of("payload"), List.of());
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.desc("created_at", NullOrder.LAST));

        assertThrows(ScopeAccessException.class, () -> client.keysetPage(query, page));
        assertEquals(0, executions.get());

        SyncFormClient allowed = client.withFieldUsePolicy(
                FieldUsePolicy.builder()
                              .visibility("payload", FieldVisibility.FULL)
                              .visibility("created_at", FieldVisibility.FULL)
                              .visibility("id", FieldVisibility.FULL)
                              .allow("created_at", FieldUse.SORT)
                              .allowInternal("id", FieldUseOrigin.INTERNAL_TIE_BREAKER,
                                             FieldUse.SORT)
                              .build());
        assertDoesNotThrow(() -> allowed.keysetPage(query, page));
        assertEquals(1, executions.get());
    }

    @Test
    void hiddenTieBreakerCannotPublishItsFullCursorValueOnSyncOrReactivePath() {
        AtomicInteger syncExecutions = new AtomicInteger();
        AtomicInteger reactiveExecutions = new AtomicInteger();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                                              .visibility("payload", FieldVisibility.FULL)
                                              .visibility("created_at", FieldVisibility.FULL)
                                              .allow("created_at", FieldUse.SORT)
                                              .allowInternal(
                                                      "id", FieldUseOrigin.INTERNAL_TIE_BREAKER,
                                                      FieldUse.SORT)
                                              .build();
        QuerySpec query = QuerySpec.of(form(), ConditionGroup.and().build())
                                   .withProjection(List.of("payload"), List.of());
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.desc("created_at", NullOrder.LAST));

        assertThrows(ScopeAccessException.class,
                     () -> client(syncExecutions)
                             .withFieldUsePolicy(policy)
                             .keysetPage(query, page));
        assertThrows(ScopeAccessException.class,
                     () -> reactiveClient(reactiveExecutions)
                             .withFieldUsePolicy(policy)
                             .keysetPage(query, page)
                             .block());
        assertEquals(0, syncExecutions.get());
        assertEquals(0, reactiveExecutions.get());
    }

    @Test
    void maskedProjectedSortCannotPublishItsFullCursorValue() {
        AtomicInteger executions = new AtomicInteger();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                                              .visibility("created_at", FieldVisibility.MASKED)
                                              .visibility("id", FieldVisibility.FULL)
                                              .allow("created_at", FieldUse.SORT)
                                              .allowInternal(
                                                      "id", FieldUseOrigin.INTERNAL_TIE_BREAKER,
                                                      FieldUse.SORT)
                                              .build();
        QuerySpec query = QuerySpec.of(form(), ConditionGroup.and().build())
                                   .withProjection(List.of("created_at"), List.of());
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.desc("created_at", NullOrder.LAST));

        assertThrows(ScopeAccessException.class,
                     () -> client(executions).withFieldUsePolicy(policy).keysetPage(query, page));
        assertEquals(0, executions.get());
    }

    @Test
    void fullyVisibleFinalSortsKeepTheStablePosition() {
        AtomicInteger executions = new AtomicInteger();
        LocalDateTime first = LocalDateTime.parse("2026-09-03T00:00:00");
        LocalDateTime second = LocalDateTime.parse("2026-09-03T00:01:00");
        LocalDateTime third = LocalDateTime.parse("2026-09-03T00:02:00");
        SyncFormClient client = client(executions, List.of(
                physicalRow("one", first, 1L),
                physicalRow("two", second, 2L),
                physicalRow("three", third, 3L)))
                .withFieldUsePolicy(FieldUsePolicy.builder()
                                                  .visibility("payload", FieldVisibility.FULL)
                                                  .visibility("created_at", FieldVisibility.FULL)
                                                  .visibility("id", FieldVisibility.FULL)
                                                  .allow("created_at", FieldUse.SORT)
                                                  .allowInternal(
                                                          "id", FieldUseOrigin.INTERNAL_TIE_BREAKER,
                                                          FieldUse.SORT)
                                                  .build());
        QuerySpec query = QuerySpec.of(form(), ConditionGroup.and().build())
                                   .withProjection(List.of("payload"), List.of());

        KeysetPageResult<DynamicRow> result = client.keysetPage(
                query, KeysetPageQuery.first(
                        2, KeysetSort.asc("created_at", NullOrder.LAST)));

        assertEquals(List.of(second, 2L), result.nextPosition().values());
        assertEquals(List.of(Map.of("payload", "one"), Map.of("payload", "two")), result.rows());
        assertEquals(1, executions.get());
    }

    @Test
    void finalStableSortIsChargedToTheSameBudgetBeforeExecution() {
        AtomicInteger executions = new AtomicInteger();
        SyncFormClient client = client(executions)
                .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxSortCount(1));
        QuerySpec query = QuerySpec.of(form(), ConditionGroup.and().build())
                                   .withProjection(List.of("payload"), List.of());

        assertThrows(IllegalArgumentException.class,
                     () -> client.keysetPage(query, KeysetPageQuery.first(
                             20, KeysetSort.desc("created_at", NullOrder.LAST))));
        assertEquals(0, executions.get());
    }

    @Test
    void maskedSortCannotPublishItsFullCursorValue() {
        AtomicInteger executions = new AtomicInteger();
        DynamicForm masked = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("created_at", "VARCHAR").withNullable(false))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .masked("created_at", MaskedFieldDefinition.builder("full").build())
                .build();
        QuerySpec query = QuerySpec.of(masked, ConditionGroup.and().build()).masked();

        assertThrows(IllegalArgumentException.class,
                     () -> client(executions).keysetPage(
                             query,
                             KeysetPageQuery.first(
                                     20, KeysetSort.asc("created_at", NullOrder.FIRST))));
        assertEquals(0, executions.get());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("events", "events")
                .addField(DynamicField.of("payload", "VARCHAR"))
                .addField(DynamicField.of("created_at", "TIMESTAMP").withNullable(false))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
    }

    private static SyncFormClient client(AtomicInteger executions) {
        return client(executions, List.of());
    }

    private static SyncFormClient client(AtomicInteger executions, List<DynamicRow> rows) {
        SyncSqlExecutor executor = (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("query")) {
                        executions.incrementAndGet();
                        return rows;
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
        SyncBatchExecutor batches = (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> {
                    throw new UnsupportedOperationException(method.toString());
                });
        return SyncFormClient.create(executor, batches, FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()));
    }

    private static ReactiveFormClient reactiveClient(AtomicInteger executions) {
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.defer(() -> {
                    executions.incrementAndGet();
                    return Flux.empty();
                });
            }

            @Override
            public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
        return ReactiveFormClient.create(executor, FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()));
    }

    private static DynamicRow physicalRow(String payload, LocalDateTime createdAt, long id) {
        return DynamicRow.copyOf(Map.of(
                "payload", payload,
                "__fo_ks_0", createdAt,
                "__fo_ks_1", id));
    }
}
