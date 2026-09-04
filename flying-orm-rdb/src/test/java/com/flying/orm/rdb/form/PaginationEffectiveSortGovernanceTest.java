package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaginationEffectiveSortGovernanceTest {

    @Test
    void offsetPageApprovesTheSortThatWillActuallyExecute() {
        AtomicInteger executions = new AtomicInteger();
        SyncFormClient client = SyncFormClient.create(
                        syncExecutor(executions), syncBatchExecutor(), renderer())
                .withFieldUsePolicy(projectIdOnly());

        assertThrows(ScopeAccessException.class, () -> client.page(
                query(), PageQuery.of(1, 20, PageSort.asc("secret"))));
        assertEquals(0, executions.get());
    }

    @Test
    void cursorPageChargesEveryFinalSortToTheBudget() {
        AtomicInteger executions = new AtomicInteger();
        ReactiveFormClient client = ReactiveFormClient.create(
                        reactiveExecutor(executions), renderer())
                .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxSortCount(1));

        assertThrows(IllegalArgumentException.class, () -> client.cursorPage(
                cursorQuery(), CursorPageQuery.first(
                        20, CursorSort.asc("secret"), CursorSort.asc("id"))).block());
        assertEquals(0, executions.get());
    }

    @Test
    void cursorPageNeverPublishesAHiddenSortValue() {
        AtomicInteger executions = new AtomicInteger();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .visibility("id", FieldVisibility.FULL)
                .allow("id", FieldUse.PROJECT)
                .allow("secret", FieldUse.PROJECT)
                .allow("secret", FieldUse.SORT)
                .allowInternal("id", FieldUseOrigin.INTERNAL_TIE_BREAKER, FieldUse.SORT)
                .build();
        ReactiveFormClient client = ReactiveFormClient.create(
                        reactiveExecutor(executions), renderer())
                .withFieldUsePolicy(policy);

        assertThrows(ScopeAccessException.class, () -> client.cursorPage(
                cursorQuery(), CursorPageQuery.first(20, CursorSort.asc("secret"))).block());
        assertEquals(0, executions.get());
    }

    private static QuerySpec query() {
        return QuerySpec.of(form(), ConditionGroup.and().build())
                .withProjection(List.of("id"), List.of());
    }

    private static QuerySpec cursorQuery() {
        return QuerySpec.of(form(), ConditionGroup.and().build())
                .withProjection(List.of("id", "secret"), List.of());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR").withNullable(false))
                .build();
    }

    private static FieldUsePolicy projectIdOnly() {
        return FieldUsePolicy.builder()
                .visibility("id", FieldVisibility.FULL)
                .allow("id", FieldUse.PROJECT)
                .build();
    }

    private static SyncSqlExecutor syncExecutor(AtomicInteger executions) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("query")) {
                        executions.incrementAndGet();
                        return List.<DynamicRow>of();
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static SyncBatchExecutor syncBatchExecutor() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> {
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static ReactiveSqlExecutor reactiveExecutor(AtomicInteger executions) {
        return new ReactiveSqlExecutor() {
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
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
