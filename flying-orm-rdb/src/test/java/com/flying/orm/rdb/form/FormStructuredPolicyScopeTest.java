package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import reactor.core.publisher.Flux;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormStructuredPolicyScopeTest {

    private static final DynamicForm FORM = DynamicForm.builder("scope_policy", "scope_policy")
            .addField(DynamicField.primaryKey("a", "VARCHAR"))
            .addField(DynamicField.of("b", "VARCHAR"))
            .build();

    @Test
    void readableScopeCannotBroadenTheStructuredAllowlist() {
        assertAll(Arrays.stream(ReadEntry.values()).map(entry -> () -> {
            Clients clients = new Clients();
            QuerySpec query = query("b", StructuredConditionPolicy.defaults().allowOnlyFields(List.of("a")),
                                    FieldScope.readable("a", "b"));

            assertRejected(() -> entry.read(clients, query));
            assertTrue(clients.requests.isEmpty());
        }));
    }

    @Test
    void disjointPolicyAndReadableScopeRemainDenied() {
        assertBothRejected(query("b", StructuredConditionPolicy.defaults().allowOnlyFields(List.of("a")),
                                 FieldScope.readable("b")));
    }

    @Test
    void explicitlyEmptyPolicyRemainsDeniedWithReadableFields() {
        assertBothRejected(query("a", StructuredConditionPolicy.defaults().allowOnlyFields(List.of()),
                                 FieldScope.readable("a", "b")));
    }

    @Test
    void explicitDenyStillWinsOverBothAllowlists() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                .allowOnlyFields(List.of("a", "b")).denyFields(List.of(" B "));
        assertBothRejected(query("b", policy, FieldScope.readable("a", "b")));
    }

    @Test
    void readableScopeStillRejectsFieldsAllowedByPolicy() {
        assertBothRejected(query("b", StructuredConditionPolicy.defaults().allowOnlyFields(List.of("a", "b")),
                                 FieldScope.readable("a")));
    }

    @Test
    void emptyMergedReadableScopeDoesNotBecomeUnrestricted() {
        DataScope scope = DataScope.none().withFields(FieldScope.readable("a"))
                .and(DataScope.none().withFields(FieldScope.readable("b")));
        assertBothRejected(query("a", StructuredConditionPolicy.defaults(), FieldScope.unrestricted())
                                   .withScope(scope));
    }

    @Test
    void allowedIntersectionKeepsCaseAndWhitespaceFieldIdentity() {
        Clients clients = new Clients();
        QuerySpec query = query(" A ", StructuredConditionPolicy.defaults().allowOnlyFields(List.of(" a ")),
                                FieldScope.readable(" A ", "b"));

        clients.sync.select(query);
        clients.reactive.select(query).collectList().block();

        assertEquals(2, clients.requests.size());
        for (SqlRequest request : clients.requests) {
            assertTrue(request.sql().contains("where \"a\" = ?"));
            assertEquals(List.of("value"), request.parameters());
        }
    }

    @Test
    void tenantAndLogicDeleteFieldsRemainServerOwned() {
        DynamicForm form = DynamicForm.builder("scoped", "scoped")
                .addField(DynamicField.of("a", "VARCHAR"))
                .addField(DynamicField.of("tenant_id", "VARCHAR"))
                .addField(DynamicField.of("deleted", "INTEGER"))
                .tenant("tenant_id", TenantStrategy.AUTO).logicDelete("deleted").build();
        for (String field : List.of("tenant_id", "deleted")) {
            assertBothRejected(QuerySpec.structured(form, StructuredConditionInput.term(field, "=", "1"))
                    .withStructuredPolicy(StructuredConditionPolicy.defaults()
                            .allowOnlyFields(List.of("a", "tenant_id", "deleted")))
                    .withScope(DataScope.tenant("tenant_id", "1")
                            .withFields(FieldScope.readable("a", "tenant_id", "deleted"))));
        }
    }

    private static QuerySpec query(String field, StructuredConditionPolicy policy, FieldScope fields) {
        return QuerySpec.structured(FORM, StructuredConditionInput.term(field, "=", "value"))
                .withStructuredPolicy(policy).withScope(DataScope.none().withFields(fields));
    }

    @Test
    void frontendProjectionSortAndPageRetainBusinessAndScopeConditions() {
        String inputValue = "alpha' or 1=1 --";
        QuerySpec query = QuerySpec.structured(FORM, StructuredConditionInput.or(
                        StructuredConditionInput.term("a", "=", inputValue),
                        StructuredConditionInput.term("b", "=", "beta")))
                .withWhere(ConditionGroup.and().where("a", "!=", "blocked").build())
                .withScope(DataScope.where(ConditionGroup.and().where("b", "!=", "hidden").build()))
                .withProjection(List.of("a", "b"), List.of())
                .withExcludedFields(List.of("b"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormOperationPlanner planner = new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());

        for (boolean explicitSort : List.of(false, true)) {
            QuerySpec selected = explicitSort ? query.withSorts(List.of(PageSort.asc("a"))) : query;
            FormReadPlanner.PlannedQuery plan = planner.reads.page(selected, PageQuery.of(2, 7, PageSort.desc("b")));
            SqlRequest count = plan.countRequest();
            SqlRequest data = plan.request();
            assertEquals(List.of("a"), plan.outputFields());
            assertEquals(explicitSort ? List.of(PageSort.asc("a")) : List.of(PageSort.desc("b")), plan.page().sorts());
            assertTrue(data.sql().startsWith("select \"a\" from"), data.sql());
            assertTrue(count.sql().contains(" and (\"a\" = ? or \"b\" = ?) and "), count.sql());
            assertEquals(count.sql().substring(count.sql().indexOf(" where ")),
                    data.sql().substring(data.sql().indexOf(" where "), data.sql().indexOf(" order by ")));
            assertTrue(data.sql().contains(explicitSort ? "order by \"a\" asc" : "order by \"b\" desc"), data.sql());
            assertTrue(!count.sql().contains("order by") && !count.sql().contains("limit"), count.sql());
            assertTrue(!data.sql().contains(inputValue), "frontend text must only be bound");
            assertEquals(count.parameters(), data.parameters().subList(0, count.parameters().size()));
            assertEquals(List.of("blocked", inputValue, "beta", "hidden"), count.parameters());
            assertEquals(List.of(7, 7L), data.parameters().subList(count.parameters().size(), data.parameters().size()));
        }
    }

    private static void assertBothRejected(QuerySpec query) {
        Clients clients = new Clients();
        assertRejected(() -> clients.sync.select(query));
        assertRejected(() -> clients.reactive.select(query).collectList().block());
        assertTrue(clients.requests.isEmpty());
    }

    private static void assertRejected(Executable operation) {
        StructuredConditionException error = assertThrows(StructuredConditionException.class, operation);
        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, error.code());
    }

    enum ReadEntry {
        SYNC_SELECT, REACTIVE_SELECT, SYNC_PAGE_TYPED, REACTIVE_PAGE_TYPED,
        SYNC_CURSOR_TYPED, REACTIVE_CURSOR_TYPED;

        void read(Clients clients, QuerySpec query) {
            switch (this) {
                case SYNC_SELECT -> clients.sync.select(query);
                case REACTIVE_SELECT -> clients.reactive.select(query).collectList().block();
                case SYNC_PAGE_TYPED -> clients.sync.page(query, PageQuery.of(1, 10), Result.class);
                case REACTIVE_PAGE_TYPED -> clients.reactive.page(query, PageQuery.of(1, 10), Result.class).block();
                case SYNC_CURSOR_TYPED -> clients.sync.keysetPage(
                        query, KeysetPageQuery.first(10, KeysetSort.asc("a", com.flying.orm.core.page.NullOrder.LAST)), Result.class);
                case REACTIVE_CURSOR_TYPED -> clients.reactive.keysetPage(
                        query, KeysetPageQuery.first(10, KeysetSort.asc("a", com.flying.orm.core.page.NullOrder.LAST)), Result.class).block();
            }
        }
    }

    public record Result(String a, String b) { }

    private static final class Clients {
        private final List<SqlRequest> requests = new ArrayList<>();
        private final SyncFormClient sync;
        private final ReactiveFormClient reactive;

        private Clients() {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
            sync = SyncFormClient.create(executor(com.flying.orm.rdb.support.ListQueryFixture.class), executor(SyncBatchExecutor.class), renderer);
            reactive = ReactiveFormClient.create(executor(ReactiveSqlExecutor.class), renderer);
        }

        private <T> T executor(Class<T> type) {
            return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                    (proxy, method, args) -> {
                        if (method.isDefault()) {
                            return InvocationHandler.invokeDefault(proxy, method, args);
                        }
                        if (method.getName().equals("query")) {
                            requests.add((SqlRequest) args[0]);
                            return type == ReactiveSqlExecutor.class ? Flux.empty() : List.of();
                        }
                        throw new AssertionError("Unexpected executor call: " + method);
                    }));
        }
    }
}
