package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
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
                case SYNC_CURSOR_TYPED -> clients.sync.cursorPage(
                        query, CursorPageQuery.first(10, CursorSort.asc("a")), Result.class);
                case REACTIVE_CURSOR_TYPED -> clients.reactive.cursorPage(
                        query, CursorPageQuery.first(10, CursorSort.asc("a")), Result.class).block();
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
            sync = SyncFormClient.create(executor(SyncSqlExecutor.class), executor(SyncBatchExecutor.class), renderer);
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
