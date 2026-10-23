package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class DmlConditionGroupReuseTest {
    private static final DynamicForm FORM = DynamicForm.builder("orders", "orders")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("status", "VARCHAR"))
            .addField(DynamicField.of("tenant_id", "BIGINT")).build();

    @TestFactory
    Stream<DynamicTest> existingGroupIsAppendedWithoutWideningScope() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).flatMap(bound ->
                Stream.of("query", "update", "delete").map(kind -> DynamicTest.dynamicTest(
                        reactive + "/" + bound + "/" + kind, () -> {
                            Fixture fixture = new Fixture();
                            ConditionGroup.Builder builder = ConditionGroup.or().where("id", "=", 1L).where("id", "=", 2L);
                            ConditionGroup shared = builder.build();
                            builder.where("id", "=", 3L);
                            fixture.execute(reactive, bound, kind, shared);
                            SqlRequest request = fixture.request.get();
                            String sql = request.sql().replace("\"", "");
                            assertTrue(sql.contains("(id = ? or id = ?)"), request.sql());
                            assertTrue(sql.contains("tenant_id = ?"), request.sql());
                            assertEquals(kind.equals("update") ? List.of("closed", "ready", 1L, 2L, 7L)
                                    : List.of("ready", 1L, 2L, 7L), request.parameters());
                            assertEquals(2, shared.children().size());
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> mixedCaseReferencesUseOneFieldIdentityInBothWriteModes() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of("update", "delete")
                .map(kind -> DynamicTest.dynamicTest(reactive + "/" + kind, () -> {
                    Fixture fixture = new Fixture();
                    ConditionGroup shared = ConditionGroup.and().where("STATUS", "=", "ready").build();
                    assertDoesNotThrow(() -> fixture.execute(reactive, false, kind, shared));
                    SqlRequest request = fixture.request.get();
                    assertTrue(request.sql().contains("\"status\" = ?"), request.sql());
                    assertFalse(request.sql().contains("\"STATUS\""), request.sql());
                    assertEquals(kind.equals("update") ? List.of("closed", "ready", "ready", 7L)
                            : List.of("ready", "ready", 7L), request.parameters());
                })));
    }

    @TestFactory
    Stream<DynamicTest> duplicateAssignmentsStillFailBeforeExecution() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("duplicate SET/" + reactive, () -> {
            Fixture fixture = new Fixture();
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> {
                if (reactive) {
                    fixture.reactive.update("orders").set("status", "closed").set("STATUS", "ready")
                            .where("id", 1L).execute().block();
                } else {
                    fixture.sync.update("orders").set("status", "closed").set("STATUS", "ready")
                            .where("id", 1L).execute();
                }
            });
            assertTrue(failure.getMessage().contains("duplicate"), failure.getMessage());
            assertNull(fixture.request.get());
        }));
    }

    @TestFactory
    Stream<DynamicTest> unknownGroupFieldsAreRejectedBeforeExecution() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of("query", "update", "delete")
                .map(kind -> DynamicTest.dynamicTest(reactive + "/" + kind, () -> {
                    Fixture fixture = new Fixture();
                    ConditionGroup group = ConditionGroup.and().where("unknown", "=", 1).build();
                    assertThrows(IllegalArgumentException.class, () -> fixture.execute(reactive, true, kind, group));
                    assertNull(fixture.request.get());
                })));
    }

    private static final class Fixture {
        private final AtomicReference<SqlRequest> request = new AtomicReference<>();
        private final SyncDmlOperator sync;
        private final DmlOperator reactive;

        private Fixture() {
            SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().build();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
            DataScope scope = DataScope.tenant("tenant_id", 7L);
            SyncSqlExecutor syncExecutor = (SyncSqlExecutor) Proxy.newProxyInstance(
                    SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{com.flying.orm.rdb.support.ListQueryFixture.class}, (proxy, method, args) -> {
                        if (method.isDefault()) {
                            return java.lang.reflect.InvocationHandler.invokeDefault(proxy, method, args);
                        }
                        request.set((SqlRequest) args[0]);
                        return switch (method.getName()) {
                            case "query" -> List.of(DynamicRow.copyOf(Map.of("id", 1L)));
                            case "rowsUpdated" -> 1L;
                            default -> throw new AssertionError(method);
                        };
                    });
            SyncBatchExecutor batches = (SyncBatchExecutor) Proxy.newProxyInstance(
                    SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                    (proxy, method, args) -> { throw new AssertionError(method); });
            ReactiveSqlExecutor reactiveExecutor = new ReactiveSqlExecutor() {
                @Override public Flux<DynamicRow> query(SqlRequest sql) {
                    return Flux.defer(() -> { request.set(sql); return Flux.just(DynamicRow.copyOf(Map.of("id", 1L))); });
                }
                @Override public Mono<Long> rowsUpdated(SqlRequest sql) {
                    return Mono.fromSupplier(() -> { request.set(sql); return 1L; });
                }
            };
            sync = new SyncDmlOperator(SyncFormClient.create(syncExecutor, batches, renderer).withDefaultDataScope(scope),
                    syncExecutor, conditions, scope, RdbDialect.postgresql());
            reactive = new DmlOperator(ReactiveFormClient.create(reactiveExecutor, renderer).withDefaultDataScope(scope),
                    reactiveExecutor, conditions, scope, RdbDialect.postgresql());
        }

        private void execute(boolean async, boolean bound, String kind, ConditionGroup group) {
            if (async) {
                switch (kind) {
                    case "query" -> (bound ? reactive.query(FORM) : reactive.query().from("orders"))
                            .select("id").where("status", "ready").where(group).fetch().collectList().block();
                    case "update" -> (bound ? reactive.update(FORM) : reactive.update("orders"))
                            .set("status", "closed").where("status", "ready").where(group).execute().block();
                    case "delete" -> (bound ? reactive.delete(FORM) : reactive.delete("orders"))
                            .where("status", "ready").where(group).execute().block();
                    default -> throw new AssertionError(kind);
                }
            } else {
                switch (kind) {
                    case "query" -> (bound ? sync.query(FORM) : sync.query().from("orders"))
                            .select("id").where("status", "ready").where(group).fetch();
                    case "update" -> (bound ? sync.update(FORM) : sync.update("orders"))
                            .set("status", "closed").where("status", "ready").where(group).execute();
                    case "delete" -> (bound ? sync.delete(FORM) : sync.delete("orders"))
                            .where("status", "ready").where(group).execute();
                    default -> throw new AssertionError(kind);
                }
            }
        }
    }
}
