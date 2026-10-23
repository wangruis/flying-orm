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
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScopedDmlWriteRegressionTest {

    private static final DataScope TENANT = DataScope.tenant("tenant_id", 7L);
    private static final SqlRenderer CONDITIONS = SqlRenderer.builder().addDefaultTerms().build();
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    @TestFactory
    Stream<DynamicTest> mapAssignmentsShareScalarSemanticsAcrossBothEntryPoints() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(bound ->
                DynamicTest.dynamicTest("map/reactive=" + reactive + "/bound=" + bound, () -> {
                    AtomicReference<SqlRequest> request = new AtomicReference<>();
                    DynamicForm form = DynamicForm.builder("orders", "orders")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("status", "VARCHAR"))
                            .addField(DynamicField.of("note", "VARCHAR"))
                            .addField(DynamicField.of("priority", "INTEGER")).build();
                    Consumer<Map<String, ?>> assign;
                    BiConsumer<String, Object> scalar;
                    LongSupplier execute;
                    if (reactive) {
                        ReactiveSqlExecutor executor = reactiveExecutor(request);
                        DmlOperator dml = new DmlOperator(ReactiveFormClient.create(executor, renderer()),
                                executor, CONDITIONS, DataScope.none(), RdbDialect.postgresql());
                        DmlUpdateOperator update = (bound ? dml.update(form) : dml.update("orders"))
                                .where("id", 1L).scope(TENANT);
                        assign = update::set;
                        scalar = update::set;
                        execute = () -> update.execute().block(TIMEOUT);
                    } else {
                        SyncSqlExecutor executor = syncExecutor(request);
                        SyncDmlOperator dml = new SyncDmlOperator(
                                SyncFormClient.create(executor, noBatches(), renderer()), executor,
                                CONDITIONS, DataScope.none(), RdbDialect.postgresql());
                        SyncDmlUpdateOperator update = (bound ? dml.update(form) : dml.update("orders"))
                                .where("id", 1L).scope(TENANT);
                        assign = update::set;
                        scalar = update::set;
                        execute = update::execute;
                    }
                    assign.accept(Map.of());
                    assertThrows(IllegalArgumentException.class, execute::getAsLong);
                    assertNull(request.get());
                    scalar.accept("status", "initial");
                    Map<String, Object> patch = new LinkedHashMap<>();
                    patch.put("status", "mapped");
                    patch.put("note", null);
                    assign.accept(patch);
                    patch.clear();
                    patch.put("injected_later", "ignored");
                    scalar.accept("status", "closed");
                    Map<String, Integer> priorities = Map.of("priority", 3);
                    assign.accept(priorities);
                    assertEquals(1L, execute.getAsLong());
                    assertEquals(List.of("closed", new SqlNullParameter(bound ? String.class : Object.class),
                            3, 1L, 7L), request.get().parameters());
                    assertTrue(request.get().sql().contains("\"status\" = ?, \"note\" = ?, \"priority\" = ?"));
                    request.set(null);
                    assertThrows(IllegalArgumentException.class,
                            () -> assign.accept(Map.of("status = 1; drop table orders --", "bad")));
                    assertNull(request.get());
                    if (bound) {
                        assign.accept(Map.of("unknown_business", "bad"));
                        assertThrows(IllegalArgumentException.class, execute::getAsLong);
                        assertNull(request.get());
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> scopeOnlyFieldsSurviveEveryOperatorWritePath() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true)
                .flatMap(defaultScope -> Stream.of(false, true).flatMap(businessWhere -> Stream.of(Kind.values()).map(kind -> DynamicTest.dynamicTest(
                        (reactive ? "reactive" : "sync") + "/"
                                + (defaultScope ? "default" : "explicit") + "/" + kind + "/where=" + businessWhere,
                        () -> {
                            AtomicReference<SqlRequest> request = new AtomicReference<>();
                            long rows = assertDoesNotThrow(() -> reactive
                                    ? executeReactive(kind, defaultScope, businessWhere, request)
                                    : executeSync(kind, defaultScope, businessWhere, request));
                            assertEquals(1L, rows);
                            assertTrue(request.get().sql().contains("\"tenant_id\" = ?"), request.get().sql());
                            assertEquals(businessWhere, request.get().sql().contains("\"id\" = ?"), request.get().sql());
                            assertEquals(switch (kind) {
                                case UPDATE -> businessWhere ? List.of("closed", 1L, 7L) : List.of("closed", 7L);
                                case LOGICAL_DELETE -> businessWhere ? List.of(1, 1L, 7L, 0) : List.of(1, 7L, 0);
                                case PHYSICAL_DELETE -> businessWhere ? List.of(1L, 7L) : List.of(7L);
                            }, request.get().parameters());
                            assertTrue(request.get().sql().startsWith(
                                    kind == Kind.PHYSICAL_DELETE ? "delete from " : "update "));
                        })))));
    }

    @TestFactory
    Stream<DynamicTest> unknownBusinessFieldsRemainRejectedBeforeExecution() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(Kind.values())
                .map(kind -> DynamicTest.dynamicTest((reactive ? "reactive" : "sync") + "/" + kind, () -> {
                    AtomicReference<SqlRequest> request = new AtomicReference<>();
                    DynamicForm form = DynamicForm.builder("orders", "orders")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("status", "VARCHAR"))
                            .addField(DynamicField.of("deleted", "INTEGER"))
                            .logicDelete("deleted", 0, 1).build();
                    ConditionGroup where = ConditionGroup.and().where("unknown_business", "=", 1L).build();
                    WriteSpec spec = (kind == Kind.UPDATE
                            ? WriteSpec.update(form, Map.of("status", "closed"), where)
                            : WriteSpec.delete(form, where)).withScope(TENANT);
                    assertThrows(IllegalArgumentException.class, () -> {
                        if (reactive) {
                            ReactiveFormClient forms = ReactiveFormClient.create(reactiveExecutor(request), renderer());
                            switch (kind) {
                                case UPDATE -> forms.update(spec).block(TIMEOUT);
                                case LOGICAL_DELETE -> forms.delete(spec).block(TIMEOUT);
                                case PHYSICAL_DELETE -> forms.physicalDelete(spec).block(TIMEOUT);
                            }
                        } else {
                            SyncFormClient forms = SyncFormClient.create(syncExecutor(request), noBatches(), renderer());
                            switch (kind) {
                                case UPDATE -> forms.update(spec);
                                case LOGICAL_DELETE -> forms.delete(spec);
                                case PHYSICAL_DELETE -> forms.physicalDelete(spec);
                            }
                        }
                    });
                    assertNull(request.get());
                })));
    }

    private static long executeSync(Kind kind, boolean defaultScope, boolean businessWhere,
                                    AtomicReference<SqlRequest> request) {
        SyncSqlExecutor executor = syncExecutor(request);
        SyncFormClient forms = SyncFormClient.create(executor, noBatches(), renderer());
        DataScope defaults = defaultScope ? TENANT : DataScope.none();
        SyncDmlOperator dml = new SyncDmlOperator(forms.withDefaultDataScope(defaults),
                executor, CONDITIONS, defaults, RdbDialect.postgresql());
        DataScope explicit = defaultScope ? DataScope.none() : TENANT;
        if (kind == Kind.UPDATE) {
            SyncDmlUpdateOperator update = dml.update("orders").set("status", "closed").scope(explicit);
            if (businessWhere) update.where("id", 1L);
            return update.execute();
        }
        SyncDmlDeleteOperator delete = dml.delete("orders").logicDelete("deleted").scope(explicit);
        if (businessWhere) delete.where("id", 1L);
        return (kind == Kind.PHYSICAL_DELETE ? delete.physical() : delete).execute();
    }

    private static long executeReactive(Kind kind, boolean defaultScope, boolean businessWhere,
                                        AtomicReference<SqlRequest> request) {
        ReactiveSqlExecutor executor = reactiveExecutor(request);
        ReactiveFormClient forms = ReactiveFormClient.create(executor, renderer());
        DataScope defaults = defaultScope ? TENANT : DataScope.none();
        DmlOperator dml = new DmlOperator(forms.withDefaultDataScope(defaults), executor, CONDITIONS, defaults, RdbDialect.postgresql());
        DataScope explicit = defaultScope ? DataScope.none() : TENANT;
        if (kind == Kind.UPDATE) {
            DmlUpdateOperator update = dml.update("orders").set("status", "closed").scope(explicit);
            if (businessWhere) update.where("id", 1L);
            return update.execute().block(TIMEOUT);
        }
        DmlDeleteOperator delete = dml.delete("orders").logicDelete("deleted").scope(explicit);
        if (businessWhere) delete.where("id", 1L);
        return (kind == Kind.PHYSICAL_DELETE ? delete.physical() : delete).execute().block(TIMEOUT);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(CONDITIONS, RdbDialect.postgresql());
    }

    // Only the database execution boundary is replaced; planning and binding remain real.
    private static SyncSqlExecutor syncExecutor(AtomicReference<SqlRequest> request) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(SyncSqlExecutor.class.getClassLoader(),
                new Class<?>[]{com.flying.orm.rdb.support.ListQueryFixture.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("rowsUpdated")) {
                        request.set((SqlRequest) arguments[0]);
                        return 1L;
                    }
                    throw new AssertionError("unexpected execution: " + method);
                });
    }

    private static ReactiveSqlExecutor reactiveExecutor(AtomicReference<SqlRequest> request) {
        return new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(SqlRequest sql) {
                return Flux.error(new AssertionError("unexpected query"));
            }

            @Override public Mono<Long> rowsUpdated(SqlRequest sql) {
                request.set(sql);
                return Mono.just(1L);
            }
        };
    }

    private static SyncBatchExecutor noBatches() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(SyncBatchExecutor.class.getClassLoader(),
                new Class<?>[]{SyncBatchExecutor.class}, (proxy, method, arguments) -> {
                    throw new AssertionError("unexpected batch: " + method);
                });
    }

    private enum Kind {
        UPDATE, LOGICAL_DELETE, PHYSICAL_DELETE
    }
}
