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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
    Stream<DynamicTest> scopeOnlyFieldsSurviveEveryOperatorWritePath() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true)
                .flatMap(defaultScope -> Stream.of(Kind.values()).map(kind -> DynamicTest.dynamicTest(
                        (reactive ? "reactive" : "sync") + "/"
                                + (defaultScope ? "default" : "explicit") + "/" + kind,
                        () -> {
                            AtomicReference<SqlRequest> request = new AtomicReference<>();
                            long rows = assertDoesNotThrow(() -> reactive
                                    ? executeReactive(kind, defaultScope, request)
                                    : executeSync(kind, defaultScope, request));
                            assertEquals(1L, rows);
                            assertTrue(request.get().sql().contains("\"tenant_id\" = ?"), request.get().sql());
                            assertTrue(request.get().sql().contains("\"id\" = ?"), request.get().sql());
                            assertEquals(switch (kind) {
                                case UPDATE -> List.of("closed", 1L, 7L);
                                case LOGICAL_DELETE -> List.of(1, 1L, 7L, 0);
                                case PHYSICAL_DELETE -> List.of(1L, 7L);
                            }, request.get().parameters());
                            assertTrue(request.get().sql().startsWith(
                                    kind == Kind.PHYSICAL_DELETE ? "delete from " : "update "));
                        }))));
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

    private static long executeSync(Kind kind, boolean defaultScope, AtomicReference<SqlRequest> request) {
        SyncSqlExecutor executor = syncExecutor(request);
        SyncFormClient forms = SyncFormClient.create(executor, noBatches(), renderer());
        DataScope defaults = defaultScope ? TENANT : DataScope.none();
        SyncDmlOperator dml = new SyncDmlOperator(forms.withDefaultDataScope(defaults),
                executor, CONDITIONS, defaults);
        DataScope explicit = defaultScope ? DataScope.none() : TENANT;
        if (kind == Kind.UPDATE) {
            return dml.update("orders").set("status", "closed").where(where -> where.is("id", 1L))
                    .scope(explicit).execute();
        }
        SyncDmlDeleteOperator delete = dml.delete("orders").where(where -> where.is("id", 1L))
                .logicDelete("deleted").scope(explicit);
        return (kind == Kind.PHYSICAL_DELETE ? delete.physical() : delete).execute();
    }

    private static long executeReactive(Kind kind, boolean defaultScope, AtomicReference<SqlRequest> request) {
        ReactiveSqlExecutor executor = reactiveExecutor(request);
        ReactiveFormClient forms = ReactiveFormClient.create(executor, renderer());
        DataScope defaults = defaultScope ? TENANT : DataScope.none();
        DmlOperator dml = new DmlOperator(forms.withDefaultDataScope(defaults), executor, CONDITIONS, defaults);
        DataScope explicit = defaultScope ? DataScope.none() : TENANT;
        if (kind == Kind.UPDATE) {
            return dml.update("orders").set("status", "closed").where(where -> where.is("id", 1L))
                    .scope(explicit).execute().block(TIMEOUT);
        }
        DmlDeleteOperator delete = dml.delete("orders").where(where -> where.is("id", 1L))
                .logicDelete("deleted").scope(explicit);
        return (kind == Kind.PHYSICAL_DELETE ? delete.physical() : delete).execute().block(TIMEOUT);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(CONDITIONS, RdbDialect.postgresql());
    }

    // Only the database execution boundary is replaced; planning and binding remain real.
    private static SyncSqlExecutor syncExecutor(AtomicReference<SqlRequest> request) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(SyncSqlExecutor.class.getClassLoader(),
                new Class<?>[]{SyncSqlExecutor.class}, (proxy, method, arguments) -> {
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
