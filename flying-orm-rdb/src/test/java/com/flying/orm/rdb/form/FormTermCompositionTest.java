package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormTermCompositionTest {

    @TestFactory
    Stream<DynamicTest> keepsLogicalDeleteAndVersionPredicatesOutsideCustomDisjunctions() {
        return Stream.of(RdbDialect.mysql(), RdbDialect.postgresql(), RdbDialect.oracle(),
                        RdbDialect.sqlServer(), RdbDialect.h2())
                .map(dialect -> DynamicTest.dynamicTest(dialect.name(), () -> {
                    SqlRenderer standard = SqlRenderer.builder().addDefaultTerms().build();
                    SqlRenderer extended = SqlRenderer.builder().addDefaultTerms().addTerm(
                            SqlTermHandler.of("eq-or-null", (term, context) -> standard
                                    .withIdentifierRenderer(context::identifier).renderWhere(ConditionGroup.or()
                                            .where(term.field(), "=", term.value())
                                            .where(term.field(), "is-null", null).build()))).build();
                    DynamicForm form = DynamicForm.builder("items", "items")
                            .addField(DynamicField.of("x", "INTEGER"))
                            .addField(DynamicField.of("label", "VARCHAR"))
                            .addField(DynamicField.of("deleted", "INTEGER"))
                            .addField(DynamicField.of("version", "BIGINT"))
                            .logicDelete("deleted").build();
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(extended, dialect);
                    FormOperationPlanner planner = new FormOperationPlanner(renderer,
                            new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                            SqlExecutionOptions.safeDefaults());
                    SqlRequest request = planner.update(WriteSpec.update(form, Map.of("label", "changed"),
                            extended.conditions().where("x", "eq-or-null", 10).build())
                            .withLock(OptimisticLockOptions.increment("version", 7L))).request();
                    String x = renderer.conditionRenderer().identifier("x");
                    String deleted = renderer.conditionRenderer().identifier("deleted");
                    String version = renderer.conditionRenderer().identifier("version");
                    assertTrue(request.sql().contains("(" + x + " = ? or " + x + " is null) and "
                            + deleted + " = ? and " + version + " = ?"), request.sql());
                    assertEquals(List.of("changed", 10, 0, 7L), request.parameters());
                }));
    }

    @TestFactory
    Stream<DynamicTest> keepsTenantScopeOutsideCustomDisjunctions() {
        return Stream.of(RdbDialect.mysql(), RdbDialect.postgresql(), RdbDialect.oracle(),
                        RdbDialect.sqlServer(), RdbDialect.h2())
                .flatMap(dialect -> Stream.of(false, true).flatMap(reactive ->
                        Stream.of("select", "update", "delete").map(operation -> DynamicTest.dynamicTest(
                                dialect.name() + "/" + reactive + "/" + operation,
                                () -> verify(dialect, reactive, operation)))));
    }

    private static void verify(RdbDialect dialect, boolean reactive, String operation) {
        SqlRenderer standard = SqlRenderer.builder().addDefaultTerms().build();
        SqlRenderer extended = SqlRenderer.builder().addDefaultTerms().addTerm(
                SqlTermHandler.of("eq-or-null", (term, context) -> standard
                        .withIdentifierRenderer(context::identifier).renderWhere(ConditionGroup.or()
                                .where(term.field(), "=", term.value())
                                .where(term.field(), "is-null", null).build()))).build();
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("x", "INTEGER"))
                .addField(DynamicField.of("label", "VARCHAR"))
                .addField(DynamicField.of("tenant_id", "VARCHAR"))
                .tenant("tenant_id", TenantStrategy.MANUAL).build();
        ConditionGroup where = extended.conditions().where("x", "eq-or-null", 10).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(extended, dialect);
        List<SqlRequest> requests = new ArrayList<>();
        if (reactive) {
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> { requests.add(request); return Flux.empty(); });
                }
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(() -> { requests.add(request); return 0L; });
                }
            };
            ReactiveFormClient client = ReactiveFormClient.create(executor, renderer)
                    .withDefaultDataScope(DataScope.tenant("tenant_id", "A"));
            switch (operation) {
                case "select" -> client.select(QuerySpec.of(form, where)).collectList().block();
                case "update" -> client.update(WriteSpec.update(form, Map.of("label", "changed"), where)).block();
                default -> client.delete(WriteSpec.delete(form, where)).block();
            }
        } else {
            SyncSqlExecutor executor = new SyncSqlExecutor() {
                public List<DynamicRow> query(SqlRequest request) { requests.add(request); return List.of(); }
                public long rowsUpdated(SqlRequest request) { requests.add(request); return 0L; }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("unexpected generated keys");
                }
            };
            SyncBatchExecutor batch = (SyncBatchExecutor) Proxy.newProxyInstance(
                    SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                    (object, method, arguments) -> { throw new AssertionError("unexpected batch"); });
            SyncFormClient client = SyncFormClient.create(executor, batch, renderer)
                    .withDefaultDataScope(DataScope.tenant("tenant_id", "A"));
            switch (operation) {
                case "select" -> client.select(QuerySpec.of(form, where));
                case "update" -> client.update(WriteSpec.update(form, Map.of("label", "changed"), where));
                default -> client.delete(WriteSpec.delete(form, where));
            }
        }
        assertEquals(1, requests.size());
        SqlRequest request = requests.getFirst();
        String x = renderer.conditionRenderer().identifier("x");
        String tenant = renderer.conditionRenderer().identifier("tenant_id");
        assertEquals("(" + x + " = ? or " + x + " is null) and " + tenant + " = ?",
                request.sql().substring(request.sql().indexOf(" where ") + 7));
        assertEquals(operation.equals("update") ? List.of("changed", 10, "A") : List.of(10, "A"),
                request.parameters());
    }
}
