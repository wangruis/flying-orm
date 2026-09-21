package com.flying.orm.rdb.extension;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.json.JsonConditionValue;
import com.flying.orm.rdb.json.JsonTermHandlers;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonGovernedQueryTest {

    @TestFactory
    Stream<DynamicTest> governancePreservesEverySupportedJsonQuery() {
        return Stream.of(false, true).flatMap(mysql -> Stream.of(false, true).flatMap(reactive ->
                conditions().entrySet().stream().flatMap(condition -> Stream.of(false, true).map(fieldPolicy ->
                        DynamicTest.dynamicTest((mysql ? "mysql" : "postgresql") + "/" + reactive + "/"
                                + condition.getKey() + "/" + (fieldPolicy ? "field-policy" : "shape-limits"), () -> {
                            SqlRenderer conditions = conditions(mysql);
                            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                                    conditions, mysql ? RdbDialect.mysql() : RdbDialect.postgresql());
                            QuerySpec query = query(conditions, condition.getKey(), condition.getValue());
                            List<SqlRequest> requests = new ArrayList<>();
                            select(renderer, query, reactive, FieldUsePolicy.unrestricted(),
                                    QueryShapeLimits.defaults(), requests);

                            assertDoesNotThrow(() -> select(renderer, query, reactive,
                                    fieldPolicy ? readableJsonPolicy() : FieldUsePolicy.unrestricted(),
                                    fieldPolicy ? QueryShapeLimits.defaults()
                                            : QueryShapeLimits.defaults().withMaxProjectionCount(1), requests));

                            assertEquals(2, requests.size());
                            assertEquals(requests.getFirst().sql(), requests.getLast().sql());
                            assertArrayEquals(requests.getFirst().parameters().toArray(),
                                    requests.getLast().parameters().toArray());
                        })))));
    }

    @TestFactory
    Stream<DynamicTest> jsonCapabilityDoesNotBypassPolicyBudgetOrMissingCapability() {
        return Stream.of(false, true).flatMap(mysql -> Stream.of(false, true).map(reactive ->
                DynamicTest.dynamicTest((mysql ? "mysql" : "postgresql") + "/" + reactive, () -> {
                    SqlRenderer conditions = conditions(mysql);
                    RdbDialect dialect = mysql ? RdbDialect.mysql() : RdbDialect.postgresql();
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect);
                    QuerySpec query = query(conditions, "json-exists", JsonConditionValue.exists(List.of("active")));
                    List<SqlRequest> requests = new ArrayList<>();

                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            FieldUsePolicy.builder().allow("payload", FieldUse.PROJECT).build(),
                            QueryShapeLimits.defaults(), requests));
                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            readableJsonPolicy(), QueryShapeLimits.defaults().withMaxProjectionCount(0), requests));

                    RdbDialect undeclared = RdbDialect.of("custom", dialect.schema(), dialect.pagination(),
                            dialect.upsert(), dialect.json());
                    assertThrows(UnsupportedOperationException.class, () -> select(
                            FormDataSqlRenderer.create(conditions, undeclared), query, reactive,
                            readableJsonPolicy(), QueryShapeLimits.defaults(), requests));
                    assertEquals(0, requests.size());
                })));
    }

    private static Map<String, JsonConditionValue> conditions() {
        return Map.of(
                "json-path-eq", JsonConditionValue.pathEquals(List.of("active"), true),
                "json-contains", JsonConditionValue.contains(Map.of("active", true)),
                "json-exists", JsonConditionValue.exists(List.of("active")),
                "json-array-contains", JsonConditionValue.arrayContains(List.of("tags"), "orm"));
    }

    private static SqlRenderer conditions(boolean mysql) {
        return SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(mysql ? JsonTermHandlers.mysql() : JsonTermHandlers.postgresql()).build();
    }

    private static QuerySpec query(SqlRenderer conditions, String operator, JsonConditionValue value) {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        return QuerySpec.of(form, conditions.conditions().where("payload", operator, value).build());
    }

    private static FieldUsePolicy readableJsonPolicy() {
        return FieldUsePolicy.builder().allow("payload", FieldUse.PROJECT, FieldUse.FILTER).build();
    }

    private static void select(FormDataSqlRenderer renderer, QuerySpec query, boolean reactive,
                               FieldUsePolicy policy, QueryShapeLimits limits, List<SqlRequest> requests) {
        if (reactive) {
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> { requests.add(request); return Flux.empty(); });
                }
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    throw new AssertionError("unexpected write");
                }
            };
            ReactiveFormClient.create(executor, renderer).withFieldUsePolicy(policy).withQueryShapeLimits(limits)
                    .select(query).collectList().block();
        } else {
            SyncSqlExecutor executor = new SyncSqlExecutor() {
                public List<DynamicRow> query(SqlRequest request) {
                    requests.add(request);
                    return List.of();
                }
                public long rowsUpdated(SqlRequest request) {
                    throw new AssertionError("unexpected write");
                }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("unexpected generated keys");
                }
            };
            SyncBatchExecutor batch = (SyncBatchExecutor) Proxy.newProxyInstance(
                    SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                    (object, method, arguments) -> { throw new AssertionError("unexpected batch"); });
            SyncFormClient.create(executor, batch, renderer).withFieldUsePolicy(policy).withQueryShapeLimits(limits)
                    .select(query);
        }
    }
}
