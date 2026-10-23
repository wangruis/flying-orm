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
    Stream<DynamicTest> objectContainmentPreservesFieldPolicyAndActualBindBudget() {
        return Stream.of(false, true).flatMap(oracle -> Stream.of(false, true).flatMap(reactive ->
                Stream.of("json-contains", "json-not-contains").map(operator -> DynamicTest.dynamicTest(
                        (oracle ? "oracle" : "sqlserver") + "/" + operator + "/reactive=" + reactive, () -> {
                    SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                            .addTermPackage(oracle ? JsonTermHandlers.oracle() : JsonTermHandlers.sqlServer()).build();
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions,
                            oracle ? RdbDialect.oracle() : RdbDialect.sqlServer());
                    QuerySpec query = query(conditions, operator,
                            JsonConditionValue.contains(Map.of("profile", Map.of("name", "active", "level", 3))));
                    List<SqlRequest> requests = new ArrayList<>();
                    select(renderer, query, reactive, readableJsonPolicy(), QueryShapeLimits.defaults(), requests);
                    int binds = requests.getFirst().parameters().size();
                    assertEquals(oracle ? 2 : 4, binds);
                    select(renderer, query, reactive, readableJsonPolicy(),
                            QueryShapeLimits.defaults().withMaxBindCount(binds), requests);
                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            readableJsonPolicy(), QueryShapeLimits.defaults().withMaxBindCount(binds - 1), requests));
                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            FieldUsePolicy.builder().allow("payload", FieldUse.PROJECT).build(),
                            QueryShapeLimits.defaults(), requests));
                    assertEquals(2, requests.size(), "rejected plans must not reach the executor");
                }))));
    }

    @TestFactory
    Stream<DynamicTest> sqlServerMembershipPreservesFieldPolicyAndActualBindBudget() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of("json-path-in", "json-path-not-in")
                .map(operator -> DynamicTest.dynamicTest(operator + "/reactive=" + reactive, () -> {
                    SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                            .addTermPackage(JsonTermHandlers.sqlServer()).build();
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.sqlServer());
                    for (List<?> values : List.of(List.of(), java.util.Arrays.asList("active", true, null))) {
                        QuerySpec query = query(conditions, operator, JsonConditionValue.pathIn(List.of("status"), values));
                        List<SqlRequest> requests = new ArrayList<>();
                        select(renderer, query, reactive, readableJsonPolicy(), QueryShapeLimits.defaults(), requests);
                        int binds = requests.getFirst().parameters().size();
                        select(renderer, query, reactive, readableJsonPolicy(),
                                QueryShapeLimits.defaults().withMaxBindCount(binds), requests);
                        assertEquals(2, requests.size());
                        assertEquals(requests.getFirst().sql(), requests.getLast().sql());
                        if (binds > 0) assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                                readableJsonPolicy(), QueryShapeLimits.defaults().withMaxBindCount(binds - 1), requests));
                        assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                                FieldUsePolicy.builder().allow("payload", FieldUse.PROJECT).build(),
                                QueryShapeLimits.defaults(), requests));
                        assertEquals(2, requests.size(), "rejected plans must not reach the executor");
                    }
                })));
    }

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
                                            : QueryShapeLimits.defaults().withMaxProjectionCount(1)
                                                    .withMaxBindCount(requests.getFirst().parameters().size()), requests));

                            assertEquals(2, requests.size());
                            assertEquals(requests.getFirst().sql(), requests.getLast().sql());
                            assertArrayEquals(requests.getFirst().parameters().toArray(),
                                    requests.getLast().parameters().toArray());
                        })))));
    }

    @TestFactory
    Stream<DynamicTest> jsonCapabilityDoesNotBypassPolicyBudgetOrMissingCapability() {
        return Stream.of(false, true).flatMap(mysql -> Stream.of(false, true).flatMap(reactive ->
                conditions().entrySet().stream().map(condition -> DynamicTest.dynamicTest(
                        (mysql ? "mysql" : "postgresql") + "/" + reactive + "/" + condition.getKey(), () -> {
                    SqlRenderer conditions = conditions(mysql);
                    RdbDialect dialect = mysql ? RdbDialect.mysql() : RdbDialect.postgresql();
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect);
                    QuerySpec query = query(conditions, condition.getKey(), condition.getValue());
                    List<SqlRequest> requests = new ArrayList<>();

                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            FieldUsePolicy.builder().allow("payload", FieldUse.PROJECT).build(),
                            QueryShapeLimits.defaults(), requests));
                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            readableJsonPolicy(), QueryShapeLimits.defaults().withMaxProjectionCount(0), requests));
                    int binds = switch (condition.getValue().kind()) {
                        case CONTAINS, EXISTS, PATH_NULL -> 1;
                        case PATH_MEMBERSHIP -> mysql ? 4 : 3;
                        default -> 2;
                    };
                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            readableJsonPolicy(), QueryShapeLimits.defaults().withMaxBindCount(binds - 1), requests));

                    RdbDialect undeclared = RdbDialect.of("custom", dialect.schema(), dialect.pagination(),
                            dialect.upsert(), dialect.json());
                    assertThrows(UnsupportedOperationException.class, () -> select(
                            FormDataSqlRenderer.create(conditions, undeclared), query, reactive,
                            readableJsonPolicy(), QueryShapeLimits.defaults(), requests));
                    assertEquals(0, requests.size());
                }))));
    }

    @TestFactory
    Stream<DynamicTest> emptyMembershipKeepsFieldGovernanceWithZeroBindings() {
        return Stream.of(false, true).flatMap(mysql -> Stream.of(false, true).flatMap(reactive ->
                Stream.of("json-path-in", "json-path-not-in").map(operator -> DynamicTest.dynamicTest(
                        (mysql ? "mysql" : "postgresql") + "/" + reactive + "/empty/" + operator, () -> {
                    SqlRenderer conditions = conditions(mysql);
                    FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions,
                            mysql ? RdbDialect.mysql() : RdbDialect.postgresql());
                    QuerySpec query = query(conditions, operator, JsonConditionValue.pathIn(List.of("status"), List.of()));
                    List<SqlRequest> requests = new ArrayList<>();
                    select(renderer, query, reactive, readableJsonPolicy(),
                            QueryShapeLimits.defaults().withMaxBindCount(0), requests);
                    assertEquals(1, requests.size());
                    assertEquals(0, requests.getFirst().parameters().size());
                    assertThrows(IllegalArgumentException.class, () -> select(renderer, query, reactive,
                            FieldUsePolicy.builder().allow("payload", FieldUse.PROJECT).build(),
                            QueryShapeLimits.defaults(), requests));
                    assertEquals(1, requests.size());
                }))));
    }

    private static Map<String, JsonConditionValue> conditions() {
        return Map.ofEntries(
                Map.entry("json-path-eq", JsonConditionValue.pathEquals(List.of("active"), true)),
                Map.entry("json-path-ne", JsonConditionValue.pathCompare(List.of("status"), "disabled")),
                Map.entry("json-path-gt", JsonConditionValue.pathCompare(List.of("age"), 18)),
                Map.entry("json-path-ge", JsonConditionValue.pathCompare(List.of("age"), 18)),
                Map.entry("json-path-lt", JsonConditionValue.pathCompare(List.of("age"), 65)),
                Map.entry("json-path-le", JsonConditionValue.pathCompare(List.of("age"), 65)),
                Map.entry("json-path-like", JsonConditionValue.pathLike(List.of("name"), "张%")),
                Map.entry("json-path-not-like", JsonConditionValue.pathLike(List.of("name"), "张%")),
                Map.entry("json-path-is-null", JsonConditionValue.pathNull(List.of("name"))),
                Map.entry("json-path-not-null", JsonConditionValue.pathNull(List.of("name"))),
                Map.entry("json-path-in", JsonConditionValue.pathIn(List.of("status"), List.of("active", "pending"))),
                Map.entry("json-path-not-in", JsonConditionValue.pathIn(List.of("status"), List.of("active", "pending"))),
                Map.entry("json-contains", JsonConditionValue.contains(Map.of("active", true))),
                Map.entry("json-not-contains", JsonConditionValue.contains(Map.of("active", true))),
                Map.entry("json-contained-by", JsonConditionValue.contains(Map.of("active", true))),
                Map.entry("json-not-contained-by", JsonConditionValue.contains(Map.of("active", true))),
                Map.entry("json-exists", JsonConditionValue.exists(List.of("active"))),
                Map.entry("json-not-exists", JsonConditionValue.exists(List.of("active"))),
                Map.entry("json-array-contains", JsonConditionValue.arrayContains(List.of("tags"), "orm")),
                Map.entry("json-array-not-contains", JsonConditionValue.arrayContains(List.of("tags"), "orm")));
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
            SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) {
                    requests.add(request);
                    return List.of();
                }
                public long rowsUpdated(SqlRequest request) {
                    throw new AssertionError("unexpected write");
                }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
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
