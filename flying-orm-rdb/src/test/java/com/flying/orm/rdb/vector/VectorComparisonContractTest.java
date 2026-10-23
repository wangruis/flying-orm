package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VectorComparisonContractTest {

    private static final DynamicForm FORM = DynamicForm.builder("vectors", "vectors")
            .addField(DynamicField.of("embedding", "VECTOR").withLength(2)).build();
    private static final SqlRenderer RENDERER = SqlRenderer.builder().addDefaultTerms()
            .addTermPackage(VectorTermHandlers.postgresql()).build();

    @Test
    void everyMetricSupportsAllSixBoundComparisons() {
        Map<String, String> operators = Map.of("eq", "=", "ne", "<>", "lt", "<", "le", "<=", "gt", ">", "ge", ">=");
        Map<String, String> reversed = Map.of("eq", "=", "ne", "<>", "lt", ">", "le", ">=", "gt", "<", "ge", "<=");
        Map<String, String> metrics = Map.of("l2", "<->", "cosine", "<=>", "inner-product", "<#>");
        for (Map.Entry<String, String> metric : metrics.entrySet()) {
            boolean innerProduct = metric.getKey().equals("inner-product");
            for (String comparison : operators.keySet()) {
                float[] vector = {1, 2};
                SqlFragment sql = RENDERER.renderWhere(VectorStructuredConditions.postgresql().compile(FORM,
                        StructuredConditionInput.term("embedding", "vector-" + metric.getKey() + "-" + comparison,
                                Map.of("vector", vector, innerProduct ? "similarity" : "distance", 0.5))));
                vector[0] = 99;
                assertEquals("((embedding " + metric.getValue() + " cast(? as vector)) "
                        + (innerProduct ? reversed : operators).get(comparison) + " ?)", sql.sql());
                assertArrayEquals(new float[]{1, 2}, (float[]) sql.parameters().getFirst());
                assertEquals(innerProduct ? -0.5 : 0.5, sql.parameters().get(1));
                assertEquals(2, sql.parameters().size());
            }
        }
    }

    @Test
    void syncAndReactiveQueriesKeepFieldCapabilityAndBindingGovernance() {
        RdbDialect dialect = RdbDialect.postgresql();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(RENDERER, dialect);
        FormDataSqlRenderer undeclared = FormDataSqlRenderer.create(RENDERER,
                RdbDialect.of("custom", dialect.schema(), dialect.pagination(), dialect.upsert(), dialect.json()));
        FieldUsePolicy allowed = FieldUsePolicy.builder().allow("embedding", FieldUse.PROJECT, FieldUse.FILTER).build();
        FieldUsePolicy denied = FieldUsePolicy.builder().allow("embedding", FieldUse.PROJECT).build();
        QueryShapeLimits limits = QueryShapeLimits.defaults().withMaxBindCount(2);
        for (boolean reactive : List.of(false, true)) {
            for (String metric : List.of("l2", "cosine", "inner-product")) {
                for (String comparison : List.of("eq", "ne", "lt", "le", "gt", "ge")) {
                    QuerySpec query = QuerySpec.structured(FORM, StructuredConditionInput.term("embedding",
                            "vector-" + metric + "-" + comparison, Map.of("vector", List.of(1, 2),
                                    metric.equals("inner-product") ? "similarity" : "distance", 0.5)));
                    List<SqlRequest> requests = new ArrayList<>();
                    select(renderer, query, reactive, allowed, limits, requests);
                    assertEquals(1, requests.size());
                    assertEquals(2, requests.getFirst().parameters().size());
                    assertThrows(IllegalArgumentException.class,
                            () -> select(renderer, query, reactive, denied, limits, requests));
                    assertThrows(IllegalArgumentException.class,
                            () -> select(renderer, query, reactive, allowed, limits.withMaxBindCount(1), requests));
                    assertThrows(UnsupportedOperationException.class,
                            () -> select(undeclared, query, reactive, allowed, limits, requests));
                    assertEquals(1, requests.size());
                }
            }
        }
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
            ReactiveFormClient.create(executor, renderer)
                    .withStructuredConditionResolver(VectorStructuredConditions.postgresql())
                    .withFieldUsePolicy(policy).withQueryShapeLimits(limits).select(query).collectList().block();
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
            SyncFormClient.create(executor, batch, renderer)
                    .withStructuredConditionResolver(VectorStructuredConditions.postgresql())
                    .withFieldUsePolicy(policy).withQueryShapeLimits(limits).select(query);
        }
    }

    @Test
    void invalidInputsKeepExistingTypeDimensionAndFiniteChecks() {
        for (String metric : List.of("l2", "cosine", "inner-product")) {
            String threshold = metric.equals("inner-product") ? "similarity" : "distance";
            for (String comparison : List.of("eq", "ne", "lt", "le", "gt", "ge")) {
                String operator = "vector-" + metric + "-" + comparison;
                for (Object value : List.of(
                        Map.of("vector", List.of(1), threshold, 0.5),
                        Map.of("vector", List.of(1, 2), threshold, Double.NaN),
                        Map.of("vector", List.of(1, 2), threshold, "0.5 or 1=1"),
                        Map.of("vector", List.of(1, 2), threshold, 0.5, "operator", "or true"))) {
                    assertThrows(IllegalArgumentException.class, () -> VectorStructuredConditions.postgresql()
                            .compile(FORM, StructuredConditionInput.term("embedding", operator, value)));
                }
            }
        }
        assertThrows(IllegalArgumentException.class, () -> VectorStructuredConditions.postgresql().compile(FORM,
                StructuredConditionInput.term("embedding", "vector-l2-lt or true",
                        Map.of("vector", List.of(1, 2), "distance", 0.5))));
    }
}
