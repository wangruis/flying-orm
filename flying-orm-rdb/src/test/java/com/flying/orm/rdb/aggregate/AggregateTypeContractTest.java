package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateTypeContractTest {

    @Test
    void rejectsTextBackedOffsetTimeMinAndMax() {
        QuerySpec query = temporalQuery();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner planner = temporalPlanner(dialect);
            assertAll(
                    () -> assertOffsetTimeRejected(() -> planner.plan(spec(query, AggregateExpression.min(
                            "remote_time", "first", LogicalType.OFFSET_TIME, OffsetTime.class)))),
                    () -> assertOffsetTimeRejected(() -> planner.plan(spec(query, AggregateExpression.max(
                            "remote_time", "last", LogicalType.OFFSET_TIME, OffsetTime.class)))));
        }
    }

    @Test
    void rejectsTextBackedOffsetTimeGroupAliasOrderingButAllowsCountOrdering() {
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner planner = temporalPlanner(dialect);
            AggregateSpec grouped = AggregateSpec.builder(temporalQuery().withSorts(List.of(PageSort.asc("clock"))))
                    .group(GroupSelection.of("remote_time", "clock"))
                    .aggregate(AggregateExpression.count("id", "total")).build();
            assertOffsetTimeRejected(() -> planner.plan(grouped));
            assertDoesNotThrow(() -> planner.plan(AggregateSpec.builder(temporalQuery())
                    .group(GroupSelection.of("remote_time", "clock"))
                    .aggregate(AggregateExpression.count("id", "total")).build()));
            assertDoesNotThrow(() -> planner.plan(spec(
                    temporalQuery().withSorts(List.of(PageSort.asc("total"))),
                    AggregateExpression.count("remote_time", "total"))));
        }
    }

    @Test
    void nativeOffsetTimeAggregationAndOrderingRemainSupported() {
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.postgresql())) {
            FormAggregatePlanner planner = temporalPlanner(dialect);
            assertDoesNotThrow(() -> planner.plan(spec(temporalQuery(), AggregateExpression.min(
                    "remote_time", "first", LogicalType.OFFSET_TIME, OffsetTime.class))));
            assertDoesNotThrow(() -> planner.plan(spec(temporalQuery(), AggregateExpression.max(
                    "remote_time", "last", LogicalType.OFFSET_TIME, OffsetTime.class))));
            assertDoesNotThrow(() -> planner.plan(AggregateSpec.builder(
                    temporalQuery().withSorts(List.of(PageSort.asc("clock"))))
                    .group(GroupSelection.of("remote_time", "clock"))
                    .aggregate(AggregateExpression.count("id", "total")).build()));
        }
    }

    private static QuerySpec temporalQuery() {
        return QuerySpec.of(DynamicForm.builder("temporal_metrics", "temporal_metrics")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("remote_time", "OFFSET_TIME")).build(), ConditionGroup.and().build());
    }

    private static FormAggregatePlanner temporalPlanner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormAggregatePlanner(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(), SqlExecutionOptions.safeDefaults(), FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
    }

    private static void assertOffsetTimeRejected(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().contains("OFFSET_TIME"), failure.getMessage());
    }

    @Test
    void commonFunctionsPublishStableJavaAndLogicalResultTypes() {
        AggregateExpression<Long> count = AggregateExpression.count("id", "total");
        AggregateExpression<Long> distinct = AggregateExpression.countDistinct("tenant_id", "tenants");
        AggregateExpression<BigDecimal> sum = AggregateExpression.sum("amount", "amount_sum");
        AggregateExpression<BigDecimal> average = AggregateExpression.avg("amount", "amount_avg");
        AggregateExpression<Instant> minimum = AggregateExpression.min(
                "created_at", "first_created_at", LogicalType.OFFSET_TIMESTAMP, Instant.class);

        assertEquals(Long.class, count.javaType());
        assertEquals(LogicalType.BIG_INTEGER, distinct.resultLogicalType());
        assertEquals(BigDecimal.class, sum.javaType());
        assertEquals(LogicalType.DECIMAL, average.resultLogicalType());
        assertEquals(Instant.class, minimum.javaType());
    }

    @Test
    void rejectsNonComparableMinMaxContractsAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                     () -> AggregateExpression.min(
                             "payload", "minimum_payload", LogicalType.JSON, String.class));
        assertThrows(IllegalArgumentException.class,
                     () -> AggregateExpression.max(
                             "payload", "maximum_payload", LogicalType.BINARY, byte[].class));
    }

    @Test
    void typedRowsAreBoundToTheirDeclaredExpression() {
        AggregateExpression<Long> count = AggregateExpression.count("id", "total");
        AggregateRowLayout layout = AggregateRowLayout.of(
                java.util.List.of(), java.util.List.of(count));
        AggregateRow row = AggregateRow.of(layout, java.util.List.of(12L));

        assertEquals(12L, row.get(count));
        assertThrows(IllegalArgumentException.class,
                     () -> row.get(AggregateExpression.count("id", "other_total")));
    }

    @Test
    void plannerValidatesEveryFunctionAgainstTheSourceFieldType() {
        DynamicForm form = DynamicForm.builder("metrics", "metrics")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("label", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .addField(DynamicField.of("payload", "JSON"))
                .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build());
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        FormAggregatePlanner planner = new FormAggregatePlanner(
                renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(),
                SqlExecutionOptions.safeDefaults(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());

        assertDoesNotThrow(() -> planner.plan(spec(query, AggregateExpression.count("payload", "total"))));
        assertDoesNotThrow(() -> planner.plan(spec(
                query, AggregateExpression.countDistinct("label", "labels"))));
        assertDoesNotThrow(() -> planner.plan(spec(query, AggregateExpression.sum("amount", "sum"))));
        assertDoesNotThrow(() -> planner.plan(spec(query, AggregateExpression.avg("amount", "average"))));
        assertDoesNotThrow(() -> planner.plan(spec(query, AggregateExpression.min(
                "label", "first", LogicalType.TEXT, String.class))));
        assertDoesNotThrow(() -> planner.plan(spec(query, AggregateExpression.max(
                "id", "largest", LogicalType.BIG_INTEGER, Long.class))));

        assertThrows(IllegalArgumentException.class,
                     () -> planner.plan(spec(query, AggregateExpression.count("missing", "total"))));
        assertThrows(IllegalArgumentException.class,
                     () -> planner.plan(spec(
                             query, AggregateExpression.countDistinct("payload", "payloads"))));
        assertThrows(IllegalArgumentException.class,
                     () -> planner.plan(spec(query, AggregateExpression.sum("label", "sum"))));
        assertThrows(IllegalArgumentException.class,
                     () -> planner.plan(spec(query, AggregateExpression.avg("label", "average"))));
        assertThrows(IllegalArgumentException.class,
                     () -> planner.plan(spec(query, AggregateExpression.min(
                             "id", "minimum", LogicalType.DECIMAL, BigDecimal.class))));
    }

    private static AggregateSpec spec(QuerySpec query, AggregateExpression<?> expression) {
        return AggregateSpec.builder(query).aggregate(expression).build();
    }
}
