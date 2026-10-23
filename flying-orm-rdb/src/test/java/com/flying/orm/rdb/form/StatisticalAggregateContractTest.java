package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StatisticalAggregateContractTest {
    private static final DynamicForm FORM = DynamicForm.builder("sales", "sales")
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("category", "VARCHAR")).build();

    @Test
    void populationAndSampleFunctionsHaveExplicitDialectNames() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .stddevPop("amount", "dp").stddevSample("amount", "ds")
                .variancePop("amount", "vp").varianceSample("amount", "vs").build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            String sql = planner(dialect, FieldUsePolicy.unrestricted()).plan(spec).request().sql();
            boolean sqlServer = dialect.name().equals("sqlserver");
            for (String function : sqlServer ? List.of("stdevp", "stdev", "varp", "var")
                    : List.of("stddev_pop", "stddev_samp", "var_pop", "var_samp")) {
                assertTrue(sql.contains(function + "("), sql);
            }
        }
    }

    @Test
    void computedStatisticsPreserveSelectWhereHavingParameterOrderAndPage() {
        ValueExpression amount = ValueExpression.field("amount").add(ValueExpression.value(2));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM,
                ConditionGroup.and().where("amount", ">", 3).build()))
                .group("category", "category")
                .aggregate(AggregateExpression.stddevPop(amount, "dp"))
                .aggregate(AggregateExpression.stddevSample(amount, "ds"))
                .aggregate(AggregateExpression.variancePop(amount, "vp"))
                .aggregate(AggregateExpression.varianceSample(amount, "vs"))
                .having(group -> group.where("vs", ">", 4))
                .orderBy(PageSort.desc("vs")).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted())
                .planPage(spec, PageQuery.of(1, 5), false);
        assertEquals(List.of(2, 2, 2, 2, 3, 4), plan.countRequest().orElseThrow().parameters());
        assertEquals(List.of(2, 2, 2, 2, 3, 4, 5, 0L), plan.request().parameters());
        assertTrue(plan.request().sql().contains("flying_aggregate_result"));
        assertTrue(plan.request().sql().contains("var_samp((\"amount\" + ?))"), plan.request().sql());
    }

    @Test
    void directHavingReferencesDecimalResultInsteadOfIntegerSourceType() {
        DynamicForm integers = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("quantity", "INTEGER")).build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(integers, ConditionGroup.and().build()))
                .stddevSample("quantity", "spread")
                .having(group -> group.where("spread", ">", new BigDecimal("1.25"))).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.sqlServer(), FieldUsePolicy.unrestricted()).plan(spec);
        assertTrue(plan.request().sql().contains("having stdev("), plan.request().sql());
        assertEquals(List.of(new BigDecimal("1.25")), plan.request().parameters());
    }

    @Test
    void statisticsRetainNumericFieldGovernanceAndHiddenResults() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build())).varianceSample("amount", "spread").build();
        assertThrows(ScopeAccessException.class, () -> planner(RdbDialect.postgresql(),
                FieldUsePolicy.builder().allow("category", FieldUse.AGGREGATE).build()).plan(spec));
        FieldUsePolicy hidden = FieldUsePolicy.builder().allow("amount", FieldUse.AGGREGATE)
                .visibility("amount", FieldVisibility.HIDDEN).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), hidden).plan(spec);
        assertNull(new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(Map.of("spread", 2.0)))
                .get("spread", BigDecimal.class));
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted())
                .plan(AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build())).stddevPop("category", "spread").build()));
        DynamicForm secrets = DynamicForm.builder("secret", "secret")
                .addField(DynamicField.of("amount", "VARCHAR"))
                .encrypted("amount", EncryptedFieldDefinition.builder().build()).build();
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(AggregateSpec.builder(
                        QuerySpec.of(secrets, ConditionGroup.and().build())).variancePop("amount", "spread").build()));
        assertEquals("encrypted field must not be used as an aggregate source", error.getMessage());
    }

    @Test
    void decimalDecoderPreservesNullAndNormalizesFloatingPointResults() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .stddevPop("amount", "dp").stddevSample("amount", "ds")
                .variancePop("amount", "vp").varianceSample("amount", "vs").build();
        AggregateResultDecoder decoder = new AggregateResultDecoder(
                planner(RdbDialect.sqlServer(), FieldUsePolicy.unrestricted()).plan(spec));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("dp", 1.5d);
        values.put("ds", "2.25");
        values.put("vp", new BigDecimal("3.50"));
        values.put("vs", null);
        AggregateRow row = decoder.decode(DynamicRow.copyOf(values));
        assertEquals(new BigDecimal("1.5"), row.get("dp", BigDecimal.class));
        assertEquals(new BigDecimal("2.25"), row.get("ds", BigDecimal.class));
        assertEquals(new BigDecimal("3.50"), row.get("vp", BigDecimal.class));
        assertNull(row.get("vs", BigDecimal.class));
    }

    @Test
    void generatedSqlComputesPopulationAndSampleWithoutOrmSideAggregation() throws Exception {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .stddevPop("amount", "dp").stddevSample("amount", "ds")
                .variancePop("amount", "vp").varianceSample("amount", "vs").build();
        String sql = planner(RdbDialect.h2(), FieldUsePolicy.unrestricted()).plan(spec).request().sql();
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:statistical_aggregate;DATABASE_TO_LOWER=TRUE");
             Statement statement = connection.createStatement()) {
            statement.execute("create table sales(amount decimal(12,2), category varchar(8))");
            statement.execute("insert into sales(amount) values (1), (3), (null)");
            try (ResultSet rows = statement.executeQuery(sql)) {
                assertTrue(rows.next());
                assertEquals(1.0, rows.getDouble("dp"), 1e-10);
                assertEquals(Math.sqrt(2), rows.getDouble("ds"), 1e-10);
                assertEquals(1.0, rows.getDouble("vp"), 1e-10);
                assertEquals(2.0, rows.getDouble("vs"), 1e-10);
            }
            statement.execute("delete from sales where amount = 3");
            try (ResultSet rows = statement.executeQuery(sql)) {
                assertTrue(rows.next());
                assertEquals(0.0, rows.getDouble("dp"));
                assertEquals(0.0, rows.getDouble("vp"));
                assertNull(rows.getObject("ds"));
                assertNull(rows.getObject("vs"));
            }
            statement.execute("delete from sales");
            try (ResultSet rows = statement.executeQuery(sql)) {
                assertTrue(rows.next());
                for (int column = 1; column <= 4; column++) assertNull(rows.getObject(column));
            }
        }
    }

    private static FormAggregatePlanner planner(RdbDialect dialect, FieldUsePolicy policy) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                policy,
                QueryShapeLimits.defaults());
    }
}
