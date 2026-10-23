package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
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
import com.flying.orm.rdb.mapping.RowMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

class AggregateDecoderVisibilityTest {
    private static final DynamicForm FORM = DynamicForm.builder("sales", "sales")
            .addField(DynamicField.of("Name", "VARCHAR"))
            .addField(DynamicField.of("Amount", "DECIMAL"))
            .addField(DynamicField.of("Weight", "INTEGER"))
            .masked("Name", MaskedFieldDefinition.builder("full").build()).build();

    @Test
    void noPerValueScalarPlanFactoryRemainsInTheRenderer() {
        assertTrue(Arrays.stream(FormDataSqlRenderer.class.getDeclaredMethods())
                .noneMatch(method -> method.getName().equals("readScalarValue")),
                "aggregate fallback must not keep a per-value scalar plan factory");
    }

    @Test
    void columnReadRulesArePreparedOnceForTheResultSet() throws Exception {
        AggregateResultDecoder decoder = decoder(FieldUsePolicy.unrestricted());
        Field field = assertDoesNotThrow(() -> AggregateResultDecoder.class.getDeclaredField("readers"),
                "aggregate result decoding must prepare column readers once, not resolve field types per row");
        field.setAccessible(true);
        Object readers = field.get(decoder);
        assertNotNull(readers);
        for (int index = 1; index <= 3; index++) {
            assertEquals(BigDecimal.valueOf(index), decoder.decode(row("secret", index)).get("total", BigDecimal.class));
            assertSame(readers, field.get(decoder));
        }
    }

    @Test
    void boundColumnsUseTheSameDecodingAndVisibilityWithoutRetainingTheValueReader() {
        for (FieldUsePolicy policy : List.of(FieldUsePolicy.unrestricted(), policy(FieldVisibility.HIDDEN))) {
            AggregateResultDecoder decoder = decoder(policy);
            RowMapper<?> mapper = assertInstanceOf(RowMapper.class, decoder);
            Function<IntFunction<Object>, ?> bound = mapper.bindColumns(row("secret", 1));
            assertNotNull(bound, "aggregate rows must use the existing executor layout binding");
            Object[] values = {"secret", 1, 10, 1};
            AggregateRow first = (AggregateRow) bound.apply(index -> values[index]);
            assertEquals(decoder.decode(row("secret", 1)).values(), first.values());
            values[1] = 2;
            values[2] = 20;
            values[3] = 2;
            AggregateRow second = (AggregateRow) bound.apply(index -> values[index]);
            assertEquals(decoder.decode(row("secret", 2)).values(), second.values());
            assertEquals(BigDecimal.ONE, first.get("total", BigDecimal.class),
                    "the executor reuses its value buffer, not published aggregate results");
            assertThrows(IllegalArgumentException.class,
                    () -> mapper.bindColumns(DynamicRow.copyOf(Map.of("wrong", 1))));
        }
    }

    @Test
    void ordinaryCountsAndSumsShareStatelessReadersWithoutCapturingThePlan() throws Exception {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .countRows("rows").count("Amount", "nonnull").countDistinct("Amount", "distinct")
                .sum("Amount", "total").avg("Amount", "average").build();
        FormAggregatePlanner.Plan plan = planner(FieldUsePolicy.unrestricted()).plan(spec);
        AggregateResultDecoder first = new AggregateResultDecoder(plan);
        AggregateResultDecoder second = new AggregateResultDecoder(plan);
        List<?> firstReaders = (List<?>) readers(first);
        List<?> secondReaders = (List<?>) readers(second);
        assertSame(firstReaders.get(0), firstReaders.get(1));
        assertSame(firstReaders.get(1), firstReaders.get(2));
        assertSame(firstReaders.get(3), firstReaders.get(4));
        for (int index = 0; index < firstReaders.size(); index++) {
            assertSame(firstReaders.get(index), secondReaders.get(index));
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (String alias : List.of("rows", "nonnull", "distinct", "total", "average")) {
            values.put(alias, null);
        }
        assertEquals(Arrays.asList(null, null, null, null, null), first.decode(DynamicRow.copyOf(values)).values());
    }

    @Test
    void governedVisibilityIsResolvedOncePerDecoderAndReusedAcrossRows() throws Exception {
        AggregateResultDecoder decoder = decoder(policy(FieldVisibility.HIDDEN));
        Object readers = readers(decoder);
        for (int index = 1; index <= 3; index++) {
            AggregateRow row = decoder.decode(row("secret", index));
            assertEquals(Arrays.asList("******", BigDecimal.valueOf(index), null, (long) index), row.values());
            assertSame(readers, readers(decoder));
        }
    }

    @Test
    void separateRequestsDoNotReusePermissionDecisions() throws Exception {
        AggregateResultDecoder hidden = decoder(policy(FieldVisibility.HIDDEN));
        AggregateResultDecoder visible = decoder(policy(FieldVisibility.FULL));
        assertNotSame(readers(hidden), readers(visible));
        assertNull(hidden.decode(row("secret", 2)).get("weighted", BigDecimal.class));
        assertEquals(new BigDecimal("20"), visible.decode(row("secret", 2)).get("weighted", BigDecimal.class));
        assertNull(hidden.decode(row("secret", 3)).get("weighted", BigDecimal.class));
    }

    @Test
    void visibilityDoesNotNeedAParallelPerColumnArray() {
        AggregateResultDecoder decoder = decoder(FieldUsePolicy.unrestricted());
        assertTrue(Arrays.stream(AggregateResultDecoder.class.getDeclaredFields())
                .noneMatch(field -> field.getType() == FieldVisibility[].class));
        assertEquals(new BigDecimal("20"), decoder.decode(row("secret", 2)).get("weighted", BigDecimal.class));
    }

    @Test
    void missingMaskStillFailsOnRowDeliveryNotOnDecoderCreation() {
        DynamicForm plain = DynamicForm.builder("plain", "plain")
                .addField(DynamicField.of("name", "VARCHAR")).build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(plain, ConditionGroup.and().build()))
                .group("name", "name").countRows("rows").build();
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("name", FieldUse.GROUP, FieldUse.PROJECT)
                .visibility("name", FieldVisibility.MASKED).build();
        AggregateResultDecoder decoder = new AggregateResultDecoder(planner(policy).plan(spec));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("name", null);
        values.put("rows", 1L);
        assertThrows(ScopeAccessException.class, () -> decoder.decode(DynamicRow.copyOf(values)));
    }

    private static FieldUsePolicy policy(FieldVisibility weight) {
        return FieldUsePolicy.builder().allow("Name", FieldUse.GROUP, FieldUse.PROJECT)
                .visibility("Name", FieldVisibility.MASKED)
                .allow("Amount", FieldUse.AGGREGATE).visibility("Amount", FieldVisibility.FULL)
                .allow("Weight", FieldUse.AGGREGATE).visibility("Weight", weight).build();
    }

    private static AggregateResultDecoder decoder(FieldUsePolicy policy) {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .group("Name", "name").sum("Amount", "total")
                .aggregate(AggregateExpression.sum(ValueExpression.field("Amount")
                        .multiply(ValueExpression.field("Weight")), "weighted"))
                .countRows("rows").build();
        return new AggregateResultDecoder(planner(policy).plan(spec));
    }

    private static FormAggregatePlanner planner(FieldUsePolicy policy) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(),
                RdbDialect.postgresql());
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                policy,
                QueryShapeLimits.defaults());
    }

    private static DynamicRow row(String name, int number) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("name", name);
        values.put("total", number);
        values.put("weighted", number * 10);
        values.put("rows", number);
        return DynamicRow.copyOf(values);
    }

    // Inspect ownership without exposing mutable layout state through a production API.
    private static Object readers(AggregateResultDecoder decoder) throws Exception {
        Field field = AggregateResultDecoder.class.getDeclaredField("readers");
        field.setAccessible(true);
        return field.get(decoder);
    }
}
