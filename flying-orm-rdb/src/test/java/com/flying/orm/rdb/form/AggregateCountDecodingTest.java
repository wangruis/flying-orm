package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateCountDecodingTest {
    private static volatile AggregateRow allocationSink;

    @Test
    void decodedImmutableValuesKeepTheirOwnedContainer() throws ReflectiveOperationException {
        AggregateResultDecoder decoder = decoder(AggregateExpression.countRows("total"));
        DynamicRow raw = DynamicRow.copyOf(Collections.singletonMap("total", 7L));
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean)
                ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        for (int index = 0; index < 100_000; index++) allocationSink = decoder.decode(raw);
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < 100_000; index++) allocationSink = decoder.decode(raw);
        System.out.println("aggregate-count-decode-bytes-per-row="
                + (allocation.getThreadAllocatedBytes(thread) - before) / 100_000.0);
        Field storage = AggregateRow.class.getDeclaredField("values");
        storage.setAccessible(true);
        assertTrue(OwnedBindableValues.isPublished((List<?>) storage.get(allocationSink)));
        assertEquals(7L, allocationSink.get("total", Long.class));
    }

    @Test
    void countVariantsReuseAlreadyExactLongValues() {
        for (AggregateExpression<Long> expression : counts()) {
            AggregateResultDecoder decoder = decoder(expression);
            for (Long value : List.of(10_000L, Long.MIN_VALUE, Long.MAX_VALUE)) {
                assertSame(value, decoded(decoder, expression, value));
            }
        }
    }

    @Test
    void preservesExactIntegralConversionsAndNull() {
        AggregateExpression<Long> expression = AggregateExpression.countRows("total");
        AggregateResultDecoder decoder = decoder(expression);
        for (Object value : List.of((byte) 7, (short) 7, 7, 7L, BigInteger.valueOf(7),
                new BigDecimal("7.000"), 7.0D, 7.0F, "7")) {
            assertEquals(7L, decoded(decoder, expression, value));
        }
        assertEquals((long) Integer.MIN_VALUE, decoded(decoder, expression, Integer.MIN_VALUE));
        assertEquals((long) Integer.MAX_VALUE, decoded(decoder, expression, Integer.MAX_VALUE));
        assertNull(decoded(decoder, expression, null));
    }

    @Test
    void stillRejectsFractionalOverflowAndNonFiniteNumbers() {
        AggregateExpression<Long> expression = AggregateExpression.countRows("total");
        AggregateResultDecoder decoder = decoder(expression);
        for (Object value : List.of(new BigDecimal("1.5"), BigInteger.ONE.shiftLeft(63),
                BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
                new BigDecimal("9223372036854775808"), 1.5D, Double.NaN,
                Double.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, "1.5", "9223372036854775808")) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> decoded(decoder, expression, value));
            assertEquals("count result must be an exact long integer", failure.getMessage());
        }
    }

    private static List<AggregateExpression<Long>> counts() {
        return List.of(AggregateExpression.countRows("total"), AggregateExpression.count("id", "total"),
                AggregateExpression.countDistinct("id", "total"));
    }

    private static Long decoded(AggregateResultDecoder decoder, AggregateExpression<Long> expression, Object value) {
        return decoder.decode(DynamicRow.copyOf(Collections.singletonMap("total", value))).get(expression);
    }

    private static AggregateResultDecoder decoder(AggregateExpression<Long> expression) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormAggregatePlanner planner = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        return new AggregateResultDecoder(planner.plan(AggregateSpec.builder(
                QuerySpec.of(form, ConditionGroup.and().build())).aggregate(expression).build()));
    }
}
