package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.type.LogicalType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class AggregateResultTypeReuseTest {

    @Test
    void countVariantsReuseTheirFixedTypeButRetainIndependentAliases() throws ReflectiveOperationException {
        DynamicField source = DynamicField.of("id", "BIGINT UNSIGNED");
        DynamicField rows = resultField(AggregateExpression.countRows("rows"), null);
        DynamicField count = resultField(AggregateExpression.count("id", "nonnull"), source);
        DynamicField distinct = resultField(AggregateExpression.countDistinct("id", "unique_ids"), source);
        assertEquals("BIGINT", rows.dataType());
        assertSame(rows.databaseType(), count.databaseType());
        assertSame(rows.databaseType(), distinct.databaseType());
        assertNotSame(rows, count);
        assertEquals("nonnull", count.name());
        assertEquals("unique_ids", distinct.name());
    }

    @Test
    void sumAndAverageReuseUnconstrainedDecimalWithoutInheritingSourcePrecision() throws ReflectiveOperationException {
        DynamicField source = DynamicField.of("amount", "NUMERIC(12,2)");
        DynamicField sum = resultField(AggregateExpression.sum("amount", "total"), source);
        DynamicField average = resultField(AggregateExpression.avg("amount", "average"), source);
        assertEquals("DECIMAL", sum.dataType());
        assertSame(sum.databaseType(), average.databaseType());
        assertNotSame(source.databaseType(), sum.databaseType());
        assertEquals("total", sum.name());
        assertEquals("average", average.name());
    }

    @Test
    void extremaKeepTheExactSourceType() throws ReflectiveOperationException {
        DynamicField source = DynamicField.of("amount", "NUMERIC(12,2)");
        DynamicField minimum = resultField(
                AggregateExpression.min("amount", "minimum", LogicalType.DECIMAL, BigDecimal.class), source);
        DynamicField maximum = resultField(
                AggregateExpression.max("amount", "maximum", LogicalType.DECIMAL, BigDecimal.class), source);
        assertSame(source.databaseType(), minimum.databaseType());
        assertSame(source.databaseType(), maximum.databaseType());
    }

    // Observe internal metadata identity without exposing a test-only production API or imposing a byte budget.
    private static DynamicField resultField(AggregateExpression<?> expression, DynamicField source)
            throws ReflectiveOperationException {
        Method factory = FormAggregatePlanner.class.getDeclaredMethod(
                "resultField", AggregateExpression.class, DynamicField.class);
        factory.setAccessible(true);
        return (DynamicField) factory.invoke(null, expression, source);
    }
}
