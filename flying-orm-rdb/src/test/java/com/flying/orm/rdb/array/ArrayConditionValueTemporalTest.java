package com.flying.orm.rdb.array;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** PostgreSQL 数组条件移除元素精度时必须保留时区语义。 */
class ArrayConditionValueTemporalTest {

    @Test
    void preservesTimestampTimezoneSuffixAfterRemovingPrecision() {
        OffsetDateTime value = OffsetDateTime.of(2026, 8, 22, 12, 34, 56, 0, ZoneOffset.ofHours(8));
        ArrayConditionValue condition = new ArrayConditionValue(
                List.of(value), "TIMESTAMP(3) WITH TIME ZONE[]");

        assertEquals("timestamp with time zone[]", condition.postgresqlCastType());
    }

    @Test
    void preservesTimeTimezoneSuffixAfterRemovingPrecision() {
        OffsetTime value = OffsetTime.of(12, 34, 56, 0, ZoneOffset.ofHours(8));
        ArrayConditionValue condition = new ArrayConditionValue(
                List.of(value), "TIME(3) WITH TIME ZONE[]");

        assertEquals("time with time zone[]", condition.postgresqlCastType());

        ArrayConditionValue metadataCondition = new ArrayConditionValue(
                List.of(value), "TIMETZ(3)[]");
        assertEquals("time with time zone[]", metadataCondition.postgresqlCastType());
    }

    @Test
    void acceptsTheLogicalOffsetTimeArrayType() {
        OffsetTime value = OffsetTime.of(12, 34, 56, 123_456_000, ZoneOffset.ofHours(8));
        ArrayConditionValue condition = new ArrayConditionValue(List.of(value), "OFFSET_TIME[]");

        assertEquals("time with time zone[]", condition.postgresqlCastType());
        assertEquals(value, ((OffsetTime[]) condition.parameter())[0]);
    }

    @Test
    void keepsPrecisionQualifiedLocalTimestampLocal() {
        ArrayConditionValue condition = new ArrayConditionValue(
                List.of(LocalDateTime.of(2026, 8, 22, 12, 34, 56)), "TIMESTAMP(3) WITHOUT TIME ZONE[]");

        assertEquals("timestamp[]", condition.postgresqlCastType());
    }

    @Test
    void keepsTheLegacyStringDataTypeAccessorForBinaryCompatibility() throws Exception {
        var accessor = ArrayConditionValue.class.getMethod("dataType");
        assertEquals(String.class, accessor.getReturnType());
        ArrayConditionValue condition = new ArrayConditionValue(List.of(1), "INTEGER[]");
        assertEquals("INTEGER[]", accessor.invoke(condition));
    }
}
