package com.flying.orm.rdb.codec;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 覆盖一维 SQL Array 的强类型绑定、实体读取和嵌套数组拒绝规则。 */
class ArrayValueCodecTest {

    @Test
    void convertsArrayValuesForDriverDynamicMapAndEntityTargets() {
        assertTrue(ArrayValueCodec.isArrayDataType("BIGINT[]"));
        assertEquals(Long[].class, ArrayValueCodec.parameterType("BIGINT[]"));

        Object driverValue = ArrayValueCodec.write(List.of(1, 2L), "BIGINT[]");
        assertInstanceOf(Long[].class, driverValue);
        assertArrayEquals(new Long[]{1L, 2L}, (Long[]) driverValue);

        assertEquals(List.of("alpha", "beta"),
                     ArrayValueCodec.read(new String[]{"alpha", "beta"}));
        assertArrayEquals(new Long[]{3L, 4L},
                          (Long[]) ArrayValueCodec.read(new Integer[]{3, 4}, Long[].class));
    }

    /** 带时区 SQL 数组必须保留 Java 偏移类型，不能在绑定前降级成本地时间。 */
    @Test
    void preservesOffsetTypesForTimeZoneArrays() {
        OffsetTime time = OffsetTime.parse("10:15:30+08:00");
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-08-12T10:15:30+08:00");

        assertEquals(OffsetTime[].class, ArrayValueCodec.parameterType("TIME WITH TIME ZONE[]"));
        assertEquals(OffsetDateTime[].class,
                     ArrayValueCodec.parameterType("TIMESTAMP WITH TIME ZONE[]"));
        assertArrayEquals(new OffsetTime[]{time},
                          (OffsetTime[]) ArrayValueCodec.write(List.of(time), "TIME WITH TIME ZONE[]"));
        assertArrayEquals(new OffsetDateTime[]{timestamp},
                          (OffsetDateTime[]) ArrayValueCodec.write(
                                  List.of(timestamp), "TIMESTAMP WITH TIME ZONE[]"));
    }

    /** PostgreSQL 元数据使用的内部类型名也必须映射为驱动要求的精确 Java 组件类型。 */
    @Test
    void usesExactDriverTypesForPostgresqlArrayMetadataNames() {
        OffsetTime time = OffsetTime.parse("10:15:30+08:00");
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-08-12T10:15:30+08:00");

        assertEquals(Short[].class, ArrayValueCodec.parameterType("int2[]"));
        assertEquals(Float[].class, ArrayValueCodec.parameterType("float4[]"));
        assertEquals(OffsetTime[].class, ArrayValueCodec.parameterType("timetz[]"));
        assertEquals(OffsetDateTime[].class, ArrayValueCodec.parameterType("timestamptz[]"));
        assertEquals(String[].class, ArrayValueCodec.parameterType("bpchar[]"));
        assertArrayEquals(new Short[]{2}, (Short[]) ArrayValueCodec.write(List.of(2), "int2[]"));
        assertArrayEquals(new Float[]{1.5F}, (Float[]) ArrayValueCodec.write(List.of(1.5D), "float4[]"));
        assertArrayEquals(new OffsetTime[]{time},
                          (OffsetTime[]) ArrayValueCodec.write(List.of(time), "timetz[]"));
        assertArrayEquals(new OffsetDateTime[]{timestamp},
                          (OffsetDateTime[]) ArrayValueCodec.write(List.of(timestamp), "timestamptz[]"));
    }

    @Test
    void rejectsNestedAndNonArrayValuesBeforeSqlExecution() {
        assertThrows(IllegalArgumentException.class,
                     () -> ArrayValueCodec.write("not-an-array", "VARCHAR[]"));
        assertThrows(IllegalArgumentException.class,
                     () -> ArrayValueCodec.write(List.of(List.of("nested")), "VARCHAR[]"));
    }
}
