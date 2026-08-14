package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 Oracle、SQL Server 驱动最常见的标量差异在字段边界被收口，不把 NUMBER(1)、Timestamp 等
 * 驱动对象泄漏给动态表单业务代码。
 */
class DialectScalarValueCodecTest {

    private static final ValueCodecRegistry VALUE_CODECS = ValueCodecRegistry.standard();

    @Test
    void writesBooleanInTheShapeExpectedByEachDialectVersion() {
        assertEquals(1,
                     DialectScalarValueCodec.write(true, "BOOLEAN", "oracle", false, VALUE_CODECS));
        assertEquals(false,
                     DialectScalarValueCodec.write(false, "BOOLEAN", "oracle", true, VALUE_CODECS));
        assertEquals(true,
                     DialectScalarValueCodec.write(true, "BOOLEAN", "sqlserver", false, VALUE_CODECS));
        assertEquals(Integer.class,
                     DialectScalarValueCodec.parameterType("BOOLEAN", "oracle", false));
        assertEquals(Boolean.class,
                     DialectScalarValueCodec.parameterType("BOOLEAN", "sqlserver", false));
    }

    @Test
    void normalizesDriverNumbersAndTimestampsForDynamicFormRows() {
        assertEquals(true,
                     DialectScalarValueCodec.read(BigDecimal.ONE, "BOOLEAN", VALUE_CODECS));
        assertEquals(false,
                     DialectScalarValueCodec.read(BigDecimal.ZERO, "BOOLEAN", VALUE_CODECS));

        Object timestamp = DialectScalarValueCodec.read(
                Timestamp.valueOf("2026-08-01 12:30:45"), "TIMESTAMP", VALUE_CODECS);
        assertInstanceOf(LocalDateTime.class, timestamp);
        assertEquals(LocalDateTime.of(2026, 8, 1, 12, 30, 45), timestamp);
    }

    /** 验证调用方传入的无效类型名不会进入公开错误消息。 */
    @Test
    void rejectsInvalidOffsetTimeTypeWithoutEchoingCallerInput() {
        String callerType = "secret_" + "x".repeat(8_192);

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> OffsetTimeValueCodec.parameterType(callerType, "postgresql"));

        assertEquals("data type is not OFFSET_TIME", failure.getMessage());
    }
}
