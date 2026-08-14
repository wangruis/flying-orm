package com.flying.orm.core.codec;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 覆盖共享值 codec 的精确数值、时间、枚举和二进制转换边界。 */
class ValueCodecRegistryTest {

    @Test
    void writesEnumAsStableName() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertEquals("ACTIVE", registry.write(Status.ACTIVE));
    }

    @Test
    void writesBinaryValueWithoutCopying() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        byte[] bytes = new byte[]{1, 2, 3};

        assertSame(bytes, registry.write(bytes));
    }

    /** 验证写入路径按运行时 ByteBuffer 实现类查找 codec 时仍接受普通二进制参数。 */
    @Test
    void writesByteBufferWithoutRequiringItsImplementationClass() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        ByteBuffer value = ByteBuffer.wrap(new byte[]{1, 2, 3});

        assertSame(value, registry.write(value));
    }

    @Test
    void convertsBinaryValuesWithoutMovingTheSourceBuffer() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{9, 1, 2, 3});
        buffer.position(1);

        assertArrayEquals(new byte[]{1, 2, 3}, registry.read(buffer, byte[].class));
        assertEquals(1, buffer.position());
        assertEquals(ByteBuffer.wrap(new byte[]{4, 5}),
                     registry.read(new byte[]{4, 5}, ByteBuffer.class));
    }

    /** 实体允许声明 boxed byte 数组时，写入和读回必须与 primitive byte 数组语义一致。 */
    @Test
    void convertsBoxedBinaryArraysInBothDirections() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) registry.write(new Byte[]{1, 2, 3}));
        assertArrayEquals(new Byte[]{4, 5}, registry.read(new byte[]{4, 5}, Byte[].class));
        assertEquals("boxed binary value must not contain null",
                     assertThrows(IllegalArgumentException.class,
                                  () -> registry.write(new Byte[]{1, null})).getMessage());
    }

    /** byte[] 解码为 ByteBuffer 后必须与驱动或调用方仍持有的源数组隔离。 */
    @Test
    void copiesByteArrayWhenDecodingToByteBuffer() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        byte[] databaseValue = {4, 5};

        ByteBuffer decoded = registry.read(databaseValue, ByteBuffer.class);
        databaseValue[0] = 9;

        assertEquals(4, decoded.get(0));
    }

    /** 不能承诺构造实际无法返回的 ByteBuffer 子类，避免泛型调用点发生类型混淆。 */
    @Test
    void rejectsByteBufferSubtypeItCannotCreate() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertThrows(IllegalArgumentException.class,
                     () -> registry.read(new byte[]{4, 5}, MappedByteBuffer.class));
    }

    @Test
    void writesCharacterSequencesAsPlainTextWithoutTrimming() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertEquals("  large text  ", registry.write(new StringBuilder("  large text  ")));
    }

    /** Character/char 是合法实体标量，必须作为单字符文本完成双向转换。 */
    @Test
    void convertsCharactersAsSingleValueText() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertEquals("X", registry.write('X'));
        assertEquals('Y', registry.read("Y", Character.class));
    }

    @Test
    void readsBooleanFromDatabaseFriendlyValues() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertEquals(Boolean.TRUE, registry.read("1", Boolean.class));
        assertEquals(Boolean.TRUE, registry.read(1, Boolean.class));
        assertEquals(Boolean.TRUE, registry.read(new BigInteger("4294967296"), Boolean.class));
        assertEquals(Boolean.TRUE, registry.read(new BigDecimal("0.5"), Boolean.class));
        assertEquals(Boolean.FALSE, registry.read("false", Boolean.class));
    }

    @Test
    void readsNumbersIntoRequestedJavaType() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertEquals(12, registry.read("12", Integer.class));
        assertEquals(99L, registry.read(BigDecimal.valueOf(99), Long.class));
        assertEquals(new BigDecimal("12.30"), registry.read("12.30", BigDecimal.class));
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE), registry.read(Long.MAX_VALUE, BigInteger.class));
    }

    @Test
    void keepsIntegralNumberSubclassesExactBeyondDoublePrecision() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        long value = 9_007_199_254_740_993L;

        assertEquals(value, registry.read(new AtomicLong(value), Long.class));
        assertEquals(BigDecimal.valueOf(value), registry.read(new AtomicLong(value), BigDecimal.class));
    }

    @Test
    void rejectsLossyIntegerConversion() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertThrows(IllegalArgumentException.class, () -> registry.read("12.5", Integer.class));
    }

    /** 有限十进制数超出浮点范围时必须拒绝，不能静默变成数据库中并不存在的无穷大。 */
    @Test
    void rejectsFiniteDecimalOutsideFloatingPointRange() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertThrows(IllegalArgumentException.class,
                     () -> registry.read(new BigDecimal("1E1000"), Double.class));
        assertThrows(IllegalArgumentException.class,
                     () -> registry.read(new BigDecimal("1E1000"), Float.class));
    }

    @Test
    void readsEnumFromName() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertEquals(Status.DISABLED, registry.read("DISABLED", Status.class));
    }

    @Test
    void readsJavaTimeFromSqlAndTextValues() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        LocalDateTime time = LocalDateTime.of(2026, 7, 29, 13, 40, 0);

        assertEquals(LocalDate.of(2026, 7, 29), registry.read("2026-07-29", LocalDate.class));
        assertEquals(time, registry.read(Timestamp.valueOf(time), LocalDateTime.class));
        assertEquals(Instant.parse("2026-07-29T05:40:00Z"),
                     registry.read("2026-07-29T05:40:00Z", Instant.class));
    }

    @Test
    void convertsUuidAndOffsetTimeWithoutLosingDatabaseFriendlyTypes() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        UUID uuid = UUID.fromString("06fb6f53-ae7d-4e2b-8d38-17a72865e726");
        OffsetTime time = OffsetTime.parse("13:40:00+08:00");

        assertSame(uuid, registry.write(uuid));
        assertEquals(uuid, registry.read(uuid.toString(), UUID.class));
        assertSame(time, registry.write(time));
        assertEquals(time, registry.read(time.toString(), OffsetTime.class));
    }

    @Test
    void keepsNullAsNullAndFailsUnknownTargetsExplicitly() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertNull(registry.read(null, Integer.class));
        assertThrows(IllegalArgumentException.class, () -> registry.read("x", UnknownValue.class));
    }

    @Test
    void customCodecCanOverrideBuiltInConversionWithoutChangingTheSharedRegistry() {
        ValueCodec customBoolean = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Boolean.class;
            }

            @Override
            public Object write(Object value) {
                return Boolean.TRUE.equals(value) ? "Y" : "N";
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return "Y".equals(value);
            }
        };

        ValueCodecRegistry customized = ValueCodecRegistry.standard().withFirst(customBoolean);

        assertEquals("Y", customized.write(true));
        assertEquals(Boolean.TRUE, customized.read("Y", Boolean.class));
        assertEquals(Boolean.TRUE, ValueCodecRegistry.standard().write(true));
    }

    @Test
    void conversionFailuresDoNotExposeInputValuesThroughMessagesOrCauses() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();

        assertDoesNotExpose(registry, "secret-date-token", LocalDate.class, "secret-date-token");
        assertDoesNotExpose(registry, "secret-uuid-token", UUID.class, "secret-uuid-token");
        assertDoesNotExpose(registry, "secret-enum-token", Status.class, "secret-enum-token");
        assertDoesNotExpose(registry, "99999", Short.class, "99999");
    }

    /** 内置 codec 自己包装失败时也必须从完整异常图恢复 fatal，不能让后续安全分类永久丢失原对象。 */
    @Test
    void builtInCodecsPropagateNestedVirtualMachineErrors() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        OutOfMemoryError uuidFatal = new OutOfMemoryError("uuid fatal");
        OutOfMemoryError enumFatal = new OutOfMemoryError("enum fatal");
        OutOfMemoryError timeFatal = new OutOfMemoryError("time fatal");

        Object uuidValue = failingText(new IllegalArgumentException("wrapper", uuidFatal));
        Object enumValue = failingText(new IllegalArgumentException("wrapper", enumFatal));
        DateTimeParseException timeFailure = new DateTimeParseException("wrapper", "", 0, timeFatal);
        Object timeValue = failingText(timeFailure);

        assertSame(uuidFatal, assertThrows(OutOfMemoryError.class, () -> registry.read(uuidValue, UUID.class)));
        assertSame(enumFatal, assertThrows(OutOfMemoryError.class, () -> registry.read(enumValue, Status.class)));
        assertSame(timeFatal, assertThrows(OutOfMemoryError.class, () -> registry.read(timeValue, LocalDate.class)));
    }

    /** 精确数值转换必须穿透 suppressed/cause 环并保持其中 fatal 的原对象身份。 */
    @Test
    void exactNumberConversionPropagatesSuppressedFatalFromCyclicGraph() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        OutOfMemoryError fatal = new OutOfMemoryError("numeric fatal");
        ArithmeticException exactFailure = new ArithmeticException("exact wrapper");
        IllegalStateException first = new IllegalStateException("first wrapper");
        IllegalStateException second = new IllegalStateException("second wrapper");
        first.initCause(second);
        second.addSuppressed(first);
        second.addSuppressed(fatal);
        exactFailure.addSuppressed(first);
        BigDecimal failingNumber = new BigDecimal("1") {
            @Override
            public int intValueExact() {
                throw exactFailure;
            }
        };

        assertSame(fatal,
                   assertThrows(OutOfMemoryError.class, () -> registry.read(failingNumber, Integer.class)));
    }

    private static Object failingText(RuntimeException failure) {
        return new Object() {
            @Override
            public String toString() {
                throw failure;
            }
        };
    }

    private static void assertDoesNotExpose(ValueCodecRegistry registry,
                                            Object value,
                                            Class<?> targetType,
                                            String secret) {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> registry.read(value, targetType));
        for (Throwable current = error; current != null; current = current.getCause()) {
            assertFalse(String.valueOf(current.getMessage()).contains(secret));
        }
    }

    private enum Status {
        ACTIVE,
        DISABLED
    }

    private record UnknownValue(String value) {
    }
}
