package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.DriverValueAdapter;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 绝对时间与本地日期时间的原生类型边界。 */
class NativeTemporalTypeTest {

    @Test
    void adaptsLegacyJdbcTemporalValuesAtTheScalarDriverBoundary() {
        LocalDate date = LocalDate.of(2026, 8, 30);
        LocalTime time = LocalTime.of(10, 11, 12);
        LocalDateTime localTimestamp = LocalDateTime.of(2026, 8, 30, 10, 11, 12, 123_456_789);
        Instant absoluteTimestamp = Instant.parse("2026-08-30T10:11:12.123456789Z");
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();

        assertEquals(date, DialectScalarValueCodec.read(java.sql.Date.valueOf(date), "DATE", codecs));
        assertEquals(time, DialectScalarValueCodec.read(Time.valueOf(time), "TIME", codecs));
        assertEquals(localTimestamp, DialectScalarValueCodec.read(
                Timestamp.valueOf(localTimestamp), "TIMESTAMP(9)", codecs));
        assertEquals(absoluteTimestamp.atOffset(ZoneOffset.UTC), DialectScalarValueCodec.read(
                Timestamp.from(absoluteTimestamp), "TIMESTAMPTZ(9)", codecs));
    }

    @Test
    void preservesApplicationCodecPriorityForLegacyJdbcTemporalValues() {
        Timestamp source = Timestamp.valueOf("2026-08-30 10:11:12.123456789");
        AtomicReference<Object> observed = new AtomicReference<>();
        ValueCodec custom = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == LocalDateTime.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                observed.set(value);
                return ((Timestamp) value).toLocalDateTime().plusSeconds(1);
            }
        };

        Object decoded = DialectScalarValueCodec.read(
                source, "TIMESTAMP(9)", ValueCodecRegistry.standard().withFirst(custom));

        assertSame(source, observed.get());
        assertEquals(source.toLocalDateTime().plusSeconds(1), decoded);
    }

    @Test
    void preservesRegisteredDriverAdapterPriorityForLegacyJdbcTemporalValues() {
        Timestamp source = Timestamp.valueOf("2026-08-30 10:11:12.123456789");
        AtomicInteger adaptations = new AtomicInteger();
        DriverValueAdapter custom = new DriverValueAdapter() {
            @Override
            public boolean supports(Object value) {
                return value instanceof Timestamp;
            }

            @Override
            public Object unwrap(Object value) {
                adaptations.incrementAndGet();
                return ((Timestamp) value).toLocalDateTime().plusSeconds(2);
            }
        };

        Object decoded = DialectScalarValueCodec.read(
                source, "TIMESTAMP(9)", ValueCodecRegistry.standard().withDriverAdapter(custom));

        assertEquals(1, adaptations.get());
        assertEquals(source.toLocalDateTime().plusSeconds(2), decoded);
    }

    @Test
    void preservesBareMysqlTinyintOneAsAnIntegerValue() {
        assertTrue(DialectScalarValueCodec.supports("TINYINT(1)"));
        assertEquals(Integer.class,
                     DialectScalarValueCodec.parameterType("TINYINT(1)", "mysql", true));
        assertEquals(2, DialectScalarValueCodec.write(
                2, "TINYINT(1)", "mysql", true, ValueCodecRegistry.standard()));
        assertEquals(-1, DialectScalarValueCodec.write(
                -1, "TINYINT(1)", "mysql", true, ValueCodecRegistry.standard()));
        assertEquals(1, DialectScalarValueCodec.read(
                1, "TINYINT(1)", ValueCodecRegistry.standard()));
        assertThrows(IllegalArgumentException.class,
                     () -> DialectScalarValueCodec.read(
                             Boolean.TRUE, "TINYINT(1)", ValueCodecRegistry.standard()));
        assertFalse(DialectScalarValueCodec.supports("BIT(1)"));
        assertEquals(Boolean.class,
                     DialectScalarValueCodec.parameterType("BIT(1)", "mysql", true));
        assertEquals(1, DialectScalarValueCodec.read(
                1, "BIT(1)", ValueCodecRegistry.standard()));
        assertFalse(DialectScalarValueCodec.supports("BIT(8)"));
        assertEquals(Object.class,
                     DialectScalarValueCodec.parameterType("BIT(8)", "mysql", true));
        assertEquals(Boolean.class,
                     DialectScalarValueCodec.parameterType("BOOL", "mysql", true));
        assertEquals(Integer.class,
                     DialectScalarValueCodec.parameterType("TINYINT(1) UNSIGNED", "mysql", true));
        assertEquals(255, DialectScalarValueCodec.read(
                (short) 255, "TINYINT(1) UNSIGNED", ValueCodecRegistry.standard()));
        assertEquals(Integer.class,
                     DialectScalarValueCodec.parameterType("SMALLINT UNSIGNED", "mysql", true));
        assertEquals(65_535, DialectScalarValueCodec.read(
                65_535, "SMALLINT UNSIGNED", ValueCodecRegistry.standard()));
        assertEquals(Integer.class,
                     DialectScalarValueCodec.parameterType("MEDIUMINT UNSIGNED", "mysql", true));
        assertTrue(DialectScalarValueCodec.supports("INT(11) UNSIGNED"));
        assertEquals(Long.class,
                     DialectScalarValueCodec.parameterType("INT(11) UNSIGNED", "mysql", true));
        assertTrue(DialectScalarValueCodec.supports("INT(11) UNSIGNED ZEROFILL"));
        assertEquals(Long.class,
                     DialectScalarValueCodec.parameterType("INT(11) UNSIGNED ZEROFILL", "mysql", true));
        assertEquals(Long.class,
                     DialectScalarValueCodec.parameterType("INT(11) ZEROFILL", "mysql", true));
        assertEquals(BigInteger.class,
                     DialectScalarValueCodec.parameterType("BIGINT UNSIGNED", "mysql", true));
        assertEquals(4_294_967_295L, DialectScalarValueCodec.read(
                4_294_967_295L, "INT UNSIGNED", ValueCodecRegistry.standard()));
        BigInteger unsignedBigintMax = new BigInteger("18446744073709551615");
        assertEquals(unsignedBigintMax, DialectScalarValueCodec.read(
                unsignedBigintMax, "BIGINT UNSIGNED", ValueCodecRegistry.standard()));
    }

    @Test
    void keepsMysqlBooleanAliasesOutOfPostgresqlBitStrings() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();

        assertEquals(Boolean.class,
                     DialectScalarValueCodec.parameterType("BIT(1)", "mysql", true));
        assertEquals(Object.class,
                     DialectScalarValueCodec.parameterType("BIT(1)", "postgresql", true));
        assertEquals(Integer.class,
                     DialectScalarValueCodec.parameterType("TINYINT(1)", "postgresql", true));
        assertEquals("1", DialectScalarValueCodec.write("1", "BIT(1)", "postgresql", true, codecs));
        assertEquals(1, DialectScalarValueCodec.read(1, "BIT(1)", codecs));
    }

    @Test
    void infersAbsoluteAndLocalTemporalEntityTypesWithoutMixingTheirSemantics() {
        var metadata = EntityMetadataResolver.createUncached(TemporalEntity.class);

        assertEquals("TIMESTAMPTZ", metadata.field("createdAt").dataType());
        assertEquals("TIMESTAMPTZ", metadata.field("observedAt").dataType());
        assertEquals("DATE", metadata.field("businessDate").dataType());
        assertEquals("TIME", metadata.field("businessTime").dataType());
        assertEquals("TIMESTAMP", metadata.field("localOccurrence").dataType());
    }

    @Test
    void mapsAbsoluteTimeToEachBuiltInDialectNativeType() {
        assertEquals("TIMESTAMP WITH TIME ZONE", RdbDialect.h2().schema().dataType("TIMESTAMPTZ"));
        assertEquals("TIMESTAMP(6)", RdbDialect.mysql().schema().dataType("TIMESTAMPTZ"));
        assertEquals("TIMESTAMP(3)", RdbDialect.mysql().schema().dataType("TIMESTAMPTZ", null, 3, null));
        assertEquals("TIMESTAMPTZ", RdbDialect.postgresql().schema().dataType("TIMESTAMPTZ"));
        assertEquals("TIMESTAMP WITH TIME ZONE", RdbDialect.oracle().schema().dataType("TIMESTAMPTZ"));
        assertEquals("DATETIMEOFFSET", RdbDialect.sqlServer().schema().dataType("TIMESTAMPTZ"));
    }

    @Test
    void bindsInstantAtTheNativeDriverBoundaryWithoutChangingTheInstant() {
        Instant instant = Instant.parse("2026-08-21T03:04:05.123456Z");

        Object postgresBound = DialectScalarValueCodec.write(
                instant, "TIMESTAMPTZ", "postgresql", true, ValueCodecRegistry.standard());
        Object mysqlBound = DialectScalarValueCodec.write(
                instant, "TIMESTAMPTZ", "mysql", true, ValueCodecRegistry.standard());

        assertEquals(instant.atOffset(ZoneOffset.UTC), postgresBound);
        assertEquals(LocalDateTime.ofInstant(instant, ZoneOffset.UTC), mysqlBound);
        assertEquals(OffsetDateTime.class,
                     DialectScalarValueCodec.parameterType("TIMESTAMP WITH TIME ZONE", "oracle", false));
        assertEquals(OffsetDateTime.class,
                     DialectScalarValueCodec.parameterType("DATETIMEOFFSET", "sqlserver", true));
        assertEquals(LocalDateTime.class,
                     DialectScalarValueCodec.parameterType("TIMESTAMPTZ", "mysql", true));
        assertEquals(LocalDateTime.class,
                     DialectScalarValueCodec.parameterType("TIMESTAMP", "postgresql", true));
        assertEquals(LocalDateTime.class,
                     DialectScalarValueCodec.parameterType("ORACLE_DATE", "oracle", false));
    }

    @Test
    void preservesExplicitOffsetWhenTheNativeTypeCanCarryIt() {
        OffsetDateTime value = OffsetDateTime.parse("2026-08-21T11:04:05.123456+08:00");

        assertEquals(value, DialectScalarValueCodec.write(
                value, "TIMESTAMPTZ", "h2", true, ValueCodecRegistry.standard()));
        assertEquals(value, DialectScalarValueCodec.write(
                value, "TIMESTAMP WITH TIME ZONE", "oracle", false, ValueCodecRegistry.standard()));
        assertEquals(value, DialectScalarValueCodec.write(
                value, "DATETIMEOFFSET", "sqlserver", true, ValueCodecRegistry.standard()));
        assertEquals(value, DialectScalarValueCodec.write(
                value, "TIMESTAMPTZ", "postgresql", true, ValueCodecRegistry.standard()));
        assertEquals(LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC), DialectScalarValueCodec.write(
                value, "TIMESTAMPTZ", "mysql", true, ValueCodecRegistry.standard()));
    }

    @Test
    void preservesOffsetSemanticsWhenNativeTemporalTypesDeclarePrecision() {
        assertEquals(OffsetDateTime.class,
                     DialectScalarValueCodec.parameterType("DATETIMEOFFSET(7)", "sqlserver", true));
        assertEquals(OffsetDateTime.class,
                     DialectScalarValueCodec.parameterType("TIMESTAMP(6) WITH TIME ZONE", "oracle", false));
        assertEquals(LocalDateTime.class,
                     DialectScalarValueCodec.parameterType("TIMESTAMP(6) WITH LOCAL TIME ZONE", "oracle", false));
        assertEquals(LocalDateTime.class,
                     DialectScalarValueCodec.parameterType("TIMESTAMP(6)", "postgresql", true));
        assertEquals(OffsetDateTime[].class,
                     ArrayValueCodec.parameterType("TIMESTAMP(6) WITH TIME ZONE[]"));
        assertEquals(OffsetTime[].class,
                     ArrayValueCodec.parameterType("TIMETZ(3)[]"));
        assertEquals(OffsetTime[].class,
                     ArrayValueCodec.parameterType("OFFSET_TIME[]"));
    }

    @Test
    void readsAbsoluteTimeThroughTheStableOffsetDateTimeBoundary() {
        OffsetDateTime driverValue = OffsetDateTime.parse("2026-08-21T11:04:05.123456+08:00");

        Object decoded = DialectScalarValueCodec.read(
                driverValue, "TIMESTAMPTZ", ValueCodecRegistry.standard());

        assertEquals(driverValue, decoded);
        assertEquals(driverValue.toInstant(), ValueCodecRegistry.standard().read(decoded, Instant.class));

        LocalDateTime sessionLocalValue = LocalDateTime.of(2026, 8, 21, 3, 4, 5, 123456000);
        assertThrows(IllegalArgumentException.class, () -> DialectScalarValueCodec.read(
                sessionLocalValue, "TIMESTAMPTZ", ValueCodecRegistry.standard()));
    }

    @Test
    void treatsStandardTimeZoneAliasesAsTheSameRuntimeTemporalTypes() {
        OffsetTime value = OffsetTime.of(12, 34, 56, 123456000, ZoneOffset.ofHours(8));
        String standardOffsetTime = "TIME(6) WITH TIME ZONE";

        assertEquals(OffsetTime.class,
                     OffsetTimeValueCodec.parameterType(standardOffsetTime, "postgresql"));
        assertEquals(value, OffsetTimeValueCodec.write(value, standardOffsetTime, "postgresql"));
        assertEquals(String.class, OffsetTimeValueCodec.parameterType(standardOffsetTime, "mysql"));
        assertEquals(String.class, OffsetTimeValueCodec.parameterType(standardOffsetTime, "oracle"));
        assertEquals(String.class, OffsetTimeValueCodec.parameterType(standardOffsetTime, "sqlserver"));
        assertEquals(value.toString(), OffsetTimeValueCodec.write(value, standardOffsetTime, "mysql"));
        assertEquals(value.toString(), OffsetTimeValueCodec.write(value, "OFFSET_TIME(6)", "oracle"));
        assertEquals(value, OffsetTimeValueCodec.read(value.toString()));

        assertFalse(OffsetTimeValueCodec.isOffsetTimeDataType("TIME(6) WITHOUT TIME ZONE"));
        assertFalse(OffsetTimeValueCodec.isOffsetTimeDataType("TIMETZ(6)"));
        assertEquals(LocalTime.class,
                     DialectScalarValueCodec.parameterType(
                             "TIME(6) WITHOUT TIME ZONE", "postgresql", true));
        assertEquals(LocalDateTime.class,
                     DialectScalarValueCodec.parameterType(
                             "TIMESTAMP(6) WITHOUT TIME ZONE", "postgresql", true));
    }

    @Test
    void usesACompleteTextBoundaryForOracleLocalTime() {
        LocalTime value = LocalTime.of(23, 59, 59, 999_999_999);

        assertEquals("VARCHAR2(18)", RdbDialect.oracle().schema().dataType("TIME"));
        assertEquals("VARCHAR2(18)", RdbDialect.oracle().schema().dataType("TIME(9)"));
        assertEquals("VARCHAR2(18)", RdbDialect.oracle().schema().dataType("TIME(9) WITHOUT TIME ZONE"));
        assertEquals(String.class,
                     DialectScalarValueCodec.parameterType("TIME", "oracle", false));
        assertEquals(String.class,
                     DialectScalarValueCodec.parameterType("TIME(9) WITHOUT TIME ZONE", "oracle", false));
        assertEquals(value.toString(), DialectScalarValueCodec.write(
                value, "TIME", "oracle", false, ValueCodecRegistry.standard()));
        assertEquals(value.toString(), DialectScalarValueCodec.write(
                value, "TIME(9) WITHOUT TIME ZONE", "oracle", false, ValueCodecRegistry.standard()));
        assertEquals(LocalTime.class,
                     DialectScalarValueCodec.parameterType("TIME", "postgresql", true));
    }

    @Test
    void passesOracleIntervalValuesThroughAtTheirNativeDriverTypes() {
        Duration dayToSecond = Duration.ofDays(2).plusNanos(345_000_000);
        Period yearToMonth = Period.of(3, 7, 0);

        assertEquals(dayToSecond, DialectScalarValueCodec.write(
                dayToSecond, "INTERVAL DAY TO SECOND", "oracle", false,
                ValueCodecRegistry.standard()));
        assertEquals(yearToMonth, DialectScalarValueCodec.write(
                yearToMonth, "INTERVAL YEAR TO MONTH", "oracle", false,
                ValueCodecRegistry.standard()));
        assertThrows(IllegalArgumentException.class, () -> DialectScalarValueCodec.write(
                     yearToMonth, "INTERVAL DAY TO SECOND", "oracle", false,
                     ValueCodecRegistry.standard()));
    }

    @Test
    void appliesDomainCodecBeforeSpecialScalarDriverConversion() {
        AtomicInteger writes = new AtomicInteger();
        ValueCodec flagCodec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Flag.class;
            }

            @Override
            public Object write(Object value) {
                writes.incrementAndGet();
                return ((Flag) value).enabled();
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new AssertionError("write conversion must not use the read direction");
            }
        };
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(flagCodec);

        Object parameter = DialectScalarValueCodec.write(
                new Flag(true), "BOOLEAN", "postgresql", true, codecs);

        assertEquals(Boolean.TRUE, parameter);
        assertEquals(1, writes.get());
    }

    private record TemporalEntity(Long id,
                                  Instant createdAt,
                                  OffsetDateTime observedAt,
                                  LocalDate businessDate,
                                  LocalTime businessTime,
                                  LocalDateTime localOccurrence) {
    }

    private record Flag(boolean enabled) {
    }
}
