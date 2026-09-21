package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.codec.DriverValueAdapter;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityTypeMappingTemporalRegistryTest {

    private static final LocalDateTime LOCAL = LocalDateTime.parse("2026-09-21T12:30:15.123456789");
    private static final Instant INSTANT = LOCAL.toInstant(ZoneOffset.UTC);

    @TestFactory
    Stream<DynamicTest> temporalRegistryPaths() {
        return Map.of("core", ValueCodecRegistry.standard(),
                "entity", EntityTypeMappingRegistry.standard().valueCodecs(),
                "descriptor", EntitySchemaDescriptor.builder(Times.class).build().valueCodecs())
                .entrySet().stream().flatMap(entry -> Stream.of(
                        DynamicTest.dynamicTest(entry.getKey() + "/mapping",
                                () -> mapsLegacyJdbcValuesThroughPublicRegistries(entry.getValue())),
                        DynamicTest.dynamicTest(entry.getKey() + "/enum",
                                () -> mapsTemporalEnumMembersThroughPublicRegistries(entry.getValue())),
                        DynamicTest.dynamicTest(entry.getKey() + "/scalar",
                                () -> scalarReadAndWriteKeepTheSameTemporalFallback(entry.getValue()))));
    }

    private void mapsLegacyJdbcValuesThroughPublicRegistries(ValueCodecRegistry codecs) {
        Map<String, Object> row = Map.of("date", java.sql.Date.valueOf(LOCAL.toLocalDate()),
                "time", java.sql.Time.valueOf(LOCAL.toLocalTime().withNano(0)),
                "local", Timestamp.valueOf(LOCAL), "instant", Timestamp.from(INSTANT),
                "offset", Timestamp.from(INSTANT));
        Times expected = new Times(LOCAL.toLocalDate(), LOCAL.toLocalTime().withNano(0), LOCAL,
                INSTANT, INSTANT.atOffset(ZoneOffset.UTC));

        assertEquals(expected, assertDoesNotThrow(() -> RowMapper.of(Times.class, codecs).map(row)));
        TimesBean bean = assertDoesNotThrow(() -> RowMapper.of(TimesBean.class, codecs).map(row));
        assertEquals(expected, new Times(bean.date, bean.time, bean.local, bean.instant, bean.offset));
    }

    private void mapsTemporalEnumMembersThroughPublicRegistries(ValueCodecRegistry codecs) {
        Map<String, Object> row = Map.of("value", Timestamp.from(INSTANT));
        assertSame(TimeCode.VALUE, assertDoesNotThrow(() -> RowMapper.of(EnumRecord.class, codecs).map(row)).value());
        assertSame(TimeCode.VALUE, assertDoesNotThrow(() -> RowMapper.of(EnumBean.class, codecs).map(row)).value);
    }

    private void scalarReadAndWriteKeepTheSameTemporalFallback(ValueCodecRegistry codecs) {
        Timestamp source = Timestamp.from(INSTANT);
        assertEquals(INSTANT.atOffset(ZoneOffset.UTC), assertDoesNotThrow(() ->
                DialectScalarValueCodec.read(source, "TIMESTAMPTZ", codecs)));
        assertEquals(LOCAL, assertDoesNotThrow(() ->
                DialectScalarValueCodec.write(INSTANT, "TIMESTAMPTZ", "mysql", true, codecs)));
    }

    @Test
    void applicationCodecRejectionStillPrecedesTheJdbcFallback() {
        IllegalArgumentException rejection = new IllegalArgumentException("application temporal rejection");
        ValueCodec custom = new ValueCodec() {
            @Override public boolean supports(Class<?> type) { return type == Instant.class; }
            @Override public Object read(Object value, Class<?> type) { throw rejection; }
        };
        ValueCodecRegistry codecs = EntityTypeMappingRegistry.standard().valueCodecs().withFirst(custom);
        MappingException failure = assertThrows(MappingException.class, () -> RowMapper.of(EnumRecord.class, codecs)
                .map(Map.of("value", Timestamp.from(INSTANT))));
        assertSame(rejection, failure.getCause());
    }

    @Test
    void driverAdapterStillRunsBeforeTemporalFallback() {
        DriverValueAdapter adapter = new DriverValueAdapter() {
            @Override public boolean supports(Object value) { return value instanceof WrappedTime; }
            @Override public Object unwrap(Object value) { return ((WrappedTime) value).timestamp(); }
        };
        ValueCodecRegistry codecs = EntityTypeMappingRegistry.standard().valueCodecs().withDriverAdapter(adapter);
        EnumRecord mapped = assertDoesNotThrow(() -> RowMapper.of(EnumRecord.class, codecs)
                .map(Map.of("value", new WrappedTime(Timestamp.from(INSTANT)))));
        assertSame(TimeCode.VALUE, mapped.value());
    }

    private record Times(LocalDate date, LocalTime time, LocalDateTime local, Instant instant, OffsetDateTime offset) { }
    private static final class TimesBean {
        private LocalDate date;
        private LocalTime time;
        private LocalDateTime local;
        private Instant instant;
        private OffsetDateTime offset;
    }
    private record EnumRecord(TimeCode value) { }
    private static final class EnumBean { private TimeCode value; }
    private record WrappedTime(Timestamp timestamp) { }
    private enum TimeCode {
        VALUE(INSTANT);
        @EnumValue private final Instant value;
        TimeCode(Instant value) { this.value = value; }
    }
}
