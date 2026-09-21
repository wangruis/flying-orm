package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityEnumTemporalCarrierTest {

    @TestFactory
    List<DynamicTest> readsJdbcAndJavaTimeCarriersForRecordsAndBeans() {
        List<Carrier> carriers = List.of(
                new Carrier("date", DateCode.DAY, DateCode.DAY.value, java.sql.Date.valueOf(DateCode.DAY.value)),
                new Carrier("time", TimeCode.NOON, TimeCode.NOON.value, java.sql.Time.valueOf(TimeCode.NOON.value)),
                new Carrier("local", LocalCode.MOMENT, LocalCode.MOMENT.value,
                        Timestamp.valueOf(LocalCode.MOMENT.value)),
                new Carrier("instant", InstantCode.MOMENT, InstantCode.MOMENT.value,
                        Timestamp.from(InstantCode.MOMENT.value)),
                new Carrier("offset", OffsetCode.MOMENT, OffsetCode.MOMENT.value,
                        Timestamp.from(OffsetCode.MOMENT.value.toInstant())));
        List<DynamicTest> tests = new ArrayList<>();
        for (Carrier carrier : carriers) {
            for (boolean jdbc : List.of(false, true)) {
                for (boolean bean : List.of(false, true)) {
                    tests.add(DynamicTest.dynamicTest(carrier.field() + "/jdbc=" + jdbc + "/bean=" + bean, () -> {
                        Map<String, Object> row = Map.of(carrier.field(), jdbc ? carrier.jdbc() : carrier.javaTime());
                        Object actual = assertDoesNotThrow(() -> bean
                                ? RowMapper.of(TemporalBean.class).map(row).value(carrier.field())
                                : RowMapper.of(TemporalRecord.class).map(row).value(carrier.field()));
                        assertSame(carrier.expected(), actual);
                    }));
                }
            }
        }
        return tests;
    }

    @Test
    void explicitTemporalCodecKeepsPrecedenceOverTheJdbcFallback() {
        AtomicInteger reads = new AtomicInteger();
        Timestamp source = Timestamp.valueOf("2000-01-01 00:00:00");
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == LocalDateTime.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                assertSame(source, value);
                reads.incrementAndGet();
                return LocalCode.MOMENT.value;
            }
        };

        TemporalRecord mapped = RowMapper.of(TemporalRecord.class, ValueCodecRegistry.standard().withFirst(codec))
                .map(Map.of("local", source));

        assertSame(LocalCode.MOMENT, mapped.local());
        assertEquals(1, reads.get());
    }

    @Test
    void offsetlessMysqlCarriersUseTheAbsoluteFieldsUtcConvention() {
        LocalDateTime source = LocalDateTime.ofInstant(InstantCode.MOMENT.value, ZoneOffset.UTC);
        Map<String, Object> row = Map.of("instant", source, "offset", source);

        TemporalRecord record = assertDoesNotThrow(() -> RowMapper.of(TemporalRecord.class).map(row));
        TemporalBean bean = assertDoesNotThrow(() -> RowMapper.of(TemporalBean.class).map(row));

        assertSame(InstantCode.MOMENT, record.instant());
        assertSame(OffsetCode.MOMENT, record.offset());
        assertSame(InstantCode.MOMENT, bean.instant);
        assertSame(OffsetCode.MOMENT, bean.offset);
    }

    @Test
    void offsetlessNormalizationStillUsesTheApplicationTemporalCodec() {
        AtomicInteger reads = new AtomicInteger();
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Instant.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                reads.incrementAndGet();
                return ((OffsetDateTime) value).toInstant().plusSeconds(1);
            }
        };
        LocalDateTime source = LocalDateTime.ofInstant(InstantCode.MOMENT.value.minusSeconds(1), ZoneOffset.UTC);

        TemporalRecord mapped = assertDoesNotThrow(() -> RowMapper.of(
                TemporalRecord.class, ValueCodecRegistry.standard().withFirst(codec)).map(Map.of("instant", source)));

        assertSame(InstantCode.MOMENT, mapped.instant());
        assertEquals(1, reads.get());
    }

    @Test
    void unknownAndNullTemporalCodesKeepTheirExistingSemantics() {
        RowMapper<TemporalRecord> mapper = RowMapper.of(TemporalRecord.class);
        assertEquals(new TemporalRecord(null, null, null, null, null), mapper.map(Map.of()));
        assertThrows(MappingException.class,
                () -> mapper.map(Map.of("local", LocalCode.MOMENT.value.plusNanos(1))));
    }

    private record Carrier(String field, Enum<?> expected, Object javaTime, Object jdbc) {
    }

    private record TemporalRecord(DateCode date, TimeCode time, LocalCode local,
                                  InstantCode instant, OffsetCode offset) {
        Object value(String field) {
            return switch (field) {
                case "date" -> date;
                case "time" -> time;
                case "local" -> local;
                case "instant" -> instant;
                case "offset" -> offset;
                default -> throw new AssertionError(field);
            };
        }
    }

    private static final class TemporalBean {
        private DateCode date;
        private TimeCode time;
        private LocalCode local;
        private InstantCode instant;
        private OffsetCode offset;

        Object value(String field) {
            return switch (field) {
                case "date" -> date;
                case "time" -> time;
                case "local" -> local;
                case "instant" -> instant;
                case "offset" -> offset;
                default -> throw new AssertionError(field);
            };
        }
    }

    private enum DateCode {
        DAY(LocalDate.of(2026, 9, 20));
        @EnumValue private final LocalDate value;
        DateCode(LocalDate value) { this.value = value; }
    }

    private enum TimeCode {
        NOON(LocalTime.of(12, 30, 15));
        @EnumValue private final LocalTime value;
        TimeCode(LocalTime value) { this.value = value; }
    }

    private enum LocalCode {
        MOMENT(LocalDateTime.parse("2026-09-20T12:30:15.123456789"));
        @EnumValue private final LocalDateTime value;
        LocalCode(LocalDateTime value) { this.value = value; }
    }

    private enum InstantCode {
        MOMENT(Instant.parse("2026-09-20T12:30:15.123456789Z"));
        @EnumValue private final Instant value;
        InstantCode(Instant value) { this.value = value; }
    }

    private enum OffsetCode {
        MOMENT(OffsetDateTime.parse("2026-09-20T12:30:15.123456789Z"));
        @EnumValue private final OffsetDateTime value;
        OffsetCode(OffsetDateTime value) { this.value = value; }
    }
}
