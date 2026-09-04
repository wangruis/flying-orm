package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.DriverValueAdapter;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeTemporalEntityMappingTest {

    @Test
    void adaptsLegacyJdbcTemporalValuesAtTheEntityReadBoundary() {
        LocalDate date = LocalDate.of(2026, 8, 30);
        LocalTime time = LocalTime.of(10, 11, 12);
        LocalDateTime timestamp = LocalDateTime.of(2026, 8, 30, 10, 11, 12, 123_456_789);

        LegacyTimes mapped = MappingPlan.of(LegacyTimes.class).map(DynamicRow.copyOf(Map.of(
                "business_date", java.sql.Date.valueOf(date),
                "business_time", Time.valueOf(time),
                "local_occurrence", Timestamp.valueOf(timestamp))));

        assertEquals(new LegacyTimes(date, time, timestamp), mapped);
    }

    @Test
    void mapsOffsetlessMysqlTimestampCarriersToAbsoluteEntityFieldsAtUtc() {
        LocalDateTime carrier = LocalDateTime.of(2026, 8, 23, 10, 11, 12, 123_456_000);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("occurred_at", carrier);
        values.put("modified_at", carrier);

        AbsoluteTimes mapped = MappingPlan.of(AbsoluteTimes.class).map(DynamicRow.copyOf(values));
        AbsoluteTimesBean bean = MappingPlan.of(AbsoluteTimesBean.class).map(DynamicRow.copyOf(values));

        assertEquals(carrier.toInstant(ZoneOffset.UTC), mapped.occurredAt());
        assertEquals(carrier.atOffset(ZoneOffset.UTC), mapped.modifiedAt());
        assertEquals(mapped.occurredAt(), bean.occurredAt);
        assertEquals(mapped.modifiedAt(), bean.modifiedAt);
    }

    @Test
    void preservesApplicationCodecPriorityForOffsetlessAbsoluteTimestampCarriers() {
        AtomicInteger reads = new AtomicInteger();
        ValueCodec customInstant = new ValueCodec() {
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
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(customInstant);
        LocalDateTime carrier = LocalDateTime.of(2026, 8, 23, 10, 11, 12);

        Instant mapped = MappingPlan.of(InstantOnly.class, codecs)
                                    .map(DynamicRow.copyOf(Map.of("occurred_at", carrier)))
                                    .occurredAt();

        assertEquals(carrier.toInstant(ZoneOffset.UTC).plusSeconds(1), mapped);
        assertEquals(1, reads.get());
    }

    @Test
    void preservesApplicationCodecRejectionForOffsetlessAbsoluteTimestampCarriers() {
        ValueCodec rejectingInstant = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Instant.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                if (value instanceof OffsetDateTime) {
                    throw new IllegalArgumentException("application rejected normalized timestamp carrier");
                }
                return value;
            }
        };
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(rejectingInstant);
        LocalDateTime carrier = LocalDateTime.of(2026, 8, 23, 10, 11, 12);

        assertThrows(MappingException.class, () -> MappingPlan.of(InstantOnly.class, codecs)
                                                               .map(DynamicRow.copyOf(
                                                                       Map.of("occurred_at", carrier))));
    }

    @Test
    void preservesStandardAbsoluteTimestampMappingWithUnrelatedRegistryExtensions() {
        ValueCodec unrelatedCodec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == UUID.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return UUID.fromString(value.toString());
            }
        };
        DriverValueAdapter unrelatedAdapter = new DriverValueAdapter() {
            @Override
            public boolean supports(Object value) {
                return value instanceof StringBuilder;
            }

            @Override
            public Object unwrap(Object value) {
                return value.toString();
            }
        };
        ValueCodecRegistry codecs = ValueCodecRegistry.standard()
                                                       .withFirst(unrelatedCodec)
                                                       .withDriverAdapter(unrelatedAdapter);
        LocalDateTime carrier = LocalDateTime.of(2026, 8, 23, 10, 11, 12);

        Instant mapped = MappingPlan.of(InstantOnly.class, codecs)
                                    .map(DynamicRow.copyOf(Map.of("occurred_at", carrier)))
                                    .occurredAt();

        assertEquals(carrier.toInstant(ZoneOffset.UTC), mapped);
    }

    private record AbsoluteTimes(Instant occurredAt, OffsetDateTime modifiedAt) {
    }

    private record InstantOnly(Instant occurredAt) {
    }

    private record LegacyTimes(LocalDate businessDate,
                               LocalTime businessTime,
                               LocalDateTime localOccurrence) {
    }

    private static final class AbsoluteTimesBean {

        private Instant occurredAt;
        private OffsetDateTime modifiedAt;
    }
}
