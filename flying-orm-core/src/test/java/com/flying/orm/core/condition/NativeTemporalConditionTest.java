package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.scope.TimeScope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 原生时间类型带精度时仍保持绝对时间与本地时间语义。 */
class NativeTemporalConditionTest {

    private final StructuredConditionValueNormalizer normalizer =
            new StructuredConditionValueNormalizer(ValueCodecRegistry.standard());

    @Test
    void keepsPrecisionQualifiedOracleLocalTimeZoneTimestampLocal() {
        LocalDateTime local = LocalDateTime.of(2026, 8, 21, 11, 4, 5, 123456000);

        Object normalized = normalizer.normalize(local,
                                                 DynamicField.of("observed_at",
                                                                 "TIMESTAMP(6) WITH LOCAL TIME ZONE"),
                                                 StructuredConditionPolicy.defaults(),
                                                 "$value",
                                                 "=");

        assertEquals(local, normalized);
    }

    @Test
    void keepsPrecisionQualifiedLocalTimestampWithoutAnOffset() {
        LocalDateTime local = LocalDateTime.of(2026, 8, 21, 11, 4, 5, 123456000);

        Object normalized = normalizer.normalize(local,
                                                 DynamicField.of("local_occurrence", "TIMESTAMP(6)"),
                                                 StructuredConditionPolicy.defaults(),
                                                 "$value",
                                                 "=");

        assertEquals(local, normalized);
    }

    @Test
    void validatesOffsetDateTimeRangesByTheirAbsoluteTimeline() {
        OffsetDateTime laterLocalRepresentation = OffsetDateTime.parse("2026-08-22T10:00:00+02:00");
        OffsetDateTime earlierLocalRepresentation = OffsetDateTime.parse("2026-08-22T09:00:00+01:00");

        assertDoesNotThrow(() -> TimeScope.closed(
                "observed_at", laterLocalRepresentation, earlierLocalRepresentation));
        assertThrows(IllegalArgumentException.class, () -> TimeScope.between(
                "observed_at", earlierLocalRepresentation, laterLocalRepresentation));
    }

    @Test
    void normalizesOffsetDateTimeBetweenByItsAbsoluteTimeline() {
        OffsetDateTime laterLocalRepresentation = OffsetDateTime.parse("2026-08-22T10:00:00+02:00");
        OffsetDateTime earlierLocalRepresentation = OffsetDateTime.parse("2026-08-22T09:00:00+01:00");

        assertDoesNotThrow(() -> normalizer.normalize(
                List.of(laterLocalRepresentation, earlierLocalRepresentation),
                DynamicField.of("observed_at", "TIMESTAMPTZ"),
                StructuredConditionPolicy.defaults(),
                "$value",
                "between"));
    }

    @Test
    void validatesOffsetTimeRangesByTheirUtcTimeline() {
        OffsetTime laterLocalRepresentation = OffsetTime.parse("10:00:00+02:00");
        OffsetTime earlierLocalRepresentation = OffsetTime.parse("09:00:00+01:00");

        assertDoesNotThrow(() -> TimeScope.closed(
                "daily_cutoff", laterLocalRepresentation, earlierLocalRepresentation));
        assertThrows(IllegalArgumentException.class, () -> TimeScope.between(
                "daily_cutoff", earlierLocalRepresentation, laterLocalRepresentation));
    }

    @Test
    void normalizesOffsetTimeBetweenByItsUtcTimeline() {
        OffsetTime laterLocalRepresentation = OffsetTime.parse("10:00:00+02:00");
        OffsetTime earlierLocalRepresentation = OffsetTime.parse("09:00:00+01:00");

        assertDoesNotThrow(() -> normalizer.normalize(
                List.of(laterLocalRepresentation, earlierLocalRepresentation),
                DynamicField.of("daily_cutoff", "TIME WITH TIME ZONE"),
                StructuredConditionPolicy.defaults(),
                "$value",
                "between"));
    }

    @Test
    void normalizesLogicalOffsetTimeValuesRestoredFromMetadata() {
        OffsetTime value = OffsetTime.parse("10:15:30.123456789+05:30");

        Object normalized = normalizer.normalize(value.toString(),
                                                 DynamicField.of("daily_cutoff", "OFFSET_TIME"),
                                                 StructuredConditionPolicy.defaults(),
                                                 "$value",
                                                 "=");

        assertEquals(value, normalized);
    }

    @Test
    void normalizesLegacyLocalTimestampsRestoredFromMetadata() {
        LocalDateTime value = LocalDateTime.of(2026, 8, 23, 14, 20, 30);

        for (String dataType : List.of("ORACLE_DATE", "SQLSERVER_DATETIME", "SQLSERVER_SMALLDATETIME")) {
            Object normalized = normalizer.normalize(value.toString(),
                                                     DynamicField.of("legacy_created_at", dataType),
                                                     StructuredConditionPolicy.defaults(),
                                                     "$value",
                                                     "=");

            assertEquals(value, normalized, dataType);
        }
    }

    @Test
    void normalizesPhysicalSqlServerSmallDatetimeConditions() {
        LocalDateTime value = LocalDateTime.of(2026, 8, 23, 14, 20);

        Object normalized = normalizer.normalize(value.toString(),
                                                 DynamicField.of("legacy_created_at", "SMALLDATETIME"),
                                                 StructuredConditionPolicy.defaults(),
                                                 "$value",
                                                 "=");

        assertEquals(value, normalized);
    }

    @Test
    void normalizesUuidStringsToTheNativeUuidType() {
        UUID value = UUID.randomUUID();

        Object normalized = normalizer.normalize(value.toString(),
                                                 DynamicField.of("id", "UUID"),
                                                 StructuredConditionPolicy.defaults(),
                                                 "$value",
                                                 "=");

        assertEquals(value, normalized);
    }

    @Test
    void reportsCodecFailuresAsValueConversionFailures() {
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> normalizer.normalize("not-an-integer",
                                           DynamicField.of("amount", "INTEGER"),
                                           StructuredConditionPolicy.defaults(),
                                           "$value",
                                           "="));

        assertEquals(StructuredConditionErrorCode.VALUE_CONVERSION_FAILED, error.code());
    }

}
