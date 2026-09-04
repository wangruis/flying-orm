package com.flying.orm.rdb.reactive;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class BatchPayloadHasherTemporalTest {

    @Test
    void distinguishesR2dbcParameterDatabaseTypesWithTheSameJavaType() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();

        String varchar = hasher.hashRows(List.<Object[]>of(new Object[]{
                Parameters.in(R2dbcType.VARCHAR, "same")
        }));
        String clob = hasher.hashRows(List.<Object[]>of(new Object[]{
                Parameters.in(R2dbcType.CLOB, "same")
        }));

        assertNotEquals(varchar, clob);
    }

    @Test
    void distinguishesR2dbcParameterDirections() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();

        String input = hasher.hashRows(List.<Object[]>of(new Object[]{
                Parameters.in(R2dbcType.VARCHAR, "same")
        }));
        String inputOutput = hasher.hashRows(List.<Object[]>of(new Object[]{
                Parameters.inOut(R2dbcType.VARCHAR, "same")
        }));

        assertNotEquals(input, inputOutput);
    }

    @Test
    void hashesEveryLegacyTemporalAcceptedByTheBatchSnapshotBoundary() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();
        Timestamp timestamp = Timestamp.from(Instant.parse("2026-08-22T10:11:12.123456789Z"));
        Object[] row = {
                timestamp,
                new java.sql.Date(1_700_000_000_000L),
                new Time(1_700_000_000_000L),
                new java.util.Date(1_700_000_000_000L)
        };

        assertDoesNotThrow(() -> hasher.hashRows(List.<Object[]>of(row)));
    }

    @Test
    void includesTimestampNanosecondsInTheReceiptIdentity() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();
        Timestamp first = Timestamp.from(Instant.parse("2026-08-22T10:11:12.123456789Z"));
        Timestamp second = Timestamp.from(Instant.parse("2026-08-22T10:11:12.123456790Z"));

        String firstHash = hasher.hashRows(List.<Object[]>of(new Object[]{first}));
        String secondHash = hasher.hashRows(List.<Object[]>of(new Object[]{second}));

        assertNotEquals(firstHash, secondHash);
    }

    @Test
    void hashesJdbcDateAndTimeByTheirLogicalValuesAcrossJvmTimeZones() {
        BatchPayloadHasher hasher = new BatchPayloadHasher();
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            String utc = hasher.hashRows(List.<Object[]>of(new Object[]{
                    java.sql.Date.valueOf(LocalDate.of(2026, 8, 22)),
                    Time.valueOf(LocalTime.of(10, 11, 12))
            }));

            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            String shanghai = hasher.hashRows(List.<Object[]>of(new Object[]{
                    java.sql.Date.valueOf(LocalDate.of(2026, 8, 22)),
                    Time.valueOf(LocalTime.of(10, 11, 12))
            }));

            assertEquals(utc, shanghai);
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
