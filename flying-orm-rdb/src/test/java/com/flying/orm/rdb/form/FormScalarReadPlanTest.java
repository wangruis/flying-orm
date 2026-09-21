package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FormScalarReadPlanTest {

    @Test
    void readsMysqlZonedTimestampAsTheSameUtcInstant() {
        FormScalarReadPlan plan = FormScalarReadPlan.compile(
                DynamicField.of("recorded_at", "TIMESTAMPTZ"),
                "mysql", true, ValueCodecRegistry.standard());
        ZonedDateTime driverValue = ZonedDateTime.of(
                2026, 9, 8, 9, 2, 3, 123_456_000, ZoneId.of("Asia/Shanghai"));

        assertEquals(OffsetDateTime.ofInstant(driverValue.toInstant(), ZoneOffset.UTC),
                     assertDoesNotThrow(() -> plan.read(driverValue)));
    }
}
