package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormScalarWriteDispatchTest {

    @Test
    void ordinaryAndUnknownDeclarationsKeepCodecOutputAndCallItOnce() {
        Object marker = new Object();
        AtomicInteger calls = new AtomicInteger();
        ValueCodecRegistry codecs = codecs(marker, calls);
        int expected = 0;
        for (String dialect : List.of("postgresql", "mysql", "oracle", "sqlserver", "custom")) {
            FormSqlRenderSupport support = support(dialect, codecs);
            for (String type : List.of("BIGINT", "DECIMAL", "DATE", "TIMESTAMP", "VARCHAR", "VENDOR_CODE")) {
                DynamicField field = DynamicField.of("value", type);
                assertSame(marker, support.writeValue(field, new Token()));
                assertEquals(++expected, calls.get());
                assertNull(support.writeValue(field, null));
                assertEquals(expected, calls.get());
            }
        }
    }

    @Test
    void binaryFieldsKeepTheirDedicatedConversionBeforeScalarCodecs() {
        AtomicInteger calls = new AtomicInteger();
        FormSqlRenderSupport support = support("postgresql", codecs(new Object(), calls));
        DynamicField binary = DynamicField.of("value", "VARBINARY");
        byte[] bytes = {1, 2};
        assertSame(bytes, support.writeValue(binary, bytes));
        assertThrows(IllegalArgumentException.class, () -> support.writeValue(binary, new Token()));
        assertEquals(0, calls.get());
    }

    @Test
    void uuidCodecOutputUsesTextFallbackOnlyForTextOrTextBackedUuid() {
        UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        AtomicInteger calls = new AtomicInteger();
        ValueCodecRegistry codecs = codecs(uuid, calls);
        int expected = 0;
        for (String dialect : List.of("postgresql", "mysql", "oracle", "sqlserver", "custom")) {
            FormSqlRenderSupport support = support(dialect, codecs);
            assertEquals(uuid.toString(), support.writeValue(DynamicField.of("value", "VARCHAR"), new Token()));
            assertSame(uuid, support.writeValue(DynamicField.of("value", "BIGINT"), new Token()));
            assertSame(uuid, support.writeValue(DynamicField.of("value", "VENDOR_CODE"), new Token()));
            Object uuidValue = support.writeValue(DynamicField.of("value", "UUID"), new Token());
            assertEquals(List.of("mysql", "oracle", "sqlserver").contains(dialect) ? uuid.toString() : uuid, uuidValue);
            assertEquals(expected += 4, calls.get());
        }
    }

    @Test
    void booleanTimeTimestampAndIntervalKeepTheirDialectRepresentation() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        FormSqlRenderSupport oracle = support("oracle", codecs);
        FormSqlRenderSupport mysql = support("mysql", codecs);
        assertEquals(1, oracle.writeValue(DynamicField.of("value", "BOOLEAN"), true));
        assertEquals(true, mysql.writeValue(DynamicField.of("value", "BIT(1)"), true));
        assertEquals("10:11:12", oracle.writeValue(DynamicField.of("value", "TIME"), LocalTime.of(10, 11, 12)));
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-10-05T12:30:00+08:00");
        assertEquals(timestamp.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime(),
                mysql.writeValue(DynamicField.of("value", "TIMESTAMPTZ"), timestamp));
        Period period = Period.ofMonths(15);
        assertSame(period, oracle.writeValue(DynamicField.of("value", "INTERVAL YEAR TO MONTH"), period));
    }

    @Test
    void offsetTimeUsesRegisteredCodecBeforeChoosingItsDriverRepresentation() {
        OffsetTime time = OffsetTime.parse("12:34:56.123456+08:00");
        AtomicInteger calls = new AtomicInteger();
        ValueCodecRegistry codecs = codecs(time, calls);
        int expected = 0;
        for (String dialect : List.of("postgresql", "h2", "mysql", "oracle", "sqlserver", "custom")) {
            FormSqlRenderSupport support = support(dialect, codecs);
            for (String declaration : List.of("OFFSET_TIME", "TIME(6) WITH TIME ZONE")) {
                DynamicField field = DynamicField.of("value", declaration);
                boolean nativeTime = List.of("postgresql", "h2").contains(dialect);
                assertEquals(nativeTime ? time : time.toString(), support.writeValue(field, new Token()));
                assertEquals(nativeTime ? OffsetTime.class : String.class, support.parameterType(field));
                assertEquals(++expected, calls.get());
                assertNull(support.writeValue(field, null));
                FormScalarReadPlan read = FormScalarReadPlan.compile(
                        field, dialect, false, ValueCodecRegistry.standard());
                assertEquals(time, read.read(time.toString()));
                assertSame(time, read.read(time));
                assertNull(read.read(null));
            }
        }
    }

    private static ValueCodecRegistry codecs(Object output, AtomicInteger calls) {
        return ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            public boolean supports(Class<?> type) { return type == Token.class; }
            public Object write(Object value) { calls.incrementAndGet(); return output; }
            public Object read(Object value, Class<?> type) { throw new AssertionError("unexpected read conversion"); }
        });
    }

    private static FormSqlRenderSupport support(String dialect, ValueCodecRegistry codecs) {
        RdbDialect syntax = RdbDialect.postgresql();
        return new FormSqlRenderSupport(SqlRenderer.builder().addDefaultTerms().valueCodecs(codecs).build(),
                syntax.json(), dialect, false, syntax.schema()::identifier,
                StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()), java.util.Map.of(),
                syntax.capabilities(), syntax.schema()::relationIdentifier);
    }

    private record Token() { }
}
