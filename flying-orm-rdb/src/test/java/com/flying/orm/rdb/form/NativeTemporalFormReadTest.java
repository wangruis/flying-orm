package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.OracleVersion;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 方言感知的表单时间读取、比较和排序边界。 */
class NativeTemporalFormReadTest {

    @Test
    void compilesJoinSqlAtTheTrustedRenderingBoundary() {
        DynamicForm form = offsetTimeForm();
        FormDataSqlRenderer renderer = renderer(RdbDialect.postgresql());
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.select(root, "remote_time").build();

        var request = renderer.joinQueries().select(spec, Map.of());

        assertTrue(request.statement().prepared());
        assertEquals(request.sql(), request.statement().transportSql("postgresql").orElseThrow());
    }

    @Test
    void keepsPostgresqlBitStringsRawWhileMysqlBitOneRemainsBoolean() {
        FormDataSqlRenderer postgresql = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        DynamicForm bitForm = DynamicForm.builder("flags", "flags")
                                         .addField(DynamicField.of("value", "BIT(1)"))
                                         .build();

        assertEquals("1", postgresql.insert(bitForm, Map.of("value", "1")).parameters().getFirst());
        assertEquals(Object.class,
                     postgresql.batchRenderer.insertPlan(
                             bitForm, Map.of("value", "1")).parameterTypes().getFirst());
        assertEquals("1", postgresql.readScalarValue(bitForm.field("value"), "1"));

        FormDataSqlRenderer mysql = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        assertEquals(Boolean.TRUE, mysql.insert(bitForm, Map.of("value", 1)).parameters().getFirst());
        assertEquals(Boolean.class,
                     mysql.batchRenderer.insertPlan(
                             bitForm, Map.of("value", 1)).parameterTypes().getFirst());
        assertEquals(Boolean.TRUE, mysql.readScalarValue(bitForm.field("value"), 1));
    }

    @Test
    void rejectsBooleanDriverValuesForNumericMysqlTinyintOne() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        DynamicField field = DynamicField.of("attempts", "TINYINT(1)");

        assertThrows(IllegalArgumentException.class,
                     () -> renderer.readScalarValue(field, Boolean.TRUE));
    }

    @Test
    void interpretsMySqlTimestampAsUtcOnlyAtTheMySqlFormBoundary() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        LocalDateTime driverValue = LocalDateTime.of(2026, 8, 21, 3, 4, 5, 123456000);

        Object decoded = renderer.readScalarValue(DynamicField.of("created_at", "TIMESTAMPTZ"), driverValue);

        assertEquals(driverValue.atOffset(ZoneOffset.UTC), decoded);
    }

    @Test
    void preservesApplicationCodecPriorityForMySqlUtcTimestampCarrier() {
        AtomicInteger reads = new AtomicInteger();
        ValueCodec customOffsetDateTime = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == OffsetDateTime.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                reads.incrementAndGet();
                return ((java.time.Instant) value).atOffset(ZoneOffset.UTC).plusSeconds(1);
            }
        };
        SqlRenderer sqlRenderer = SqlRenderer.builder()
                                             .addDefaultTerms()
                                             .build()
                                             .withValueCodecs(ValueCodecRegistry.standard()
                                                                                 .withFirst(customOffsetDateTime));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sqlRenderer, RdbDialect.mysql());
        LocalDateTime driverValue = LocalDateTime.of(2026, 8, 21, 3, 4, 5, 123456000);

        Object decoded = renderer.readScalarValue(DynamicField.of("created_at", "TIMESTAMPTZ"), driverValue);

        assertEquals(driverValue.atOffset(ZoneOffset.UTC).plusSeconds(1), decoded);
        assertEquals(1, reads.get());
    }

    @Test
    void interpretsPrecisionQualifiedMySqlTimestampAsUtc() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        LocalDateTime driverValue = LocalDateTime.of(2026, 8, 21, 3, 4, 5, 123456000);

        Object decoded = renderer.readScalarValue(DynamicField.of("created_at", "TIMESTAMPTZ(6)"), driverValue);

        assertEquals(driverValue.atOffset(ZoneOffset.UTC), decoded);
    }

    @Test
    void interpretsStandardTimeZoneTimestampAliasAsUtcAtTheMySqlFormBoundary() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());
        LocalDateTime driverValue = LocalDateTime.of(2026, 8, 21, 3, 4, 5, 123456000);

        Object decoded = renderer.readScalarValue(
                DynamicField.of("created_at", "TIMESTAMP(6) WITH TIME ZONE"), driverValue);

        assertEquals(driverValue.atOffset(ZoneOffset.UTC), decoded);
    }

    @Test
    void preservesOracleDateClockTimeAsLocalDateTime() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.oracle(OracleVersion.V19C));
        LocalDateTime expected = LocalDateTime.of(2026, 8, 22, 14, 30, 45);

        Object decoded = renderer.readScalarValue(
                DynamicField.of("legacy_created_at", "ORACLE_DATE"), Timestamp.valueOf(expected));
        Object reactiveDecoded = renderer.readScalarValue(
                DynamicField.of("legacy_created_at", "ORACLE_DATE"), expected);

        assertEquals(expected, decoded);
        assertEquals(expected, reactiveDecoded);
    }

    @Test
    void preservesOracleLocalTimeZoneTimestampAsLocalDateTime() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.oracle(OracleVersion.V19C));
        LocalDateTime expected = LocalDateTime.of(2026, 8, 22, 14, 30, 45, 123_456_000);

        Object decoded = renderer.readScalarValue(
                DynamicField.of("session_at", "TIMESTAMP(6) WITH LOCAL TIME ZONE"), expected);

        assertEquals(expected, decoded);
    }

    @Test
    void decodesOracleTextBackedLocalTimeAsLocalTime() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.oracle(OracleVersion.V19C));
        LocalTime expected = LocalTime.of(23, 59, 59, 999_999_999);

        Object decoded = renderer.readScalarValue(
                DynamicField.of("business_time", "TIME"), expected.toString());
        Object precisionDecoded = renderer.readScalarValue(
                DynamicField.of("business_time", "TIME(9) WITHOUT TIME ZONE"), expected.toString());

        assertEquals(expected, decoded);
        assertEquals(expected, precisionDecoded);
    }

    @Test
    void rejectsTimelineComparisonsForTextBackedOffsetTime() {
        DynamicForm form = offsetTimeForm();
        OffsetTime boundary = OffsetTime.parse("09:00:00-10:00");
        for (RdbDialect dialect : textBackedOffsetTimeDialects()) {
            FormDataSqlRenderer renderer = renderer(dialect);
            for (String operator : List.of(">", ">=", "<", "<=")) {
                ConditionGroup comparison = ConditionGroup.and()
                                                          .where("remote_time", operator, boundary)
                                                          .build();
                assertThrows(IllegalArgumentException.class, () -> renderer.select(form, comparison));
            }
            for (String operator : List.of("between", "not-between")) {
                ConditionGroup range = ConditionGroup.and()
                                                     .where("remote_time", operator, List.of(
                                                             OffsetTime.parse("10:00:00+14:00"), boundary))
                                                     .build();
                assertThrows(IllegalArgumentException.class, () -> renderer.select(form, range));
            }
        }
    }

    @Test
    void rejectsEveryOrderingPathForTextBackedOffsetTime() {
        DynamicForm form = offsetTimeForm();
        ConditionGroup empty = ConditionGroup.and().build();
        CursorPageQuery cursor = CursorPageQuery.first(20, CursorSort.asc("remote_time"));

        for (RdbDialect dialect : textBackedOffsetTimeDialects()) {
            FormDataSqlRenderer renderer = renderer(dialect);
            assertThrows(IllegalArgumentException.class,
                         () -> renderer.selectOrdered(form, empty, List.of(PageSort.asc("remote_time"))));
            assertThrows(IllegalArgumentException.class, () -> renderer.select(form, empty, cursor));

            JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
            JoinSource root = builder.root();
            JoinQuerySpec spec = builder.select(root, "remote_time")
                                        .orderBy(root, "remote_time", PageSort.Direction.ASC)
                                        .build();
            assertThrows(IllegalArgumentException.class,
                         () -> renderer.joinQueries().select(spec, Map.of()));
        }
    }

    @Test
    void keepsNativeOffsetTimeComparisonAndOrderingAvailable() {
        DynamicForm form = offsetTimeForm();
        ConditionGroup comparison = ConditionGroup.and()
                                                  .where("remote_time", ">",
                                                         OffsetTime.parse("09:00:00-10:00"))
                                                  .build();
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.postgresql())) {
            FormDataSqlRenderer renderer = renderer(dialect);
            assertDoesNotThrow(() -> renderer.select(form, comparison));
            assertDoesNotThrow(() -> renderer.selectOrdered(
                    form, ConditionGroup.and().build(), List.of(PageSort.asc("remote_time"))));
        }
    }

    @Test
    void keepsTextBackedOffsetTimeExactAndSetMatchingAvailable() {
        DynamicForm form = offsetTimeForm();
        OffsetTime value = OffsetTime.parse("12:34:56.123456+08:00");
        ConditionGroup exact = ConditionGroup.and().where("remote_time", "=", value).build();
        ConditionGroup membership = ConditionGroup.and()
                                                  .where("remote_time", "in", List.of(value))
                                                  .build();
        for (RdbDialect dialect : textBackedOffsetTimeDialects()) {
            FormDataSqlRenderer renderer = renderer(dialect);
            assertDoesNotThrow(() -> renderer.select(form, exact));
            assertDoesNotThrow(() -> renderer.select(form, membership));
        }
    }

    private static FormDataSqlRenderer renderer(RdbDialect dialect) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
    }

    private static DynamicForm offsetTimeForm() {
        return DynamicForm.builder("event_clock", "event_clock")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("remote_time", "OFFSET_TIME").withNullable(false))
                          .build();
    }

    private static List<RdbDialect> textBackedOffsetTimeDialects() {
        return List.of(RdbDialect.mysql(),
                       RdbDialect.oracle(OracleVersion.V19C),
                       RdbDialect.sqlServer());
    }
}
