package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.schema.RelationalSchemaPlanReviewer;
import com.flying.orm.rdb.schema.ReviewedSchemaPlan;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import com.flying.orm.rdb.type.DatabaseTypes;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDialectLiteralReadbackTest {

    @Test
    void oracleTypedTemporalDefaultsReadControlledAnsiLiterals() {
        var dialect = InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE;
        assertAll(
                () -> assertEquals(ColumnDefault.literal(LocalDate.parse("2026-09-05")),
                        RelationalMetadataValueParser.columnDefault(
                                "DATE '2026-09-05'", DatabaseType.of("ORACLE_DATE"), dialect)),
                () -> assertEquals(ColumnDefault.literal(LocalDateTime.parse("2026-09-05T12:30:00.123456")),
                        RelationalMetadataValueParser.columnDefault(
                                "(TIMESTAMP '2026-09-05 12:30:00.123456')", DatabaseType.of("TIMESTAMP"), dialect)),
                () -> assertEquals(ColumnDefault.literal(OffsetDateTime.parse("2026-09-05T12:30:00.123456+08:00")),
                        RelationalMetadataValueParser.columnDefault("TIMESTAMP '2026-09-05 12:30:00.123456 +08:00'",
                                DatabaseType.of("TIMESTAMPTZ"), dialect)),
                () -> assertEquals(ColumnDefault.literal(OffsetDateTime.parse("2026-09-05T04:30:00Z")),
                        RelationalMetadataValueParser.columnDefault("timestamp '2026-09-05 04:30:00 +00:00'",
                                DatabaseType.of("TIMESTAMPTZ"), dialect)));
    }

    @Test
    void oracleTypedTemporalChecksReadComparisonRangeAndIn() {
        var dialect = InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE;
        LocalDate date = LocalDate.parse("2026-09-05");
        LocalDateTime timestamp = LocalDateTime.parse("2026-09-05T12:30:00.123456");
        OffsetDateTime offsetTimestamp = OffsetDateTime.parse("2026-09-05T12:30:00.123456+08:00");
        assertAll(
                () -> assertEquals(CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.GREATER_THAN, date),
                        RelationalMetadataValueParser.checkPredicate("\"value\" > DATE '2026-09-05'",
                                Map.of("value", DatabaseType.of("ORACLE_DATE")), dialect)),
                () -> assertEquals(CheckPredicate.range("value", timestamp, timestamp),
                        RelationalMetadataValueParser.checkPredicate(
                                "(\"value\" >= TIMESTAMP '2026-09-05 12:30:00.123456'"
                                        + " and \"value\" <= TIMESTAMP '2026-09-05 12:30:00.123456')",
                                Map.of("value", DatabaseType.of("TIMESTAMP")), dialect)),
                () -> assertEquals(CheckPredicate.in("value", List.of(offsetTimestamp)),
                        RelationalMetadataValueParser.checkPredicate(
                                "\"value\" in (TIMESTAMP '2026-09-05 12:30:00.123456 +08:00')",
                                Map.of("value", DatabaseType.of("TIMESTAMPTZ")), dialect)));
    }

    @Test
    void oracleCompleteMetadataSnapshotWithTypedTemporalLiteralsConverges() {
        RelationIdentity identity = RelationIdentity.table("events");
        LocalDate date = LocalDate.parse("2026-09-05");
        Instant instant = Instant.parse("2026-09-05T04:30:00Z");
        RelationalTableDefinition desired = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("created_on", "DATE")
                        .defaultValue(ColumnDefault.literal(date)).build())
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMPTZ").temporalPrecision(6)
                        .defaultValue(ColumnDefault.literal(instant)).build())
                .addCheck(CheckConstraintDefinition.of("ck_date", CheckPredicate.compare(
                        "created_on", CheckPredicate.ComparisonOperator.GREATER_THAN, date)))
                .addCheck(CheckConstraintDefinition.of("ck_instant", CheckPredicate.compare(
                        "created_at", CheckPredicate.ComparisonOperator.GREATER_THAN, instant)))
                .build();
        SchemaSnapshot snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(identity,
                List.of(Map.of("COLUMN_NAME", "created_on", "DATA_TYPE", "DATE", "NULLABLE", "Y",
                                "COLUMN_REPRESENTABLE", true, "COLUMN_DEFAULT", "DATE '2026-09-05'"),
                        Map.of("COLUMN_NAME", "created_at", "DATA_TYPE", "TIMESTAMP WITH TIME ZONE", "NULLABLE", "Y",
                                "COLUMN_REPRESENTABLE", true, "TEMPORAL_PRECISION", 6,
                                "COLUMN_DEFAULT", "TIMESTAMP '2026-09-05 04:30:00 +00:00'")),
                List.of(Map.of("TABLE_REPRESENTABLE", true)),
                List.of(), List.of(), List.of(), List.of(),
                List.of(Map.of("CONSTRAINT_NAME", "ck_date", "CHECK_REPRESENTABLE", true,
                                "CHECK_EXPRESSION", "\"created_on\" > DATE '2026-09-05'"),
                        Map.of("CONSTRAINT_NAME", "ck_instant", "CHECK_REPRESENTABLE", true,
                                "CHECK_EXPRESSION", "\"created_at\" > TIMESTAMP '2026-09-05 04:30:00 +00:00'")),
                type -> DatabaseTypes.logicalDeclaration(type, "oracle"),
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE);
        RdbDialect dialect = RdbDialect.oracle();
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("Oracle", "19", dialect), desired, snapshot,
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);

        assertTrue(plan.steps().isEmpty(), () -> plan.operations().toString());
    }

    @Test
    void oracleTypedTemporalReadbackRemainsClosedToExpressionsAndOtherDialects() {
        var oracle = InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE;
        for (String expression : List.of("DATE '2026-09-05' + 1", "DATE '2026-09-05' || 'x'",
                "DATE '2026-09-05' 'extra'", "TO_DATE('2026-09-05', 'YYYY-MM-DD')")) {
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                    expression, DatabaseType.of("ORACLE_DATE"), oracle), expression);
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.checkPredicate(
                    "\"value\" > " + expression, Map.of("value", DatabaseType.of("ORACLE_DATE")), oracle), expression);
        }
        assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                "DATE '2026-09-05'", DatabaseType.of("VARCHAR"), oracle));
        assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.checkPredicate(
                "\"value\" > DATE '2026-09-05'", Map.of("value", DatabaseType.of("VARCHAR")), oracle));
        for (var dialect : List.of(InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL,
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER,
                InformationSchemaFormMetadataReader.SnapshotDialect.H2)) {
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                    "DATE '2026-09-05'", DatabaseType.of("DATE"), dialect));
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.checkPredicate(
                    "\"value\" > DATE '2026-09-05'", Map.of("value", DatabaseType.of("DATE")), dialect));
        }
    }

    @Test
    void oracleTimeOnlyLiteralsKeepTheirTextReadback() {
        var dialect = InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE;
        assertEquals(ColumnDefault.literal(LocalTime.parse("12:30:00.123456")),
                RelationalMetadataValueParser.columnDefault("'12:30:00.123456'", DatabaseType.of("TIME"), dialect));
        assertEquals(ColumnDefault.literal(OffsetTime.parse("12:30:00.123456+08:00")),
                RelationalMetadataValueParser.columnDefault(
                        "'12:30:00.123456+08:00'", DatabaseType.of("OFFSET_TIME"), dialect));
    }

    @Test
    void mysqlCurrentDateDefaultReadsItsDictionaryAlias() {
        ColumnDefault actual = assertDoesNotThrow(() -> RelationalMetadataValueParser.columnDefault(
                "(curdate())", DatabaseType.of("DATE"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));

        assertEquals(ColumnDefault.currentDate(), actual);
    }

    @Test
    void mysqlCurrentTimeDefaultReadsItsDictionaryAliasAndPrecision() {
        for (String expression : new String[]{"curtime()", "(curtime(6))", "current_time(6)"}) {
            ColumnDefault actual = assertDoesNotThrow(() -> RelationalMetadataValueParser.columnDefault(
                    expression, DatabaseType.of("TIME"),
                    InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL), expression);

            assertEquals(ColumnDefault.currentTime(), actual, expression);
        }
    }

    @Test
    void mysqlTemporalAliasesDoNotBecomeArbitraryOrCrossDialectDefaults() {
        for (InformationSchemaFormMetadataReader.SnapshotDialect dialect : List.of(
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL,
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE)) {
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                    "curdate()", DatabaseType.of("DATE"), dialect));
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                    "curtime(6)", DatabaseType.of("TIME"), dialect));
        }
        assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                "curdate() + interval 1 day", DatabaseType.of("DATE"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        assertEquals(ColumnDefault.literal("curdate()"), RelationalMetadataValueParser.columnDefault(
                "curdate()", DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        assertEquals(ColumnDefault.currentTimestamp(), RelationalMetadataValueParser.columnDefault(
                "current_timestamp(6)", DatabaseType.of("TIMESTAMP"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }

    @Test
    void h2NumericCastsDoNotRewriteQuotedCheckValues() {
        String value = "CAST(1 AS DECIMAL) and 'CAST(2 AS NUMERIC)'";
        assertEquals(CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.NOT_EQUAL, value),
                RelationalMetadataValueParser.checkPredicate(
                        "(\"value\" <> 'CAST(1 AS DECIMAL) and ''CAST(2 AS NUMERIC)''')",
                        Map.of("value", DatabaseType.of("VARCHAR")),
                        InformationSchemaFormMetadataReader.SnapshotDialect.H2));
    }

    @Test
    void sqlServerUnicodeDefaultReturnsTheOriginalLiteral() {
        assertEquals(ColumnDefault.literal("中文"), RelationalMetadataValueParser.columnDefault(
                "(N'中文')", DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void sqlServerChecksDecodeIdentifiersWithoutRewritingStringContents() {
        assertEquals(CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.NOT_EQUAL, "中文[值]"),
                RelationalMetadataValueParser.checkPredicate("([value]<>N'中文[值]')",
                        Map.of("value", DatabaseType.of("VARCHAR")),
                        InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void sqlServerDateDefaultRoundTripsItsCanonicalCast() {
        assertEquals(ColumnDefault.currentDate(), RelationalMetadataValueParser.columnDefault(
                "(CONVERT([date],getdate()))", DatabaseType.of("DATE"),
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void sqlServerTimeDefaultRoundTripsItsCanonicalCast() {
        for (String expression : new String[]{"(CONVERT([time],getdate(),0))",
                "(cast(current_timestamp as time))"}) {
            assertEquals(ColumnDefault.currentTime(), RelationalMetadataValueParser.columnDefault(
                    expression, DatabaseType.of("TIME"),
                    InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
        }
    }

    @Test
    void mysqlHexCheckAndStoredEscapedCheckReturnTheOriginalString() {
        String value = "C:\\new\\file'中文";
        String hex = java.util.HexFormat.of().formatHex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (String expression : new String[]{"(`value` <> _utf8mb4 X'" + hex + "')",
                "(`value` <> _utf8mb4'C:\\\\new\\\\file\\'中文')"}) {
            assertEquals(CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.NOT_EQUAL, value),
                    RelationalMetadataValueParser.checkPredicate(expression,
                            Map.of("value", DatabaseType.of("VARCHAR")),
                            InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        }
        assertEquals(ColumnDefault.literal(value), RelationalMetadataValueParser.columnDefault(
                value, DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }
}
