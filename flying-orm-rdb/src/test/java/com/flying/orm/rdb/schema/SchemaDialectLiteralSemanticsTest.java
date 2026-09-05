package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDialectLiteralSemanticsTest {

    @Test
    void oracleCreateTableUsesNlsIndependentTypedTemporalDefaults() {
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.oracle().schema());
        assertAll(oracleTemporalLiterals().stream().map(example -> () -> {
            RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                    .addColumn(example.column()).build();
            SchemaOperation operation = SchemaOperation.of(
                    SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                    null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

            assertEquals("create table \"events\" (\"value\" " + example.physicalType()
                    + " default " + example.sqlLiteral() + ")", renderer.render(operation).getFirst().sql());
        }));
    }

    @Test
    void oracleAddColumnUsesNlsIndependentTypedTemporalDefaults() {
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.oracle().schema());
        assertAll(oracleTemporalLiterals().stream().map(example -> () -> {
            SchemaOperation operation = SchemaOperation.of(
                    SchemaOperation.Kind.ADD_COLUMN, RelationIdentity.table("events"), "value",
                    null, example.column(), SchemaOperation.Compatibility.REQUIRES_REVIEW);

            assertEquals("alter table \"events\" add (\"value\" " + example.physicalType()
                    + " default " + example.sqlLiteral() + ")", renderer.render(operation).getFirst().sql());
        }));
    }

    @Test
    void oracleChecksUseTheSameTypedTemporalLiteralBoundary() {
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.oracle().schema());
        assertAll(oracleTemporalLiterals().stream().map(example -> () -> {
            CheckConstraintDefinition comparison = CheckConstraintDefinition.of("ck_value",
                    CheckPredicate.compare("value", CheckPredicate.ComparisonOperator.GREATER_THAN, example.value()));
            CheckConstraintDefinition range = CheckConstraintDefinition.of("ck_value",
                    CheckPredicate.range("value", example.value(), example.value()));
            CheckConstraintDefinition in = CheckConstraintDefinition.of("ck_value",
                    CheckPredicate.in("value", List.of(example.value())));
            String prefix = "alter table \"events\" add constraint \"ck_value\" check (";

            assertAll(
                    () -> assertEquals(prefix + "\"value\" > " + example.sqlLiteral() + ")",
                            addCheckSql(renderer, comparison)),
                    () -> assertEquals(prefix + "(\"value\" >= " + example.sqlLiteral()
                                    + " and \"value\" <= " + example.sqlLiteral() + "))",
                            addCheckSql(renderer, range)),
                    () -> assertEquals(prefix + "\"value\" in (" + example.sqlLiteral() + "))",
                            addCheckSql(renderer, in)));
        }));
    }

    @Test
    void oracleTimeOnlyLiteralsKeepTheirTextStorageRepresentation() {
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.oracle().schema());
        for (Object value : List.of(LocalTime.parse("12:30:00.123456"),
                OffsetTime.parse("12:30:00.123456+08:00"))) {
            String type = value instanceof LocalTime ? "TIME" : "OFFSET_TIME";
            ColumnDefinition column = ColumnDefinition.builder("value", type)
                    .defaultValue(ColumnDefault.literal(value)).build();
            SchemaOperation operation = SchemaOperation.of(
                    SchemaOperation.Kind.ADD_COLUMN, RelationIdentity.table("events"), "value",
                    null, column, SchemaOperation.Compatibility.REQUIRES_REVIEW);

            assertTrue(renderer.render(operation).getFirst().sql().contains(" default '" + value + "')"));
            assertEquals("alter table \"events\" add constraint \"ck_value\" check (\"value\" > '" + value + "')",
                    addCheckSql(renderer, CheckConstraintDefinition.of("ck_value", CheckPredicate.compare(
                            "value", CheckPredicate.ComparisonOperator.GREATER_THAN, value))));
        }
    }

    @Test
    void mysqlCreateTableParenthesizesCurrentDateAndTimeDefaults() {
        RelationalTableDefinition table = temporalDefaultsTable();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        String sql = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(operation).getFirst().sql();

        assertEquals("create table `events` (`created_on` DATE default (current_date), "
                + "`created_time` TIME(6) default (current_time))", sql);
    }

    @Test
    void mysqlAddColumnParenthesizesCurrentDateAndTimeDefaults() {
        RelationalTableDefinition table = temporalDefaultsTable();
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema());
        List<String> sql = table.columns().stream()
                .map(column -> SchemaOperation.of(
                        SchemaOperation.Kind.ADD_COLUMN, table.identity(), column.name(),
                        null, column, SchemaOperation.Compatibility.REQUIRES_REVIEW))
                .map(operation -> renderer.render(operation).getFirst().sql())
                .toList();

        assertEquals(List.of(
                "alter table `events` add column `created_on` DATE default (current_date)",
                "alter table `events` add column `created_time` TIME(6) default (current_time)"), sql);
    }

    @Test
    void oracleCurrentTimeDefaultRequiresManualSqlForCreateAndAdd() {
        RdbDialect dialect = RdbDialect.oracle();
        ColumnDefinition column = ColumnDefinition.builder("created_time", "TIME")
                .defaultValue(ColumnDefault.currentTime()).build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .addColumn(column).build();
        SchemaOperation create = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        SchemaOperation add = SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN, table.identity(), column.name(),
                null, column, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(dialect.schema());

        assertAll(
                () -> assertThrows(UnsupportedOperationException.class, () -> renderer.render(create)),
                () -> assertThrows(UnsupportedOperationException.class, () -> renderer.render(add)));
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("Oracle", "19", dialect), table, SchemaSnapshot.absent(table.identity()),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
        assertTrue(plan.requiresManualAction());
        assertEquals(1, plan.steps().size());
        assertTrue(plan.steps().getFirst().request().isEmpty());
    }

    @Test
    void sqlServerDefaultsAndChecksPreserveUnicodeOutsideTheDatabaseCodePage() {
        String sql = createSql(RdbDialect.sqlServer(), ColumnDefault.literal("中文"), "中文");

        assertTrue(sql.contains("default N'中文'"), sql);
        assertTrue(sql.contains("<> N'中文'"), sql);
    }

    @Test
    void mysqlDefaultsAndChecksDoNotDependOnBackslashEscapeSqlMode() {
        String value = "C:\\new\\file'中文";
        String sql = createSql(RdbDialect.mysql(), ColumnDefault.literal(value), value);
        // MySQL's character-set-introduced hex literal contains no escape sequences in either SQL mode.
        String literal = "_utf8mb4 X'" + HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8)) + "'";

        assertTrue(sql.contains("default " + literal), sql);
        assertTrue(sql.contains("<> " + literal), sql);
        assertFalse(sql.contains("\\"), sql);
    }

    @Test
    void mysqlBackslashCommentsDeclareTheirSessionModeRequirement() {
        String comment = "Windows path C:\\data\\files";
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .comment(comment)
                .addColumn(ColumnDefinition.builder("value", "VARCHAR").length(128)
                        .comment(comment).build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        String sql = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(operation).getFirst().sql();

        assertTrue(sql.contains("/*flying-orm:mysql-comment-no-backslash-escapes*/"), sql);
        SqlExecutionStatements.canonical(new SqlRequest(sql, List.of()), "mysql");
    }

    @Test
    void mysqlTrailingBackslashCommentStillExposesTheModeRequirement() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .comment("Windows path C:\\")
                .addColumn(ColumnDefinition.builder("value", "VARCHAR").length(128).build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        SqlRequest request = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(operation).getFirst();

        assertTrue(MySqlSchemaCommentSupport.requiresModeValidation(List.of(request)), request.sql());
    }

    @Test
    void mysqlMarkerTextInsideAnOrdinaryCommentDoesNotRequireTheMode() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .comment("/*flying-orm:mysql-comment-no-backslash-escapes*/")
                .addColumn(ColumnDefinition.builder("value", "VARCHAR").length(128).build())
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        SqlRequest request = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(operation).getFirst();

        assertFalse(MySqlSchemaCommentSupport.requiresModeValidation(List.of(request)), request.sql());
    }

    @Test
    void sqlServerDateDefaultUsesAnExpressionAvailableBeforeSqlServer2025() {
        ColumnDefinition column = ColumnDefinition.builder("created_on", "DATE")
                .defaultValue(ColumnDefault.currentDate()).build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN, RelationIdentity.table("events"), column.name(),
                null, column, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        var request = RelationalSchemaSqlRenderer.create(RdbDialect.sqlServer().schema())
                .render(operation).getFirst();

        assertTrue(request.sql().contains("default (cast(current_timestamp as date))"), request.sql());
        SqlExecutionStatements.canonical(request, "sqlserver");
    }

    private static String createSql(RdbDialect dialect, ColumnDefault defaultValue, String checkValue) {
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("value", "VARCHAR").length(128)
                        .defaultValue(defaultValue).build())
                .addCheck(CheckConstraintDefinition.of("ck_events_value", CheckPredicate.compare(
                        "value", CheckPredicate.ComparisonOperator.NOT_EQUAL, checkValue)))
                .build();
        SchemaOperation operation = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE, table.identity(), table.identity().table(),
                null, table, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        var request = RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation).getFirst();
        SqlExecutionStatements.canonical(request, dialect.name());
        return request.sql();
    }

    private static RelationalTableDefinition temporalDefaultsTable() {
        return RelationalTableDefinition.builder(RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("created_on", "DATE")
                        .defaultValue(ColumnDefault.currentDate()).build())
                .addColumn(ColumnDefinition.builder("created_time", "TIME").temporalPrecision(6)
                        .defaultValue(ColumnDefault.currentTime()).build())
                .build();
    }

    private static String addCheckSql(RelationalSchemaSqlRenderer renderer, CheckConstraintDefinition check) {
        return renderer.render(SchemaOperation.of(
                SchemaOperation.Kind.ADD_CHECK, RelationIdentity.table("events"), check.name(),
                null, check, SchemaOperation.Compatibility.REQUIRES_REVIEW)).getFirst().sql();
    }

    private static List<OracleTemporalLiteral> oracleTemporalLiterals() {
        return List.of(
                new OracleTemporalLiteral("DATE", "DATE", LocalDate.parse("2026-09-05"),
                        "date '2026-09-05'"),
                new OracleTemporalLiteral("TIMESTAMP", "TIMESTAMP(6)",
                        LocalDateTime.parse("2026-09-05T12:30:00.123456"),
                        "timestamp '2026-09-05 12:30:00.123456'"),
                new OracleTemporalLiteral("TIMESTAMPTZ", "TIMESTAMP(6) WITH TIME ZONE",
                        OffsetDateTime.parse("2026-09-05T12:30:00.123456+08:00"),
                        "timestamp '2026-09-05 12:30:00.123456 +08:00'"),
                new OracleTemporalLiteral("TIMESTAMPTZ", "TIMESTAMP(6) WITH TIME ZONE",
                        Instant.parse("2026-09-05T04:30:00Z"),
                        "timestamp '2026-09-05 04:30:00 +00:00'"));
    }

    private record OracleTemporalLiteral(String type, String physicalType, Object value, String sqlLiteral) {
        private ColumnDefinition column() {
            ColumnDefinition.Builder builder = ColumnDefinition.builder("value", type)
                    .defaultValue(ColumnDefault.literal(value));
            if (!"DATE".equals(type)) {
                builder.temporalPrecision(6);
            }
            return builder.build();
        }
    }
}
