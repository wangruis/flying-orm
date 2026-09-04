package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDialectLiteralSemanticsTest {

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
}
