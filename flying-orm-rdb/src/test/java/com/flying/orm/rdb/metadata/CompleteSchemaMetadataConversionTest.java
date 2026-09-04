package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotFingerprint;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompleteSchemaMetadataConversionTest {

    @Test
    void convertsEveryObservedRelationalFactIntoOneCompleteSnapshot() {
        RelationIdentity identity = RelationIdentity.of(null, "app", "orders");
        SchemaSnapshot snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(
                identity,
                columns(),
                List.of(row("TABLE_COMMENT", "订单")),
                List.of(row("CONSTRAINT_NAME", "pk_orders", "COLUMN_NAME", "id")),
                List.of(row("CONSTRAINT_NAME", "uk_orders_status", "COLUMN_NAME", "status")),
                List.of(row("INDEX_NAME", "ix_orders_total", "COLUMN_NAME", "total",
                            "UNIQUE_INDEX", false, "INDEX_REPRESENTABLE", true,
                            "INDEX_DIRECTION", "DESC")),
                List.of(row("TABLE_SCHEMA", "app", "FOREIGN_KEY_NAME", "fk_orders_customer",
                            "COLUMN_NAME", "customer_id", "REFERENCED_TABLE_SCHEMA", "crm",
                            "REFERENCED_TABLE_NAME", "customers", "REFERENCED_COLUMN_NAME", "id",
                            "ON_DELETE", "CASCADE", "ON_UPDATE", "NO_ACTION")),
                List.of(row("CONSTRAINT_NAME", "ck_orders_total",
                            "CHECK_EXPRESSION", "(\"total\" >= 0::numeric)",
                            "CHECK_REPRESENTABLE", true)),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);

        assertTrue(snapshot.completeTable().isPresent());
        assertEquals(SchemaSnapshotFingerprint.of(SchemaSnapshot.present(expected(identity))),
                     SchemaSnapshotFingerprint.of(snapshot));
    }

    @Test
    void restoresPostgresqlAnyArrayAsTheOriginalControlledInPredicate() {
        CheckPredicate predicate = RelationalMetadataValueParser.checkPredicate(
                "(\"status\" = ANY (ARRAY['NEW'::character varying, 'DONE'::character varying]))",
                Map.of("status", com.flying.orm.core.type.DatabaseType.of("VARCHAR")),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);

        assertEquals(CheckPredicate.in("status", List.of("NEW", "DONE")), predicate);
    }

    @Test
    void restoresPostgresqlParenthesizedNumericLiteralCast() {
        CheckPredicate predicate = RelationalMetadataValueParser.checkPredicate(
                "(\"total\" >= (0)::numeric)",
                Map.of("total", com.flying.orm.core.type.DatabaseType.of("DECIMAL")),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);

        assertEquals(CheckPredicate.compare(
                "total", CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, BigDecimal.ZERO), predicate);
    }

    @Test
    void restoresH2NumericLiteralCastAddedByTheDatabaseDictionary() {
        CheckPredicate predicate = RelationalMetadataValueParser.checkPredicate(
                "\"TOTAL\" >= CAST(0 AS NUMERIC(1))",
                Map.of("total", com.flying.orm.core.type.DatabaseType.of("DECIMAL")),
                InformationSchemaFormMetadataReader.SnapshotDialect.H2);

        assertEquals(CheckPredicate.compare(
                "total", CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, BigDecimal.ZERO), predicate);
    }

    @Test
    void rejectsPostgresqlCompositeDefaultInsteadOfTruncatingTheFirstLiteral() {
        assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.columnDefault(
                "('x'::text || 'y'::text)",
                com.flying.orm.core.type.DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL));
    }

    @Test
    void preservesMysqlEmptyStringDefaultDuringMetadataConversion() {
        SchemaSnapshot snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.table("settings"),
                List.of(row("COLUMN_NAME", "value", "DATA_TYPE", "VARCHAR",
                            "CHARACTER_MAXIMUM_LENGTH", 32, "NULLABLE", false,
                            "COLUMN_DEFAULT", "")),
                List.of(row("TABLE_COMMENT", null)),
                List.of(), List.of(), List.of(), List.of(), List.of(),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL);

        assertEquals(ColumnDefault.literal(""),
                     snapshot.completeTable().orElseThrow().columns().getFirst().defaultValue());
    }

    @Test
    void preservesMysqlRawStringDefaultWithoutParsingItsContentsAsSql() {
        for (String literal : List.of("  padded  ", " ", "(literal)", "'literal'", "O'Brien")) {
            assertEquals(ColumnDefault.literal(literal), RelationalMetadataValueParser.columnDefault(
                    literal, com.flying.orm.core.type.DatabaseType.of("VARCHAR"),
                    InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        }
    }

    @Test
    void restoresMysqlPrecisionQualifiedCurrentTimestampDefault() {
        assertEquals(ColumnDefault.currentTimestamp(), RelationalMetadataValueParser.columnDefault(
                "CURRENT_TIMESTAMP(6)",
                com.flying.orm.core.type.DatabaseType.of("TIMESTAMP"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }

    @Test
    void restoresSqlServerGetDateAsCurrentTimestampDefault() {
        assertEquals(ColumnDefault.currentTimestamp(), RelationalMetadataValueParser.columnDefault(
                "(getdate())",
                com.flying.orm.core.type.DatabaseType.of("TIMESTAMP"),
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void restoresSqlServerParenthesizedEmptyStringDefault() {
        assertEquals(ColumnDefault.literal(""), RelationalMetadataValueParser.columnDefault(
                "('')",
                com.flying.orm.core.type.DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void restoresSqlServerParenthesizedStringDefaultWithAnEscapedQuote() {
        assertEquals(ColumnDefault.literal("O'Brien"), RelationalMetadataValueParser.columnDefault(
                "('O''Brien')",
                com.flying.orm.core.type.DatabaseType.of("VARCHAR"),
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER));
    }

    @Test
    void keepsMysqlTemporalKeywordTextAsAStringDefaultForStringColumns() {
        for (String keyword : List.of(
                "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                "CURRENT_TIME(6)", "CURRENT_TIMESTAMP(6)")) {
            assertEquals(ColumnDefault.literal(keyword),
                         RelationalMetadataValueParser.columnDefault(
                                 keyword,
                                 com.flying.orm.core.type.DatabaseType.of("VARCHAR"),
                                 InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        }
    }

    @Test
    void restoresMysqlTemporalKeywordsForTheirTemporalColumnTypes() {
        assertEquals(ColumnDefault.currentDate(), RelationalMetadataValueParser.columnDefault(
                "CURRENT_DATE", com.flying.orm.core.type.DatabaseType.of("DATE"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        assertEquals(ColumnDefault.currentTime(), RelationalMetadataValueParser.columnDefault(
                "CURRENT_TIME", com.flying.orm.core.type.DatabaseType.of("TIME"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
        assertEquals(ColumnDefault.currentTimestamp(), RelationalMetadataValueParser.columnDefault(
                "CURRENT_TIMESTAMP", com.flying.orm.core.type.DatabaseType.of("TIMESTAMP"),
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }

    @Test
    void restoresNumericBooleanCheckLiteralsUsedBySqlServerAndOracle() {
        for (InformationSchemaFormMetadataReader.SnapshotDialect dialect : List.of(
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER,
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE)) {
            assertEquals(CheckPredicate.compare(
                            "enabled", CheckPredicate.ComparisonOperator.EQUAL, true),
                         RelationalMetadataValueParser.checkPredicate(
                                 "(\"enabled\"=(1))",
                                 Map.of("enabled", com.flying.orm.core.type.DatabaseType.of("BOOLEAN")),
                                 dialect));
            assertEquals(CheckPredicate.compare(
                            "enabled", CheckPredicate.ComparisonOperator.EQUAL, false),
                         RelationalMetadataValueParser.checkPredicate(
                                 "(\"enabled\"=(0))",
                                 Map.of("enabled", com.flying.orm.core.type.DatabaseType.of("BOOLEAN")),
                                 dialect));
        }
    }

    @Test
    void rejectsNumericBooleanCheckLiteralsOtherThanZeroAndOne() {
        for (InformationSchemaFormMetadataReader.SnapshotDialect dialect : List.of(
                InformationSchemaFormMetadataReader.SnapshotDialect.SQL_SERVER,
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE)) {
            assertThrows(IllegalStateException.class, () -> RelationalMetadataValueParser.checkPredicate(
                    "(\"enabled\"=(2))",
                    Map.of("enabled", com.flying.orm.core.type.DatabaseType.of("BOOLEAN")),
                    dialect));
        }
    }

    @Test
    void restoresRenderedRangeAsTheOriginalControlledRangePredicate() {
        CheckPredicate predicate = RelationalMetadataValueParser.checkPredicate(
                "(\"score\" > 1 and \"score\" <= 10)",
                Map.of("score", com.flying.orm.core.type.DatabaseType.of("INTEGER")),
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);

        assertEquals(CheckPredicate.range("score", 1, false, 10, true), predicate);
    }

    @Test
    void rejectsAnUnrepresentableDatabaseCheckInsteadOfPublishingCompleteCoverage() {
        assertThrows(IllegalStateException.class, () -> FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.table("orders"),
                columns(),
                List.of(row("TABLE_COMMENT", null)),
                List.of(), List.of(), List.of(), List.of(),
                List.of(row("CONSTRAINT_NAME", "ck_custom",
                            "CHECK_EXPRESSION", "custom_function(total)",
                            "CHECK_REPRESENTABLE", true)),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }

    @Test
    void rejectsAnUnrepresentableColumnInsteadOfDroppingItsGenerationSemantics() {
        List<Map<String, Object>> generatedColumns = List.of(
                row("COLUMN_NAME", "calculated_total", "DATA_TYPE", "DECIMAL",
                    "NUMERIC_PRECISION", 12, "NUMERIC_SCALE", 2, "NULLABLE", false,
                    "COLUMN_REPRESENTABLE", false,
                    "UNSUPPORTED_COLUMN_REASON", "generated expression"));

        assertThrows(IllegalStateException.class, () -> FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.table("orders"),
                generatedColumns,
                List.of(row("TABLE_COMMENT", null)),
                List.of(), List.of(), List.of(), List.of(), List.of(),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL));
    }

    @Test
    void rejectsAnUnrepresentableConstraintInsteadOfDroppingItsTimingSemantics() {
        List<Map<String, Object>> primaryKey = List.of(
                row("CONSTRAINT_NAME", "pk_orders", "COLUMN_NAME", "id",
                    "CONSTRAINT_REPRESENTABLE", false));

        assertThrows(IllegalStateException.class, () -> FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.table("orders"),
                columns(),
                List.of(row("TABLE_COMMENT", null)),
                primaryKey, List.of(), List.of(), List.of(), List.of(),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL));
    }

    @Test
    void restoresPostgresqlGenerationOptionsInsteadOfReplacingThemWithDefaults() {
        List<Map<String, Object>> generatedColumns = List.of(
                row("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "NULLABLE", false,
                    "COLUMN_REPRESENTABLE", true, "IS_IDENTITY", true,
                    "GENERATION_START", 7L, "GENERATION_INCREMENT", 3L,
                    "GENERATION_CACHE", 32));

        SchemaSnapshot snapshot = FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.table("orders"),
                generatedColumns,
                List.of(row("TABLE_COMMENT", null)),
                List.of(), List.of(), List.of(), List.of(), List.of(),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);

        assertEquals(ValueGeneration.identity(7L, 3L, 32),
                     snapshot.completeTable().orElseThrow().columns().getFirst().generation());
    }

    @Test
    void rejectsAnUnrepresentableTableInsteadOfIgnoringPartitionSemantics() {
        assertThrows(IllegalStateException.class, () -> FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.table("orders"),
                columns(),
                List.of(row("TABLE_COMMENT", null, "TABLE_REPRESENTABLE", false)),
                List.of(), List.of(), List.of(), List.of(), List.of(),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.MYSQL));
    }

    @Test
    void restoresOracleDescendingIndexColumnFromItsDictionaryExpression() {
        SchemaSnapshot snapshot = snapshotWithIndex(row(
                "INDEX_NAME", "ix_orders_created", "COLUMN_NAME", "SYS_NC00006$",
                "INDEX_EXPRESSION", "\"created_at\"", "UNIQUE_INDEX", false,
                "INDEX_REPRESENTABLE", true, "INDEX_DIRECTION", "DESC"));

        assertEquals(IndexKeyPart.desc("created_at"),
                     snapshot.completeTable().orElseThrow().indexes().getFirst().keys().getFirst());
    }

    @Test
    void rejectsOracleFunctionBasedDescendingIndexExpressions() {
        assertThrows(IllegalStateException.class, () -> snapshotWithIndex(row(
                "INDEX_NAME", "ix_orders_status", "COLUMN_NAME", "SYS_NC00007$",
                "INDEX_EXPRESSION", "UPPER(\"status\")", "UNIQUE_INDEX", false,
                "INDEX_REPRESENTABLE", true, "INDEX_DIRECTION", "DESC")));
    }

    private static SchemaSnapshot snapshotWithIndex(Map<String, Object> index) {
        return FormMetadataRowConverter.toCompleteSchemaSnapshot(
                RelationIdentity.table("orders"), columns(),
                List.of(row("TABLE_COMMENT", null)),
                List.of(), List.of(), List.of(index), List.of(), List.of(),
                value -> value,
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE);
    }

    private static List<Map<String, Object>> columns() {
        return List.of(
                row("COLUMN_NAME", "id", "DATA_TYPE", "BIGINT", "NULLABLE", false,
                    "IS_IDENTITY", true),
                row("COLUMN_NAME", "customer_id", "DATA_TYPE", "BIGINT", "NULLABLE", false),
                row("COLUMN_NAME", "status", "DATA_TYPE", "VARCHAR",
                    "CHARACTER_MAXIMUM_LENGTH", 16, "NULLABLE", false, "REMARKS", "状态"),
                row("COLUMN_NAME", "total", "DATA_TYPE", "DECIMAL", "NUMERIC_PRECISION", 12,
                    "NUMERIC_SCALE", 2, "NULLABLE", false),
                row("COLUMN_NAME", "created_at", "DATA_TYPE", "TIMESTAMP",
                    "TEMPORAL_PRECISION", 6, "NULLABLE", false,
                    "COLUMN_DEFAULT", "CURRENT_TIMESTAMP"));
    }

    private static RelationalTableDefinition expected(RelationIdentity identity) {
        return RelationalTableDefinition.builder(identity)
                .comment("订单")
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                                           .nullable(false)
                                           .generation(ValueGeneration.identity())
                                           .build())
                .addColumn(ColumnDefinition.builder("customer_id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("status", "VARCHAR")
                                           .length(16).nullable(false).comment("状态").build())
                .addColumn(ColumnDefinition.builder("total", "DECIMAL")
                                           .precision(12).scale(2).nullable(false).build())
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMP")
                                           .temporalPrecision(6).nullable(false)
                                           .defaultValue(ColumnDefault.currentTimestamp()).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_orders", "id"))
                .addUnique(UniqueConstraintDefinition.of("uk_orders_status", "status"))
                .addIndex(IndexDefinition.builder("ix_orders_total")
                                         .addKey(IndexKeyPart.desc("total")).build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_orders_customer")
                                                    .addColumn("customer_id")
                                                    .reference(RelationIdentity.of(null, "crm", "customers"))
                                                    .addReferenceColumn("id")
                                                    .onDelete(ReferentialAction.CASCADE)
                                                    .build())
                .addCheck(CheckConstraintDefinition.of(
                        "ck_orders_total",
                        CheckPredicate.compare("total",
                                               CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                                               BigDecimal.ZERO)))
                .build();
    }

    private static Map<String, Object> row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return row;
    }
}
