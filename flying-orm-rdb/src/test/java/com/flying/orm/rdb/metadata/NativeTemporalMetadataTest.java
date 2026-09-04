package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 各库元数据读取器保留绝对时间与本地时间的逻辑差异。 */
class NativeTemporalMetadataTest {

    @Test
    void rejectsPostgreSqlIndexesThatCannotBeRepresentedWithoutLosingSemantics() {
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.of("email", "VARCHAR"))
                                      .build();
        Map<String, Object> partialIndex = Map.of(
                "INDEX_NAME", "uq_active_email",
                "COLUMN_NAME", "email",
                "UNIQUE_INDEX", true,
                "INDEX_REPRESENTABLE", false,
                "UNSUPPORTED_INDEX_REASON", "partial predicate");

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> FormMetadataRowConverter.toTableMetadata(
                        "users", form, List.of(partialIndex), List.of()));

        assertTrue(failure.getMessage().contains("uq_active_email"));
        assertTrue(failure.getMessage().contains("partial predicate"));
    }

    @Test
    void postgreSqlIndexProbeRepresentsBothDirectionsAndRejectsUnsupportedShapes() {
        String sql = PostgreSqlReactiveFormMetadataReader.queries()
                                                             .indexQuery()
                                                             .create(null, "users")
                                                             .sql();

        assertTrue(sql.contains("am.amname = 'btree'"));
        assertTrue(sql.contains("opc.opcdefault"));
        assertTrue(sql.contains("ix.indoption[ord.position] in (0, 3)"));
        assertTrue(sql.contains("ix.indcollation[ord.position] = a.attcollation"));
        assertTrue(sql.contains("indnullsnotdistinct"));
    }

    @Test
    void otherDialectIndexProbesRepresentBothDirectionsAndRejectUnsupportedShapes() {
        String mysql = MySqlReactiveFormMetadataReader.queries().indexQuery().create(null, "users").sql();
        assertTrue(mysql.contains("INDEX_REPRESENTABLE"));
        assertTrue(mysql.contains("s.SUB_PART is null"));
        assertTrue(mysql.contains("s.INDEX_TYPE = 'BTREE'"));
        assertTrue(mysql.contains("s.COLLATION in ('A', 'D')"));

        String oracle = OracleReactiveFormMetadataReader.queries().indexQuery().create(null, "users").sql();
        assertTrue(oracle.contains("INDEX_REPRESENTABLE"));
        assertTrue(oracle.contains("ALL_IND_EXPRESSIONS"));
        assertTrue(oracle.contains("ic.DESCEND in ('ASC', 'DESC')"));
        assertTrue(oracle.contains("i.INDEX_TYPE in ('NORMAL', 'FUNCTION-BASED NORMAL')"));

        String sqlServer = SqlServerReactiveFormMetadataReader.queries()
                                                                    .indexQuery()
                                                                    .create(null, "users")
                                                                    .sql();
        assertTrue(sqlServer.contains("INDEX_REPRESENTABLE"));
        assertTrue(sqlServer.contains("i.has_filter = 0"));
        assertTrue(sqlServer.contains("case when ic.is_descending_key = 0 then 'ASC' else 'DESC' end"));
        assertTrue(sqlServer.contains("included.is_included_column = 1"));
    }

    @Test
    void indexProbesRejectOperationalAndPartitionShapesMissingFromIndexMetadata() {
        String h2 = H2ReactiveFormMetadataReader.queries().indexQuery().create(null, "users").sql();
        assertTrue(h2.contains("i.INDEX_TYPE_NAME in ('INDEX', 'UNIQUE INDEX')"));
        assertTrue(h2.contains("ic.ORDERING_SPECIFICATION = 'ASC'"));

        String oracle = OracleReactiveFormMetadataReader.queries().indexQuery().create(null, "users").sql();
        assertTrue(oracle.contains("i.PARTITIONED = 'NO'"));
        assertTrue(oracle.contains("i.VISIBILITY = 'VISIBLE'"));
        assertTrue(oracle.contains("i.STATUS = 'VALID'"));

        String sqlServer = SqlServerReactiveFormMetadataReader.queries()
                                                                    .indexQuery()
                                                                    .create(null, "users")
                                                                    .sql();
        assertTrue(sqlServer.contains("partitioned.partition_ordinal > 0"));
    }

    @Test
    void preservesNativeAbsoluteTimestampTypes() {
        assertEquals("TIMESTAMPTZ", H2ReactiveFormMetadataReader.queries()
                .typeMapper().apply("TIMESTAMP WITH TIME ZONE"));
        assertEquals("TIMESTAMPTZ", PostgreSqlReactiveFormMetadataReader.queries()
                .typeMapper().apply("timestamp with time zone"));
        assertEquals("TIMESTAMPTZ", OracleReactiveFormMetadataReader.queries()
                .typeMapper().apply("TIMESTAMP WITH TIME ZONE"));
        assertEquals("TIMESTAMPTZ", OracleReactiveFormMetadataReader.queries()
                .typeMapper().apply("TIMESTAMP(6) WITH TIME ZONE"));
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", OracleReactiveFormMetadataReader.queries()
                .typeMapper().apply("TIMESTAMP(6) WITH LOCAL TIME ZONE"));
        assertEquals("TIMESTAMPTZ", SqlServerReactiveFormMetadataReader.queries()
                .typeMapper().apply("datetimeoffset"));
        assertEquals("TIMESTAMPTZ", SqlServerReactiveFormMetadataReader.queries()
                .typeMapper().apply("datetimeoffset(7)"));
    }

    @Test
    void keepsNativeLocalTimestampTypesWithoutAnOffset() {
        assertEquals("TIMESTAMP", H2ReactiveFormMetadataReader.queries()
                .typeMapper().apply("TIMESTAMP WITHOUT TIME ZONE"));
        assertEquals("TIMESTAMP", PostgreSqlReactiveFormMetadataReader.queries()
                .typeMapper().apply("timestamp without time zone"));
        assertEquals("TIMESTAMP", OracleReactiveFormMetadataReader.queries()
                .typeMapper().apply("TIMESTAMP"));
        assertEquals("TIMESTAMP", SqlServerReactiveFormMetadataReader.queries()
                .typeMapper().apply("datetime2"));
        assertEquals("TIMESTAMPTZ", MySqlReactiveFormMetadataReader.queries()
                .typeMapper().apply("timestamp"));
        assertEquals("TIMESTAMP", MySqlReactiveFormMetadataReader.queries()
                .typeMapper().apply("datetime"));
    }

    @Test
    void preservesLegacySqlServerTimestampStorageIdentity() {
        assertEquals("SQLSERVER_DATETIME", SqlServerReactiveFormMetadataReader.queries()
                .typeMapper().apply("datetime"));
        assertEquals("SQLSERVER_SMALLDATETIME", SqlServerReactiveFormMetadataReader.queries()
                .typeMapper().apply("smalldatetime"));
        assertEquals("ROWVERSION", SqlServerReactiveFormMetadataReader.queries()
                .typeMapper().apply("timestamp"));
        assertEquals("DATETIME", RdbDialect.sqlServer().schema().dataType("SQLSERVER_DATETIME"));
        assertEquals("SMALLDATETIME", RdbDialect.sqlServer().schema().dataType("SQLSERVER_SMALLDATETIME"));
        assertEquals("ROWVERSION", RdbDialect.sqlServer().schema().dataType("ROWVERSION"));
    }

    @Test
    void keepsOracleDateTimeOfDayWithoutBreakingPhysicalDateRoundTrip() {
        assertEquals("ORACLE_DATE", OracleReactiveFormMetadataReader.queries()
                .typeMapper().apply("DATE"));

        DynamicForm form = FormMetadataRowConverter.toDynamicForm(
                "business_calendar", "business_calendar", List.of(
                        Map.of("COLUMN_NAME", "business_date",
                               "DATA_TYPE", "DATE",
                               "PRIMARY_KEY", false,
                               "NULLABLE", "YES")),
                OracleReactiveFormMetadataReader.queries().typeMapper());

        assertEquals("ORACLE_DATE", form.field("business_date").dataType());
        assertEquals("DATE", RdbDialect.oracle().schema().dataType(
                form.toTableMetadata().column("business_date").dataType()));
    }

    @Test
    void preservesPostgreSqlArrayElementTypeArguments() {
        InformationSchemaFormMetadataReader.Queries queries = PostgreSqlReactiveFormMetadataReader.queries();
        String sql = queries.columnQuery().create("public", "events").sql();

        assertTrue(sql.contains("pg_catalog.format_type"));
        assertTrue(sql.contains("column_attribute.atttypmod"));
        assertEquals("TIMESTAMPTZ(3)[]",
                     queries.typeMapper().apply("timestamp(3) with time zone[]"));
        assertEquals("TIMESTAMP(2)[]",
                     queries.typeMapper().apply("timestamp(2) without time zone[]"));
        assertEquals("TIMETZ(1)[]",
                     queries.typeMapper().apply("time(1) with time zone[]"));
        assertEquals("VARCHAR(64)[]",
                     queries.typeMapper().apply("character varying(64)[]"));
        assertEquals("DECIMAL(12,3)[]",
                     queries.typeMapper().apply("numeric(12, 3)[]"));
        assertEquals("SMALLINT[]", queries.typeMapper().apply("smallint[]"));
    }

    @Test
    void preservesPostgreSqlBitStringLengthsAcrossMetadataRoundTrips() {
        InformationSchemaFormMetadataReader.Queries queries = PostgreSqlReactiveFormMetadataReader.queries();
        String sql = queries.columnQuery().create("public", "events").sql().toLowerCase(java.util.Locale.ROOT);

        assertTrue(sql.contains("c.data_type in ('bit', 'bit varying')"));
        assertPostgreSqlBitRoundTrip(queries, "bit(8)", "BIT(8)");
        assertPostgreSqlBitRoundTrip(queries, "bit varying(8)", "BIT VARYING(8)");
    }

    @Test
    void preservesMySqlTemporalPrecisionWithoutTreatingItAsNumericScale() {
        String sql = MySqlReactiveFormMetadataReader.queries()
                                                       .columnQuery()
                                                       .create(null, "events")
                                                       .sql();
        assertTrue(sql.contains("c.DATETIME_PRECISION as TEMPORAL_PRECISION"));

        DynamicForm form = FormMetadataRowConverter.toDynamicForm(
                "events", "events", List.of(
                        Map.of("COLUMN_NAME", "created_at",
                               "DATA_TYPE", "timestamp",
                               "TEMPORAL_PRECISION", 6,
                               "PRIMARY_KEY", false,
                               "NULLABLE", "YES"),
                        Map.of("COLUMN_NAME", "processed_at",
                               "DATA_TYPE", "timestamp",
                               "TEMPORAL_PRECISION", 0,
                               "PRIMARY_KEY", false,
                               "NULLABLE", "YES")),
                MySqlReactiveFormMetadataReader.queries().typeMapper());

        assertEquals("TIMESTAMPTZ", form.field("created_at").dataType());
        assertEquals(6, form.field("created_at").precision());
        assertNull(form.field("created_at").scale());
        assertEquals(0, form.field("processed_at").precision());
        assertEquals(0, form.toTableMetadata().column("processed_at").precision());
        assertEquals("TIMESTAMP(0)",
                     RdbDialect.mysql().schema().dataType("TIMESTAMPTZ", null, 0, null));
        assertThrows(IllegalArgumentException.class,
                     () -> DynamicField.of("processed_at", "TIMESTAMPTZ").withPrecision(0, 0));
        assertThrows(IllegalArgumentException.class,
                     () -> ColumnMetadata.of("processed_at", "TIMESTAMPTZ").withPrecision(0, 0));
        assertThrows(IllegalArgumentException.class,
                     () -> DynamicField.of("amount", "DECIMAL").withPrecision(0, 0));
    }

    @Test
    void preservesMysqlIntegerModifiersAcrossMetadataRoundTrips() {
        InformationSchemaFormMetadataReader.Queries queries = MySqlReactiveFormMetadataReader.queries();
        String sql = queries.columnQuery().create(null, "devices").sql().toLowerCase(java.util.Locale.ROOT);

        assertTrue(sql.contains("c.data_type"));
        assertTrue(sql.contains("then c.column_type"));
        assertTrue(sql.contains("'bit'"));
        assertTrue(sql.contains("'binary'"));
        assertTrue(sql.contains("'varbinary'"));
        assertFalse(sql.contains("when lower(c.column_type) = 'tinyint(1)' then 'boolean'"));
        assertEquals("TINYINT(1)", queries.typeMapper().apply("tinyint(1)"));
        assertEquals("TINYINT UNSIGNED", queries.typeMapper().apply("tinyint(1) unsigned"));
        assertEquals("SMALLINT UNSIGNED", queries.typeMapper().apply("smallint(6) unsigned"));
        assertEquals("MEDIUMINT UNSIGNED", queries.typeMapper().apply("mediumint(8) unsigned"));
        assertEquals("INT UNSIGNED", queries.typeMapper().apply("int(11) unsigned"));
        assertEquals("BIGINT UNSIGNED ZEROFILL",
                     queries.typeMapper().apply("bigint(20) unsigned zerofill"));
        assertEquals("BIT(1)", queries.typeMapper().apply("bit(1)"));
        assertEquals("BIT(8)", queries.typeMapper().apply("bit(8)"));
        assertEquals("MYSQL_BINARY", queries.typeMapper().apply("binary(16)"));
        assertEquals("VARBINARY(64)", queries.typeMapper().apply("varbinary(64)"));
        assertEquals("TINYBLOB", queries.typeMapper().apply("tinyblob"));
        assertEquals("MYSQL_BLOB", queries.typeMapper().apply("blob"));
        assertEquals("MEDIUMBLOB", queries.typeMapper().apply("mediumblob"));
        assertEquals("BLOB", queries.typeMapper().apply("longblob"));

        var schema = RdbDialect.mysql().schema();
        String readbackType = schema.dataType(queries.typeMapper().apply("int(11) unsigned"));
        assertEquals("INT UNSIGNED", readbackType);
        assertEquals(readbackType, schema.dataType("INT UNSIGNED"));
        assertEquals("INT UNSIGNED ZEROFILL", schema.dataType("INTEGER UNSIGNED ZEROFILL"));
        assertFalse(schema.dataType("INTEGER").equals(readbackType));

        assertMysqlIntegerRoundTrip(queries, "tinyint(1) unsigned", "TINYINT(1) UNSIGNED");
        assertMysqlIntegerRoundTrip(queries, "BOOLEAN", "TINYINT(1)");
        assertMysqlIntegerRoundTrip(queries, "BOOLEAN", "BOOL");
        assertMysqlIntegerRoundTrip(queries, "smallint(6) unsigned", "SMALLINT UNSIGNED");
        assertMysqlIntegerRoundTrip(queries, "mediumint(8) unsigned zerofill",
                                    "MEDIUMINT UNSIGNED ZEROFILL");
        assertMysqlIntegerRoundTrip(queries, "int(11) unsigned zerofill", "INTEGER(11) ZEROFILL");
        assertMysqlIntegerRoundTrip(queries, "bit(1)", "BIT");
        assertMysqlIntegerRoundTrip(queries, "bit(8)", "BIT(8)");
        assertMysqlBinaryRoundTrip(queries, "tinyblob", 255, "TINYBLOB");
        assertMysqlBinaryRoundTrip(queries, "mediumblob", 16_777_215, "MEDIUMBLOB");
        assertEquals("BINARY(16)", schema.dataType("MYSQL_BINARY", 16, null, null));
        assertMysqlBinaryRoundTrip(queries, "binary(16)", 16, "BINARY(16)");
        assertMysqlBinaryRoundTrip(queries, "varbinary(64)", 64, "VARBINARY(64)");

        DynamicForm currentBlob = metadataForm(queries, "blob", 65_535);
        DynamicForm targetLongBlob = DynamicForm.builder("counters", "counters")
                                                .addField(DynamicField.of("value", "BLOB"))
                                                .build();
        var blobWidening = FormSchemaSqlRenderer.create(schema)
                                                .migrateSafelyPlan(currentBlob.toTableMetadata(),
                                                                   targetLongBlob, List.of());
        assertTrue(blobWidening.hasExecutableSql() || blobWidening.requiresManualReview());

        DynamicForm currentTinyint = metadataForm(queries, "tinyint", null);
        DynamicForm targetInteger = DynamicForm.builder("counters", "counters")
                                               .addField(DynamicField.of("value", "INTEGER"))
                                               .build();
        var widening = FormSchemaSqlRenderer.create(schema)
                                            .migrateSafelyPlan(currentTinyint.toTableMetadata(),
                                                               targetInteger, List.of());
        assertTrue(widening.hasExecutableSql() || widening.requiresManualReview());
    }

    @Test
    void keepsFrameworkLargeObjectSchemasIdempotentAfterMetadataReadback() {
        assertLobRoundTrip(RdbDialect.h2(), H2ReactiveFormMetadataReader.queries(), "CLOB", "CLOB");
        assertLobRoundTrip(RdbDialect.h2(), H2ReactiveFormMetadataReader.queries(), "TEXT", "TEXT");
        assertLobRoundTrip(RdbDialect.h2(), H2ReactiveFormMetadataReader.queries(),
                           "TEXT", "VARCHAR(1000000000)");
        assertLobRoundTrip(RdbDialect.h2(), H2ReactiveFormMetadataReader.queries(),
                           "CHARACTER LARGE OBJECT", "NCLOB");
        assertLobRoundTrip(RdbDialect.mysql(), MySqlReactiveFormMetadataReader.queries(), "longtext", "CLOB");
        assertLobRoundTrip(RdbDialect.oracle(), OracleReactiveFormMetadataReader.queries(), "NCLOB", "NCLOB");
        assertLobRoundTrip(RdbDialect.sqlServer(), SqlServerReactiveFormMetadataReader.queries(),
                           "nvarchar(max)", "TEXT");
        assertSqlServerBinaryRoundTrip(64);

        String sql = SqlServerReactiveFormMetadataReader.queries()
                                                          .columnQuery()
                                                          .create(null, "documents")
                                                          .sql()
                                                          .toLowerCase(java.util.Locale.ROOT);
        assertTrue(sql.contains("character_maximum_length = -1"));
        String h2Sql = H2ReactiveFormMetadataReader.queries()
                                                         .columnQuery()
                                                         .create(null, "documents")
                                                         .sql()
                                                         .toLowerCase(java.util.Locale.ROOT);
        assertTrue(h2Sql.contains("character_maximum_length = 1000000000"));
    }

    @Test
    void preservesSqlServerMaxTypesWithoutApplyingTheUnlimitedLengthSentinel() {
        InformationSchemaFormMetadataReader.Queries queries = SqlServerReactiveFormMetadataReader.queries();
        String sql = queries.columnQuery().create(null, "documents").sql().toLowerCase(java.util.Locale.ROOT);

        assertTrue(sql.contains("in ('nvarchar', 'varchar')"));
        assertEquals("VARCHAR(MAX)", queries.typeMapper().apply("varchar(max)"));
        assertEquals("BLOB", queries.typeMapper().apply("varbinary(max)"));
        assertEquals("VARCHAR(MAX)", metadataForm(queries, "varchar(max)", -1).field("value").dataType());
        assertEquals("BLOB", metadataForm(queries, "varbinary(max)", -1).field("value").dataType());
        assertEquals("VARCHAR(MAX)", RdbDialect.sqlServer().schema().dataType("VARCHAR(MAX)"));
    }

    @Test
    void keepsTextBackedOffsetTimeCapacityWhenLogicalPrecisionIsPresent() {
        assertEquals("VARCHAR2(32)",
                     RdbDialect.oracle().schema().dataType("OFFSET_TIME", null, 0, null));
        assertEquals("VARCHAR2(32)",
                     RdbDialect.oracle().schema().dataType("OFFSET_TIME", null, 6, null));
        assertEquals("VARCHAR(32)",
                     RdbDialect.mysql().schema().dataType("OFFSET_TIME", null, 6, null));
        assertEquals("VARCHAR(32)",
                     RdbDialect.sqlServer().schema().dataType("OFFSET_TIME", null, 6, null));
    }

    @Test
    void permitsZeroFractionalPrecisionForTemporalArrays() {
        for (String dataType : List.of("TIME[]", "TIMETZ[]", "TIMESTAMP[]", "TIMESTAMPTZ[]")) {
            DynamicField field = DynamicField.of("event_times", dataType).withPrecision(0, null);
            ColumnMetadata column = ColumnMetadata.of("event_times", dataType).withPrecision(0, null);

            assertEquals(0, field.precision());
            assertEquals(0, column.precision());
            assertTrue(RdbDialect.postgresql().schema()
                                 .dataType(dataType, null, 0, null)
                                 .contains("(0)[]"));
        }
    }

    @Test
    void everyNativeTemporalMetadataReaderProjectsTemporalPrecision() {
        assertTemporalPrecisionProjection(H2ReactiveFormMetadataReader.queries()
                                                                                 .columnQuery()
                                                                                 .create(null, "events")
                                                                                 .sql());
        assertTemporalPrecisionProjection(PostgreSqlReactiveFormMetadataReader.queries()
                                                                                         .columnQuery()
                                                                                         .create(null, "events")
                                                                                         .sql());
        assertTemporalPrecisionProjection(OracleReactiveFormMetadataReader.queries()
                                                                                     .columnQuery()
                                                                                     .create(null, "events")
                                                                                     .sql());
        assertTemporalPrecisionProjection(SqlServerReactiveFormMetadataReader.queries()
                                                                                        .columnQuery()
                                                                                        .create(null, "events")
                                                                                        .sql());

        DynamicForm oracleForm = FormMetadataRowConverter.toDynamicForm(
                "events", "events", List.of(
                        Map.of("COLUMN_NAME", "created_at",
                               "DATA_TYPE", "TIMESTAMP(3) WITH TIME ZONE",
                               "PRIMARY_KEY", false,
                               "NULLABLE", "YES"),
                        Map.of("COLUMN_NAME", "session_at",
                               "DATA_TYPE", "TIMESTAMP WITH LOCAL TIME ZONE",
                               "TEMPORAL_PRECISION", 4,
                               "PRIMARY_KEY", false,
                               "NULLABLE", "YES")),
                OracleReactiveFormMetadataReader.queries().typeMapper());
        assertEquals(3, oracleForm.field("created_at").precision());
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", oracleForm.field("session_at").dataType());
        assertEquals(4, oracleForm.field("session_at").precision());
        ColumnMetadata sessionColumn = oracleForm.toTableMetadata().column("session_at");
        assertEquals("TIMESTAMP WITH LOCAL TIME ZONE", sessionColumn.dataType());
        assertEquals(4, sessionColumn.precision());
    }

    @Test
    void oracleMetadataQueryRestoresTextBackedTimeIdentity() {
        String sql = OracleReactiveFormMetadataReader.queries()
                                                      .columnQuery()
                                                      .create(null, "events")
                                                      .sql();
        String normalizedSql = sql.toLowerCase(java.util.Locale.ROOT);

        assertTrue(sql.contains("[[flying-orm:v1:TIME]]"));
        assertTrue(sql.contains("[[flying-orm:v1:OFFSET_TIME]]"));
        assertTrue(sql.contains("[[flying-orm:v1:COMMENT]]"));
        assertTrue(sql.contains("then 'TIME'"));
        assertTrue(sql.contains("then 'OFFSET_TIME'"));
        assertTrue(normalizedSql.contains("nullif(substr(case when"));
        assertTrue(normalizedSql.contains("'nls_sort=binary'"));
        assertFalse(normalizedSql.contains("cc.comments like '[[flying-orm:v1:"));
    }

    @Test
    void mysqlAndSqlServerMetadataQueriesRestoreTextBackedOffsetTimeIdentity() {
        String mysql = MySqlReactiveFormMetadataReader.queries()
                                                       .columnQuery()
                                                       .create(null, "events")
                                                       .sql();
        String sqlServer = SqlServerReactiveFormMetadataReader.queries()
                                                               .columnQuery()
                                                               .create(null, "events")
                                                               .sql();

        for (String sql : List.of(mysql, sqlServer)) {
            assertTrue(sql.contains("[[flying-orm:v1:OFFSET_TIME]]"));
            assertTrue(sql.contains("[[flying-orm:v1:COMMENT]]"));
            assertTrue(sql.contains("then 'OFFSET_TIME'"));
        }
        assertTrue(mysql.contains("binary left(c.COLUMN_COMMENT"));
        assertTrue(sqlServer.contains("convert(varbinary(128), left(cast(ep.value"));
    }

    private static void assertTemporalPrecisionProjection(String sql) {
        assertTrue(sql.toUpperCase(java.util.Locale.ROOT).contains("TEMPORAL_PRECISION"));
    }

    private static void assertLobRoundTrip(RdbDialect dialect,
                                           InformationSchemaFormMetadataReader.Queries queries,
                                           String physicalType,
                                           String targetType) {
        DynamicForm currentForm = FormMetadataRowConverter.toDynamicForm(
                "documents", "documents", List.of(
                        Map.of("COLUMN_NAME", "content",
                               "DATA_TYPE", physicalType,
                               "CHARACTER_MAXIMUM_LENGTH", -1,
                               "PRIMARY_KEY", false,
                               "NULLABLE", "YES")),
                queries.typeMapper());
        DynamicForm target = DynamicForm.builder("documents", "documents")
                                        .addField(DynamicField.of("content", targetType))
                                        .build();

        var plan = FormSchemaSqlRenderer.create(dialect.schema())
                                        .migrateSafelyPlan(currentForm.toTableMetadata(), target, List.of());
        assertFalse(plan.hasExecutableSql(), plan.sqlTexts().toString());
        assertFalse(plan.requiresManualReview(), plan.skippedSummaries().toString());
    }

    private static void assertMysqlIntegerRoundTrip(InformationSchemaFormMetadataReader.Queries queries,
                                                    String physicalType,
                                                    String targetType) {
        DynamicForm current = metadataForm(queries, physicalType, null);
        DynamicForm target = DynamicForm.builder("counters", "counters")
                                        .addField(DynamicField.of("value", targetType))
                                        .build();
        var plan = FormSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                                        .migrateSafelyPlan(current.toTableMetadata(), target, List.of());
        assertFalse(plan.hasExecutableSql(), plan.sqlTexts().toString());
        assertFalse(plan.requiresManualReview(), plan.skippedSummaries().toString());
    }

    private static void assertMysqlBinaryRoundTrip(InformationSchemaFormMetadataReader.Queries queries,
                                                   String physicalType,
                                                   int length,
                                                   String targetType) {
        DynamicForm current = metadataForm(queries, physicalType, length);
        DynamicForm target = DynamicForm.builder("counters", "counters")
                                        .addField(DynamicField.of("value", targetType))
                                        .build();
        var plan = FormSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                                        .migrateSafelyPlan(current.toTableMetadata(), target, List.of());
        assertFalse(plan.hasExecutableSql(), plan.sqlTexts().toString());
        assertFalse(plan.requiresManualReview(), plan.skippedSummaries().toString());
    }

    private static void assertPostgreSqlBitRoundTrip(InformationSchemaFormMetadataReader.Queries queries,
                                                     String physicalType,
                                                     String targetType) {
        DynamicForm current = metadataForm(queries, physicalType, 8);
        DynamicForm target = DynamicForm.builder("counters", "counters")
                                        .addField(DynamicField.of("value", targetType))
                                        .build();
        var plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql().schema())
                                        .migrateSafelyPlan(current.toTableMetadata(), target, List.of());
        assertFalse(plan.hasExecutableSql(), plan.sqlTexts().toString());
        assertFalse(plan.requiresManualReview(), plan.skippedSummaries().toString());
    }

    private static void assertSqlServerBinaryRoundTrip(int length) {
        var queries = SqlServerReactiveFormMetadataReader.queries();
        DynamicForm current = metadataForm(queries, "varbinary", length);
        DynamicForm target = DynamicForm.builder("counters", "counters")
                                        .addField(DynamicField.of("value", "BLOB").withLength(length))
                                        .build();
        var schema = RdbDialect.sqlServer().schema();
        assertEquals("VARBINARY(" + length + ")", schema.dataType("BLOB", length, null, null));
        var plan = FormSchemaSqlRenderer.create(schema)
                                        .migrateSafelyPlan(current.toTableMetadata(), target, List.of());
        assertFalse(plan.hasExecutableSql(), plan.sqlTexts().toString());
        assertFalse(plan.requiresManualReview(), plan.skippedSummaries().toString());
    }

    private static DynamicForm metadataForm(InformationSchemaFormMetadataReader.Queries queries,
                                            String dataType,
                                            Integer length) {
        java.util.HashMap<String, Object> row = new java.util.HashMap<>();
        row.put("COLUMN_NAME", "value");
        row.put("DATA_TYPE", dataType);
        row.put("PRIMARY_KEY", false);
        row.put("NULLABLE", "YES");
        if (length != null) {
            row.put("CHARACTER_MAXIMUM_LENGTH", length);
        }
        return FormMetadataRowConverter.toDynamicForm(
                "counters", "counters", List.of(row), queries.typeMapper());
    }
}
