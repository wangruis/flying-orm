package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.form.FieldChange;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 数据库默认时间精度与裸类型在迁移比较中应视为相同物理语义。 */
class SchemaDialectTemporalPrecisionTest {

    @Test
    void doesNotTreatIntervalPrecisionChangesAsSafeNumericWidening() {
        assertFalse(SchemaDialectTypeSupport.safeWideningDataType(
                "INTERVAL DAY(2) TO SECOND(3)", "INTERVAL DAY(3) TO SECOND(4)"));
    }

    @Test
    void ignoresOnlyMysqlIntegerDisplayWidthDuringPhysicalComparison() {
        SchemaDialect mysql = RdbDialect.mysql().schema();

        assertEquals(SchemaDialect.GeneratedValueStyle.MYSQL, mysql.generatedValueStyle());
        assertTrue(mysql.sameDataType(mysql.dataType("BOOLEAN"), mysql.dataType("TINYINT(1)")));
        assertTrue(mysql.sameDataType(mysql.dataType("BOOL"), mysql.dataType("TINYINT(1)")));
        assertFalse(mysql.sameDataType(mysql.dataType("BOOLEAN"), mysql.dataType("TINYINT(1) UNSIGNED")));
        assertTrue(mysql.sameDataType("TINYINT UNSIGNED", "TINYINT(1) UNSIGNED"));
        assertTrue(mysql.sameDataType("SMALLINT UNSIGNED", "SMALLINT(6) UNSIGNED"));
        assertTrue(mysql.sameDataType("MEDIUMINT UNSIGNED ZEROFILL", "MEDIUMINT(8) UNSIGNED ZEROFILL"));
        assertTrue(mysql.sameDataType("INT ZEROFILL", "INT UNSIGNED ZEROFILL"));
        assertTrue(mysql.sameDataType("INT", "INTEGER(11)"));
        assertTrue(mysql.sameDataType("BIT", "BIT(1)"));
        assertFalse(mysql.sameDataType("BOOLEAN", "BIT(1)"));
        assertFalse(mysql.sameDataType("TINYINT", "INT"));

        SchemaDialect h2 = RdbDialect.h2().schema();
        assertTrue(h2.sameDataType("TEXT", "VARCHAR(1000000000)"));
        assertTrue(h2.sameDataType("TEXT", "VARCHAR"));
        assertFalse(h2.sameDataType("TEXT", "VARCHAR(255)"));
    }

    @Test
    void treatsDialectDefaultTemporalPrecisionAsEquivalent() {
        assertTrue(RdbDialect.h2().schema().sameDataType("TIME", "TIME(0)"));
        assertTrue(RdbDialect.h2().schema()
                                   .sameDataType("TIME WITH TIME ZONE", "TIME(0) WITH TIME ZONE"));
        assertTrue(RdbDialect.h2().schema().sameDataType("TIMESTAMP", "TIMESTAMP(6)"));
        assertTrue(RdbDialect.h2().schema()
                                   .sameDataType("TIMESTAMP WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE"));
        assertTrue(RdbDialect.postgresql().schema().sameDataType("TIMESTAMPTZ", "TIMESTAMPTZ(6)"));
        assertTrue(RdbDialect.oracle().schema()
                                       .sameDataType("TIMESTAMP WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE"));
        assertTrue(RdbDialect.sqlServer().schema().sameDataType("DATETIMEOFFSET", "DATETIMEOFFSET(7)"));
        assertTrue(RdbDialect.mysql().schema().sameDataType("TIME", "TIME(0)"));
        assertTrue(RdbDialect.mysql().schema().sameDataType("TIMESTAMP", "TIMESTAMP(0)"));
    }

    @Test
    void preservesNonDefaultTemporalPrecisionDifferences() {
        assertFalse(RdbDialect.h2().schema().sameDataType("TIME", "TIME(6)"));
        assertFalse(RdbDialect.h2().schema()
                                    .sameDataType("TIME WITH TIME ZONE", "TIME(6) WITH TIME ZONE"));
        assertFalse(RdbDialect.h2().schema().sameDataType("TIMESTAMP", "TIMESTAMP(0)"));
        assertFalse(RdbDialect.h2().schema()
                                    .sameDataType("TIMESTAMP WITH TIME ZONE", "TIMESTAMP(0) WITH TIME ZONE"));
        assertFalse(RdbDialect.postgresql().schema().sameDataType("TIMESTAMPTZ", "TIMESTAMPTZ(3)"));
        assertFalse(RdbDialect.sqlServer().schema().sameDataType("DATETIMEOFFSET", "DATETIMEOFFSET(3)"));
        assertFalse(RdbDialect.mysql().schema().sameDataType("TIME", "TIME(3)"));
        assertFalse(RdbDialect.oracle().schema().sameDataType(
                "TIMESTAMP(6) WITH LOCAL TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE"));
    }

    @Test
    void treatsOracleIntegerNumberScaleAsTheSamePhysicalType() {
        SchemaDialect oracle = RdbDialect.oracle().schema();
        assertTrue(oracle.sameDataType("NUMBER(10)", "NUMBER(10,0)"));
        assertTrue(oracle.sameDataType("NUMBER(19)", "NUMBER(19,0)"));
        assertFalse(oracle.sameDataType("NUMBER(10)", "NUMBER(10,1)"));

        TableMetadata current = TableMetadata.builder("devices")
                .addColumn(ColumnMetadata.of("retry_count", "DECIMAL").withPrecision(10, 0))
                .addColumn(ColumnMetadata.of("event_count", "DECIMAL").withPrecision(19, 0))
                .build();
        DynamicForm target = DynamicForm.builder("devices", "devices")
                .addField(DynamicField.of("retry_count", "INTEGER"))
                .addField(DynamicField.of("event_count", "BIGINT"))
                .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(oracle)
                .migrateSafelyPlan(current, target, List.of());
        assertFalse(plan.hasExecutableSql());
        assertFalse(plan.requiresManualReview());
    }

    @Test
    void preservesOracleLocalTimeZoneThroughTableMetadataRendering() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("session_at", "TIMESTAMP WITH LOCAL TIME ZONE")
                                                            .withPrecision(4, null))
                                      .build();
        ColumnMetadata current = form.toTableMetadata().column("session_at");
        DynamicField target = DynamicField.of("session_at", "TIMESTAMPTZ").withPrecision(4, null);
        SchemaTableSqlRenderer renderer = new SchemaTableSqlRenderer(RdbDialect.oracle().schema());

        assertEquals("TIMESTAMP(4) WITH LOCAL TIME ZONE", renderer.dataType(current));
        assertEquals("TIMESTAMP(4) WITH TIME ZONE", renderer.dataType(target));
        assertFalse(renderer.sameDataType(renderer.dataType(current), renderer.dataType(target)));
    }

    @Test
    void rejectsTemporalPrecisionBeyondEachDialectLimit() {
        assertEquals("TIMESTAMP(9) WITH TIME ZONE",
                     RdbDialect.h2().schema().dataType("TIMESTAMPTZ", null, 9, null));
        assertEquals("TIMESTAMP(6)",
                     RdbDialect.mysql().schema().dataType("TIMESTAMPTZ", null, 6, null));
        assertEquals("TIMESTAMPTZ(6)",
                     RdbDialect.postgresql().schema().dataType("TIMESTAMPTZ", null, 6, null));
        assertEquals("TIMESTAMP(9) WITH TIME ZONE",
                     RdbDialect.oracle().schema().dataType("TIMESTAMPTZ", null, 9, null));
        assertEquals("DATETIMEOFFSET(7)",
                     RdbDialect.sqlServer().schema().dataType("TIMESTAMPTZ", null, 7, null));

        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.h2().schema().dataType("TIMESTAMPTZ", null, 10, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema().dataType("TIMESTAMPTZ", null, 7, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema().dataType("OFFSET_TIME", null, 10, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.postgresql().schema().dataType("TIMESTAMPTZ", null, 7, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.oracle().schema().dataType("TIMESTAMPTZ", null, 10, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.oracle().schema().dataType("OFFSET_TIME", null, 10, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.sqlServer().schema().dataType("TIMESTAMPTZ", null, 8, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.sqlServer().schema().dataType("OFFSET_TIME", null, 10, null));
    }

    @Test
    void rejectsModifiersForFixedPrecisionTemporalTypes() {
        SchemaDialect oracle = RdbDialect.oracle().schema();
        SchemaDialect sqlServer = RdbDialect.sqlServer().schema();

        assertThrows(IllegalArgumentException.class,
                     () -> oracle.dataType("DATE", null, 1, null));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.dataType("DATE(1)"));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.dataType("ORACLE_DATE", 10, null, null));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.dataType("ORACLE_DATE", null, 9, null));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.dataType("ORACLE_DATE(9)"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.dataType("SQLSERVER_DATETIME", null, 3, null));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.dataType("SQLSERVER_DATETIME(3)"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.dataType("SQLSERVER_SMALLDATETIME", null, 0, null));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.dataType("SQLSERVER_SMALLDATETIME(0)"));
        assertThrows(IllegalArgumentException.class,
                     () -> sqlServer.dataType("SMALLDATETIME(0)"));
    }

    @Test
    void mapsAndValidatesInlineTemporalPrecision() {
        assertEquals("TIMESTAMP(9) WITH TIME ZONE",
                     RdbDialect.h2().schema().dataType("TIMESTAMPTZ(9)"));
        assertEquals("TIMESTAMP(6)",
                     RdbDialect.mysql().schema().dataType("TIMESTAMPTZ(6)"));
        assertEquals("TIMESTAMPTZ(6)",
                     RdbDialect.postgresql().schema().dataType("TIMESTAMPTZ(6)"));
        assertEquals("TIMESTAMP(9) WITH TIME ZONE",
                     RdbDialect.oracle().schema().dataType("TIMESTAMPTZ(9)"));
        assertEquals("DATETIMEOFFSET(7)",
                     RdbDialect.sqlServer().schema().dataType("TIMESTAMPTZ(7)"));
        assertEquals("VARCHAR2(32)",
                     RdbDialect.oracle().schema().dataType("OFFSET_TIME(9)"));
        assertEquals("TIMESTAMP(3)",
                     RdbDialect.mysql().schema().dataType("TIMESTAMPTZ(3)", null, 3, null));

        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.h2().schema().dataType("TIMESTAMPTZ(10)"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema().dataType("TIMESTAMPTZ(7)"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.postgresql().schema().dataType("TIMESTAMPTZ(7)"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.oracle().schema().dataType("TIMESTAMPTZ(10)"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.sqlServer().schema().dataType("TIMESTAMPTZ(8)"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema().dataType("TIMESTAMPTZ(3)", null, 6, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema().dataType("TIMESTAMPTZ", 7, 6, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.postgresql().schema().dataType("TIMESTAMPTZ", null, 6, 1));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.oracle().schema().dataType("OFFSET_TIME", 10, null, null));
    }

    @Test
    void mapsStandardTimeZoneModifiersBeforeApplyingPrecision() {
        assertEquals("TIMESTAMP(3) WITH TIME ZONE",
                     RdbDialect.h2().schema().dataType("TIMESTAMP(3) WITH TIME ZONE"));
        assertEquals("TIMESTAMP(3)",
                     RdbDialect.mysql().schema().dataType("TIMESTAMP(3) WITH TIME ZONE"));
        assertEquals("TIMESTAMPTZ(3)",
                     RdbDialect.postgresql().schema().dataType("TIMESTAMP(3) WITH TIME ZONE"));
        assertEquals("TIMESTAMP(3) WITH TIME ZONE",
                     RdbDialect.oracle().schema().dataType("TIMESTAMP(3) WITH TIME ZONE"));
        assertEquals("DATETIMEOFFSET(3)",
                     RdbDialect.sqlServer().schema().dataType("TIMESTAMP(3) WITH TIME ZONE"));

        assertEquals("TIME(3) WITH TIME ZONE",
                     RdbDialect.h2().schema().dataType("TIME(3) WITH TIME ZONE"));
        assertEquals("VARCHAR(32)",
                     RdbDialect.mysql().schema().dataType("TIME(3) WITH TIME ZONE"));
        assertEquals("TIME(3) WITH TIME ZONE",
                     RdbDialect.postgresql().schema().dataType("TIME(3) WITH TIME ZONE"));
        assertEquals("VARCHAR2(32)",
                     RdbDialect.oracle().schema().dataType("TIME(3) WITH TIME ZONE"));
        assertEquals("VARCHAR(32)",
                     RdbDialect.sqlServer().schema().dataType("TIME(3) WITH TIME ZONE"));

        assertEquals("DATETIME(3)",
                     RdbDialect.mysql().schema().dataType("TIMESTAMP(3) WITHOUT TIME ZONE"));
        assertEquals("DATETIME2(3)",
                     RdbDialect.sqlServer().schema().dataType("TIMESTAMP WITHOUT TIME ZONE", null, 3, null));
        assertEquals("TIMESTAMP(3) WITH TIME ZONE",
                     RdbDialect.oracle().schema().dataType("TIMESTAMP WITH TIME ZONE", null, 3, null));
        assertEquals("TIMESTAMP(3) WITH LOCAL TIME ZONE",
                     RdbDialect.oracle().schema().dataType("TIMESTAMP(3) WITH LOCAL TIME ZONE"));

        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema().dataType("TIMESTAMP(3) WITH LOCAL TIME ZONE"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.postgresql().schema()
                                            .dataType("TIMESTAMP WITH LOCAL TIME ZONE", null, 3, null));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.oracle().schema().dataType("TIME(3) WITH LOCAL TIME ZONE"));
        assertThrows(IllegalArgumentException.class,
                     () -> RdbDialect.mysql().schema()
                                       .dataType("TIMESTAMP(3) WITH TIME ZONE", null, 6, null));
    }

    @Test
    void mapsTheLogicalOffsetTimeArrayToPostgresqlNativeType() {
        assertEquals("TIME WITH TIME ZONE[]",
                     RdbDialect.postgresql().schema().dataType("OFFSET_TIME[]"));
    }

    @Test
    void publishesReversibleMarkersForOracleTextBackedTimes() {
        String timeMarker = "[[flying-orm:v1:TIME]]";
        String commentEscape = "[[flying-orm:v1:COMMENT]]";
        DynamicField localTime = DynamicField.of("business_time", "TIME")
                                             .withComment(timeMarker + "opening hours");
        DynamicField offsetTime = DynamicField.of("remote_time", "OFFSET_TIME");
        DynamicField ordinaryText = DynamicField.of("label", "VARCHAR")
                                                .withLength(18)
                                                .withComment(timeMarker + "ordinary text");
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(localTime)
                                      .addField(offsetTime)
                                      .addField(ordinaryText)
                                      .build();
        SchemaTableSqlRenderer renderer = new SchemaTableSqlRenderer(RdbDialect.oracle().schema());

        var createRequests = renderer.createTable(form);

        assertTrue(createRequests.stream().anyMatch(request -> request.sql().contains(
                timeMarker + timeMarker + "opening hours")));
        assertTrue(createRequests.stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:OFFSET_TIME]]")));
        assertTrue(createRequests.stream().anyMatch(request -> request.sql().contains(
                commentEscape + timeMarker + "ordinary text")));

        var migrationRequests = new ArrayList<com.flying.orm.core.sql.render.SqlRequest>();
        ColumnMetadata physicalColumn = ColumnMetadata.of("business_time", "VARCHAR")
                                                        .withLength(18)
                                                        .withComment(timeMarker + "opening hours");
        renderer.addMissingComment(migrationRequests, "events", physicalColumn, localTime);

        assertEquals(1, migrationRequests.size());
        assertTrue(migrationRequests.getFirst().sql().contains(timeMarker + timeMarker + "opening hours"));
    }

    @Test
    void publishesReversibleMarkersForEveryTextBackedOffsetTime() {
        String offsetTimeMarker = "[[flying-orm:v1:OFFSET_TIME]]";
        String commentEscape = "[[flying-orm:v1:COMMENT]]";
        DynamicField remoteTime = DynamicField.of("remote_time", "OFFSET_TIME")
                                              .withComment("remote clock");
        DynamicField label = DynamicField.of("label", "VARCHAR")
                                         .withLength(32)
                                         .withComment(offsetTimeMarker + "ordinary text");
        DynamicForm mysqlForm = DynamicForm.builder("events", "events")
                                           .addField(remoteTime)
                                           .addField(label)
                                           .build();
        DynamicForm sqlServerForm = DynamicForm.builder("events", "dbo.events")
                                               .addField(remoteTime)
                                               .addField(label)
                                               .build();

        var mysqlRequests = FormSchemaSqlRenderer.create(RdbDialect.mysql()).createTable(mysqlForm);
        var sqlServerRequests = FormSchemaSqlRenderer.create(RdbDialect.sqlServer()).createTable(sqlServerForm);

        assertTrue(mysqlRequests.stream().anyMatch(request -> request.sql().contains(
                offsetTimeMarker + "remote clock")));
        assertTrue(mysqlRequests.stream().anyMatch(request -> request.sql().contains(
                commentEscape + offsetTimeMarker + "ordinary text")));
        assertTrue(sqlServerRequests.stream().anyMatch(request -> request.sql().contains(
                offsetTimeMarker + "remote clock")));
        assertTrue(sqlServerRequests.stream().anyMatch(request -> request.sql().contains(
                commentEscape + offsetTimeMarker + "ordinary text")));
    }

    @Test
    void doesNotAdvanceOracleTimeMarkerWhenSafeModeSkipsNarrowing() {
        ColumnMetadata currentColumn = ColumnMetadata.of("event_time", "OFFSET_TIME")
                                                     .withComment("event clock");
        TableMetadata current = TableMetadata.builder("events").addColumn(currentColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("event_time", "TIME")
                                                              .withComment("event clock"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        assertTrue(plan.skippedChanges().stream().anyMatch(change -> change.name().equals("event_time")));
        assertFalse(plan.requests().stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:TIME]]")));

        ColumnMetadata localColumn = ColumnMetadata.of("event_time", "TIME").withComment("event clock");
        TableMetadata localCurrent = TableMetadata.builder("events").addColumn(localColumn).build();
        DynamicForm offsetTarget = DynamicForm.builder("events", "events")
                                              .addField(DynamicField.of("event_time", "OFFSET_TIME")
                                                                    .withComment("event clock"))
                                              .build();
        SchemaMigrationPlan wideningPlan = renderer.migrateSafelyPlan(localCurrent, offsetTarget, List.of());

        assertTrue(wideningPlan.skippedChanges().stream().anyMatch(change -> change.name().equals("event_time")));
        assertFalse(wideningPlan.requests().stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:OFFSET_TIME]]")));

        ColumnMetadata textColumn = ColumnMetadata.of("event_time", "VARCHAR").withLength(18);
        TableMetadata textCurrent = TableMetadata.builder("events").addColumn(textColumn).build();
        SchemaMigrationPlan reinterpretPlan = renderer.migrateSafelyPlan(textCurrent, target, List.of());

        assertTrue(reinterpretPlan.skippedChanges().stream().anyMatch(change -> change.name().equals("event_time")));
        assertFalse(reinterpretPlan.requests().stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:TIME]]")));
    }

    @Test
    void restoresOracleTimeMarkersInReviewedRollback() {
        ColumnMetadata offsetColumn = ColumnMetadata.of("event_time", "OFFSET_TIME")
                                                    .withComment("event clock");
        TableMetadata offsetCurrent = TableMetadata.builder("events").addColumn(offsetColumn).build();
        DynamicForm timeTarget = DynamicForm.builder("events", "events")
                                            .addField(DynamicField.of("event_time", "TIME")
                                                                  .withComment("event clock"))
                                            .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());
        SchemaMigrationPlan typePlan = renderer.migrateSafelyPlan(
                offsetCurrent, timeTarget, List.of(), SchemaMigrationOptions.safe().allowColumnChange());
        ReviewedSchemaMigrationPlan reviewedTypePlan = SchemaMigrationReviewer.create(renderer).review(
                offsetCurrent, typePlan, SchemaMigrationReviewPolicy.allowBlocking());

        assertTrue(reviewedTypePlan.rollback().requests().stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:OFFSET_TIME]]event clock")));

        ColumnMetadata varcharColumn = ColumnMetadata.of("business_time", "VARCHAR")
                                                     .withLength(18)
                                                     .withComment("opening hours");
        TableMetadata varcharCurrent = TableMetadata.builder("events").addColumn(varcharColumn).build();
        DynamicForm markerTarget = DynamicForm.builder("events", "events")
                                              .addField(DynamicField.of("business_time", "TIME")
                                                                    .withComment("opening hours"))
                                              .build();
        SchemaMigrationPlan markerPlan = renderer.migrateSafelyPlan(
                varcharCurrent, markerTarget, List.of(), SchemaMigrationOptions.safe().allowColumnChange());
        ReviewedSchemaMigrationPlan reviewedMarkerPlan = SchemaMigrationReviewer.create(renderer).review(
                varcharCurrent, markerPlan, SchemaMigrationReviewPolicy.allowBlocking());

        assertTrue(reviewedMarkerPlan.rollback().requests().stream().anyMatch(request ->
                request.sql().contains("comment on column") && request.sql().endsWith(" is 'opening hours'")));
    }

    @Test
    void usesTheCurrentColumnNameUntilReviewedTemporalRenameRollbackCompletes() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("old_name", "OFFSET_TIME")
                                                    .withComment("source clock");
        TableMetadata current = TableMetadata.builder("events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("new_name", "TIME")
                                                              .withComment("target clock"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());
        SchemaMigrationOptions options = SchemaMigrationOptions.safe()
                                                                 .allowColumnChange()
                                                                 .renameColumn("old_name", "new_name");
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of(), options);

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current,
                plan,
                SchemaMigrationReviewPolicy.allowBlocking().withColumnRenames(options.columnRenames()));
        List<String> rollback = reviewed.rollback().requests().stream().map(request -> request.sql()).toList();

        assertTrue(rollback.get(0).contains("\"new_name\""), rollback.toString());
        assertFalse(rollback.get(0).contains("\"old_name\""), rollback.toString());
        assertTrue(rollback.get(1).contains("\"new_name\""), rollback.toString());
        assertFalse(rollback.get(1).contains("\"old_name\""), rollback.toString());
        assertTrue(rollback.getLast().contains("rename column \"new_name\" to \"old_name\""), rollback.toString());
    }

    @Test
    void usesTheCurrentColumnNameForReviewedTemporalNullabilityRollback() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("old_name", "OFFSET_TIME");
        TableMetadata current = TableMetadata.builder("events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("new_name", "OFFSET_TIME")
                                                              .withNullable(false))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());
        SchemaMigrationOptions options = SchemaMigrationOptions.safe()
                                                                 .allowColumnChange()
                                                                 .renameColumn("old_name", "new_name");
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of(), options);

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current,
                plan,
                SchemaMigrationReviewPolicy.allowBlocking().withColumnRenames(options.columnRenames()));
        List<String> rollback = reviewed.rollback().requests().stream().map(request -> request.sql()).toList();

        assertTrue(rollback.getFirst().contains("\"new_name\" null"), rollback.toString());
        assertFalse(rollback.getFirst().contains("\"old_name\""), rollback.toString());
        assertTrue(rollback.getLast().contains("rename column \"new_name\" to \"old_name\""), rollback.toString());
    }

    @Test
    void restoresSqlServerTemporalColumnCommentsInReviewedRollback() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("scheduled_at", "TIMESTAMP")
                                                    .withComment("source schedule");
        TableMetadata current = TableMetadata.builder("dbo.events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "dbo.events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, plan, SchemaMigrationReviewPolicy.allowBlocking());

        assertTrue(plan.requests().stream().anyMatch(request -> request.sql().contains(
                "sp_updateextendedproperty") && request.sql().contains("target schedule")));
        assertTrue(reviewed.rollback().requests().stream().anyMatch(request -> request.sql().contains(
                "sp_updateextendedproperty") && request.sql().contains("source schedule")));
    }

    @Test
    void appliesAndRestoresMysqlTemporalCommentOnlyChanges() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("scheduled_at", "TIMESTAMP")
                                                    .withComment("source schedule");
        TableMetadata current = TableMetadata.builder("events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, plan, SchemaMigrationReviewPolicy.allowBlocking());

        assertTrue(plan.requests().stream().anyMatch(request -> request.sql().contains("modify column")
                && request.sql().contains("comment 'target schedule'")));
        assertTrue(reviewed.rollback().requests().stream().anyMatch(request ->
                request.sql().contains("modify column") && request.sql().contains("comment 'source schedule'")));
    }

    @Test
    void appliesAndRestoresH2TemporalCommentOnlyChanges() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("scheduled_at", "TIMESTAMP")
                                                    .withComment("source schedule");
        TableMetadata current = TableMetadata.builder("events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current, plan, SchemaMigrationReviewPolicy.allowBlocking());

        assertTrue(plan.requests().stream().anyMatch(request -> request.sql().contains("comment on column")
                && request.sql().contains("target schedule")));
        assertTrue(reviewed.rollback().requests().stream().anyMatch(request ->
                request.sql().contains("comment on column") && request.sql().contains("source schedule")));
    }

    @Test
    void rejectsUnsupportedTemporalShapeAndCommentChangesBeforeExecution() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("scheduled_at", "TIMESTAMP")
                                                    .withPrecision(0, null)
                                                    .withComment("source schedule");
        TableMetadata current = TableMetadata.builder("events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withPrecision(6, null)
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder().build());

        assertThrows(IllegalArgumentException.class,
                     () -> renderer.migrateSafelyPlan(current, target, List.of()));
    }

    @Test
    void rejectsUnsupportedCommentBeforeSkippedShapeAndRenamePlanning() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("old_name", "TIMESTAMP")
                                                    .withPrecision(6, null)
                                                    .withComment("source schedule");
        TableMetadata current = TableMetadata.builder("events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("new_name", "TIMESTAMP")
                                                              .withPrecision(0, null)
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder().build());
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().renameColumn("old_name", "new_name");

        assertThrows(IllegalArgumentException.class,
                     () -> renderer.migrateSafelyPlan(current, target, List.of(), options));
    }

    @Test
    void rejectsUnsupportedNewTemporalCommentBeforeRenamePlanning() {
        ColumnMetadata sourceColumn = ColumnMetadata.of("old_name", "VARCHAR");
        TableMetadata current = TableMetadata.builder("events").addColumn(sourceColumn).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("new_name", "VARCHAR"))
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder().build());
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().renameColumn("old_name", "new_name");

        assertThrows(IllegalArgumentException.class,
                     () -> renderer.migrateSafelyPlan(current, target, List.of(), options));
    }

    @Test
    void directMigrationRejectsMysqlStyleWithoutCommentCapability() {
        DynamicField sourceField = DynamicField.of("scheduled_at", "TIMESTAMP")
                                                 .withComment("source schedule");
        DynamicField targetField = sourceField.withComment("target schedule");
        DynamicForm source = DynamicForm.builder("events", "events").addField(sourceField).build();
        DynamicForm target = DynamicForm.builder("events", "events").addField(targetField).build();
        DynamicFormChangeSet changes = new DynamicFormChangeSet(source,
                                                                 target,
                                                                 List.of(),
                                                                 List.of(),
                                                                 List.of(new FieldChange(sourceField, targetField)));
        SchemaDialect dialect = SchemaDialect.builder()
                                             .generatedValues(SchemaDialect.GeneratedValueStyle.MYSQL)
                                             .build();

        assertThrows(IllegalArgumentException.class,
                     () -> FormSchemaSqlRenderer.create(dialect).migrate(changes));
    }
}
