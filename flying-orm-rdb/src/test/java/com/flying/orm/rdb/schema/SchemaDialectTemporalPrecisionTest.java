package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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

        RelationalTableDefinition current = RelationalTableDefinition.builder("devices")
                .addColumn(DynamicField.of("retry_count", "DECIMAL").withPrecision(10, 0).column())
                .addColumn(DynamicField.of("event_count", "DECIMAL").withPrecision(19, 0).column())
                .build();
        DynamicForm target = DynamicForm.builder("devices", "devices")
                .addField(DynamicField.of("retry_count", "INTEGER"))
                .addField(DynamicField.of("event_count", "BIGINT"))
                .build();

        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(oracle)
                .migrateSafelyPlan(current, target, List.of());
        assertFalse(plan.hasExecutableSql());
        assertFalse(plan.hasSkippedChanges());
    }

    @Test
    void preservesOracleLocalTimeZoneThroughTableMetadataRendering() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("session_at", "TIMESTAMP WITH LOCAL TIME ZONE")
                                                            .withPrecision(4, null))
                                      .build();
        DynamicField current = new DynamicField(form.toTableDefinition().column("session_at"), false, false);
        DynamicField target = DynamicField.of("session_at", "TIMESTAMPTZ").withPrecision(4, null);
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.oracle().schema());

        assertEquals("TIMESTAMP(4) WITH LOCAL TIME ZONE", renderer.dataType(current.column()));
        assertEquals("TIMESTAMP(4) WITH TIME ZONE", renderer.dataType(target.column()));
        assertFalse(renderer.sameDataType(renderer.dataType(current.column()), renderer.dataType(target.column())));
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
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.oracle().schema());

        var createRequests = FormSchemaSqlRenderer.create(RdbDialect.oracle()).createTable(form);

        assertTrue(createRequests.stream().anyMatch(request -> request.sql().contains(
                timeMarker + timeMarker + "opening hours")));
        assertTrue(createRequests.stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:OFFSET_TIME]]")));
        assertTrue(createRequests.stream().anyMatch(request -> request.sql().contains(
                commentEscape + timeMarker + "ordinary text")));

        DynamicField physicalColumn = DynamicField.of("business_time", "VARCHAR")
                                                        .withLength(18)
                                                        .withComment(timeMarker + "opening hours");
        List<com.flying.orm.core.sql.render.SqlRequest> migrationRequests = FormSchemaSqlRenderer.create(RdbDialect.oracle())
                .migrateSafelyPlan(RelationalTableDefinition.builder("events").addColumn(physicalColumn.column()).build(),
                        DynamicForm.builder("events", "events").addField(localTime).build(), List.of(),
                        SchemaMigrationOptions.safe().allowColumnChange()).requests();

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
        DynamicField currentColumn = DynamicField.of("event_time", "OFFSET_TIME")
                                                     .withComment("event clock");
        RelationalTableDefinition current = RelationalTableDefinition.builder("events").addColumn(currentColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("event_time", "TIME")
                                                              .withComment("event clock"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        assertTrue(plan.skippedChanges().stream().anyMatch(change -> change.name().equals("event_time")));
        assertFalse(plan.requests().stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:TIME]]")));

        DynamicField localColumn = DynamicField.of("event_time", "TIME").withComment("event clock");
        RelationalTableDefinition localCurrent = RelationalTableDefinition.builder("events").addColumn(localColumn.column()).build();
        DynamicForm offsetTarget = DynamicForm.builder("events", "events")
                                              .addField(DynamicField.of("event_time", "OFFSET_TIME")
                                                                    .withComment("event clock"))
                                              .build();
        SchemaMigrationPlan wideningPlan = renderer.migrateSafelyPlan(localCurrent, offsetTarget, List.of());

        assertTrue(wideningPlan.skippedChanges().stream().anyMatch(change -> change.name().equals("event_time")));
        assertFalse(wideningPlan.requests().stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:OFFSET_TIME]]")));

        DynamicField textColumn = DynamicField.of("event_time", "VARCHAR").withLength(18);
        RelationalTableDefinition textCurrent = RelationalTableDefinition.builder("events").addColumn(textColumn.column()).build();
        SchemaMigrationPlan reinterpretPlan = renderer.migrateSafelyPlan(textCurrent, target, List.of());

        assertTrue(reinterpretPlan.skippedChanges().stream().anyMatch(change -> change.name().equals("event_time")));
        assertFalse(reinterpretPlan.requests().stream().anyMatch(request -> request.sql().contains(
                "[[flying-orm:v1:TIME]]")));
    }







    @Test
    void changesSqlServerTemporalColumnComments() {
        DynamicField sourceColumn = DynamicField.of("scheduled_at", "TIMESTAMP")
                                                    .withComment("source schedule");
        RelationalTableDefinition current = RelationalTableDefinition.builder("dbo.events").addColumn(sourceColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "dbo.events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        SchemaMigrationPlan reviewed = renderer.renderPlan(current, null, plan, SchemaMigrationOptions.safe());

        assertTrue(plan.requests().stream().anyMatch(request -> request.sql().contains(
                "sp_updateextendedproperty") && request.sql().contains("target schedule")));
    }

    @Test
    void rejectsMysqlTemporalCommentRewriteWithoutCompletePhysicalMetadata() {
        DynamicField sourceColumn = DynamicField.of("scheduled_at", "TIMESTAMP")
                                                    .withComment("source schedule");
        RelationalTableDefinition current = RelationalTableDefinition.builder("events").addColumn(sourceColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> renderer.migrateSafelyPlan(current, target, List.of()));

        assertEquals("rewriting an existing column requires complete physical column metadata",
                failure.getMessage());
    }

    @Test
    void appliesMysqlTemporalCommentWithCompletePhysicalMetadata() {
        DynamicField sourceColumn = DynamicField.of("scheduled_at", "TIMESTAMP")
                .withPrecision(6, null).withNullable(false).withComment("source schedule");
        RelationalTableDefinition current = RelationalTableDefinition.builder("events").addColumn(sourceColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                        .withPrecision(6, null).withNullable(false).withComment("target schedule"))
                .build();
        SchemaSnapshot snapshot = SchemaSnapshot.present(RelationalTableDefinition.builder(
                        RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("scheduled_at", "DATETIME")
                        .temporalPrecision(6).nullable(false).defaultValue(ColumnDefault.currentTimestamp())
                        .comment("source schedule").build())
                .build());
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.mysql());
        SchemaMigrationPlan plan = renderer.migrationPlanner().migrateSafelyPlan(
                current, target, List.of(), List.of(), SchemaMigrationOptions.safe(), snapshot);

        SchemaMigrationPlan reviewed = renderer.renderPlan(current, snapshot, plan, SchemaMigrationOptions.safe());

        assertEquals(1, plan.requests().size());
        String forward = plan.requests().getFirst().sql().toLowerCase(Locale.ROOT);
        for (String sql : List.of(forward)) {
            assertTrue(sql.contains("modify column"), sql);
            assertTrue(sql.contains("datetime(6) not null default current_timestamp(6)"), sql);
        }
        assertTrue(forward.contains("comment 'target schedule'"), forward);
    }

    @Test
    void appliesAndRestoresH2TemporalCommentOnlyChanges() {
        DynamicField sourceColumn = DynamicField.of("scheduled_at", "TIMESTAMP")
                                                    .withComment("source schedule");
        RelationalTableDefinition current = RelationalTableDefinition.builder("events").addColumn(sourceColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2());
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of());

        SchemaMigrationPlan reviewed = renderer.renderPlan(current, null, plan, SchemaMigrationOptions.safe());

        assertTrue(plan.requests().stream().anyMatch(request -> request.sql().contains("comment on column")
                && request.sql().contains("target schedule")));
    }

    @Test
    void rejectsUnsupportedTemporalShapeAndCommentChangesBeforeExecution() {
        DynamicField sourceColumn = DynamicField.of("scheduled_at", "TIMESTAMP")
                                                    .withPrecision(0, null)
                                                    .withComment("source schedule");
        RelationalTableDefinition current = RelationalTableDefinition.builder("events").addColumn(sourceColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withPrecision(6, null)
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder().build());

        assertThrows(UnsupportedOperationException.class,
                     () -> renderer.migrateSafelyPlan(current, target, List.of()));
    }

    @Test
    void skippedColumnChangeDoesNotRenderItsCommentOrPreventExplicitRename() {
        DynamicField sourceColumn = DynamicField.of("old_name", "TIMESTAMP")
                                                    .withPrecision(6, null)
                                                    .withComment("source schedule");
        RelationalTableDefinition current = RelationalTableDefinition.builder("events").addColumn(sourceColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("new_name", "TIMESTAMP")
                                                              .withPrecision(0, null)
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder().build());
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().renameColumn("old_name", "new_name");

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(current, target, List.of(), options);
        assertEquals(1, plan.requests().size());
        assertTrue(plan.requests().getFirst().sql().contains("rename column old_name to new_name"));
        assertEquals(1, plan.skippedChanges().size());
        assertEquals(SkippedSchemaChange.Kind.CHANGE_COLUMN, plan.skippedChanges().getFirst().kind());
    }

    @Test
    void rejectsUnsupportedNewTemporalCommentBeforeRenamePlanning() {
        DynamicField sourceColumn = DynamicField.of("old_name", "VARCHAR");
        RelationalTableDefinition current = RelationalTableDefinition.builder("events").addColumn(sourceColumn.column()).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("new_name", "VARCHAR"))
                                        .addField(DynamicField.of("scheduled_at", "TIMESTAMP")
                                                              .withComment("target schedule"))
                                        .build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(SchemaDialect.builder().build());
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().renameColumn("old_name", "new_name");

        assertThrows(UnsupportedOperationException.class,
                     () -> renderer.migrateSafelyPlan(current, target, List.of(), options));
    }

    @Test
    void directMigrationRejectsMysqlStyleWithoutCommentCapability() {
        DynamicField sourceField = DynamicField.of("scheduled_at", "TIMESTAMP")
                                                 .withComment("source schedule");
        DynamicField targetField = sourceField.withComment("target schedule");
        DynamicForm source = DynamicForm.builder("events", "events").addField(sourceField).build();
        DynamicForm target = DynamicForm.builder("events", "events").addField(targetField).build();
        DynamicFormChangeSet changes = source.diffTo(target);
        SchemaDialect dialect = SchemaDialect.builder()
                                             .generatedValues(SchemaDialect.GeneratedValueStyle.MYSQL)
                                             .build();

        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        assertThrows(IllegalStateException.class, () -> renderer.migrate(changes));
        SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.table("events"))
                .tablePresent().physicalColumns(List.of(sourceField.column())).build();
        assertThrows(UnsupportedOperationException.class, () -> renderer.migrate(changes, snapshot));
    }
}
