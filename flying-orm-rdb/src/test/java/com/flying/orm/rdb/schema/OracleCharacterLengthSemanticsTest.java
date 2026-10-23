package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.Locale;
import static org.junit.jupiter.api.Assertions.*;

class OracleCharacterLengthSemanticsTest {

    @Test
    void customTimeStorageDoesNotShrinkWhenItIncludesAUnit() {
        for (String unit : List.of("CHAR", "BYTE")) {
            assertEquals("VARCHAR2(32 " + unit + ")", SchemaDialect.builder().oracleGeneratedValues()
                    .mapType("TIME", "VARCHAR2(32 " + unit + ")").build().dataType("TIME"));
            assertEquals("VARCHAR2(18 " + unit + ")", SchemaDialect.builder().oracleGeneratedValues()
                    .mapType("TIME", "VARCHAR2(10 " + unit + ")").build().dataType("TIME"));
        }
    }

    @Test
    void explicitLengthUnitsRemainSafeTextTypesAndSurviveLengthOverrides() {
        SchemaDialect dialect = RdbDialect.oracle().schema();
        for (String base : List.of("CHAR", "VARCHAR2")) {
            for (String unit : List.of("BYTE", "CHAR")) {
                String type = base + "(10 " + unit + ")";
                assertTrue(DatabaseType.of(type).safeDeclaration());
                assertTrue(DatabaseType.of(type).isTextual());
                assertEquals(base + "(20 " + unit + ")", dialect.dataType(type, 20, null, null));
                assertTrue(SchemaTypeComparison.safeWidening(type, base + "(20 " + unit + ")"));
            }
        }
        assertFalse(SchemaTypeComparison.safeWidening("VARCHAR2(10 BYTE)", "VARCHAR2(20 CHAR)"));
        for (String invalid : List.of("NUMBER(10 CHAR)", "NVARCHAR2(10 BYTE)", "VARCHAR2(10 CHAR); DROP TABLE x")) {
            assertFalse(DatabaseType.of(invalid).safeDeclaration());
        }
    }

    @Test
    void observedUnitsAreDistinctButUnspecifiedDesiredUnitsDoNotOverrideThem() {
        SchemaDialect dialect = RdbDialect.oracle().schema();
        ColumnDefinition bytes = ColumnDefinition.builder("code", "VARCHAR2(10 BYTE)").build();
        ColumnDefinition chars = ColumnDefinition.builder("code", "VARCHAR2(10 CHAR)").build();
        assertFalse(SchemaDefinitionEquality.sameColumnType(bytes, chars, dialect));
        assertTrue(SchemaDefinitionEquality.sameColumnType(chars,
                ColumnDefinition.builder("code", "VARCHAR").length(10).build(), dialect));
    }

    @Test
    void legacyTypeChangesAndRollbackRetainObservedUnits() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());
        for (String unit : List.of("CHAR", "BYTE")) {
            RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                    .addColumn(DynamicField.of("code", "VARCHAR2").withLength(10).column()).build();
            DynamicForm target = DynamicForm.builder("items", "items")
                    .addField(DynamicField.of("code", "VARCHAR").withLength(20)).build();
            SchemaSnapshot snapshot = SchemaSnapshot.builder(RelationIdentity.table("items")).tablePresent()
                    .physicalColumns(List.of(ColumnDefinition.builder("code", "VARCHAR2(10 " + unit + ")").build()))
                    .build();
            SchemaMigrationPlan migration = renderer.migrationPlanner().migrateSafelyPlan(
                    current, target, List.of(), List.of(), SchemaMigrationOptions.safe().allowColumnChange(), snapshot);
            SchemaMigrationPlan reviewed = renderer.renderPlan(current, snapshot, migration, SchemaMigrationOptions.safe());
            assertTrue(migration.requests().getFirst().sql().toUpperCase(Locale.ROOT).contains("VARCHAR2(20 " + unit + ")"));
        }
    }

    @Test
    void reviewedRelationalWideningInheritsUnitsWithoutTreatingUnitChangesAsSafe() {
        RdbDialect dialect = RdbDialect.oracle();
        RelationIdentity identity = RelationIdentity.table("items");
        RelationalTableDefinition actual = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("code", "VARCHAR2(10 CHAR)").build()).build();
        RelationalTableDefinition target = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(20).build()).build();
        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), target,
                SchemaSnapshot.present(actual), SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
        assertTrue(plan.requests().getFirst().sql().toUpperCase(Locale.ROOT).contains("VARCHAR2(20 CHAR)"));
    }

    @Test
    void dropOnlyPlansDoNotReadUnusedCharacterLengthUnits() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.oracle());
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR2").withLength(10)).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT")).build();
        SchemaSnapshot physical = SchemaSnapshot.builder(RelationIdentity.table("items")).tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("id", "NUMBER").precision(19).build(),
                        ColumnDefinition.builder("code", "VARCHAR2(10 CHAR)").build())).build();
        java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean snapshotAvailable = new java.util.concurrent.atomic.AtomicBoolean(true);
        ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.just(source);
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                reads.incrementAndGet();
                return snapshotAvailable.get() ? Mono.just(physical) : Mono.empty();
            }
        };
        SchemaMigrationOptions options = SchemaMigrationOptions.safe().allowDropColumn();
        renderer.migrationPlanner().plan(target, List.of(), List.of(), reader, options).block();
        assertEquals(0, reads.get());
        SchemaMigrationPlan reviewed = renderer.migrationPlanner().plan(target, List.of(), List.of(), reader,
                options).block();
        assertEquals(0, reads.get());
        assertEquals(List.of("alter table \"items\" drop column \"code\""), reviewed.sqlTexts());
        snapshotAvailable.set(false);
        SchemaMigrationPlan withoutSnapshot = renderer.migrationPlanner().plan(
                target, List.of(), List.of(), reader, options).block();
        assertEquals(reviewed.requests(), withoutSnapshot.requests());
        assertEquals(0, reads.get());
    }
}
