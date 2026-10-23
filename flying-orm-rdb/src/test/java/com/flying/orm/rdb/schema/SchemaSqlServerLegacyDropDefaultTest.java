package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaSqlServerLegacyDropDefaultTest {
    @Test
    void skippedColumnChangeDoesNotReadPhysicalDefaults() {
        AtomicInteger reads = new AtomicInteger();
        DynamicForm desired = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER"))
                .addField(new DynamicField(ColumnDefinition.builder("code", "VARCHAR").length(16)
                        .defaultValue(ColumnDefault.literal("new")).build(), false, false)).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.sqlServer()).migrationPlanner()
                .plan(desired, List.of(), List.of(), reader(reads), SchemaMigrationOptions.safe()).block();

        assertEquals(0, reads.get());
        assertTrue(plan.sqlTexts().isEmpty());
        assertEquals(List.of("code"), plan.skippedChanges().stream().map(SkippedSchemaChange::name).toList());
    }

    @Test
    void changingOnlyAnIntegerDefaultReadsAndReplacesItsActualConstraint() {
        AtomicInteger reads = new AtomicInteger();
        DynamicForm desired = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER"))
                .addField(new DynamicField(ColumnDefinition.builder("code", "INTEGER")
                        .defaultValue(ColumnDefault.literal(9)).build(), false, false)).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.sqlServer()).migrationPlanner()
                .plan(desired, List.of(), List.of(), reader(reads), SchemaMigrationOptions.safe()).block();

        assertEquals(1, reads.get());
        assertTrue(plan.skippedChanges().isEmpty());
        assertEquals(List.of("alter table [items] drop constraint [df_items_code]",
                "alter table [items] add constraint [df_items_code] default 9 for [code]"), plan.sqlTexts());
    }

    @Test
    void addedSequenceColumnHasAnExplicitDefaultConstraintName() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        DynamicForm current = target();
        DynamicForm desired = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER"))
                .addField(DynamicField.of("code", "BIGINT")
                        .withGeneration(ValueGeneration.sequence("items_code_seq"))).build();
        SchemaMigrationPlan migration = renderer.migrateSafelyPlan(current.toTableDefinition(), desired, List.of());
        SchemaMigrationPlan reviewed = renderer.renderPlan(current.toTableDefinition(), null, migration, SchemaMigrationOptions.safe());
        String add = migration.requests().stream().map(SqlRequest::sql)
                .filter(sql -> sql.startsWith("alter table [items] add [code] ")).findFirst().orElseThrow();
        java.util.regex.Matcher constraint = java.util.regex.Pattern.compile(
                "constraint (\\[[^]]+]) default next value for ").matcher(add);

        assertTrue(constraint.find(), "the added sequence default needs a name frozen in the reviewed SQL: " + add);
    }

    @Test
    void directMigrationDropsTheObservedDefaultConstraintWithItsColumn() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());

        assertDrop(renderer.migrate(source().diffTo(target()), snapshot()));
    }

    @Test
    void plainForwardPlanReadsTheDefaultConstraintNeededForDroppingItsColumn() {
        SchemaMigrationPlanner planner = FormSchemaSqlRenderer.create(RdbDialect.sqlServer()).migrationPlanner();
        AtomicInteger snapshotReads = new AtomicInteger();

        SchemaMigrationPlan plan = planner.plan(target(), List.of(), List.of(), reader(snapshotReads),
                SchemaMigrationOptions.safe().allowDropColumn()).block();

        assertDrop(plan.requests());
        assertEquals(1, snapshotReads.get());
    }

    @Test
    void reviewedForwardPlanUsesTheSameObservedDefaultConstraintAsRollback() {
        SchemaMigrationPlanner planner = FormSchemaSqlRenderer.create(RdbDialect.sqlServer()).migrationPlanner();
        AtomicInteger snapshotReads = new AtomicInteger();

        SchemaMigrationPlan reviewed = planner.plan(target(), List.of(), List.of(), reader(snapshotReads),
                SchemaMigrationOptions.safe().allowDropColumn()).block();

        assertDrop(reviewed.requests());
        assertEquals(1, snapshotReads.get());
    }

    private static void assertDrop(List<SqlRequest> requests) {
        assertEquals(List.of("alter table [items] drop constraint [df_items_code], column [code]"),
                requests.stream().map(SqlRequest::sql).toList());
    }

    private static DynamicForm source() {
        return DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER"))
                .addField(DynamicField.of("code", "INTEGER")).build();
    }

    private static DynamicForm target() {
        return DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER")).build();
    }

    private static SchemaSnapshot snapshot() {
        return SchemaSnapshot.builder(RelationIdentity.table("items")).tablePresent()
                .physicalColumns(List.of(ColumnDefinition.builder("id", "INTEGER").build(),
                        ColumnDefinition.builder("code", "INTEGER")
                                .defaultValue(ColumnDefault.literal(7))
                                .defaultConstraintName("df_items_code").build())).build();
    }

    private static ReactiveFormMetadataReader reader(AtomicInteger snapshotReads) {
        return new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.just(source());
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                snapshotReads.incrementAndGet();
                return Mono.just(snapshot());
            }
        };
    }
}
