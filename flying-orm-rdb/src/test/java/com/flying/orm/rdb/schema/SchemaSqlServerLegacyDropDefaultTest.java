package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
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
    void addedSequenceColumnHasAReviewTimeDefaultNameThatRollbackCanDrop() {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.sqlServer());
        DynamicForm current = target();
        DynamicForm desired = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER"))
                .addField(DynamicField.of("code", "BIGINT")
                        .withGeneration(ValueGeneration.sequence("items_code_seq"))).build();
        SchemaMigrationPlan migration = renderer.migrateSafelyPlan(current.toTableMetadata(), desired, List.of());
        ReviewedSchemaMigrationPlan reviewed = SchemaMigrationReviewer.create(renderer).review(
                current.toTableMetadata(), migration, SchemaMigrationReviewPolicy.allowBlocking());
        String add = migration.requests().stream().map(SqlRequest::sql)
                .filter(sql -> sql.startsWith("alter table [items] add [code] ")).findFirst().orElseThrow();
        java.util.regex.Matcher constraint = java.util.regex.Pattern.compile(
                "constraint (\\[[^]]+]) default next value for ").matcher(add);

        assertTrue(constraint.find(), "the added sequence default needs a name frozen in the reviewed SQL: " + add);
        assertTrue(reviewed.rollback().requests().stream().map(SqlRequest::sql).anyMatch(sql -> sql.equals(
                "alter table [items] drop constraint " + constraint.group(1) + ", column [code]")));
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

        ReviewedSchemaMigrationPlan reviewed = planner.review(target(), List.of(), List.of(), reader(snapshotReads),
                SchemaMigrationOptions.safe().allowDropColumn(), SchemaMigrationReviewPolicy.allowBlocking()).block();

        assertDrop(reviewed.migration().requests());
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
            public Mono<DynamicForm> readForm(String id, String table) { return Mono.just(source()); }
            public Mono<DynamicForm> readForm(String id, String schema, String table) { return Mono.just(source()); }
            public Mono<SchemaSnapshot> readSnapshot(String table) {
                snapshotReads.incrementAndGet();
                return Mono.just(snapshot());
            }
        };
    }
}
