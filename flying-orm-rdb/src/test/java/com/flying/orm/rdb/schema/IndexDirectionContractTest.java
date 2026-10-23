package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexDirectionContractTest {
    private static final DynamicForm FORM = DynamicForm.builder("orders", "orders")
            .addField(DynamicField.of("id", "BIGINT"))
            .addField(DynamicField.of("created_at", "TIMESTAMP")).build();

    @Test
    void retainsDirectionThroughBothMetadataViewsAndSnapshots() {
        IndexDefinition.Builder builder = IndexDefinition.builder("ix_orders").unique()
                .addColumn("id").addKey(IndexKeyPart.desc("created_at"));
        IndexDefinition index = builder.build();
        builder.addColumn("late");
        assertEquals(List.of("id", "created_at"), index.columns());
        assertThrows(UnsupportedOperationException.class, () -> index.keys().clear());
        RelationalTableDefinition table = table(index);
        var relational = table;
        assertEquals(index.keys(), relational.indexes().getFirst().keys());
        IndexDefinition ascending = IndexDefinition.builder("ix_orders").unique().addColumn("id").addColumn("created_at").build();
        assertNotEquals(SchemaSnapshot.fromPartial(table, true, false).knownDefinition().indexes(),
                SchemaSnapshot.fromPartial(table(ascending), true, false).knownDefinition().indexes());
    }

    @Test
    void detectsDirectionChangesAndRetainsThemWhenRebuilding() {
        IndexDefinition ascending = IndexDefinition.builder("ix_orders").addColumn("id").addColumn("created_at").build();
        IndexDefinition descending = IndexDefinition.builder("ix_orders").addColumn("id").addKey(IndexKeyPart.desc("created_at")).build();
        FormSchemaSqlRenderer plans = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        assertTrue(plans.migrateSafelyPlan(table(descending), FORM, List.of(descending)).requests().isEmpty());
        SchemaMigrationPlan safe = plans.migrateSafelyPlan(table(ascending), FORM, List.of(descending));
        assertTrue(safe.requests().isEmpty());
        assertEquals(SkippedSchemaChange.Kind.CHANGE_INDEX, safe.skippedChanges().getFirst().kind());
        SchemaMigrationPlan rebuild = plans.migrateSafelyPlan(table(ascending), FORM, List.of(descending),
                SchemaMigrationOptions.safe().allowRebuildIndex());
        assertEquals(2, rebuild.requests().size());
        assertTrue(rebuild.sqlTexts().getFirst().startsWith("drop index"));
        assertTrue(rebuild.sqlTexts().getLast().contains(" desc"));
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect.schema());
            String sql = renderer.createIndexes("orders", List.of(descending)).getFirst().sql();
            assertTrue(sql.contains(dialect.schema().identifier("created_at") + " desc"), sql);
            assertFalse(renderer.createIndexes("orders", List.of(ascending)).getFirst().sql().contains(" desc"));
        }
    }

    @Test
    void columnRenamePreservesIndexDirectionWithoutAnUnnecessaryRebuild() {
        for (boolean descending : List.of(false, true)) {
            IndexDefinition current = IndexDefinition.builder("ix_orders").addColumn("id")
                    .addKey(descending ? IndexKeyPart.desc("created_at") : IndexKeyPart.asc("created_at")).build();
            IndexDefinition desired = IndexDefinition.builder("ix_orders").addColumn("id")
                    .addKey(descending ? IndexKeyPart.desc("updated_at") : IndexKeyPart.asc("updated_at")).build();
            DynamicForm target = DynamicForm.builder("orders", "orders")
                    .addField(DynamicField.of("id", "BIGINT"))
                    .addField(DynamicField.of("updated_at", "TIMESTAMP")).build();
            SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                    .migrateSafelyPlan(table(current), target, List.of(desired),
                            SchemaMigrationOptions.safe().allowRebuildIndex().renameColumn("created_at", "updated_at"));
            assertEquals(List.of("alter table \"orders\" rename column \"created_at\" to \"updated_at\""),
                    plan.sqlTexts());
            assertTrue(plan.skippedChanges().isEmpty());
        }
    }

    @Test
    void replacesMysqlUniqueIndexInOneStatementBeforeRemovingItsOldColumn() {
        IndexDefinition original = IndexDefinition.builder("ix_orders").unique()
                .addColumn("created_at").build();
        IndexDefinition replacement = IndexDefinition.builder("ix_orders").unique()
                .addColumn("id").build();
        DynamicForm target = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("id", "BIGINT")).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.mysql())
                .migrateSafelyPlan(table(original), target, List.of(replacement),
                        SchemaMigrationOptions.safe().allowRebuildIndex().allowDropColumn());
        assertEquals(List.of(
                "alter table `orders` drop index `ix_orders`, add unique index `ix_orders` (`id` asc)",
                "alter table `orders` drop column `created_at`"), plan.sqlTexts());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void retainsExplicitIndexRebuildForGenericAndOracleDialects() {
        IndexDefinition ascending = IndexDefinition.builder("ix_orders").unique().addColumn("id").build();
        IndexDefinition descending = IndexDefinition.builder("ix_orders").unique()
                .addKey(IndexKeyPart.desc("id")).build();
        for (SchemaDialect dialect : List.of(SchemaDialect.standard(), RdbDialect.oracle().schema())) {
            SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(dialect).migrateSafelyPlan(
                    table(ascending), FORM, List.of(descending), SchemaMigrationOptions.safe().allowRebuildIndex());
            assertEquals(2, plan.requests().size());
            assertTrue(plan.sqlTexts().getFirst().startsWith("drop index"));
            assertTrue(plan.sqlTexts().getLast().endsWith(dialect.identifier("id") + " desc)"));
        }
    }

    @Test
    void usesRenamedColumnIdentityWhenRebuildingAnOracleIndex() {
        IndexDefinition original = IndexDefinition.builder("ix_orders").unique().addColumn("id").build();
        IndexDefinition replacement = IndexDefinition.builder("ix_orders").unique()
                .addKey(IndexKeyPart.desc("new_id")).build();
        DynamicForm target = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("new_id", "BIGINT"))
                .addField(DynamicField.of("created_at", "TIMESTAMP")).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.oracle()).migrateSafelyPlan(
                table(original), target, List.of(replacement),
                SchemaMigrationOptions.safe().allowRebuildIndex().renameColumn("id", "new_id"));
        assertEquals(List.of("alter table \"orders\" rename column \"id\" to \"new_id\"",
                "drop index \"ix_orders\"",
                "create unique index \"ix_orders\" on \"orders\" (\"new_id\" desc)"), plan.sqlTexts());
    }

    @Test
    void generatedIndexNameDoesNotAdoptAnUnrelatedExistingIndex() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("id", "BIGINT").withUnique(true))
                .addField(DynamicField.of("created_at", "TIMESTAMP")).build();
        IndexDefinition existing = IndexDefinition.builder("business_unique").unique().addColumn("id").build();
        IndexDefinition desired = IndexDefinition.builder("uk_orders_id").unique().addColumn("id").build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        SchemaMigrationPlan safe = renderer.migrateSafelyPlan(table(existing), target, List.of(desired));
        assertEquals(List.of("create unique index \"uk_orders_id\" on \"orders\" (\"id\" asc)"), safe.sqlTexts());
        assertEquals(List.of(SkippedSchemaChange.Kind.DROP_INDEX),
                safe.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());
        SchemaMigrationPlan replace = renderer.migrateSafelyPlan(table(existing), target, List.of(desired),
                SchemaMigrationOptions.safe().allowDropIndex());
        assertEquals(2, replace.requests().size());
        assertTrue(replace.sqlTexts().contains("drop index \"business_unique\""));
        assertTrue(replace.skippedChanges().isEmpty());
    }

    @Test
    void descendingUniqueIndexIsNotMistakenForGeneratedAscendingIndex() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("id", "BIGINT").withUnique(true))
                .addField(DynamicField.of("created_at", "TIMESTAMP")).build();
        IndexDefinition target = IndexDefinition.builder("uk_orders_id").unique().addColumn("id").build();
        IndexDefinition old = IndexDefinition.builder("old_unique").unique().addKey(IndexKeyPart.desc("id")).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .migrateSafelyPlan(table(old), form, List.of(target));
        assertEquals(List.of("create unique index \"uk_orders_id\" on \"orders\" (\"id\" asc)"), plan.sqlTexts());
        assertEquals(SkippedSchemaChange.Kind.DROP_INDEX, plan.skippedChanges().getFirst().kind());
    }

    private static RelationalTableDefinition table(IndexDefinition index) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder("orders");
        FORM.fields().forEach(field -> table.addColumn(field.column()));
        return table.addIndex(index).build();
    }
}
