package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaMigrationPlanTest {

    @Test
    void singleStepPlanReusesItsImmutableRequests() {
        DynamicForm form = DynamicForm.builder("people", "people")
                .addField(DynamicField.of("name", "VARCHAR").withLength(64)).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .createTablePlan(form, List.of());
        assertEquals(1, plan.steps().size());
        assertSame(plan.steps().getFirst().requests(), plan.requests());
        assertThrows(UnsupportedOperationException.class, () -> plan.requests().clear());
    }

    @Test
    void incrementalIndexAndForeignKeyCanReferenceAnExistingUndeclaredColumn() {
        DynamicForm declared = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("orders")
                .addColumn(declared.field("id").column())
                .addColumn(DynamicField.of("parent_id", "BIGINT").column())
                .primaryKey(declared.toTableDefinition().primaryKey().orElseThrow()).build();
        IndexDefinition index = IndexDefinition.builder("ix_parent").addColumn("parent_id").build();
        ForeignKeyDefinition key = ForeignKeyDefinition.builder("fk_parent")
                .addColumn("parent_id").reference(RelationIdentity.table("parents"))
                .addReferenceColumn("id").build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .migrateSafelyPlan(current, declared, List.of(index), List.of(key), SchemaMigrationOptions.safe());

        assertSame(index, plan.target().indexes().getFirst());
        assertSame(key, plan.target().foreignKeys().getFirst());
        assertEquals(current.column("parent_id"), plan.target().column("parent_id"));
        assertTrue(plan.sqlTexts().stream().anyMatch(sql -> sql.contains("ix_parent")));
        assertTrue(plan.sqlTexts().stream().anyMatch(sql -> sql.contains("fk_parent")));
        assertTrue(plan.sqlTexts().stream().noneMatch(sql -> sql.contains("drop column")));
    }

    @Test
    void publishesCanonicalTableFactsForOrdinarySchemaPlans() {
        DynamicForm form = DynamicForm.builder("orders", "sales.orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR").withLength(32).withUnique(true))
                .build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .createTablePlan(form, List.of());

        RelationalTableDefinition target = assertInstanceOf(RelationalTableDefinition.class, plan.target());
        assertEquals(RelationIdentity.of(null, "sales", "orders"), target.identity());
        assertEquals(List.of("id"), target.primaryKey().orElseThrow().columns());
        assertEquals(32, target.column("code").length());
        assertEquals(List.of("code"), target.indexes().getFirst().columns());
        assertEquals(List.of(target.identity()), SchemaMigrationSupport.metadataRelations(plan));
    }

    @Test
    void validatesAndPublishesTheSameAdditionalTableSnapshot() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        List<RelationIdentity> changingSource = new AbstractList<>() {
            private int reads;

            @Override
            public RelationIdentity get(int index) {
                if (index != 0) {
                    throw new IndexOutOfBoundsException(index);
                }
                return reads++ == 0 ? RelationIdentity.table("audit_aux") : null;
            }

            @Override
            public int size() {
                return 1;
            }
        };

        SchemaMigrationPlan plan = new SchemaMigrationPlan(
                target.toTableDefinition(), true,
                List.of(),
                List.of(),
                changingSource);

        assertEquals(List.of(RelationIdentity.table("audit_aux")), plan.additionalCreatedTables());
    }

    @Test
    void rejectsNullAdditionalCreatedTableAtThePublicPlanBoundary() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();

        assertThrows(NullPointerException.class, () -> new SchemaMigrationPlan(
                target.toTableDefinition(), true,
                List.of(),
                List.of(),
                java.util.Collections.singletonList(null)));
    }
}
