package com.flying.orm.core.metadata;

import com.flying.orm.core.form.DynamicField;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RelationalMetadataImmutabilityTest {
    @Test
    void tableCollectionsHaveOnlyAddMethods() {
        assertFalse(java.util.Arrays.stream(RelationalTableDefinition.Builder.class.getDeclaredMethods())
                .anyMatch(method -> java.util.List.of("unique", "index", "foreignKey", "check")
                        .contains(method.getName())));
    }

    @Test
    void dynamicFieldConstructorsShareTheCompleteColumnDeclaration() {
        for (java.lang.reflect.Constructor<?> constructor : DynamicField.class.getConstructors()) {
            assertEquals(ColumnDefinition.class, constructor.getParameterTypes()[0]);
        }
        ColumnDefinition column = ColumnDefinition.builder("status", "varchar").length(32)
                .defaultValue(ColumnDefault.literal("NEW")).collation("utf8_bin").build();
        DynamicField field = new DynamicField(column, false, true);
        assertEquals(new DynamicField(column, false, true, null, FeatureRegistry.empty()), field);
        assertSame(column, field.column());
        assertNull(field.runtimeDefault());
    }


    @Test
    void dynamicFieldEditsPreserveTheCompleteColumnDeclaration() {
        ColumnDefinition column = ColumnDefinition.builder("status", "varchar").length(32)
                .codecId("status-codec").defaultValue(ColumnDefault.literal("NEW"))
                .defaultConstraintName("df_status").charset("utf8").collation("utf8_bin").build();
        DynamicField field = new DynamicField(column, false, true);
        DynamicField changed = field.withLength(64).withComment("状态").withNullable(false);

        assertSame(column, field.column());
        assertEquals(32, field.length());
        assertEquals(64, changed.length());
        assertEquals("状态", changed.comment());
        assertEquals(false, changed.nullable());
        assertEquals(true, changed.unique());
        assertEquals("status-codec", changed.column().codecId());
        assertEquals(ColumnDefault.literal("NEW"), changed.column().defaultValue());
        assertEquals("df_status", changed.column().defaultConstraintName());
        assertEquals("utf8", changed.column().charset());
        assertEquals("utf8_bin", changed.column().collation());
    }

    @Test
    void dynamicFieldUsesTemporalPrecisionWithoutInventingNumericPrecision() {
        DynamicField field = DynamicField.of("created_at", "timestamp").withPrecision(0, null);

        assertEquals(0, field.precision());
        assertEquals(0, field.column().temporalPrecision());
        assertNull(field.column().precision());
        assertEquals(field, DynamicField.of("created_at", "timestamp").withPrecision(0, null));
    }

    @Test
    void primaryKeyNormalizationKeepsTheOtherColumnOptions() {
        ColumnDefinition column = ColumnDefinition.builder("id", "bigint")
                .generation(ValueGeneration.identity(10, 2, 7))
                .comment("主键").build();
        DynamicField key = new DynamicField(column, true, false).withNullable(true);

        assertEquals(false, key.nullable());
        assertEquals("主键", key.comment());
        assertEquals(ValueGeneration.identity(10, 2, 7), key.generation());
        assertEquals(true, column.nullable());
    }

    @Test
    void identicalColumnDeclarationsHaveValueEquality() {
        ColumnDefinition left = ColumnDefinition.builder("amount", "decimal")
                .precision(18).scale(2).nullable(false).defaultValue(ColumnDefault.literal(0)).build();
        ColumnDefinition right = ColumnDefinition.builder("amount", "decimal")
                .precision(18).scale(2).nullable(false).defaultValue(ColumnDefault.literal(0)).build();

        assertEquals(left, right);
        assertEquals(right, left);
        assertEquals(left.hashCode(), right.hashCode());
        assertNotEquals(left, ColumnDefinition.builder("amount", "decimal")
                .precision(18).scale(3).nullable(false).defaultValue(ColumnDefault.literal(0)).build());
    }

    @Test
    void declarationEqualityDoesNotIgnoreObservedOnlyOptions() {
        ColumnDefinition desired = ColumnDefinition.builder("status", "varchar").length(32).build();
        ColumnDefinition observed = ColumnDefinition.builder("status", "varchar").length(32)
                .defaultConstraintName("df_status").charset("utf8").collation("utf8_bin").build();

        assertNotEquals(desired, observed);
        assertNotEquals(observed, desired);
        assertNotEquals(desired, null);
    }

    @Test
    void columnCallbackPublishesBeforeItsBuilderCanBeChangedAgain() {
        AtomicReference<ColumnDefinition.Builder> retained = new AtomicReference<>();
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(RelationIdentity.table("orders"))
                .addColumn("status", "VARCHAR", column -> {
                    column.length(32).defaultValue(ColumnDefault.literal("NEW"));
                    retained.set(column);
                });
        retained.get().length(64).defaultValue(ColumnDefault.literal("CHANGED"));
        ColumnDefinition column = table.build().column("status");
        assertEquals(32, column.length());
        assertEquals(ColumnDefault.literal("NEW"), column.defaultValue());
    }

    @Test
    void failingColumnCallbackDoesNotAddAPartialColumn() {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(RelationIdentity.table("orders"));
        assertThrows(IllegalStateException.class, () -> table.addColumn("status", "VARCHAR", column -> {
            column.length(32);
            throw new IllegalStateException("configuration failed");
        }));
        assertEquals(0, table.build().columns().size());
    }

    @Test
    void buildPublishesASnapshotThatDoesNotFollowTheBuilder() {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(
                RelationIdentity.table("orders"));
        builder.addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build());

        RelationalTableDefinition snapshot = builder.build();
        builder.addColumn(ColumnDefinition.builder("status", "VARCHAR").length(32).build());

        assertEquals(1, snapshot.columns().size());
        assertThrows(UnsupportedOperationException.class,
                     () -> snapshot.columns().add(ColumnDefinition.builder("extra", "INT").build()));
    }
}
