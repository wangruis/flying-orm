package com.flying.orm.core.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FeatureRegistryHierarchyTest {
    record Label(String id, String value) implements Feature { }

    @Test
    void localIdsOverrideParentsWithoutRestrictingImplementationClasses() {
        FeatureRegistry root = FeatureRegistry.builder().add(new Label("mode", "schema"))
                .add(new Label("fallback", "inherited")).build();
        FeatureRegistry table = FeatureRegistry.builder().parent(root).add(new Label("MODE", "table")).build();
        assertEquals("table", ((Label) table.feature("mode")).value());
        assertEquals("inherited", ((Label) table.feature("fallback")).value());
        assertEquals(2, table.features().size());
        assertThrows(IllegalArgumentException.class, () -> table.feature(Label.class));
        assertEquals("table", table.withParent(FeatureRegistry.empty()).feature(Label.class).value());
        assertEquals("schema", ((Label) root.feature("mode")).value());
    }

    @Test
    void fieldOverridesAndRebindingUseTheCurrentTableAndSchema() {
        SchemaMetadata schema = SchemaMetadata.builder("app").features(FeatureRegistry.builder()
                .add(new Label("mode", "schema")).add(new Label("fallback", "fallback")).build()).build();
        FeatureRegistry table = FeatureRegistry.builder().parent(schema.features())
                .add(new Label("mode", "table")).build();
        DynamicForm first = DynamicForm.builder("first", "items").features(table)
                .addField(DynamicField.of("inherited", "VARCHAR"))
                .addField(DynamicField.of("local", "VARCHAR").withFeatures(FeatureRegistry.builder()
                        .add(new Label("mode", "column")).build()).withLength(20)).build();
        assertEquals("table", mode(first.field("inherited")));
        assertEquals("column", mode(first.field("local")));
        assertEquals("fallback", ((Label) first.field("local").features().feature("fallback")).value());
        DynamicForm second = DynamicForm.builder("second", "items").features(schema.features())
                .addField(first.field("inherited")).addField(first.field("local")).build();
        assertEquals("schema", mode(second.field("inherited")));
        assertEquals("column", mode(second.field("local")));
        assertTrue(first.diffTo(second).isEmpty());
        assertEquals(first.structureFingerprint(), second.structureFingerprint());
        DynamicForm plain = DynamicForm.builder("plain", "items").addField(first.field("inherited")).build();
        assertTrue(plain.field("inherited").features().find("mode").isEmpty());
        assertEquals("table", mode(first.field("inherited")));
    }

    private static String mode(DynamicField field) {
        return ((Label) field.features().feature("mode")).value();
    }
}
