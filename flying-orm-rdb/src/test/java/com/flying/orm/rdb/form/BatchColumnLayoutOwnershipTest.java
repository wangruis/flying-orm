package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BatchColumnLayoutOwnershipTest {
    private static final DynamicForm FORM = DynamicForm.builder("items", "items")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();

    @Test
    void declaredWriteOrderReusesImmutableMetadataWithoutRetainingTheInputMap() {
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put(" ID ", 1L);
        row.put("NAME", "Ada");
        FormSqlRenderSupport.WriteFields values = support().writeFields(FORM, row, 0L);
        assertSame(FORM.fields(), values.fields());
        row.clear();
        assertArrayEquals(new Object[]{1L, "Ada"}, values.parameters());
        assertThrows(UnsupportedOperationException.class, () -> values.fields().clear());
        assertEquals(List.of(FORM.field("id")),
                support().writeFields(FORM, Map.of("id", 2L), 0L).fields());
    }

    @Test
    void reorderedWritesRetainValuesAndRejectDuplicatesOfAnAlreadyMatchedPrefix() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("age", "INTEGER")).build();
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("id", 1L);
        row.put("age", 20);
        row.put("name", null);
        FormSqlRenderSupport.WriteFields values = support().writeFields(form, row, 0L);
        assertEquals(List.of(form.field("id"), form.field("age"), form.field("name")), values.fields());
        assertArrayEquals(new Object[]{1L, 20, null}, values.parameters());
        row.put("ID", 2L);
        assertEquals("duplicate normalized dynamic write field", assertThrows(IllegalArgumentException.class,
                () -> support().writeFields(form, row, 0L)).getMessage());
        row.clear();
        row.put("id", 1L);
        row.put("ID", 2L);
        assertThrows(IllegalArgumentException.class, () -> support().writeFields(form, row, 0L));
        row.clear();
        row.put("id", 1L);
        row.put("name", "Ada");
        row.put("ID", 2L);
        assertEquals("duplicate normalized dynamic write field", assertThrows(IllegalArgumentException.class,
                () -> support().writeFields(FORM, row, 0L)).getMessage());
    }

    @Test
    void sourceListMutationCannotChangeParameterOrderOrPublishedFields() {
        List<DynamicField> source = new ArrayList<>(List.of(FORM.field("name"), FORM.field("id")));
        BatchColumnLayout layout = layout(FORM, source);
        source.clear();
        source.add(DynamicField.of("foreign", "VARCHAR"));
        assertEquals(List.of(FORM.field("name"), FORM.field("id")), layout.fields());
        assertThrows(UnsupportedOperationException.class, () -> layout.fields().clear());
        assertArrayEquals(new Object[]{"Ada", 1L}, layout.parameters(Map.of("ID", 1L, "NAME", "Ada"), 1));
    }

    @Test
    void completeDeclaredOrderNeedsNoNameIndexButReorderingStillBuildsOne() throws ReflectiveOperationException {
        BatchColumnLayout layout = layout(FORM, FORM.fields());
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put(" ID ", 1L);
        row.put("NAME", null);
        assertArrayEquals(new Object[]{1L, null}, layout.parameters(row, 1));
        java.lang.reflect.Field indexes = BatchColumnLayout.class.getDeclaredField("indexesByNormalizedName");
        indexes.setAccessible(true);
        assertNull(indexes.get(layout));
        row.clear();
        row.put("name", "Ada");
        row.put("id", 2L);
        assertArrayEquals(new Object[]{2L, "Ada"}, layout.parameters(row, 2));
        assertNotNull(indexes.get(layout));
        row.clear();
        row.put("id", 2L);
        row.put("ID", 3L);
        assertThrows(IllegalArgumentException.class, () -> layout.parameters(row, 3));
    }

    @Test
    void partialFirstRowOrderBindsWithoutANameIndex() throws ReflectiveOperationException {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("displayName", "VARCHAR"))
                .addField(DynamicField.of("age", "INTEGER")).build();
        FormSqlRenderSupport support = support();
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("age", 20);
        row.put("displayName", null);
        BatchColumnLayout layout = BatchColumnLayout.of(form, support.writeFields(form, row, 0L), support);
        assertArrayEquals(new Object[]{20, null}, layout.parameters(row, 1));
        row.clear();
        row.put(" AGE ", 21);
        row.put("DISPLAYNAME", "Ada");
        assertArrayEquals(new Object[]{21, "Ada"}, layout.parameters(row, 2));
        java.lang.reflect.Field indexes = BatchColumnLayout.class.getDeclaredField("indexesByNormalizedName");
        indexes.setAccessible(true);
        assertNull(indexes.get(layout), "the first row defines positional binding, not the entire form");
        row.clear();
        row.put("displayName", null);
        row.put("age", 22);
        Object[] parameters = {"old", "old", "scope"};
        layout.writeParameters(row, 3, parameters);
        assertArrayEquals(new Object[]{22, null, "scope"}, parameters);
        row.clear();
        row.put("age", 23);
        row.put("displayName", "Bob");
        layout.writeParameters(row, 4, parameters);
        assertArrayEquals(new Object[]{23, "Bob", "scope"}, parameters);
    }

    @Test
    void indexedLayoutFieldsMustBelongToTheFormAndHaveAUniqueNormalizedName() {
        assertArrayEquals(new Object[0], layout(FORM, List.of()).parameters(Map.of(), 1));
        IllegalArgumentException duplicate = assertThrows(IllegalArgumentException.class,
                () -> layout(FORM,
                        List.of(FORM.field("id"), DynamicField.of("ID", "BIGINT"))).indexOf("id"));
        assertEquals("duplicate batch column field: ID", duplicate.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> layout(FORM,
                        List.of(DynamicField.of("foreign", "VARCHAR"))).indexOf("foreign"));
        BatchColumnLayout subset = layout(FORM, List.of(FORM.field("id")));
        assertArrayEquals(new Object[]{1L}, subset.parameters(Map.of("id", 1L), 1));
        assertThrows(IllegalArgumentException.class, () -> subset.parameters(Map.of("name", "Ada"), 2));
    }

    @Test
    void layoutLookupPreservesFieldIdentityAndUnknownFieldErrors() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("\u0130D", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
        BatchColumnLayout layout = layout(form, List.of(form.field("\u0130D")));
        assertArrayEquals(new Object[]{7L}, layout.parameters(Map.of(" i\u0307d ", 7L), 1));
        assertEquals("dynamic field does not exist", assertThrows(IllegalArgumentException.class,
                () -> layout.parameters(Map.of("missing", 7L), 2)).getMessage());
        assertEquals("batch insert row [3] fields must match the first row",
                assertThrows(IllegalArgumentException.class,
                        () -> layout.parameters(Map.of("name", "Ada"), 3)).getMessage());
        assertThrows(IllegalArgumentException.class, () -> layout.parameters(Map.of(" ", 7L), 4));
    }

    @Test
    void publicFirstRowStillRejectsDuplicateAndUnknownFieldsBeforePublishingARequest() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        Map<String, Object> duplicate = new java.util.LinkedHashMap<>();
        duplicate.put("id", 1L);
        duplicate.put("ID", 2L);
        assertThrows(IllegalArgumentException.class, () -> renderer.insertBatch(FORM, List.of(duplicate)));
        assertThrows(IllegalArgumentException.class,
                () -> renderer.insertBatch(FORM, List.of(Map.of("foreign", 1L))));
    }

    @Test
    void sharedLayoutPublishesItsLazyIndexSafelyForConcurrentRows() {
        BatchColumnLayout layout = layout(FORM, FORM.fields());
        java.util.stream.IntStream.range(0, 100).parallel().forEach(index ->
                assertArrayEquals(new Object[]{(long) index, "row"},
                        layout.parameters(Map.of("NAME", "row", "ID", (long) index), index)));
    }

    private static BatchColumnLayout layout(DynamicForm form, List<DynamicField> fields) {
        return BatchColumnLayout.of(form,
                new FormSqlRenderSupport.WriteFields(fields, new Object[fields.size()], List.of(), null), support());
    }

    private static FormSqlRenderSupport support() {
        RdbDialect dialect = RdbDialect.postgresql();
        return new FormSqlRenderSupport(SqlRenderer.builder().addDefaultTerms().build(),
                dialect.json(), dialect.name(), true, dialect.schema()::identifier,
                StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()), java.util.Map.of(),
                dialect.capabilities(), dialect.schema()::relationIdentifier);
    }
}
