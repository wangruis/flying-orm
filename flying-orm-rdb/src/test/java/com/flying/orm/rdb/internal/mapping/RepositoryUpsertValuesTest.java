package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RepositoryUpsertValuesTest {

    @Test
    void runtimeDefaultsDoNotReintroduceFieldsExcludedFromInsertStage() {
        com.flying.orm.core.form.DynamicForm form = com.flying.orm.core.form.DynamicForm.builder("t", "t")
                .addField(com.flying.orm.core.form.DynamicField.primaryKey("id", "BIGINT"))
                .addField(com.flying.orm.core.form.DynamicField.of("name", "VARCHAR")
                        .withRuntimeDefault(() -> { throw new AssertionError("excluded insert field"); })).build();
        Map<String, Object> updates = Map.of("name", "update-only");
        RepositoryUpsertValues row = new RepositoryUpsertValues(
                Map.of("id", 1L, "name", "update-only"), Map.of("id", 1L), updates);
        assertSame(row, RepositoryUpsertValues.prepareInsert(form, row, false, Set.of()));
        assertEquals(updates, row.updateValues());
    }

    @Test
    void exclusionsRetainInsertNullsAndCanonicalizeEachRowWithoutMutatingTheInput() {
        com.flying.orm.core.form.DynamicForm form = com.flying.orm.core.form.DynamicForm.builder("t", "t")
                .addField(com.flying.orm.core.form.DynamicField.primaryKey("id", "BIGINT"))
                .addField(com.flying.orm.core.form.DynamicField.of("name", "VARCHAR")).build();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 1L);
        row.put("NAME", null);
        assertSame(row, RepositoryUpsertValues.prepareInsert(form, row, false, Set.of()));
        RepositoryUpsertValues prepared = (RepositoryUpsertValues) RepositoryUpsertValues.prepareInsert(
                form, row, false, Set.of("name"));
        assertEquals(List.of("id", "name"), List.copyOf(prepared.keySet()));
        assertTrue(prepared.insertValues().containsKey("name"));
        assertFalse(prepared.updateValues().containsKey("name"));
        row.put("NAME", "changed");
        assertNull(prepared.get("name"));
        assertThrows(UnsupportedOperationException.class, () -> prepared.put("id", 2L));
        assertThrows(UnsupportedOperationException.class, () -> prepared.updateValues().put("name", "changed"));
        Map<String, Object> second = RepositoryUpsertValues.prepareInsert(form, Map.of("id", 2L, "name", "next"), false, Set.of("name"));
        assertTrue(RepositoryUpsertValues.sameLayout(prepared, second));
        row.put("name", "duplicate");
        assertThrows(IllegalArgumentException.class, () -> RepositoryUpsertValues.prepareInsert(form, row, false, Set.of("name")));
    }

    @Test
    void ignoreConflictsPreservesOriginalInsertStageRegardlessOfExclusions() {
        RepositoryUpsertValues row = EntityValues.createUncached(Split.class)
                .repositoryUpsertValues(new Split("prefix", null, "middle", "changed", "tail", "ignored"));
        assertSame(row.insertValues(), RepositoryUpsertValues.prepareInsert(
                EntityMetadataResolver.createUncached(Split.class).toDynamicForm(), row, true, Set.of("middle")));
    }

    @Test
    void identicalStagesShareOneOwnedReadOnlyMap() {
        RepositoryUpsertValues snapshot = EntityValues.createUncached(Shared.class)
                .repositoryUpsertValues(new Shared("first", null));

        assertEquals(List.of("name", "note"), List.copyOf(snapshot.keySet()));
        assertNull(snapshot.get("note"));
        assertSame(snapshot.entrySet(), snapshot.insertValues().entrySet());
        assertSame(snapshot.insertValues(), snapshot.updateValues());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.insertValues().put("note", "changed"));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.updateValues().remove("name"));
    }

    @Test
    void ordinaryIdentityOnlySplitsTheUpdateStage() {
        EntityValues<WithIdentity> values = EntityValues.createUncached(WithIdentity.class);
        RepositoryUpsertValues first = values.repositoryUpsertValues(new WithIdentity(1L, "first"));
        RepositoryUpsertValues second = values.repositoryUpsertValues(new WithIdentity(2L, "second"));

        assertSame(first.entrySet(), first.insertValues().entrySet());
        assertEquals(Map.of("name", "first"), first.updateValues());
        assertEquals(Map.of("id", 1L, "name", "first"), first);
        assertEquals(Map.of("id", 2L, "name", "second"), second);
        assertNotSame(first.insertValues(), second.insertValues());
    }

    @Test
    void stageForksKeepOrderAndDoNotReintroduceExcludedColumns() {
        RepositoryUpsertValues snapshot = EntityValues.createUncached(Split.class)
                .repositoryUpsertValues(new Split("prefix", null, "middle", "changed", "tail", "ignored"));

        assertEquals(List.of("prefix", "insert_only", "middle", "update_only", "tail"),
                List.copyOf(snapshot.keySet()));
        assertEquals(List.of("prefix", "insert_only", "middle", "tail"),
                List.copyOf(snapshot.insertValues().keySet()));
        assertEquals(List.of("prefix", "middle", "update_only", "tail"),
                List.copyOf(snapshot.updateValues().keySet()));
        assertNull(snapshot.insertValues().get("insert_only"));
        assertFalse(snapshot.updateValues().containsKey("insert_only"));
        assertFalse(snapshot.insertValues().containsKey("update_only"));
        assertEquals("changed", snapshot.updateValues().get("update_only"));
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.entrySet().iterator().next().setValue("changed"));
    }

    @Test
    void conditionalStagesSplitPerSnapshotWithoutSharingMutableState() {
        EntityValues<Conditional> values = EntityValues.createUncached(Conditional.class);
        RepositoryUpsertValues omitted = values.repositoryUpsertValues(new Conditional(null, "tail"));
        RepositoryUpsertValues included = values.repositoryUpsertValues(new Conditional("present", "tail"));

        assertEquals(Map.of("tail", "tail"), omitted.insertValues());
        assertTrue(omitted.updateValues().containsKey("value"));
        assertNull(omitted.updateValues().get("value"));
        assertSame(omitted.entrySet(), omitted.updateValues().entrySet());
        assertSame(included.insertValues(), included.updateValues());
        assertEquals(Map.of("value", "present", "tail", "tail"), included);
        assertFalse(omitted.insertValues().containsKey("value"));
    }

    @Test
    void keyLookupsReuseTheOwnedMapIndex() {
        IndexedValues values = new IndexedValues();
        for (int index = 0; index < 64; index++) values.put("column" + index, index);
        values.put("nullable", null);
        RepositoryUpsertValues snapshot = new RepositoryUpsertValues(values, Map.of(), Map.of());

        assertEquals(63, snapshot.get("column63"));
        assertTrue(snapshot.containsKey("nullable"));
        assertNull(snapshot.get("nullable"));
        assertFalse(snapshot.containsKey("absent"));
        assertEquals("fallback", snapshot.getOrDefault("absent", "fallback"));
        assertNull(snapshot.getOrDefault("nullable", "fallback"));
        assertEquals(0, values.entryViews, "key lookup must not fall back to entry traversal");
    }

    @Test
    void indexedAccessKeepsOrderedReadOnlyAndStageSemantics() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 7L);
        values.put("insertOnly", null);
        values.put("updateOnly", "changed");
        Map<String, Object> inserts = new LinkedHashMap<>();
        inserts.put("id", 7L);
        inserts.put("insertOnly", null);
        Map<String, Object> updates = Map.of("updateOnly", "changed");
        RepositoryUpsertValues snapshot = new RepositoryUpsertValues(values, inserts, updates);

        assertEquals(List.of("id", "insertOnly", "updateOnly"), snapshot.keySet().stream().toList());
        assertEquals(values, snapshot);
        assertEquals(snapshot, values);
        assertEquals(values.hashCode(), snapshot.hashCode());
        assertEquals(inserts, snapshot.insertValues());
        assertEquals(updates, snapshot.updateValues());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.put("id", 8L));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.remove("id"));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.entrySet().iterator().next().setValue(8L));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.insertValues().put("id", 8L));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.updateValues().clear());
    }

    @TableName("shared")
    private record Shared(String name, String note) {}

    @TableName("with_identity")
    private record WithIdentity(@TableId(type = IdType.INPUT) Long id, String name) {}

    @TableName("split")
    private record Split(String prefix,
                         @TableField(updateStrategy = FieldStrategy.NEVER) String insertOnly,
                         String middle,
                         @TableField(insertStrategy = FieldStrategy.NEVER) String updateOnly,
                         String tail,
                         @TableField(insertStrategy = FieldStrategy.NEVER,
                                 updateStrategy = FieldStrategy.NEVER) String ignored) {}

    @TableName("conditional")
    private record Conditional(@TableField(insertStrategy = FieldStrategy.NOT_NULL) String value, String tail) {}

    private static final class IndexedValues extends LinkedHashMap<String, Object> {
        private int entryViews;

        @Override
        public Set<Map.Entry<String, Object>> entrySet() {
            entryViews++;
            return super.entrySet();
        }
    }
}
