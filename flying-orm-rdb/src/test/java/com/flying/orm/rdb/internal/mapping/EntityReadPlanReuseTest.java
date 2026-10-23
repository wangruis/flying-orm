package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EntityReadPlanReuseTest {
    @Test
    void retainedValuesKeepAllPersistentValuesWithoutFillingOrCloningThem() {
        EntityValues<RetainedEntry> plan = EntityValues.createUncached(RetainedEntry.class,
                EntityMetadataResolver.createUncached(RetainedEntry.class), (entity, field, operation, value) -> {
                    throw new AssertionError("retention must not fill entity values");
                });
        byte[] payload = {1, 2};
        RetainedEntry entity = new RetainedEntry(null, Status.ACTIVE, payload, "not persistent");
        Object[] snapshot = plan.readRetainedValues(entity);
        assertArrayEquals(new Object[]{null, "A", payload}, snapshot);
        assertSame(payload, snapshot[2], "budget inspection must preserve shared object identity");
        snapshot[1] = "mutated array";
        assertArrayEquals(new Object[]{null, "A", payload}, plan.readRetainedValues(entity));
    }

    @TableName("retained_entry")
    private record RetainedEntry(@TableField(fill = FieldFill.INSERT_UPDATE,
                                          insertStrategy = FieldStrategy.NEVER,
                                          updateStrategy = FieldStrategy.NEVER) String nullable,
                                 Status status, byte[] payload,
                                 @TableField(exist = false) String ignored) { }

    @Test
    void writeSnapshotsOnlyReadAndFillFieldsParticipatingInTheOperation() {
        List<String> filled = new ArrayList<>();
        EntityValues<SelectiveBean> values = EntityValues.createUncached(SelectiveBean.class,
                EntityMetadataResolver.createUncached(SelectiveBean.class), (entity, field, operation, value) -> {
                    filled.add(field.name());
                    return value;
                });
        SelectiveBean entity = new SelectiveBean();
        assertEquals(Map.of("insert_only", "insert"), values.readForInsert(entity));
        assertEquals(List.of("insertOnly"), filled);
        assertEquals(1, entity.insertReads);
        assertEquals(0, entity.updateReads);

        filled.clear();
        assertEquals(Map.of("update_only", "update"), values.readForUpdate(entity));
        assertEquals(List.of("updateOnly"), filled);
        assertEquals(1, entity.insertReads);
        assertEquals(1, entity.updateReads);

        filled.clear();
        assertEquals(Map.of("update_only", "update"),
                values.readForUpdate(entity, new java.util.LinkedHashSet<>(
                        List.of("ignored", "insert_only", "update_only"))));
        assertEquals(List.of("updateOnly"), filled);
        assertEquals(1, entity.insertReads);
        assertEquals(2, entity.updateReads);
    }

    @Test
    void upsertReadsEachParticipatingFieldOnceButDoesNotTouchNeverFields() {
        SelectiveBean entity = new SelectiveBean();
        EntityValues<SelectiveBean> values = EntityValues.createUncached(SelectiveBean.class,
                EntityMetadataResolver.createUncached(SelectiveBean.class), (source, field, operation, value) -> value);
        RepositoryUpsertValues snapshot = values.repositoryUpsertValues(entity);
        assertEquals(Map.of("insert_only", "insert"), snapshot.insertValues());
        assertEquals(Map.of("update_only", "update"), snapshot.updateValues());
        assertEquals(1, entity.insertReads);
        assertEquals(1, entity.updateReads);
        assertThrows(com.flying.orm.rdb.mapping.MappingException.class, () -> values.readRetainedValues(entity),
                "the unfiltered snapshot must still read every persistent property");
    }

    @Test
    void databaseGeneratedValueIsNotReadToBuildInsertParameters() {
        assertEquals(Map.of("name", "stored"), EntityValues.createUncached(GeneratedBean.class)
                .readForInsert(new GeneratedBean()));
    }

    @Test
    void repositorySetSkipsVersionFillWithoutChangingGenericUpdateSnapshots() {
        EntityValues<FilledVersionEntry> plan = EntityValues.createUncached(FilledVersionEntry.class,
                EntityMetadataResolver.createUncached(FilledVersionEntry.class), (entity, field, operation, value) -> {
                    throw new IllegalStateException("version fill must not run for a discarded SET field");
                });
        FilledVersionEntry entry = new FilledVersionEntry(7L, 2L, 0, Status.ACTIVE);
        assertEquals(Map.of("deleted", 0, "status_code", "A"), plan.readForRepositoryUpdate(entry));
        assertEquals(Map.of("id", 7L, "revision", 2L), plan.readIdentity(entry));
        assertThrows(com.flying.orm.rdb.mapping.MappingException.class, () -> plan.readForUpdate(entry));
    }

    @Test
    void versionAndLogicalDeleteSelectionsAreResolvedAtMetadataConstruction() {
        EntityMetadata<Entry> metadata = EntityMetadataResolver.createUncached(Entry.class);
        assertSame(metadata.versionField(), metadata.versionField());
        assertSame(metadata.logicDeleteField(), metadata.logicDeleteField());
        assertEquals("revision", metadata.versionField().orElseThrow().name());
        assertEquals("deleted", metadata.logicDeleteField().orElseThrow().name());
        EntityMetadata<Bean> plain = EntityMetadataResolver.createUncached(Bean.class);
        assertTrue(plain.versionField().isEmpty());
        assertTrue(plain.logicDeleteField().isEmpty());
    }

    @Test
    void recordReadersRetainExactFieldMetadataIncludingEncodedReaders() throws Exception {
        EntityMetadata<Entry> metadata = EntityMetadataResolver.createUncached(Entry.class);
        EntityValues<Entry> plan = EntityValues.createUncached(Entry.class, metadata);
        assertBoundMetadata(plan, metadata);
        Entry first = new Entry(7L, 2L, 0, Status.ACTIVE);
        Entry second = new Entry(8L, 3L, 0, Status.DISABLED);
        Map<String, Object> firstValues = plan.readForUpdate(first);
        assertEquals("A", firstValues.get("status_code"));
        assertEquals("D", plan.readForUpdate(second).get("status_code"));
        assertEquals("A", firstValues.get("status_code"));
        assertEquals(List.of("revision", "deleted", "status_code"), List.copyOf(firstValues.keySet()));
        assertEquals(Map.of("id", 7L, "revision", 2L), plan.readIdentity(first));
        assertEquals(Map.of("status_code", "A"), plan.readForUpdate(first, Set.of("status_code")));
        assertEquals("D", plan.repositoryUpsertValues(second).insertValues().get("status_code"));
    }

    @Test
    void beanGetterAndFieldFallbackUseTheSameBoundMetadataContract() throws Exception {
        EntityMetadata<Bean> metadata = EntityMetadataResolver.createUncached(Bean.class);
        EntityValues<Bean> plan = EntityValues.createUncached(Bean.class, metadata);
        assertBoundMetadata(plan, metadata);
        Bean entity = new Bean();
        assertArrayEquals(new Object[]{"from-getter", 11L}, plan.readRetainedValues(entity));
        assertEquals(1, entity.reads);
        assertEquals(Map.of("id", 11L), plan.readIdentity(entity));
        assertEquals(1, entity.reads);
    }

    // Inspect the cached plan without adding test-only accessors or mutating production objects.
    private static void assertBoundMetadata(EntityValues<?> plan, EntityMetadata<?> metadata) throws Exception {
        Field readersField = EntityValues.class.getDeclaredField("readers");
        readersField.setAccessible(true);
        Map<?, ?> readers = (Map<?, ?>) readersField.get(plan);
        for (Map.Entry<?, ?> entry : readers.entrySet()) {
            Object reader = entry.getValue();
            Method field = reader.getClass().getDeclaredMethod("field");
            field.setAccessible(true);
            assertSame(metadata.field((String) entry.getKey()), field.invoke(reader));
        }
    }

    enum Status {
        ACTIVE("A"), DISABLED("D");
        @EnumValue
        private final String code;
        Status(String code) { this.code = code; }
    }

    @TableName("entries")
    record Entry(@TableId Long id, @Version Long revision, @TableLogic Integer deleted,
                 @TableField("status_code") Status status) { }

    @TableName("entries")
    record FilledVersionEntry(@TableId Long id, @Version @TableField(fill = FieldFill.UPDATE) Long revision,
                              @TableLogic Integer deleted, @TableField("status_code") Status status) { }

    @TableName("beans")
    static final class Bean {
        @TableId
        private Long id = 11L;
        private String label;
        @TableField(exist = false)
        private int reads;
        public String getLabel() { reads++; return "from-getter"; }
    }

    @TableName("selective_beans")
    static final class SelectiveBean {
        @TableField(insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER,
                fill = FieldFill.INSERT_UPDATE)
        private String ignored;
        @TableField(updateStrategy = FieldStrategy.NEVER, fill = FieldFill.INSERT_UPDATE)
        private String insertOnly;
        @TableField(insertStrategy = FieldStrategy.NEVER, fill = FieldFill.INSERT_UPDATE)
        private String updateOnly;
        @TableField(exist = false)
        private int insertReads;
        @TableField(exist = false)
        private int updateReads;
        public String getIgnored() { throw new IllegalStateException("never-read property"); }
        public String getInsertOnly() { insertReads++; return "insert"; }
        public String getUpdateOnly() { updateReads++; return "update"; }
    }

    @TableName("generated_beans")
    static final class GeneratedBean {
        @TableId(type = IdType.AUTO)
        private Long id;
        private String name = "stored";
        public Long getId() { throw new IllegalStateException("generated value is absent before insert"); }
    }
}
