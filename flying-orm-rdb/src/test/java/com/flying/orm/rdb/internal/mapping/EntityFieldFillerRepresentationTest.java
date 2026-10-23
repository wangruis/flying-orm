package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.mapping.EntityFieldFiller;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityFieldFillerRepresentationTest {

    @Test
    void propertySuppliersAreLazyFreshAndUseJavaNamesBeforeEncoding() {
        AtomicInteger sequence = new AtomicInteger();
        Map<String, Supplier<?>> suppliers = new HashMap<>();
        suppliers.put("created", sequence::incrementAndGet);
        suppliers.put("status", () -> Status.DISABLED);
        EntityFieldFiller filler = EntityFieldFiller.byProperty(suppliers);
        suppliers.clear();
        EntityValues<PropertyDefaults> plan = EntityValues.createUncached(PropertyDefaults.class,
                EntityMetadataResolver.createUncached(PropertyDefaults.class), filler);
        assertEquals(0, sequence.get());
        PropertyDefaults empty = new PropertyDefaults(1L, null, null);
        assertEquals(Map.of("id", 1L, "created_seq", 1, "status_code", "D"), plan.readForInsert(empty));
        RepositoryUpsertValues second = plan.repositoryUpsertValues(new PropertyDefaults(2L, null, Status.ACTIVE));
        assertEquals(2, second.insertValues().get("created_seq"));
        assertEquals("A", second.updateValues().get("status_code"));
        assertFalse(second.updateValues().containsKey("created_seq"));
        RepositoryUpsertValues third = plan.repositoryUpsertValues(empty);
        assertEquals(3, third.insertValues().get("created_seq"));
        assertFalse(third.updateValues().containsKey("status_code"));
        plan.readForUpdate(empty);
        assertEquals(3, sequence.get());
        assertNull(empty.created());
        assertNull(empty.status());
    }

    @Test
    void missingPropertySupplierFailsOnlyWhenFillingIsRequired() {
        EntityValues<DefaultEntry> plan = EntityValues.createUncached(DefaultEntry.class,
                EntityMetadataResolver.createUncached(DefaultEntry.class), EntityFieldFiller.byProperty(Map.of()));
        assertEquals("A", plan.readForInsert(new DefaultEntry(1L, Status.ACTIVE)).get("status"));
        assertNull(plan.readForUpdate(new DefaultEntry(1L, null)).get("status"));
        MappingException error = assertThrows(MappingException.class,
                () -> plan.readForInsert(new DefaultEntry(1L, null)));
        assertTrue(error.getCause().getMessage().contains("status"));
    }

    @Test
    void propertySupplierMayProduceNull() {
        EntityValues<DefaultEntry> plan = EntityValues.createUncached(DefaultEntry.class,
                EntityMetadataResolver.createUncached(DefaultEntry.class),
                EntityFieldFiller.byProperty(Map.of("status", () -> null)));
        Map<String, Object> values = plan.readForInsert(new DefaultEntry(1L, null));
        assertTrue(values.containsKey("status"));
        assertNull(values.get("status"));
    }

    @TableName("property_defaults")
    record PropertyDefaults(@TableId Long id,
                            @TableField(value = "created_seq", fill = FieldFill.INSERT_IF_NULL,
                                    updateStrategy = FieldStrategy.NEVER) Integer created,
                            @TableField(value = "status_code", fill = FieldFill.INSERT_DEFAULT) Status status) { }

    @Test
    void nullOnlyInsertFillIsAnExplicitPolicy() {
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> FieldFill.valueOf("INSERT_IF_NULL"));
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> FieldFill.valueOf("INSERT_DEFAULT"));
    }

    @TestFactory
    List<DynamicTest> nullOnlyInsertFillPreservesSuppliedValuesAndSkipsUpdates() {
        List<DynamicTest> tests = new ArrayList<>();
        for (EntityFieldFiller.Operation operation : EntityFieldFiller.Operation.values()) {
            for (Status supplied : java.util.Arrays.asList(null, Status.ACTIVE)) {
                tests.add(DynamicTest.dynamicTest(operation + "/" + supplied, () -> {
                    java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
                    EntityFieldFiller filler = (entity, field, actual, current) -> {
                        calls.incrementAndGet();
                        assertSame(operation, actual);
                        assertNull(current);
                        return Status.DISABLED;
                    };
                    EntityValues<DefaultEntry> plan = EntityValues.createUncached(DefaultEntry.class,
                            EntityMetadataResolver.createUncached(DefaultEntry.class), filler);
                    DefaultEntry entity = new DefaultEntry(1L, supplied);
                    Map<String, Object> row = switch (operation) {
                        case INSERT -> plan.readForInsert(entity);
                        case UPDATE -> plan.readForUpdate(entity);
                        case UPSERT -> plan.repositoryUpsertValues(entity);
                    };
                    boolean filled = supplied == null && operation != EntityFieldFiller.Operation.UPDATE;
                    assertEquals(filled ? "D" : supplied == null ? null : "A", row.get("status"));
                    assertEquals(filled ? 1 : 0, calls.get());
                    assertSame(supplied, entity.status(), "filling must not mutate the source entity");
                    if (row instanceof RepositoryUpsertValues staged) {
                        assertEquals(row.get("status"), staged.updateValues().get("status"));
                    }
                }));
            }
        }
        return tests;
    }

    @Test
    void nullOnlyFillKeepsConflictUpdatePolicyExplicitAndBatchLayoutStable() {
        EntityValues<InsertDefaultEntry> plan = EntityValues.createUncached(InsertDefaultEntry.class,
                EntityMetadataResolver.createUncached(InsertDefaultEntry.class),
                (entity, field, operation, current) -> Status.DISABLED);
        RepositoryUpsertValues filled = plan.repositoryUpsertValues(new InsertDefaultEntry(1L, null));
        RepositoryUpsertValues supplied = plan.repositoryUpsertValues(new InsertDefaultEntry(2L, Status.ACTIVE));

        assertEquals("D", filled.insertValues().get("status"));
        assertEquals("A", supplied.insertValues().get("status"));
        assertFalse(filled.updateValues().containsKey("status"));
        assertFalse(supplied.updateValues().containsKey("status"));
        assertTrue(RepositoryUpsertValues.sameLayout(filled, supplied));
    }

    @Test
    void insertDefaultsDoNotOverwriteExistingValuesButExplicitValuesDo() {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        EntityValues<InsertDefault> plan = EntityValues.createUncached(InsertDefault.class,
                EntityMetadataResolver.createUncached(InsertDefault.class), (entity, field, operation, current) -> {
                    calls.incrementAndGet();
                    assertNull(current);
                    return Status.DISABLED;
                });
        InsertDefault empty = new InsertDefault(1L, null);
        RepositoryUpsertValues filled = plan.repositoryUpsertValues(empty);
        RepositoryUpsertValues supplied = plan.repositoryUpsertValues(new InsertDefault(2L, Status.ACTIVE));
        assertEquals("D", filled.insertValues().get("status"));
        assertFalse(filled.updateValues().containsKey("status"));
        assertEquals("A", supplied.insertValues().get("status"));
        assertEquals("A", supplied.updateValues().get("status"));
        assertNull(empty.status());
        assertEquals(1, calls.get());
        assertNull(plan.readForUpdate(empty).get("status"));
        assertEquals(1, calls.get(), "ordinary updates must not generate insert defaults");
        assertEquals("D", plan.readForInsert(empty).get("status"));
        assertFalse(RepositoryUpsertValues.sameLayout(filled, supplied));
        assertFalse(RepositoryUpsertValues.sameLayout(supplied, filled));
    }

    @Test
    void nullDefaultsAndInsertExcludedFieldsRetainExplicitStageStrategies() {
        EntityValues<InsertDefault> nulls = EntityValues.createUncached(InsertDefault.class,
                EntityMetadataResolver.createUncached(InsertDefault.class), (entity, field, operation, current) -> null);
        RepositoryUpsertValues row = nulls.repositoryUpsertValues(new InsertDefault(1L, null));
        assertTrue(row.insertValues().containsKey("status"));
        assertTrue(row.updateValues().containsKey("status"), "no generated default means no implicit exclusion");
        EntityValues<UpdateOnlyDefault> updateOnly = EntityValues.createUncached(UpdateOnlyDefault.class,
                EntityMetadataResolver.createUncached(UpdateOnlyDefault.class), (entity, field, operation, current) -> {
                    throw new AssertionError("an insert-excluded field must not generate an insert default");
                });
        RepositoryUpsertValues excluded = updateOnly.repositoryUpsertValues(new UpdateOnlyDefault(1L, null));
        assertFalse(excluded.insertValues().containsKey("status"));
        assertTrue(excluded.updateValues().containsKey("status"));
    }

    @TableName("insert_default")
    record InsertDefault(@TableId Long id, @TableField(fill = FieldFill.INSERT_DEFAULT) Status status) { }

    @TableName("update_only_default")
    record UpdateOnlyDefault(@TableId Long id,
                             @TableField(fill = FieldFill.INSERT_DEFAULT, insertStrategy = FieldStrategy.NEVER)
                             Status status) { }

    @Test
    void suppliedValuesNeedNoFillerAndEmptyTextIsNotNull() {
        EntityValues<DefaultText> plan = EntityValues.createUncached(DefaultText.class);
        assertEquals("", plan.readForInsert(new DefaultText(1L, "")).get("text"));
        assertEquals(" ", plan.repositoryUpsertValues(new DefaultText(2L, " ")).get("text"));
        assertNull(plan.readForUpdate(new DefaultText(3L, null)).get("text"));
        org.junit.jupiter.api.Assertions.assertThrows(com.flying.orm.rdb.mapping.MappingException.class,
                () -> plan.readForInsert(new DefaultText(4L, null)));
    }

    @Test
    void nullFillerResultStillUsesTheDeclaredWriteStrategy() {
        EntityValues<DefaultEntry> plan = EntityValues.createUncached(DefaultEntry.class,
                EntityMetadataResolver.createUncached(DefaultEntry.class),
                (entity, field, operation, current) -> null);
        assertNull(plan.readForInsert(new DefaultEntry(1L, null)).get("status"));
        assertEquals(java.util.Set.of("id", "status"), plan.repositoryUpsertValues(
                new DefaultEntry(1L, null)).insertValues().keySet());
    }

    @TestFactory
    List<DynamicTest> fillerReceivesDomainValueBeforeStorageEncoding() {
        List<DynamicTest> tests = new ArrayList<>();
        for (EntityFieldFiller.Operation operation : EntityFieldFiller.Operation.values()) {
            tests.add(DynamicTest.dynamicTest(operation.toString(), () -> {
                EntityFieldFiller filler = (entity, field, actual, currentValue) -> {
                    assertSame(operation, actual);
                    assertSame(Status.ACTIVE, currentValue);
                    return currentValue;
                };

                assertEquals("A", values(filler, operation, new Entry(1L, Status.ACTIVE)).get("status"));
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> fillerResultIsEncodedExactlyOnce() {
        List<DynamicTest> tests = new ArrayList<>();
        for (EntityFieldFiller.Operation operation : EntityFieldFiller.Operation.values()) {
            tests.add(DynamicTest.dynamicTest(operation.toString(), () -> {
                EntityFieldFiller filler = (entity, field, actual, currentValue) -> {
                    assertSame(operation, actual);
                    assertNull(currentValue);
                    return Status.DISABLED;
                };

                assertEquals("D", values(filler, operation, new Entry(1L, null)).get("status"));
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> fillerMayReturnTheStoredParameterRepresentation() {
        List<DynamicTest> tests = new ArrayList<>();
        for (EntityFieldFiller.Operation operation : EntityFieldFiller.Operation.values()) {
            tests.add(DynamicTest.dynamicTest(operation.toString(), () -> {
                EntityFieldFiller filler = (entity, field, actual, currentValue) -> "D";

                assertEquals("D", values(filler, operation, new Entry(1L, Status.ACTIVE)).get("status"));
            }));
        }
        return tests;
    }

    @Test
    void stagedUpsertKeepsAStoredParameterReturnedByTheFiller() {
        EntityMetadata<Entry> metadata = EntityMetadataResolver.createUncached(Entry.class);
        EntityValues<Entry> values = EntityValues.createUncached(
                Entry.class, metadata, (entity, field, operation, currentValue) -> "D");

        RepositoryUpsertValues upsert = values.repositoryUpsertValues(new Entry(1L, Status.ACTIVE));

        assertEquals("D", upsert.get("status"));
        assertEquals("D", upsert.insertValues().get("status"));
        assertEquals("D", upsert.updateValues().get("status"));
    }

    @TestFactory
    List<DynamicTest> fieldsWithoutFillKeepTheirExistingRepresentation() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Status status : Status.values()) {
            tests.add(DynamicTest.dynamicTest(status.toString(), () -> {
                EntityMetadata<PlainEntry> metadata = EntityMetadataResolver.createUncached(PlainEntry.class);
                EntityValues<PlainEntry> values = EntityValues.createUncached(
                        PlainEntry.class, metadata, EntityFieldFiller.none());

                assertEquals(status.code, values.readForInsert(new PlainEntry(1L, status)).get("status"));
            }));
        }
        return tests;
    }

    private static Map<String, Object> values(EntityFieldFiller filler,
                                              EntityFieldFiller.Operation operation,
                                              Entry entry) {
        EntityMetadata<Entry> metadata = EntityMetadataResolver.createUncached(Entry.class);
        EntityValues<Entry> values = EntityValues.createUncached(Entry.class, metadata, filler);
        return switch (operation) {
            case INSERT -> values.readForInsert(entry);
            case UPDATE -> values.readForUpdate(entry);
            case UPSERT -> values.repositoryUpsertValues(entry);
        };
    }

    enum Status {
        ACTIVE("A"), DISABLED("D");

        @EnumValue
        private final String code;

        Status(String code) {
            this.code = code;
        }
    }

    @TableName("filler_entries")
    record Entry(@TableId Long id,
                 @TableField(fill = FieldFill.INSERT_UPDATE) Status status) {
    }

    @TableName("plain_entries")
    record PlainEntry(@TableId Long id, Status status) {
    }

    @TableName("default_entries")
    record DefaultEntry(@TableId Long id,
                        @TableField(fill = FieldFill.INSERT_IF_NULL) Status status) {
    }

    @TableName("insert_default_entries")
    record InsertDefaultEntry(@TableId Long id,
                              @TableField(fill = FieldFill.INSERT_IF_NULL,
                                      updateStrategy = FieldStrategy.NEVER) Status status) {
    }

    @TableName("default_text")
    record DefaultText(@TableId Long id,
                       @TableField(fill = FieldFill.INSERT_IF_NULL) String text) {
    }

}
